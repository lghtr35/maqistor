use std::{
    fs::File, future::Future, io::BufReader, marker::PhantomData, num::NonZeroU32, pin::Pin,
    sync::Arc,
};

use maqistor_worker_protocol::{
    JobResult, ProtocolError, WireFrame, WorkerMessage, decode_frame, encode_frame,
};
use rustls::{
    ClientConfig, RootCertStore,
    pki_types::{CertificateDer, ServerName},
};
use serde::de::DeserializeOwned;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::{Mutex, Semaphore},
};
use tokio_rustls::TlsConnector;
use uuid::Uuid;

pub trait Queue: Send + Sync + 'static {
    type Payload: DeserializeOwned + Send + 'static;
    const NAME: &'static str;
}

#[derive(Debug)]
pub struct Job<T> {
    pub id: i64,
    pub dispatch_id: String,
    pub execution_count: u32,
    pub payload: T,
}

#[derive(Debug, Clone)]
pub struct WorkerConnection {
    pub maqistor_addr: String,
    pub server_name: String,
    pub ca_cert_path: String,
    pub client_cert_path: String,
    pub client_key_path: String,
}

type Handler<Q> = Arc<
    dyn Fn(
            Job<<Q as Queue>::Payload>,
        ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, String>> + Send>>
        + Send
        + Sync,
>;

pub struct Worker<Q: Queue> {
    connection: WorkerConnection,
    concurrency: NonZeroU32,
    handler: Handler<Q>,
    _queue: PhantomData<Q>,
}
impl<Q: Queue> Worker<Q> {
    pub fn new<F, Fut>(connection: WorkerConnection, concurrency: NonZeroU32, handler: F) -> Self
    where
        F: Fn(Job<Q::Payload>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Vec<u8>, String>> + Send + 'static,
    {
        Self {
            connection,
            concurrency,
            handler: Arc::new(move |job| Box::pin(handler(job))),
            _queue: PhantomData,
        }
    }

    pub async fn run(self) -> Result<(), WorkerRunError> {
        let tcp = TcpStream::connect(&self.connection.maqistor_addr).await?;
        let server_name = ServerName::try_from(self.connection.server_name.clone())
            .map_err(|_| WorkerRunError::Configuration("invalid TLS server name".into()))?;
        let connector = TlsConnector::from(Arc::new(client_config(&self.connection)?));
        let stream = connector.connect(server_name, tcp).await?;
        let (mut reader, writer) = tokio::io::split(stream);
        let instance_id = Uuid::new_v4();
        let lifecycle: AsyncWorkerLifecycle<Q> = AsyncWorkerLifecycle {
            stream: Arc::new(Mutex::new(writer)),
            handler: self.handler,
            slots: Arc::new(Semaphore::new(self.concurrency.get() as usize)),
            concurrency: self.concurrency.get(),
            _queue: PhantomData,
        };
        lifecycle
            .write(WorkerMessage::Register {
                instance_id,
                queue_name: Q::NAME.into(),
                running_jobs: 0,
                free_slots: self.concurrency.get(),
            })
            .await?;
        let heartbeats = {
            let lifecycle = lifecycle.clone();
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
                loop {
                    interval.tick().await;
                    if lifecycle.write(WorkerMessage::Heartbeat).await.is_err() {
                        break;
                    }
                }
            })
        };
        loop {
            let message = read_async_frame(&mut reader).await?.payload;
            match message {
                WorkerMessage::Registered { queue_name } if queue_name == Q::NAME => {}
                WorkerMessage::JobDispatch {
                    job_id,
                    dispatch_id,
                    execution_count,
                    payload,
                } => {
                    let lifecycle = lifecycle.clone();
                    tokio::spawn(async move {
                        let _ = lifecycle
                            .execute_dispatch(job_id, dispatch_id, execution_count, payload)
                            .await;
                    });
                }
                WorkerMessage::Error { code, message } => {
                    heartbeats.abort();
                    return Err(WorkerRunError::Remote { code, message });
                }
                WorkerMessage::Heartbeat => {}
                _ => {
                    heartbeats.abort();
                    return Err(WorkerRunError::Configuration(
                        "unexpected worker frame".into(),
                    ));
                }
            }
        }
    }
}

fn client_config(connection: &WorkerConnection) -> Result<ClientConfig, WorkerRunError> {
    let mut roots = RootCertStore::empty();
    for cert in certs(&connection.ca_cert_path)? {
        roots
            .add(cert)
            .map_err(|err| WorkerRunError::Configuration(err.to_string()))?;
    }
    let certs = certs(&connection.client_cert_path)?;
    let key = key(&connection.client_key_path)?;
    Ok(ClientConfig::builder()
        .with_root_certificates(roots)
        .with_client_auth_cert(certs, key)?)
}
fn certs(path: &str) -> Result<Vec<CertificateDer<'static>>, WorkerRunError> {
    let mut reader = BufReader::new(File::open(path)?);
    Ok(rustls_pemfile::certs(&mut reader).collect::<Result<Vec<_>, _>>()?)
}
fn key(path: &str) -> Result<rustls::pki_types::PrivateKeyDer<'static>, WorkerRunError> {
    let mut reader = BufReader::new(File::open(path)?);
    rustls_pemfile::private_key(&mut reader)?
        .ok_or_else(|| WorkerRunError::Configuration("no client key in PEM".into()))
}

async fn read_async_frame<R: AsyncRead + Unpin>(
    reader: &mut R,
) -> Result<WireFrame, ProtocolError> {
    let mut length = [0; 4];
    reader.read_exact(&mut length).await?;
    let size = u32::from_be_bytes(length) as usize;
    if size > maqistor_worker_protocol::MAX_FRAME_BYTES {
        return Err(ProtocolError::FrameTooLarge);
    }
    let mut body = vec![0; size];
    reader.read_exact(&mut body).await?;
    let mut frame = length.to_vec();
    frame.extend(body);
    decode_frame(&frame)
}

struct AsyncWorkerLifecycle<Q: Queue> {
    stream: Arc<Mutex<tokio::io::WriteHalf<tokio_rustls::client::TlsStream<TcpStream>>>>,
    handler: Handler<Q>,
    slots: Arc<Semaphore>,
    concurrency: u32,
    _queue: PhantomData<Q>,
}
impl<Q: Queue> Clone for AsyncWorkerLifecycle<Q> {
    fn clone(&self) -> Self {
        Self {
            stream: self.stream.clone(),
            handler: self.handler.clone(),
            slots: self.slots.clone(),
            concurrency: self.concurrency,
            _queue: PhantomData,
        }
    }
}
impl<Q: Queue> AsyncWorkerLifecycle<Q> {
    async fn write(&self, payload: WorkerMessage) -> Result<(), WorkerRunError> {
        let frame = WireFrame::v1(payload);
        let mut stream = self.stream.lock().await;
        stream.write_all(&encode_frame(&frame)?).await?;
        Ok(())
    }
    async fn execute_dispatch(
        &self,
        job_id: i64,
        dispatch_id: String,
        execution_count: u32,
        payload: Vec<u8>,
    ) -> Result<(), WorkerRunError> {
        let payload = match serde_json::from_slice(&payload) {
            Ok(payload) => payload,
            Err(err) => {
                self.report(
                    job_id,
                    dispatch_id,
                    Err(format!("invalid job payload: {err}")),
                )
                .await?;
                return Ok(());
            }
        };
        let slot = self
            .slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| WorkerRunError::Stopped)?;
        let result = (self.handler)(Job {
            id: job_id,
            dispatch_id: dispatch_id.clone(),
            execution_count,
            payload,
        })
        .await;
        drop(slot);
        self.report(job_id, dispatch_id, result).await
    }
    async fn report(
        &self,
        job_id: i64,
        dispatch_id: String,
        result: Result<Vec<u8>, String>,
    ) -> Result<(), WorkerRunError> {
        let free_slots = self.slots.available_permits() as u32;
        let result = match result {
            Ok(payload) => JobResult::Succeeded { payload },
            Err(message) => JobResult::Failed { message },
        };
        self.write(WorkerMessage::JobResult {
            job_id,
            dispatch_id,
            result,
            running_jobs: self.concurrency.saturating_sub(free_slots),
            free_slots,
        })
        .await
    }
}

#[derive(Debug, thiserror::Error)]
pub enum WorkerRunError {
    #[error("worker stopped")]
    Stopped,
    #[error("worker configuration error: {0}")]
    Configuration(String),
    #[error("remote worker error {code}: {message}")]
    Remote { code: String, message: String },
    #[error("protocol error: {0}")]
    Protocol(#[from] ProtocolError),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("TLS error: {0}")]
    Tls(#[from] rustls::Error),
}
