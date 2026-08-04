use std::{
    fs::File, future::Future, io::BufReader, num::NonZeroU32, pin::Pin, sync::Arc,
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
    sync::{Semaphore, mpsc},
};
use tokio_rustls::TlsConnector;
use uuid::Uuid;

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

type Handler<P> = Arc<
    dyn Fn(Job<P>) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, String>> + Send>>
        + Send
        + Sync,
>;

type Outbound = mpsc::UnboundedSender<WorkerMessage>;

pub struct Worker<P> {
    connection: WorkerConnection,
    concurrency: NonZeroU32,
    handler: Handler<P>,
    queue_name: &'static str,
}

impl<P> Worker<P>
where
    P: DeserializeOwned + Send + 'static,
{
    pub fn new<F, Fut>(
        connection: WorkerConnection,
        queue_name: &'static str,
        concurrency: NonZeroU32,
        handler: F,
    ) -> Self
    where
        F: Fn(Job<P>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Vec<u8>, String>> + Send + 'static,
    {
        Self {
            connection,
            concurrency,
            handler: Arc::new(move |job| Box::pin(handler(job))),
            queue_name,
        }
    }

    pub async fn run(self) -> Result<(), WorkerRunError> {
        let tcp = TcpStream::connect(&self.connection.maqistor_addr).await?;
        let server_name = ServerName::try_from(self.connection.server_name.clone())
            .map_err(|_| WorkerRunError::Configuration("invalid TLS server name".into()))?;
        let connector = TlsConnector::from(Arc::new(client_config(&self.connection)?));
        let stream = connector.connect(server_name, tcp).await?;
        let (mut reader, mut writer) = tokio::io::split(stream);

        let queue_name = self.queue_name;
        let concurrency = self.concurrency.get();
        let handler = self.handler;
        let slots = Arc::new(Semaphore::new(concurrency as usize));

        write_message(
            &mut writer,
            WorkerMessage::Register {
                instance_id: Uuid::new_v4(),
                queue_name: queue_name.into(),
                running_jobs: 0,
                free_slots: concurrency,
            },
        )
        .await?;

        let (outbound, inbound) = mpsc::unbounded_channel();
        let writer_task = tokio::spawn(async move {
            writer_loop(writer, inbound).await;
        });

        let heartbeats = {
            let outbound = outbound.clone();
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
                loop {
                    interval.tick().await;
                    if outbound.send(WorkerMessage::Heartbeat).is_err() {
                        break;
                    }
                }
            })
        };

        let result = serve(
            &mut reader,
            queue_name,
            concurrency,
            handler,
            slots,
            outbound,
        )
        .await;

        heartbeats.abort();
        writer_task.abort();
        result
    }
}

async fn serve<P>(
    reader: &mut tokio::io::ReadHalf<tokio_rustls::client::TlsStream<TcpStream>>,
    queue_name: &'static str,
    concurrency: u32,
    handler: Handler<P>,
    slots: Arc<Semaphore>,
    outbound: Outbound,
) -> Result<(), WorkerRunError>
where
    P: DeserializeOwned + Send + 'static,
{
    loop {
        let message = read_async_frame(reader).await?.payload;
        match message {
            WorkerMessage::Registered {
                queue_name: registered,
            } if registered == queue_name => {}
            WorkerMessage::JobDispatch {
                job_id,
                dispatch_id,
                execution_count,
                payload,
            } => {
                let handler = handler.clone();
                let slots = slots.clone();
                let outbound = outbound.clone();
                tokio::spawn(async move {
                    execute_job(
                        handler,
                        slots,
                        concurrency,
                        outbound,
                        job_id,
                        dispatch_id,
                        execution_count,
                        payload,
                    )
                    .await;
                });
            }
            WorkerMessage::Error { code, message } => {
                return Err(WorkerRunError::Remote { code, message });
            }
            WorkerMessage::Heartbeat => {}
            _ => {
                return Err(WorkerRunError::Configuration(
                    "unexpected worker frame".into(),
                ));
            }
        }
    }
}

async fn writer_loop(
    mut writer: tokio::io::WriteHalf<tokio_rustls::client::TlsStream<TcpStream>>,
    mut inbound: mpsc::UnboundedReceiver<WorkerMessage>,
) {
    while let Some(message) = inbound.recv().await {
        if write_message(&mut writer, message).await.is_err() {
            break;
        }
    }
}

async fn write_message<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    payload: WorkerMessage,
) -> Result<(), WorkerRunError> {
    let frame = WireFrame::v1(payload);
    writer.write_all(&encode_frame(&frame)?).await?;
    Ok(())
}

async fn execute_job<P>(
    handler: Handler<P>,
    slots: Arc<Semaphore>,
    concurrency: u32,
    outbound: Outbound,
    job_id: i64,
    dispatch_id: String,
    execution_count: u32,
    payload: Vec<u8>,
) where
    P: DeserializeOwned + Send + 'static,
{
    let payload = match serde_json::from_slice(&payload) {
        Ok(payload) => payload,
        Err(err) => {
            let _ = report(
                &slots,
                concurrency,
                &outbound,
                job_id,
                dispatch_id,
                Err(format!("invalid job payload: {err}")),
            );
            return;
        }
    };
    let Ok(slot) = slots.clone().acquire_owned().await else {
        return;
    };
    let result = handler(Job {
        id: job_id,
        dispatch_id: dispatch_id.clone(),
        execution_count,
        payload,
    })
    .await;
    drop(slot);
    let _ = report(&slots, concurrency, &outbound, job_id, dispatch_id, result);
}

fn report(
    slots: &Semaphore,
    concurrency: u32,
    outbound: &Outbound,
    job_id: i64,
    dispatch_id: String,
    result: Result<Vec<u8>, String>,
) -> Result<(), WorkerRunError> {
    let free_slots = slots.available_permits() as u32;
    let result = match result {
        Ok(payload) => JobResult::Succeeded { payload },
        Err(message) => JobResult::Failed { message },
    };
    outbound
        .send(WorkerMessage::JobResult {
            job_id,
            dispatch_id,
            result,
            running_jobs: concurrency.saturating_sub(free_slots),
            free_slots,
        })
        .map_err(|_| WorkerRunError::Stopped)
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
