use std::{fs::File, future::Future, io::BufReader, num::NonZeroU32, pin::Pin, sync::Arc};

use maqistor_worker_protocol::{
    JobResult, ProtocolError, WireFrame, WorkerMessage, decode_frame, encode_frame,
};
use rustls::{
    ClientConfig, RootCertStore,
    pki_types::{CertificateDer, ServerName},
};
use serde::de::DeserializeOwned;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpStream,
    sync::{Semaphore, mpsc, oneshot, watch},
    task::JoinSet,
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
    dyn Fn(Job<P>) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, String>> + Send>> + Send + Sync,
>;

type Outbound = mpsc::UnboundedSender<OutboundFrame>;

struct OutboundFrame {
    payload: WorkerMessage,
    written: Option<oneshot::Sender<()>>,
}

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
        self.run_until(std::future::pending()).await
    }

    /// Run this worker until its session fails or the caller requests a graceful drain.
    pub async fn run_until<S>(self, shutdown: S) -> Result<(), WorkerRunError>
    where
        S: Future<Output = ()> + Send,
    {
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
        let (writer_alive_tx, writer_alive) = watch::channel(true);
        let writer_task = tokio::spawn(async move {
            let result = writer_loop(writer, inbound).await;
            let _ = writer_alive_tx.send(false);
            result
        });

        let heartbeats = {
            let outbound = outbound.clone();
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
                loop {
                    interval.tick().await;
                    if !send_unacknowledged(&outbound, WorkerMessage::Heartbeat) {
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
            outbound.clone(),
            shutdown,
            writer_alive,
        )
        .await;

        heartbeats.abort();
        let _ = heartbeats.await;
        drop(outbound);
        match result {
            Ok(()) => writer_task.await.map_err(|_| WorkerRunError::Stopped)?,
            Err(error) => {
                writer_task.abort();
                let _ = writer_task.await;
                Err(error)
            }
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SessionState {
    Serving,
    AwaitingDrain,
    Draining,
}

async fn serve<P, R, S>(
    reader: &mut R,
    queue_name: &'static str,
    concurrency: u32,
    handler: Handler<P>,
    slots: Arc<Semaphore>,
    outbound: Outbound,
    shutdown: S,
    mut writer_alive: watch::Receiver<bool>,
) -> Result<(), WorkerRunError>
where
    P: DeserializeOwned + Send + 'static,
    R: AsyncRead + Unpin,
    S: Future<Output = ()> + Send,
{
    let mut jobs = JoinSet::new();
    let mut state = SessionState::Serving;
    tokio::pin!(shutdown);
    loop {
        if state == SessionState::Draining && jobs.is_empty() {
            return Ok(());
        }
        tokio::select! {
            biased;
            _ = &mut shutdown, if state == SessionState::Serving => {
                send_and_wait(&outbound, WorkerMessage::Drain).await?;
                state = SessionState::AwaitingDrain;
            }
            _ = writer_alive.changed() => return Err(WorkerRunError::Stopped),
            completed = jobs.join_next(), if !jobs.is_empty() => {
                match completed {
                    Some(Ok(Ok(()))) => {}
                    Some(Ok(Err(error))) => return Err(error),
                    Some(Err(_)) => return Err(WorkerRunError::Stopped),
                    None => {}
                }
            }
            message = read_async_frame(reader) => match message?.payload {
                WorkerMessage::Registered { queue_name: registered } if registered == queue_name => {}
                WorkerMessage::JobDispatch {
                    job_id,
                    dispatch_id,
                    execution_count,
                    payload,
                } if state != SessionState::Draining => {
                    jobs.spawn(execute_job(
                        handler.clone(),
                        slots.clone(),
                        concurrency,
                        outbound.clone(),
                        job_id,
                        dispatch_id,
                        execution_count,
                        payload,
                    ));
                }
                WorkerMessage::Draining if state == SessionState::AwaitingDrain => {
                    state = SessionState::Draining;
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
}

async fn writer_loop<W: AsyncWrite + Unpin>(
    mut writer: W,
    mut inbound: mpsc::UnboundedReceiver<OutboundFrame>,
) -> Result<(), WorkerRunError> {
    while let Some(message) = inbound.recv().await {
        write_message(&mut writer, message.payload).await?;
        if let Some(written) = message.written {
            let _ = written.send(());
        }
    }
    Ok(())
}

fn send_unacknowledged(outbound: &Outbound, payload: WorkerMessage) -> bool {
    outbound
        .send(OutboundFrame {
            payload,
            written: None,
        })
        .is_ok()
}

async fn send_and_wait(outbound: &Outbound, payload: WorkerMessage) -> Result<(), WorkerRunError> {
    let (written, received) = oneshot::channel();
    outbound
        .send(OutboundFrame {
            payload,
            written: Some(written),
        })
        .map_err(|_| WorkerRunError::Stopped)?;
    received.await.map_err(|_| WorkerRunError::Stopped)
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
) -> Result<(), WorkerRunError>
where
    P: DeserializeOwned + Send + 'static,
{
    let payload = match serde_json::from_slice(&payload) {
        Ok(payload) => payload,
        Err(err) => {
            return report(
                &slots,
                concurrency,
                &outbound,
                job_id,
                dispatch_id,
                Err(format!("invalid job payload: {err}")),
            )
            .await;
        }
    };
    let slot = slots
        .clone()
        .acquire_owned()
        .await
        .map_err(|_| WorkerRunError::Stopped)?;
    let result = handler(Job {
        id: job_id,
        dispatch_id: dispatch_id.clone(),
        execution_count,
        payload,
    })
    .await;
    drop(slot);
    report(&slots, concurrency, &outbound, job_id, dispatch_id, result).await
}

async fn report(
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
    send_and_wait(
        outbound,
        WorkerMessage::JobResult {
            job_id,
            dispatch_id,
            result,
            running_jobs: concurrency.saturating_sub(free_slots),
            free_slots,
        },
    )
    .await
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::{sync::Mutex, time::Duration};

    use tokio::{
        io::{duplex, split},
        sync::Notify,
        time::timeout,
    };

    fn handler() -> Handler<serde_json::Value> {
        Arc::new(|_| Box::pin(async { Ok(vec![1]) }))
    }

    #[tokio::test]
    async fn drain_finishes_pre_ack_jobs_before_returning() {
        let (client, mut server) = duplex(4_096);
        let (mut reader, writer) = split(client);
        let (outbound, inbound) = mpsc::unbounded_channel();
        let writer_task = tokio::spawn(writer_loop(writer, inbound));
        let (_writer_alive_tx, writer_alive) = watch::channel(true);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let serve_task = tokio::spawn({
            let outbound = outbound.clone();
            async move {
                serve(
                    &mut reader,
                    "email",
                    1,
                    handler(),
                    Arc::new(Semaphore::new(1)),
                    outbound,
                    async move {
                        let _ = shutdown_rx.await;
                    },
                    writer_alive,
                )
                .await
            }
        });

        shutdown_tx.send(()).expect("request drain");
        assert!(matches!(
            read_async_frame(&mut server).await.unwrap().payload,
            WorkerMessage::Drain
        ));

        write_message(
            &mut server,
            WorkerMessage::JobDispatch {
                job_id: 7,
                dispatch_id: "dispatch-7".into(),
                execution_count: 1,
                payload: b"null".to_vec(),
            },
        )
        .await
        .unwrap();
        write_message(&mut server, WorkerMessage::Draining)
            .await
            .unwrap();

        assert!(matches!(
            read_async_frame(&mut server).await.unwrap().payload,
            WorkerMessage::JobResult { job_id: 7, .. }
        ));
        assert!(
            timeout(Duration::from_secs(1), serve_task)
                .await
                .expect("drain finishes")
                .expect("serve task")
                .is_ok()
        );

        drop(outbound);
        writer_task
            .await
            .expect("writer task")
            .expect("writer result");
    }

    struct NotifyOnDrop(Option<oneshot::Sender<()>>);

    impl Drop for NotifyOnDrop {
        fn drop(&mut self) {
            if let Some(done) = self.0.take() {
                let _ = done.send(());
            }
        }
    }

    #[tokio::test]
    async fn connection_failure_aborts_in_flight_handlers() {
        let (client, mut server) = duplex(4_096);
        let (mut reader, writer) = split(client);
        let (outbound, inbound) = mpsc::unbounded_channel();
        let writer_task = tokio::spawn(writer_loop(writer, inbound));
        let (_writer_alive_tx, writer_alive) = watch::channel(true);
        let (dropped_tx, dropped_rx) = oneshot::channel();
        let dropped_tx = Arc::new(Mutex::new(Some(dropped_tx)));
        let started = Arc::new(Notify::new());
        let handler: Handler<serde_json::Value> = {
            let dropped_tx = dropped_tx.clone();
            let started = started.clone();
            Arc::new(move |_| {
                let dropped_tx = dropped_tx.clone();
                let started = started.clone();
                Box::pin(async move {
                    let _guard = NotifyOnDrop(dropped_tx.lock().expect("drop lock").take());
                    started.notify_one();
                    std::future::pending::<()>().await;
                    Ok(Vec::new())
                })
            })
        };
        let started_wait = started.notified();

        let serve_task = tokio::spawn({
            let outbound = outbound.clone();
            async move {
                serve(
                    &mut reader,
                    "email",
                    1,
                    handler,
                    Arc::new(Semaphore::new(1)),
                    outbound,
                    std::future::pending(),
                    writer_alive,
                )
                .await
            }
        });

        write_message(
            &mut server,
            WorkerMessage::JobDispatch {
                job_id: 8,
                dispatch_id: "dispatch-8".into(),
                execution_count: 1,
                payload: b"null".to_vec(),
            },
        )
        .await
        .unwrap();
        timeout(Duration::from_secs(1), started_wait)
            .await
            .expect("handler started");
        drop(server);

        assert!(
            timeout(Duration::from_secs(1), serve_task)
                .await
                .expect("serve returns")
                .expect("serve task")
                .is_err()
        );
        timeout(Duration::from_secs(1), dropped_rx)
            .await
            .expect("handler aborted")
            .expect("drop notification");

        drop(outbound);
        writer_task.abort();
        let _ = writer_task.await;
    }
}
