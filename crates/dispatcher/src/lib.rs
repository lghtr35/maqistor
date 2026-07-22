use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::BufReader,
    net::SocketAddr,
    sync::Arc,
    time::Instant,
};

use anyhow::{Context, Result};
use bollard::{
    Docker,
    container::{Config, CreateContainerOptions, RemoveContainerOptions, StartContainerOptions},
    image::CreateImageOptions,
    models::{HostConfig, RestartPolicy, RestartPolicyNameEnum},
};
use futures_util::StreamExt;
use maqistor_engine::{
    DispatchError, DispatchPermit, Job, JobOutcome, QueueReservation, ReservedDispatch,
    WorkerDispatcher, WorkerEvent, WorkerResult,
};
use maqistor_worker_protocol::{
    MAX_FRAME_BYTES, WireFrame, WorkerMessage, decode_frame, encode_frame,
};
use rustls::{
    RootCertStore, ServerConfig,
    pki_types::{CertificateDer, PrivateKeyDer},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpListener,
    sync::{Mutex, mpsc},
    time::{Duration, timeout},
};
use tokio_rustls::{TlsAcceptor, server::TlsStream};
use tracing::{info, warn};
use uuid::Uuid;

#[derive(Clone)]
pub struct RegistryDispatcher {
    registry: WorkerRegistry,
}
impl RegistryDispatcher {
    pub fn new(registry: WorkerRegistry) -> Self {
        Self { registry }
    }
}
impl WorkerDispatcher for RegistryDispatcher {
    fn reserve(
        &self,
        queues: Vec<QueueReservation>,
    ) -> impl std::future::Future<Output = Result<Vec<ReservedDispatch>, DispatchError>> + Send
    {
        let registry = self.registry.clone();
        async move {
            let mut workers = registry.0.lock().await;
            let mut reserved = Vec::new();
            for request in queues {
                for _ in 0..request.count {
                    let Some((worker_id, state)) = workers.iter_mut().find(|(_, worker)| {
                        worker.queue_name == request.queue_name
                            && worker.free_slots.saturating_sub(worker.reserved_slots) > 0
                    }) else {
                        break;
                    };
                    state.reserved_slots += 1;
                    reserved.push(ReservedDispatch::new(
                        request.queue_name.clone(),
                        Box::new(RegistryPermit {
                            worker_id: *worker_id,
                            registry: registry.clone(),
                        }),
                    ));
                }
            }
            Ok(reserved)
        }
    }
    fn dispatch(
        &self,
        permit: ReservedDispatch,
        job: Job,
    ) -> impl std::future::Future<Output = Result<(), DispatchError>> + Send {
        async move {
            let permit = permit
                .into_permit()
                .into_any()
                .downcast::<RegistryPermit>()
                .map_err(|_| DispatchError::Internal("foreign dispatch permit".into()))?;
            let dispatch_id = job
                .dispatch_id
                .clone()
                .ok_or_else(|| DispatchError::Internal("claimed job has no dispatch id".into()))?;
            let frame = WireFrame::v1(WorkerMessage::JobDispatch {
                job_id: job.id,
                dispatch_id,
                execution_count: job.execution_count,
                payload: job.payload,
            });
            let outbound = {
                let workers = permit.registry.0.lock().await;
                workers
                    .get(&permit.worker_id)
                    .map(|worker| worker.outbound.clone())
            }
            .ok_or_else(|| DispatchError::Internal("reserved worker disappeared".into()))?;
            let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
            let queued = outbound.send(OutboundFrame { frame, ack: ack_tx }).is_ok();
            let wrote = queued && matches!(ack_rx.await, Ok(Ok(())));
            if !wrote {
                release_permit(&permit.registry, permit.worker_id).await;
                return Err(DispatchError::Internal(
                    "worker dispatch write failed".into(),
                ));
            }
            Ok(())
        }
    }
    fn release(&self, permit: ReservedDispatch) -> impl std::future::Future<Output = ()> + Send {
        async move {
            if let Ok(permit) = permit.into_permit().into_any().downcast::<RegistryPermit>() {
                release_permit(&permit.registry, permit.worker_id).await;
            }
        }
    }
    fn subscribe_events(&self) -> Option<tokio::sync::broadcast::Receiver<WorkerEvent>> {
        Some(self.registry.1.subscribe())
    }
}

#[derive(Debug, Clone)]
pub struct TlsFiles {
    pub ca_cert_path: String,
    pub cert_path: String,
    pub key_path: String,
}
#[derive(Debug, Clone)]
pub struct WorkerState {
    pub queue_name: String,
    pub running_jobs: u32,
    pub free_slots: u32,
    pub last_activity: Instant,
    reserved_slots: u32,
    outbound: mpsc::UnboundedSender<OutboundFrame>,
}

struct OutboundFrame {
    frame: WireFrame,
    ack: tokio::sync::oneshot::Sender<Result<()>>,
}

struct RegistryPermit {
    worker_id: Uuid,
    registry: WorkerRegistry,
}
impl DispatchPermit for RegistryPermit {
    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any + Send> {
        self
    }
}

async fn release_permit(registry: &WorkerRegistry, worker_id: Uuid) {
    if let Some(worker) = registry.0.lock().await.get_mut(&worker_id) {
        worker.reserved_slots = worker.reserved_slots.saturating_sub(1);
    }
}
#[derive(Clone)]
pub struct WorkerRegistry(
    Arc<Mutex<HashMap<Uuid, WorkerState>>>,
    tokio::sync::broadcast::Sender<WorkerEvent>,
);
impl Default for WorkerRegistry {
    fn default() -> Self {
        let (events, _) = tokio::sync::broadcast::channel(65_536);
        Self(Arc::new(Mutex::new(HashMap::new())), events)
    }
}
impl WorkerRegistry {
    pub async fn snapshot(&self) -> HashMap<Uuid, WorkerState> {
        self.0.lock().await.clone()
    }
    pub async fn has_capacity(&self, queue_name: &str) -> bool {
        self.0.lock().await.values().any(|worker| {
            worker.queue_name == queue_name
                && worker.free_slots.saturating_sub(worker.reserved_slots) > 0
        })
    }
}

#[derive(Debug, Clone)]
pub struct ManagedQueue {
    pub name: String,
    pub image: String,
    pub replicas: u32,
}

#[derive(Clone)]
pub struct DockerWorkerSupervisor {
    docker: Docker,
    queues: Vec<ManagedQueue>,
    desired_images: Arc<Mutex<HashMap<String, String>>>,
}
impl DockerWorkerSupervisor {
    pub fn connect(queues: Vec<ManagedQueue>) -> Result<Self> {
        Ok(Self {
            docker: Docker::connect_with_local_defaults().context("connect to Docker")?,
            queues,
            desired_images: Arc::new(Mutex::new(HashMap::new())),
        })
    }
    pub async fn reconcile(&self) -> Result<()> {
        for queue in &self.queues {
            for ordinal in 0..queue.replicas {
                self.ensure(queue, ordinal).await?;
            }
        }
        Ok(())
    }
    pub fn spawn(self) {
        tokio::spawn(async move {
            loop {
                if let Err(error) = self.reconcile().await {
                    tracing::warn!(%error, "managed worker reconciliation failed");
                }
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        });
    }
    async fn ensure(&self, queue: &ManagedQueue, ordinal: u32) -> Result<()> {
        let name = container_name(&queue.name, ordinal);
        let desired_image = self.resolve_image_id(&queue.image).await?;
        if let Ok(container) = self.docker.inspect_container(&name, None).await {
            if container.image.as_deref() != Some(desired_image.as_str()) {
                let managed = container
                    .config
                    .as_ref()
                    .and_then(|config| config.labels.as_ref())
                    .is_some_and(|labels| {
                        labels
                            .get("io.maqistor.managed")
                            .is_some_and(|value| value == "true")
                    });
                anyhow::ensure!(managed, "refusing to replace non-Maqistor container {name}");
                self.docker
                    .remove_container(
                        &name,
                        Some(RemoveContainerOptions {
                            force: true,
                            ..Default::default()
                        }),
                    )
                    .await
                    .context("remove outdated managed worker")?;
            } else {
                let _ = self
                    .docker
                    .start_container(&name, None::<StartContainerOptions<String>>)
                    .await;
                return Ok(());
            }
        }
        let labels = HashMap::from([
            ("io.maqistor.managed".to_string(), "true".to_string()),
            ("io.maqistor.queue".to_string(), queue.name.clone()),
            ("io.maqistor.replica".to_string(), ordinal.to_string()),
        ]);
        let config = Config::<String> {
            image: Some(queue.image.clone()),
            labels: Some(labels),
            host_config: Some(HostConfig {
                restart_policy: Some(RestartPolicy {
                    name: Some(RestartPolicyNameEnum::UNLESS_STOPPED),
                    maximum_retry_count: None,
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        self.docker
            .create_container(
                Some(CreateContainerOptions {
                    name: name.clone(),
                    platform: None,
                }),
                config,
            )
            .await
            .context("create managed worker")?;
        self.docker
            .start_container(&name, None::<StartContainerOptions<String>>)
            .await
            .context("start managed worker")?;
        Ok(())
    }

    async fn resolve_image_id(&self, image: &str) -> Result<String> {
        if let Some(id) = self.desired_images.lock().await.get(image).cloned() {
            return Ok(id);
        }
        let local = self.docker.inspect_image(image).await;
        let inspected = match local {
            Ok(image) => image,
            Err(_) => {
                let mut pull = self.docker.create_image(
                    Some(CreateImageOptions {
                        from_image: image,
                        ..Default::default()
                    }),
                    None,
                    None,
                );
                while let Some(event) = pull.next().await {
                    event.context("pull managed worker image")?;
                }
                self.docker
                    .inspect_image(image)
                    .await
                    .context("inspect managed worker image after pull")?
            }
        };
        let id = inspected
            .id
            .context("Docker returned an image without an ID")?;
        self.desired_images
            .lock()
            .await
            .insert(image.to_owned(), id.clone());
        Ok(id)
    }
}
fn container_name(queue: &str, ordinal: u32) -> String {
    format!(
        "maqistor-{}-{ordinal}",
        queue
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
            .collect::<String>()
    )
}

pub async fn start_worker_listener(
    addr: SocketAddr,
    tls: TlsFiles,
    allowed_queues: HashSet<String>,
) -> Result<WorkerRegistry> {
    let listener = TcpListener::bind(addr)
        .await
        .context("bind worker listener")?;
    let acceptor = TlsAcceptor::from(Arc::new(server_config(&tls)?));
    let registry = WorkerRegistry::default();
    tokio::spawn({
        let registry = registry.clone();
        async move {
            loop {
                let (stream, peer_addr) = match listener.accept().await {
                    Ok(connection) => connection,
                    Err(error) => {
                        warn!(%error, "worker listener failed to accept connection");
                        continue;
                    }
                };
                let acceptor = acceptor.clone();
                let registry = registry.clone();
                let queues = allowed_queues.clone();
                tokio::spawn(async move {
                    match acceptor.accept(stream).await {
                        Ok(stream) => {
                            if let Err(error) =
                                handle_worker(stream, registry, queues, peer_addr).await
                            {
                                warn!(%peer_addr, %error, "worker registration or connection failed");
                            }
                        }
                        Err(error) => {
                            warn!(%peer_addr, %error, "worker TLS handshake failed");
                        }
                    }
                });
            }
        }
    });
    Ok(registry)
}

fn server_config(files: &TlsFiles) -> Result<ServerConfig> {
    let server_certs = certs(&files.cert_path)?;
    let key = key(&files.key_path)?;
    let mut roots = RootCertStore::empty();
    for cert in certs(&files.ca_cert_path)? {
        roots.add(cert)?;
    }
    let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots)).build()?;
    Ok(ServerConfig::builder()
        .with_client_cert_verifier(verifier)
        .with_single_cert(server_certs, key)?)
}
fn certs(path: &str) -> Result<Vec<CertificateDer<'static>>> {
    let mut reader =
        BufReader::new(File::open(path).with_context(|| format!("open certificate {path}"))?);
    rustls_pemfile::certs(&mut reader)
        .collect::<std::result::Result<Vec<_>, _>>()
        .context("read PEM certificates")
}
fn key(path: &str) -> Result<PrivateKeyDer<'static>> {
    let mut reader = BufReader::new(File::open(path).with_context(|| format!("open key {path}"))?);
    rustls_pemfile::private_key(&mut reader)?.context("no private key in PEM")
}

async fn read_frame<R: AsyncRead + Unpin>(stream: &mut R) -> Result<WireFrame> {
    let mut len = [0; 4];
    stream.read_exact(&mut len).await?;
    let size = u32::from_be_bytes(len) as usize;
    anyhow::ensure!(size <= MAX_FRAME_BYTES, "oversized worker frame");
    let mut bytes = len.to_vec();
    bytes.resize(4 + size, 0);
    stream.read_exact(&mut bytes[4..]).await?;
    Ok(decode_frame(&bytes)?)
}
async fn write_frame<W: AsyncWrite + Unpin>(stream: &mut W, frame: &WireFrame) -> Result<()> {
    stream.write_all(&encode_frame(frame)?).await?;
    Ok(())
}

async fn handle_worker(
    mut stream: TlsStream<tokio::net::TcpStream>,
    registry: WorkerRegistry,
    allowed: HashSet<String>,
    peer_addr: SocketAddr,
) -> Result<()> {
    let register = timeout(Duration::from_secs(15), read_frame(&mut stream)).await??;
    let WorkerMessage::Register {
        instance_id,
        queue_name,
        running_jobs,
        free_slots,
    } = register.payload
    else {
        warn!(%peer_addr, "worker registration rejected: first frame was not Register");
        anyhow::bail!("first worker frame must register");
    };
    if queue_name.is_empty() || !allowed.contains(&queue_name) {
        warn!(%peer_addr, %instance_id, queue = %queue_name, "worker registration rejected: unknown queue");
        write_frame(
            &mut stream,
            &WireFrame::v1(WorkerMessage::Error {
                code: "unknown_queue".into(),
                message: "registration contains an unknown queue".into(),
            }),
        )
        .await?;
        anyhow::bail!("unknown queue");
    }
    let (mut reader, mut writer) = tokio::io::split(stream);
    let (outbound, mut outbound_rx) = mpsc::unbounded_channel::<OutboundFrame>();
    tokio::spawn(async move {
        while let Some(outbound) = outbound_rx.recv().await {
            let result = write_frame(&mut writer, &outbound.frame).await;
            let failed = result.is_err();
            let _ = outbound.ack.send(result);
            if failed {
                break;
            }
        }
    });
    {
        let mut workers = registry.0.lock().await;
        if workers.contains_key(&instance_id) {
            warn!(%peer_addr, %instance_id, queue = %queue_name, "worker registration rejected: duplicate instance ID");
        }
        anyhow::ensure!(
            !workers.contains_key(&instance_id),
            "duplicate worker instance"
        );
        workers.insert(
            instance_id,
            WorkerState {
                queue_name: queue_name.clone(),
                running_jobs,
                free_slots,
                last_activity: Instant::now(),
                reserved_slots: 0,
                outbound: outbound.clone(),
            },
        );
    }
    let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
    outbound
        .send(OutboundFrame {
            frame: WireFrame::v1(WorkerMessage::Registered {
                queue_name: queue_name.clone(),
            }),
            ack: ack_tx,
        })
        .map_err(|_| anyhow::anyhow!("worker writer stopped during registration"))?;
    ack_rx.await??;
    info!(
        %peer_addr,
        %instance_id,
        queue = %queue_name,
        running_jobs,
        free_slots,
        "worker registered"
    );
    let _ = registry.1.send(WorkerEvent::Registered {
        queue_name: queue_name.clone(),
    });
    let result: Result<()> = async {
        loop {
            let frame = timeout(Duration::from_secs(15), read_frame(&mut reader)).await??;
            let mut workers = registry.0.lock().await;
            let state = workers
                .get_mut(&instance_id)
                .context("worker disappeared")?;
            state.last_activity = Instant::now();
            match frame.payload {
                WorkerMessage::JobResult {
                    job_id,
                    dispatch_id,
                    result,
                    running_jobs,
                    free_slots,
                } => {
                    state.running_jobs = running_jobs;
                    state.free_slots = free_slots;
                    state.reserved_slots = 0;
                    let outcome = match result {
                        maqistor_worker_protocol::JobResult::Succeeded { payload } => {
                            JobOutcome::Succeeded(payload)
                        }
                        maqistor_worker_protocol::JobResult::Failed { message } => {
                            JobOutcome::Failed(message)
                        }
                    };
                    let _ = registry.1.send(WorkerEvent::Result {
                        queue_name: state.queue_name.clone(),
                        result: WorkerResult {
                            job_id,
                            dispatch_id,
                            outcome,
                        },
                    });
                }
                WorkerMessage::Heartbeat => {}
                _ => anyhow::bail!("invalid post-registration worker frame"),
            }
        }
    }
    .await;
    registry.0.lock().await.remove(&instance_id);
    result
}
