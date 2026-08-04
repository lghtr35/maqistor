use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::BufReader,
    net::SocketAddr,
    sync::Arc,
    time::Instant,
};

use anyhow::{Context, Result, bail};
use bollard::{
    API_DEFAULT_VERSION, Docker,
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
                        !worker.draining
                            && worker.queue_name == request.queue_name
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
    async fn dispatch(&self, permit: ReservedDispatch, job: Job) -> Result<(), DispatchError> {
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
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        let queued = {
            let mut workers = permit.registry.0.lock().await;
            let Some(worker) = workers.get_mut(&permit.worker_id) else {
                return Err(DispatchError::Internal(
                    "reserved worker disappeared".into(),
                ));
            };
            if worker.draining {
                worker.reserved_slots = worker.reserved_slots.saturating_sub(1);
                return Err(DispatchError::Internal("worker is draining".into()));
            }
            worker
                .outbound
                .send(OutboundFrame { frame, ack: ack_tx })
                .is_ok()
        };
        let wrote = queued && matches!(ack_rx.await, Ok(Ok(())));
        if !wrote {
            release_permit(&permit.registry, permit.worker_id).await;
            return Err(DispatchError::Internal(
                "worker dispatch write failed".into(),
            ));
        }
        Ok(())
    }
    async fn release(&self, permit: ReservedDispatch) {
        if let Ok(permit) = permit.into_permit().into_any().downcast::<RegistryPermit>() {
            release_permit(&permit.registry, permit.worker_id).await;
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
struct WorkerState {
    queue_name: String,
    running_jobs: u32,
    free_slots: u32,
    last_activity: Instant,
    reserved_slots: u32,
    draining: bool,
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

fn record_worker_capacity(state: &mut WorkerState, running_jobs: u32, free_slots: u32) {
    state.running_jobs = running_jobs;
    state.free_slots = free_slots;
    state.reserved_slots = 0;
}

fn begin_drain(state: &mut WorkerState) -> Result<()> {
    state.draining = true;
    state.free_slots = 0;
    let (ack, _ignored) = tokio::sync::oneshot::channel();
    state
        .outbound
        .send(OutboundFrame {
            frame: WireFrame::v1(WorkerMessage::Draining),
            ack,
        })
        .map_err(|_| anyhow::anyhow!("worker writer stopped during drain"))?;
    Ok(())
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
#[derive(Debug, Clone)]
pub struct ManagedQueue {
    pub name: String,
    pub image: String,
    pub replicas: u32,
    pub env: Vec<String>,
}

/// How to reach the Docker daemon used for managed worker containers.
///
/// An empty / default value uses bollard local defaults (Unix socket or Windows
/// named pipe). Set `endpoint` for an explicit socket, named pipe, or remote
/// TCP daemon. TLS paths must be supplied together for `tcp://` / `https://`
/// endpoints; `https://` always uses explicit mTLS.
#[derive(Debug, Clone, Default)]
pub struct DockerConnectOptions {
    pub endpoint: Option<String>,
    pub ca_cert_path: Option<String>,
    pub cert_path: Option<String>,
    pub key_path: Option<String>,
}

const DOCKER_CONNECT_TIMEOUT_SECS: u64 = 120;

fn connect_docker(options: &DockerConnectOptions) -> Result<Docker> {
    match options.endpoint.as_deref() {
        None => Docker::connect_with_local_defaults().context("connect to Docker"),
        Some(endpoint) if endpoint.starts_with("unix://") => {
            #[cfg(unix)]
            {
                Docker::connect_with_unix(
                    endpoint,
                    DOCKER_CONNECT_TIMEOUT_SECS,
                    API_DEFAULT_VERSION,
                )
                .context("connect to Docker via unix socket")
            }
            #[cfg(not(unix))]
            {
                let _ = endpoint;
                bail!("unix:// Docker endpoints are only supported on Unix hosts");
            }
        }
        Some(endpoint) if endpoint.starts_with("npipe://") => {
            #[cfg(windows)]
            {
                Docker::connect_with_named_pipe(
                    endpoint,
                    DOCKER_CONNECT_TIMEOUT_SECS,
                    API_DEFAULT_VERSION,
                )
                .context("connect to Docker via named pipe")
            }
            #[cfg(not(windows))]
            {
                let _ = endpoint;
                bail!("npipe:// Docker endpoints are only supported on Windows hosts");
            }
        }
        Some(endpoint)
            if endpoint.starts_with("tcp://")
                || endpoint.starts_with("http://")
                || endpoint.starts_with("https://") =>
        {
            match (
                options.ca_cert_path.as_deref(),
                options.cert_path.as_deref(),
                options.key_path.as_deref(),
            ) {
                (Some(ca), Some(cert), Some(key)) => Docker::connect_with_ssl(
                    endpoint,
                    std::path::Path::new(key),
                    std::path::Path::new(cert),
                    std::path::Path::new(ca),
                    DOCKER_CONNECT_TIMEOUT_SECS,
                    API_DEFAULT_VERSION,
                )
                .context("connect to Docker via TLS"),
                (None, None, None) => Docker::connect_with_http(
                    endpoint,
                    DOCKER_CONNECT_TIMEOUT_SECS,
                    API_DEFAULT_VERSION,
                )
                .context("connect to Docker via HTTP"),
                _ => bail!("docker TLS requires ca_cert_path, cert_path, and key_path together"),
            }
        }
        Some(endpoint) => bail!("unsupported docker endpoint scheme: {endpoint}"),
    }
}

#[derive(Clone)]
pub struct DockerWorkerSupervisor {
    docker: Docker,
    queues: Vec<ManagedQueue>,
    desired_images: Arc<Mutex<HashMap<String, String>>>,
}
impl DockerWorkerSupervisor {
    pub async fn connect(
        queues: Vec<ManagedQueue>,
        options: &DockerConnectOptions,
    ) -> Result<Self> {
        let docker = connect_docker(options)?;
        docker.ping().await.context("ping Docker daemon")?;
        Ok(Self {
            docker,
            queues,
            desired_images: Arc::new(Mutex::new(HashMap::new())),
        })
    }
    async fn reconcile(&self) -> Result<()> {
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
        let desired_env = &queue.env;
        if let Ok(container) = self.docker.inspect_container(&name, None).await {
            let mut need_update = false;
            if container.image.as_deref() != Some(desired_image.as_str()) {
                need_update = true;
            }
            let current_env = container
                .config
                .as_ref()
                .and_then(|config| config.env.as_deref())
                .unwrap_or(&[]);
            if !desired_env
                .iter()
                .all(|wanted| current_env.iter().any(|have| have == wanted))
            {
                need_update = true;
            }
            if need_update {
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
            env: Some(queue.env.clone()),
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
                draining: false,
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
            let event = {
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
                        record_worker_capacity(state, running_jobs, free_slots);
                        let outcome = match result {
                            maqistor_worker_protocol::JobResult::Succeeded { payload } => {
                                JobOutcome::Succeeded(payload)
                            }
                            maqistor_worker_protocol::JobResult::Failed { message } => {
                                JobOutcome::Failed(message)
                            }
                        };
                        Some(WorkerEvent::Result {
                            queue_name: state.queue_name.clone(),
                            result: WorkerResult {
                                job_id,
                                dispatch_id,
                                outcome,
                            },
                        })
                    }
                    WorkerMessage::Drain => {
                        begin_drain(state)?;
                        None
                    }
                    WorkerMessage::Heartbeat => None,
                    _ => anyhow::bail!("invalid post-registration worker frame"),
                }
            };
            if let Some(event) = event {
                let _ = registry.1.send(event);
            }
        }
    }
    .await;
    registry.0.lock().await.remove(&instance_id);
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worker_result_replaces_reservation_estimate_with_capacity_snapshot() {
        let (outbound, _rx) = mpsc::unbounded_channel();
        let mut worker = WorkerState {
            queue_name: "email".into(),
            running_jobs: 3,
            free_slots: 0,
            last_activity: Instant::now(),
            reserved_slots: 3,
            draining: false,
            outbound,
        };

        record_worker_capacity(&mut worker, 2, 1);

        assert_eq!(worker.reserved_slots, 0);
        assert_eq!(worker.running_jobs, 2);
        assert_eq!(worker.free_slots, 1);
    }

    #[tokio::test]
    async fn draining_workers_are_not_reserved() {
        let (outbound, _inbound) = mpsc::unbounded_channel();
        let worker_id = Uuid::new_v4();
        let registry = WorkerRegistry::default();
        registry.0.lock().await.insert(
            worker_id,
            WorkerState {
                queue_name: "email".into(),
                running_jobs: 0,
                free_slots: 1,
                last_activity: Instant::now(),
                reserved_slots: 0,
                draining: true,
                outbound,
            },
        );

        let reserved = RegistryDispatcher::new(registry)
            .reserve(vec![QueueReservation {
                queue_name: "email".into(),
                count: 1,
            }])
            .await
            .expect("reserve succeeds");

        assert!(reserved.is_empty());
    }

    #[tokio::test]
    async fn drain_ack_follows_a_dispatch_queued_before_drain() {
        let (outbound, mut inbound) = mpsc::unbounded_channel();
        let worker_id = Uuid::new_v4();
        let registry = WorkerRegistry::default();
        registry.0.lock().await.insert(
            worker_id,
            WorkerState {
                queue_name: "email".into(),
                running_jobs: 0,
                free_slots: 1,
                last_activity: Instant::now(),
                reserved_slots: 1,
                draining: false,
                outbound,
            },
        );
        let dispatcher = RegistryDispatcher::new(registry.clone());
        let mut accepted = maqistor_engine::AcceptedJob::new("email", b"{}".to_vec());
        accepted.id = 9;
        accepted.dispatch_id = Some("dispatch-9".into());
        let job = Job::from_accepted(accepted, None);
        let permit = ReservedDispatch::new(
            "email".into(),
            Box::new(RegistryPermit {
                worker_id,
                registry: registry.clone(),
            }),
        );

        let dispatch = tokio::spawn(async move { dispatcher.dispatch(permit, job).await });
        let dispatch_frame = inbound.recv().await.expect("queued dispatch");
        assert!(matches!(
            dispatch_frame.frame.payload,
            WorkerMessage::JobDispatch { job_id: 9, .. }
        ));

        begin_drain(
            registry
                .0
                .lock()
                .await
                .get_mut(&worker_id)
                .expect("worker exists"),
        )
        .expect("queue drain acknowledgement");
        let drain_frame = inbound.recv().await.expect("queued drain acknowledgement");
        assert!(matches!(drain_frame.frame.payload, WorkerMessage::Draining));

        dispatch_frame.ack.send(Ok(())).expect("ack dispatch write");
        dispatch
            .await
            .expect("dispatch task")
            .expect("dispatch result");
    }
}
