mod adaptive;
mod types;

use futures_util::StreamExt;
use std::{
    collections::{HashMap, HashSet},
    future::Future,
    sync::{Arc, Mutex},
};
use tokio::sync::mpsc;

pub use adaptive::{AdaptiveBatch, DirectionStreak, Ewma};
pub use types::{Job, JobQueue, JobStatus, StoreError, unix_now};

/// The largest single durable claim. This bounds a scheduler pass even when a
/// caller supplies an excessively large dispatch batch configuration.
pub const MAX_CLAIM_BATCH_SIZE: usize = 16_384;

/// A worker outcome fenced by the dispatch lease that produced it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobOutcome {
    Succeeded(Vec<u8>),
    Failed(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerResult {
    pub job_id: i64,
    pub dispatch_id: String,
    pub outcome: JobOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkerEvent {
    Registered {
        queue_name: String,
    },
    Result {
        queue_name: String,
        result: WorkerResult,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error("unknown job queue: {0}")]
    UnknownQueue(String),
    #[error("job not found: {0}")]
    JobNotFound(i64),
    #[error("engine storage is unavailable")]
    Storage {
        #[source]
        source: StoreError,
    },
    #[error("failed to serialize payload: {0}")]
    Payload(String),
}

impl From<StoreError> for EngineError {
    fn from(error: StoreError) -> Self {
        match error {
            StoreError::QueueNotFound(name) => Self::UnknownQueue(name),
            StoreError::NotFound(id) => Self::JobNotFound(id),
            source @ StoreError::Internal(_) => Self::Storage { source },
        }
    }
}

pub trait DurableStore: Send + Sync {
    fn upsert_queue(
        &self,
        queue: JobQueue,
    ) -> impl Future<Output = Result<JobQueue, StoreError>> + Send;
    fn get_queue(
        &self,
        name: &str,
    ) -> impl Future<Output = Result<Option<JobQueue>, StoreError>> + Send;
    fn list_queues(&self) -> impl Future<Output = Result<Vec<JobQueue>, StoreError>> + Send;
    fn enqueue(&self, job: Job) -> impl Future<Output = Result<Job, StoreError>> + Send;
    fn get_job(&self, job_id: i64) -> impl Future<Output = Result<Job, StoreError>> + Send;
    fn status(&self, job_id: i64) -> impl Future<Output = Result<JobStatus, StoreError>> + Send;
    fn claim_next(
        &self,
        queue_name: &str,
        lease_duration_secs: i64,
    ) -> impl Future<Output = Result<Option<Job>, StoreError>> + Send;
    fn claim_batch(
        &self,
        queue_name: &str,
        lease_duration_secs: i64,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<Job>, StoreError>> + Send {
        async move {
            let mut jobs = Vec::with_capacity(limit.min(MAX_CLAIM_BATCH_SIZE));
            for _ in 0..limit.min(MAX_CLAIM_BATCH_SIZE) {
                let Some(job) = self.claim_next(queue_name, lease_duration_secs).await? else {
                    break;
                };
                jobs.push(job);
            }
            Ok(jobs)
        }
    }
    fn complete(
        &self,
        _job_id: i64,
        _dispatch_id: &str,
        _outcome: JobOutcome,
    ) -> impl Future<Output = Result<Option<Job>, StoreError>> + Send {
        async {
            Err(StoreError::Internal(
                "store does not support job completion".into(),
            ))
        }
    }
    fn release_claim(
        &self,
        _job_id: i64,
        _dispatch_id: &str,
    ) -> impl Future<Output = Result<bool, StoreError>> + Send {
        async {
            Err(StoreError::Internal(
                "store does not support claim release".into(),
            ))
        }
    }
    fn recover_stale_leases(
        &self,
        now: i64,
    ) -> impl Future<Output = Result<Vec<Job>, StoreError>> + Send;
}

#[derive(Debug, thiserror::Error)]
pub enum DispatchError {
    #[error("no worker capacity is currently available")]
    NoCapacity,
    #[error("dispatcher error: {0}")]
    Internal(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueReservation {
    pub queue_name: String,
    pub count: usize,
}

/// Opaque worker-capacity reservation. The engine can carry it but never
/// learns which worker or slot owns it.
pub trait DispatchPermit: Send {
    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any + Send>;
}

pub struct ReservedDispatch {
    pub queue_name: String,
    permit: Box<dyn DispatchPermit>,
}

impl ReservedDispatch {
    pub fn new(queue_name: String, permit: Box<dyn DispatchPermit>) -> Self {
        Self { queue_name, permit }
    }
    pub fn into_permit(self) -> Box<dyn DispatchPermit> {
        self.permit
    }
}

pub trait WorkerDispatcher: Send + Sync {
    fn reserve(
        &self,
        _queues: Vec<QueueReservation>,
    ) -> impl Future<Output = Result<Vec<ReservedDispatch>, DispatchError>> + Send {
        async { Ok(Vec::new()) }
    }
    fn dispatch(
        &self,
        permit: ReservedDispatch,
        job: Job,
    ) -> impl Future<Output = Result<(), DispatchError>> + Send;
    fn release(&self, _permit: ReservedDispatch) -> impl Future<Output = ()> + Send {
        async {}
    }
    fn subscribe_events(&self) -> Option<tokio::sync::broadcast::Receiver<WorkerEvent>> {
        None
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubmitJob {
    pub name: String,
    pub payload: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobView {
    pub id: i64,
    pub name: String,
    pub status: JobStatus,
}

#[derive(Debug, Clone)]
pub struct DispatchOptions {
    pub batch_size_max: usize,
    pub max_in_flight: usize,
}

impl Default for DispatchOptions {
    fn default() -> Self {
        Self {
            batch_size_max: 8_192,
            max_in_flight: 1_024,
        }
    }
}

impl DispatchOptions {
    pub fn validate(&self) -> Result<(), String> {
        if self.batch_size_max == 0 || self.batch_size_max > MAX_CLAIM_BATCH_SIZE {
            return Err(format!(
                "dispatch.batch_size_max must be in 1..={MAX_CLAIM_BATCH_SIZE}"
            ));
        }
        if self.max_in_flight == 0 {
            return Err("dispatch.max_in_flight must be greater than zero".into());
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct Engine<S: DurableStore, D: WorkerDispatcher> {
    store: S,
    dispatcher: D,
    scheduler: Arc<Scheduler>,
}

struct Scheduler {
    tx: mpsc::UnboundedSender<String>,
    awake: Mutex<HashMap<String, bool>>,
    options: DispatchOptions,
}

impl<
    S: DurableStore + Clone + Send + Sync + 'static,
    D: WorkerDispatcher + Clone + Send + Sync + 'static,
> Engine<S, D>
{
    pub fn with_dispatcher(store: S, dispatcher: D, options: DispatchOptions) -> Self {
        options.validate().expect("invalid dispatch options");
        let (tx, rx) = mpsc::unbounded_channel();
        let engine = Self {
            store,
            dispatcher,
            scheduler: Arc::new(Scheduler {
                tx,
                awake: Mutex::new(HashMap::new()),
                options,
            }),
        };
        engine.start_scheduler(rx);
        engine
    }

    pub async fn submit(&self, job: SubmitJob) -> Result<JobView, EngineError> {
        let payload = serde_json::to_vec(&job.payload)
            .map_err(|err| EngineError::Payload(err.to_string()))?;
        let result = self
            .store
            .enqueue(Job::new_pending(job.name, payload))
            .await?;
        self.ensure_awake(result.name.clone()).await;
        Ok(JobView {
            id: result.id,
            name: result.name,
            status: result.status,
        })
    }

    pub async fn get_job(&self, id: i64) -> Result<JobView, EngineError> {
        let job = self.store.get_job(id).await?;
        Ok(JobView {
            id: job.id,
            name: job.name,
            status: job.status,
        })
    }

    pub async fn recover(&self, now: i64) -> Result<Vec<Job>, EngineError> {
        let recovered = self.store.recover_stale_leases(now).await?;
        for queue in recovered
            .iter()
            .filter(|job| job.status == JobStatus::Pending)
            .map(|job| job.name.clone())
            .collect::<HashSet<_>>()
        {
            self.ensure_awake(queue).await;
        }
        Ok(recovered)
    }

    /// Persists a fenced worker result and schedules immediate retries only
    /// after the result transaction has committed.
    pub async fn complete(
        &self,
        job_id: i64,
        dispatch_id: &str,
        outcome: JobOutcome,
    ) -> Result<Option<Job>, EngineError> {
        let job = self.store.complete(job_id, dispatch_id, outcome).await?;
        if let Some(job) = &job
            && job.status == JobStatus::Pending
        {
            self.ensure_awake(job.name.clone()).await;
        }
        Ok(job)
    }

    /// Connects a dispatcher result stream to durable completion and capacity
    /// wakeups. Call once after constructing the engine.
    pub fn start_result_listener(&self) {
        let Some(mut events) = self.dispatcher.subscribe_events() else {
            return;
        };
        let engine = self.clone();
        tokio::spawn(async move {
            loop {
                match events.recv().await {
                    Ok(event) => match event {
                        WorkerEvent::Registered { queue_name } => {
                            engine.ensure_awake(queue_name).await;
                        }
                        WorkerEvent::Result { queue_name, result } => {
                            let completion_engine = engine.clone();
                            tokio::spawn(async move {
                                let _ = completion_engine
                                    .complete(result.job_id, &result.dispatch_id, result.outcome)
                                    .await;
                            });
                            engine.ensure_awake(queue_name).await;
                        }
                    },
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        });
    }

    pub async fn dispatch(&self, permit: ReservedDispatch, job: Job) -> Result<(), DispatchError> {
        self.dispatcher.dispatch(permit, job).await
    }

    async fn ensure_awake(&self, queue: String) {
        let mut awake = self
            .scheduler
            .awake
            .lock()
            .expect("engine wake lock poisoned");
        if let Some(rewake) = awake.get_mut(&queue) {
            *rewake = true;
            return;
        }
        awake.insert(queue.clone(), false);
        let _ = self.scheduler.tx.send(queue);
    }

    fn start_scheduler(&self, mut rx: mpsc::UnboundedReceiver<String>) {
        let engine = self.clone();
        tokio::spawn(async move {
            let mut recovery = tokio::time::interval(std::time::Duration::from_secs(30));
            loop {
                tokio::select! {
                    Some(queue) = rx.recv() => {
                        let mut queues = HashSet::from([queue]);
                        while let Ok(queue) = rx.try_recv() { queues.insert(queue); }
                        engine.drain_pass(queues).await;
                    }
                    _ = recovery.tick() => {
                        let now = crate::unix_now();
                        if let Ok(recovered) = engine.store.recover_stale_leases(now).await {
                            let queues = recovered.into_iter().filter(|job| job.status == JobStatus::Pending).map(|job| job.name).collect();
                            engine.wake_after_pass(queues);
                        }
                    }
                    else => break,
                }
            }
        });
    }

    async fn drain_pass(&self, queues: HashSet<String>) {
        let count = self
            .scheduler
            .options
            .batch_size_max
            .min(MAX_CLAIM_BATCH_SIZE);
        let requests: Vec<_> = queues
            .iter()
            .map(|queue_name| QueueReservation {
                queue_name: queue_name.clone(),
                count,
            })
            .collect();
        let Ok(permits) = self.dispatcher.reserve(requests).await else {
            self.wake_after_pass(queues);
            return;
        };
        let mut permits_by_queue: HashMap<String, Vec<ReservedDispatch>> = HashMap::new();
        for permit in permits {
            permits_by_queue
                .entry(permit.queue_name.clone())
                .or_default()
                .push(permit);
        }
        let mut capped = HashSet::new();
        for queue_name in &queues {
            let Some(permits) = permits_by_queue.remove(queue_name) else {
                continue;
            };
            let Ok(Some(queue)) = self.store.get_queue(queue_name).await else {
                for permit in permits {
                    self.dispatcher.release(permit).await;
                }
                continue;
            };
            let reserved = permits.len();
            let Ok(jobs) = self
                .store
                .claim_batch(queue_name, queue.timeout_secs as i64, reserved)
                .await
            else {
                for permit in permits {
                    self.dispatcher.release(permit).await;
                }
                continue;
            };
            // Filled every reserved permit and hit the ask ceiling: more pending
            // may remain with free capacity still available.
            if jobs.len() == reserved && reserved == count {
                capped.insert(queue_name.clone());
            }
            let mut permits = permits.into_iter();
            let mut dispatches = Vec::with_capacity(jobs.len());
            for job in jobs {
                let permit = permits
                    .next()
                    .expect("claim cannot exceed reserved permits");
                dispatches.push((permit, job));
            }
            let dispatcher = self.dispatcher.clone();
            let store = self.store.clone();
            let max_in_flight = self.scheduler.options.max_in_flight;
            futures_util::stream::iter(dispatches)
                .for_each_concurrent(Some(max_in_flight), move |(permit, job)| {
                    let dispatcher = dispatcher.clone();
                    let store = store.clone();
                    async move {
                        if dispatcher.dispatch(permit, job.clone()).await.is_err()
                            && let Some(dispatch_id) = job.dispatch_id.as_deref()
                        {
                            let _ = store.release_claim(job.id, dispatch_id).await;
                        }
                    }
                })
                .await;
            for permit in permits {
                self.dispatcher.release(permit).await;
            }
        }
        self.wake_after_pass(capped.clone());
        for queue in queues.difference(&capped) {
            let mut awake = self
                .scheduler
                .awake
                .lock()
                .expect("engine wake lock poisoned");
            if awake.remove(queue).unwrap_or(false) {
                let _ = self.scheduler.tx.send(queue.clone());
            }
        }
    }

    fn wake_after_pass(&self, queues: HashSet<String>) {
        for queue in queues {
            let mut awake = self
                .scheduler
                .awake
                .lock()
                .expect("engine wake lock poisoned");
            if let Some(rewake) = awake.get_mut(&queue) {
                *rewake = false;
            }
            let _ = self.scheduler.tx.send(queue);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dispatch_options_reject_unsafe_claim_batches() {
        let options = DispatchOptions {
            batch_size_max: MAX_CLAIM_BATCH_SIZE + 1,
            ..DispatchOptions::default()
        };
        assert!(options.validate().is_err());
    }
}
