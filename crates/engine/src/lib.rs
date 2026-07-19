mod types;

use std::future::Future;

pub use types::{Job, JobQueue, JobStatus, StoreError};

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

pub trait WorkerDispatcher: Send + Sync {
    fn dispatch(&self, job: Job) -> impl Future<Output = Result<(), DispatchError>> + Send;
}

#[derive(Clone, Default)]
pub struct NoopDispatcher;

impl WorkerDispatcher for NoopDispatcher {
    async fn dispatch(&self, _job: Job) -> Result<(), DispatchError> {
        Err(DispatchError::NoCapacity)
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

#[derive(Clone)]
pub struct Engine<S: DurableStore, D: WorkerDispatcher = NoopDispatcher> {
    store: S,
    dispatcher: D,
}

impl<S: DurableStore> Engine<S, NoopDispatcher> {
    pub fn new(store: S) -> Self {
        Self::with_dispatcher(store, NoopDispatcher)
    }
}

impl<S: DurableStore, D: WorkerDispatcher> Engine<S, D> {
    pub fn with_dispatcher(store: S, dispatcher: D) -> Self {
        Self { store, dispatcher }
    }

    pub async fn submit(&self, job: SubmitJob) -> Result<JobView, EngineError> {
        let payload = serde_json::to_vec(&job.payload)
            .map_err(|err| EngineError::Payload(err.to_string()))?;
        let result = self
            .store
            .enqueue(Job::new_pending(job.name, payload))
            .await?;
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
        Ok(self.store.recover_stale_leases(now).await?)
    }

    pub async fn dispatch(&self, job: Job) -> Result<(), DispatchError> {
        self.dispatcher.dispatch(job).await
    }
}
