use maqistor_api::{JobRequest, JobResponse};
use maqistor_persistence::{Job, JobStore, StoreError};
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("failed to serialize payload: {0}")]
    Payload(String),
}

#[derive(Clone)]
pub struct Engine<S: JobStore> {
    store: S,
}

impl<S: JobStore> Engine<S> {
    pub fn new(store: S) -> Self {
        Self { store }
    }

    pub async fn submit(&self, job: JobRequest) -> Result<JobResponse, EngineError> {
        let payload = serde_json::to_vec(&job.payload)
            .map_err(|err| EngineError::Payload(err.to_string()))?;
        let result = self
            .store
            .enqueue(Job::new_pending(job.name, payload))
            .await?;
        Ok(JobResponse {
            id: result.id,
            name: result.name,
            status: result.status.to_string(),
        })
    }

    pub async fn get_job(&self, id: Uuid) -> Result<JobResponse, EngineError> {
        let job = self.store.get_job(id).await?;
        Ok(JobResponse {
            id: job.id,
            name: job.name,
            status: job.status.to_string(),
        })
    }
}
