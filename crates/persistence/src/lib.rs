mod sqlite;
mod types;

use uuid::Uuid;

pub use sqlite::{SqliteStore, SqliteWriteOptions};
pub use types::{Job, JobQueue, JobStatus, StoreError};

pub trait JobStore: Send + Sync {
    /// Register or refresh queue metadata from config (survives restarts).
    fn upsert_queue(
        &self,
        queue: JobQueue,
    ) -> impl std::future::Future<Output = Result<JobQueue, StoreError>> + Send;

    fn get_queue(
        &self,
        name: &str,
    ) -> impl std::future::Future<Output = Result<Option<JobQueue>, StoreError>> + Send;

    fn list_queues(
        &self,
    ) -> impl std::future::Future<Output = Result<Vec<JobQueue>, StoreError>> + Send;

    /// Persist a new pending job. The queue must already exist (registered at init).
    fn enqueue(
        &self,
        job: Job,
    ) -> impl std::future::Future<Output = Result<Job, StoreError>> + Send;

    fn get_job(
        &self,
        job_id: Uuid,
    ) -> impl std::future::Future<Output = Result<Job, StoreError>> + Send;

    fn status(
        &self,
        job_id: Uuid,
    ) -> impl std::future::Future<Output = Result<JobStatus, StoreError>> + Send;

    /// Claim the oldest pending job in a queue and mark it running with a lease.
    fn claim_next(
        &self,
        queue_name: &str,
        lease_duration_secs: i64,
    ) -> impl std::future::Future<Output = Result<Option<Job>, StoreError>> + Send;

    /// Re-queue running jobs whose lease expired (crash / worker recovery).
    fn recover_stale_leases(
        &self,
        now: i64,
    ) -> impl std::future::Future<Output = Result<Vec<Job>, StoreError>> + Send;
}
