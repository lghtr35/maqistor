use std::path::Path;

use maqistor_engine::{AcceptedJob, DurableStore, Job, JobOutcome, JobQueue, ExecutionStatus, StoreError};

use super::options::DurabilityMode;
use super::common::{
    default_results_path, heal_orphan_claims, merge_job, unix_now, RwConnection,
};
use super::ingest::{IngestClaimed, IngestHandle};
use super::options::SqliteWriteOptions;
use super::results::{CompletionDisposition, DispatchInsert, DispatchedExecution, ResultsHandle};

#[derive(Clone)]
pub struct SqliteStore {
    ingest: IngestHandle,
    results: ResultsHandle,
}

impl SqliteStore {
    pub fn open(ingest_path: impl AsRef<Path>) -> Result<Self, StoreError> {
        Self::open_with_options(ingest_path, SqliteWriteOptions::default())
    }

    pub fn open_with_options(
        ingest_path: impl AsRef<Path>,
        options: SqliteWriteOptions,
    ) -> Result<Self, StoreError> {
        let ingest_path = ingest_path.as_ref().to_path_buf();
        let results_path = default_results_path(&ingest_path);
        Self::open_with_options_pair(ingest_path, results_path, options)
    }

    pub fn open_with_options_pair(
        ingest_path: impl AsRef<Path>,
        results_path: impl AsRef<Path>,
        options: SqliteWriteOptions,
    ) -> Result<Self, StoreError> {
        options.validate().map_err(StoreError::Internal)?;
        let ingest_path = ingest_path.as_ref().to_path_buf();
        let results_path = results_path.as_ref().to_path_buf();
        let ingest = IngestHandle::open(ingest_path.clone(), &options)?;
        let results = ResultsHandle::open(results_path, &options)?;
        Self::heal_on_open(&ingest_path, &ingest, &results)?;
        Ok(Self { ingest, results})
    }

    fn heal_on_open(
        ingest_path: &Path,
        ingest: &IngestHandle,
        results: &ResultsHandle,
    ) -> Result<(), StoreError> {
        let durability = DurabilityMode::Balanced;
        let ingest_rw = RwConnection::open(ingest_path, durability)?;
        let results_rw = RwConnection::open(results.path(), durability)?;
        heal_orphan_claims(&ingest_rw.conn, &results_rw.conn)?;
        let _ = ingest;
        Ok(())
    }

    async fn composed_job(&self, job_id: i64) -> Result<Job, StoreError> {
        let accepted = self.ingest.accepted_row(job_id).await?;
        let execution = self.results.execution(job_id).await?;
        Ok(merge_job(accepted, execution))
    }

    fn job_from_claimed(
        claimed: &IngestClaimed,
        dispatched: &DispatchedExecution,
    ) -> Job {
        Job {
            id: claimed.id,
            name: claimed.name.clone(),
            status: ExecutionStatus::Running,
            payload: claimed.payload.clone(),
            execution_count: dispatched.execution_count,
            lease_expires_at: Some(dispatched.lease_expires_at),
            dispatch_id: Some(dispatched.dispatch_id.clone()),
            result_payload: None,
            result_error: None,
            created_at: claimed.created_at,
            updated_at: dispatched.updated_at,
        }
    }

    async fn claim_batch_inner(
        &self,
        queue_name: &str,
        limit: usize,
    ) -> Result<Vec<Job>, StoreError> {
        let claimed = self.ingest.claim_batch(queue_name, limit).await?;
        if claimed.is_empty() {
            return Ok(Vec::new());
        }
        let claimed_at = unix_now();
        let mut dispatch_rows = Vec::with_capacity(claimed.len());
        for row in &claimed {
            let lease_expires_at = claimed_at
                .saturating_add((row.timeout_secs as i64).saturating_mul(1000));
            dispatch_rows.push(DispatchInsert {
                job_id: row.id,
                queue_name: row.name.clone(),
                dispatch_id: row.dispatch_id.clone(),
                lease_expires_at,
            });
        }
        let dispatched = match self.results.dispatch(dispatch_rows).await {
            Ok(dispatched) => dispatched,
            Err(err) => {
                for row in &claimed {
                    let _ = self.ingest.repend(row.id, &row.dispatch_id).await;
                }
                return Err(err);
            }
        };
        let mut jobs = Vec::with_capacity(claimed.len());
        for (claimed, dispatched) in claimed.iter().zip(dispatched.iter()) {
            jobs.push(Self::job_from_claimed(claimed, dispatched));
        }
        Ok(jobs)
    }

    pub async fn cleanup_expired_records(&self, cutoff: i64) -> Result<usize, StoreError> {
        let mut deleted = 0;
        deleted += self.ingest.cleanup_expired_records(cutoff).await?;
        deleted += self.results.cleanup_expired_records(cutoff).await?;
        Ok(deleted)
    }
}

impl DurableStore for SqliteStore {
    async fn upsert_queue(&self, queue: JobQueue) -> Result<JobQueue, StoreError> {
        let queue = self.ingest.upsert_queue(queue).await?;
        self.results.upsert_queue(queue.clone()).await?;
        Ok(queue)
    }

    async fn get_queue(&self, name: &str) -> Result<Option<JobQueue>, StoreError> {
        self.ingest.reads.queue(name.to_string()).await
    }

    async fn list_queues(&self) -> Result<Vec<JobQueue>, StoreError> {
        self.ingest.reads.queues().await
    }

    async fn enqueue(&self, job: AcceptedJob) -> Result<AcceptedJob, StoreError> {
        self.ingest.enqueue(job).await
    }

    async fn get_job(&self, job_id: i64) -> Result<Job, StoreError> {
        self.composed_job(job_id).await
    }

    async fn status(&self, job_id: i64) -> Result<ExecutionStatus, StoreError> {
        Ok(self.composed_job(job_id).await?.status)
    }

    async fn claim_next(&self, queue_name: &str) -> Result<Option<Job>, StoreError> {
        Ok(self.claim_batch(queue_name, 1).await?.pop())
    }

    async fn claim_batch(
        &self,
        queue_name: &str,
        limit: usize,
    ) -> Result<Vec<Job>, StoreError> {
        self.claim_batch_inner(queue_name, limit)
            .await
    }

    async fn complete(
        &self,
        job_id: i64,
        dispatch_id: &str,
        outcome: JobOutcome,
    ) -> Result<Option<Job>, StoreError> {
        let disposition = self.results.complete(job_id, dispatch_id, outcome).await?;
        if disposition == CompletionDisposition::Ignored {
            return Ok(None);
        }
        if disposition == CompletionDisposition::Repend {
            self.ingest.repend(job_id, dispatch_id).await?;
        }
        self.composed_job(job_id).await.map(Some)
    }

    async fn complete_worker_result(
        &self,
        job_id: i64,
        dispatch_id: &str,
        outcome: JobOutcome,
    ) -> Result<bool, StoreError> {
        let disposition = self.results.complete(job_id, dispatch_id, outcome).await?;
        if disposition == CompletionDisposition::Repend {
            self.ingest.repend(job_id, dispatch_id).await?;
            return Ok(true);
        }
        Ok(false)
    }


    async fn release_claim(&self, job_id: i64, dispatch_id: &str) -> Result<bool, StoreError> {
        let accepted = self.ingest.accepted_row(job_id).await?;
        if accepted.dispatch_id.as_deref() != Some(dispatch_id) {
            return Ok(false);
        }
        self.results.abandon(job_id, dispatch_id).await?;
        self.ingest.repend(job_id, dispatch_id).await?;
        Ok(true)
    }

    async fn recover_stale_leases(&self, now: i64) -> Result<Vec<Job>, StoreError> {
        let recovered = self.results.recover_stale(now).await?;
        let mut jobs = Vec::with_capacity(recovered.len());
        for item in recovered {
            if item.should_repend {
                self.ingest.repend(item.job_id, &item.dispatch_id).await?;
            }
            let accepted = self.ingest.accepted_row(item.job_id).await?;
            let execution = self.results.execution(item.job_id).await?;
            jobs.push(merge_job(accepted, execution));
        }
        Ok(jobs)
    }
}
