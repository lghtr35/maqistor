use std::fs;
use std::path::Path;
use std::time::{Duration, Instant};

use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

use crate::types::{unix_now, Job, JobQueue, JobStatus, StoreError};
use crate::JobStore;

const SCHEMA_VERSION: i32 = 1;
const CHANNEL_CAPACITY: usize = 1024;
const DEFAULT_BATCH_SIZE: usize = 64;
const DEFAULT_BATCH_WAIT_MS: u64 = 5;
const DEFAULT_BATCH_WAIT_MIN_MS: u64 = 1;
const DEFAULT_BATCH_WAIT_MAX_MS: u64 = 100;

#[derive(Debug, Clone)]
pub struct SqliteWriteOptions {
    pub batch_size: usize,
    pub adaptive_batch_wait: bool,
    pub batch_wait: Duration,
    pub batch_wait_min: Duration,
    pub batch_wait_max: Duration,
}

impl Default for SqliteWriteOptions {
    fn default() -> Self {
        Self {
            batch_size: DEFAULT_BATCH_SIZE,
            adaptive_batch_wait: false,
            batch_wait: Duration::from_millis(DEFAULT_BATCH_WAIT_MS),
            batch_wait_min: Duration::from_millis(DEFAULT_BATCH_WAIT_MIN_MS),
            batch_wait_max: Duration::from_millis(DEFAULT_BATCH_WAIT_MAX_MS),
        }
    }
}

impl SqliteWriteOptions {
    pub fn fixed(batch_size: usize, batch_wait: Duration) -> Self {
        Self {
            batch_size: batch_size.max(1),
            adaptive_batch_wait: false,
            batch_wait,
            batch_wait_min: batch_wait,
            batch_wait_max: batch_wait,
        }
    }

    pub fn adaptive(
        batch_size: usize,
        batch_wait_min: Duration,
        batch_wait_max: Duration,
    ) -> Self {
        let batch_wait_min = batch_wait_min;
        let batch_wait_max = batch_wait_max.max(batch_wait_min);
        Self {
            batch_size: batch_size.max(1),
            adaptive_batch_wait: true,
            batch_wait: batch_wait_min,
            batch_wait_min,
            batch_wait_max,
        }
    }

    fn initial_live_wait(&self) -> Duration {
        if self.adaptive_batch_wait {
            self.batch_wait_min
        } else {
            self.batch_wait
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum FlushReason {
    FullBatch,
    Timeout,
    Interrupted,
}

fn clamp_wait_ms(ms: u64, min: Duration, max: Duration) -> Duration {
    let lo = min.as_millis() as u64;
    let hi = max.as_millis() as u64;
    Duration::from_millis(ms.clamp(lo, hi))
}

/// Move live wait toward min under load, toward max when quiet.
fn adapt_live_wait(
    live: Duration,
    min: Duration,
    max: Duration,
    batch_size: usize,
    filled: usize,
    reason: FlushReason,
) -> Duration {
    let ms = live.as_millis() as u64;
    let next = match reason {
        FlushReason::FullBatch => ms / 2,
        FlushReason::Timeout if filled * 10 >= batch_size.saturating_mul(7) => (ms * 4) / 5,
        FlushReason::Timeout => ms.saturating_mul(3) / 2 + 1,
        FlushReason::Interrupted => ms,
    };
    clamp_wait_ms(next, min, max)
}

enum DbRequest {
    UpsertQueue {
        queue: JobQueue,
        reply: oneshot::Sender<Result<JobQueue, StoreError>>,
    },
    GetQueue {
        name: String,
        reply: oneshot::Sender<Result<Option<JobQueue>, StoreError>>,
    },
    ListQueues {
        reply: oneshot::Sender<Result<Vec<JobQueue>, StoreError>>,
    },
    Enqueue {
        job: Job,
        reply: oneshot::Sender<Result<Job, StoreError>>,
    },
    GetJob {
        job_id: Uuid,
        reply: oneshot::Sender<Result<Job, StoreError>>,
    },
    Status {
        job_id: Uuid,
        reply: oneshot::Sender<Result<JobStatus, StoreError>>,
    },
    ClaimNext {
        queue_name: String,
        lease_duration_secs: i64,
        reply: oneshot::Sender<Result<Option<Job>, StoreError>>,
    },
    RecoverStaleLeases {
        now: i64,
        reply: oneshot::Sender<Result<Vec<Job>, StoreError>>,
    },
}

struct PendingEnqueue {
    job: Job,
    reply: oneshot::Sender<Result<Job, StoreError>>,
}

struct SqliteConn {
    conn: Connection,
}

impl SqliteConn {
    fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let path = path.as_ref();
        if let Some(parent) = path.parent().filter(|parent| !parent.as_os_str().is_empty()) {
            fs::create_dir_all(parent).map_err(|err| StoreError::Internal(err.to_string()))?;
        }

        let conn = Connection::open(path).map_err(|err| StoreError::Internal(err.to_string()))?;
        conn.busy_timeout(std::time::Duration::from_secs(5))
            .map_err(|err| StoreError::Internal(err.to_string()))?;
        conn.pragma_update(None, "journal_mode", "WAL")
            .map_err(|err| StoreError::Internal(err.to_string()))?;
        conn.pragma_update(None, "foreign_keys", "ON")
            .map_err(|err| StoreError::Internal(err.to_string()))?;

        let store = Self { conn };
        store.migrate()?;
        Ok(store)
    }

    fn migrate(&self) -> Result<(), StoreError> {
        self.conn
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS schema_version (
                    version INTEGER NOT NULL
                );",
            )
            .map_err(|err| StoreError::Internal(err.to_string()))?;

        let version: i32 = self
            .conn
            .query_row(
                "SELECT version FROM schema_version LIMIT 1",
                [],
                |row| row.get(0),
            )
            .optional()
            .map_err(|err| StoreError::Internal(err.to_string()))?
            .unwrap_or(-1);

        if version < 1 {
            self.apply_v1_schema()?;
            self.conn
                .execute("DELETE FROM schema_version", [])
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            self.conn
                .execute(
                    "INSERT INTO schema_version (version) VALUES (?1)",
                    params![SCHEMA_VERSION],
                )
                .map_err(|err| StoreError::Internal(err.to_string()))?;
        }

        Ok(())
    }

    fn apply_v1_schema(&self) -> Result<(), StoreError> {
        self.conn
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS job_queues (
                    name TEXT PRIMARY KEY,
                    concurrency INTEGER NOT NULL,
                    max_retries INTEGER NOT NULL,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY,
                    queue_name TEXT NOT NULL REFERENCES job_queues(name),
                    status TEXT NOT NULL,
                    payload BLOB NOT NULL,
                    attempt INTEGER NOT NULL,
                    lease_expires_at INTEGER,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_jobs_queue_pending
                    ON jobs(queue_name, created_at)
                    WHERE status = 'pending';

                CREATE INDEX IF NOT EXISTS idx_jobs_stale_leases
                    ON jobs(lease_expires_at)
                    WHERE status = 'running';",
            )
            .map_err(|err| StoreError::Internal(err.to_string()))
    }

    fn require_queue(&self, name: &str) -> Result<(), StoreError> {
        let exists: Option<i64> = self
            .conn
            .query_row(
                "SELECT 1 FROM job_queues WHERE name = ?1",
                params![name],
                |row| row.get(0),
            )
            .optional()
            .map_err(|err| StoreError::Internal(err.to_string()))?;

        if exists.is_some() {
            Ok(())
        } else {
            Err(StoreError::QueueNotFound(name.to_string()))
        }
    }

    fn upsert_queue_internal(&self, queue: &JobQueue) -> Result<(), StoreError> {
        self.conn
            .execute(
                "INSERT INTO job_queues (name, concurrency, max_retries, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(name) DO UPDATE SET
                    concurrency = excluded.concurrency,
                    max_retries = excluded.max_retries,
                    updated_at = excluded.updated_at",
                params![
                    queue.name,
                    queue.concurrency,
                    queue.max_retries,
                    queue.created_at,
                    queue.updated_at,
                ],
            )
            .map_err(|err| StoreError::Internal(err.to_string()))?;
        Ok(())
    }

    fn row_to_job(row: &rusqlite::Row<'_>) -> rusqlite::Result<Job> {
        let id: String = row.get(0)?;
        let queue_name: String = row.get(1)?;
        let status: String = row.get(2)?;
        let payload: Vec<u8> = row.get(3)?;
        let attempt: i64 = row.get(4)?;
        let lease_expires_at: Option<i64> = row.get(5)?;
        let created_at: i64 = row.get(6)?;
        let updated_at: i64 = row.get(7)?;

        Ok(Job {
            id: Uuid::parse_str(&id).map_err(|err| {
                rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(err))
            })?,
            name: queue_name,
            status: JobStatus::parse(&status).ok_or_else(|| {
                rusqlite::Error::FromSqlConversionFailure(
                    2,
                    rusqlite::types::Type::Text,
                    Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("unknown job status: {status}"),
                    )),
                )
            })?,
            payload,
            attempt: u32::try_from(attempt).map_err(|err| {
                rusqlite::Error::FromSqlConversionFailure(4, rusqlite::types::Type::Integer, Box::new(err))
            })?,
            lease_expires_at,
            created_at,
            updated_at,
        })
    }

    fn row_to_queue(row: &rusqlite::Row<'_>) -> rusqlite::Result<JobQueue> {
        Ok(JobQueue {
            name: row.get(0)?,
            concurrency: u32::try_from(row.get::<_, i64>(1)?).map_err(|err| {
                rusqlite::Error::FromSqlConversionFailure(1, rusqlite::types::Type::Integer, Box::new(err))
            })?,
            max_retries: u32::try_from(row.get::<_, i64>(2)?).map_err(|err| {
                rusqlite::Error::FromSqlConversionFailure(2, rusqlite::types::Type::Integer, Box::new(err))
            })?,
            created_at: row.get(3)?,
            updated_at: row.get(4)?,
        })
    }

    fn upsert_queue(&mut self, queue: JobQueue) -> Result<JobQueue, StoreError> {
        let mut queue = queue;
        queue.updated_at = unix_now();
        self.upsert_queue_internal(&queue)?;
        self.get_queue(&queue.name)?
            .ok_or_else(|| StoreError::QueueNotFound(queue.name))
    }

    fn get_queue(&self, name: &str) -> Result<Option<JobQueue>, StoreError> {
        self.conn
            .query_row(
                "SELECT name, concurrency, max_retries, created_at, updated_at
                 FROM job_queues WHERE name = ?1",
                params![name],
                Self::row_to_queue,
            )
            .optional()
            .map_err(|err| StoreError::Internal(err.to_string()))
    }

    fn list_queues(&self) -> Result<Vec<JobQueue>, StoreError> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT name, concurrency, max_retries, created_at, updated_at
                 FROM job_queues ORDER BY name ASC",
            )
            .map_err(|err| StoreError::Internal(err.to_string()))?;

        let queues = stmt
            .query_map([], Self::row_to_queue)
            .map_err(|err| StoreError::Internal(err.to_string()))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| StoreError::Internal(err.to_string()))?;

        Ok(queues)
    }

    fn enqueue_batch(&mut self, batch: Vec<PendingEnqueue>) {
        if batch.is_empty() {
            return;
        }

        let mut to_insert = Vec::with_capacity(batch.len());
        for pending in batch {
            match self.require_queue(&pending.job.name) {
                Ok(()) => to_insert.push(pending),
                Err(err) => {
                    let _ = pending.reply.send(Err(err));
                }
            }
        }

        if to_insert.is_empty() {
            return;
        }

        let result = (|| -> Result<(), StoreError> {
            let tx = self
                .conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(|err| StoreError::Internal(err.to_string()))?;

            for pending in &to_insert {
                tx.execute(
                    "INSERT INTO jobs (
                        id, queue_name, status, payload, attempt,
                        lease_expires_at, created_at, updated_at
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                    params![
                        pending.job.id.to_string(),
                        pending.job.name,
                        pending.job.status.as_str(),
                        pending.job.payload,
                        pending.job.attempt,
                        pending.job.lease_expires_at,
                        pending.job.created_at,
                        pending.job.updated_at,
                    ],
                )
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            }

            tx.commit()
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            Ok(())
        })();

        match result {
            Ok(()) => {
                for pending in to_insert {
                    let _ = pending.reply.send(Ok(pending.job));
                }
            }
            Err(err) => {
                for pending in to_insert {
                    let _ = pending.reply.send(Err(err.clone()));
                }
            }
        }
    }

    fn get_job(&self, job_id: Uuid) -> Result<Job, StoreError> {
        self.conn
            .query_row(
                "SELECT id, queue_name, status, payload, attempt,
                        lease_expires_at, created_at, updated_at
                 FROM jobs WHERE id = ?1",
                params![job_id.to_string()],
                Self::row_to_job,
            )
            .optional()
            .map_err(|err| StoreError::Internal(err.to_string()))?
            .ok_or(StoreError::NotFound(job_id))
    }

    fn status(&self, job_id: Uuid) -> Result<JobStatus, StoreError> {
        Ok(self.get_job(job_id)?.status)
    }

    fn claim_next(
        &mut self,
        queue_name: &str,
        lease_duration_secs: i64,
    ) -> Result<Option<Job>, StoreError> {
        self.require_queue(queue_name)?;

        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|err| StoreError::Internal(err.to_string()))?;

        let job_id: Option<String> = tx
            .query_row(
                "SELECT id FROM jobs
                 WHERE queue_name = ?1 AND status = 'pending'
                 ORDER BY created_at ASC
                 LIMIT 1",
                params![queue_name],
                |row| row.get(0),
            )
            .optional()
            .map_err(|err| StoreError::Internal(err.to_string()))?;

        let Some(job_id) = job_id else {
            return Ok(None);
        };

        let now = unix_now();
        let lease_expires_at = now + lease_duration_secs;
        let updated = tx
            .execute(
                "UPDATE jobs
                 SET status = 'running', lease_expires_at = ?1, updated_at = ?2
                 WHERE id = ?3 AND status = 'pending'",
                params![lease_expires_at, now, job_id],
            )
            .map_err(|err| StoreError::Internal(err.to_string()))?;

        if updated == 0 {
            return Ok(None);
        }

        tx.commit()
            .map_err(|err| StoreError::Internal(err.to_string()))?;

        self.get_job(Uuid::parse_str(&job_id).map_err(|err| StoreError::Internal(err.to_string()))?)
            .map(Some)
    }

    fn recover_stale_leases(&mut self, now: i64) -> Result<Vec<Job>, StoreError> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT id, queue_name, status, payload, attempt,
                        lease_expires_at, created_at, updated_at
                 FROM jobs
                 WHERE status = 'running'
                   AND lease_expires_at IS NOT NULL
                   AND lease_expires_at < ?1",
            )
            .map_err(|err| StoreError::Internal(err.to_string()))?;

        let stale_jobs = stmt
            .query_map(params![now], Self::row_to_job)
            .map_err(|err| StoreError::Internal(err.to_string()))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| StoreError::Internal(err.to_string()))?;

        if stale_jobs.is_empty() {
            return Ok(Vec::new());
        }

        self.conn
            .execute(
                "UPDATE jobs
                 SET status = 'pending',
                     lease_expires_at = NULL,
                     attempt = attempt + 1,
                     updated_at = ?1
                 WHERE status = 'running'
                   AND lease_expires_at IS NOT NULL
                   AND lease_expires_at < ?2",
                params![now, now],
            )
            .map_err(|err| StoreError::Internal(err.to_string()))?;

        let mut recovered = Vec::with_capacity(stale_jobs.len());
        for job in stale_jobs {
            recovered.push(self.get_job(job.id)?);
        }

        Ok(recovered)
    }

    fn handle(&mut self, request: DbRequest) {
        match request {
            DbRequest::UpsertQueue { queue, reply } => {
                let _ = reply.send(self.upsert_queue(queue));
            }
            DbRequest::GetQueue { name, reply } => {
                let _ = reply.send(self.get_queue(&name));
            }
            DbRequest::ListQueues { reply } => {
                let _ = reply.send(self.list_queues());
            }
            DbRequest::Enqueue { job, reply } => {
                // Defensive: writer_loop batches Enqueue; should not reach here.
                self.enqueue_batch(vec![PendingEnqueue { job, reply }]);
            }
            DbRequest::GetJob { job_id, reply } => {
                let _ = reply.send(self.get_job(job_id));
            }
            DbRequest::Status { job_id, reply } => {
                let _ = reply.send(self.status(job_id));
            }
            DbRequest::ClaimNext {
                queue_name,
                lease_duration_secs,
                reply,
            } => {
                let _ = reply.send(self.claim_next(&queue_name, lease_duration_secs));
            }
            DbRequest::RecoverStaleLeases { now, reply } => {
                let _ = reply.send(self.recover_stale_leases(now));
            }
        }
    }
}

/// Cloneable async handle. All ops are serialized on a dedicated writer task.
#[derive(Clone)]
pub struct SqliteStore {
    tx: mpsc::Sender<DbRequest>,
}

impl SqliteStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        Self::open_with_options(path, SqliteWriteOptions::default())
    }

    pub fn open_with_options(
        path: impl AsRef<Path>,
        options: SqliteWriteOptions,
    ) -> Result<Self, StoreError> {
        let conn = SqliteConn::open(path)?;
        let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
        tokio::spawn(writer_loop(conn, rx, options));
        Ok(Self { tx })
    }

    async fn call<T>(
        &self,
        build: impl FnOnce(oneshot::Sender<Result<T, StoreError>>) -> DbRequest,
    ) -> Result<T, StoreError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(build(reply))
            .await
            .map_err(|_| StoreError::Internal("db writer stopped".into()))?;
        rx.await
            .map_err(|_| StoreError::Internal("db writer dropped reply".into()))?
    }
}

async fn writer_loop(
    mut conn: SqliteConn,
    mut rx: mpsc::Receiver<DbRequest>,
    options: SqliteWriteOptions,
) {
    let mut pending: Vec<PendingEnqueue> = Vec::new();
    let mut batch_deadline: Option<Instant> = None;
    let mut live_wait = options.initial_live_wait();

    loop {
        let request = if pending.is_empty() {
            match rx.recv().await {
                Some(request) => request,
                None => break,
            }
        } else {
            let deadline = batch_deadline.expect("deadline set when pending is non-empty");
            let remaining = deadline.saturating_duration_since(Instant::now());
            match tokio::time::timeout(remaining, rx.recv()).await {
                Ok(Some(request)) => request,
                Ok(None) => {
                    flush_pending(
                        &mut conn,
                        &mut pending,
                        &mut batch_deadline,
                        &mut live_wait,
                        &options,
                        FlushReason::Timeout,
                    );
                    break;
                }
                Err(_) => {
                    flush_pending(
                        &mut conn,
                        &mut pending,
                        &mut batch_deadline,
                        &mut live_wait,
                        &options,
                        FlushReason::Timeout,
                    );
                    continue;
                }
            }
        };

        match request {
            DbRequest::Enqueue { job, reply } => {
                if pending.is_empty() {
                    batch_deadline = Some(Instant::now() + live_wait);
                }
                pending.push(PendingEnqueue { job, reply });

                while pending.len() < options.batch_size {
                    match rx.try_recv() {
                        Ok(DbRequest::Enqueue { job, reply }) => {
                            pending.push(PendingEnqueue { job, reply });
                        }
                        Ok(other) => {
                            flush_pending(
                                &mut conn,
                                &mut pending,
                                &mut batch_deadline,
                                &mut live_wait,
                                &options,
                                FlushReason::Interrupted,
                            );
                            conn.handle(other);
                            break;
                        }
                        Err(mpsc::error::TryRecvError::Empty) => break,
                        Err(mpsc::error::TryRecvError::Disconnected) => {
                            flush_pending(
                                &mut conn,
                                &mut pending,
                                &mut batch_deadline,
                                &mut live_wait,
                                &options,
                                FlushReason::Timeout,
                            );
                            return;
                        }
                    }
                }

                if pending.len() >= options.batch_size {
                    flush_pending(
                        &mut conn,
                        &mut pending,
                        &mut batch_deadline,
                        &mut live_wait,
                        &options,
                        FlushReason::FullBatch,
                    );
                }
            }
            other => {
                if !pending.is_empty() {
                    flush_pending(
                        &mut conn,
                        &mut pending,
                        &mut batch_deadline,
                        &mut live_wait,
                        &options,
                        FlushReason::Interrupted,
                    );
                }
                conn.handle(other);
            }
        }
    }
}

fn flush_pending(
    conn: &mut SqliteConn,
    pending: &mut Vec<PendingEnqueue>,
    batch_deadline: &mut Option<Instant>,
    live_wait: &mut Duration,
    options: &SqliteWriteOptions,
    reason: FlushReason,
) {
    let filled = pending.len();
    if filled == 0 {
        *batch_deadline = None;
        return;
    }
    conn.enqueue_batch(std::mem::take(pending));
    *batch_deadline = None;
    if options.adaptive_batch_wait {
        *live_wait = adapt_live_wait(
            *live_wait,
            options.batch_wait_min,
            options.batch_wait_max,
            options.batch_size,
            filled,
            reason,
        );
    }
}

impl JobStore for SqliteStore {
    async fn upsert_queue(&self, queue: JobQueue) -> Result<JobQueue, StoreError> {
        self.call(|reply| DbRequest::UpsertQueue { queue, reply })
            .await
    }

    async fn get_queue(&self, name: &str) -> Result<Option<JobQueue>, StoreError> {
        let name = name.to_string();
        self.call(|reply| DbRequest::GetQueue { name, reply }).await
    }

    async fn list_queues(&self) -> Result<Vec<JobQueue>, StoreError> {
        self.call(|reply| DbRequest::ListQueues { reply }).await
    }

    async fn enqueue(&self, job: Job) -> Result<Job, StoreError> {
        self.call(|reply| DbRequest::Enqueue { job, reply }).await
    }

    async fn get_job(&self, job_id: Uuid) -> Result<Job, StoreError> {
        self.call(|reply| DbRequest::GetJob { job_id, reply }).await
    }

    async fn status(&self, job_id: Uuid) -> Result<JobStatus, StoreError> {
        self.call(|reply| DbRequest::Status { job_id, reply }).await
    }

    async fn claim_next(
        &self,
        queue_name: &str,
        lease_duration_secs: i64,
    ) -> Result<Option<Job>, StoreError> {
        let queue_name = queue_name.to_string();
        self.call(|reply| DbRequest::ClaimNext {
            queue_name,
            lease_duration_secs,
            reply,
        })
        .await
    }

    async fn recover_stale_leases(&self, now: i64) -> Result<Vec<Job>, StoreError> {
        self.call(|reply| DbRequest::RecoverStaleLeases { now, reply })
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn persists_queues_and_jobs_across_reopen() {
        let path = std::env::temp_dir().join(format!("maqistor-test-{}.db", Uuid::new_v4()));
        let job_id = {
            let store = SqliteStore::open(&path).expect("open store");
            store
                .upsert_queue(JobQueue::new("email"))
                .await
                .expect("upsert queue");
            let job = store
                .enqueue(Job::new_pending("email", b"payload".to_vec()))
                .await
                .expect("enqueue");
            job.id
        };

        let store = SqliteStore::open(&path).expect("reopen store");
        let queues = store.list_queues().await.expect("list queues");
        assert_eq!(queues.len(), 1);
        assert_eq!(queues[0].name, "email");

        let job = store.get_job(job_id).await.expect("get job");
        assert_eq!(job.status, JobStatus::Pending);
        assert_eq!(job.payload, b"payload");

        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn claim_and_recover_stale_lease() {
        let path = std::env::temp_dir().join(format!("maqistor-test-{}.db", Uuid::new_v4()));
        let store = SqliteStore::open(&path).expect("open store");
        store
            .upsert_queue(JobQueue::new("email"))
            .await
            .expect("upsert queue");
        let job = store
            .enqueue(Job::new_pending("email", vec![]))
            .await
            .expect("enqueue");

        let claimed = store
            .claim_next("email", 30)
            .await
            .expect("claim")
            .expect("claimed job");
        assert_eq!(claimed.id, job.id);
        assert_eq!(claimed.status, JobStatus::Running);

        let recovered = store
            .recover_stale_leases(claimed.lease_expires_at.unwrap() + 1)
            .await
            .expect("recover");
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].status, JobStatus::Pending);
        assert_eq!(recovered[0].attempt, 1);

        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn enqueue_rejects_unknown_queue() {
        let path = std::env::temp_dir().join(format!("maqistor-test-{}.db", Uuid::new_v4()));
        let store = SqliteStore::open(&path).expect("open store");

        let error = store
            .enqueue(Job::new_pending("missing", vec![]))
            .await
            .expect_err("unknown queue should fail");

        assert!(matches!(error, StoreError::QueueNotFound(name) if name == "missing"));

        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn concurrent_enqueues_are_durable_after_await() {
        let path = std::env::temp_dir().join(format!("maqistor-test-{}.db", Uuid::new_v4()));
        let store = SqliteStore::open_with_options(
            &path,
            SqliteWriteOptions::fixed(32, Duration::from_millis(20)),
        )
        .expect("open store");
        store
            .upsert_queue(JobQueue::new("email"))
            .await
            .expect("upsert queue");

        let mut handles = Vec::new();
        for i in 0..50 {
            let store = store.clone();
            handles.push(tokio::spawn(async move {
                store
                    .enqueue(Job::new_pending("email", format!("p{i}").into_bytes()))
                    .await
            }));
        }

        let mut ids = Vec::new();
        for handle in handles {
            let job = handle.await.expect("join").expect("enqueue");
            ids.push(job.id);
        }

        for id in ids {
            let job = store.get_job(id).await.expect("get job");
            assert_eq!(job.status, JobStatus::Pending);
            assert_eq!(job.name, "email");
        }

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(format!("{}-wal", path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", path.display()));
    }

    #[tokio::test]
    async fn mixed_unknown_queue_does_not_block_valid_enqueues() {
        let path = std::env::temp_dir().join(format!("maqistor-test-{}.db", Uuid::new_v4()));
        let store = SqliteStore::open_with_options(
            &path,
            SqliteWriteOptions::fixed(64, Duration::from_millis(30)),
        )
        .expect("open store");
        store
            .upsert_queue(JobQueue::new("email"))
            .await
            .expect("upsert queue");

        let store_ok = store.clone();
        let store_bad = store.clone();
        let ok = tokio::spawn(async move {
            store_ok
                .enqueue(Job::new_pending("email", b"ok".to_vec()))
                .await
        });
        let bad = tokio::spawn(async move {
            store_bad
                .enqueue(Job::new_pending("missing", b"no".to_vec()))
                .await
        });

        let ok_job = ok.await.expect("join").expect("valid enqueue");
        let bad_err = bad.await.expect("join").expect_err("unknown queue");
        assert!(matches!(bad_err, StoreError::QueueNotFound(_)));
        assert_eq!(
            store.get_job(ok_job.id).await.expect("get").payload,
            b"ok"
        );

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn adaptive_wait_moves_toward_min_when_full_and_max_when_sparse() {
        let min = Duration::from_millis(1);
        let max = Duration::from_millis(100);
        let live = Duration::from_millis(40);

        let after_full = adapt_live_wait(live, min, max, 32, 32, FlushReason::FullBatch);
        assert!(after_full < live);
        assert!(after_full >= min);

        let after_sparse = adapt_live_wait(live, min, max, 32, 2, FlushReason::Timeout);
        assert!(after_sparse > live);
        assert!(after_sparse <= max);

        let at_min = adapt_live_wait(min, min, max, 32, 32, FlushReason::FullBatch);
        assert_eq!(at_min, min);

        let at_max = adapt_live_wait(max, min, max, 32, 1, FlushReason::Timeout);
        assert_eq!(at_max, max);
    }
}
