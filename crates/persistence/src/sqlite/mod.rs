use std::collections::HashSet;
use std::fs;
use std::path::Path;
use std::sync::mpsc::sync_channel;
use std::thread;
use std::time::{Duration, Instant};

use rusqlite::{
    Connection, OptionalExtension, ToSql, TransactionBehavior, params, params_from_iter,
};
use serde::Deserialize;
use tokio::sync::{mpsc, oneshot};
use tracing::debug;

use maqistor_engine::{DurableStore, Job, JobQueue, JobStatus, StoreError};

const SCHEMA_VERSION: i32 = 2;
const CHANNEL_CAPACITY: usize = 1024;
const DEFAULT_BATCH_SIZE_MIN: usize = 1;
const DEFAULT_BATCH_SIZE_MAX: usize = 1024;
const DEFAULT_BATCH_WAIT_MIN_MS: u64 = 1;
const DEFAULT_BATCH_WAIT_MAX_MS: u64 = 100;
const DEFAULT_EWMA_WINDOW: usize = 16;
const DIRECTION_STREAK_SAMPLES: u8 = 3;
const DEFAULT_BATCH_PROBE_FACTOR: f64 = 1.10;
const DEFAULT_BATCH_BACKOFF_FACTOR: f64 = 0.80;
const WAIT_ADJUST_UP: f64 = 1.25;
const WAIT_ADJUST_DOWN: f64 = 0.80;
const WAIT_DIRECTION_HIGH: f64 = 1.20;
const WAIT_DIRECTION_LOW: f64 = 0.80;
const MAX_QUEUEING_RATIO: f64 = 1.20;
const TARGET_FILL_RATIO: f64 = 0.75;
const BASELINE_RELAXATION: f64 = 0.02;
const LOW_FILL_RATIO: f64 = 0.50;
const LOW_FILL_TIMEOUTS: u8 = 3;
// Seven bound values per job leaves comfortable room below SQLite's common
// variable limit while reducing Rust-to-SQLite calls from one per job to one
// per chunk. The final short chunk gets its own cached statement.
const INSERT_ROWS_PER_STATEMENT: usize = 64;

fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_secs() as i64
}

fn jobs_insert_sql(rows: usize) -> String {
    debug_assert!(rows > 0);
    let values = (0..rows)
        .map(|row| {
            let offset = row * 7;
            format!(
                "(?{}, ?{}, ?{}, ?{}, ?{}, ?{}, ?{})",
                offset + 1,
                offset + 2,
                offset + 3,
                offset + 4,
                offset + 5,
                offset + 6,
                offset + 7,
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "INSERT INTO jobs (
            queue_name, status, payload, attempt,
            lease_expires_at, created_at, updated_at
         ) VALUES {values}"
    )
}

#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum DurabilityMode {
    #[default]
    Balanced,
    Strict,
}

#[derive(Debug, Clone)]
pub struct AdaptiveBatchLimits {
    pub batch_size_min: usize,
    pub batch_size_max: usize,
    pub batch_wait_min: Duration,
    pub batch_wait_max: Duration,
}

impl Default for AdaptiveBatchLimits {
    fn default() -> Self {
        Self {
            batch_size_min: DEFAULT_BATCH_SIZE_MIN,
            batch_size_max: DEFAULT_BATCH_SIZE_MAX,
            batch_wait_min: Duration::from_millis(DEFAULT_BATCH_WAIT_MIN_MS),
            batch_wait_max: Duration::from_millis(DEFAULT_BATCH_WAIT_MAX_MS),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SqliteWriteOptions {
    pub durability: DurabilityMode,
    pub limits: AdaptiveBatchLimits,
    pub ewma_window: usize,
    pub batch_probe_factor: f64,
    pub batch_backoff_factor: f64,
}

impl Default for SqliteWriteOptions {
    fn default() -> Self {
        Self {
            durability: DurabilityMode::default(),
            limits: AdaptiveBatchLimits::default(),
            ewma_window: DEFAULT_EWMA_WINDOW,
            batch_probe_factor: DEFAULT_BATCH_PROBE_FACTOR,
            batch_backoff_factor: DEFAULT_BATCH_BACKOFF_FACTOR,
        }
    }
}

impl SqliteWriteOptions {
    pub fn validate(&self) -> Result<(), String> {
        let limits = &self.limits;
        if self.ewma_window == 0 {
            return Err("persistence.ewma_window must be greater than zero".into());
        }
        if !self.batch_probe_factor.is_finite() || self.batch_probe_factor <= 1.0 {
            return Err(
                "persistence.adaptation.batch_probe_factor must be greater than one".into(),
            );
        }
        if !self.batch_backoff_factor.is_finite()
            || !(0.0..1.0).contains(&self.batch_backoff_factor)
        {
            return Err(
                "persistence.adaptation.batch_backoff_factor must be greater than zero and less than one".into(),
            );
        }
        if limits.batch_size_min == 0 {
            return Err("persistence.limits.batch_size_min must be greater than zero".into());
        }
        if limits.batch_size_min > limits.batch_size_max {
            return Err("persistence.limits.batch_size_min must not exceed batch_size_max".into());
        }
        if limits.batch_wait_min.is_zero() {
            return Err("persistence.limits.batch_wait_min_ms must be greater than zero".into());
        }
        if limits.batch_wait_min > limits.batch_wait_max {
            return Err(
                "persistence.limits.batch_wait_min_ms must not exceed batch_wait_max_ms".into(),
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
enum FlushReason {
    FullBatch,
    Timeout,
    Interrupted,
}

#[derive(Debug, Clone, Copy)]
struct Ewma {
    alpha: f64,
    value: Option<f64>,
}

impl Ewma {
    fn new(window: usize) -> Self {
        Self {
            alpha: 2.0 / (window as f64 + 1.0),
            value: None,
        }
    }

    fn observe(&mut self, sample: f64) {
        if !sample.is_finite() || sample < 0.0 {
            return;
        }
        self.value = Some(match self.value {
            Some(value) => self.alpha * sample + (1.0 - self.alpha) * value,
            None => sample,
        });
    }

    fn value(&self) -> Option<f64> {
        self.value
    }
}

/// Requires the same non-zero direction several times in a row before acting.
#[derive(Debug, Default)]
struct DirectionStreak {
    direction: i8,
    samples: u8,
}

impl DirectionStreak {
    fn confirm(&mut self, direction: i8) -> bool {
        if direction == 0 {
            self.direction = 0;
            self.samples = 0;
            return false;
        }
        if self.direction == direction {
            self.samples = self.samples.saturating_add(1);
        } else {
            self.direction = direction;
            self.samples = 1;
        }
        if self.samples >= DIRECTION_STREAK_SAMPLES {
            self.samples = 0;
            return true;
        }
        return false;
    }
}

struct AdaptiveBatchController {
    limits: AdaptiveBatchLimits,
    request_rate: Ewma,
    commit_rate: Ewma,
    commit_duration: Ewma,
    fill_ratio: Ewma,
    baseline_commit_duration: Option<f64>,
    batch_size: usize,
    batch_wait: Duration,
    batch_probe_factor: f64,
    batch_backoff_factor: f64,
    backlog: usize,
    low_fill_timeouts: u8,
    last_request: Option<Instant>,
    last_commit: Option<Instant>,
    batch_direction_streak: DirectionStreak,
    wait_direction_streak: DirectionStreak,
}

impl AdaptiveBatchController {
    fn new(options: &SqliteWriteOptions) -> Self {
        let limits = options.limits.clone();
        Self {
            batch_size: ((limits.batch_size_min as f64 * limits.batch_size_max as f64)
                .sqrt()
                .round() as usize)
                .clamp(limits.batch_size_min, limits.batch_size_max),
            batch_wait: limits.batch_wait_min,
            batch_probe_factor: options.batch_probe_factor,
            batch_backoff_factor: options.batch_backoff_factor,
            backlog: 0,
            low_fill_timeouts: 0,
            limits,
            request_rate: Ewma::new(options.ewma_window),
            commit_rate: Ewma::new(options.ewma_window),
            commit_duration: Ewma::new(options.ewma_window),
            fill_ratio: Ewma::new(options.ewma_window),
            baseline_commit_duration: None,
            last_request: None,
            last_commit: None,
            batch_direction_streak: DirectionStreak::default(),
            wait_direction_streak: DirectionStreak::default(),
        }
    }

    fn observe_request(&mut self, now: Instant) {
        if let Some(previous) = self.last_request.replace(now) {
            let elapsed = now.saturating_duration_since(previous).as_secs_f64();
            if elapsed > 0.0 {
                self.request_rate.observe(1.0 / elapsed);
            }
        }
    }

    fn record_successful_commit(
        &mut self,
        filled: usize,
        elapsed: Duration,
        completed_at: Instant,
        backlog: usize,
        reason: FlushReason,
    ) {
        self.backlog = backlog;
        let duration = elapsed.as_secs_f64();
        self.commit_duration.observe(duration);
        self.observe_commit_baseline(duration);
        if let Some(previous) = self.last_commit.replace(completed_at) {
            let interval = completed_at
                .saturating_duration_since(previous)
                .as_secs_f64();
            if interval > 0.0 {
                self.commit_rate.observe(1.0 / interval);
            }
        }
        let fill_ratio = filled as f64 / self.batch_size.max(1) as f64;
        self.fill_ratio.observe(fill_ratio);
        if matches!(reason, FlushReason::Timeout) && backlog == 0 && fill_ratio < LOW_FILL_RATIO {
            self.low_fill_timeouts = self.low_fill_timeouts.saturating_add(1);
        } else {
            self.low_fill_timeouts = 0;
        }
        self.adjust_batch_size();
        self.adjust_batch_wait();
    }

    fn observe_commit_baseline(&mut self, sample: f64) {
        self.baseline_commit_duration = Some(match self.baseline_commit_duration {
            None => sample,
            Some(baseline) if sample < baseline => sample,
            Some(baseline) => baseline + (sample - baseline) * BASELINE_RELAXATION,
        });
    }

    fn adjust_batch_size(&mut self) {
        let Some(commit_duration) = self.commit_duration.value() else {
            return;
        };
        let Some(baseline) = self.baseline_commit_duration else {
            return;
        };
        let queueing_ratio = commit_duration / baseline.max(f64::MIN_POSITIVE);

        if self.low_fill_timeouts >= LOW_FILL_TIMEOUTS && queueing_ratio <= MAX_QUEUEING_RATIO {
            self.batch_size = (self.batch_size as f64 * self.batch_backoff_factor).floor() as usize;
            self.batch_size = self
                .batch_size
                .max(1)
                .clamp(self.limits.batch_size_min, self.limits.batch_size_max);
            self.low_fill_timeouts = 0;
            self.batch_direction_streak.confirm(0);
            return;
        }

        let demand_exceeds_service = match (self.request_rate.value(), self.commit_rate.value()) {
            (Some(request_rate), Some(commit_rate)) if commit_rate > 0.0 => {
                request_rate > self.batch_size as f64 * commit_rate
            }
            _ => false,
        };
        let direction = if queueing_ratio > MAX_QUEUEING_RATIO {
            -1
        } else if self.backlog > 0 || demand_exceeds_service {
            1
        } else {
            0
        };
        if !self.batch_direction_streak.confirm(direction) {
            return;
        }
        self.batch_size = match direction {
            1 => (self.batch_size as f64 * self.batch_probe_factor).ceil() as usize,
            -1 => (self.batch_size as f64 * self.batch_backoff_factor).floor() as usize,
            _ => self.batch_size,
        }
        .max(1)
        .clamp(self.limits.batch_size_min, self.limits.batch_size_max);
    }

    fn adjust_batch_wait(&mut self) {
        let Some(request_rate) = self.request_rate.value().filter(|rate| *rate > 0.0) else {
            return;
        };
        let desired = Duration::from_secs_f64(
            (self.batch_size as f64 * TARGET_FILL_RATIO / request_rate)
                .max(self.limits.batch_wait_min.as_secs_f64()),
        )
        .clamp(self.limits.batch_wait_min, self.limits.batch_wait_max);
        let direction =
            if desired.as_secs_f64() > self.batch_wait.as_secs_f64() * WAIT_DIRECTION_HIGH {
                1
            } else if desired.as_secs_f64() < self.batch_wait.as_secs_f64() * WAIT_DIRECTION_LOW {
                -1
            } else {
                0
            };
        if !self.wait_direction_streak.confirm(direction) {
            return;
        }
        let next = match direction {
            1 => {
                Duration::from_secs_f64(self.batch_wait.as_secs_f64() * WAIT_ADJUST_UP).min(desired)
            }
            -1 => Duration::from_secs_f64(self.batch_wait.as_secs_f64() * WAIT_ADJUST_DOWN)
                .max(desired),
            _ => self.batch_wait,
        };
        self.batch_wait = next.clamp(self.limits.batch_wait_min, self.limits.batch_wait_max);
    }
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
        job_id: i64,
        reply: oneshot::Sender<Result<Job, StoreError>>,
    },
    Status {
        job_id: i64,
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

struct BatchCommit {
    inserted: usize,
    duration: Duration,
}

struct SqliteConn {
    conn: Connection,
}

impl SqliteConn {
    fn open(path: impl AsRef<Path>, durability: DurabilityMode) -> Result<Self, StoreError> {
        let path = path.as_ref();
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent).map_err(|err| StoreError::Internal(err.to_string()))?;
        }

        let conn = Connection::open(path).map_err(|err| StoreError::Internal(err.to_string()))?;
        conn.busy_timeout(std::time::Duration::from_secs(5))
            .map_err(|err| StoreError::Internal(err.to_string()))?;
        conn.pragma_update(None, "journal_mode", "WAL")
            .map_err(|err| StoreError::Internal(err.to_string()))?;
        conn.pragma_update(None, "foreign_keys", "ON")
            .map_err(|err| StoreError::Internal(err.to_string()))?;
        let synchronous = match durability {
            DurabilityMode::Balanced => "NORMAL",
            DurabilityMode::Strict => "FULL",
        };
        conn.pragma_update(None, "synchronous", synchronous)
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
            .query_row("SELECT version FROM schema_version LIMIT 1", [], |row| {
                row.get(0)
            })
            .optional()
            .map_err(|err| StoreError::Internal(err.to_string()))?
            .unwrap_or(-1);

        if version == -1 {
            self.apply_schema()?;
            self.conn
                .execute("DELETE FROM schema_version", [])
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            self.conn
                .execute(
                    "INSERT INTO schema_version (version) VALUES (?1)",
                    params![SCHEMA_VERSION],
                )
                .map_err(|err| StoreError::Internal(err.to_string()))?;
        } else if version != SCHEMA_VERSION {
            return Err(StoreError::Internal(format!(
                "unsupported database schema version {version}; reset the database"
            )));
        }

        Ok(())
    }

    fn apply_schema(&self) -> Result<(), StoreError> {
        self.conn
            .execute_batch(
                "CREATE TABLE job_queues (
                    name TEXT PRIMARY KEY,
                    concurrency INTEGER NOT NULL,
                    max_retries INTEGER NOT NULL,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL
                );

                CREATE TABLE jobs (
                    id INTEGER PRIMARY KEY,
                    queue_name TEXT NOT NULL REFERENCES job_queues(name),
                    status TEXT NOT NULL,
                    payload BLOB NOT NULL,
                    attempt INTEGER NOT NULL,
                    lease_expires_at INTEGER,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL
                );

                CREATE INDEX idx_jobs_queue_pending
                    ON jobs(queue_name, created_at)
                    WHERE status = 'pending';

                CREATE INDEX idx_jobs_stale_leases
                    ON jobs(lease_expires_at)
                    WHERE status = 'running';",
            )
            .map_err(|err| StoreError::Internal(err.to_string()))
    }

    fn queue_names(&self) -> Result<HashSet<String>, StoreError> {
        let mut statement = self
            .conn
            .prepare("SELECT name FROM job_queues")
            .map_err(|err| StoreError::Internal(err.to_string()))?;
        statement
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(|err| StoreError::Internal(err.to_string()))?
            .collect::<Result<HashSet<_>, _>>()
            .map_err(|err| StoreError::Internal(err.to_string()))
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
        let id: i64 = row.get(0)?;
        let queue_name: String = row.get(1)?;
        let status: String = row.get(2)?;
        let payload: Vec<u8> = row.get(3)?;
        let attempt: i64 = row.get(4)?;
        let lease_expires_at: Option<i64> = row.get(5)?;
        let created_at: i64 = row.get(6)?;
        let updated_at: i64 = row.get(7)?;

        Ok(Job {
            id,
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
                rusqlite::Error::FromSqlConversionFailure(
                    4,
                    rusqlite::types::Type::Integer,
                    Box::new(err),
                )
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
                rusqlite::Error::FromSqlConversionFailure(
                    1,
                    rusqlite::types::Type::Integer,
                    Box::new(err),
                )
            })?,
            max_retries: u32::try_from(row.get::<_, i64>(2)?).map_err(|err| {
                rusqlite::Error::FromSqlConversionFailure(
                    2,
                    rusqlite::types::Type::Integer,
                    Box::new(err),
                )
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

    fn enqueue_batch(
        &mut self,
        batch: Vec<PendingEnqueue>,
        queue_names: &HashSet<String>,
    ) -> Option<BatchCommit> {
        if batch.is_empty() {
            return None;
        }

        let mut to_insert = Vec::with_capacity(batch.len());
        for pending in batch {
            if queue_names.contains(&pending.job.name) {
                to_insert.push(pending);
            } else {
                let _ = pending
                    .reply
                    .send(Err(StoreError::QueueNotFound(pending.job.name)));
            }
        }

        if to_insert.is_empty() {
            return None;
        }

        let inserted = to_insert.len();
        let started = Instant::now();
        let result = (|| -> Result<(), StoreError> {
            let tx = self
                .conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(|err| StoreError::Internal(err.to_string()))?;

            for chunk in to_insert.chunks_mut(INSERT_ROWS_PER_STATEMENT) {
                let statuses: Vec<&str> = chunk
                    .iter()
                    .map(|pending| pending.job.status.as_str())
                    .collect();
                let mut values: Vec<&dyn ToSql> = Vec::with_capacity(chunk.len() * 7);
                for (pending, status) in chunk.iter().zip(&statuses) {
                    values.push(&pending.job.name);
                    values.push(status);
                    values.push(&pending.job.payload);
                    values.push(&pending.job.attempt);
                    values.push(&pending.job.lease_expires_at);
                    values.push(&pending.job.created_at);
                    values.push(&pending.job.updated_at);
                }
                tx.prepare_cached(&jobs_insert_sql(chunk.len()))
                    .map_err(|err| StoreError::Internal(err.to_string()))?
                    .execute(params_from_iter(values))
                    .map_err(|err| StoreError::Internal(err.to_string()))?;
                let first_id = tx.last_insert_rowid() - chunk.len() as i64 + 1;
                for (offset, pending) in chunk.iter_mut().enumerate() {
                    pending.job.id = first_id + offset as i64;
                }
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
                Some(BatchCommit {
                    inserted,
                    duration: started.elapsed(),
                })
            }
            Err(err) => {
                for pending in to_insert {
                    let _ = pending.reply.send(Err(err.clone()));
                }
                None
            }
        }
    }

    fn get_job(&self, job_id: i64) -> Result<Job, StoreError> {
        self.conn
            .query_row(
                "SELECT id, queue_name, status, payload, attempt,
                        lease_expires_at, created_at, updated_at
                 FROM jobs WHERE id = ?1",
                params![job_id],
                Self::row_to_job,
            )
            .optional()
            .map_err(|err| StoreError::Internal(err.to_string()))?
            .ok_or(StoreError::NotFound(job_id))
    }

    fn status(&self, job_id: i64) -> Result<JobStatus, StoreError> {
        Ok(self.get_job(job_id)?.status)
    }

    fn claim_next(
        &mut self,
        queue_name: &str,
        lease_duration_secs: i64,
        queue_names: &HashSet<String>,
    ) -> Result<Option<Job>, StoreError> {
        if !queue_names.contains(queue_name) {
            return Err(StoreError::QueueNotFound(queue_name.to_string()));
        }

        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|err| StoreError::Internal(err.to_string()))?;

        let job_id: Option<i64> = tx
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

        self.get_job(job_id).map(Some)
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

    fn handle(&mut self, request: DbRequest, queue_names: &mut HashSet<String>) {
        match request {
            DbRequest::UpsertQueue { queue, reply } => {
                let name = queue.name.clone();
                let result = self.upsert_queue(queue);
                if result.is_ok() {
                    queue_names.insert(name);
                }
                let _ = reply.send(result);
            }
            DbRequest::GetQueue { name, reply } => {
                let _ = reply.send(self.get_queue(&name));
            }
            DbRequest::ListQueues { reply } => {
                let _ = reply.send(self.list_queues());
            }
            DbRequest::Enqueue { job, reply } => {
                // Defensive: writer_loop batches Enqueue; should not reach here.
                let _ = self.enqueue_batch(vec![PendingEnqueue { job, reply }], queue_names);
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
                let _ = reply.send(self.claim_next(&queue_name, lease_duration_secs, queue_names));
            }
            DbRequest::RecoverStaleLeases { now, reply } => {
                let _ = reply.send(self.recover_stale_leases(now));
            }
        }
    }
}

/// Cloneable async handle. All ops are serialized on a dedicated writer thread.
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
        options.validate().map_err(StoreError::Internal)?;
        let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
        let path = path.as_ref().to_path_buf();
        let durability = options.durability;
        let (ready_tx, ready_rx) = sync_channel(1);
        thread::Builder::new()
            .name("maqistor-sqlite-writer".into())
            .spawn(move || {
                let runtime = match tokio::runtime::Builder::new_current_thread()
                    .enable_time()
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(err) => {
                        let _ = ready_tx.send(Err(StoreError::Internal(err.to_string())));
                        return;
                    }
                };
                runtime.block_on(async move {
                    let conn = match SqliteConn::open(path, durability) {
                        Ok(conn) => conn,
                        Err(err) => {
                            let _ = ready_tx.send(Err(err));
                            return;
                        }
                    };
                    let queue_names = match conn.queue_names() {
                        Ok(queue_names) => queue_names,
                        Err(err) => {
                            let _ = ready_tx.send(Err(err));
                            return;
                        }
                    };
                    if ready_tx.send(Ok(())).is_ok() {
                        writer_loop(conn, rx, options, queue_names).await;
                    }
                });
            })
            .map_err(|err| StoreError::Internal(err.to_string()))?;
        ready_rx
            .recv()
            .map_err(|_| StoreError::Internal("db writer failed to start".into()))??;
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
    mut queue_names: HashSet<String>,
) {
    let mut pending: Vec<PendingEnqueue> = Vec::new();
    let mut batch_deadline: Option<Instant> = None;
    let mut controller = AdaptiveBatchController::new(&options);

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
                        &mut controller,
                        &queue_names,
                        FlushReason::Timeout,
                        rx.len(),
                    );
                    break;
                }
                Err(_) => {
                    flush_pending(
                        &mut conn,
                        &mut pending,
                        &mut batch_deadline,
                        &mut controller,
                        &queue_names,
                        FlushReason::Timeout,
                        rx.len(),
                    );
                    continue;
                }
            }
        };

        match request {
            DbRequest::Enqueue { job, reply } => {
                if pending.is_empty() {
                    batch_deadline = Some(Instant::now() + controller.batch_wait);
                }
                controller.observe_request(Instant::now());
                pending.push(PendingEnqueue { job, reply });

                while pending.len() < controller.batch_size {
                    match rx.try_recv() {
                        Ok(DbRequest::Enqueue { job, reply }) => {
                            controller.observe_request(Instant::now());
                            pending.push(PendingEnqueue { job, reply });
                        }
                        Ok(other) => {
                            flush_pending(
                                &mut conn,
                                &mut pending,
                                &mut batch_deadline,
                                &mut controller,
                                &queue_names,
                                FlushReason::Interrupted,
                                rx.len(),
                            );
                            conn.handle(other, &mut queue_names);
                            break;
                        }
                        Err(mpsc::error::TryRecvError::Empty) => break,
                        Err(mpsc::error::TryRecvError::Disconnected) => {
                            flush_pending(
                                &mut conn,
                                &mut pending,
                                &mut batch_deadline,
                                &mut controller,
                                &queue_names,
                                FlushReason::Timeout,
                                rx.len(),
                            );
                            return;
                        }
                    }
                }

                if pending.len() >= controller.batch_size {
                    flush_pending(
                        &mut conn,
                        &mut pending,
                        &mut batch_deadline,
                        &mut controller,
                        &queue_names,
                        FlushReason::FullBatch,
                        rx.len(),
                    );
                }
            }
            other => {
                if !pending.is_empty() {
                    flush_pending(
                        &mut conn,
                        &mut pending,
                        &mut batch_deadline,
                        &mut controller,
                        &queue_names,
                        FlushReason::Interrupted,
                        rx.len(),
                    );
                }
                conn.handle(other, &mut queue_names);
            }
        }
    }
}

fn flush_pending(
    conn: &mut SqliteConn,
    pending: &mut Vec<PendingEnqueue>,
    batch_deadline: &mut Option<Instant>,
    controller: &mut AdaptiveBatchController,
    queue_names: &HashSet<String>,
    reason: FlushReason,
    backlog: usize,
) {
    let filled = pending.len();
    if filled == 0 {
        *batch_deadline = None;
        return;
    }
    let commit = conn.enqueue_batch(std::mem::take(pending), queue_names);
    *batch_deadline = None;
    if let Some(commit) = commit {
        controller.record_successful_commit(
            filled.min(commit.inserted),
            commit.duration,
            Instant::now(),
            backlog,
            reason,
        );
        debug!(
            ?reason,
            filled,
            batch_size = controller.batch_size,
            batch_wait_ms = controller.batch_wait.as_millis(),
            backlog,
            request_rate = ?controller.request_rate.value(),
            commit_rate = ?controller.commit_rate.value(),
            commit_duration_ms = ?controller.commit_duration.value().map(|duration| duration * 1_000.0),
            fill_ratio = ?controller.fill_ratio.value(),
            "adaptive sqlite batch updated"
        );
    }
}

impl DurableStore for SqliteStore {
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

    async fn get_job(&self, job_id: i64) -> Result<Job, StoreError> {
        self.call(|reply| DbRequest::GetJob { job_id, reply }).await
    }

    async fn status(&self, job_id: i64) -> Result<JobStatus, StoreError> {
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
mod tests;
