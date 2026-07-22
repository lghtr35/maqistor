use std::fs;
use std::path::Path;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use rusqlite::{Connection, OpenFlags, OptionalExtension, params};
use uuid::Uuid;

use maqistor_engine::{Job, JobQueue, JobStatus, StoreError};

use super::options::DurabilityMode;

pub(crate) const SCHEMA_VERSION: i32 = 1;

pub(crate) fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_millis() as i64
}

pub fn default_results_path(ingest: &Path) -> std::path::PathBuf {
    let stem = ingest
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("maqistor");
    let results_stem = stem
        .strip_suffix("-ingest")
        .map(|base| format!("{base}-results"))
        .unwrap_or_else(|| format!("{stem}-results"));
    ingest.with_file_name(format!("{results_stem}.db"))
}

pub(crate) struct RwConnection {
    pub(crate) conn: Connection,
}

impl RwConnection {
    pub(crate) fn open(path: impl AsRef<Path>, durability: DurabilityMode) -> Result<Self, StoreError> {
        let path = path.as_ref();
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent).map_err(|err| StoreError::Internal(err.to_string()))?;
        }

        let conn = Connection::open(path).map_err(|err| StoreError::Internal(err.to_string()))?;
        conn.busy_timeout(Duration::from_secs(5))
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
        Ok(Self { conn })
    }

    pub(crate) fn migrate_schema(
        &self,
        apply: impl FnOnce(&Connection) -> Result<(), StoreError>,
    ) -> Result<(), StoreError> {
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
            apply(&self.conn)?;
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
                "unsupported database schema version {version}; expected {SCHEMA_VERSION} — delete the database file and restart"
            )));
        }

        Ok(())
    }
}

pub(crate) fn apply_ingest_schema(conn: &Connection) -> Result<(), StoreError> {
    conn.execute_batch(
        "CREATE TABLE job_queues (
            name TEXT PRIMARY KEY,
            max_retries INTEGER NOT NULL,
            timeout_secs INTEGER NOT NULL,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        );

        CREATE TABLE jobs (
            id INTEGER PRIMARY KEY,
            queue_name TEXT NOT NULL REFERENCES job_queues(name),
            status TEXT NOT NULL,
            payload BLOB NOT NULL,
            execution_count INTEGER NOT NULL,
            dispatch_id TEXT,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        );

        CREATE INDEX idx_jobs_queue_pending
            ON jobs(queue_name, created_at, id)
            WHERE status = 'pending';",
    )
    .map_err(|err| StoreError::Internal(err.to_string()))
}

pub(crate) fn apply_results_schema(conn: &Connection) -> Result<(), StoreError> {
    conn.execute_batch(
        "CREATE TABLE job_attempts (
            id INTEGER PRIMARY KEY,
            job_id INTEGER NOT NULL,
            queue_name TEXT NOT NULL,
            status TEXT NOT NULL,
            execution_count INTEGER NOT NULL,
            max_retries_at_claim INTEGER NOT NULL,
            lease_expires_at INTEGER,
            dispatch_id TEXT NOT NULL UNIQUE,
            result_payload BLOB,
            result_error TEXT,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        );

        CREATE INDEX idx_attempts_job_id ON job_attempts(job_id, id);
        CREATE INDEX idx_attempts_stale_leases
            ON job_attempts(lease_expires_at)
            WHERE status = 'running';",
    )
    .map_err(|err| StoreError::Internal(err.to_string()))
}

#[derive(Debug, Clone)]
pub(crate) struct IngestJobRow {
    pub id: i64,
    pub queue_name: String,
    pub status: String,
    pub payload: Vec<u8>,
    pub execution_count: u32,
    pub dispatch_id: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone)]
pub(crate) struct AttemptRow {
    pub id: i64,
    pub job_id: i64,
    pub status: String,
    pub execution_count: u32,
    pub max_retries_at_claim: u32,
    pub lease_expires_at: Option<i64>,
    pub dispatch_id: String,
    pub result_payload: Option<Vec<u8>>,
    pub result_error: Option<String>,
    #[allow(dead_code)]
    pub created_at: i64,
    pub updated_at: i64,
}

pub(crate) fn row_to_ingest_job(row: &rusqlite::Row<'_>) -> rusqlite::Result<IngestJobRow> {
    Ok(IngestJobRow {
        id: row.get(0)?,
        queue_name: row.get(1)?,
        status: row.get(2)?,
        payload: row.get(3)?,
        execution_count: u32::try_from(row.get::<_, i64>(4)?).map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(4, rusqlite::types::Type::Integer, Box::new(err))
        })?,
        dispatch_id: row.get(5)?,
        created_at: row.get(6)?,
        updated_at: row.get(7)?,
    })
}

pub(crate) fn row_to_attempt(row: &rusqlite::Row<'_>) -> rusqlite::Result<AttemptRow> {
    let execution_count: i64 = row.get(4)?;
    let max_retries_at_claim: i64 = row.get(5)?;
    Ok(AttemptRow {
        id: row.get(0)?,
        job_id: row.get(1)?,
        status: row.get(3)?,
        execution_count: u32::try_from(execution_count).map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(4, rusqlite::types::Type::Integer, Box::new(err))
        })?,
        max_retries_at_claim: u32::try_from(max_retries_at_claim).map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(5, rusqlite::types::Type::Integer, Box::new(err))
        })?,
        lease_expires_at: row.get(6)?,
        dispatch_id: row.get(7)?,
        result_payload: row.get(8)?,
        result_error: row.get(9)?,
        created_at: row.get(10)?,
        updated_at: row.get(11)?,
    })
}

pub(crate) fn row_to_queue(row: &rusqlite::Row<'_>) -> rusqlite::Result<JobQueue> {
    Ok(JobQueue {
        name: row.get(0)?,
        max_retries: u32::try_from(row.get::<_, i64>(1)?).map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(1, rusqlite::types::Type::Integer, Box::new(err))
        })?,
        timeout_secs: u64::try_from(row.get::<_, i64>(2)?).map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(2, rusqlite::types::Type::Integer, Box::new(err))
        })?,
        created_at: row.get(3)?,
        updated_at: row.get(4)?,
    })
}

pub(crate) fn merge_job(ingest: IngestJobRow, attempt: Option<AttemptRow>) -> Result<Job, StoreError> {
    let mut job = Job {
        id: ingest.id,
        name: ingest.queue_name,
        status: JobStatus::Pending,
        payload: ingest.payload,
        execution_count: ingest.execution_count,
        lease_expires_at: None,
        dispatch_id: None,
        result_payload: None,
        result_error: None,
        created_at: ingest.created_at,
        updated_at: ingest.updated_at,
    };

    let Some(attempt) = attempt else {
        return Ok(job);
    };

    job.execution_count = attempt.execution_count;
    job.lease_expires_at = attempt.lease_expires_at;
    job.dispatch_id = Some(attempt.dispatch_id);
    job.result_payload = attempt.result_payload;
    job.result_error = attempt.result_error;
    job.updated_at = attempt.updated_at;

    job.status = match attempt.status.as_str() {
        "running" if ingest.status == "claimed" => JobStatus::Running,
        "running" => JobStatus::Pending,
        "completed" => JobStatus::Completed,
        "failed" if ingest.status == "pending" => JobStatus::Pending,
        "failed" => JobStatus::Failed,
        other => {
            return Err(StoreError::Internal(format!(
                "unknown attempt status: {other}"
            )));
        }
    };

    Ok(job)
}

pub(crate) fn new_dispatch_id() -> String {
    Uuid::new_v4().to_string()
}

const INGEST_JOB_SELECT: &str =
    "SELECT id, queue_name, status, payload, execution_count, dispatch_id, created_at, updated_at FROM jobs";
const ATTEMPT_SELECT: &str = "SELECT id, job_id, queue_name, status, execution_count, max_retries_at_claim, lease_expires_at, dispatch_id, result_payload, result_error, created_at, updated_at FROM job_attempts";

#[derive(Clone)]
pub(crate) struct ReadPool {
    connections: Arc<Vec<Mutex<Connection>>>,
    next: Arc<AtomicUsize>,
    job_sql: &'static str,
    attempt_latest_sql: &'static str,
    queue_sql: &'static str,
    queues_sql: &'static str,
}

impl ReadPool {
    pub(crate) fn open_ingest(path: &Path) -> Result<Self, StoreError> {
        let job_sql = format!("{INGEST_JOB_SELECT} WHERE id = ?1");
        let queue_sql = "SELECT name, max_retries, timeout_secs, created_at, updated_at FROM job_queues WHERE name = ?1";
        let queues_sql = "SELECT name, max_retries, timeout_secs, created_at, updated_at FROM job_queues ORDER BY name ASC";
        Self::open_with_sql(path, Box::leak(job_sql.into_boxed_str()), "", queue_sql, queues_sql)
    }

    pub(crate) fn open_results(path: &Path) -> Result<Self, StoreError> {
        let attempt_latest_sql = format!(
            "{ATTEMPT_SELECT} WHERE job_id = ?1 ORDER BY id DESC LIMIT 1"
        );
        Self::open_with_sql(path, "", Box::leak(attempt_latest_sql.into_boxed_str()), "", "")
    }

    fn open_with_sql(
        path: &Path,
        job_sql: &'static str,
        attempt_latest_sql: &'static str,
        queue_sql: &'static str,
        queues_sql: &'static str,
    ) -> Result<Self, StoreError> {
        let mut connections = Vec::with_capacity(4);
        for _ in 0..4 {
            let conn = Connection::open_with_flags(
                path,
                OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            )
            .map_err(|err| StoreError::Internal(err.to_string()))?;
            conn.busy_timeout(Duration::from_secs(5))
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            conn.pragma_update(None, "query_only", "ON")
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            connections.push(Mutex::new(conn));
        }
        Ok(Self {
            connections: Arc::new(connections),
            next: Arc::new(AtomicUsize::new(0)),
            job_sql,
            attempt_latest_sql,
            queue_sql,
            queues_sql,
        })
    }

    fn connection(&self) -> Result<usize, StoreError> {
        Ok(self.next.fetch_add(1, Ordering::Relaxed) % self.connections.len())
    }

    pub(crate) async fn ingest_job(&self, job_id: i64) -> Result<IngestJobRow, StoreError> {
        let connections = self.connections.clone();
        let sql = self.job_sql;
        let index = self.connection()?;
        tokio::task::spawn_blocking(move || {
            let conn = connections[index]
                .lock()
                .map_err(|_| StoreError::Internal("read connection poisoned".into()))?;
            conn.query_row(sql, params![job_id], row_to_ingest_job)
                .optional()
                .map_err(|err| StoreError::Internal(err.to_string()))?
                .ok_or(StoreError::NotFound(job_id))
        })
        .await
        .map_err(|err| StoreError::Internal(err.to_string()))?
    }

    pub(crate) async fn latest_attempt(&self, job_id: i64) -> Result<Option<AttemptRow>, StoreError> {
        if self.attempt_latest_sql.is_empty() {
            return Ok(None);
        }
        let connections = self.connections.clone();
        let sql = self.attempt_latest_sql;
        let index = self.connection()?;
        tokio::task::spawn_blocking(move || {
            let conn = connections[index]
                .lock()
                .map_err(|_| StoreError::Internal("read connection poisoned".into()))?;
            conn.query_row(sql, params![job_id], row_to_attempt)
                .optional()
                .map_err(|err| StoreError::Internal(err.to_string()))
        })
        .await
        .map_err(|err| StoreError::Internal(err.to_string()))?
    }

    pub(crate) async fn queue(&self, name: String) -> Result<Option<JobQueue>, StoreError> {
        if self.queue_sql.is_empty() {
            return Ok(None);
        }
        let connections = self.connections.clone();
        let sql = self.queue_sql;
        let index = self.connection()?;
        tokio::task::spawn_blocking(move || {
            let conn = connections[index]
                .lock()
                .map_err(|_| StoreError::Internal("read connection poisoned".into()))?;
            conn.query_row(sql, params![name], row_to_queue)
                .optional()
                .map_err(|err| StoreError::Internal(err.to_string()))
        })
        .await
        .map_err(|err| StoreError::Internal(err.to_string()))?
    }

    pub(crate) async fn queues(&self) -> Result<Vec<JobQueue>, StoreError> {
        if self.queues_sql.is_empty() {
            return Ok(Vec::new());
        }
        let connections = self.connections.clone();
        let sql = self.queues_sql;
        let index = self.connection()?;
        tokio::task::spawn_blocking(move || {
            let conn = connections[index]
                .lock()
                .map_err(|_| StoreError::Internal("read connection poisoned".into()))?;
            let mut stmt = conn
                .prepare(sql)
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            stmt.query_map([], row_to_queue)
                .map_err(|err| StoreError::Internal(err.to_string()))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|err| StoreError::Internal(err.to_string()))
        })
        .await
        .map_err(|err| StoreError::Internal(err.to_string()))?
    }
}

pub(crate) fn heal_orphan_claims(
    ingest: &Connection,
    results: &Connection,
) -> Result<(), StoreError> {
    let mut stmt = ingest
        .prepare(
            "SELECT id, dispatch_id FROM jobs WHERE status = 'claimed' AND dispatch_id IS NOT NULL",
        )
        .map_err(|err| StoreError::Internal(err.to_string()))?;
    let orphans: Vec<(i64, String)> = stmt
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .map_err(|err| StoreError::Internal(err.to_string()))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| StoreError::Internal(err.to_string()))?;

    let now = unix_now();
    for (job_id, dispatch_id) in orphans {
        let exists: i64 = results
            .query_row(
                "SELECT COUNT(1) FROM job_attempts WHERE dispatch_id = ?1",
                params![dispatch_id],
                |row| row.get(0),
            )
            .map_err(|err| StoreError::Internal(err.to_string()))?;
        if exists == 0 {
            ingest
                .execute(
                    "UPDATE jobs SET status = 'pending', dispatch_id = NULL, updated_at = ?1
                     WHERE id = ?2 AND status = 'claimed' AND dispatch_id = ?3",
                    params![now, job_id, dispatch_id],
                )
                .map_err(|err| StoreError::Internal(err.to_string()))?;
        }
    }
    Ok(())
}
