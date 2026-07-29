use std::fs;
use std::path::Path;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use rusqlite::{Connection, OpenFlags, OptionalExtension, params};
use uuid::Uuid;

use maqistor_engine::{
    AcceptedJob, Execution, ExecutionStatus, ExecutionWithQueueConfig, Job, JobQueue, StoreError,
};

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
            DurabilityMode::None => "OFF",
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

pub(crate) fn apply_acceptance_schema(conn: &Connection) -> Result<(), StoreError> {
    conn.execute_batch(
        "CREATE TABLE job_queues (
            name TEXT PRIMARY KEY,
            max_retries INTEGER NOT NULL,
            timeout_secs INTEGER NOT NULL,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        );

        CREATE TABLE accepted_jobs (
            id INTEGER PRIMARY KEY,
            queue_name TEXT NOT NULL REFERENCES job_queues(name),
            payload BLOB NOT NULL,
            dispatch_id TEXT,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        );

        CREATE INDEX idx_accepted_jobs_queue_available
            ON accepted_jobs(queue_name, created_at, id)
            WHERE dispatch_id IS NULL;",
    )
    .map_err(|err| StoreError::Internal(err.to_string()))
}

pub(crate) fn apply_executions_schema(conn: &Connection) -> Result<(), StoreError> {
    conn.execute_batch(
        "CREATE TABLE execution_queues (
            name TEXT PRIMARY KEY,
            max_retries INTEGER NOT NULL,
            timeout_secs INTEGER NOT NULL,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        );

        CREATE TABLE executions (
            id INTEGER PRIMARY KEY,
            job_id INTEGER NOT NULL,
            queue_name TEXT NOT NULL REFERENCES execution_queues(name),
            status TEXT NOT NULL,
            execution_count INTEGER NOT NULL,
            lease_expires_at INTEGER,
            dispatch_id TEXT NOT NULL UNIQUE,
            result_payload BLOB,
            result_error TEXT,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        );

        CREATE UNIQUE INDEX idx_executions_job_id ON executions(job_id);
        CREATE INDEX idx_executions_stale_leases
            ON executions(lease_expires_at)
            WHERE status = 'running';",
    )
    .map_err(|err| StoreError::Internal(err.to_string()))
}

pub(crate) fn row_to_execution_status(
    row: &rusqlite::Row<'_>,
    idx: usize,
) -> rusqlite::Result<ExecutionStatus> {
    let raw: String = row.get(idx)?;
    ExecutionStatus::parse(&raw).ok_or_else(|| {
        rusqlite::Error::FromSqlConversionFailure(
            idx,
            rusqlite::types::Type::Text,
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("unknown execution status: {raw}"),
            )),
        )
    })
}

pub(crate) fn row_to_accepted_job(row: &rusqlite::Row<'_>) -> rusqlite::Result<AcceptedJob> {
    row_to_accepted_job_at(row, 0)
}

pub(crate) fn row_to_accepted_job_at(
    row: &rusqlite::Row<'_>,
    offset: usize,
) -> rusqlite::Result<AcceptedJob> {
    Ok(AcceptedJob {
        id: row.get(offset)?,
        queue_name: row.get(offset + 1)?,
        payload: row.get(offset + 2)?,
        dispatch_id: row.get(offset + 3)?,
        created_at: row.get(offset + 4)?,
        updated_at: row.get(offset + 5)?,
    })
}

pub(crate) fn row_to_execution(row: &rusqlite::Row<'_>) -> rusqlite::Result<Execution> {
    row_to_execution_at(row, 0)
}

pub(crate) fn row_to_execution_at(
    row: &rusqlite::Row<'_>,
    offset: usize,
) -> rusqlite::Result<Execution> {
    Ok(Execution {
        id: row.get(offset)?,
        job_id: row.get(offset + 1)?,
        queue_name: row.get(offset + 2)?,
        status: row_to_execution_status(row, offset + 3)?,
        execution_count: u32::try_from(row.get::<_, i64>(offset + 4)?).map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(
                offset + 4,
                rusqlite::types::Type::Integer,
                Box::new(err),
            )
        })?,
        lease_expires_at: row.get(offset + 5)?,
        dispatch_id: row.get(offset + 6)?,
        result_payload: row.get(offset + 7)?,
        result_error: row.get(offset + 8)?,
        created_at: row.get(offset + 9)?,
        updated_at: row.get(offset + 10)?,
    })
}

pub(crate) fn row_to_queue(row: &rusqlite::Row<'_>) -> rusqlite::Result<JobQueue> {
    row_to_queue_at(row, 0)
}

pub(crate) fn row_to_queue_at(
    row: &rusqlite::Row<'_>,
    offset: usize,
) -> rusqlite::Result<JobQueue> {
    Ok(JobQueue {
        name: row.get(offset)?,
        max_retries: u32::try_from(row.get::<_, i64>(offset + 1)?).map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(
                offset + 1,
                rusqlite::types::Type::Integer,
                Box::new(err),
            )
        })?,
        timeout_secs: u64::try_from(row.get::<_, i64>(offset + 2)?).map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(
                offset + 2,
                rusqlite::types::Type::Integer,
                Box::new(err),
            )
        })?,
        created_at: row.get(offset + 3)?,
        updated_at: row.get(offset + 4)?,
    })
}

pub(crate) fn row_to_execution_with_queue_config(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<ExecutionWithQueueConfig> {
    Ok(ExecutionWithQueueConfig {
        execution: row_to_execution_at(row, 0)?,
        queue: row_to_queue_at(row, 11)?,
    })
}

pub(crate) const EXECUTION_WITH_QUEUE_COLUMNS: &str = "\
e.id, e.job_id, e.queue_name, e.status, e.execution_count, e.lease_expires_at, \
e.dispatch_id, e.result_payload, e.result_error, e.created_at, e.updated_at, \
q.name, q.max_retries, q.timeout_secs, q.created_at, q.updated_at";

pub(crate) const EXECUTION_WITH_QUEUE_FROM: &str = "\
FROM executions e \
JOIN execution_queues q ON q.name = e.queue_name";

pub(crate) fn merge_job(accepted: AcceptedJob, execution: Option<Execution>) -> Job {
    Job::from_accepted(accepted, execution.as_ref())
}

pub(crate) fn new_dispatch_id() -> String {
    Uuid::new_v4().to_string()
}

const ACCEPTED_JOB_SELECT: &str =
    "SELECT id, queue_name, payload, dispatch_id, created_at, updated_at FROM accepted_jobs";
const EXECUTION_SELECT: &str = "SELECT id, job_id, queue_name, status, execution_count, \
lease_expires_at, dispatch_id, result_payload, result_error, created_at, updated_at \
FROM executions";

#[derive(Clone)]
pub(crate) struct ReadPool {
    connections: Arc<Vec<Mutex<Connection>>>,
    next: Arc<AtomicUsize>,
    accepted_job_sql: &'static str,
    execution_sql: &'static str,
    queue_sql: &'static str,
    queues_sql: &'static str,
}

impl ReadPool {
    pub(crate) fn open_ingest(path: &Path) -> Result<Self, StoreError> {
        let accepted_job_sql = format!("{ACCEPTED_JOB_SELECT} WHERE id = ?1");
        let queue_sql = "SELECT name, max_retries, timeout_secs, created_at, updated_at FROM job_queues WHERE name = ?1";
        let queues_sql = "SELECT name, max_retries, timeout_secs, created_at, updated_at FROM job_queues ORDER BY name ASC";
        Self::open_with_sql(
            path,
            Box::leak(accepted_job_sql.into_boxed_str()),
            "",
            queue_sql,
            queues_sql,
        )
    }

    pub(crate) fn open_results(path: &Path) -> Result<Self, StoreError> {
        let execution_sql = format!("{EXECUTION_SELECT} WHERE job_id = ?1");
        Self::open_with_sql(
            path,
            "",
            Box::leak(execution_sql.into_boxed_str()),
            "",
            "",
        )
    }

    fn open_with_sql(
        path: &Path,
        accepted_job_sql: &'static str,
        execution_sql: &'static str,
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
            accepted_job_sql,
            execution_sql,
            queue_sql,
            queues_sql,
        })
    }

    fn connection(&self) -> Result<usize, StoreError> {
        Ok(self.next.fetch_add(1, Ordering::Relaxed) % self.connections.len())
    }

    pub(crate) async fn accepted_job(&self, job_id: i64) -> Result<AcceptedJob, StoreError> {
        let connections = self.connections.clone();
        let sql = self.accepted_job_sql;
        let index = self.connection()?;
        tokio::task::spawn_blocking(move || {
            let conn = connections[index]
                .lock()
                .map_err(|_| StoreError::Internal("read connection poisoned".into()))?;
            conn.query_row(sql, params![job_id], row_to_accepted_job)
                .optional()
                .map_err(|err| StoreError::Internal(err.to_string()))?
                .ok_or(StoreError::NotFound(job_id))
        })
        .await
        .map_err(|err| StoreError::Internal(err.to_string()))?
    }

    pub(crate) async fn execution(
        &self,
        job_id: i64,
    ) -> Result<Option<Execution>, StoreError> {
        if self.execution_sql.is_empty() {
            return Ok(None);
        }
        let connections = self.connections.clone();
        let sql = self.execution_sql;
        let index = self.connection()?;
        tokio::task::spawn_blocking(move || {
            let conn = connections[index]
                .lock()
                .map_err(|_| StoreError::Internal("read connection poisoned".into()))?;
            conn.query_row(sql, params![job_id], row_to_execution)
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
    acceptance: &Connection,
    executions: &Connection,
) -> Result<(), StoreError> {
    let mut stmt = acceptance
        .prepare("SELECT id, dispatch_id FROM accepted_jobs WHERE dispatch_id IS NOT NULL")
        .map_err(|err| StoreError::Internal(err.to_string()))?;
    let orphans: Vec<(i64, String)> = stmt
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .map_err(|err| StoreError::Internal(err.to_string()))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| StoreError::Internal(err.to_string()))?;

    let now = unix_now();
    for (job_id, dispatch_id) in orphans {
        let exists: i64 = executions
            .query_row(
                "SELECT COUNT(1) FROM executions WHERE dispatch_id = ?1",
                params![dispatch_id],
                |row| row.get(0),
            )
            .map_err(|err| StoreError::Internal(err.to_string()))?;
        if exists == 0 {
            acceptance
                .execute(
                    "UPDATE accepted_jobs SET dispatch_id = NULL, updated_at = ?1
                     WHERE id = ?2 AND dispatch_id = ?3",
                    params![now, job_id, dispatch_id],
                )
                .map_err(|err| StoreError::Internal(err.to_string()))?;
        }
    }
    Ok(())
}
