use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Job {
    /// Assigned by SQLite when the durable insert commits.
    pub id: i64,
    pub name: String,
    pub status: JobStatus,
    pub payload: Vec<u8>,
    pub execution_count: u32,
    pub lease_expires_at: Option<i64>,
    pub dispatch_id: Option<String>,
    pub result_payload: Option<Vec<u8>>,
    pub result_error: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

impl Job {
    pub fn new_pending(name: impl Into<String>, payload: Vec<u8>) -> Self {
        let now = unix_now();
        Self {
            id: 0,
            name: name.into(),
            status: JobStatus::Pending,
            payload,
            execution_count: 0,
            lease_expires_at: None,
            dispatch_id: None,
            result_payload: None,
            result_error: None,
            created_at: now,
            updated_at: now,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct JobQueue {
    pub name: String,
    pub max_retries: u32,
    pub timeout_secs: u64,
    /// Unix time in milliseconds.
    pub created_at: i64,
    /// Unix time in milliseconds.
    pub updated_at: i64,
}

impl JobQueue {
    pub fn new(name: impl Into<String>) -> Self {
        let now = unix_now();
        Self {
            name: name.into(),
            max_retries: 3,
            timeout_secs: 60,
            created_at: now,
            updated_at: now,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
pub enum JobStatus {
    Pending,
    Running,
    Completed,
    Failed,
}

impl JobStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "pending" => Some(Self::Pending),
            "running" => Some(Self::Running),
            "completed" => Some(Self::Completed),
            "failed" => Some(Self::Failed),
            _ => None,
        }
    }
}

impl fmt::Display for JobStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum StoreError {
    #[error("job not found: {0}")]
    NotFound(i64),
    #[error("queue not found: {0}")]
    QueueNotFound(String),
    #[error("internal store error: {0}")]
    Internal(String),
}

/// Current unix time in milliseconds.
pub fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_millis() as i64
}
