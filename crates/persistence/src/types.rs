use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Job {
    pub id: Uuid,
    pub name: String,
    pub status: JobStatus,
    pub payload: Vec<u8>,
    pub attempt: u32,
    pub lease_expires_at: Option<i64>,
    pub created_at: i64,
    pub updated_at: i64,
}

impl Job {
    pub fn new_pending(name: impl Into<String>, payload: Vec<u8>) -> Self {
        let now = unix_now();
        Self {
            id: Uuid::new_v4(),
            name: name.into(),
            status: JobStatus::Pending,
            payload,
            attempt: 0,
            lease_expires_at: None,
            created_at: now,
            updated_at: now,
        }
    }
}

/// Registered job queue metadata persisted across restarts.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct JobQueue {
    pub name: String,
    pub concurrency: u32,
    pub max_retries: u32,
    pub created_at: i64,
    pub updated_at: i64,
}

impl JobQueue {
    pub fn new(name: impl Into<String>) -> Self {
        let now = unix_now();
        Self {
            name: name.into(),
            concurrency: 1,
            max_retries: 3,
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
            JobStatus::Pending => "pending",
            JobStatus::Running => "running",
            JobStatus::Completed => "completed",
            JobStatus::Failed => "failed",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "pending" => Some(JobStatus::Pending),
            "running" => Some(JobStatus::Running),
            "completed" => Some(JobStatus::Completed),
            "failed" => Some(JobStatus::Failed),
            _ => None,
        }
    }
}

impl fmt::Display for JobStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum StoreError {
    #[error("job not found: {0}")]
    NotFound(Uuid),
    #[error("queue not found: {0}")]
    QueueNotFound(String),
    #[error("internal store error: {0}")]
    Internal(String),
}

pub(crate) fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_secs() as i64
}
