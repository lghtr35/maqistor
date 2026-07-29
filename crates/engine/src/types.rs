use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct AcceptedJob {
    pub id: i64,
    pub queue_name: String,
    pub payload: Vec<u8>,
    pub dispatch_id: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

impl AcceptedJob {
    pub fn new(queue_name: impl Into<String>, payload: Vec<u8>) -> Self {
        let now = unix_now();
        Self {
            id: 0,
            queue_name: queue_name.into(),
            payload,
            dispatch_id: None,
            created_at: now,
            updated_at: now,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Execution {
    pub id: i64,
    pub job_id: i64,
    pub queue_name: String,
    pub status: ExecutionStatus,
    pub execution_count: u32,
    pub lease_expires_at: Option<i64>,
    pub dispatch_id: String,
    pub result_payload: Option<Vec<u8>>,
    pub result_error: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ExecutionWithQueueConfig {
    pub execution: Execution,
    pub queue: JobQueue,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Job {
    pub id: i64,
    pub name: String,
    pub status: ExecutionStatus,
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
    pub fn from_accepted(accepted: AcceptedJob, execution: Option<&Execution>) -> Self {
        let mut job = Self {
            id: accepted.id,
            name: accepted.queue_name,
            status: ExecutionStatus::Pending,
            payload: accepted.payload,
            execution_count: 0,
            lease_expires_at: None,
            dispatch_id: accepted.dispatch_id.clone(),
            result_payload: None,
            result_error: None,
            created_at: accepted.created_at,
            updated_at: accepted.updated_at,
        };

        let Some(execution) = execution else {
            return job;
        };

        job.execution_count = execution.execution_count;
        job.lease_expires_at = execution.lease_expires_at;
        job.dispatch_id = Some(execution.dispatch_id.clone());
        job.result_payload = execution.result_payload.clone();
        job.result_error = execution.result_error.clone();
        job.updated_at = execution.updated_at;
        job.status = match execution.status {
            ExecutionStatus::Failed if accepted.dispatch_id.is_none() => ExecutionStatus::Pending,
            status => status,
        };
        job
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct JobQueue {
    pub name: String,
    pub max_retries: u32,
    pub timeout_secs: u64,
    pub created_at: i64,
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
pub enum ExecutionStatus {
    Pending,
    Running,
    Completed,
    Failed,
}

impl ExecutionStatus {
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

impl fmt::Display for ExecutionStatus {
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

pub fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_millis() as i64
}
