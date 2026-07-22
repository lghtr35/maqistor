use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::mpsc::sync_channel;
use std::thread;
use std::time::{Duration, Instant};

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use tokio::sync::{mpsc, oneshot};
use tracing::debug;

use maqistor_engine::{JobOutcome, StoreError};

use super::adaptive::{AdaptiveBatchController, FlushReason};
use super::common::{
    AttemptRow, ReadPool, RwConnection, apply_results_schema, row_to_attempt, unix_now,
};
use super::options::{DurabilityMode, SqliteWriteOptions};

const CHANNEL_CAPACITY: usize = 1024;

#[derive(Debug, Clone)]
pub(crate) struct RunningInsert {
    pub job_id: i64,
    pub queue_name: String,
    pub dispatch_id: String,
    pub execution_count: u32,
    pub max_retries_at_claim: u32,
    pub lease_expires_at: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CompletionDisposition {
    Ignored,
    Completed,
    Repend,
}

pub(crate) struct RecoveredStale {
    pub job_id: i64,
    pub dispatch_id: String,
    pub should_repend: bool,
}

enum ResultsRequest {
    InsertRunning {
        rows: Vec<RunningInsert>,
        reply: oneshot::Sender<Result<(), StoreError>>,
    },
    Complete {
        job_id: i64,
        dispatch_id: String,
        outcome: JobOutcome,
        reply: oneshot::Sender<Result<CompletionDisposition, StoreError>>,
    },
    Abandon {
        job_id: i64,
        dispatch_id: String,
        reply: oneshot::Sender<Result<(), StoreError>>,
    },
    RecoverStale {
        now: i64,
        reply: oneshot::Sender<Result<Vec<RecoveredStale>, StoreError>>,
    },
}

struct PendingCompletion {
    job_id: i64,
    dispatch_id: String,
    outcome: JobOutcome,
    reply: oneshot::Sender<Result<CompletionDisposition, StoreError>>,
}

struct BatchCommit {
    count: usize,
    duration: Duration,
}

struct ResultsConn {
    conn: Connection,
}

impl ResultsConn {
    fn open(path: &Path, durability: DurabilityMode) -> Result<Self, StoreError> {
        let rw = RwConnection::open(path, durability)?;
        rw.migrate_schema(apply_results_schema)?;
        Ok(Self { conn: rw.conn })
    }

    fn insert_running_batch(&mut self, rows: Vec<RunningInsert>) -> Result<(), StoreError> {
        if rows.is_empty() {
            return Ok(());
        }
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|err| StoreError::Internal(err.to_string()))?;
        let now = unix_now();
        let mut insert = tx
            .prepare(
                "INSERT INTO job_attempts (
                    job_id, queue_name, status, execution_count, max_retries_at_claim,
                    lease_expires_at, dispatch_id, created_at, updated_at
                 ) VALUES (?1, ?2, 'running', ?3, ?4, ?5, ?6, ?7, ?7)",
            )
            .map_err(|err| StoreError::Internal(err.to_string()))?;
        for row in rows {
            insert.execute(
                params![
                    row.job_id,
                    row.queue_name,
                    row.execution_count,
                    row.max_retries_at_claim,
                    row.lease_expires_at,
                    row.dispatch_id,
                    now,
                ],
            )
            .map_err(|err| StoreError::Internal(err.to_string()))?;
        }
        drop(insert);
        tx.commit()
            .map_err(|err| StoreError::Internal(err.to_string()))?;
        Ok(())
    }
}

impl ResultsConn {
    fn complete_batch(&mut self, batch: Vec<PendingCompletion>) -> Option<BatchCommit> {
        if batch.is_empty() {
            return None;
        }
        let started = Instant::now();
        let count = batch.len();
        let result = (|| -> Result<Vec<CompletionDisposition>, StoreError> {
            let tx = self
                .conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            let mut complete_success = tx
                .prepare(
                    "UPDATE job_attempts SET status = 'completed', lease_expires_at = NULL,
                     result_payload = ?1, result_error = NULL, updated_at = ?2
                     WHERE job_id = ?3 AND dispatch_id = ?4 AND status = 'running'",
                )
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            let mut complete_failure = tx
                .prepare(
                    "UPDATE job_attempts SET status = 'failed', lease_expires_at = NULL,
                     result_error = ?1, updated_at = ?2
                     WHERE job_id = ?3 AND dispatch_id = ?4 AND status = 'running'
                     RETURNING execution_count, max_retries_at_claim",
                )
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            let now = unix_now();
            let mut results = Vec::with_capacity(batch.len());
            for pending in &batch {
                let result = match &pending.outcome {
                    JobOutcome::Succeeded(payload) => {
                        let updated = complete_success
                            .execute(params![payload, now, pending.job_id, pending.dispatch_id])
                            .map_err(|err| StoreError::Internal(err.to_string()))?;
                        if updated == 0 {
                            CompletionDisposition::Ignored
                        } else {
                            CompletionDisposition::Completed
                        }
                    }
                    JobOutcome::Failed(message) => {
                        let attempt = complete_failure
                            .query_row(
                                params![message, now, pending.job_id, pending.dispatch_id],
                                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
                            )
                            .optional()
                            .map_err(|err| StoreError::Internal(err.to_string()))?;
                        match attempt {
                            Some((execution_count, max_retries)) if execution_count <= max_retries => {
                                CompletionDisposition::Repend
                            }
                            Some(_) => CompletionDisposition::Completed,
                            None => CompletionDisposition::Ignored,
                        }
                    }
                };
                results.push(result);
            }
            drop(complete_failure);
            drop(complete_success);
            tx.commit()
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            Ok(results)
        })();
        match result {
            Ok(results) => {
                for (pending, result) in batch.into_iter().zip(results) {
                    let _ = pending.reply.send(Ok(result));
                }
                Some(BatchCommit {
                    count,
                    duration: started.elapsed(),
                })
            }
            Err(error) => {
                for pending in batch {
                    let _ = pending.reply.send(Err(error.clone()));
                }
                None
            }
        }
    }

    fn abandon(&mut self, job_id: i64, dispatch_id: &str) -> Result<(), StoreError> {
        self.conn
            .execute(
                "UPDATE job_attempts SET status = 'failed', lease_expires_at = NULL,
                 result_error = 'abandoned', updated_at = ?1
                 WHERE job_id = ?2 AND dispatch_id = ?3 AND status = 'running'",
                params![unix_now(), job_id, dispatch_id],
            )
            .map_err(|err| StoreError::Internal(err.to_string()))?;
        Ok(())
    }

    fn recover_stale(
        &mut self,
        now: i64,
    ) -> Result<Vec<RecoveredStale>, StoreError> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT id, job_id, queue_name, status, execution_count, max_retries_at_claim,
                        lease_expires_at, dispatch_id, result_payload, result_error, created_at, updated_at
                 FROM job_attempts
                 WHERE status = 'running'
                   AND lease_expires_at IS NOT NULL
                   AND lease_expires_at < ?1",
            )
            .map_err(|err| StoreError::Internal(err.to_string()))?;
        let stale = stmt
            .query_map(params![now], row_to_attempt)
            .map_err(|err| StoreError::Internal(err.to_string()))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| StoreError::Internal(err.to_string()))?;

        let mut recovered = Vec::with_capacity(stale.len());
        for attempt in stale {
            let should_repend = attempt.execution_count <= attempt.max_retries_at_claim;
            self.conn
                .execute(
                    "UPDATE job_attempts SET status = 'failed', lease_expires_at = NULL,
                     result_error = 'lease expired', updated_at = ?1
                     WHERE id = ?2 AND status = 'running'",
                    params![now, attempt.id],
                )
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            recovered.push(RecoveredStale {
                job_id: attempt.job_id,
                dispatch_id: attempt.dispatch_id,
                should_repend,
            });
        }
        Ok(recovered)
    }

    fn handle(&mut self, request: ResultsRequest) {
        match request {
            ResultsRequest::InsertRunning { rows, reply } => {
                let _ = reply.send(self.insert_running_batch(rows));
            }
            ResultsRequest::Complete {
                job_id,
                dispatch_id,
                outcome,
                reply,
            } => {
                let _ = self.complete_batch(vec![PendingCompletion {
                    job_id,
                    dispatch_id,
                    outcome,
                    reply,
                }]);
            }
            ResultsRequest::Abandon {
                job_id,
                dispatch_id,
                reply,
            } => {
                let _ = reply.send(self.abandon(job_id, &dispatch_id));
            }
            ResultsRequest::RecoverStale { now, reply } => {
                let _ = reply.send(self.recover_stale(now));
            }
        }
    }
}

#[derive(Clone)]
pub(crate) struct ResultsHandle {
    tx: mpsc::Sender<ResultsRequest>,
    pub(crate) reads: ReadPool,
    path: PathBuf,
}

impl ResultsHandle {
    pub(crate) fn open(path: PathBuf, options: &SqliteWriteOptions) -> Result<Self, StoreError> {
        let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
        let writer_path = path.clone();
        let durability = options.durability;
        let completion_options = options.completion.clone();
        let (ready_tx, ready_rx) = sync_channel(1);
        thread::Builder::new()
            .name("maqistor-sqlite-results".into())
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
                    let conn = match ResultsConn::open(&writer_path, durability) {
                        Ok(conn) => conn,
                        Err(err) => {
                            let _ = ready_tx.send(Err(err));
                            return;
                        }
                    };
                    if ready_tx.send(Ok(())).is_ok() {
                        results_writer_loop(conn, rx, completion_options).await;
                    }
                });
            })
            .map_err(|err| StoreError::Internal(err.to_string()))?;
        ready_rx
            .recv()
            .map_err(|_| StoreError::Internal("results writer failed to start".into()))??;
        let reads = ReadPool::open_results(&path)?;
        Ok(Self { tx, reads, path })
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    async fn call<T>(
        &self,
        build: impl FnOnce(oneshot::Sender<Result<T, StoreError>>) -> ResultsRequest,
    ) -> Result<T, StoreError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(build(reply))
            .await
            .map_err(|_| StoreError::Internal("results writer stopped".into()))?;
        rx.await
            .map_err(|_| StoreError::Internal("results writer dropped reply".into()))?
    }

    pub(crate) async fn insert_running(&self, rows: Vec<RunningInsert>) -> Result<(), StoreError> {
        self.call(|reply| ResultsRequest::InsertRunning { rows, reply })
            .await
    }

    pub(crate) async fn complete(
        &self,
        job_id: i64,
        dispatch_id: &str,
        outcome: JobOutcome,
    ) -> Result<CompletionDisposition, StoreError> {
        let dispatch_id = dispatch_id.to_string();
        self.call(|reply| ResultsRequest::Complete {
            job_id,
            dispatch_id,
            outcome,
            reply,
        })
        .await
    }


    pub(crate) async fn abandon(&self, job_id: i64, dispatch_id: &str) -> Result<(), StoreError> {
        let dispatch_id = dispatch_id.to_string();
        self.call(|reply| ResultsRequest::Abandon {
            job_id,
            dispatch_id,
            reply,
        })
        .await
    }

    pub(crate) async fn recover_stale(
        &self,
        now: i64,
    ) -> Result<Vec<RecoveredStale>, StoreError> {
        self.call(|reply| ResultsRequest::RecoverStale { now, reply })
        .await
    }

    pub(crate) async fn latest_attempt(&self, job_id: i64) -> Result<Option<AttemptRow>, StoreError> {
        self.reads.latest_attempt(job_id).await
    }
}

struct ResultsQueues {
    meta: VecDeque<ResultsRequest>,
    insert: VecDeque<ResultsRequest>,
    complete: VecDeque<PendingCompletion>,
    complete_wait_since: Option<Instant>,
}

impl ResultsQueues {
    fn is_empty(&self) -> bool {
        self.meta.is_empty() && self.insert.is_empty() && self.complete.is_empty()
    }

    fn push(&mut self, request: ResultsRequest) {
        match request {
            ResultsRequest::Complete {
                job_id,
                dispatch_id,
                outcome,
                reply,
            } => {
                if self.complete.is_empty() {
                    self.complete_wait_since = Some(Instant::now());
                }
                self.complete.push_back(PendingCompletion {
                    job_id,
                    dispatch_id,
                    outcome,
                    reply,
                });
            }
            ResultsRequest::InsertRunning { .. } => self.insert.push_back(request),
            ResultsRequest::Abandon { .. }
            | ResultsRequest::RecoverStale { .. } => self.meta.push_back(request),
        }
    }
}

async fn results_writer_loop(
    mut conn: ResultsConn,
    mut rx: mpsc::Receiver<ResultsRequest>,
    completion_options: super::options::BatchOptions,
) {
    let mut queues = ResultsQueues {
        meta: VecDeque::new(),
        insert: VecDeque::new(),
        complete: VecDeque::new(),
        complete_wait_since: None,
    };
    let mut controller = AdaptiveBatchController::new(&completion_options);

    loop {
        if queues.is_empty() {
            match rx.recv().await {
                Some(request) => queues.push(request),
                None => break,
            }
        }
        while let Ok(request) = rx.try_recv() {
            queues.push(request);
        }

        if !queues.meta.is_empty() {
            let request = queues.meta.pop_front().unwrap();
            conn.handle(request);
            continue;
        }
        if !queues.insert.is_empty() {
            while let Some(ResultsRequest::InsertRunning { rows, reply }) =
                queues.insert.pop_front()
            {
                let _ = reply.send(conn.insert_running_batch(rows));
            }
            continue;
        }
        if !queues.complete.is_empty() {
            let disconnected =
                run_complete_turn(&mut conn, &mut rx, &mut queues, &mut controller).await;
            if disconnected {
                flush_complete(&mut conn, &mut queues, &mut controller, rx.len());
                while let Some(request) = queues.meta.pop_front() {
                    conn.handle(request);
                }
                while let Some(ResultsRequest::InsertRunning { rows, reply }) =
                    queues.insert.pop_front()
                {
                    let _ = reply.send(conn.insert_running_batch(rows));
                }
                return;
            }
            continue;
        }
    }

    flush_complete(&mut conn, &mut queues, &mut controller, 0);
    while let Some(request) = queues.meta.pop_front() {
        conn.handle(request);
    }
}

async fn run_complete_turn(
    conn: &mut ResultsConn,
    rx: &mut mpsc::Receiver<ResultsRequest>,
    queues: &mut ResultsQueues,
    controller: &mut AdaptiveBatchController,
) -> bool {
    let mut pending = Vec::new();
    let target = controller.batch_size();
    while pending.len() < target {
        let Some(item) = queues.complete.pop_front() else {
            break;
        };
        if queues.complete.is_empty() {
            queues.complete_wait_since = None;
        }
        controller.observe_request(Instant::now());
        pending.push(item);
    }
    if pending.is_empty() {
        return false;
    }

    let deadline = Instant::now() + controller.batch_wait;
    let mut disconnected = false;
    while pending.len() < target {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, rx.recv()).await {
            Ok(Some(ResultsRequest::Complete {
                job_id,
                dispatch_id,
                outcome,
                reply,
            })) => {
                controller.observe_request(Instant::now());
                pending.push(PendingCompletion {
                    job_id,
                    dispatch_id,
                    outcome,
                    reply,
                });
            }
            Ok(Some(request)) => {
                let preempt = matches!(
                    request,
                    ResultsRequest::InsertRunning { .. }
                        | ResultsRequest::Abandon { .. }
                        | ResultsRequest::RecoverStale { .. }
                );
                queues.push(request);
                if preempt {
                    break;
                }
            }
            Ok(None) => {
                disconnected = true;
                break;
            }
            Err(_) => break,
        }
    }

    let filled = pending.len();
    let reason = if filled >= target {
        FlushReason::FullBatch
    } else {
        FlushReason::Timeout
    };
    if let Some(commit) = conn.complete_batch(pending) {
        controller.record_successful_commit(
            filled.min(commit.count),
            commit.duration,
            Instant::now(),
            rx.len(),
            reason,
        );
        debug!(
            ?reason,
            filled,
            batch_size = controller.batch_size(),
            batch_wait_ms = controller.batch_wait.as_millis(),
            backlog = rx.len(),
            "adaptive results batch updated"
        );
    }
    disconnected
}

fn flush_complete(
    conn: &mut ResultsConn,
    queues: &mut ResultsQueues,
    controller: &mut AdaptiveBatchController,
    backlog: usize,
) {
    if !queues.complete.is_empty() {
        let batch: Vec<_> = queues.complete.drain(..).collect();
        queues.complete_wait_since = None;
        let filled = batch.len();
        if let Some(commit) = conn.complete_batch(batch) {
            controller.record_successful_commit(
                filled.min(commit.count),
                commit.duration,
                Instant::now(),
                backlog,
                FlushReason::Timeout,
            );
        }
    }
}
