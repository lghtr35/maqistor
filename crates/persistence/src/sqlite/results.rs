use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::mpsc::sync_channel;
use std::thread;
use std::time::{Duration, Instant};

use rusqlite::{Connection, ToSql, TransactionBehavior, params, params_from_iter};
use tokio::sync::{mpsc, oneshot};
use tracing::debug;

use maqistor_engine::{Execution, JobOutcome, JobQueue, StoreError};

use super::adaptive::{
    AdaptiveBatchController, FlushReason, ResultsLane, ResultsLaneController,
};
use super::bulk::{self, ROWS_PER_STATEMENT};
use super::common::{
    EXECUTION_WITH_QUEUE_COLUMNS, EXECUTION_WITH_QUEUE_FROM, ReadPool, RwConnection,
    apply_executions_schema, row_to_execution_with_queue_config, row_to_queue, unix_now,
};
use super::options::{DurabilityMode, SqliteWriteOptions};

const CHANNEL_CAPACITY: usize = 1024;

const EXECUTIONS_UPSERT_COLUMNS: &[&str] = &[
    "job_id",
    "queue_name",
    "status",
    "execution_count",
    "lease_expires_at",
    "dispatch_id",
    "created_at",
    "updated_at",
];

#[derive(Debug, Clone)]
pub(crate) struct DispatchInsert {
    pub job_id: i64,
    pub queue_name: String,
    pub dispatch_id: String,
    pub lease_expires_at: i64,
}

#[derive(Debug, Clone)]
pub(crate) struct DispatchedExecution {
    #[allow(dead_code)]
    pub job_id: i64,
    pub execution_count: u32,
    pub dispatch_id: String,
    pub lease_expires_at: i64,
    pub updated_at: i64,
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
    UpsertQueue {
        queue: JobQueue,
        reply: oneshot::Sender<Result<JobQueue, StoreError>>,
    },
    Dispatch {
        rows: Vec<DispatchInsert>,
        reply: oneshot::Sender<Result<Vec<DispatchedExecution>, StoreError>>,
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
    CleanupExpiredRecords {
        cutoff: i64,
        reply: oneshot::Sender<Result<usize, StoreError>>,
    }
}

struct PendingCompletion {
    job_id: i64,
    dispatch_id: String,
    outcome: JobOutcome,
    reply: oneshot::Sender<Result<CompletionDisposition, StoreError>>,
    queued_at: Instant,
}

struct PendingDispatch {
    rows: Vec<DispatchInsert>,
    reply: oneshot::Sender<Result<Vec<DispatchedExecution>, StoreError>>,
    queued_at: Instant,
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
        rw.migrate_schema(apply_executions_schema)?;
        Ok(Self { conn: rw.conn })
    }

    fn upsert_queue(&mut self, queue: JobQueue) -> Result<JobQueue, StoreError> {
        let mut queue = queue;
        queue.updated_at = unix_now();
        self.conn
            .execute(
                "INSERT INTO execution_queues (name, max_retries, timeout_secs, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(name) DO UPDATE SET
                    max_retries = excluded.max_retries,
                    timeout_secs = excluded.timeout_secs,
                    updated_at = excluded.updated_at",
                params![
                    queue.name,
                    queue.max_retries,
                    queue.timeout_secs,
                    queue.created_at,
                    queue.updated_at,
                ],
            )
            .map_err(|err| StoreError::Internal(err.to_string()))?;
        self.conn
            .query_row(
                "SELECT name, max_retries, timeout_secs, created_at, updated_at
                 FROM execution_queues WHERE name = ?1",
                params![queue.name],
                row_to_queue,
            )
            .map_err(|err| StoreError::Internal(err.to_string()))
    }

    fn dispatch_batch(
        &mut self,
        rows: Vec<DispatchInsert>,
    ) -> Result<Vec<DispatchedExecution>, StoreError> {
        if rows.is_empty() {
            return Ok(Vec::new());
        }
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|err| StoreError::Internal(err.to_string()))?;
        let now = unix_now();
        let status = "running";
        let execution_count = 1_i64;
        let mut by_job: std::collections::HashMap<i64, DispatchedExecution> =
            std::collections::HashMap::with_capacity(rows.len());

        for chunk in rows.chunks(ROWS_PER_STATEMENT) {
            let mut values: Vec<&dyn ToSql> = Vec::with_capacity(chunk.len() * 8);
            for row in chunk {
                values.push(&row.job_id);
                values.push(&row.queue_name);
                values.push(&status);
                values.push(&execution_count);
                values.push(&row.lease_expires_at);
                values.push(&row.dispatch_id);
                values.push(&now);
                values.push(&now);
            }
            let sql = bulk::upsert_sql(
                "executions",
                EXECUTIONS_UPSERT_COLUMNS,
                chunk.len(),
                "job_id",
                "status = 'running', \
                 execution_count = execution_count + 1, \
                 dispatch_id = excluded.dispatch_id, \
                 lease_expires_at = excluded.lease_expires_at, \
                 result_payload = NULL, \
                 result_error = NULL, \
                 updated_at = excluded.updated_at",
                Some("executions.status IN ('failed', 'pending')"),
                Some("job_id, execution_count, dispatch_id, updated_at"),
            );
            let upserted = bulk::query_pairs_cached_tx(
                &tx,
                &sql,
                params_from_iter(values),
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, i64>(3)?,
                    ))
                },
            )?;
            let lease_by_id: std::collections::HashMap<i64, i64> = chunk
                .iter()
                .map(|row| (row.job_id, row.lease_expires_at))
                .collect();
            for (job_id, execution_count, dispatch_id, updated_at) in upserted {
                let execution_count = u32::try_from(execution_count)
                    .map_err(|err| StoreError::Internal(err.to_string()))?;
                let lease_expires_at = lease_by_id.get(&job_id).copied().unwrap_or(0);
                by_job.insert(
                    job_id,
                    DispatchedExecution {
                        job_id,
                        execution_count,
                        dispatch_id,
                        lease_expires_at,
                        updated_at,
                    },
                );
            }
        }

        tx.commit()
            .map_err(|err| StoreError::Internal(err.to_string()))?;

        let mut out = Vec::with_capacity(rows.len());
        for row in &rows {
            let Some(dispatched) = by_job.remove(&row.job_id) else {
                return Err(StoreError::Internal(format!(
                    "dispatch did not produce an execution for job {}",
                    row.job_id
                )));
            };
            out.push(dispatched);
        }
        Ok(out)
    }

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
            let now = unix_now();

            let mut dispositions = vec![CompletionDisposition::Ignored; batch.len()];
            let mut success_idx = Vec::new();
            let mut fail_idx = Vec::new();
            for (i, pending) in batch.iter().enumerate() {
                match &pending.outcome {
                    JobOutcome::Succeeded(_) => success_idx.push(i),
                    JobOutcome::Failed(_) => fail_idx.push(i),
                }
            }

            for chunk in success_idx.chunks(ROWS_PER_STATEMENT) {
                let mut values: Vec<&dyn ToSql> = Vec::with_capacity(chunk.len() * 4);
                for &i in chunk {
                    let pending = &batch[i];
                    let JobOutcome::Succeeded(payload) = &pending.outcome else {
                        unreachable!("success partition");
                    };
                    values.push(&pending.job_id);
                    values.push(&pending.dispatch_id);
                    values.push(payload);
                    values.push(&now);
                }
                let sql = bulk::update_from_values_sql(
                    "executions",
                    "v",
                    "status = 'completed', lease_expires_at = NULL, \
                     result_payload = v.payload, result_error = NULL, updated_at = v.updated_at",
                    &["job_id", "dispatch_id", "payload", "updated_at"],
                    chunk.len(),
                    "executions.job_id = v.job_id AND executions.dispatch_id = v.dispatch_id \
                     AND executions.status = 'running'",
                    Some("executions.job_id, executions.dispatch_id"),
                );
                let updated = bulk::query_pairs_cached_tx(
                    &tx,
                    &sql,
                    params_from_iter(values),
                    |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
                )?;
                let updated: std::collections::HashSet<(i64, String)> = updated.into_iter().collect();
                for &i in chunk {
                    let pending = &batch[i];
                    if updated.contains(&(pending.job_id, pending.dispatch_id.clone())) {
                        dispositions[i] = CompletionDisposition::Completed;
                    }
                }
            }

            for chunk in fail_idx.chunks(ROWS_PER_STATEMENT) {
                let mut values: Vec<&dyn ToSql> = Vec::with_capacity(chunk.len() * 4);
                for &i in chunk {
                    let pending = &batch[i];
                    let JobOutcome::Failed(message) = &pending.outcome else {
                        unreachable!("fail partition");
                    };
                    values.push(&pending.job_id);
                    values.push(&pending.dispatch_id);
                    values.push(message);
                    values.push(&now);
                }
                let sql = bulk::update_from_values_sql(
                    "executions",
                    "v",
                    "status = 'failed', lease_expires_at = NULL, \
                     result_error = v.message, updated_at = v.updated_at",
                    &["job_id", "dispatch_id", "message", "updated_at"],
                    chunk.len(),
                    "executions.job_id = v.job_id AND executions.dispatch_id = v.dispatch_id \
                     AND executions.status = 'running'",
                    Some(
                        "executions.job_id, executions.dispatch_id, \
                         executions.execution_count, executions.queue_name",
                    ),
                );
                let updated = bulk::query_pairs_cached_tx(
                    &tx,
                    &sql,
                    params_from_iter(values),
                    |row| {
                        Ok((
                            row.get::<_, i64>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, i64>(2)?,
                            row.get::<_, String>(3)?,
                        ))
                    },
                )?;
                let mut queue_limits: std::collections::HashMap<String, i64> =
                    std::collections::HashMap::new();
                for (_, _, _, queue_name) in &updated {
                    if queue_limits.contains_key(queue_name) {
                        continue;
                    }
                    let max_retries: i64 = tx
                        .query_row(
                            "SELECT max_retries FROM execution_queues WHERE name = ?1",
                            params![queue_name],
                            |row| row.get(0),
                        )
                        .map_err(|err| StoreError::Internal(err.to_string()))?;
                    queue_limits.insert(queue_name.clone(), max_retries);
                }
                let by_key: std::collections::HashMap<(i64, String), (i64, i64)> = updated
                    .into_iter()
                    .map(|(job_id, dispatch_id, execution_count, queue_name)| {
                        let max_retries = queue_limits.get(&queue_name).copied().unwrap_or(0);
                        ((job_id, dispatch_id), (execution_count, max_retries))
                    })
                    .collect();
                for &i in chunk {
                    let pending = &batch[i];
                    dispositions[i] = match by_key.get(&(pending.job_id, pending.dispatch_id.clone()))
                    {
                        Some((execution_count, max_retries))
                            if *execution_count <= *max_retries =>
                        {
                            CompletionDisposition::Repend
                        }
                        Some(_) => CompletionDisposition::Completed,
                        None => CompletionDisposition::Ignored,
                    };
                }
            }

            tx.commit()
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            Ok(dispositions)
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
                "UPDATE executions SET status = 'failed', lease_expires_at = NULL,
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
        let sql = format!(
            "SELECT {EXECUTION_WITH_QUEUE_COLUMNS}
             {EXECUTION_WITH_QUEUE_FROM}
             WHERE e.status = 'running'
               AND e.lease_expires_at IS NOT NULL
               AND e.lease_expires_at < ?1"
        );
        let mut stmt = self
            .conn
            .prepare(&sql)
            .map_err(|err| StoreError::Internal(err.to_string()))?;
        let stale = stmt
            .query_map(params![now], row_to_execution_with_queue_config)
            .map_err(|err| StoreError::Internal(err.to_string()))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| StoreError::Internal(err.to_string()))?;
        drop(stmt);

        if stale.is_empty() {
            return Ok(Vec::new());
        }

        let limits: std::collections::HashMap<i64, u32> = stale
            .iter()
            .map(|item| (item.execution.id, item.queue.max_retries))
            .collect();
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|err| StoreError::Internal(err.to_string()))?;
        let mut recovered = Vec::with_capacity(stale.len());
        for chunk in stale.chunks(ROWS_PER_STATEMENT) {
            let ids: Vec<i64> = chunk.iter().map(|item| item.execution.id).collect();
            let mut values: Vec<&dyn ToSql> = Vec::with_capacity(ids.len() * 2);
            for id in &ids {
                values.push(id);
                values.push(&now);
            }
            let sql = bulk::update_from_values_sql(
                "executions",
                "v",
                "status = 'failed', lease_expires_at = NULL, \
                 result_error = 'lease expired', updated_at = v.updated_at",
                &["id", "updated_at"],
                ids.len(),
                "executions.id = v.id AND executions.status = 'running'",
                Some(
                    "executions.id, executions.job_id, executions.dispatch_id, \
                     executions.execution_count",
                ),
            );
            let updated = bulk::query_pairs_cached_tx(
                &tx,
                &sql,
                params_from_iter(values),
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, i64>(3)?,
                    ))
                },
            )?;
            for (id, job_id, dispatch_id, execution_count) in updated {
                let execution_count = u32::try_from(execution_count)
                    .map_err(|err| StoreError::Internal(err.to_string()))?;
                let max_retries = limits.get(&id).copied().unwrap_or(0);
                recovered.push(RecoveredStale {
                    job_id,
                    dispatch_id,
                    should_repend: execution_count <= max_retries,
                });
            }
        }
        tx.commit()
            .map_err(|err| StoreError::Internal(err.to_string()))?;
        Ok(recovered)
    }

    fn cleanup_expired_records(&mut self, cutoff: i64) -> Result<usize, StoreError> {
        let mut statement = self
            .conn
            .prepare("DELETE FROM executions WHERE updated_at < ?1")
            .map_err(|err| StoreError::Internal(err.to_string()))?;
        let rows_affected = statement.execute(params![cutoff])
            .map_err(|err| StoreError::Internal(err.to_string()))?;
        Ok(rows_affected)
    }

    fn handle(&mut self, request: ResultsRequest) {
        match request {
            ResultsRequest::UpsertQueue { queue, reply } => {
                let _ = reply.send(self.upsert_queue(queue));
            }
            ResultsRequest::Dispatch { rows, reply } => {
                let _ = reply.send(self.dispatch_batch(rows));
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
                    queued_at: Instant::now(),
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
            ResultsRequest::CleanupExpiredRecords { cutoff, reply } => {
                let _ = reply.send(self.cleanup_expired_records(cutoff));
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

    pub(crate) async fn upsert_queue(&self, queue: JobQueue) -> Result<JobQueue, StoreError> {
        self.call(|reply| ResultsRequest::UpsertQueue { queue, reply })
            .await
    }

    pub(crate) async fn dispatch(
        &self,
        rows: Vec<DispatchInsert>,
    ) -> Result<Vec<DispatchedExecution>, StoreError> {
        self.call(|reply| ResultsRequest::Dispatch { rows, reply })
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

    pub(crate) async fn execution(
        &self,
        job_id: i64,
    ) -> Result<Option<Execution>, StoreError> {
        self.reads.execution(job_id).await
    }

    pub(crate) async fn cleanup_expired_records(&self, cutoff: i64) -> Result<usize, StoreError> {
        self.call(|reply| ResultsRequest::CleanupExpiredRecords { cutoff, reply })
            .await
    }
}

struct ResultsQueues {
    meta: VecDeque<ResultsRequest>,
    dispatch: VecDeque<PendingDispatch>,
    complete: VecDeque<PendingCompletion>,
}

impl ResultsQueues {
    fn is_empty(&self) -> bool {
        self.meta.is_empty() && self.dispatch.is_empty() && self.complete.is_empty()
    }

    fn push(&mut self, request: ResultsRequest) {
        match request {
            ResultsRequest::Complete {
                job_id,
                dispatch_id,
                outcome,
                reply,
            } => self.complete.push_back(PendingCompletion {
                job_id,
                dispatch_id,
                outcome,
                reply,
                queued_at: Instant::now(),
            }),
            ResultsRequest::Dispatch { rows, reply } => self.dispatch.push_back(PendingDispatch {
                rows,
                reply,
                queued_at: Instant::now(),
            }),
            ResultsRequest::UpsertQueue { .. }
            | ResultsRequest::Abandon { .. }
            | ResultsRequest::RecoverStale { .. }
            | ResultsRequest::CleanupExpiredRecords { .. } => self.meta.push_back(request),
        }
    }

    fn dispatch_rows(&self) -> usize {
        self.dispatch.iter().map(|pending| pending.rows.len()).sum()
    }

    fn dispatch_oldest(&self) -> Option<Instant> {
        self.dispatch.front().map(|pending| pending.queued_at)
    }

    fn completion_oldest(&self) -> Option<Instant> {
        self.complete.front().map(|pending| pending.queued_at)
    }
}

async fn results_writer_loop(
    mut conn: ResultsConn,
    mut rx: mpsc::Receiver<ResultsRequest>,
    completion_options: super::options::BatchOptions,
) {
    let mut queues = ResultsQueues {
        meta: VecDeque::new(),
        dispatch: VecDeque::new(),
        complete: VecDeque::new(),
    };
    let mut controller = AdaptiveBatchController::new(&completion_options);
    let mut lane_controller = ResultsLaneController::new(completion_options.ewma_window);

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
        let dispatch_rows = queues.dispatch_rows();
        let completion_rows = queues.complete.len();
        if let Some((lane, selection_reason)) = lane_controller.select(
            dispatch_rows,
            queues.dispatch_oldest(),
            completion_rows,
            queues.completion_oldest(),
            Instant::now(),
        ) {
            debug!(
                ?lane,
                ?selection_reason,
                dispatch_rows,
                completion_rows,
                "results writer lane selected"
            );
            let disconnected = match lane {
                ResultsLane::Dispatch => {
                    run_dispatch_turn(&mut conn, &mut queues, &mut lane_controller);
                    false
                }
                ResultsLane::Completion => {
                    run_complete_turn(
                        &mut conn,
                        &mut rx,
                        &mut queues,
                        &mut controller,
                        &mut lane_controller,
                    )
                    .await
                }
            };
            if disconnected {
                flush_complete(&mut conn, &mut queues, &mut controller, rx.len());
                while let Some(request) = queues.meta.pop_front() {
                    conn.handle(request);
                }
                while let Some(pending) = queues.dispatch.pop_front() {
                    let _ = pending.reply.send(conn.dispatch_batch(pending.rows));
                }
                return;
            }
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
    lane_controller: &mut ResultsLaneController,
) -> bool {
    let mut pending = Vec::new();
    let target = controller.batch_size();
    while pending.len() < target {
        let Some(item) = queues.complete.pop_front() else {
            break;
        };
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
                    queued_at: Instant::now(),
                });
            }
            Ok(Some(request)) => {
                let preempt = matches!(
                    request,
                    ResultsRequest::UpsertQueue { .. }
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
        lane_controller.observe_success(ResultsLane::Completion, commit.count, commit.duration);
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
    lane_controller.record_turn(ResultsLane::Completion);
    disconnected
}

fn run_dispatch_turn(
    conn: &mut ResultsConn,
    queues: &mut ResultsQueues,
    lane_controller: &mut ResultsLaneController,
) {
    let Some(pending) = queues.dispatch.pop_front() else {
        return;
    };
    let rows = pending.rows.len();
    let started = Instant::now();
    let result = conn.dispatch_batch(pending.rows);
    if result.is_ok() {
        lane_controller.observe_success(ResultsLane::Dispatch, rows, started.elapsed());
    }
    let _ = pending.reply.send(result);
    lane_controller.record_turn(ResultsLane::Dispatch);
}

fn flush_complete(
    conn: &mut ResultsConn,
    queues: &mut ResultsQueues,
    controller: &mut AdaptiveBatchController,
    backlog: usize,
) {
    if !queues.complete.is_empty() {
        let batch: Vec<_> = queues.complete.drain(..).collect();
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

#[cfg(test)]
mod results_writer_tests {
    use super::*;
    use uuid::Uuid;

    fn dispatch(job_id: i64, dispatch_id: &str) -> DispatchInsert {
        DispatchInsert {
            job_id,
            queue_name: "email".into(),
            dispatch_id: dispatch_id.into(),
            lease_expires_at: unix_now() + 60_000,
        }
    }

    fn pending_completion(job_id: i64, dispatch_id: &str) -> PendingCompletion {
        let (reply, _rx) = oneshot::channel();
        PendingCompletion {
            job_id,
            dispatch_id: dispatch_id.into(),
            outcome: JobOutcome::Succeeded(Vec::new()),
            reply,
            queued_at: Instant::now(),
        }
    }

    fn pending_dispatch(job_id: i64, dispatch_id: &str) -> PendingDispatch {
        let (reply, _rx) = oneshot::channel();
        PendingDispatch {
            rows: vec![dispatch(job_id, dispatch_id)],
            reply,
            queued_at: Instant::now(),
        }
    }

    fn single_item_completion_options() -> super::super::options::BatchOptions {
        super::super::options::BatchOptions {
            batch_size_min: 1,
            batch_size_max: 1,
            batch_wait_min: Duration::from_millis(1),
            batch_wait_max: Duration::from_millis(10),
            ..super::super::options::BatchOptions::completion_defaults()
        }
    }

    fn remove_database(path: &Path) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(format!("{}-wal", path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", path.display()));
    }

    #[tokio::test]
    async fn dispatch_does_not_preempt_an_open_completion_batch() {
        let path = std::env::temp_dir().join(format!("maqistor-results-{}.db", Uuid::new_v4()));
        let mut conn = ResultsConn::open(&path, DurabilityMode::Balanced).unwrap();
        conn.upsert_queue(JobQueue::new("email")).unwrap();
        conn.dispatch_batch(vec![dispatch(1, "first")]).unwrap();

        let (completion_reply, _completion_rx) = oneshot::channel();
        let mut queues = ResultsQueues {
            meta: VecDeque::new(),
            dispatch: VecDeque::new(),
            complete: VecDeque::from([PendingCompletion {
                job_id: 1,
                dispatch_id: "first".into(),
                outcome: JobOutcome::Succeeded(Vec::new()),
                reply: completion_reply,
                queued_at: Instant::now(),
            }]),
        };
        let options = super::super::options::BatchOptions {
            batch_size_min: 2,
            batch_size_max: 2,
            batch_wait_min: Duration::from_millis(1),
            batch_wait_max: Duration::from_millis(10),
            ..super::super::options::BatchOptions::completion_defaults()
        };
        let mut batch_controller = AdaptiveBatchController::new(&options);
        let mut lane_controller = ResultsLaneController::new(options.ewma_window);
        let (tx, mut rx) = mpsc::channel(1);
        let (dispatch_reply, mut dispatch_rx) = oneshot::channel();
        tx.send(ResultsRequest::Dispatch {
            rows: vec![dispatch(2, "second")],
            reply: dispatch_reply,
        })
        .await
        .unwrap();

        assert!(
            !run_complete_turn(
                &mut conn,
                &mut rx,
                &mut queues,
                &mut batch_controller,
                &mut lane_controller,
            )
            .await
        );
        assert_eq!(queues.dispatch.len(), 1);
        assert!(matches!(
            dispatch_rx.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        let status: String = conn
            .conn
            .query_row(
                "SELECT status FROM executions WHERE job_id = 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(status, "completed");

        drop(conn);
        remove_database(&path);
    }

    #[tokio::test]
    async fn sustained_dispatch_turns_cannot_starve_a_queued_completion() {
        let path = std::env::temp_dir().join(format!("maqistor-results-{}.db", Uuid::new_v4()));
        let mut conn = ResultsConn::open(&path, DurabilityMode::Balanced).unwrap();
        conn.upsert_queue(JobQueue::new("email")).unwrap();
        conn.dispatch_batch(vec![dispatch(1, "completed")]).unwrap();

        let mut queues = ResultsQueues {
            meta: VecDeque::new(),
            dispatch: (2..=34)
                .map(|job_id| pending_dispatch(job_id, &format!("dispatch-{job_id}")))
                .collect(),
            complete: VecDeque::from([pending_completion(1, "completed")]),
        };
        let options = single_item_completion_options();
        let mut batch_controller = AdaptiveBatchController::new(&options);
        let mut lane_controller = ResultsLaneController::new(1);
        lane_controller.observe_success(ResultsLane::Dispatch, 1, Duration::from_millis(1));
        lane_controller.observe_success(ResultsLane::Completion, 1, Duration::from_millis(1));
        let now = Instant::now();
        for turn in 0..3 {
            let (lane, _) = lane_controller
                .select(
                    queues.dispatch_rows(),
                    queues.dispatch_oldest(),
                    queues.complete.len(),
                    queues.completion_oldest(),
                    now,
                )
                .unwrap();
            assert_eq!(
                lane,
                if turn < 2 {
                    ResultsLane::Completion
                } else {
                    ResultsLane::Dispatch
                }
            );
        }

        let (_tx, mut rx) = mpsc::channel(1);
        let mut completed = false;
        for _ in 0..=32 {
            let (lane, _) = lane_controller
                .select(
                    queues.dispatch_rows(),
                    queues.dispatch_oldest(),
                    queues.complete.len(),
                    queues.completion_oldest(),
                    Instant::now(),
                )
                .unwrap();
            match lane {
                ResultsLane::Dispatch => run_dispatch_turn(&mut conn, &mut queues, &mut lane_controller),
                ResultsLane::Completion => {
                    run_complete_turn(
                        &mut conn,
                        &mut rx,
                        &mut queues,
                        &mut batch_controller,
                        &mut lane_controller,
                    )
                    .await;
                    completed = true;
                    break;
                }
            }
        }
        assert!(completed, "completion should run within the liveness cap");

        let status: String = conn
            .conn
            .query_row("SELECT status FROM executions WHERE job_id = 1", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(status, "completed");

        drop(conn);
        remove_database(&path);
    }

    #[tokio::test]
    async fn sustained_completion_turns_cannot_starve_a_queued_dispatch() {
        let path = std::env::temp_dir().join(format!("maqistor-results-{}.db", Uuid::new_v4()));
        let mut conn = ResultsConn::open(&path, DurabilityMode::Balanced).unwrap();
        conn.upsert_queue(JobQueue::new("email")).unwrap();
        let completed_jobs: Vec<_> = (1..=33)
            .map(|job_id| dispatch(job_id, &format!("complete-{job_id}")))
            .collect();
        conn.dispatch_batch(completed_jobs).unwrap();

        let (dispatch_reply, dispatch_rx) = oneshot::channel();
        let mut queues = ResultsQueues {
            meta: VecDeque::new(),
            dispatch: VecDeque::from([PendingDispatch {
                rows: vec![dispatch(100, "dispatch")],
                reply: dispatch_reply,
                queued_at: Instant::now(),
            }]),
            complete: (1..=33)
                .map(|job_id| pending_completion(job_id, &format!("complete-{job_id}")))
                .collect(),
        };
        let options = single_item_completion_options();
        let mut batch_controller = AdaptiveBatchController::new(&options);
        let mut lane_controller = ResultsLaneController::new(1);
        lane_controller.observe_success(ResultsLane::Dispatch, 1, Duration::from_millis(1));
        lane_controller.observe_success(ResultsLane::Completion, 1, Duration::from_millis(1));
        let now = Instant::now();
        for _ in 0..3 {
            let (lane, _) = lane_controller
                .select(
                    queues.dispatch_rows(),
                    queues.dispatch_oldest(),
                    queues.complete.len(),
                    queues.completion_oldest(),
                    now,
                )
                .unwrap();
            assert_eq!(lane, ResultsLane::Completion);
        }

        let (_tx, mut rx) = mpsc::channel(1);
        for _ in 0..32 {
            let (lane, _) = lane_controller
                .select(
                    queues.dispatch_rows(),
                    queues.dispatch_oldest(),
                    queues.complete.len(),
                    queues.completion_oldest(),
                    Instant::now(),
                )
                .unwrap();
            assert_eq!(lane, ResultsLane::Completion);
            run_complete_turn(
                &mut conn,
                &mut rx,
                &mut queues,
                &mut batch_controller,
                &mut lane_controller,
            )
            .await;
        }
        let (lane, _) = lane_controller
            .select(
                queues.dispatch_rows(),
                queues.dispatch_oldest(),
                queues.complete.len(),
                queues.completion_oldest(),
                Instant::now(),
            )
            .unwrap();
        assert_eq!(lane, ResultsLane::Dispatch);
        run_dispatch_turn(&mut conn, &mut queues, &mut lane_controller);
        assert!(dispatch_rx.await.unwrap().is_ok());

        drop(conn);
        remove_database(&path);
    }
}
