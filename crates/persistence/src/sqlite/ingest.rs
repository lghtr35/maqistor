use std::collections::{HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::mpsc::sync_channel;
use std::thread;
use std::time::{Duration, Instant};

use rusqlite::{
    Connection, ToSql, TransactionBehavior, params, params_from_iter,
};
use tokio::sync::{mpsc, oneshot};
use tracing::debug;

use maqistor_engine::{AcceptedJob, JobQueue, MAX_CLAIM_BATCH_SIZE, StoreError};

use super::adaptive::{AdaptiveBatchController, FlushReason};
use super::bulk::{self, ROWS_PER_STATEMENT};
use super::common::{
    ReadPool, RwConnection, apply_acceptance_schema, new_dispatch_id, row_to_queue, unix_now,
};
use super::options::{DurabilityMode, SqliteWriteOptions};

const CHANNEL_CAPACITY: usize = 1024;

const JOBS_INSERT_COLUMNS: &[&str] = &[
    "queue_name",
    "payload",
    "created_at",
    "updated_at",
];

#[derive(Debug, Clone)]
pub(crate) struct IngestClaimed {
    pub id: i64,
    pub name: String,
    pub payload: Vec<u8>,
    pub timeout_secs: u64,
    pub dispatch_id: String,
    pub created_at: i64,
}

enum IngestRequest {
    UpsertQueue {
        queue: JobQueue,
        reply: oneshot::Sender<Result<JobQueue, StoreError>>,
    },
    Enqueue {
        job: AcceptedJob,
        reply: oneshot::Sender<Result<AcceptedJob, StoreError>>,
    },
    ClaimBatch {
        queue_name: String,
        limit: usize,
        reply: oneshot::Sender<Result<Vec<IngestClaimed>, StoreError>>,
    },
    Repend {
        job_id: i64,
        dispatch_id: String,
        reply: oneshot::Sender<Result<(), StoreError>>,
    },
    CleanupExpiredRecords {
        cutoff: i64,
        reply: oneshot::Sender<Result<usize, StoreError>>,
    },
}

struct PendingEnqueue {
    job: AcceptedJob,
    reply: oneshot::Sender<Result<AcceptedJob, StoreError>>,
}

struct BatchCommit {
    inserted: usize,
    duration: Duration,
}

struct IngestConn {
    conn: Connection,
}

impl IngestConn {
    fn open(path: &Path, durability: DurabilityMode) -> Result<Self, StoreError> {
        let rw = RwConnection::open(path, durability)?;
        rw.migrate_schema(apply_acceptance_schema)?;
        Ok(Self { conn: rw.conn })
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

    fn upsert_queue(&mut self, queue: JobQueue) -> Result<JobQueue, StoreError> {
        let mut queue = queue;
        queue.updated_at = unix_now();
        self.conn
            .execute(
                "INSERT INTO job_queues (name, max_retries, timeout_secs, created_at, updated_at)
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
                 FROM job_queues WHERE name = ?1",
                params![queue.name],
                row_to_queue,
            )
            .map_err(|err| StoreError::Internal(err.to_string()))
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
            if queue_names.contains(&pending.job.queue_name) {
                to_insert.push(pending);
            } else {
                let _ = pending
                    .reply
                    .send(Err(StoreError::QueueNotFound(pending.job.queue_name)));
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

            for chunk in to_insert.chunks_mut(ROWS_PER_STATEMENT) {
                let mut values: Vec<&dyn ToSql> = Vec::with_capacity(chunk.len() * 4);
                for pending in chunk.iter() {
                    values.push(&pending.job.queue_name);
                    values.push(&pending.job.payload);
                    values.push(&pending.job.created_at);
                    values.push(&pending.job.updated_at);
                }
                bulk::execute_cached_tx(
                    &tx,
                    &bulk::insert_sql("accepted_jobs", JOBS_INSERT_COLUMNS, chunk.len()),
                    params_from_iter(values),
                )?;
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

    fn claim_batch(
        &mut self,
        queue_name: &str,
        limit: usize,
        queue_names: &HashSet<String>,
    ) -> Result<Vec<IngestClaimed>, StoreError> {
        if !queue_names.contains(queue_name) {
            return Err(StoreError::QueueNotFound(queue_name.to_string()));
        }
        let limit = limit.clamp(1, MAX_CLAIM_BATCH_SIZE);
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|err| StoreError::Internal(err.to_string()))?;
        struct PendingClaim {
            id: i64,
            name: String,
            payload: Vec<u8>,
            created_at: i64,
            timeout_secs: i64,
        }
        let pending: Vec<PendingClaim> = {
            let mut statement = tx
                .prepare(
                    "SELECT accepted_jobs.id, accepted_jobs.queue_name, accepted_jobs.payload,
                            accepted_jobs.created_at, job_queues.timeout_secs
                     FROM accepted_jobs JOIN job_queues ON job_queues.name = accepted_jobs.queue_name
                     WHERE accepted_jobs.queue_name = ?1 AND accepted_jobs.dispatch_id IS NULL
                     ORDER BY accepted_jobs.created_at ASC, accepted_jobs.id ASC LIMIT ?2",
                )
                .map_err(|err| StoreError::Internal(err.to_string()))?;
            statement
                .query_map(params![queue_name, limit as i64], |row| {
                    Ok(PendingClaim {
                        id: row.get(0)?,
                        name: row.get(1)?,
                        payload: row.get(2)?,
                        created_at: row.get(3)?,
                        timeout_secs: row.get(4)?,
                    })
                })
                .map_err(|err| StoreError::Internal(err.to_string()))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|err| StoreError::Internal(err.to_string()))?
        };
        let now = unix_now();
        let mut claimed = Vec::with_capacity(pending.len());
        for chunk in pending.chunks(ROWS_PER_STATEMENT) {
            let dispatch_ids: Vec<String> =
                (0..chunk.len()).map(|_| new_dispatch_id()).collect();
            let mut values: Vec<&dyn ToSql> = Vec::with_capacity(chunk.len() * 3);
            for (row, dispatch_id) in chunk.iter().zip(dispatch_ids.iter()) {
                values.push(&row.id);
                values.push(dispatch_id);
                values.push(&now);
            }
            let sql = bulk::update_from_values_sql(
                "accepted_jobs",
                "v",
                "dispatch_id = v.dispatch_id, updated_at = v.updated_at",
                &["id", "dispatch_id", "updated_at"],
                chunk.len(),
                "accepted_jobs.id = v.id AND accepted_jobs.dispatch_id IS NULL",
                Some("accepted_jobs.id, accepted_jobs.dispatch_id"),
            );
            let updated = bulk::query_pairs_cached_tx(&tx, &sql, params_from_iter(values), |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
            })?;
            let by_id: std::collections::HashMap<i64, String> =
                updated.into_iter().collect();
            for row in chunk {
                let Some(dispatch_id) = by_id.get(&row.id) else {
                    continue;
                };
                let timeout_secs = u64::try_from(row.timeout_secs)
                    .map_err(|err| StoreError::Internal(err.to_string()))?;
                claimed.push(IngestClaimed {
                    id: row.id,
                    name: row.name.clone(),
                    payload: row.payload.clone(),
                    timeout_secs,
                    dispatch_id: dispatch_id.clone(),
                    created_at: row.created_at,
                });
            }
        }
        tx.commit()
            .map_err(|err| StoreError::Internal(err.to_string()))?;
        Ok(claimed)
    }

    fn repend(&mut self, job_id: i64, dispatch_id: &str) -> Result<(), StoreError> {
        self.repend_batch(&[(job_id, dispatch_id)])
    }

    fn repend_batch(&mut self, pairs: &[(i64, &str)]) -> Result<(), StoreError> {
        if pairs.is_empty() {
            return Ok(());
        }
        let now = unix_now();
        for chunk in pairs.chunks(ROWS_PER_STATEMENT) {
            let mut values: Vec<&dyn ToSql> = Vec::with_capacity(chunk.len() * 3);
            for (job_id, dispatch_id) in chunk {
                values.push(job_id);
                values.push(dispatch_id);
                values.push(&now);
            }
            let sql = bulk::update_from_values_sql(
                "accepted_jobs",
                "v",
                "dispatch_id = NULL, updated_at = v.updated_at",
                &["id", "dispatch_id", "updated_at"],
                chunk.len(),
                "accepted_jobs.id = v.id AND accepted_jobs.dispatch_id = v.dispatch_id",
                None,
            );
            bulk::execute_cached(&self.conn, &sql, params_from_iter(values))?;
        }
        Ok(())
    }

    fn cleanup_expired_records(&mut self, cutoff: i64) -> Result<usize, StoreError> {
        let mut statement = self
            .conn
            .prepare("DELETE FROM accepted_jobs WHERE created_at < ?1")
            .map_err(|err| StoreError::Internal(err.to_string()))?;
        let rows_affected = statement.execute(params![cutoff])
            .map_err(|err| StoreError::Internal(err.to_string()))?;
        Ok(rows_affected)
    }

    fn handle(&mut self, request: IngestRequest, queue_names: &mut HashSet<String>) {
        match request {
            IngestRequest::UpsertQueue { queue, reply } => {
                let name = queue.name.clone();
                let result = self.upsert_queue(queue);
                if result.is_ok() {
                    queue_names.insert(name);
                }
                let _ = reply.send(result);
            }
            IngestRequest::Enqueue { job, reply } => {
                let _ = self.enqueue_batch(vec![PendingEnqueue { job, reply }], queue_names);
            }
            IngestRequest::ClaimBatch {
                queue_name,
                limit,
                reply,
            } => {
                let _ = reply.send(self.claim_batch(&queue_name, limit, queue_names));
            }
            IngestRequest::Repend {
                job_id,
                dispatch_id,
                reply,
            } => {
                let _ = reply.send(self.repend(job_id, &dispatch_id));
            }
            IngestRequest::CleanupExpiredRecords { cutoff, reply } => {
                let _ = reply.send(self.cleanup_expired_records(cutoff));
            }
        }
    }
}

#[derive(Clone)]
pub(crate) struct IngestHandle {
    tx: mpsc::Sender<IngestRequest>,
    pub(crate) reads: ReadPool,
}

impl IngestHandle {
    pub(crate) fn open(path: PathBuf, options: &SqliteWriteOptions) -> Result<Self, StoreError> {
        let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
        let writer_path = path.clone();
        let durability = options.durability;
        let enqueue_options = options.enqueue.clone();
        let (ready_tx, ready_rx) = sync_channel(1);
        thread::Builder::new()
            .name("maqistor-sqlite-ingest".into())
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
                    let conn = match IngestConn::open(&writer_path, durability) {
                        Ok(conn) => conn,
                        Err(err) => {
                            let _ = ready_tx.send(Err(err));
                            return;
                        }
                    };
                    let queue_names = match conn.queue_names() {
                        Ok(names) => names,
                        Err(err) => {
                            let _ = ready_tx.send(Err(err));
                            return;
                        }
                    };
                    if ready_tx.send(Ok(())).is_ok() {
                        ingest_writer_loop(conn, rx, enqueue_options, queue_names).await;
                    }
                });
            })
            .map_err(|err| StoreError::Internal(err.to_string()))?;
        ready_rx
            .recv()
            .map_err(|_| StoreError::Internal("ingest writer failed to start".into()))??;
        let reads = ReadPool::open_ingest(&path)?;
        Ok(Self { tx, reads })
    }

    async fn call<T>(
        &self,
        build: impl FnOnce(oneshot::Sender<Result<T, StoreError>>) -> IngestRequest,
    ) -> Result<T, StoreError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(build(reply))
            .await
            .map_err(|_| StoreError::Internal("ingest writer stopped".into()))?;
        rx.await
            .map_err(|_| StoreError::Internal("ingest writer dropped reply".into()))?
    }

    pub(crate) async fn upsert_queue(&self, queue: JobQueue) -> Result<JobQueue, StoreError> {
        self.call(|reply| IngestRequest::UpsertQueue { queue, reply })
            .await
    }

    pub(crate) async fn enqueue(&self, job: AcceptedJob) -> Result<AcceptedJob, StoreError> {
        self.call(|reply| IngestRequest::Enqueue { job, reply }).await
    }

    pub(crate) async fn claim_batch(
        &self,
        queue_name: &str,
        limit: usize,
    ) -> Result<Vec<IngestClaimed>, StoreError> {
        let queue_name = queue_name.to_string();
        self.call(|reply| IngestRequest::ClaimBatch {
            queue_name,
            limit,
            reply,
        })
        .await
    }

    pub(crate) async fn repend(&self, job_id: i64, dispatch_id: &str) -> Result<(), StoreError> {
        let dispatch_id = dispatch_id.to_string();
        self.call(|reply| IngestRequest::Repend {
            job_id,
            dispatch_id,
            reply,
        })
        .await
    }


    pub(crate) async fn accepted_row(&self, job_id: i64) -> Result<AcceptedJob, StoreError> {
        self.reads.accepted_job(job_id).await
    }

    pub(crate) async fn cleanup_expired_records(&self, cutoff: i64) -> Result<usize, StoreError> {
        self.call(|reply| IngestRequest::CleanupExpiredRecords { cutoff, reply })
            .await
    }
}

struct IngestQueues {
    meta: VecDeque<IngestRequest>,
    claim: VecDeque<IngestRequest>,
    ingest: VecDeque<PendingEnqueue>,
}

impl IngestQueues {
    fn is_empty(&self) -> bool {
        self.meta.is_empty() && self.claim.is_empty() && self.ingest.is_empty()
    }

    fn push(&mut self, request: IngestRequest) {
        match request {
            IngestRequest::Enqueue { job, reply } => {
                self.ingest.push_back(PendingEnqueue { job, reply });
            }
            IngestRequest::ClaimBatch { .. } => self.claim.push_back(request),
            IngestRequest::UpsertQueue { .. } | IngestRequest::Repend { .. } | IngestRequest::CleanupExpiredRecords { .. } => {
                self.meta.push_back(request);
            }
        }
    }
}

async fn ingest_writer_loop(
    mut conn: IngestConn,
    mut rx: mpsc::Receiver<IngestRequest>,
    enqueue_options: super::options::BatchOptions,
    mut queue_names: HashSet<String>,
) {
    let mut queues = IngestQueues {
        meta: VecDeque::new(),
        claim: VecDeque::new(),
        ingest: VecDeque::new(),
    };
    let mut controller = AdaptiveBatchController::new(&enqueue_options);

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
            conn.handle(request, &mut queue_names);
            continue;
        }
        if !queues.claim.is_empty() {
            let request = queues.claim.pop_front().unwrap();
            conn.handle(request, &mut queue_names);
            continue;
        }
        if !queues.ingest.is_empty() {
            let disconnected = run_ingest_turn(&mut conn, &mut rx, &mut queues, &mut controller, &queue_names)
                .await;
            if disconnected {
                flush_ingest(&mut conn, &mut queues, &mut controller, &queue_names, rx.len());
                while let Some(request) = queues.meta.pop_front() {
                    conn.handle(request, &mut queue_names);
                }
                while let Some(request) = queues.claim.pop_front() {
                    conn.handle(request, &mut queue_names);
                }
                return;
            }
            continue;
        }
    }

    flush_ingest(&mut conn, &mut queues, &mut controller, &queue_names, 0);
    while let Some(request) = queues.meta.pop_front() {
        conn.handle(request, &mut queue_names);
    }
    while let Some(request) = queues.claim.pop_front() {
        conn.handle(request, &mut queue_names);
    }
}

async fn run_ingest_turn(
    conn: &mut IngestConn,
    rx: &mut mpsc::Receiver<IngestRequest>,
    queues: &mut IngestQueues,
    controller: &mut AdaptiveBatchController,
    queue_names: &HashSet<String>,
) -> bool {
    let mut pending = Vec::new();
    let target = controller.batch_size();
    while pending.len() < target {
        let Some(item) = queues.ingest.pop_front() else {
            break;
        };
        controller.observe_request(Instant::now());
        pending.push(item);
    }
    if pending.is_empty() {
        return false;
    }

    let mut batch_deadline = Some(Instant::now() + controller.batch_wait);
    let mut disconnected = false;
    while pending.len() < target {
        let deadline = batch_deadline.expect("deadline set");
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, rx.recv()).await {
            Ok(Some(IngestRequest::Enqueue { job, reply })) => {
                controller.observe_request(Instant::now());
                pending.push(PendingEnqueue { job, reply });
            }
            Ok(Some(request)) => {
                let preempt = matches!(
                    request,
            IngestRequest::ClaimBatch { .. }
                | IngestRequest::UpsertQueue { .. }
                | IngestRequest::Repend { .. }
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

    let reason = if pending.len() >= target {
        FlushReason::FullBatch
    } else {
        FlushReason::Timeout
    };
    flush_pending(conn, &mut pending, &mut batch_deadline, controller, queue_names, reason, rx.len());
    disconnected
}

fn flush_ingest(
    conn: &mut IngestConn,
    queues: &mut IngestQueues,
    controller: &mut AdaptiveBatchController,
    queue_names: &HashSet<String>,
    backlog: usize,
) {
    if !queues.ingest.is_empty() {
        let mut pending: Vec<_> = queues.ingest.drain(..).collect();
        flush_pending(
            conn,
            &mut pending,
            &mut None,
            controller,
            queue_names,
            FlushReason::Timeout,
            backlog,
        );
    }
}

fn flush_pending(
    conn: &mut IngestConn,
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
            batch_size = controller.batch_size(),
            batch_wait_ms = controller.batch_wait.as_millis(),
            backlog,
            "adaptive ingest batch updated"
        );
    }
}
