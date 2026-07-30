mod adaptive;
mod types;

use std::{
    collections::{HashMap, HashSet, VecDeque},
    future::Future,
    sync::{Arc, Mutex},
};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore, mpsc},
    time::{Duration, MissedTickBehavior},
};

pub use adaptive::{AdaptiveBatch, DirectionStreak, Ewma};
pub use types::{
    AcceptedJob, Execution, ExecutionStatus, ExecutionWithQueueConfig, Job, JobQueue, StoreError,
    unix_now,
};

pub const MAX_CLAIM_BATCH_SIZE: usize = 1_048_576;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobOutcome {
    Succeeded(Vec<u8>),
    Failed(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerResult {
    pub job_id: i64,
    pub dispatch_id: String,
    pub outcome: JobOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkerEvent {
    Registered {
        queue_name: String,
    },
    Result {
        queue_name: String,
        result: WorkerResult,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error("unknown job queue: {0}")]
    UnknownQueue(String),
    #[error("job not found: {0}")]
    JobNotFound(i64),
    #[error("engine storage is unavailable")]
    Storage {
        #[source]
        source: StoreError,
    },
    #[error("failed to serialize payload: {0}")]
    Payload(String),
}

impl From<StoreError> for EngineError {
    fn from(error: StoreError) -> Self {
        match error {
            StoreError::QueueNotFound(name) => Self::UnknownQueue(name),
            StoreError::NotFound(id) => Self::JobNotFound(id),
            source @ StoreError::Internal(_) => Self::Storage { source },
        }
    }
}

pub trait DurableStore: Send + Sync {
    fn upsert_queue(
        &self,
        queue: JobQueue,
    ) -> impl Future<Output = Result<JobQueue, StoreError>> + Send;
    fn get_queue(
        &self,
        name: &str,
    ) -> impl Future<Output = Result<Option<JobQueue>, StoreError>> + Send;
    fn list_queues(&self) -> impl Future<Output = Result<Vec<JobQueue>, StoreError>> + Send;
    fn enqueue(&self, job: AcceptedJob) -> impl Future<Output = Result<AcceptedJob, StoreError>> + Send;
    fn get_job(&self, job_id: i64) -> impl Future<Output = Result<Job, StoreError>> + Send;
    fn status(&self, job_id: i64) -> impl Future<Output = Result<ExecutionStatus, StoreError>> + Send;
    fn claim_next(
        &self,
        queue_name: &str,
    ) -> impl Future<Output = Result<Option<Job>, StoreError>> + Send;
    fn claim_batch(
        &self,
        queue_name: &str,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<Job>, StoreError>> + Send {
        async move {
            let mut jobs = Vec::with_capacity(limit.min(MAX_CLAIM_BATCH_SIZE));
            for _ in 0..limit.min(MAX_CLAIM_BATCH_SIZE) {
                let Some(job) = self.claim_next(queue_name).await? else {
                    break;
                };
                jobs.push(job);
            }
            Ok(jobs)
        }
    }
    fn complete(
        &self,
        _job_id: i64,
        _dispatch_id: &str,
        _outcome: JobOutcome,
    ) -> impl Future<Output = Result<Option<Job>, StoreError>> + Send {
        async {
            Err(StoreError::Internal(
                "store does not support job completion".into(),
            ))
        }
    }
    fn complete_worker_result(
        &self,
        job_id: i64,
        dispatch_id: &str,
        outcome: JobOutcome,
    ) -> impl Future<Output = Result<bool, StoreError>> + Send {
        async move {
            Ok(matches!(
                self.complete(job_id, dispatch_id, outcome).await?,
                Some(job) if job.status == ExecutionStatus::Pending
            ))
        }
    }
    fn release_claim(
        &self,
        _job_id: i64,
        _dispatch_id: &str,
    ) -> impl Future<Output = Result<bool, StoreError>> + Send {
        async {
            Err(StoreError::Internal(
                "store does not support claim release".into(),
            ))
        }
    }
    fn recover_stale_leases(
        &self,
        now: i64,
    ) -> impl Future<Output = Result<Vec<Job>, StoreError>> + Send;
}

#[derive(Debug, thiserror::Error)]
pub enum DispatchError {
    #[error("no worker capacity is currently available")]
    NoCapacity,
    #[error("dispatcher error: {0}")]
    Internal(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueReservation {
    pub queue_name: String,
    pub count: usize,
}

pub trait DispatchPermit: Send {
    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any + Send>;
}

pub struct ReservedDispatch {
    pub queue_name: String,
    permit: Box<dyn DispatchPermit>,
}

impl ReservedDispatch {
    pub fn new(queue_name: String, permit: Box<dyn DispatchPermit>) -> Self {
        Self { queue_name, permit }
    }
    pub fn into_permit(self) -> Box<dyn DispatchPermit> {
        self.permit
    }
}

pub trait WorkerDispatcher: Send + Sync {
    fn reserve(
        &self,
        _queues: Vec<QueueReservation>,
    ) -> impl Future<Output = Result<Vec<ReservedDispatch>, DispatchError>> + Send {
        async { Ok(Vec::new()) }
    }
    fn dispatch(
        &self,
        permit: ReservedDispatch,
        job: Job,
    ) -> impl Future<Output = Result<(), DispatchError>> + Send;
    fn release(&self, _permit: ReservedDispatch) -> impl Future<Output = ()> + Send {
        async {}
    }
    fn subscribe_events(&self) -> Option<tokio::sync::broadcast::Receiver<WorkerEvent>> {
        None
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubmitJob {
    pub name: String,
    pub payload: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobView {
    pub id: i64,
    pub name: String,
    pub status: ExecutionStatus,
}

#[derive(Debug, Clone)]
pub struct DispatchOptions {
    pub batch_size_max: usize,
    pub max_delivery_in_flight: usize,
    pub idle_probe_interval: Duration,
    pub idle_probe_batch_size: usize,
}

impl Default for DispatchOptions {
    fn default() -> Self {
        Self {
            batch_size_max: 8_192,
            max_delivery_in_flight: 8_192,
            idle_probe_interval: Duration::from_secs(1),
            idle_probe_batch_size: 64,
        }
    }
}

impl DispatchOptions {
    pub fn validate(&self) -> Result<(), String> {
        if self.batch_size_max == 0 || self.batch_size_max > MAX_CLAIM_BATCH_SIZE {
            return Err(format!(
                "dispatch.batch_size_max must be in 1..={MAX_CLAIM_BATCH_SIZE}"
            ));
        }
        if self.max_delivery_in_flight == 0 {
            return Err("dispatch.max_delivery_in_flight must be greater than zero".into());
        }
        if self.idle_probe_interval.is_zero() {
            return Err("dispatch.idle_probe_interval must be greater than zero".into());
        }
        if self.idle_probe_batch_size == 0 {
            return Err("dispatch.idle_probe_batch_size must be greater than zero".into());
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct Engine<S: DurableStore, D: WorkerDispatcher> {
    store: S,
    dispatcher: D,
    scheduler: Arc<Scheduler>,
}

struct Scheduler {
    tx: mpsc::UnboundedSender<String>,
    delivery_tx: mpsc::Sender<DeliveryWork>,
    delivery_budget: Arc<Semaphore>,
    state: Mutex<SchedulerState>,
    options: DispatchOptions,
}

#[derive(Default)]
struct SchedulerState {
    scheduled: HashMap<String, QueueSchedule>,
    idle: VecDeque<String>,
    idle_set: HashSet<String>,
}

#[derive(Default)]
struct QueueSchedule {
    rerun: bool,
    retry_count: u8,
}

enum PassOutcome {
    Active,
    Idle,
    CapacityBlocked,
    Retry,
}

struct DeliveryWork {
    permit: ReservedDispatch,
    job: Job,
    _budget: OwnedSemaphorePermit,
}

impl<
    S: DurableStore + Clone + Send + Sync + 'static,
    D: WorkerDispatcher + Clone + Send + Sync + 'static,
> Engine<S, D>
{
    pub fn with_dispatcher(store: S, dispatcher: D, options: DispatchOptions) -> Self {
        options.validate().expect("invalid dispatch options");
        let (tx, rx) = mpsc::unbounded_channel();
        let (delivery_tx, delivery_rx) = mpsc::channel(options.max_delivery_in_flight);
        let engine = Self {
            store,
            dispatcher,
            scheduler: Arc::new(Scheduler {
                tx,
                delivery_tx,
                delivery_budget: Arc::new(Semaphore::new(options.max_delivery_in_flight)),
                state: Mutex::new(SchedulerState::default()),
                options,
            }),
        };
        engine.start_delivery_pump(delivery_rx);
        engine.start_scheduler(rx);
        engine
    }

    pub async fn submit(&self, job: SubmitJob) -> Result<JobView, EngineError> {
        let payload = serde_json::to_vec(&job.payload)
            .map_err(|err| EngineError::Payload(err.to_string()))?;
        let result = self
            .store
            .enqueue(AcceptedJob::new(job.name, payload))
            .await?;
        self.schedule_queue(result.queue_name.clone());
        Ok(JobView {
            id: result.id,
            name: result.queue_name,
            status: ExecutionStatus::Pending,
        })
    }

    pub async fn get_job(&self, id: i64) -> Result<JobView, EngineError> {
        let job = self.store.get_job(id).await?;
        Ok(JobView {
            id: job.id,
            name: job.name,
            status: job.status,
        })
    }

    async fn complete_worker_result(
        &self,
        job_id: i64,
        dispatch_id: &str,
        outcome: JobOutcome,
    ) -> Result<bool, EngineError> {
        Ok(self
            .store
            .complete_worker_result(job_id, dispatch_id, outcome)
            .await?)
    }

    pub fn start_result_listener(&self) {
        let Some(mut events) = self.dispatcher.subscribe_events() else {
            return;
        };
        let engine = self.clone();
        tokio::spawn(async move {
            loop {
                match events.recv().await {
                    Ok(event) => match event {
                        WorkerEvent::Registered { queue_name } => {
                            engine.schedule_queue(queue_name);
                        }
                        WorkerEvent::Result { queue_name, result } => {
                            let completion_engine = engine.clone();
                            let completion_queue = queue_name.clone();
                            tokio::spawn(async move {
                                if matches!(
                                    completion_engine
                                        .complete_worker_result(
                                            result.job_id,
                                            &result.dispatch_id,
                                            result.outcome,
                                        )
                                        .await,
                                    Ok(true)
                                ) {
                                    completion_engine.schedule_queue(completion_queue);
                                }
                            });
                            engine.schedule_queue(queue_name);
                        }
                    },
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        });
    }

    fn schedule_queue(&self, queue: String) {
        let should_send = {
            let mut state = self
                .scheduler
                .state
                .lock()
                .expect("engine scheduler lock poisoned");
            if let Some(schedule) = state.scheduled.get_mut(&queue) {
                schedule.rerun = true;
                false
            } else {
                state.idle_set.remove(&queue);
                state
                    .scheduled
                    .insert(queue.clone(), QueueSchedule::default());
                true
            }
        };
        if should_send {
            let _ = self.scheduler.tx.send(queue);
        }
    }

    fn start_scheduler(&self, mut rx: mpsc::UnboundedReceiver<String>) {
        let engine = self.clone();
        tokio::spawn(async move {
            engine.seed_idle_queues().await;
            let mut recovery = tokio::time::interval(Duration::from_secs(30));
            let mut idle_probe =
                tokio::time::interval(engine.scheduler.options.idle_probe_interval);
            idle_probe.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    Some(queue) = rx.recv() => {
                        let mut queues = HashSet::from([queue]);
                        while let Ok(queue) = rx.try_recv() { queues.insert(queue); }
                        engine.drain_pass(queues).await;
                    }
                    _ = recovery.tick() => {
                        let now = crate::unix_now();
                        if let Ok(recovered) = engine.store.recover_stale_leases(now).await {
                            for queue in recovered.into_iter().filter(|job| job.status == ExecutionStatus::Pending).map(|job| job.name) {
                                engine.schedule_queue(queue);
                            }
                        }
                    }
                    _ = idle_probe.tick() => engine.probe_idle_queues(),
                    else => break,
                }
            }
        });
    }

    async fn seed_idle_queues(&self) {
        let Ok(queues) = self.store.list_queues().await else {
            return;
        };
        let mut state = self
            .scheduler
            .state
            .lock()
            .expect("engine scheduler lock poisoned");
        for queue in queues {
            if state.idle_set.insert(queue.name.clone()) {
                state.idle.push_back(queue.name);
            }
        }
    }

    fn probe_idle_queues(&self) {
        let queues = {
            let mut state = self
                .scheduler
                .state
                .lock()
                .expect("engine scheduler lock poisoned");
            let mut queues = Vec::with_capacity(self.scheduler.options.idle_probe_batch_size);
            while queues.len() < self.scheduler.options.idle_probe_batch_size {
                let Some(queue) = state.idle.pop_front() else {
                    break;
                };
                state.idle_set.remove(&queue);
                if !state.scheduled.contains_key(&queue) {
                    queues.push(queue);
                }
            }
            queues
        };
        for queue in queues {
            self.schedule_queue(queue);
        }
    }

    fn start_delivery_pump(&self, mut rx: mpsc::Receiver<DeliveryWork>) {
        let engine = self.clone();
        tokio::spawn(async move {
            while let Some(work) = rx.recv().await {
                let engine = engine.clone();
                tokio::spawn(async move {
                    let DeliveryWork {
                        permit,
                        job,
                        _budget,
                    } = work;
                    let queue_name = job.name.clone();
                    let failed = engine
                        .dispatcher
                        .dispatch(permit, job.clone())
                        .await
                        .is_err();
                    if failed && let Some(dispatch_id) = job.dispatch_id.as_deref() {
                        let _ = engine.store.release_claim(job.id, dispatch_id).await;
                    }

                    engine.schedule_queue(queue_name);
                });
            }
        });
    }

    async fn drain_pass(&self, queues: HashSet<String>) {
        let batch_size_max = self
            .scheduler
            .options
            .batch_size_max
            .min(MAX_CLAIM_BATCH_SIZE);
        let headroom = self.scheduler.delivery_budget.available_permits();
        if headroom == 0 {
            for queue in queues {
                self.finish_pass(queue, PassOutcome::CapacityBlocked);
            }
            return;
        }
        let queue_count = queues.len().max(1);
        let per_queue = (headroom / queue_count).max(1).min(batch_size_max);
        let requests: Vec<_> = queues
            .iter()
            .map(|queue_name| QueueReservation {
                queue_name: queue_name.clone(),
                count: per_queue,
            })
            .collect();
        let Ok(permits) = self.dispatcher.reserve(requests).await else {
            for queue in queues {
                self.finish_pass(queue, PassOutcome::Retry);
            }
            return;
        };
        let mut permits_by_queue: HashMap<String, Vec<(ReservedDispatch, OwnedSemaphorePermit)>> =
            HashMap::new();
        for permit in permits {
            let queue_name = permit.queue_name.clone();
            match self.scheduler.delivery_budget.clone().try_acquire_owned() {
                Ok(budget) => permits_by_queue
                    .entry(queue_name)
                    .or_default()
                    .push((permit, budget)),
                Err(_) => self.dispatcher.release(permit).await,
            }
        }
        for queue_name in &queues {
            let Some(permits) = permits_by_queue.remove(queue_name) else {
                self.finish_pass(queue_name.clone(), PassOutcome::CapacityBlocked);
                continue;
            };
            let reserved = permits.len();
            let Ok(jobs) = self.store.claim_batch(queue_name, reserved).await else {
                for (permit, _budget) in permits {
                    self.dispatcher.release(permit).await;
                }
                self.finish_pass(queue_name.clone(), PassOutcome::Retry);
                continue;
            };
            let outcome = if jobs.len() == reserved {
                PassOutcome::Active
            } else {
                PassOutcome::Idle
            };
            let mut permits = permits.into_iter();
            for job in jobs {
                let (permit, budget) = permits
                    .next()
                    .expect("claim cannot exceed reserved permits");
                let work = DeliveryWork {
                    permit,
                    job,
                    _budget: budget,
                };
                if let Err(error) = self.scheduler.delivery_tx.try_send(work) {
                    let DeliveryWork { permit, job, .. } = match error {
                        mpsc::error::TrySendError::Full(work)
                        | mpsc::error::TrySendError::Closed(work) => work,
                    };
                    self.dispatcher.release(permit).await;
                    if let Some(dispatch_id) = job.dispatch_id.as_deref() {
                        let _ = self.store.release_claim(job.id, dispatch_id).await;
                    }
                    self.schedule_queue(job.name);
                }
            }
            for (permit, _budget) in permits {
                self.dispatcher.release(permit).await;
            }
            self.finish_pass(queue_name.clone(), outcome);
        }
    }

    fn finish_pass(&self, queue: String, outcome: PassOutcome) {
        let mut schedule_retry = None;
        let should_send = {
            let mut state = self
                .scheduler
                .state
                .lock()
                .expect("engine scheduler lock poisoned");
            let Some(schedule) = state.scheduled.get_mut(&queue) else {
                return;
            };
            match outcome {
                PassOutcome::Active => {
                    schedule.rerun = false;
                    schedule.retry_count = 0;
                    true
                }
                PassOutcome::Idle | PassOutcome::CapacityBlocked => {
                    if schedule.rerun {
                        schedule.rerun = false;
                        schedule.retry_count = 0;
                        true
                    } else {
                        state.scheduled.remove(&queue);
                        if state.idle_set.insert(queue.clone()) {
                            state.idle.push_back(queue.clone());
                        }
                        false
                    }
                }
                PassOutcome::Retry => {
                    schedule.rerun = false;
                    schedule.retry_count = schedule.retry_count.saturating_add(1).min(7);
                    let delay_ms = 10_u64.saturating_mul(1_u64 << schedule.retry_count);
                    schedule_retry = Some(Duration::from_millis(delay_ms.min(1_000)));
                    false
                }
            }
        };
        if should_send {
            let _ = self.scheduler.tx.send(queue);
        } else if let Some(delay) = schedule_retry {
            let engine = self.clone();
            tokio::spawn(async move {
                tokio::time::sleep(delay).await;
                engine.resume_retry(queue);
            });
        }
    }

    fn resume_retry(&self, queue: String) {
        let should_send = {
            let state = self
                .scheduler
                .state
                .lock()
                .expect("engine scheduler lock poisoned");
            state.scheduled.contains_key(&queue)
        };
        if should_send {
            let _ = self.scheduler.tx.send(queue);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        collections::VecDeque,
        sync::atomic::{AtomicUsize, Ordering},
    };

    #[derive(Clone)]
    struct TestStore {
        pending: Arc<Mutex<VecDeque<Job>>>,
        queues: Arc<Mutex<Vec<JobQueue>>>,
        claims: Arc<AtomicUsize>,
        releases: Arc<AtomicUsize>,
    }

    impl TestStore {
        fn with_pending(count: i64) -> Self {
            let pending = (1..=count)
                .map(|id| {
                    let mut accepted = AcceptedJob::new("email", vec![]);
                    accepted.id = id;
                    Job::from_accepted(accepted, None)
                })
                .collect();
            Self {
                pending: Arc::new(Mutex::new(pending)),
                queues: Arc::new(Mutex::new(Vec::new())),
                claims: Arc::new(AtomicUsize::new(0)),
                releases: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn with_pending_known_queue(count: i64) -> Self {
            let store = Self::with_pending(count);
            store.queues.lock().unwrap().push(JobQueue::new("email"));
            store
        }
    }

    impl DurableStore for TestStore {
        async fn upsert_queue(&self, queue: JobQueue) -> Result<JobQueue, StoreError> {
            Ok(queue)
        }

        async fn get_queue(&self, _name: &str) -> Result<Option<JobQueue>, StoreError> {
            Ok(None)
        }

        async fn list_queues(&self) -> Result<Vec<JobQueue>, StoreError> {
            Ok(self.queues.lock().unwrap().clone())
        }

        async fn enqueue(&self, job: AcceptedJob) -> Result<AcceptedJob, StoreError> {
            self.pending
                .lock()
                .unwrap()
                .push_back(Job::from_accepted(job.clone(), None));
            Ok(job)
        }

        async fn get_job(&self, job_id: i64) -> Result<Job, StoreError> {
            Err(StoreError::NotFound(job_id))
        }

        async fn status(&self, job_id: i64) -> Result<ExecutionStatus, StoreError> {
            Err(StoreError::NotFound(job_id))
        }

        async fn claim_next(&self, _queue_name: &str) -> Result<Option<Job>, StoreError> {
            let mut job = self.pending.lock().unwrap().pop_front();
            if let Some(job) = &mut job {
                job.status = ExecutionStatus::Running;
                job.execution_count = 1;
                job.dispatch_id = Some(format!("dispatch-{}", job.id));
                self.claims.fetch_add(1, Ordering::SeqCst);
            }
            Ok(job)
        }

        async fn release_claim(
            &self,
            _job_id: i64,
            _dispatch_id: &str,
        ) -> Result<bool, StoreError> {
            self.releases.fetch_add(1, Ordering::SeqCst);
            Ok(true)
        }

        async fn recover_stale_leases(&self, _now: i64) -> Result<Vec<Job>, StoreError> {
            Ok(Vec::new())
        }
    }

    struct TestPermit;

    impl DispatchPermit for TestPermit {
        fn into_any(self: Box<Self>) -> Box<dyn std::any::Any + Send> {
            self
        }
    }

    #[derive(Clone)]
    struct TestDispatcher {
        dispatches: Arc<AtomicUsize>,
        releases: Arc<AtomicUsize>,
        block_dispatch: bool,
        fail_dispatch: bool,
        unblock: Arc<tokio::sync::Notify>,
    }

    impl TestDispatcher {
        fn blocking() -> Self {
            Self {
                dispatches: Arc::new(AtomicUsize::new(0)),
                releases: Arc::new(AtomicUsize::new(0)),
                block_dispatch: true,
                fail_dispatch: false,
                unblock: Arc::new(tokio::sync::Notify::new()),
            }
        }

        fn failing() -> Self {
            Self {
                dispatches: Arc::new(AtomicUsize::new(0)),
                releases: Arc::new(AtomicUsize::new(0)),
                block_dispatch: false,
                fail_dispatch: true,
                unblock: Arc::new(tokio::sync::Notify::new()),
            }
        }
    }

    impl WorkerDispatcher for TestDispatcher {
        async fn reserve(
            &self,
            queues: Vec<QueueReservation>,
        ) -> Result<Vec<ReservedDispatch>, DispatchError> {
            Ok(queues
                .into_iter()
                .flat_map(|request| {
                    (0..request.count).map(move |_| {
                        ReservedDispatch::new(request.queue_name.clone(), Box::new(TestPermit))
                    })
                })
                .collect())
        }

        async fn dispatch(
            &self,
            _permit: ReservedDispatch,
            _job: Job,
        ) -> Result<(), DispatchError> {
            self.dispatches.fetch_add(1, Ordering::SeqCst);
            if self.block_dispatch {
                self.unblock.notified().await;
            }
            if self.fail_dispatch {
                Err(DispatchError::Internal("worker writer failed".into()))
            } else {
                Ok(())
            }
        }

        async fn release(&self, _permit: ReservedDispatch) {
            self.releases.fetch_add(1, Ordering::SeqCst);
        }
    }

    async fn wait_for(counter: &AtomicUsize, expected: usize) {
        tokio::time::timeout(std::time::Duration::from_millis(250), async {
            while counter.load(Ordering::SeqCst) < expected {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("background task should make progress");
    }

    fn test_options(max_delivery_in_flight: usize) -> DispatchOptions {
        DispatchOptions {
            batch_size_max: 1,
            max_delivery_in_flight,
            ..DispatchOptions::default()
        }
    }

    #[test]
    fn dispatch_options_reject_unsafe_claim_batches() {
        let options = DispatchOptions {
            batch_size_max: MAX_CLAIM_BATCH_SIZE + 1,
            ..DispatchOptions::default()
        };
        assert!(options.validate().is_err());
    }

    #[test]
    fn dispatch_options_reject_zero_idle_probe_limits() {
        let interval = DispatchOptions {
            idle_probe_interval: Duration::ZERO,
            ..DispatchOptions::default()
        };
        assert!(interval.validate().is_err());
        let batch = DispatchOptions {
            idle_probe_batch_size: 0,
            ..DispatchOptions::default()
        };
        assert!(batch.validate().is_err());
    }

    #[tokio::test]
    async fn schedule_requests_coalesce_to_one_rerun() {
        let store = TestStore::with_pending(0);
        let dispatcher = TestDispatcher::blocking();
        let engine = Engine::with_dispatcher(store, dispatcher, test_options(1));

        engine.schedule_queue("email".into());
        engine.schedule_queue("email".into());
        engine.schedule_queue("email".into());

        let state = engine.scheduler.state.lock().unwrap();
        let schedule = state.scheduled.get("email").expect("queue scheduled");
        assert!(schedule.rerun);
        assert_eq!(state.scheduled.len(), 1);
    }

    #[tokio::test]
    async fn idle_probe_discovers_pending_work_without_a_submit_event() {
        let store = TestStore::with_pending_known_queue(1);
        let dispatcher = TestDispatcher::blocking();
        let mut options = test_options(1);
        options.idle_probe_interval = Duration::from_millis(1);
        let _engine = Engine::with_dispatcher(store.clone(), dispatcher.clone(), options);

        wait_for(&store.claims, 1).await;
        wait_for(&dispatcher.dispatches, 1).await;
        dispatcher.unblock.notify_one();
    }

    #[tokio::test]
    async fn scheduler_hands_off_durable_claim_without_waiting_for_writer_ack() {
        let store = TestStore::with_pending(1);
        let dispatcher = TestDispatcher::blocking();
        let engine = Engine::with_dispatcher(store.clone(), dispatcher.clone(), test_options(1));

        tokio::time::timeout(
            std::time::Duration::from_millis(100),
            engine.drain_pass(HashSet::from(["email".to_string()])),
        )
        .await
        .expect("scheduler must not await the blocked writer");
        assert_eq!(store.claims.load(Ordering::SeqCst), 1);
        wait_for(&dispatcher.dispatches, 1).await;

        dispatcher.unblock.notify_one();
    }

    #[tokio::test]
    async fn delivery_headroom_prevents_excess_reservations() {
        let store = TestStore::with_pending(2);
        let dispatcher = TestDispatcher::blocking();
        let engine = Engine::with_dispatcher(store.clone(), dispatcher.clone(), test_options(1));
        let queues = HashSet::from(["email".to_string()]);

        engine.drain_pass(queues.clone()).await;
        wait_for(&dispatcher.dispatches, 1).await;
        engine.drain_pass(queues).await;

        assert_eq!(store.claims.load(Ordering::SeqCst), 1);
        assert_eq!(dispatcher.releases.load(Ordering::SeqCst), 0);
        dispatcher.unblock.notify_one();
    }

    #[tokio::test]
    async fn failed_delivery_releases_the_durable_claim() {
        let store = TestStore::with_pending(1);
        let dispatcher = TestDispatcher::failing();
        let engine = Engine::with_dispatcher(store.clone(), dispatcher.clone(), test_options(1));

        engine
            .drain_pass(HashSet::from(["email".to_string()]))
            .await;
        wait_for(&store.releases, 1).await;

        assert_eq!(dispatcher.dispatches.load(Ordering::SeqCst), 1);
        assert_eq!(store.releases.load(Ordering::SeqCst), 1);
    }
}
