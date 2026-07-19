use super::*;
use uuid::Uuid;

#[tokio::test]
async fn persists_queues_and_jobs_across_reopen() {
    let path = std::env::temp_dir().join(format!("maqistor-test-{}.db", Uuid::new_v4()));
    let job_id = {
        let store = SqliteStore::open(&path).expect("open store");
        store
            .upsert_queue(JobQueue::new("email"))
            .await
            .expect("upsert queue");
        let job = store
            .enqueue(Job::new_pending("email", b"payload".to_vec()))
            .await
            .expect("enqueue");
        job.id
    };

    let store = SqliteStore::open(&path).expect("reopen store");
    let queues = store.list_queues().await.expect("list queues");
    assert_eq!(queues.len(), 1);
    assert_eq!(queues[0].name, "email");

    let job = store.get_job(job_id).await.expect("get job");
    assert_eq!(job.status, JobStatus::Pending);
    assert_eq!(job.payload, b"payload");

    let reopened_job = store
        .enqueue(Job::new_pending("email", b"after-reopen".to_vec()))
        .await
        .expect("enqueue through reloaded queue cache");
    assert_eq!(
        store.get_job(reopened_job.id).await.expect("get").payload,
        b"after-reopen"
    );

    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn claim_and_recover_stale_lease() {
    let path = std::env::temp_dir().join(format!("maqistor-test-{}.db", Uuid::new_v4()));
    let store = SqliteStore::open(&path).expect("open store");
    store
        .upsert_queue(JobQueue::new("email"))
        .await
        .expect("upsert queue");
    let job = store
        .enqueue(Job::new_pending("email", vec![]))
        .await
        .expect("enqueue");

    let claimed = store
        .claim_next("email", 30)
        .await
        .expect("claim")
        .expect("claimed job");
    assert_eq!(claimed.id, job.id);
    assert_eq!(claimed.status, JobStatus::Running);

    let recovered = store
        .recover_stale_leases(claimed.lease_expires_at.unwrap() + 1)
        .await
        .expect("recover");
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].status, JobStatus::Pending);
    assert_eq!(recovered[0].attempt, 1);

    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn enqueue_rejects_unknown_queue() {
    let path = std::env::temp_dir().join(format!("maqistor-test-{}.db", Uuid::new_v4()));
    let store = SqliteStore::open(&path).expect("open store");

    let error = store
        .enqueue(Job::new_pending("missing", vec![]))
        .await
        .expect_err("unknown queue should fail");

    assert!(matches!(error, StoreError::QueueNotFound(name) if name == "missing"));

    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn concurrent_enqueues_are_durable_after_await() {
    let path = std::env::temp_dir().join(format!("maqistor-test-{}.db", Uuid::new_v4()));
    let store = SqliteStore::open(&path).expect("open store");
    store
        .upsert_queue(JobQueue::new("email"))
        .await
        .expect("upsert queue");

    let mut handles = Vec::new();
    for i in 0..50 {
        let store = store.clone();
        handles.push(tokio::spawn(async move {
            store
                .enqueue(Job::new_pending("email", format!("p{i}").into_bytes()))
                .await
        }));
    }

    let mut ids = Vec::new();
    for handle in handles {
        let job = handle.await.expect("join").expect("enqueue");
        ids.push(job.id);
    }

    for id in ids {
        let job = store.get_job(id).await.expect("get job");
        assert_eq!(job.status, JobStatus::Pending);
        assert_eq!(job.name, "email");
    }

    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(format!("{}-wal", path.display()));
    let _ = std::fs::remove_file(format!("{}-shm", path.display()));
}

#[tokio::test]
async fn jobs_use_sequential_integer_ids_with_dispatch_indexes() {
    let path = std::env::temp_dir().join(format!("maqistor-test-{}.db", Uuid::new_v4()));
    {
        let store = SqliteStore::open(&path).expect("open store");
        store
            .upsert_queue(JobQueue::new("email"))
            .await
            .expect("upsert queue");
        let first = store
            .enqueue(Job::new_pending("email", vec![]))
            .await
            .expect("first enqueue");
        let second = store
            .enqueue(Job::new_pending("email", vec![]))
            .await
            .expect("second enqueue");
        assert_eq!((first.id, second.id), (1, 2));
    }

    let conn = rusqlite::Connection::open(&path).expect("reopen database");
    let job_indexes: String = conn
        .query_row(
            "SELECT group_concat(name, ',')
             FROM (SELECT name FROM sqlite_master
                   WHERE type = 'index' AND tbl_name = 'jobs'
                   ORDER BY name)",
            [],
            |row| row.get::<_, Option<String>>(0),
        )
        .expect("read job indexes")
        .expect("dispatch indexes present");
    assert_eq!(job_indexes, "idx_jobs_queue_pending,idx_jobs_stale_leases");

    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(format!("{}-wal", path.display()));
    let _ = std::fs::remove_file(format!("{}-shm", path.display()));
}

#[tokio::test]
async fn mixed_unknown_queue_does_not_block_valid_enqueues() {
    let path = std::env::temp_dir().join(format!("maqistor-test-{}.db", Uuid::new_v4()));
    let store = SqliteStore::open(&path).expect("open store");
    store
        .upsert_queue(JobQueue::new("email"))
        .await
        .expect("upsert queue");

    let store_ok = store.clone();
    let store_bad = store.clone();
    let ok = tokio::spawn(async move {
        store_ok
            .enqueue(Job::new_pending("email", b"ok".to_vec()))
            .await
    });
    let bad = tokio::spawn(async move {
        store_bad
            .enqueue(Job::new_pending("missing", b"no".to_vec()))
            .await
    });

    let ok_job = ok.await.expect("join").expect("valid enqueue");
    let bad_err = bad.await.expect("join").expect_err("unknown queue");
    assert!(matches!(bad_err, StoreError::QueueNotFound(_)));
    assert_eq!(store.get_job(ok_job.id).await.expect("get").payload, b"ok");

    let _ = std::fs::remove_file(&path);
}

fn controller_options() -> SqliteWriteOptions {
    SqliteWriteOptions {
        limits: AdaptiveBatchLimits {
            batch_size_min: 1,
            batch_size_max: 64,
            batch_wait_min: Duration::from_millis(1),
            batch_wait_max: Duration::from_millis(100),
        },
        ewma_window: 1,
        ..SqliteWriteOptions::default()
    }
}

#[test]
fn ewma_window_controls_smoothing() {
    let mut short = Ewma::new(1);
    let mut long = Ewma::new(9);
    short.observe(10.0);
    long.observe(10.0);
    short.observe(0.0);
    long.observe(0.0);
    assert_eq!(short.value(), Some(0.0));
    assert!(long.value().expect("value") > 0.0);
}

#[test]
fn durability_modes_configure_sqlite_synchronous_setting() {
    let path = std::env::temp_dir().join(format!("maqistor-test-{}.db", Uuid::new_v4()));
    let strict = SqliteConn::open(&path, DurabilityMode::Strict).expect("open strict store");
    let synchronous: i64 = strict
        .conn
        .query_row("PRAGMA synchronous", [], |row| row.get(0))
        .expect("read synchronous pragma");
    assert_eq!(synchronous, 2, "FULL synchronous mode");
    drop(strict);

    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(format!("{}-wal", path.display()));
    let _ = std::fs::remove_file(format!("{}-shm", path.display()));
}

#[test]
fn batch_size_uses_request_and_sql_commit_rates_with_direction_streak() {
    let mut controller = AdaptiveBatchController::new(&controller_options());
    controller.request_rate.observe(100.0);
    controller.commit_rate.observe(10.0);
    controller.commit_duration.observe(0.001);
    controller.baseline_commit_duration = Some(0.001);

    controller.adjust_batch_size();
    controller.adjust_batch_size();
    assert_eq!(controller.batch_size, 8);
    controller.adjust_batch_size();
    assert!(controller.batch_size > 1);
    assert!(controller.batch_size <= 10);
}

#[test]
fn writer_backlog_drives_a_probe_when_closed_loop_rate_is_throttled() {
    let mut controller = AdaptiveBatchController::new(&controller_options());
    controller.request_rate.observe(100.0);
    controller.commit_rate.observe(100.0);
    controller.commit_duration.observe(0.001);
    controller.baseline_commit_duration = Some(0.001);
    controller.backlog = 100;

    for _ in 0..3 {
        controller.adjust_batch_size();
    }
    assert!(controller.batch_size > 8);
}

#[test]
fn congested_commits_back_off_without_a_fixed_latency_target() {
    let mut options = controller_options();
    options.limits.batch_size_max = 16;
    let mut controller = AdaptiveBatchController::new(&options);
    controller.batch_size = 16;
    controller.request_rate.observe(100.0);
    controller.commit_rate.observe(100.0);
    controller
        .commit_duration
        .observe(Duration::from_millis(30).as_secs_f64());
    controller.baseline_commit_duration = Some(Duration::from_millis(1).as_secs_f64());

    for _ in 0..3 {
        controller.adjust_batch_size();
    }
    assert!(controller.batch_size < 16);
    assert!(controller.batch_size >= options.limits.batch_size_min);
}

#[test]
fn neutral_conditions_hold_batch_size() {
    let mut controller = AdaptiveBatchController::new(&controller_options());
    controller.batch_size = 8;
    controller.request_rate.observe(50.0);
    controller.commit_rate.observe(25.0);
    controller.commit_duration.observe(0.001);
    controller.baseline_commit_duration = Some(0.001);

    for _ in 0..3 {
        controller.adjust_batch_size();
    }
    assert_eq!(controller.batch_size, 8);
}

#[test]
fn three_sparse_timeout_batches_back_off_once() {
    let mut controller = AdaptiveBatchController::new(&controller_options());
    controller.batch_size = 16;
    let now = Instant::now();

    for _ in 0..LOW_FILL_TIMEOUTS {
        controller.record_successful_commit(
            4,
            Duration::from_millis(1),
            now,
            0,
            FlushReason::Timeout,
        );
    }

    assert!(controller.batch_size < 16);
    assert_eq!(controller.low_fill_timeouts, 0);
}

#[test]
fn predicted_fill_time_extends_wait_inside_configured_caps() {
    let mut controller = AdaptiveBatchController::new(&controller_options());
    controller.batch_size = 4;
    controller.batch_wait = Duration::from_millis(20);
    controller.request_rate.observe(10.0);

    for _ in 0..3 {
        controller.adjust_batch_wait();
    }
    assert!(controller.batch_wait > Duration::from_millis(20));
    assert!(controller.batch_wait <= controller.limits.batch_wait_max);
}
