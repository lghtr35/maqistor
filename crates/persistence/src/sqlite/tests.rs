use std::path::Path;
use std::time::{Duration, Instant};

use super::*;
use maqistor_engine::{DurableStore, Ewma, Job, JobOutcome, JobQueue, JobStatus, StoreError};
use uuid::Uuid;

fn cleanup_store(path: &Path) {
    let results = default_results_path(path);
    for p in [path, &results] {
        let _ = std::fs::remove_file(p);
        let _ = std::fs::remove_file(format!("{}-wal", p.display()));
        let _ = std::fs::remove_file(format!("{}-shm", p.display()));
    }
}

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
    let mut queue = JobQueue::new("email");
    queue.timeout_secs = 30;
    store
        .upsert_queue(queue)
        .await
        .expect("upsert queue");
    let job = store
        .enqueue(Job::new_pending("email", vec![]))
        .await
        .expect("enqueue");

    let claimed = store
        .claim_next("email")
        .await
        .expect("claim")
        .expect("claimed job");
    assert_eq!(claimed.id, job.id);
    assert_eq!(claimed.status, JobStatus::Running);
    assert!(
        claimed.created_at > 1_000_000_000_000,
        "created_at should be unix millis"
    );
    let lease_span_ms = claimed.lease_expires_at.unwrap() - claimed.updated_at;
    assert!(
        (29_000..=31_000).contains(&lease_span_ms),
        "30s lease should be ~30000ms, got {lease_span_ms}"
    );

    let recovered = store
        .recover_stale_leases(claimed.lease_expires_at.unwrap() + 1)
        .await
        .expect("recover");
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].status, JobStatus::Pending);
    assert_eq!(recovered[0].execution_count, 1);

    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn fifo_claims_increment_counts_and_fence_results() {
    let path = std::env::temp_dir().join(format!("maqistor-test-{}.db", Uuid::new_v4()));
    let store = SqliteStore::open(&path).unwrap();
    let mut queue = JobQueue::new("email");
    queue.max_retries = 1;
    store.upsert_queue(queue).await.unwrap();
    let mut first = Job::new_pending("email", b"first".to_vec());
    first.created_at = 10;
    let mut second = Job::new_pending("email", b"second".to_vec());
    second.created_at = 10;
    let first = store.enqueue(first).await.unwrap();
    let second = store.enqueue(second).await.unwrap();

    let claimed = store.claim_batch("email", 64).await.unwrap();
    assert_eq!(
        claimed.iter().map(|job| job.id).collect::<Vec<_>>(),
        vec![first.id, second.id]
    );
    assert!(
        claimed
            .iter()
            .all(|job| job.execution_count == 1 && job.dispatch_id.is_some())
    );

    let stale = store
        .complete(first.id, "wrong-dispatch", JobOutcome::Succeeded(vec![]))
        .await
        .unwrap();
    assert!(stale.is_none());
    let first_dispatch = claimed[0].dispatch_id.as_deref().unwrap();
    let retry = store
        .complete(
            first.id,
            first_dispatch,
            JobOutcome::Failed("temporary".into()),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retry.status, JobStatus::Pending);
    assert_eq!(retry.execution_count, 1);
    assert_eq!(retry.created_at, 10);

    let retry = store.claim_next("email").await.unwrap().unwrap();
    assert_eq!(retry.id, first.id);
    assert_eq!(retry.execution_count, 2);
    let terminal = store
        .complete(
            first.id,
            retry.dispatch_id.as_deref().unwrap(),
            JobOutcome::Failed("final".into()),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(terminal.status, JobStatus::Failed);
    assert_eq!(terminal.result_error.as_deref(), Some("final"));
}

#[tokio::test]
async fn retries_use_policy_snapshotted_when_claimed() {
    let path = std::env::temp_dir().join(format!("maqistor-test-{}.db", Uuid::new_v4()));
    let store = SqliteStore::open(&path).unwrap();
    let mut queue = JobQueue::new("email");
    queue.max_retries = 1;
    store.upsert_queue(queue.clone()).await.unwrap();
    let job = store
        .enqueue(Job::new_pending("email", vec![]))
        .await
        .unwrap();

    let first = store.claim_next("email").await.unwrap().unwrap();
    queue.max_retries = 0;
    store.upsert_queue(queue).await.unwrap();
    let retry = store
        .complete(
            first.id,
            first.dispatch_id.as_deref().unwrap(),
            JobOutcome::Failed("retry under snapshotted policy".into()),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retry.status, JobStatus::Pending);

    let second = store.claim_next("email").await.unwrap().unwrap();
    assert_eq!(second.id, job.id);
    assert_eq!(second.execution_count, 2);
    let terminal = store
        .complete(
            second.id,
            second.dispatch_id.as_deref().unwrap(),
            JobOutcome::Failed("new policy disallows retry".into()),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(terminal.status, JobStatus::Failed);
}

#[tokio::test]
async fn worker_result_completion_is_fenced_and_lightweight() {
    let path = std::env::temp_dir().join(format!("maqistor-test-{}.db", Uuid::new_v4()));
    let store = SqliteStore::open(&path).unwrap();
    store.upsert_queue(JobQueue::new("email")).await.unwrap();
    let job = store
        .enqueue(Job::new_pending("email", vec![]))
        .await
        .unwrap();
    let claimed = store.claim_next("email").await.unwrap().unwrap();
    let dispatch_id = claimed.dispatch_id.as_deref().unwrap();

    assert!(!store
        .complete_worker_result(job.id, dispatch_id, JobOutcome::Succeeded(vec![]))
        .await
        .unwrap());
    assert!(!store
        .complete_worker_result(job.id, dispatch_id, JobOutcome::Succeeded(vec![]))
        .await
        .unwrap());
    assert_eq!(store.get_job(job.id).await.unwrap().status, JobStatus::Completed);
}

#[tokio::test]
async fn fifo_uses_timestamp_then_id_and_preserves_position_after_release() {
    let path = std::env::temp_dir().join(format!("maqistor-test-{}.db", Uuid::new_v4()));
    let store = SqliteStore::open(&path).unwrap();
    store.upsert_queue(JobQueue::new("email")).await.unwrap();

    let mut later = Job::new_pending("email", b"later".to_vec());
    later.created_at = 20;
    let mut first = Job::new_pending("email", b"first".to_vec());
    first.created_at = 10;
    let mut second = Job::new_pending("email", b"second".to_vec());
    second.created_at = 10;
    let later = store.enqueue(later).await.unwrap();
    let first = store.enqueue(first).await.unwrap();
    let second = store.enqueue(second).await.unwrap();

    let claimed = store.claim_batch("email", 64).await.unwrap();
    assert_eq!(
        claimed.iter().map(|job| job.id).collect::<Vec<_>>(),
        vec![first.id, second.id, later.id]
    );
    store
        .release_claim(first.id, claimed[0].dispatch_id.as_deref().unwrap())
        .await
        .unwrap();
    store
        .release_claim(second.id, claimed[1].dispatch_id.as_deref().unwrap())
        .await
        .unwrap();
    store
        .release_claim(later.id, claimed[2].dispatch_id.as_deref().unwrap())
        .await
        .unwrap();

    let reclaimed = store.claim_batch("email", 64).await.unwrap();
    assert_eq!(
        reclaimed.iter().map(|job| job.id).collect::<Vec<_>>(),
        vec![first.id, second.id, later.id]
    );
}

#[tokio::test]
async fn claim_batch_exceeds_sixty_four_and_persists_success_payload() {
    let path = std::env::temp_dir().join(format!("maqistor-test-{}.db", Uuid::new_v4()));
    let store = SqliteStore::open(&path).unwrap();
    store.upsert_queue(JobQueue::new("email")).await.unwrap();
    for _ in 0..65 {
        store
            .enqueue(Job::new_pending("email", vec![]))
            .await
            .unwrap();
    }

    let claimed = store.claim_batch("email", 100).await.unwrap();
    assert_eq!(claimed.len(), 65);
    let completed = store
        .complete(
            claimed[0].id,
            claimed[0].dispatch_id.as_deref().unwrap(),
            JobOutcome::Succeeded(b"result".to_vec()),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(completed.result_payload.as_deref(), Some(&b"result"[..]));
    assert_eq!(completed.result_error, None);
    assert!(store.claim_next("email").await.unwrap().is_none());
}

#[tokio::test]
async fn completion_results_share_a_bounded_group_commit() {
    let path = std::env::temp_dir().join(format!("maqistor-test-{}.db", Uuid::new_v4()));
    let options = SqliteWriteOptions {
        completion: BatchOptions {
            batch_size_min: 2,
            batch_size_max: 2,
            batch_wait_min: Duration::from_millis(1),
            batch_wait_max: Duration::from_millis(20),
            ..BatchOptions::default()
        },
        ..SqliteWriteOptions::default()
    };
    let store = SqliteStore::open_with_options(&path, options).unwrap();
    store.upsert_queue(JobQueue::new("email")).await.unwrap();
    store
        .enqueue(Job::new_pending("email", vec![1]))
        .await
        .unwrap();
    store
        .enqueue(Job::new_pending("email", vec![2]))
        .await
        .unwrap();
    let claimed = store.claim_batch("email", 2).await.unwrap();
    let first = claimed[0].clone();
    let second = claimed[1].clone();
    let one = tokio::spawn({
        let store = store.clone();
        async move {
            store
                .complete(
                    first.id,
                    first.dispatch_id.as_deref().unwrap(),
                    JobOutcome::Succeeded(vec![]),
                )
                .await
        }
    });
    let two = tokio::spawn({
        let store = store.clone();
        async move {
            store
                .complete(
                    second.id,
                    second.dispatch_id.as_deref().unwrap(),
                    JobOutcome::Succeeded(vec![]),
                )
                .await
        }
    });
    assert_eq!(
        one.await.unwrap().unwrap().unwrap().status,
        JobStatus::Completed
    );
    assert_eq!(
        two.await.unwrap().unwrap().unwrap().status,
        JobStatus::Completed
    );
}

#[tokio::test]
async fn enqueue_is_not_starved_while_a_completion_batch_is_open() {
    let path = std::env::temp_dir().join(format!("maqistor-test-{}.db", Uuid::new_v4()));
    let options = SqliteWriteOptions {
        completion: BatchOptions {
            batch_size_min: 64,
            batch_size_max: 64,
            batch_wait_min: Duration::from_millis(40),
            batch_wait_max: Duration::from_millis(40),
            ..BatchOptions::default()
        },
        ..SqliteWriteOptions::default()
    };
    let store = SqliteStore::open_with_options(&path, options).unwrap();
    store.upsert_queue(JobQueue::new("email")).await.unwrap();
    let job = store
        .enqueue(Job::new_pending("email", vec![1]))
        .await
        .unwrap();
    let claimed = store.claim_next("email").await.unwrap().unwrap();
    let complete = tokio::spawn({
        let store = store.clone();
        let dispatch_id = claimed.dispatch_id.clone().unwrap();
        async move {
            store
                .complete(claimed.id, &dispatch_id, JobOutcome::Succeeded(vec![]))
                .await
        }
    });
    tokio::time::sleep(Duration::from_millis(10)).await;
    let enqueue = tokio::time::timeout(
        Duration::from_millis(150),
        store.enqueue(Job::new_pending("email", vec![2])),
    )
    .await
    .expect("enqueue must finish after the completion batch wait, not hang forever")
    .unwrap();
    assert_ne!(enqueue.id, job.id);
    assert_eq!(
        complete.await.unwrap().unwrap().unwrap().status,
        JobStatus::Completed
    );
}

#[tokio::test]
async fn claim_preempts_an_open_ingest_batch() {
    let path = std::env::temp_dir().join(format!("maqistor-test-{}.db", Uuid::new_v4()));
    let options = SqliteWriteOptions {
        enqueue: BatchOptions {
            batch_size_min: 64,
            batch_size_max: 64,
            batch_wait_min: Duration::from_millis(500),
            batch_wait_max: Duration::from_millis(500),
            ..BatchOptions::default()
        },
        ..SqliteWriteOptions::default()
    };
    let store = SqliteStore::open_with_options(&path, options).unwrap();
    store.upsert_queue(JobQueue::new("email")).await.unwrap();

    let enqueue = tokio::spawn({
        let store = store.clone();
        async move { store.enqueue(Job::new_pending("email", vec![])).await }
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    let claimed = tokio::time::timeout(
        Duration::from_millis(100),
        store.claim_next("email"),
    )
    .await
    .expect("claim must preempt ingest collection instead of waiting for the 500ms deadline")
    .unwrap()
    .unwrap();
    assert_eq!(claimed.status, JobStatus::Running);
    enqueue.await.unwrap().unwrap();
}

#[tokio::test]
async fn claim_flushes_after_fair_ingest_budget() {
    let path = std::env::temp_dir().join(format!("maqistor-test-{}.db", Uuid::new_v4()));
    let options = SqliteWriteOptions {
        enqueue: BatchOptions {
            batch_size_min: 64,
            batch_size_max: 64,
            batch_wait_min: Duration::from_millis(500),
            batch_wait_max: Duration::from_millis(500),
            ..BatchOptions::default()
        },
        ..SqliteWriteOptions::default()
    };
    let store = SqliteStore::open_with_options(&path, options).unwrap();
    store.upsert_queue(JobQueue::new("email")).await.unwrap();

    let first = tokio::spawn({
        let store = store.clone();
        async move { store.enqueue(Job::new_pending("email", vec![1])).await }
    });
    tokio::time::sleep(Duration::from_millis(20)).await;

    let claim = tokio::spawn({
        let store = store.clone();
        async move { store.claim_next("email").await }
    });
    tokio::time::sleep(Duration::from_millis(20)).await;

    let mut follow_ups = Vec::new();
    for n in 2..=5 {
        let store = store.clone();
        follow_ups.push(tokio::spawn(async move {
            store.enqueue(Job::new_pending("email", vec![n])).await
        }));
    }

    let claimed = tokio::time::timeout(Duration::from_millis(200), claim)
        .await
        .expect("claim must preempt the open ingest batch")
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(claimed.status, JobStatus::Running);
    first.await.unwrap().unwrap();
    for task in follow_ups {
        task.await.unwrap().unwrap();
    }
}

#[tokio::test]
async fn completion_batches_fill_under_mixed_ingest() {
    let path = std::env::temp_dir().join(format!("maqistor-test-{}.db", Uuid::new_v4()));
    let options = SqliteWriteOptions {
        enqueue: BatchOptions {
            batch_size_min: 1,
            batch_size_max: 1,
            batch_wait_min: Duration::from_millis(1),
            batch_wait_max: Duration::from_millis(1),
            ..BatchOptions::default()
        },
        completion: BatchOptions {
            batch_size_min: 8,
            batch_size_max: 8,
            batch_wait_min: Duration::from_millis(30),
            batch_wait_max: Duration::from_millis(30),
            ..BatchOptions::default()
        },
        ..SqliteWriteOptions::default()
    };
    let store = SqliteStore::open_with_options(&path, options).unwrap();
    store.upsert_queue(JobQueue::new("email")).await.unwrap();
    for n in 0..8u8 {
        store
            .enqueue(Job::new_pending("email", vec![n]))
            .await
            .unwrap();
    }
    let claimed = store.claim_batch("email", 8).await.unwrap();
    assert_eq!(claimed.len(), 8);

    let mut completes = Vec::new();
    for job in claimed {
        let complete_store = store.clone();
        let enqueue_store = store.clone();
        let dispatch_id = job.dispatch_id.clone().unwrap();
        completes.push(tokio::spawn(async move {
            complete_store
                .complete(job.id, &dispatch_id, JobOutcome::Succeeded(vec![]))
                .await
        }));
        tokio::spawn(async move {
            let _ = enqueue_store.enqueue(Job::new_pending("email", vec![9])).await;
        });
    }

    for task in completes {
        assert_eq!(
            task.await.unwrap().unwrap().unwrap().status,
            JobStatus::Completed
        );
    }
}

#[tokio::test]
async fn completes_progress_under_continuous_ingest() {
    let path = std::env::temp_dir().join(format!("maqistor-test-{}.db", Uuid::new_v4()));
    let options = SqliteWriteOptions {
        enqueue: BatchOptions {
            batch_size_min: 8,
            batch_size_max: 8,
            batch_wait_min: Duration::from_millis(5),
            batch_wait_max: Duration::from_millis(5),
            ..BatchOptions::default()
        },
        completion: BatchOptions {
            batch_size_min: 4,
            batch_size_max: 4,
            batch_wait_min: Duration::from_millis(5),
            batch_wait_max: Duration::from_millis(5),
            ..BatchOptions::default()
        },
        ..SqliteWriteOptions::default()
    };
    let store = SqliteStore::open_with_options(&path, options).unwrap();
    store.upsert_queue(JobQueue::new("email")).await.unwrap();
    for n in 0..4u8 {
        store
            .enqueue(Job::new_pending("email", vec![n]))
            .await
            .unwrap();
    }
    let claimed = store.claim_batch("email", 4).await.unwrap();
    let ingest = tokio::spawn({
        let store = store.clone();
        async move {
            for n in 0..64u8 {
                store
                    .enqueue(Job::new_pending("email", vec![n]))
                    .await
                    .unwrap();
            }
        }
    });
    let mut completes = Vec::new();
    for job in claimed {
        let store = store.clone();
        let dispatch_id = job.dispatch_id.clone().unwrap();
        completes.push(tokio::spawn(async move {
            store
                .complete(job.id, &dispatch_id, JobOutcome::Succeeded(vec![]))
                .await
        }));
    }
    for task in completes {
        let done = tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("completes must progress while ingest continues")
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(done.status, JobStatus::Completed);
    }
    ingest.await.unwrap();
}

#[tokio::test]
async fn ingest_progresses_under_mixed_complete_traffic() {
    let path = std::env::temp_dir().join(format!("maqistor-test-{}.db", Uuid::new_v4()));
    let options = SqliteWriteOptions {
        enqueue: BatchOptions {
            batch_size_min: 4,
            batch_size_max: 4,
            batch_wait_min: Duration::from_millis(20),
            batch_wait_max: Duration::from_millis(20),
            ewma_window: 4,
            ..BatchOptions::default()
        },
        completion: BatchOptions {
            batch_size_min: 4,
            batch_size_max: 4,
            batch_wait_min: Duration::from_millis(50),
            batch_wait_max: Duration::from_millis(50),
            ewma_window: 4,
            ..BatchOptions::default()
        },
        ..SqliteWriteOptions::default()
    };
    let store = SqliteStore::open_with_options(&path, options).unwrap();
    store.upsert_queue(JobQueue::new("email")).await.unwrap();
    for n in 0..8u8 {
        store
            .enqueue(Job::new_pending("email", vec![n]))
            .await
            .unwrap();
    }
    let claimed = store.claim_batch("email", 8).await.unwrap();

    let mut completes = Vec::new();
    for job in claimed {
        let store = store.clone();
        let dispatch_id = job.dispatch_id.clone().unwrap();
        completes.push(tokio::spawn(async move {
            store
                .complete(job.id, &dispatch_id, JobOutcome::Succeeded(vec![]))
                .await
        }));
    }

    let mut enqueues = Vec::new();
    for n in 0..24u8 {
        let store = store.clone();
        enqueues.push(tokio::spawn(async move {
            store.enqueue(Job::new_pending("email", vec![n])).await
        }));
    }

    for task in enqueues {
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("ingest must progress under mixed complete traffic")
            .unwrap()
            .unwrap();
    }
    for task in completes {
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("completes must still finish via starve / EWMA")
            .unwrap()
            .unwrap()
            .unwrap();
    }
}

#[tokio::test]
async fn read_pool_serves_queries_while_an_enqueue_batch_is_open() {
    let path = std::env::temp_dir().join(format!("maqistor-test-{}.db", Uuid::new_v4()));
    let options = SqliteWriteOptions {
        enqueue: BatchOptions {
            batch_size_min: 2,
            batch_size_max: 2,
            batch_wait_min: Duration::from_millis(150),
            batch_wait_max: Duration::from_millis(150),
            ..BatchOptions::default()
        },
        ..SqliteWriteOptions::default()
    };
    let store = SqliteStore::open_with_options(&path, options).unwrap();
    store.upsert_queue(JobQueue::new("email")).await.unwrap();
    let enqueue = tokio::spawn({
        let store = store.clone();
        async move { store.enqueue(Job::new_pending("email", vec![])).await }
    });
    tokio::time::sleep(Duration::from_millis(20)).await;

    let queues = tokio::time::timeout(Duration::from_millis(60), store.list_queues())
        .await
        .expect("read pool must not wait for the enqueue batch")
        .unwrap();
    assert_eq!(
        queues
            .iter()
            .map(|queue| queue.name.as_str())
            .collect::<Vec<_>>(),
        ["email"]
    );
    enqueue.await.unwrap().unwrap();
}

#[tokio::test]
async fn zero_retries_allows_exactly_one_execution() {
    let path = std::env::temp_dir().join(format!("maqistor-test-{}.db", Uuid::new_v4()));
    let store = SqliteStore::open(&path).unwrap();
    let mut queue = JobQueue::new("email");
    queue.max_retries = 0;
    store.upsert_queue(queue).await.unwrap();
    let job = store
        .enqueue(Job::new_pending("email", vec![]))
        .await
        .unwrap();
    let claimed = store.claim_next("email").await.unwrap().unwrap();
    let done = store
        .complete(
            job.id,
            claimed.dispatch_id.as_deref().unwrap(),
            JobOutcome::Failed("no retry".into()),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(done.status, JobStatus::Failed);
    assert_eq!(done.execution_count, 1);
    assert!(store.claim_next("email").await.unwrap().is_none());
}

#[tokio::test]
async fn stale_leases_obey_retry_limit_without_incrementing_on_requeue() {
    let path = std::env::temp_dir().join(format!("maqistor-test-{}.db", Uuid::new_v4()));
    let store = SqliteStore::open(&path).unwrap();
    let mut queue = JobQueue::new("email");
    queue.max_retries = 1;
    store.upsert_queue(queue).await.unwrap();
    let job = store
        .enqueue(Job::new_pending("email", vec![]))
        .await
        .unwrap();

    let first = store.claim_next("email").await.unwrap().unwrap();
    let recovered = store
        .recover_stale_leases(first.lease_expires_at.unwrap() + 1)
        .await
        .unwrap();
    assert_eq!(recovered[0].status, JobStatus::Pending);
    assert_eq!(recovered[0].execution_count, 1);

    let second = store.claim_next("email").await.unwrap().unwrap();
    assert_eq!(second.id, job.id);
    assert_eq!(second.execution_count, 2);
    let recovered = store
        .recover_stale_leases(second.lease_expires_at.unwrap() + 1)
        .await
        .unwrap();
    assert_eq!(recovered[0].status, JobStatus::Failed);
    assert_eq!(recovered[0].execution_count, 2);
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
    assert_eq!(job_indexes, "idx_jobs_queue_pending");

    let results_path = default_results_path(&path);
    let results = rusqlite::Connection::open(&results_path).expect("open results db");
    let attempt_indexes: String = results
        .query_row(
            "SELECT group_concat(name, ',')
             FROM (SELECT name FROM sqlite_master
                   WHERE type = 'index' AND tbl_name = 'job_attempts'
                     AND name NOT LIKE 'sqlite_%'
                   ORDER BY name)",
            [],
            |row| row.get::<_, Option<String>>(0),
        )
        .expect("read attempt indexes")
        .expect("attempt indexes present");
    assert_eq!(
        attempt_indexes,
        "idx_attempts_job_id,idx_attempts_stale_leases"
    );

    cleanup_store(&path);
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
        enqueue: BatchOptions {
            batch_size_min: 1,
            batch_size_max: 64,
            batch_wait_min: Duration::from_millis(1),
            batch_wait_max: Duration::from_millis(100),
            ewma_window: 1,
            ..BatchOptions::default()
        },
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
    let strict = RwConnection::open(&path, DurabilityMode::Strict).expect("open strict store");
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
    let mut controller = AdaptiveBatchController::new(&controller_options().enqueue);
    controller.request_rate_mut().observe(100.0);
    controller.commit_rate_mut().observe(10.0);
    controller.commit_duration_mut().observe(0.001);
    controller.set_baseline_commit_duration(0.001);

    controller.adjust_batch_size();
    controller.adjust_batch_size();
    assert_eq!(controller.batch_size(), 8);
    controller.adjust_batch_size();
    assert!(controller.batch_size() > 1);
    assert!(controller.batch_size() <= 10);
}

#[test]
fn writer_backlog_drives_a_probe_when_closed_loop_rate_is_throttled() {
    let mut controller = AdaptiveBatchController::new(&controller_options().enqueue);
    controller.request_rate_mut().observe(100.0);
    controller.commit_rate_mut().observe(100.0);
    controller.commit_duration_mut().observe(0.001);
    controller.set_baseline_commit_duration(0.001);
    controller.set_backlog(100);

    for _ in 0..3 {
        controller.adjust_batch_size();
    }
    assert!(controller.batch_size() > 8);
}

#[test]
fn congested_commits_back_off_without_a_fixed_latency_target() {
    let mut options = controller_options();
    options.enqueue.batch_size_max = 16;
    let mut controller = AdaptiveBatchController::new(&options.enqueue);
    controller.set_batch_size(16);
    controller.request_rate_mut().observe(100.0);
    controller.commit_rate_mut().observe(100.0);
    controller
        .commit_duration_mut()
        .observe(Duration::from_millis(30).as_secs_f64());
    controller.set_baseline_commit_duration(Duration::from_millis(1).as_secs_f64());

    for _ in 0..3 {
        controller.adjust_batch_size();
    }
    assert!(controller.batch_size() < 16);
    assert!(controller.batch_size() >= options.enqueue.batch_size_min);
}

#[test]
fn neutral_conditions_hold_batch_size() {
    let mut controller = AdaptiveBatchController::new(&controller_options().enqueue);
    controller.set_batch_size(8);
    controller.request_rate_mut().observe(50.0);
    controller.commit_rate_mut().observe(25.0);
    controller.commit_duration_mut().observe(0.001);
    controller.set_baseline_commit_duration(0.001);

    for _ in 0..3 {
        controller.adjust_batch_size();
    }
    assert_eq!(controller.batch_size(), 8);
}

#[test]
fn three_sparse_timeout_batches_back_off_once() {
    let mut controller = AdaptiveBatchController::new(&controller_options().enqueue);
    controller.set_batch_size(16);
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

    assert!(controller.batch_size() < 16);
    assert_eq!(controller.low_fill_timeouts, 0);
}

#[test]
fn predicted_fill_time_extends_wait_inside_configured_caps() {
    let mut controller = AdaptiveBatchController::new(&controller_options().enqueue);
    controller.set_batch_size(4);
    controller.batch_wait = Duration::from_millis(20);
    controller.request_rate_mut().observe(10.0);

    for _ in 0..3 {
        controller.adjust_batch_wait();
    }
    assert!(controller.batch_wait > Duration::from_millis(20));
    assert!(
        controller.batch_wait
            <= controller_options().enqueue.batch_wait_max
    );
}
