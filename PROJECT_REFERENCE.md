# Maqistor code reference

## Mental model

Maqistor is a durable local job system. The `maqistor` binary loads TOML, creates SQLite-backed queues, accepts mutually-authenticated worker connections, optionally supervises Docker workers, starts the engine, and exposes a small HTTP API. A submitted job moves through `pending -> running -> completed|failed`; a `dispatch_id` fences late or duplicate worker results. Failed or expired leased jobs are returned to `pending` until the queue retry limit is exhausted.

```text
HTTP client -> api -> engine -> DurableStore (split SQLite persistence)
                         |                 |
                         |                 +-> ingest DB: queues/jobs/claims
                         |                 +-> results DB: attempts/outcomes/leases
                         v
                  WorkerDispatcher -> dispatcher registry -> TLS worker
                                                     ^          |
                                                     +-- result-+
```

The workspace packages are intentionally layered:

| Crate | Role | Main dependency direction |
| --- | --- | --- |
| `maqistor-engine` | Domain model, ports, scheduler | depends on no Maqistor crate |
| `maqistor-persistence` | SQLite implementation of `DurableStore` | engine |
| `maqistor-worker-protocol` | Versioned, length-prefixed CBOR wire format | standalone |
| `maqistor-dispatcher` | Worker registry/TLS listener/Docker supervisor and dispatcher implementation | engine + protocol |
| `maqistor-worker-sdk` | Worker-side typed execution client | protocol |
| `maqistor-api` | Axum HTTP adapter | engine |
| `maqistor` | Composition-root binary | all runtime crates |
| `maqistor-noop-worker` | Benchmark worker binary | worker SDK |

`Cargo.toml` at the workspace root lists these members and centralizes external versions. Every crate uses Rust 2024 and the shared `0.1.0` workspace version.

## Crate: `maqistor-engine`

### `crates/engine/src/types.rs` — durable domain records

- `struct Job` — complete persisted job record: queue name (`name`), state, raw JSON payload, retry/lease/dispatch fencing data, optional result, and millisecond timestamps.
  - `Job::new_pending(name, payload)` — creates an unpersisted job (`id = 0`) in `Pending` state with timestamps.
- `struct JobQueue` — queue configuration persisted with its name, retry limit, lease timeout, and timestamps.
  - `JobQueue::new(name)` — creates a queue with `max_retries = 3` and `timeout_secs = 60`.
- `enum JobStatus` — `Pending`, `Running`, `Completed`, or `Failed`.
  - `as_str()` — canonical lowercase database/API spelling.
  - `parse(value)` — parses that spelling, returning `None` for an unknown state.
  - `Display` — delegates to `as_str()`.
- `enum StoreError` — persistence-facing error vocabulary: `NotFound`, `QueueNotFound`, or `Internal`.
- `unix_now()` — current Unix milliseconds used for durable timestamps.

### `crates/engine/src/adaptive.rs` — shared adaptive control primitives

- `struct Ewma` — exponentially weighted moving average; stores its smoothing factor and optional current value.
  - `new(window)` — initializes the smoothing factor from a sample window.
  - `observe(sample)` — incorporates a finite, non-negative sample; ignores invalid values.
  - `value()` — returns the current average if one exists.
- `struct DirectionStreak` — requires three same-direction non-zero observations before confirming a control action.
  - `confirm(direction)` — records `-1`, `0`, or `1`; returns `true` only on a confirmed repeated direction; zero resets it.
- `struct AdaptiveBatch` — bounded batch-size controller built from `DirectionStreak`.
  - `new(min, max, probe_factor, backoff_factor)` — starts at the geometric midpoint within bounds.
  - `size()` / `set_size(size)` — reads or clamps the current size.
  - `observe_direction(direction)` — after a confirmed direction, probes up or backs off down; reports whether size changed.
  - `reset_direction()` — clears accumulated direction evidence.

### `crates/engine/src/lib.rs` — ports and scheduler

- `MAX_CLAIM_BATCH_SIZE` — hard cap (`16,384`) for a single durable claim/scheduler request.
- `enum JobOutcome` — worker completion payload: `Succeeded(Vec<u8>)` or `Failed(String)`.
- `struct WorkerResult` — a result associated with both a job ID and its dispatch lease ID.
- `enum WorkerEvent` — dispatcher-to-engine event: `Registered { queue_name }` wakes a queue; `Result { queue_name, result }` persists an outcome and wakes it.
- `enum EngineError` — API/domain-facing translation of unknown queue, missing job, storage failure, or payload serialization failure.
  - `From<StoreError>` — converts store errors to the appropriate engine error.
- `trait DurableStore` — async persistence port implemented by `SqliteStore`.
  - `upsert_queue`, `get_queue`, `list_queues` — queue configuration operations.
  - `enqueue`, `get_job`, `status` — durable job creation and reads.
  - `claim_next` — claims one pending job with a lease.
  - `claim_batch` — default implementation repeatedly calls `claim_next`, bounded by `MAX_CLAIM_BATCH_SIZE`; stores can override it atomically.
  - `complete` — persists a fenced outcome; default returns an unsupported-operation error.
  - `release_claim` — returns a matching running lease to pending after dispatch failure; default returns unsupported-operation.
  - `recover_stale_leases` — repairs expired running leases.
- `enum DispatchError` — no worker capacity or a dispatcher-specific failure.
- `struct QueueReservation` — asks a dispatcher to reserve `count` opaque worker slots for one queue.
- `trait DispatchPermit` — type-erased, sendable reservation token. `into_any` lets its owning dispatcher recover its concrete token.
- `struct ReservedDispatch` — queue-tagged opaque permit carried by the engine.
  - `new(queue_name, permit)` — wraps a concrete permit.
  - `into_permit()` — gives the permit back to the dispatcher.
- `trait WorkerDispatcher` — worker-capacity and delivery port.
  - `reserve(queues)` — default makes no reservations; implementations allocate worker slots.
  - `dispatch(permit, job)` — required job delivery operation.
  - `release(permit)` — default no-op; releases unused/failed reservations.
  - `subscribe_events()` — optional worker event receiver.
- `struct SubmitJob` — engine submission command (queue name plus JSON payload).
- `struct JobView` — small caller-facing job projection (ID, queue name, status).
- `struct DispatchOptions` — fixed per-queue `batch_size_max` and `max_in_flight` delivery concurrency.
  - `Default` — asks for at most 8,192 worker slots per queue pass and delivers at most 1,024 jobs concurrently.
  - `validate()` — requires both values to be positive and the batch cap to be no greater than `MAX_CLAIM_BATCH_SIZE`.
- `struct Engine<S, D>` — generic orchestration service over a durable store and dispatcher. It owns a background scheduler through `Scheduler`.
  - `with_dispatcher(store, dispatcher, options)` — validates options, creates the wake channel, and starts scheduling.
  - `submit(job)` — serializes JSON, enqueues it, then wakes its queue.
  - `get_job(id)` — fetches a `JobView`.
  - `recover(now)` — asks storage to repair leases and wakes recovered pending queues.
  - `complete(job_id, dispatch_id, outcome)` — commits a fenced outcome and immediately re-wakes retryable jobs.
  - `start_result_listener()` — subscribes to worker registration/result events and connects them to `complete` plus wakeups; call once after construction.
  - `dispatch(permit, job)` — direct delegate retained as a narrow convenience method.
  - `ensure_awake(queue)` *(private)* — coalesces wake requests so one queue has one active scheduler pass plus at most one re-wake.
  - `start_scheduler(rx)` *(private)* — spawns the queue-draining loop and 30-second lease recovery.
  - `drain_pass(queues)` *(private)* — reserves up to the fixed per-queue batch cap, claims durable jobs, concurrently delivers work, releases unused permits, and decides re-wakes.
  - `wake_after_pass(queues)` *(private)* — schedules queues that consumed a full requested batch.
- `struct Scheduler` *(private)* — wake sender, per-queue awake flags, and dispatch options.

### `crates/engine/tests/dispatch_port.rs` — integration proof

- `RecordingDispatcher` and `TestPermit` *(test-only)* — a fake `WorkerDispatcher` and opaque permit.
- `fake_dispatcher_accepts_a_job()` — verifies `Engine::dispatch` reaches a dispatcher. The in-file `dispatch_options_reject_unsafe_claim_batches()` test verifies the hard batch cap.

## Crate: `maqistor-persistence`

The store is now deliberately split. The ingest database owns queue definitions plus the lightweight job row and is optimized for submission/claim traffic. The results database owns one `job_attempts` row per dispatch, including leases and outcomes. `SqliteStore` composes the two records into the engine `Job` view. A short-lived cross-database gap is repaired on startup: a claimed ingest row without a matching result attempt is returned to pending.

### `crates/persistence/src/lib.rs` and `crates/persistence/src/sqlite/mod.rs` — public surface and module map

- `lib.rs` keeps SQLite private and re-exports `SqliteStore`, `SqliteWriteOptions`, `BatchOptions`, `DurabilityMode`, and `default_results_path`.
- `sqlite/mod.rs` declares the private `adaptive`, `common`, `ingest`, `options`, `results`, and `store` files. Its test-only re-exports expose controller internals to `tests.rs`.

### `crates/persistence/src/sqlite/options.rs` — policy objects

- `enum DurabilityMode` — `Balanced` (`NORMAL`, default) or `Strict` (`FULL`) SQLite synchronization.
- `struct BatchOptions` — min/max batch size and wait, EWMA window, probe factor, and backoff factor.
  - `enqueue_defaults()` / `completion_defaults()` *(crate-private)* — presets; completion favors a short wait.
  - `validate(section)` *(private)* — rejects invalid limits and adaptive parameters.
  - `Default` — enqueue defaults.
- `struct SqliteWriteOptions` — durability plus independently tuned enqueue and completion options.
  - `Default` — combines the two presets.
  - `validate()` — validates both policy groups.

### `crates/persistence/src/sqlite/adaptive.rs` — shared batching controller

- `LOW_FILL_TIMEOUTS` — number of sparse timeouts before a non-congested batch backs off.
- `enum FlushReason` *(crate-private)* — `FullBatch` or `Timeout`.
- `struct AdaptiveBatchController` *(crate-private)* — observes request/commit rates, commit duration, fill ratio, and backlog to adjust a batch’s size and waiting window.
  - `new(options)`, `observe_request(now)`, `record_successful_commit(...)` — lifecycle and observations.
  - `observe_commit_baseline(sample)` *(private)* — maintains a slowly relaxing best-case commit duration.
  - `adjust_batch_size()` / `adjust_batch_wait()` — grow under demand, back off under congestion/sparse traffic, and target 75% fill.
  - `batch_size()` — current bounded target.
  - `set_batch_size()` *(test-only)* — direct test control.
  - Test-only `request_rate_mut`, `commit_rate_mut`, `commit_duration_mut`, `set_baseline_commit_duration`, and `set_backlog` expose deterministic controller state.

### `crates/persistence/src/sqlite/common.rs` — shared schema, row mapping, and reads

- `SCHEMA_VERSION` *(crate-private)* and `unix_now()` *(crate-private)* — schema identity and millisecond clock.
- `default_results_path(ingest)` — derives `<base>-results.db`, treating an `-ingest` suffix specially.
- `struct RwConnection` *(crate-private)* — writable connection initialization shared by both databases.
  - `open(path, durability)` — creates parent directories, enables WAL/foreign keys, and configures sync mode.
  - `migrate_schema(apply)` — creates/checks the version table and invokes the supplied first-install schema.
- `apply_ingest_schema(conn)` — creates `job_queues`, `jobs`, and the FIFO pending-job index.
- `apply_results_schema(conn)` — creates append-only-style `job_attempts` plus job/lease indexes.
- `struct IngestJobRow` / `AttemptRow` *(crate-private)* — raw rows from each database before composition.
- `row_to_ingest_job`, `row_to_attempt`, `row_to_queue` *(crate-private)* — SQL row decoders with integer conversion checks.
- `merge_job(ingest, attempt)` *(crate-private)* — turns the latest attempt plus ingest state into the public `Job`; it encodes the split-store state machine (for example, an ingest `claimed` row plus running attempt becomes `Running`).
- `new_dispatch_id()` *(crate-private)* — UUID fencing token for every claim.
- `struct ReadPool` *(crate-private)* — four round-robin, query-only connections for either database.
  - `open_ingest(path)` / `open_results(path)` — configure the respective query projections.
  - `open_with_sql(...)` / `connection()` *(private)* — construct the pool and choose the next connection.
  - `ingest_job`, `latest_attempt`, `queue`, `queues` — async read helpers that isolate blocking SQLite work.
- `heal_orphan_claims(ingest, results)` *(crate-private)* — repairs claimed jobs lacking a corresponding attempt record after a partial cross-store claim.

### `crates/persistence/src/sqlite/ingest.rs` — queue, submission, and claim database

- `jobs_insert_sql(rows)` *(private)* — constructs parameterized multi-row inserts.
- `struct IngestClaimed` *(crate-private)* — claimed job data handed to the results store so its running attempt can be created.
- `enum IngestRequest` *(private)* — writer commands: queue upsert, enqueue, claim batch, or repend.
- `struct PendingEnqueue` / `BatchCommit` *(private)* — queued enqueue plus commit measurements.
- `struct IngestConn` *(private)* — writer-thread database connection.
  - `open`, `queue_names`, `upsert_queue` — migration, known queue cache, and durable configuration update.
  - `enqueue_batch` — validates queues, inserts pending jobs in 64-row chunks, assigns IDs, and replies after commit.
  - `claim_batch` — FIFO claims jobs, changing each to `claimed` and assigning its dispatch ID atomically.
  - `repend` — returns only a matching claim to pending.
  - `handle` — executes a non-batched command or defensively batches a single enqueue.
- `struct IngestHandle` *(crate-private)* — async command sender plus ingest read pool.
  - `open`, `call` *(private)* — starts the named writer thread/current-thread Tokio runtime and awaits one-shot replies.
  - `upsert_queue`, `enqueue`, `claim_batch`, `repend`, `ingest_row` — async ingest operations used by `SqliteStore`.
- `struct IngestQueues` *(private)* — FIFO queues for meta, claim, and batchable ingest work.
  - `is_empty` / `push` — writer-loop queue management.
- `ingest_writer_loop` *(private)* — gives meta and claims priority over enqueues, flushing on shutdown.
- `run_ingest_turn` *(private)* — collects until target/deadline; claim/meta work preempts an open batch.
- `flush_ingest` / `flush_pending` *(private)* — drain buffered submissions, commit them, and feed adaptive observations.

### `crates/persistence/src/sqlite/results.rs` — attempt, lease, and outcome database

- `struct RunningInsert` *(crate-private)* — newly claimed attempt to insert as running.
- `struct CompleteOutcome` / `RecoveredStale` *(crate-private)* — result of a fenced completion or stale-lease recovery, including whether ingest must repend the job.
- `enum ResultsRequest` *(private)* — insert running attempts, complete, abandon, recover stale, or retrieve maximum execution count.
- `struct PendingCompletion` / `BatchCommit` *(private)* — completion buffer and commit telemetry.
- `struct ResultsConn` *(private)* — results writer connection.
  - `open`, `max_execution_count`, `insert_running_batch` — migration, retry sequence lookup, and atomic running-attempt inserts.
  - `complete_batch` — fences and batches worker outcomes.
  - `abandon` — marks a matching running attempt failed after delivery failure.
  - `recover_stale` — marks expired attempts failed and decides whether they are eligible to repend.
  - `handle` — executes unbatched control operations.
- `complete_one(tx, ...)` *(private)* — the fenced state transition used within one completion transaction; success completes, failure records a failed attempt and requests retry when allowed.
- `struct ResultsHandle` *(crate-private)* — async results command sender, results read pool, and database path.
  - `open`, `path`, `call` *(private)* — writer startup, path access, and one-shot command handling.
  - `insert_running`, `complete`, `abandon`, `recover_stale`, `max_execution_count`, `latest_attempt` — async results operations.
- `struct ResultsQueues` *(private)* — meta, running-attempt insertion, and batchable completion queues.
  - `is_empty` / `push` — queue routing and completion wait tracking.
- `results_writer_loop` *(private)* — meta and running-attempt records preempt completion batching.
- `run_complete_turn` / `flush_complete` *(private)* — collect/commit completion batches and update the adaptive controller.

### `crates/persistence/src/sqlite/store.rs` — public split-store composition

- `struct SqliteStore` — cloneable `DurableStore` built from an `IngestHandle` and `ResultsHandle`.
  - `open(ingest_path)` — opens defaults and derives the results path.
  - `open_with_options(ingest_path, options)` — same, with write policy.
  - `open_with_options_pair(ingest_path, results_path, options)` — explicit two-database construction, migrations, read pools, and startup healing.
  - `heal_on_open` *(private)* — invokes orphan-claim repair across the two databases.
  - `composed_job` *(private)* — reads both halves and merges them.
  - `job_from_claimed` *(private)* — creates the engine `Running` view before its attempt is stored.
  - `claim_batch_inner` *(private)* — claim ingest rows, calculate each next execution count, insert running attempts; if insertion fails, repend the claims.
  - `DurableStore` implementation — delegates queues/enqueues to ingest; composes job reads; coordinates claim, completion, release, and recovery across both databases.

### `crates/persistence/src/sqlite/tests.rs` — behavioral coverage

- `cleanup_store(path)` removes both temporary ingest and derived results databases after a test.
- `controller_options()` supplies deterministic policies. `persists_queues_and_jobs_across_reopen`, `claim_and_recover_stale_lease`, `fifo_claims_increment_counts_and_fence_results`, and `fifo_uses_timestamp_then_id_and_preserves_position_after_release` cover durable split-store state and FIFO behavior.
- `claim_batch_exceeds_sixty_four_and_persists_success_payload`, `completion_results_share_a_bounded_group_commit`, `enqueue_is_not_starved_while_a_completion_batch_is_open`, `claim_preempts_an_open_ingest_batch`, `claim_flushes_after_fair_ingest_budget`, `completion_batches_fill_under_mixed_ingest`, `completes_progress_under_continuous_ingest`, and `ingest_progresses_under_mixed_complete_traffic` cover the independent writer loops under mixed load.
- `read_pool_serves_queries_while_an_enqueue_batch_is_open`, `zero_retries_allows_exactly_one_execution`, `stale_leases_obey_retry_limit_without_incrementing_on_requeue`, `enqueue_rejects_unknown_queue`, `concurrent_enqueues_are_durable_after_await`, `jobs_use_sequential_integer_ids_with_dispatch_indexes`, and `mixed_unknown_queue_does_not_block_valid_enqueues` cover isolation, retries, and ingest safety.
- `ewma_window_controls_smoothing`, `durability_modes_configure_sqlite_synchronous_setting`, `batch_size_uses_request_and_sql_commit_rates_with_direction_streak`, `writer_backlog_drives_a_probe_when_closed_loop_rate_is_throttled`, `congested_commits_back_off_without_a_fixed_latency_target`, `neutral_conditions_hold_batch_size`, `three_sparse_timeout_batches_back_off_once`, and `predicted_fill_time_extends_wait_inside_configured_caps` cover adaptive policy invariants.

## Crate: `maqistor-worker-protocol`

### `crates/worker-protocol/src/lib.rs` — shared worker wire format

- `PROTOCOL_VERSION` — currently `1`.
- `MAX_FRAME_BYTES` — one-megabyte maximum CBOR body.
- `struct ProtocolFrame<T>` — version wrapper around any payload.
  - `v1(payload)` — builds a current-version frame.
- `enum WorkerMessage` — protocol messages:
  - `Register` — worker identity, queue, running work, and free slots.
  - `JobDispatch` — server-to-worker job with ID, fencing ID, execution count, and raw payload.
  - `JobResult` — worker-to-server outcome plus fresh capacity metrics.
  - `Heartbeat`, `Registered`, and structured `Error`.
- `enum JobResult` — `Succeeded { payload }` or `Failed { message }` nested inside `JobResult` messages.
- `type WireFrame` — `ProtocolFrame<WorkerMessage>`.
- `enum ProtocolError` — unsupported version, oversized frame, malformed CBOR, or I/O failure.
- `encode_frame(frame)` — validates version/size and produces `u32` big-endian length + CBOR bytes.
- `decode_frame(bytes)` — validates exact framing/size/version and decodes CBOR.
- `write_frame(writer, frame)` / `read_frame(reader)` — blocking stream helpers around the encoding.
- Test `round_trip()` — proves heartbeat encode/decode symmetry.

## Crate: `maqistor-dispatcher`

### `crates/dispatcher/src/lib.rs` — worker registry, delivery, TLS listener, Docker

- `struct RegistryDispatcher` — `WorkerDispatcher` backed by a live `WorkerRegistry`.
  - `new(registry)` — attaches to the registry.
  - `reserve(queues)` — reserves available slots per queue and wraps them in `RegistryPermit` tokens.
  - `dispatch(permit, job)` — validates the owned permit, sends a `JobDispatch` through that worker’s serialized writer, waits for write acknowledgement, and releases a failed reservation.
  - `release(permit)` — returns an unused registry reservation.
  - `subscribe_events()` — exposes registry worker events to the engine.
- `struct TlsFiles` — CA, server certificate, and server key filesystem paths.
- `struct WorkerState` — public worker metrics/queue/last activity plus private reserved count and outbound sender.
- `struct OutboundFrame` *(private)* — a frame and acknowledgement used by the dedicated connection writer.
- `struct RegistryPermit` *(private)* — worker UUID plus registry; implements `DispatchPermit::into_any` so dispatcher methods recover it.
- `release_permit(registry, worker_id)` *(private)* — decrements one reservation, saturating at zero.
- `struct WorkerRegistry` — synchronized map of worker UUID to `WorkerState`, with a broadcast event sender.
  - `Default` — empty registry with a 65,536-event broadcast buffer.
  - `snapshot()` — cloned diagnostic view of all workers.
  - `has_capacity(queue_name)` — tests unreserved slot availability for a queue.
- `struct ManagedQueue` — Docker-managed queue name, image reference, and desired replica count.
- `struct DockerWorkerSupervisor` — Docker client, desired queues, and image-ID cache.
  - `connect(queues)` — connects to local Docker.
  - `reconcile()` — ensures every desired queue/ordinal has the expected container.
  - `spawn()` — repeats reconciliation every five seconds, logging failures.
  - `ensure(queue, ordinal)` *(private)* — starts matching containers, removes only labeled outdated Maqistor containers, or creates a labeled `unless-stopped` container.
  - `resolve_image_id(image)` *(private)* — caches local image ID or pulls then inspects a missing image.
- `container_name(queue, ordinal)` *(private)* — turns a queue name into a stable `maqistor-<sanitized>-<ordinal>` Docker name.
- `start_worker_listener(addr, tls, allowed_queues)` — binds the mutual-TLS TCP listener, spawns accept/connection tasks, and returns the registry immediately.
- `server_config(files)` *(private)* — loads server material and requires client certificates chained to the configured CA.
- `certs(path)` / `key(path)` *(private)* — PEM loading helpers.
- `read_frame(stream)` / `write_frame(stream, frame)` *(private)* — async length-prefixed protocol I/O with frame-size enforcement.
- `handle_worker(stream, registry, allowed, peer_addr)` *(private)* — requires `Register` within 15 seconds, validates queue and unique instance ID, starts serialized outbound writes, records worker state, acknowledges registration, emits events, consumes heartbeats/results, updates capacity, and removes the worker on disconnect.

## Crate: `maqistor-worker-sdk`

### `crates/worker-sdk/src/lib.rs` — typed worker runtime

- `trait Queue` — application-defined worker contract: deserializable `Payload` type and static queue `NAME`.
- `struct Job<T>` — typed dispatch given to a handler: durable ID, fencing `dispatch_id`, execution count, and decoded payload.
- `struct WorkerConnection` — server address/name and paths for CA, client certificate, and client key.
- `type Handler<Q>` *(private)* — shared boxed async function from a typed job to `Result<Vec<u8>, String>`.
- `struct Worker<Q>` — connection configuration, non-zero concurrency, handler, and queue marker.
  - `new(connection, concurrency, handler)` — boxes a typed async handler.
  - `start(stream)` — blocking/test-oriented path: writes a `Register` frame and returns a `WorkerLifecycle` around the supplied writer.
  - `connection()` — reads connection settings.
  - `run()` — production async path: opens mutually-authenticated TLS, registers, emits five-second heartbeats, accepts dispatches, and runs each in a task.
- `client_config(connection)` *(private)* — creates client TLS configuration from PEM files.
- `certs(path)` / `key(path)` *(private)* — client PEM readers.
- `read_async_frame(reader)` *(private)* — async length-prefixed protocol reader with max-frame validation.
- `struct AsyncWorkerLifecycle<Q>` *(private)* — shared TLS writer, handler, semaphore, and capacity used by `Worker::run`.
  - `Clone` — shares those resources across dispatched tasks.
  - `write(payload)` — serializes a frame under writer lock.
  - `execute_dispatch(job_id, dispatch_id, execution_count, payload)` — decodes JSON, acquires a slot, calls the handler, and reports success/failure.
  - `report(job_id, dispatch_id, result)` — sends `JobResult` with current running/free capacity.
- `struct WorkerLifecycle<Q, W>` — public lifecycle for callers driving dispatch manually over any blocking `Write` stream.
  - `execute_dispatch(...)` — decodes raw payload, reports decode failures, otherwise executes the typed job.
  - `execute(job)` — enforces concurrency, invokes handler, reports result, and returns handler failures to the caller.
  - `instance_id()` — UUID registered by `start`.
  - `report_result(...)` *(private)* — converts handler result to a wire `JobResult` with slot metrics.
- `enum WorkerExecutionError` — stopped semaphore, handler error, or protocol write error for manual execution.
- `enum WorkerRunError` — stopped runtime, configuration/remote/protocol/I/O/TLS failure for `run`.

## Crate: `maqistor-api`

### `crates/api/src/lib.rs` — HTTP adapter

- `struct JobRequest` — JSON body for `POST /jobs`: queue `name` and arbitrary JSON `payload`.
- `struct JobResponse` — JSON projection returned for submitted/fetched jobs: ID, name, status string.
- `struct ApiState<S, D>` *(private)* — cloned engine held by Axum state.
- `router(engine)` — creates routes and HTTP tracing middleware:
  - `GET /health` returns `204 No Content`.
  - `POST /jobs` submits a job and returns `201` plus `JobResponse`.
  - `GET /jobs/{id}` returns a `JobResponse`.
- `struct ErrorBody` *(private)* — `{ "error": ... }` error JSON.
- `struct ApiError` *(private)* — HTTP status plus message.
  - `IntoResponse` — turns it into JSON response.
  - `From<EngineError>` — maps unknown queue/payload to 400, missing job to 404, storage errors to 500.
- `submit_job(state, request)` *(private)* — translates request to `SubmitJob`, calls engine, emits 201.
- `get_job(state, id)` *(private)* — fetches and translates the engine view.
- `to_response(job)` *(private)* — shared `JobView` to HTTP JSON conversion.
- Test-only `MemoryStore` — in-memory `DurableStore` implementation used by `http_submission_is_persisted_through_engine()`, which verifies the end-to-end submit route.

## Crate: `maqistor` (binary)

### `crates/maqistor/src/config.rs` — TOML configuration boundary

- `struct AppConfig` — complete TOML document: HTTP and worker addresses, TLS material, split persistence/dispatch policies, and queues. Unknown fields are rejected.
  - `load(path)` — reads TOML, deserializes, and validates it.
  - `validate()` *(private)* — checks option validity; distinct listeners; unique nonempty queue names; positive timeouts; mode-specific image/replica rules.
  - `listen()` and `worker_listen()` — resolve optional listener values to documented defaults.
- `struct PersistenceConfig` — split database paths, sync mode, startup policy, enqueue batching, and completion batching.
  - `Default` — defaults every nested setting.
  - `ingest_database_path()` — returns configured ingest path or `./data/maqistor-ingest.db`.
  - `results_database_path()` — returns configured results path, `./data/maqistor-results.db` when both are defaulted, or derives an adjacent `-results.db` name from a custom ingest path.
  - `write_options()` — applies TOML overrides to `SqliteWriteOptions` and validates.
- `struct BatchConfig` — optional TOML overrides for a `BatchOptions` group.
  - `apply(options)` *(private)* — overlays only supplied values, converting millisecond fields to `Duration`.
- `struct DispatchConfig` — optional fixed scheduler batch cap and concurrent delivery limit.
  - `Default` — all optional (engine defaults remain effective).
  - `options()` — produces and validates `DispatchOptions`.
- `enum StartupPolicy` — `Recover` (default, repair stale leases at launch) or `Preserve`.
- `struct WorkerTlsConfig` — configured CA/server certificate/key paths.
- `enum QueueMode` — `Managed` (Maqistor controls Docker replicas) or `External` (workers connect independently).
- `struct QueueConfig` — queue name/mode, optional managed image and replicas, plus retry and timeout policy.
  - `replicas()` — returns configured count or the managed default of one.
- `validate_managed_image(image)` *(private)* — requires a non-floating explicit tag or SHA-256 digest; rejects `latest` and `stable`.
- Tests `defaults_hide_adaptive_details`, `custom_limits_and_window_are_applied`, `rejects_retired_batching_knobs_and_invalid_limits`, `parses_strict_durability_and_preserve_startup_policy`, and `database_paths_live_under_persistence` cover defaults, validation, and split-path derivation.

### `crates/maqistor/src/main.rs` — composition root

- `struct Cli` *(private)* — Clap command line with `--config`/`-c`, defaulting to `maqistor.toml`.
- `main()` — initializes tracing; loads config; opens/configures the explicit ingest/results SQLite pair; upserts queues; optionally recovers stale leases; starts mutual-TLS worker listener and managed Docker supervisor; wires `SqliteStore + RegistryDispatcher` into `Engine`; starts result consumption; then serves the API.

## Crate: `maqistor-noop-worker` (benchmark)

### `benchmark/noop-worker/src/main.rs`

- `struct BenchQueue` *(private)* — SDK `Queue` implementation for the `bench` queue with JSON payloads.
- `env(name, default)` *(private)* — reads an environment variable with a fallback.
- `main()` — builds a TLS `WorkerConnection` and concurrency from benchmark environment variables, then runs an 8-slot-by-default handler that immediately succeeds with an empty payload.

## Benchmark tooling (non-crate files)

### `benchmark/oha_util.py` — benchmark primitives

- Constants `BASE_URL`, `INGEST_BODY`, and `BENCH_QUEUE` — local benchmark target, submitted job body, and queue name.
- `workspace_root()` — finds and validates the Cargo workspace root.
- `default_db_path(root)` / `default_results_path(ingest)` — derive the benchmark ingest and paired results database paths.
- `open_db(path)` — opens an existing SQLite database read-only through a Windows-safe URI.
- `max_job_id(ingest)` — returns the current job-ID watermark.
- `count_open(ingest, results, queue, after_id)` — counts pending ingest jobs plus running attempts after a watermark.
- `wait_drain(...)` — polls `count_open` until drained or timed out; returns success, elapsed seconds, and remaining jobs.
- `_percentile(sorted_values, pct)` — interpolated percentile utility used for lifecycle timing.
- `cycle_stats(...)` — calculates jobs in the measured window, terminal outcomes, and create-to-completion p50/p99/max from both databases.
- `require_oha()` / `require_standing_server(script_name)` — fail early unless the load generator and live `/health` endpoint are available.
- `ensure_ingest_body(root)` — writes the JSON request body consumed by oha.
- `run_oha(...)` — builds/runs the oha command, optionally persists raw JSON, and parses its report.
- `rps(report)`, `latency_ms(report, key)`, `status_counts(report)`, `error_count(report)`, and `success_rate(report)` — normalize report variants and calculate benchmark health metrics.

### `benchmark/run.py` — capacity sweep driver

- `class Result` — one measured point: offered/achieved rates, latency/error/stability data, and optional full-cycle drain/lifecycle measurements.
- `positive_csv(value)` — argparse validator for positive comma-separated integer series.
- `parse_args()` — defines `closed`, `open`, `both`, and `full` modes plus duration, concurrency, QPS, SLO, settling, and drain settings.
- `fmt(value, digits)` — formats nullable numeric output.
- `ingest_result(...)` — runs one POST `/jobs` oha point and decides whether it passes the zero-error, p99, and (for offered load) 98%-achievement guardrails.
- `run_full_point(...)` — records an ingest watermark, runs the load point, waits for split-store drain, calculates cycle metrics, and marks incomplete drains unstable.
- `settle(seconds, remaining_points)` — pauses between sweep points when useful.
- `print_results(results, max_p99_ms, full)` — prints compact result tables and best observed/stable summaries.
- `main()` — validates prerequisites, plans the requested sweep, writes raw oha reports and a timestamped JSON summary, then prints results.

### `benchmark/generate-certs.sh`, `benchmark/maqistor.toml`, and `benchmark/noop-worker/Dockerfile`

- `generate-certs.sh` — creates benchmark CA/server/worker TLS material.
- `maqistor.toml` — benchmark server configuration, including paired SQLite paths and the external `bench` queue.
- `noop-worker/Dockerfile` — packages the no-op worker for the managed/benchmark environment.

## Non-code files worth knowing

- `maqistor/maqistor.example.toml` — commented example configuration; split database defaults and batching controls mirror `config.rs`.
- `maqistor/containerized_async_job_scheduler_design.md` — broader design notes for the system.
- Per-crate `README.md` files (`api`, `engine`, `persistence`, `dispatcher`, `maqistor`) provide focused usage/design context; this reference is the cross-crate index.
