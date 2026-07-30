# maqistor-engine

The transport- and backend-independent coordinator. It owns job lifecycle,
queue scheduling, retries, stale-lease recovery coordination, and the durable
store / worker dispatcher ports.

## Lifecycle

`pending` jobs are claimed into `running` attempts, then become `completed` or
`failed`. A retryable failure or failed delivery returns work to `pending`.
Every claim creates a new `dispatch_id`; result handling uses that token as a
fence, so stale or duplicate worker results are ignored.

The scheduler coalesces queue wakeups. For each pass it reserves free worker
slots, acquires from the global delivery budget, claims no more jobs than it
reserved, and submits delivery work. Idle queues are periodically probed, and
stale leases are recovered every 30 seconds.

## Ports

- `DurableStore` - submission, claims, completion, retries, recovery, and reads.
- `WorkerDispatcher` - capacity reservation, delivery, release, and worker
  event subscription.

The concrete ports are [SQLite persistence](../persistence/README.md) and the
[worker registry dispatcher](../dispatcher/README.md). The engine has no HTTP,
Docker, configuration, or database dependency.

## Reading graph

- [Top-level README](../../README.md) - system boundary.
- [api](../api/README.md) - submits work through this layer.
- [persistence](../persistence/README.md) - `DurableStore` implementation.
- [dispatcher](../dispatcher/README.md) - `WorkerDispatcher` implementation.
