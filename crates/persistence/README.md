# maqistor-persistence

The SQLite implementation of the [Engine](../engine/README.md) `DurableStore`
port. It keeps ingestion/claim writes and execution/completion writes in
separate database files so they have independent writer loops.

## Durable state

| Database | Owns |
| --- | --- |
| Ingest | `job_queues` and `accepted_jobs`; a job remains available while `dispatch_id` is `NULL` |
| Results | `execution_queues` and `executions`; attempts are `running`, `completed`, or `failed` |

Both schemas are version 1. An incompatible database version fails to open;
Maqistor does not migrate old prototype files. When a store opens, orphaned
claims without matching execution attempts are repaired.

## Write behavior

Each database uses WAL, foreign keys, and a five-second SQLite busy timeout.
Durability is `none`, `balanced` (`synchronous=NORMAL`), or `strict`
(`synchronous=FULL`).

The ingest writer batches enqueue requests and serializes claim/repend work.
The results writer independently batches execution-row creation and worker
completion. Both adapt batch size and wait time from request/commit rates,
commit duration, batch fill, and backlog. Configure those policies through
[`[persistence]`](../../maqistor.example.toml) in the binary configuration.

## Reading graph

- [Top-level README](../../README.md) - storage operational boundary.
- [engine](../engine/README.md) - storage contract and lifecycle semantics.
- [maqistor](../maqistor/README.md) - database paths and write-policy config.
