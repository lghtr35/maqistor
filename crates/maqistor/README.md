# maqistor

The `maqistor` binary is the composition root. It loads TOML configuration,
opens durable storage, registers queues, starts the worker listener and
(when needed) the managed worker supervisor, then serves the HTTP API.

## Startup

1. Initialize `tracing` from `RUST_LOG` (default level: `info`).
2. Load and validate configuration with unknown fields rejected.
3. Open the ingest and results SQLite databases and upsert configured queues.
4. Recover stale leases unless `persistence.startup = "preserve"`.
5. Start the mutual-TLS worker listener. When any queue has `managed_config`,
   also connect to Docker (local defaults or `[docker]`) and spawn the managed
   worker supervisor. External-only deployments skip Docker entirely.
6. Build the [Engine](../engine/README.md), subscribe to worker results, and
   bind the [HTTP API](../api/README.md).

## Configuration

[`../../maqistor.example.toml`](../../maqistor.example.toml) is the complete
annotated template. Important sections are:

| Section | Purpose |
| --- | --- |
| `worker_tls` | Required CA, server certificate, and server private-key paths for worker mTLS |
| `docker` | Optional single Docker endpoint (and optional daemon TLS) for managed replicas |
| `persistence` | Ingest/results database paths, durability, startup recovery, cleanup, and writer batching |
| `dispatch` | Claim-batch, delivery-budget, and idle-probe limits |
| `queues` | Queue retry/timeout policy; optional `managed_config` provides Docker image, replicas, and environment |

Workers may always connect independently to a configured queue. `managed_config`
adds Docker-maintained workers; it does not change the worker protocol. Sibling
managed containers and remote/external workers may share a queue.

## Reading graph

- [Top-level README](../../README.md) - install and deployment boundary.
- [api](../api/README.md) - HTTP routes exposed by this binary.
- [engine](../engine/README.md) - lifecycle and scheduling policy.
- [persistence](../persistence/README.md) - paired SQLite stores.
- [dispatcher](../dispatcher/README.md) - mTLS connections and managed workers.
