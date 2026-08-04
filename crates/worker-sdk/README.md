# maqistor-worker-sdk

The Rust reference worker runtime for the
[Maqistor worker protocol](../worker-protocol/README.md). A `Worker` is a
definition (connection, queue name, concurrency, handler). `run` opens one
session: mutual TLS, register, heartbeats, and a read loop that fills
concurrency slots with dispatched jobs.

```rust
use std::num::NonZeroU32;
use maqistor_worker_sdk::{Job, Worker, WorkerConnection};

let connection = WorkerConnection {
    maqistor_addr: "127.0.0.1:7829".into(),
    server_name: "maqistor.internal".into(),
    ca_cert_path: "certs/ca.pem".into(),
    client_cert_path: "certs/worker-cert.pem".into(),
    client_key_path: "certs/worker-key.pem".into(),
};
Worker::new(connection, "email", NonZeroU32::new(16).unwrap(), |_: Job<serde_json::Value>| async {
    Ok(Vec::new())
}).run().await?;
```

`run` returns if connection, TLS, protocol, or remote errors occur; callers
that need process-level reconnection should supervise and restart it. The
server configuration must contain the queue name and trust the worker client
certificate.

## Reading graph

- [Top-level README](../../README.md) - binary installation and TLS setup.
- [worker-protocol](../worker-protocol/README.md) - interoperable wire contract.
- [dispatcher](../dispatcher/README.md) - server-side registration and capacity behavior.
- [maqistor](../maqistor/README.md) - queue and `worker_tls` configuration.
