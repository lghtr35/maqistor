# maqistor-worker-sdk

The Rust reference worker runtime for the
[Maqistor worker protocol](https://github.com/lghtr35/maqistor/blob/main/crates/worker-protocol/README.md). A `Worker` is a
definition (connection, queue name, concurrency, handler). `run` opens one
session: mutual TLS, register, heartbeats, and a read loop that fills
concurrency slots with dispatched jobs.

## Install

```toml
[dependencies]
maqistor-worker-sdk = "0.1.0"
serde_json = "1"
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

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
that need process-level reconnection should supervise and restart it. To drain
on a process lifecycle signal, pass a caller-owned future to `run_until`:

```rust,no_run
# use maqistor_worker_sdk::Worker;
# async fn example(worker: Worker<serde_json::Value>) -> Result<(), Box<dyn std::error::Error>> {
worker.run_until(async {
    tokio::signal::ctrl_c().await.expect("install Ctrl-C handler");
}).await?;
# Ok(())
# }
```

After the signal, the worker stops accepting new jobs, completes accepted jobs,
reports their results, and closes the session. The server configuration must
contain the queue name and trust the worker client certificate.

## Reading graph

- [Top-level README](https://github.com/lghtr35/maqistor/blob/main/README.md) - binary installation and TLS setup.
- [worker-protocol](https://github.com/lghtr35/maqistor/blob/main/crates/worker-protocol/README.md) - interoperable wire contract.
- [dispatcher](https://github.com/lghtr35/maqistor/blob/main/crates/dispatcher/README.md) - server-side registration and capacity behavior.
- [maqistor](https://github.com/lghtr35/maqistor/blob/main/crates/maqistor/README.md) - queue and `worker_tls` configuration.
