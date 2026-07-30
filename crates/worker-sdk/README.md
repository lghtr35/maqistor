# maqistor-worker-sdk

The Rust reference worker runtime for the
[Maqistor worker protocol](../worker-protocol/README.md). It establishes mutual
TLS, registers a queue, sends five-second heartbeats, deserializes JSON payloads,
enforces local concurrency, and returns success or failure results.

Implement `Queue` with a static queue name and a deserializable payload, then
run a `Worker` with a nonzero concurrency limit:

```rust
use std::num::NonZeroU32;
use maqistor_worker_sdk::{Queue, Worker, WorkerConnection};

struct Email;
impl Queue for Email {
    type Payload = serde_json::Value;
    const NAME: &'static str = "email";
}

let connection = WorkerConnection {
    maqistor_addr: "127.0.0.1:7829".into(),
    server_name: "maqistor.internal".into(),
    ca_cert_path: "certs/ca.pem".into(),
    client_cert_path: "certs/worker-cert.pem".into(),
    client_key_path: "certs/worker-key.pem".into(),
};
Worker::<Email>::new(connection, NonZeroU32::new(16).unwrap(), |_job| async {
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
