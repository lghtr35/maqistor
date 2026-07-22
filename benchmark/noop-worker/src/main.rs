use std::num::NonZeroU32;

use maqistor_worker_sdk::{Queue, Worker, WorkerConnection};

struct BenchQueue;

impl Queue for BenchQueue {
    type Payload = serde_json::Value;
    const NAME: &'static str = "bench";
}

fn env(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_owned())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let concurrency = env("MAQISTOR_WORKER_CONCURRENCY", "8").parse()?;
    let concurrency = NonZeroU32::new(concurrency)
        .ok_or("MAQISTOR_WORKER_CONCURRENCY must be greater than zero")?;
    let connection = WorkerConnection {
        maqistor_addr: env("MAQISTOR_ADDR", "host.docker.internal:17829"),
        server_name: env("MAQISTOR_SERVER_NAME", "maqistor-benchmark"),
        ca_cert_path: env("MAQISTOR_CA_CERT_PATH", "/certs/ca.pem"),
        client_cert_path: env("MAQISTOR_CLIENT_CERT_PATH", "/certs/worker-cert.pem"),
        client_key_path: env("MAQISTOR_CLIENT_KEY_PATH", "/certs/worker-key.pem"),
    };

    Worker::<BenchQueue>::new(connection, concurrency, |_| async { Ok(Vec::new()) })
        .run()
        .await?;
    Ok(())
}
