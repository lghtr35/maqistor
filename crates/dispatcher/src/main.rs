use std::net::SocketAddr;

use clap::Parser;
use maqistor_engine::Engine;
use maqistor_persistence::{JobQueue, JobStore, SqliteStore};
use tokio::net::TcpListener;
use tracing::info;

mod config;
mod http;

use config::AppConfig;

#[derive(Debug, Parser)]
#[command(name = "maqistor", about = "Containerized job dispatcher")]
struct Cli {
    #[arg(short, long, default_value = "maqistor.toml")]
    config: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    let config = AppConfig::load(&cli.config)?;

    let write_options = config.persistence.write_options();
    let store = SqliteStore::open_with_options(config.database_path(), write_options)?;

    for job in &config.jobs {
        let mut queue = JobQueue::new(job.name.clone());
        queue.concurrency = job.concurrency;
        queue.max_retries = job.max_retries;
        store.upsert_queue(queue).await?;
    }

    let engine = Engine::new(store);
    let app = http::router(engine);

    let addr: SocketAddr = config
        .listen()
        .parse()
        .map_err(|err| anyhow::anyhow!("invalid listen address {}: {err}", config.listen()))?;
    let listener = TcpListener::bind(addr).await?;
    info!(
        listen = %addr,
        db = %config.database_path(),
        jobs = config.jobs.len(),
        batch_size = config.persistence.batch_size,
        adaptive_batch_size = config.persistence.adaptive_batch_size,
        batch_size_min = config.persistence.batch_size_min,
        batch_size_max = config.persistence.batch_size_max,
        batch_size_increase = config.persistence.batch_size_increase,
        adaptive_batch_wait = config.persistence.adaptive_batch_wait,
        batch_wait_ms = config.persistence.batch_wait_ms,
        batch_wait_min_ms = config.persistence.batch_wait_min_ms,
        batch_wait_max_ms = config.persistence.batch_wait_max_ms,
        "maqistor listening"
    );
    axum::serve(listener, app).await?;
    Ok(())
}
