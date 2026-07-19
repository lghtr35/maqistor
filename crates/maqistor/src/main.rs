use std::net::SocketAddr;

use clap::Parser;
use maqistor_dispatcher::DockerDispatcher;
use maqistor_engine::{DurableStore, Engine, JobQueue};
use maqistor_persistence::SqliteStore;
use tokio::net::TcpListener;
use tracing::info;

use config::StartupPolicy;

mod config;

#[derive(Debug, Parser)]
#[command(name = "maqistor", about = "Durable local job engine")]
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
    let config_path = cli.config;
    let config = config::AppConfig::load(&config_path)?;
    let store = SqliteStore::open_with_options(
        config.database_path(),
        config.persistence.write_options()?,
    )?;
    for worker in &config.workers {
        let mut queue = JobQueue::new(worker.name.clone());
        queue.concurrency = worker.concurrency;
        queue.max_retries = worker.max_retries;
        store.upsert_queue(queue).await?;
    }
    if config.persistence.startup == StartupPolicy::Recover {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|err| anyhow::anyhow!("system clock before unix epoch: {err}"))?
            .as_secs() as i64;
        let recovered = store.recover_stale_leases(now).await?;
        if !recovered.is_empty() {
            info!(
                recovered = recovered.len(),
                "recovered stale job leases at startup"
            );
        }
    }
    let engine = Engine::with_dispatcher(store, DockerDispatcher::new());
    let addr: SocketAddr = config
        .listen()
        .parse()
        .map_err(|err| anyhow::anyhow!("invalid listen address {}: {err}", config.listen()))?;
    let listener = TcpListener::bind(addr).await?;
    info!(
        listen = %addr,
        config = %config_path,
        db = %config.database_path(),
        workers = config.workers.len(),
        durability = ?config.persistence.durability,
        startup = ?config.persistence.startup,
        "maqistor listening"
    );
    axum::serve(listener, maqistor_api::router(engine)).await?;
    Ok(())
}
