use std::net::SocketAddr;

use clap::Parser;
use maqistor_dispatcher::{
    DockerWorkerSupervisor, ManagedQueue, RegistryDispatcher, TlsFiles, start_worker_listener,
};
use maqistor_engine::{DurableStore, Engine, JobQueue, unix_now};
use maqistor_persistence::SqliteStore;
use tokio::net::TcpListener;
use tracing::info;

use config::{QueueMode, StartupPolicy};

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
    let store = SqliteStore::open_with_options_pair(
        config.persistence.ingest_database_path(),
        config.persistence.results_database_path(),
        config.persistence.write_options()?,
    )?;
    for queue_config in &config.queues {
        let mut queue = JobQueue::new(queue_config.name.clone());
        queue.max_retries = queue_config.max_retries;
        queue.timeout_secs = queue_config.timeout_secs;
        store.upsert_queue(queue).await?;
    }
    if config.persistence.startup == StartupPolicy::Recover {
        let recovered = store.recover_stale_leases(unix_now()).await?;
        if !recovered.is_empty() {
            info!(
                recovered = recovered.len(),
                "recovered stale job leases at startup"
            );
        }
    }
    let worker_addr: SocketAddr = config.worker_listen().parse().map_err(|err| {
        anyhow::anyhow!(
            "invalid worker_listen address {}: {err}",
            config.worker_listen()
        )
    })?;
    let worker_registry = start_worker_listener(
        worker_addr,
        TlsFiles {
            ca_cert_path: config.worker_tls.ca_cert_path.clone(),
            cert_path: config.worker_tls.cert_path.clone(),
            key_path: config.worker_tls.key_path.clone(),
        },
        config.queues.iter().map(|q| q.name.clone()).collect(),
    )
    .await?;
    DockerWorkerSupervisor::connect(
        config
            .queues
            .iter()
            .filter(|q| q.mode == QueueMode::Managed)
            .map(|q| ManagedQueue {
                name: q.name.clone(),
                image: q.image.clone().expect("validated managed image"),
                replicas: q.replicas(),
            })
            .collect(),
    )
    .map_err(|err| anyhow::anyhow!("initialize managed worker supervisor: {err}"))?
    .spawn();
    let engine = Engine::with_dispatcher(
        store,
        RegistryDispatcher::new(worker_registry.clone()),
        config.dispatch.options()?,
    );
    engine.start_result_listener();
    let addr: SocketAddr = config
        .listen()
        .parse()
        .map_err(|err| anyhow::anyhow!("invalid listen address {}: {err}", config.listen()))?;
    let listener = TcpListener::bind(addr).await?;
    info!(
        listen = %addr,
        config = %config_path,
        db_ingest = %config.persistence.ingest_database_path(),
        db_results = %config.persistence.results_database_path(),
        queues = config.queues.len(),
        worker_listen = %worker_addr,
        durability = ?config.persistence.durability,
        startup = ?config.persistence.startup,
        "maqistor listening"
    );
    axum::serve(listener, maqistor_api::router(engine)).await?;
    Ok(())
}
