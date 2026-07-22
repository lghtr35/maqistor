use std::{fs, path::Path, time::Duration};

use maqistor_engine::DispatchOptions;
use maqistor_persistence::{BatchOptions, DurabilityMode, SqliteWriteOptions, default_results_path};
use serde::Deserialize;

const DEFAULT_LISTEN: &str = "0.0.0.0:7828";
const DEFAULT_WORKER_LISTEN: &str = "0.0.0.0:7829";
const DEFAULT_INGEST_DATABASE: &str = "./data/maqistor-ingest.db";
const DEFAULT_RESULTS_DATABASE: &str = "./data/maqistor-results.db";

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppConfig {
    pub listen: Option<String>,
    pub worker_listen: Option<String>,
    pub worker_tls: WorkerTlsConfig,
    #[serde(default)]
    pub persistence: PersistenceConfig,
    #[serde(default)]
    pub dispatch: DispatchConfig,
    #[serde(default)]
    pub queues: Vec<QueueConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PersistenceConfig {
    pub ingest_database: Option<String>,
    pub results_database: Option<String>,
    #[serde(default)]
    pub durability: DurabilityMode,
    #[serde(default)]
    pub startup: StartupPolicy,
    #[serde(default)]
    pub enqueue: BatchConfig,
    #[serde(default)]
    pub completion: BatchConfig,
}

impl Default for PersistenceConfig {
    fn default() -> Self {
        Self {
            ingest_database: None,
            results_database: None,
            durability: DurabilityMode::default(),
            startup: StartupPolicy::default(),
            enqueue: BatchConfig::default(),
            completion: BatchConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BatchConfig {
    pub batch_size_min: Option<usize>,
    pub batch_size_max: Option<usize>,
    pub batch_wait_min_ms: Option<u64>,
    pub batch_wait_max_ms: Option<u64>,
    pub ewma_window: Option<usize>,
    pub batch_probe_factor: Option<f64>,
    pub batch_backoff_factor: Option<f64>,
}

impl BatchConfig {
    fn apply(&self, options: &mut BatchOptions) {
        options.batch_size_min = self.batch_size_min.unwrap_or(options.batch_size_min);
        options.batch_size_max = self.batch_size_max.unwrap_or(options.batch_size_max);
        options.batch_wait_min = Duration::from_millis(
            self.batch_wait_min_ms
                .unwrap_or(options.batch_wait_min.as_millis() as u64),
        );
        options.batch_wait_max = Duration::from_millis(
            self.batch_wait_max_ms
                .unwrap_or(options.batch_wait_max.as_millis() as u64),
        );
        options.ewma_window = self.ewma_window.unwrap_or(options.ewma_window);
        options.batch_probe_factor = self
            .batch_probe_factor
            .unwrap_or(options.batch_probe_factor);
        options.batch_backoff_factor = self
            .batch_backoff_factor
            .unwrap_or(options.batch_backoff_factor);
    }
}

impl PersistenceConfig {
    pub fn ingest_database_path(&self) -> &str {
        self.ingest_database
            .as_deref()
            .unwrap_or(DEFAULT_INGEST_DATABASE)
    }

    pub fn results_database_path(&self) -> String {
        if let Some(path) = self.results_database.as_deref() {
            return path.to_string();
        }
        if self.ingest_database.is_none() {
            return DEFAULT_RESULTS_DATABASE.to_string();
        }
        default_results_path(Path::new(self.ingest_database_path()))
            .display()
            .to_string()
    }

    pub fn write_options(&self) -> anyhow::Result<SqliteWriteOptions> {
        let mut options = SqliteWriteOptions {
            durability: self.durability,
            ..SqliteWriteOptions::default()
        };
        self.enqueue.apply(&mut options.enqueue);
        self.completion.apply(&mut options.completion);

        options.validate().map_err(anyhow::Error::msg)?;
        Ok(options)
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DispatchConfig {
    pub batch_size_max: Option<usize>,
    pub max_in_flight: Option<usize>,
}

impl Default for DispatchConfig {
    fn default() -> Self {
        Self {
            batch_size_max: None,
            max_in_flight: None,
        }
    }
}

impl DispatchConfig {
    pub fn options(&self) -> anyhow::Result<DispatchOptions> {
        let mut options = DispatchOptions::default();
        options.batch_size_max = self.batch_size_max.unwrap_or(options.batch_size_max);
        options.max_in_flight = self.max_in_flight.unwrap_or(options.max_in_flight);
        options.validate().map_err(anyhow::Error::msg)?;
        Ok(options)
    }
}

#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum StartupPolicy {
    #[default]
    Recover,
    Preserve,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerTlsConfig {
    pub ca_cert_path: String,
    pub cert_path: String,
    pub key_path: String,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum QueueMode {
    Managed,
    External,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueConfig {
    pub name: String,
    pub mode: QueueMode,
    pub image: Option<String>,
    pub replicas: Option<u32>,
    pub max_retries: u32,
    pub timeout_secs: u64,
}

impl QueueConfig {
    pub fn replicas(&self) -> u32 {
        self.replicas.unwrap_or(1)
    }
}

impl AppConfig {
    pub fn load(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref();
        let contents = fs::read_to_string(path)
            .map_err(|err| anyhow::anyhow!("failed to read config {}: {err}", path.display()))?;
        let config: Self = toml::from_str(&contents)
            .map_err(|err| anyhow::anyhow!("failed to parse config {}: {err}", path.display()))?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> anyhow::Result<()> {
        self.persistence.write_options()?;
        self.dispatch.options()?;
        if self.listen() == self.worker_listen() {
            anyhow::bail!("listen and worker_listen must differ");
        }
        let mut names = std::collections::HashSet::new();
        for queue in &self.queues {
            if queue.name.trim().is_empty() || !names.insert(&queue.name) {
                anyhow::bail!("queue names must be nonempty and unique");
            }
            if queue.timeout_secs == 0 {
                anyhow::bail!("queue {} must have a positive timeout", queue.name);
            }
            match queue.mode {
                QueueMode::Managed
                    if queue.image.as_deref().is_none_or(str::is_empty)
                        || queue.replicas() == 0 =>
                {
                    anyhow::bail!(
                        "managed queue {} requires an image and positive replicas",
                        queue.name
                    )
                }
                QueueMode::Managed => {
                    validate_managed_image(queue.image.as_deref().expect("validated image"))?
                }
                QueueMode::External if queue.image.is_some() || queue.replicas.is_some() => {
                    anyhow::bail!(
                        "external queue {} cannot declare image or replicas",
                        queue.name
                    )
                }
                _ => {}
            }
        }
        Ok(())
    }

    pub fn listen(&self) -> &str {
        self.listen.as_deref().unwrap_or(DEFAULT_LISTEN)
    }

    pub fn worker_listen(&self) -> &str {
        self.worker_listen
            .as_deref()
            .unwrap_or(DEFAULT_WORKER_LISTEN)
    }
}

fn validate_managed_image(image: &str) -> anyhow::Result<()> {
    let reference = image.rsplit('/').next().unwrap_or(image);
    let tag = reference.rsplit_once(':').map(|(_, tag)| tag);
    let digest = image.contains("@sha256:");
    if !digest && tag.is_none() {
        anyhow::bail!(
            "managed image {image:?} must use an explicit version tag or immutable digest"
        );
    }
    if matches!(tag, Some("latest" | "stable")) {
        anyhow::bail!(
            "managed image {image:?} uses unsupported floating tag; use an explicit version tag or immutable digest"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    const TLS: &str = "[worker_tls]\nca_cert_path = 'ca.pem'\ncert_path = 'server.pem'\nkey_path = 'server.key'\n";

    #[test]
    fn defaults_hide_adaptive_details() {
        let config: AppConfig = toml::from_str(&format!("{TLS}[[queues]]\nname = 'email'\nmode = 'external'\nmax_retries = 3\ntimeout_secs = 60\n")).expect("parse");
        let options = config.persistence.write_options().expect("options");
        assert_eq!(options.enqueue.ewma_window, 16);
        assert_eq!(options.durability, DurabilityMode::Balanced);
        assert_eq!(options.completion.batch_wait_max, Duration::from_millis(20));
    }

    #[test]
    fn custom_limits_and_window_are_applied() {
        let config: AppConfig = toml::from_str(&format!("{TLS}[persistence.enqueue]\newma_window = 8\nbatch_size_min = 4\nbatch_size_max = 32\nbatch_wait_min_ms = 2\nbatch_wait_max_ms = 20\nbatch_probe_factor = 1.2\nbatch_backoff_factor = 0.7\n[persistence.completion]\nbatch_wait_max_ms = 10\n[dispatch]\nbatch_size_max = 2048\nmax_in_flight = 64\n"))
        .expect("parse");
        let options = config.persistence.write_options().expect("options");
        assert_eq!(options.enqueue.ewma_window, 8);
        assert_eq!(options.enqueue.batch_size_min, 4);
        assert_eq!(options.enqueue.batch_size_max, 32);
        assert_eq!(options.enqueue.batch_wait_min, Duration::from_millis(2));
        assert_eq!(options.enqueue.batch_wait_max, Duration::from_millis(20));
        assert_eq!(options.enqueue.batch_probe_factor, 1.2);
        assert_eq!(options.completion.batch_wait_max, Duration::from_millis(10));
        assert_eq!(config.dispatch.options().unwrap().batch_size_max, 2048);
    }

    #[test]
    fn rejects_retired_batching_knobs_and_invalid_limits() {
        let retired: Result<AppConfig, _> =
            toml::from_str(&format!("{TLS}[persistence.limits]\nbatch_size = 64\n"));
        assert!(retired.is_err());

        let config: AppConfig = toml::from_str(&format!(
            "{TLS}[persistence.enqueue]\newma_window = 0\nbatch_size_min = 8\nbatch_size_max = 4\n"
        ))
        .expect("parse");
        assert!(config.validate().is_err());

        let invalid_adaptation: AppConfig = toml::from_str(&format!(
            "{TLS}[persistence.completion]\nbatch_probe_factor = 1.0\nbatch_backoff_factor = 1.0\n"
        ))
        .expect("parse");
        assert!(invalid_adaptation.validate().is_err());
    }

    #[test]
    fn parses_strict_durability_and_preserve_startup_policy() {
        let config: AppConfig = toml::from_str(&format!(
            "{TLS}[persistence]\ndurability = 'strict'\nstartup = 'preserve'\n"
        ))
        .expect("parse");
        assert_eq!(config.persistence.durability, DurabilityMode::Strict);
        assert_eq!(config.persistence.startup, StartupPolicy::Preserve);
    }

    #[test]
    fn database_paths_live_under_persistence() {
        let defaults: AppConfig =
            toml::from_str(&format!("{TLS}")).expect("parse");
        assert_eq!(
            defaults.persistence.ingest_database_path(),
            "./data/maqistor-ingest.db"
        );
        assert_eq!(
            defaults.persistence.results_database_path(),
            "./data/maqistor-results.db"
        );

        let config: AppConfig = toml::from_str(&format!(
            "{TLS}[persistence]\ningest_database = './data/ingest.db'\nresults_database = './data/results.db'\n"
        ))
        .expect("parse");
        assert_eq!(config.persistence.ingest_database_path(), "./data/ingest.db");
        assert_eq!(
            config.persistence.results_database_path(),
            "./data/results.db"
        );

        let derived: AppConfig = toml::from_str(&format!(
            "{TLS}[persistence]\ningest_database = './bench/maqistor-ingest.db'\n"
        ))
        .expect("parse");
        assert!(
            derived
                .persistence
                .results_database_path()
                .replace('\\', "/")
                .ends_with("maqistor-results.db")
        );
    }
}
