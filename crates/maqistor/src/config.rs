use std::{fs, path::Path, time::Duration};

use maqistor_persistence::{AdaptiveBatchLimits, DurabilityMode, SqliteWriteOptions};
use serde::Deserialize;

const DEFAULT_LISTEN: &str = "0.0.0.0:8080";
const DEFAULT_DATABASE_PATH: &str = "./data/maqistor.db";
const DEFAULT_EWMA_WINDOW: usize = 16;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppConfig {
    pub listen: Option<String>,
    pub database_path: Option<String>,
    #[serde(default)]
    pub persistence: PersistenceConfig,
    #[serde(default)]
    pub workers: Vec<WorkerConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PersistenceConfig {
    #[serde(default)]
    pub durability: DurabilityMode,
    #[serde(default)]
    pub startup: StartupPolicy,
    pub ewma_window: Option<usize>,
    pub limits: Option<AdaptiveLimitsConfig>,
    pub adaptation: Option<AdaptationConfig>,
}

impl Default for PersistenceConfig {
    fn default() -> Self {
        Self {
            durability: DurabilityMode::default(),
            startup: StartupPolicy::default(),
            ewma_window: None,
            limits: None,
            adaptation: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdaptiveLimitsConfig {
    pub batch_size_min: Option<usize>,
    pub batch_size_max: Option<usize>,
    pub batch_wait_min_ms: Option<u64>,
    pub batch_wait_max_ms: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdaptationConfig {
    pub batch_probe_factor: Option<f64>,
    pub batch_backoff_factor: Option<f64>,
}

impl PersistenceConfig {
    pub fn write_options(&self) -> anyhow::Result<SqliteWriteOptions> {
        let mut options = SqliteWriteOptions {
            durability: self.durability,
            ewma_window: self.ewma_window.unwrap_or(DEFAULT_EWMA_WINDOW),
            ..SqliteWriteOptions::default()
        };

        if let Some(limits) = &self.limits {
            options.limits = AdaptiveBatchLimits {
                batch_size_min: limits
                    .batch_size_min
                    .unwrap_or(options.limits.batch_size_min),
                batch_size_max: limits
                    .batch_size_max
                    .unwrap_or(options.limits.batch_size_max),
                batch_wait_min: Duration::from_millis(
                    limits
                        .batch_wait_min_ms
                        .unwrap_or(options.limits.batch_wait_min.as_millis() as u64),
                ),
                batch_wait_max: Duration::from_millis(
                    limits
                        .batch_wait_max_ms
                        .unwrap_or(options.limits.batch_wait_max.as_millis() as u64),
                ),
            };
        }
        if let Some(adaptation) = &self.adaptation {
            options.batch_probe_factor = adaptation
                .batch_probe_factor
                .unwrap_or(options.batch_probe_factor);
            options.batch_backoff_factor = adaptation
                .batch_backoff_factor
                .unwrap_or(options.batch_backoff_factor);
        }

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
pub struct WorkerConfig {
    pub name: String,
    #[serde(default = "default_replicas")]
    pub replicas: u32,
    #[serde(default = "default_concurrency")]
    pub concurrency: u32,
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
}

fn default_replicas() -> u32 {
    1
}

fn default_concurrency() -> u32 {
    1
}

fn default_max_retries() -> u32 {
    3
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
        for worker in &self.workers {
            if worker.name.trim().is_empty() {
                anyhow::bail!("worker name must not be empty");
            }
            if worker.replicas == 0 {
                anyhow::bail!("worker {} must have at least one replica", worker.name);
            }
            if worker.concurrency == 0 {
                anyhow::bail!("worker {} must have positive concurrency", worker.name);
            }
        }
        Ok(())
    }

    pub fn listen(&self) -> &str {
        self.listen.as_deref().unwrap_or(DEFAULT_LISTEN)
    }

    pub fn database_path(&self) -> &str {
        self.database_path
            .as_deref()
            .unwrap_or(DEFAULT_DATABASE_PATH)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_hide_adaptive_details() {
        let config: AppConfig = toml::from_str("[[workers]]\nname = 'email'\n").expect("parse");
        let options = config.persistence.write_options().expect("options");
        assert_eq!(options.ewma_window, DEFAULT_EWMA_WINDOW);
        assert_eq!(options.durability, DurabilityMode::Balanced);
    }

    #[test]
    fn custom_limits_and_window_are_applied() {
        let config: AppConfig = toml::from_str(
            "[persistence]\newma_window = 8\n[persistence.limits]\nbatch_size_min = 4\nbatch_size_max = 32\nbatch_wait_min_ms = 2\nbatch_wait_max_ms = 20\n[persistence.adaptation]\nbatch_probe_factor = 1.2\nbatch_backoff_factor = 0.7\n",
        )
        .expect("parse");
        let options = config.persistence.write_options().expect("options");
        assert_eq!(options.ewma_window, 8);
        assert_eq!(options.limits.batch_size_min, 4);
        assert_eq!(options.limits.batch_size_max, 32);
        assert_eq!(options.limits.batch_wait_min, Duration::from_millis(2));
        assert_eq!(options.limits.batch_wait_max, Duration::from_millis(20));
        assert_eq!(options.batch_probe_factor, 1.2);
        assert_eq!(options.batch_backoff_factor, 0.7);
    }

    #[test]
    fn rejects_retired_batching_knobs_and_invalid_limits() {
        let retired: Result<AppConfig, _> = toml::from_str("[persistence]\nbatch_size = 64\n");
        assert!(retired.is_err());

        let config: AppConfig = toml::from_str(
            "[persistence]\newma_window = 0\n[persistence.limits]\nbatch_size_min = 8\nbatch_size_max = 4\n",
        )
        .expect("parse");
        assert!(config.validate().is_err());

        let invalid_adaptation: AppConfig = toml::from_str(
            "[persistence.adaptation]\nbatch_probe_factor = 1.0\nbatch_backoff_factor = 1.0\n",
        )
        .expect("parse");
        assert!(invalid_adaptation.validate().is_err());
    }

    #[test]
    fn parses_strict_durability_and_preserve_startup_policy() {
        let config: AppConfig =
            toml::from_str("[persistence]\ndurability = 'strict'\nstartup = 'preserve'\n")
                .expect("parse");
        assert_eq!(config.persistence.durability, DurabilityMode::Strict);
        assert_eq!(config.persistence.startup, StartupPolicy::Preserve);
    }
}
