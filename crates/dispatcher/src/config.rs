use std::fs;
use std::path::Path;
use std::time::Duration;

use maqistor_persistence::SqliteWriteOptions;
use serde::Deserialize;

const DEFAULT_LISTEN: &str = "0.0.0.0:8080";
const DEFAULT_DATABASE_PATH: &str = "./data/maqistor.db";
const DEFAULT_BATCH_SIZE: usize = 64;
const DEFAULT_BATCH_SIZE_MIN: usize = 8;
const DEFAULT_BATCH_SIZE_MAX: usize = 256;
const DEFAULT_BATCH_SIZE_INCREASE: usize = 2;
const DEFAULT_BATCH_WAIT_MS: u64 = 5;
const DEFAULT_BATCH_WAIT_MIN_MS: u64 = 1;
const DEFAULT_BATCH_WAIT_MAX_MS: u64 = 100;

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    /// Defaults to `0.0.0.0:8080` when omitted.
    pub listen: Option<String>,
    /// Defaults to `./data/maqistor.db` when omitted.
    pub database_path: Option<String>,
    #[serde(default)]
    pub persistence: PersistenceConfig,
    #[serde(default)]
    pub jobs: Vec<JobConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PersistenceConfig {
    /// Fixed batch size, or initial size when `adaptive_batch_size` is true.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    #[serde(default)]
    pub adaptive_batch_size: bool,
    #[serde(default = "default_batch_size_min")]
    pub batch_size_min: usize,
    #[serde(default = "default_batch_size_max")]
    pub batch_size_max: usize,
    /// Additive step on full-batch flush when adaptive.
    #[serde(default = "default_batch_size_increase")]
    pub batch_size_increase: usize,
    #[serde(default)]
    pub adaptive_batch_wait: bool,
    #[serde(default = "default_batch_wait_ms")]
    pub batch_wait_ms: u64,
    #[serde(default = "default_batch_wait_min_ms")]
    pub batch_wait_min_ms: u64,
    #[serde(default = "default_batch_wait_max_ms")]
    pub batch_wait_max_ms: u64,
}

impl Default for PersistenceConfig {
    fn default() -> Self {
        Self {
            batch_size: default_batch_size(),
            adaptive_batch_size: false,
            batch_size_min: default_batch_size_min(),
            batch_size_max: default_batch_size_max(),
            batch_size_increase: default_batch_size_increase(),
            adaptive_batch_wait: false,
            batch_wait_ms: default_batch_wait_ms(),
            batch_wait_min_ms: default_batch_wait_min_ms(),
            batch_wait_max_ms: default_batch_wait_max_ms(),
        }
    }
}

impl PersistenceConfig {
    pub fn write_options(&self) -> SqliteWriteOptions {
        let batch_size = self.batch_size.max(1);
        let (batch_size_min, batch_size_max) = if self.adaptive_batch_size {
            let min = self.batch_size_min.max(1);
            let max = self.batch_size_max.max(min);
            (min, max)
        } else {
            (batch_size, batch_size)
        };

        let wait = Duration::from_millis(self.batch_wait_ms);
        let (batch_wait, batch_wait_min, batch_wait_max) = if self.adaptive_batch_wait {
            let min = Duration::from_millis(self.batch_wait_min_ms);
            let max = Duration::from_millis(self.batch_wait_max_ms.max(self.batch_wait_min_ms));
            (min, min, max)
        } else {
            (wait, wait, wait)
        };

        SqliteWriteOptions {
            batch_size,
            adaptive_batch_size: self.adaptive_batch_size,
            batch_size_min,
            batch_size_max,
            batch_size_increase: self.batch_size_increase.max(1),
            adaptive_batch_wait: self.adaptive_batch_wait,
            batch_wait,
            batch_wait_min,
            batch_wait_max,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct JobConfig {
    pub name: String,
    #[serde(default = "default_concurrency")]
    pub concurrency: u32,
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
}

fn default_concurrency() -> u32 {
    1
}

fn default_max_retries() -> u32 {
    3
}

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

fn default_batch_size_min() -> usize {
    DEFAULT_BATCH_SIZE_MIN
}

fn default_batch_size_max() -> usize {
    DEFAULT_BATCH_SIZE_MAX
}

fn default_batch_size_increase() -> usize {
    DEFAULT_BATCH_SIZE_INCREASE
}

fn default_batch_wait_ms() -> u64 {
    DEFAULT_BATCH_WAIT_MS
}

fn default_batch_wait_min_ms() -> u64 {
    DEFAULT_BATCH_WAIT_MIN_MS
}

fn default_batch_wait_max_ms() -> u64 {
    DEFAULT_BATCH_WAIT_MAX_MS
}

impl AppConfig {
    pub fn load(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref();
        let contents = fs::read_to_string(path)
            .map_err(|err| anyhow::anyhow!("failed to read config {}: {err}", path.display()))?;
        let config: Self = toml::from_str(&contents)
            .map_err(|err| anyhow::anyhow!("failed to parse config {}: {err}", path.display()))?;
        Ok(config)
    }

    pub fn listen(&self) -> &str {
        self.listen.as_deref().unwrap_or(DEFAULT_LISTEN)
    }

    pub fn database_path(&self) -> &str {
        self.database_path.as_deref().unwrap_or(DEFAULT_DATABASE_PATH)
    }
}
