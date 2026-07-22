use std::time::Duration;

use serde::Deserialize;

const DEFAULT_ENQUEUE_BATCH_SIZE_MIN: usize = 64;
const DEFAULT_ENQUEUE_BATCH_SIZE_MAX: usize = 8_192;
const DEFAULT_ENQUEUE_BATCH_WAIT_MIN_MS: u64 = 1;
const DEFAULT_ENQUEUE_BATCH_WAIT_MAX_MS: u64 = 100;
const DEFAULT_COMPLETION_BATCH_SIZE_MIN: usize = 128;
const DEFAULT_COMPLETION_BATCH_SIZE_MAX: usize = 8_192;
const DEFAULT_COMPLETION_BATCH_WAIT_MIN_MS: u64 = 1;
const DEFAULT_COMPLETION_BATCH_WAIT_MAX_MS: u64 = 20;
const DEFAULT_EWMA_WINDOW: usize = 16;
const DEFAULT_BATCH_PROBE_FACTOR: f64 = 1.10;
const DEFAULT_BATCH_BACKOFF_FACTOR: f64 = 0.80;

#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum DurabilityMode {
    #[default]
    Balanced,
    Strict,
}

#[derive(Debug, Clone)]
pub struct BatchOptions {
    pub batch_size_min: usize,
    pub batch_size_max: usize,
    pub batch_wait_min: Duration,
    pub batch_wait_max: Duration,
    pub ewma_window: usize,
    pub batch_probe_factor: f64,
    pub batch_backoff_factor: f64,
}

impl BatchOptions {
    pub(crate) fn enqueue_defaults() -> Self {
        Self {
            batch_size_min: DEFAULT_ENQUEUE_BATCH_SIZE_MIN,
            batch_size_max: DEFAULT_ENQUEUE_BATCH_SIZE_MAX,
            batch_wait_min: Duration::from_millis(DEFAULT_ENQUEUE_BATCH_WAIT_MIN_MS),
            batch_wait_max: Duration::from_millis(DEFAULT_ENQUEUE_BATCH_WAIT_MAX_MS),
            ewma_window: DEFAULT_EWMA_WINDOW,
            batch_probe_factor: DEFAULT_BATCH_PROBE_FACTOR,
            batch_backoff_factor: DEFAULT_BATCH_BACKOFF_FACTOR,
        }
    }

    pub(crate) fn completion_defaults() -> Self {
        Self {
            batch_size_min: DEFAULT_COMPLETION_BATCH_SIZE_MIN,
            batch_size_max: DEFAULT_COMPLETION_BATCH_SIZE_MAX,
            batch_wait_min: Duration::from_millis(DEFAULT_COMPLETION_BATCH_WAIT_MIN_MS),
            batch_wait_max: Duration::from_millis(DEFAULT_COMPLETION_BATCH_WAIT_MAX_MS),
            ewma_window: DEFAULT_EWMA_WINDOW,
            batch_probe_factor: 1.25,
            batch_backoff_factor: DEFAULT_BATCH_BACKOFF_FACTOR,
        }
    }

    fn validate(&self, section: &str) -> Result<(), String> {
        if self.ewma_window == 0 {
            return Err(format!("{section}.ewma_window must be greater than zero"));
        }
        if !self.batch_probe_factor.is_finite() || self.batch_probe_factor <= 1.0 {
            return Err(format!(
                "{section}.batch_probe_factor must be greater than one"
            ));
        }
        if !self.batch_backoff_factor.is_finite()
            || !(0.0..1.0).contains(&self.batch_backoff_factor)
        {
            return Err(format!(
                "{section}.batch_backoff_factor must be greater than zero and less than one"
            ));
        }
        if self.batch_size_min == 0 || self.batch_size_min > self.batch_size_max {
            return Err(format!(
                "{section}.batch_size_min must be positive and not exceed batch_size_max"
            ));
        }
        if self.batch_wait_min.is_zero() || self.batch_wait_min > self.batch_wait_max {
            return Err(format!(
                "{section}.batch_wait_min_ms must be positive and not exceed batch_wait_max_ms"
            ));
        }
        Ok(())
    }
}

impl Default for BatchOptions {
    fn default() -> Self {
        Self::enqueue_defaults()
    }
}

#[derive(Debug, Clone)]
pub struct SqliteWriteOptions {
    pub durability: DurabilityMode,
    pub enqueue: BatchOptions,
    pub completion: BatchOptions,
}

impl Default for SqliteWriteOptions {
    fn default() -> Self {
        Self {
            durability: DurabilityMode::default(),
            enqueue: BatchOptions::enqueue_defaults(),
            completion: BatchOptions::completion_defaults(),
        }
    }
}

impl SqliteWriteOptions {
    pub fn validate(&self) -> Result<(), String> {
        self.enqueue.validate("persistence.enqueue")?;
        self.completion.validate("persistence.completion")?;
        Ok(())
    }
}
