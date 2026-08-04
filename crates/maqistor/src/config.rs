use std::{collections::HashMap, fs, path::Path, time::Duration};

use chrono::{DateTime, FixedOffset, NaiveTime, Utc};
use humantime::parse_duration;
use maqistor_engine::DispatchOptions;
use maqistor_persistence::{
    BatchOptions, DurabilityMode, SqliteWriteOptions, default_results_path,
};
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
    pub docker: DockerConfig,
    #[serde(default)]
    pub persistence: PersistenceConfig,
    #[serde(default)]
    pub dispatch: DispatchConfig,
    #[serde(default)]
    pub queues: Vec<QueueConfig>,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DockerConfig {
    pub endpoint: Option<String>,
    pub ca_cert_path: Option<String>,
    pub cert_path: Option<String>,
    pub key_path: Option<String>,
}

impl DockerConfig {
    pub fn connect_options(&self) -> maqistor_dispatcher::DockerConnectOptions {
        maqistor_dispatcher::DockerConnectOptions {
            endpoint: self.endpoint.clone(),
            ca_cert_path: self.ca_cert_path.clone(),
            cert_path: self.cert_path.clone(),
            key_path: self.key_path.clone(),
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PersistenceConfig {
    pub ingest_database: Option<String>,
    pub results_database: Option<String>,
    #[serde(default)]
    pub durability: DurabilityMode,
    #[serde(default)]
    pub cleanup: Option<CleanupConfig>,
    #[serde(default)]
    pub startup: StartupPolicy,
    #[serde(default)]
    pub enqueue: BatchConfig,
    #[serde(default)]
    pub completion: BatchConfig,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CleanupConfig {
    interval: String,
    retention: String,
    vacuum: Option<VacuumConfig>,
}

impl CleanupConfig {
    pub fn interval(&self) -> anyhow::Result<Duration> {
        parse_duration(&self.interval)
            .map_err(|err| anyhow::anyhow!("invalid cleanup.interval {:?}: {err}", self.interval))
    }

    pub fn retention(&self) -> anyhow::Result<Duration> {
        parse_duration(&self.retention)
            .map_err(|err| anyhow::anyhow!("invalid cleanup.retention {:?}: {err}", self.retention))
    }

    pub fn vacuum(&self) -> Option<VacuumConfig> {
        if let Some(vacuum) = &self.vacuum {
            Some(vacuum.clone())
        } else {
            None
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VacuumConfig {
    every: String,
    at: String,
}

impl VacuumConfig {
    pub fn is_due(&self, last_run: Option<i64>) -> anyhow::Result<bool> {
        let now = Utc::now();
        let at = self.at()?;
        let every = self.every()?;

        let spacing_ok = match last_run {
            None => true,
            Some(last) => {
                let every_ms = i64::try_from(every.as_millis())
                    .map_err(|_| anyhow::anyhow!("vacuum.every too large"))?;
                last.saturating_add(every_ms) <= now.timestamp_millis()
            }
        };
        let at_ok = now.time() >= at;
        Ok(spacing_ok && at_ok)
    }

    fn every(&self) -> anyhow::Result<Duration> {
        parse_duration(&self.every)
            .map_err(|err| anyhow::anyhow!("invalid vacuum.every {:?}: {err}", self.every))
    }

    fn at(&self) -> anyhow::Result<NaiveTime> {
        let stamped = if self.at.contains('+') || self.at.rfind('-').is_some_and(|i| i > 0) {
            let normalized = self.at.trim().replacen(' ', "", 1);
            format!("1970-01-01T{normalized}:00")
        } else {
            format!("1970-01-01T{}:00+00:00", self.at.trim())
        };
        let when: DateTime<FixedOffset> = stamped.parse()?;
        Ok(when.with_timezone(&Utc).time())
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

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DispatchConfig {
    pub batch_size_max: Option<usize>,
    #[serde(alias = "max_in_flight")]
    pub max_delivery_in_flight: Option<usize>,
    pub idle_probe_interval_ms: Option<u64>,
    pub idle_probe_batch_size: Option<usize>,
}

impl DispatchConfig {
    pub fn options(&self) -> anyhow::Result<DispatchOptions> {
        let mut options = DispatchOptions::default();
        options.batch_size_max = self.batch_size_max.unwrap_or(options.batch_size_max);
        options.max_delivery_in_flight = self
            .max_delivery_in_flight
            .unwrap_or(options.max_delivery_in_flight);
        options.idle_probe_interval = Duration::from_millis(
            self.idle_probe_interval_ms
                .unwrap_or(options.idle_probe_interval.as_millis() as u64),
        );
        options.idle_probe_batch_size = self
            .idle_probe_batch_size
            .unwrap_or(options.idle_probe_batch_size);
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

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueConfig {
    pub name: String,
    pub max_retries: u32,
    pub timeout_secs: u64,
    pub managed_config: Option<ManagedConfig>,
}

impl QueueConfig {
    pub fn replicas(&self) -> u32 {
        self.managed_config
            .as_ref()
            .map(|c| c.replicas)
            .unwrap_or(1)
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedConfig {
    pub image: String,
    pub replicas: u32,
    pub env_file: Option<String>,
    pub env_vars: Option<HashMap<String, String>>,
}

impl ManagedConfig {
    pub fn env(&self) -> anyhow::Result<Vec<String>> {
        let mut res: Vec<String> = self
            .env_vars
            .as_ref()
            .map(|vars| vars.iter().map(|(k, v)| format!("{k}={v}")).collect())
            .unwrap_or_default();
        if let Some(env_file) = &self.env_file {
            let contents = fs::read_to_string(env_file)
                .map_err(|err| anyhow::anyhow!("failed to read env file {env_file:?}: {err}"));
            match contents {
                Ok(contents) => {
                    res.extend(contents.lines().map(|line| line.trim().to_string()));
                }
                Err(err) => {
                    anyhow::bail!("failed to read env file {env_file:?}: {err}");
                }
            }
        }
        Ok(res)
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
        validate_docker_config(&self.docker)?;
        if let Some(cleanup) = &self.persistence.cleanup {
            validate_cleanup_config(cleanup)?;
        }
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
            if let Some(config) = &queue.managed_config {
                if config.image.is_empty() || config.replicas == 0 {
                    anyhow::bail!(
                        "managed queue {} requires an image and positive replicas",
                        queue.name
                    )
                }
                validate_managed_image(&config.image)?;
                validate_managed_env_file(&config.env_file)?;
                validate_managed_env_vars(&config.env_vars)?;
            }
        }
        Ok(())
    }

    pub fn has_managed_queues(&self) -> bool {
        self.queues.iter().any(|q| q.managed_config.is_some())
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

fn validate_docker_config(docker: &DockerConfig) -> anyhow::Result<()> {
    let tls_paths = [
        docker.ca_cert_path.as_deref(),
        docker.cert_path.as_deref(),
        docker.key_path.as_deref(),
    ];
    let tls_set = tls_paths.iter().filter(|p| p.is_some()).count();
    if tls_set != 0 && tls_set != 3 {
        anyhow::bail!("docker TLS requires ca_cert_path, cert_path, and key_path together");
    }
    if docker
        .endpoint
        .as_deref()
        .is_some_and(|endpoint| endpoint.starts_with("https://"))
        && tls_set != 3
    {
        anyhow::bail!("https:// Docker endpoints require ca_cert_path, cert_path, and key_path");
    }
    if tls_set == 3 {
        match docker.endpoint.as_deref() {
            None => anyhow::bail!("docker TLS requires a tcp:// or https:// endpoint"),
            Some(endpoint)
                if !(endpoint.starts_with("tcp://") || endpoint.starts_with("https://")) =>
            {
                anyhow::bail!("docker TLS is only valid with a tcp:// or https:// endpoint");
            }
            Some(_) => {}
        }
    }
    if let Some(endpoint) = docker.endpoint.as_deref() {
        let ok = endpoint.starts_with("unix://")
            || endpoint.starts_with("npipe://")
            || endpoint.starts_with("tcp://")
            || endpoint.starts_with("http://")
            || endpoint.starts_with("https://");
        if !ok {
            anyhow::bail!(
                "docker.endpoint must use unix://, npipe://, tcp://, http://, or https://"
            );
        }
        if endpoint.trim_end_matches('/').ends_with("://")
            || endpoint == "unix://"
            || endpoint == "npipe://"
        {
            anyhow::bail!("docker.endpoint must include a path or host");
        }
    }
    Ok(())
}

fn validate_cleanup_config(cleanup: &CleanupConfig) -> anyhow::Result<()> {
    if cleanup.interval.trim().is_empty() {
        anyhow::bail!("cleanup.interval must be set");
    }
    if cleanup.retention.trim().is_empty() {
        anyhow::bail!("cleanup.retention must be set");
    }
    let interval = cleanup.interval()?;
    let retention = cleanup.retention()?;
    if interval.is_zero() {
        anyhow::bail!("cleanup.interval must be greater than zero");
    }
    if retention.is_zero() {
        anyhow::bail!("cleanup.retention must be greater than zero");
    }
    if retention.as_millis() > i64::MAX as u128 {
        anyhow::bail!("cleanup.retention is too large");
    }
    Ok(())
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

fn validate_managed_env_file(env_file: &Option<String>) -> anyhow::Result<()> {
    if let Some(env_file) = env_file {
        let path = Path::new(env_file);
        if !path.is_file() {
            anyhow::bail!("managed env file {path:?} does not exist");
        }
    }
    Ok(())
}

fn validate_managed_env_vars(env_vars: &Option<HashMap<String, String>>) -> anyhow::Result<()> {
    if let Some(vars) = env_vars {
        for (key, value) in vars {
            if key.is_empty() || value.is_empty() {
                anyhow::bail!("managed env vars must be nonempty");
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    const TLS: &str = "[worker_tls]\nca_cert_path = 'ca.pem'\ncert_path = 'server.pem'\nkey_path = 'server.key'\n";

    #[test]
    fn defaults_hide_adaptive_details() {
        let config: AppConfig = toml::from_str(&format!(
            "{TLS}[[queues]]\nname = 'email'\nmax_retries = 3\ntimeout_secs = 60\n"
        ))
        .expect("parse");
        let options = config.persistence.write_options().expect("options");
        assert_eq!(options.enqueue.ewma_window, 16);
        assert_eq!(options.durability, DurabilityMode::None);
        assert_eq!(options.completion.batch_wait_max, Duration::from_millis(20));
    }

    #[test]
    fn custom_limits_and_window_are_applied() {
        let config: AppConfig = toml::from_str(&format!("{TLS}[persistence.enqueue]\newma_window = 8\nbatch_size_min = 4\nbatch_size_max = 32\nbatch_wait_min_ms = 2\nbatch_wait_max_ms = 20\nbatch_probe_factor = 1.2\nbatch_backoff_factor = 0.7\n[persistence.completion]\nbatch_wait_max_ms = 10\n[dispatch]\nbatch_size_max = 2048\nmax_delivery_in_flight = 64\nidle_probe_interval_ms = 250\nidle_probe_batch_size = 12\n"))
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
        assert_eq!(
            config.dispatch.options().unwrap().idle_probe_interval,
            Duration::from_millis(250)
        );
        assert_eq!(config.dispatch.options().unwrap().idle_probe_batch_size, 12);

        let legacy: AppConfig = toml::from_str(&format!("{TLS}[dispatch]\nmax_in_flight = 32\n"))
            .expect("parse legacy limit");
        assert_eq!(
            legacy.dispatch.options().unwrap().max_delivery_in_flight,
            32
        );
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

        let zero_probe: AppConfig = toml::from_str(&format!(
            "{TLS}[dispatch]\nidle_probe_interval_ms = 0\nidle_probe_batch_size = 0\n"
        ))
        .expect("parse");
        assert!(zero_probe.dispatch.options().is_err());
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
    fn validates_cleanup_interval_and_retention() {
        let ok: AppConfig = toml::from_str(&format!(
            "{TLS}[persistence.cleanup]\ninterval = '1h'\nretention = '7d'\n"
        ))
        .expect("parse");
        assert!(ok.validate().is_ok());

        let bad_interval: AppConfig = toml::from_str(&format!(
            "{TLS}[persistence.cleanup]\ninterval = 'nope'\nretention = '7d'\n"
        ))
        .expect("parse");
        assert!(bad_interval.validate().is_err());

        let zero_retention: AppConfig = toml::from_str(&format!(
            "{TLS}[persistence.cleanup]\ninterval = '1h'\nretention = '0s'\n"
        ))
        .expect("parse");
        assert!(zero_retention.validate().is_err());
    }

    #[test]
    fn database_paths_live_under_persistence() {
        let defaults: AppConfig = toml::from_str(TLS).expect("parse");
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
        assert_eq!(
            config.persistence.ingest_database_path(),
            "./data/ingest.db"
        );
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

    #[test]
    fn docker_section_defaults_and_parses_endpoint() {
        let defaults: AppConfig = toml::from_str(TLS).expect("parse");
        assert_eq!(defaults.docker, DockerConfig::default());
        assert!(!defaults.has_managed_queues());
        assert!(defaults.validate().is_ok());

        let remote: AppConfig = toml::from_str(&format!(
            "{TLS}[docker]\nendpoint = 'tcp://192.168.1.10:2375'\n"
        ))
        .expect("parse");
        assert_eq!(
            remote.docker.endpoint.as_deref(),
            Some("tcp://192.168.1.10:2375")
        );
        assert!(remote.validate().is_ok());

        let local: AppConfig = toml::from_str(&format!(
            "{TLS}[docker]\nendpoint = 'unix:///var/run/docker.sock'\n"
        ))
        .expect("parse");
        assert!(local.validate().is_ok());
    }

    #[test]
    fn docker_tls_requires_explicit_mtls_paths() {
        let partial: AppConfig = toml::from_str(&format!(
            "{TLS}[docker]\nendpoint = 'tcp://192.168.1.10:2376'\nca_cert_path = 'ca.pem'\n"
        ))
        .expect("parse");
        assert!(partial.validate().is_err());

        let tls_without_endpoint: AppConfig = toml::from_str(&format!(
            "{TLS}[docker]\nca_cert_path = 'ca.pem'\ncert_path = 'cert.pem'\nkey_path = 'key.pem'\n"
        ))
        .expect("parse");
        assert!(tls_without_endpoint.validate().is_err());

        let tls_on_unix: AppConfig = toml::from_str(&format!(
            "{TLS}[docker]\nendpoint = 'unix:///var/run/docker.sock'\nca_cert_path = 'ca.pem'\ncert_path = 'cert.pem'\nkey_path = 'key.pem'\n"
        ))
        .expect("parse");
        assert!(tls_on_unix.validate().is_err());

        let https_without_mtls: AppConfig = toml::from_str(&format!(
            "{TLS}[docker]\nendpoint = 'https://docker.example:2376'\n"
        ))
        .expect("parse");
        assert!(https_without_mtls.validate().is_err());

        let tls_on_http: AppConfig = toml::from_str(&format!(
            "{TLS}[docker]\nendpoint = 'http://docker.example:2376'\nca_cert_path = 'ca.pem'\ncert_path = 'cert.pem'\nkey_path = 'key.pem'\n"
        ))
        .expect("parse");
        assert!(tls_on_http.validate().is_err());

        let ok: AppConfig = toml::from_str(&format!(
            "{TLS}[docker]\nendpoint = 'tcp://192.168.1.10:2376'\nca_cert_path = 'ca.pem'\ncert_path = 'cert.pem'\nkey_path = 'key.pem'\n"
        ))
        .expect("parse");
        assert!(ok.validate().is_ok());
        assert_eq!(
            ok.docker.connect_options().endpoint.as_deref(),
            Some("tcp://192.168.1.10:2376")
        );

        let https: AppConfig = toml::from_str(&format!(
            "{TLS}[docker]\nendpoint = 'https://docker.example:2376'\nca_cert_path = 'ca.pem'\ncert_path = 'cert.pem'\nkey_path = 'key.pem'\n"
        ))
        .expect("parse");
        assert!(https.validate().is_ok());
    }

    #[test]
    fn rejects_unsupported_docker_endpoint_scheme() {
        let bad: AppConfig =
            toml::from_str(&format!("{TLS}[docker]\nendpoint = 'ssh://host'\n")).expect("parse");
        assert!(bad.validate().is_err());
    }

    #[test]
    fn has_managed_queues_detects_managed_config() {
        let managed: AppConfig = toml::from_str(&format!(
            "{TLS}[[queues]]\nname = 'email'\nmax_retries = 3\ntimeout_secs = 60\n[queues.managed_config]\nimage = 'ghcr.io/example/email:1.0.0'\nreplicas = 2\n"
        ))
        .expect("parse");
        assert!(managed.has_managed_queues());
        assert!(managed.validate().is_ok());
    }
}
