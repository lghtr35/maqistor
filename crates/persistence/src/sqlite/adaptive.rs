use std::time::{Duration, Instant};

use maqistor_engine::{AdaptiveBatch, DirectionStreak, Ewma};

use super::options::BatchOptions;

pub(crate) const LOW_FILL_TIMEOUTS: u8 = 3;

const WAIT_ADJUST_UP: f64 = 1.25;
const WAIT_ADJUST_DOWN: f64 = 0.80;
const WAIT_DIRECTION_HIGH: f64 = 1.20;
const WAIT_DIRECTION_LOW: f64 = 0.80;
const MAX_QUEUEING_RATIO: f64 = 1.20;
const TARGET_FILL_RATIO: f64 = 0.75;
const BASELINE_RELAXATION: f64 = 0.02;
const LOW_FILL_RATIO: f64 = 0.50;

#[derive(Debug, Clone, Copy)]
pub(crate) enum FlushReason {
    FullBatch,
    Timeout,
}

pub(crate) struct AdaptiveBatchController {
    limits: BatchOptions,
    request_rate: Ewma,
    commit_rate: Ewma,
    commit_duration: Ewma,
    fill_ratio: Ewma,
    baseline_commit_duration: Option<f64>,
    batch: AdaptiveBatch,
    pub(crate) batch_wait: Duration,
    backlog: usize,
    pub(crate) low_fill_timeouts: u8,
    last_request: Option<Instant>,
    last_commit: Option<Instant>,
    wait_direction_streak: DirectionStreak,
}

impl AdaptiveBatchController {
    pub(crate) fn new(options: &BatchOptions) -> Self {
        let limits = options.clone();
        Self {
            batch: AdaptiveBatch::new(
                limits.batch_size_min,
                limits.batch_size_max,
                options.batch_probe_factor,
                options.batch_backoff_factor,
            ),
            batch_wait: limits.batch_wait_min,
            backlog: 0,
            low_fill_timeouts: 0,
            limits,
            request_rate: Ewma::new(options.ewma_window),
            commit_rate: Ewma::new(options.ewma_window),
            commit_duration: Ewma::new(options.ewma_window),
            fill_ratio: Ewma::new(options.ewma_window),
            baseline_commit_duration: None,
            last_request: None,
            last_commit: None,
            wait_direction_streak: DirectionStreak::default(),
        }
    }

    pub(crate) fn observe_request(&mut self, now: Instant) {
        if let Some(previous) = self.last_request.replace(now) {
            let elapsed = now.saturating_duration_since(previous).as_secs_f64();
            if elapsed > 0.0 {
                self.request_rate.observe(1.0 / elapsed);
            }
        }
    }

    pub(crate) fn record_successful_commit(
        &mut self,
        filled: usize,
        elapsed: Duration,
        completed_at: Instant,
        backlog: usize,
        reason: FlushReason,
    ) {
        self.backlog = backlog;
        let duration = elapsed.as_secs_f64();
        self.commit_duration.observe(duration);
        self.observe_commit_baseline(duration);
        if let Some(previous) = self.last_commit.replace(completed_at) {
            let interval = completed_at
                .saturating_duration_since(previous)
                .as_secs_f64();
            if interval > 0.0 {
                self.commit_rate.observe(1.0 / interval);
            }
        }
        let fill_ratio = filled as f64 / self.batch.size().max(1) as f64;
        self.fill_ratio.observe(fill_ratio);
        if matches!(reason, FlushReason::Timeout) && backlog == 0 && fill_ratio < LOW_FILL_RATIO {
            self.low_fill_timeouts = self.low_fill_timeouts.saturating_add(1);
        } else {
            self.low_fill_timeouts = 0;
        }
        self.adjust_batch_size();
        self.adjust_batch_wait();
    }

    fn observe_commit_baseline(&mut self, sample: f64) {
        self.baseline_commit_duration = Some(match self.baseline_commit_duration {
            None => sample,
            Some(baseline) if sample < baseline => sample,
            Some(baseline) => baseline + (sample - baseline) * BASELINE_RELAXATION,
        });
    }

    pub(crate) fn adjust_batch_size(&mut self) {
        let Some(commit_duration) = self.commit_duration.value() else {
            return;
        };
        let Some(baseline) = self.baseline_commit_duration else {
            return;
        };
        let queueing_ratio = commit_duration / baseline.max(f64::MIN_POSITIVE);

        if self.low_fill_timeouts >= LOW_FILL_TIMEOUTS && queueing_ratio <= MAX_QUEUEING_RATIO {
            self.batch.set_size(
                (self.batch.size() as f64 * self.limits.batch_backoff_factor).floor() as usize,
            );
            self.low_fill_timeouts = 0;
            self.batch.reset_direction();
            return;
        }

        let demand_exceeds_service = match (self.request_rate.value(), self.commit_rate.value()) {
            (Some(request_rate), Some(commit_rate)) if commit_rate > 0.0 => {
                request_rate > self.batch.size() as f64 * commit_rate
            }
            _ => false,
        };
        let direction = if queueing_ratio > MAX_QUEUEING_RATIO {
            -1
        } else if self.backlog > 0 || demand_exceeds_service {
            1
        } else {
            0
        };
        self.batch.observe_direction(direction);
    }

    pub(crate) fn adjust_batch_wait(&mut self) {
        let Some(request_rate) = self.request_rate.value().filter(|rate| *rate > 0.0) else {
            return;
        };
        let desired = Duration::from_secs_f64(
            (self.batch.size() as f64 * TARGET_FILL_RATIO / request_rate)
                .max(self.limits.batch_wait_min.as_secs_f64()),
        )
        .clamp(self.limits.batch_wait_min, self.limits.batch_wait_max);
        let direction =
            if desired.as_secs_f64() > self.batch_wait.as_secs_f64() * WAIT_DIRECTION_HIGH {
                1
            } else if desired.as_secs_f64() < self.batch_wait.as_secs_f64() * WAIT_DIRECTION_LOW {
                -1
            } else {
                0
            };
        if !self.wait_direction_streak.confirm(direction) {
            return;
        }
        let next = match direction {
            1 => {
                Duration::from_secs_f64(self.batch_wait.as_secs_f64() * WAIT_ADJUST_UP).min(desired)
            }
            -1 => Duration::from_secs_f64(self.batch_wait.as_secs_f64() * WAIT_ADJUST_DOWN)
                .max(desired),
            _ => self.batch_wait,
        };
        self.batch_wait = next.clamp(self.limits.batch_wait_min, self.limits.batch_wait_max);
    }

    pub(crate) fn batch_size(&self) -> usize {
        self.batch.size()
    }

    #[cfg(test)]
    pub(crate) fn set_batch_size(&mut self, size: usize) {
        self.batch.set_size(size);
    }
}

#[cfg(test)]
impl AdaptiveBatchController {
    pub(crate) fn request_rate_mut(&mut self) -> &mut Ewma {
        &mut self.request_rate
    }

    pub(crate) fn commit_rate_mut(&mut self) -> &mut Ewma {
        &mut self.commit_rate
    }

    pub(crate) fn commit_duration_mut(&mut self) -> &mut Ewma {
        &mut self.commit_duration
    }

    pub(crate) fn set_baseline_commit_duration(&mut self, value: f64) {
        self.baseline_commit_duration = Some(value);
    }

    pub(crate) fn set_backlog(&mut self, backlog: usize) {
        self.backlog = backlog;
    }
}
