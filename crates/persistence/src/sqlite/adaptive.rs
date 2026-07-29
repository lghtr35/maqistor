use std::time::{Duration, Instant};

use maqistor_engine::{AdaptiveBatch, DirectionStreak, Ewma};

use super::options::BatchOptions;

pub(crate) const LOW_FILL_TIMEOUTS: u8 = 3;
const MAX_RESULTS_PREFERRED_LANE_TURNS: usize = 32;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ResultsLane {
    Dispatch,
    Completion,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ResultsLaneSelectionReason {
    OnlyActive,
    InitialTieBreak,
    Preferred,
    StarvationOverride,
}

pub(crate) struct ResultsLaneController {
    dispatch_rows_per_turn: Ewma,
    dispatch_turn_duration: Ewma,
    completion_rows_per_turn: Ewma,
    completion_turn_duration: Ewma,
    shared_rows_per_turn: Ewma,
    shared_turn_duration: Ewma,
    preferred: Option<ResultsLane>,
    direction_streak: DirectionStreak,
    preferred_turns: usize,
}

impl ResultsLaneController {
    pub(crate) fn new(ewma_window: usize) -> Self {
        Self {
            dispatch_rows_per_turn: Ewma::new(ewma_window),
            dispatch_turn_duration: Ewma::new(ewma_window),
            completion_rows_per_turn: Ewma::new(ewma_window),
            completion_turn_duration: Ewma::new(ewma_window),
            shared_rows_per_turn: Ewma::new(ewma_window),
            shared_turn_duration: Ewma::new(ewma_window),
            preferred: None,
            direction_streak: DirectionStreak::default(),
            preferred_turns: 0,
        }
    }

    pub(crate) fn select(
        &mut self,
        dispatch_rows: usize,
        dispatch_oldest: Option<Instant>,
        completion_rows: usize,
        completion_oldest: Option<Instant>,
        now: Instant,
    ) -> Option<(ResultsLane, ResultsLaneSelectionReason)> {
        let dispatch_active = dispatch_rows > 0;
        let completion_active = completion_rows > 0;
        match (dispatch_active, completion_active) {
            (false, false) => return None,
            (true, false) => {
                self.preferred_turns = 0;
                return Some((ResultsLane::Dispatch, ResultsLaneSelectionReason::OnlyActive));
            }
            (false, true) => {
                self.preferred_turns = 0;
                return Some((ResultsLane::Completion, ResultsLaneSelectionReason::OnlyActive));
            }
            (true, true) => {}
        }

        let dispatch_pressure =
            self.pressure(ResultsLane::Dispatch, dispatch_rows, dispatch_oldest, now);
        let completion_pressure = self.pressure(
            ResultsLane::Completion,
            completion_rows,
            completion_oldest,
            now,
        );
        let direction = match (dispatch_pressure, completion_pressure) {
            (Some(dispatch), Some(completion)) if completion > dispatch => 1,
            (Some(dispatch), Some(completion)) if dispatch > completion => -1,
            _ => 0,
        };
        if self.direction_streak.confirm(direction) {
            let next_preferred = match direction {
                1 => Some(ResultsLane::Completion),
                -1 => Some(ResultsLane::Dispatch),
                _ => self.preferred,
            };
            if next_preferred != self.preferred {
                self.preferred = next_preferred;
                self.preferred_turns = 0;
            }
        }

        let Some(preferred) = self.preferred else {
            return Some((
                ResultsLane::Completion,
                ResultsLaneSelectionReason::InitialTieBreak,
            ));
        };
        if self.preferred_turns >= MAX_RESULTS_PREFERRED_LANE_TURNS {
            return Some((
                other_lane(preferred),
                ResultsLaneSelectionReason::StarvationOverride,
            ));
        }
        Some((preferred, ResultsLaneSelectionReason::Preferred))
    }

    pub(crate) fn observe_success(&mut self, lane: ResultsLane, rows: usize, duration: Duration) {
        if rows == 0 {
            return;
        }
        let rows = rows as f64;
        let duration = duration.as_secs_f64();
        match lane {
            ResultsLane::Dispatch => {
                self.dispatch_rows_per_turn.observe(rows);
                self.dispatch_turn_duration.observe(duration);
            }
            ResultsLane::Completion => {
                self.completion_rows_per_turn.observe(rows);
                self.completion_turn_duration.observe(duration);
            }
        }
        self.shared_rows_per_turn.observe(rows);
        self.shared_turn_duration.observe(duration);
    }

    pub(crate) fn record_turn(&mut self, lane: ResultsLane) {
        if self.preferred == Some(lane) {
            self.preferred_turns = self.preferred_turns.saturating_add(1);
        } else {
            self.preferred_turns = 0;
        }
    }

    fn pressure(
        &self,
        lane: ResultsLane,
        queued_rows: usize,
        oldest: Option<Instant>,
        now: Instant,
    ) -> Option<f64> {
        let (rows_per_turn, turn_duration) = match lane {
            ResultsLane::Dispatch => (
                self.dispatch_rows_per_turn.value(),
                self.dispatch_turn_duration.value(),
            ),
            ResultsLane::Completion => (
                self.completion_rows_per_turn.value(),
                self.completion_turn_duration.value(),
            ),
        };
        let rows_per_turn = rows_per_turn.or(self.shared_rows_per_turn.value())?;
        let turn_duration = turn_duration.or(self.shared_turn_duration.value())?;
        let queued_turns = queued_rows as f64 / rows_per_turn.max(1.0);
        let wait_turns = oldest
            .map(|queued_at| now.saturating_duration_since(queued_at).as_secs_f64())
            .unwrap_or_default()
            / turn_duration.max(f64::MIN_POSITIVE);
        Some(queued_turns + wait_turns)
    }
}

fn other_lane(lane: ResultsLane) -> ResultsLane {
    match lane {
        ResultsLane::Dispatch => ResultsLane::Completion,
        ResultsLane::Completion => ResultsLane::Dispatch,
    }
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

#[cfg(test)]
mod results_lane_tests {
    use super::*;

    fn selected(
        selection: Option<(ResultsLane, ResultsLaneSelectionReason)>,
    ) -> Option<ResultsLane> {
        selection.map(|(lane, _)| lane)
    }

    fn sampled_controller() -> ResultsLaneController {
        let mut controller = ResultsLaneController::new(1);
        controller.observe_success(ResultsLane::Dispatch, 1, Duration::from_millis(1));
        controller.observe_success(ResultsLane::Completion, 1, Duration::from_millis(1));
        controller
    }

    #[test]
    fn completion_is_the_initial_dual_lane_tie_break() {
        let now = Instant::now();
        let mut controller = ResultsLaneController::new(4);

        assert_eq!(
            selected(controller.select(1, Some(now), 1, Some(now), now)),
            Some(ResultsLane::Completion)
        );
    }

    #[test]
    fn direction_requires_three_consistent_samples() {
        let now = Instant::now();
        let mut controller = sampled_controller();

        assert_eq!(
            selected(controller.select(10, Some(now), 1, Some(now), now)),
            Some(ResultsLane::Completion)
        );
        assert_eq!(
            selected(controller.select(10, Some(now), 1, Some(now), now)),
            Some(ResultsLane::Completion)
        );
        assert_eq!(
            selected(controller.select(10, Some(now), 1, Some(now), now)),
            Some(ResultsLane::Dispatch)
        );
    }

    #[test]
    fn old_work_can_reverse_a_small_queue_disadvantage() {
        let now = Instant::now();
        let mut controller = sampled_controller();
        let old = now - Duration::from_millis(20);

        for _ in 0..3 {
            controller.select(10, Some(now), 1, Some(old), now);
        }

        assert_eq!(
            selected(controller.select(10, Some(now), 1, Some(old), now)),
            Some(ResultsLane::Completion)
        );
    }

    #[test]
    fn one_reversed_sample_does_not_switch_the_preferred_lane() {
        let now = Instant::now();
        let mut controller = sampled_controller();
        for _ in 0..3 {
            controller.select(10, Some(now), 1, Some(now), now);
        }

        assert_eq!(
            selected(controller.select(1, Some(now), 10, Some(now), now)),
            Some(ResultsLane::Dispatch)
        );
    }

    #[test]
    fn active_non_preferred_lane_is_forced_after_the_turn_cap() {
        let now = Instant::now();
        let mut controller = sampled_controller();
        for _ in 0..3 {
            controller.select(10, Some(now), 1, Some(now), now);
        }
        for _ in 0..MAX_RESULTS_PREFERRED_LANE_TURNS {
            assert_eq!(
                selected(controller.select(10, Some(now), 1, Some(now), now)),
                Some(ResultsLane::Dispatch)
            );
            controller.record_turn(ResultsLane::Dispatch);
        }

        assert_eq!(
            selected(controller.select(10, Some(now), 1, Some(now), now)),
            Some(ResultsLane::Completion)
        );
    }
}
