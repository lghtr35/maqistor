/// Exponentially weighted moving average used by adaptive scheduling and
/// persistence policies.
#[derive(Debug, Clone, Copy)]
pub struct Ewma {
    alpha: f64,
    value: Option<f64>,
}

impl Ewma {
    pub fn new(window: usize) -> Self {
        Self {
            alpha: 2.0 / (window as f64 + 1.0),
            value: None,
        }
    }

    pub fn observe(&mut self, sample: f64) {
        if !sample.is_finite() || sample < 0.0 {
            return;
        }
        self.value = Some(match self.value {
            Some(value) => self.alpha * sample + (1.0 - self.alpha) * value,
            None => sample,
        });
    }

    pub fn value(&self) -> Option<f64> {
        self.value
    }
}

/// Confirms a non-zero control direction only after repeated observations.
#[derive(Debug, Default)]
pub struct DirectionStreak {
    direction: i8,
    samples: u8,
}

impl DirectionStreak {
    pub fn confirm(&mut self, direction: i8) -> bool {
        if direction == 0 {
            self.direction = 0;
            self.samples = 0;
            return false;
        }
        if self.direction == direction {
            self.samples = self.samples.saturating_add(1);
        } else {
            self.direction = direction;
            self.samples = 1;
        }
        if self.samples >= 3 {
            self.samples = 0;
            true
        } else {
            false
        }
    }
}

/// Shared bounded batch-size controller. Domain wrappers decide whether a
/// sample means growth, backoff, or no change and delegate the adjustment here.
#[derive(Debug)]
pub struct AdaptiveBatch {
    min: usize,
    max: usize,
    size: usize,
    probe_factor: f64,
    backoff_factor: f64,
    direction: DirectionStreak,
}

impl AdaptiveBatch {
    pub fn new(min: usize, max: usize, probe_factor: f64, backoff_factor: f64) -> Self {
        Self {
            min,
            max,
            size: ((min as f64 * max as f64).sqrt().round() as usize).clamp(min, max),
            probe_factor,
            backoff_factor,
            direction: DirectionStreak::default(),
        }
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn set_size(&mut self, size: usize) {
        self.size = size.clamp(self.min, self.max);
    }

    /// Applies a confirmed direction: positive probes upward and negative
    /// backs off. Returns true only when the batch size changed.
    pub fn observe_direction(&mut self, direction: i8) -> bool {
        if !self.direction.confirm(direction) {
            return false;
        }
        let previous = self.size;
        self.size = match direction {
            1 => (self.size as f64 * self.probe_factor).ceil() as usize,
            -1 => (self.size as f64 * self.backoff_factor).floor() as usize,
            _ => self.size,
        }
        .clamp(self.min, self.max);
        self.size != previous
    }

    pub fn reset_direction(&mut self) {
        self.direction.confirm(0);
    }
}
