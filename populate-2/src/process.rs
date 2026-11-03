//! Stateful value processes: random walks, clamping, rarely-changing values.
//!
//! A [`Process`] has a current value ([`Process::get`], idempotent) and moves
//! one step forward on [`Process::advance`]. Unlike independent random
//! columns, consecutive values are correlated, which is what time series
//! (metrics, sensor readings, prices) look like. They compose like TSBS's
//! `common.Distribution` constructors:
//!
//! | TSBS          | here                                   |
//! |---------------|----------------------------------------|
//! | `ND(m, s)`    | [`normal`]                             |
//! | `UD(lo, hi)`  | [`uniform`]                            |
//! | `WD(step, x)` | [`random_walk`]                        |
//! | `CWD(..)`     | [`clamped_walk`]                       |
//! | `MWD(..)`     | [`monotonic_walk`]                     |
//! | `FP(d, p)`    | [`precision`]                          |
//! | `LD(m, d, t)` | [`lazy`]                               |
//!
//! Random leaves take a [`Sampler`]: with [`GoSampler`](crate::sampler::GoSampler)
//! and a [`GoRand`](crate::sampler::GoRand) the draws match TSBS bit for bit.
//!
//! [`series`] turns any process into a column distribution.

use crate::dist::{Leaf, leaf};
use crate::sampler::Sampler;
use crate::traits::SampleIter;
use rand::Rng;
use std::cell::Cell;
use std::rc::Rc;

pub trait Process {
    fn advance(&mut self, rng: &mut impl Rng);
    fn get(&self) -> f64;
}

// ============================================================================
// Leaf processes
// ============================================================================

/// `N(mean, std_dev)`, redrawn on every advance.
#[derive(Clone, Debug)]
pub struct Normal<S> {
    pub mean: f64,
    pub std_dev: f64,
    pub sampler: S,
    value: f64,
}

pub fn normal<S: Sampler>(sampler: S, mean: f64, std_dev: f64) -> Normal<S> {
    Normal {
        mean,
        std_dev,
        sampler,
        value: 0.0,
    }
}

impl<S: Sampler> Process for Normal<S> {
    fn advance(&mut self, rng: &mut impl Rng) {
        self.value = self.sampler.normal(rng) * self.std_dev + self.mean;
    }
    fn get(&self) -> f64 {
        self.value
    }
}

/// `U[low, high)`, redrawn on every advance.
#[derive(Clone, Debug)]
pub struct Uniform<S> {
    pub low: f64,
    pub high: f64,
    pub sampler: S,
    value: f64,
}

pub fn uniform<S: Sampler>(sampler: S, low: f64, high: f64) -> Uniform<S> {
    Uniform {
        low,
        high,
        sampler,
        value: 0.0,
    }
}

impl<S: Sampler> Process for Uniform<S> {
    /// Always draws (unlike [`Sampler::runif`], which skips `low == high`).
    fn advance(&mut self, rng: &mut impl Rng) {
        let mut x = self.sampler.unif(rng);
        x *= self.high - self.low;
        x += self.low;
        self.value = x;
    }
    fn get(&self) -> f64 {
        self.value
    }
}

/// A uniform whose current value is shared by every clone.
///
/// Couples entities through one global value (e.g. TSBS's IoT
/// `current_load`, a package-level `UD` that every truck advances, so all
/// trucks observe the most recent draw by any truck).
#[derive(Clone, Debug)]
pub struct SharedUniform<S> {
    pub low: f64,
    pub high: f64,
    pub sampler: S,
    value: Rc<Cell<f64>>,
}

pub fn shared_uniform<S: Sampler>(sampler: S, low: f64, high: f64) -> SharedUniform<S> {
    SharedUniform {
        low,
        high,
        sampler,
        value: Rc::new(Cell::new(0.0)),
    }
}

impl<S: Sampler> Process for SharedUniform<S> {
    fn advance(&mut self, rng: &mut impl Rng) {
        let mut x = self.sampler.unif(rng);
        x *= self.high - self.low;
        x += self.low;
        self.value.set(x);
    }
    fn get(&self) -> f64 {
        self.value.get()
    }
}

/// Never changes.
#[derive(Clone, Debug)]
pub struct Constant(pub f64);

impl Process for Constant {
    fn advance(&mut self, _rng: &mut impl Rng) {}
    fn get(&self) -> f64 {
        self.0
    }
}

// ============================================================================
// Walks
// ============================================================================

/// Random walk `x += step`, optionally clamped to `[min, max]` and optionally
/// monotonic (`x += |step|`). One type covers TSBS's `WD`, `CWD` and `MWD`.
#[derive(Clone, Debug)]
pub struct Walk<S> {
    pub step: S,
    pub min: f64,
    pub max: f64,
    pub monotonic: bool,
    pub state: f64,
}

/// `x += step` (TSBS `WD`).
pub fn random_walk<S: Process>(step: S, state: f64) -> Walk<S> {
    Walk {
        step,
        min: f64::NEG_INFINITY,
        max: f64::INFINITY,
        monotonic: false,
        state,
    }
}

/// `x = clamp(x + step, min, max)` (TSBS `CWD`).
pub fn clamped_walk<S: Process>(step: S, min: f64, max: f64, state: f64) -> Walk<S> {
    Walk {
        step,
        min,
        max,
        monotonic: false,
        state,
    }
}

/// `x += |step|` (TSBS `MWD`).
pub fn monotonic_walk<S: Process>(step: S, state: f64) -> Walk<S> {
    Walk {
        step,
        min: f64::NEG_INFINITY,
        max: f64::INFINITY,
        monotonic: true,
        state,
    }
}

impl<S: Process> Process for Walk<S> {
    fn advance(&mut self, rng: &mut impl Rng) {
        self.step.advance(rng);
        let step = self.step.get();
        self.state += if self.monotonic { step.abs() } else { step };
        if self.state > self.max {
            self.state = self.max;
        }
        if self.state < self.min {
            self.state = self.min;
        }
    }
    fn get(&self) -> f64 {
        self.state
    }
}

/// Clamped walk that wraps back to `max` whenever it hits `min`
/// (TSBS IoT `customFuelDistribution`: an empty tank is refilled).
#[derive(Clone, Debug)]
pub struct RefillWalk<S>(pub Walk<S>);

impl<S: Process> Process for RefillWalk<S> {
    fn advance(&mut self, rng: &mut impl Rng) {
        self.0.advance(rng);
        if self.0.state == self.0.min {
            self.0.state = self.0.max;
        }
    }
    fn get(&self) -> f64 {
        self.0.get()
    }
}

// ============================================================================
// Modifiers
// ============================================================================

/// Truncate to `digits` decimal places (TSBS `FP`, digits clamped to 0..=5).
#[derive(Clone, Debug)]
pub struct Precision<P> {
    pub inner: P,
    scale: f64,
}

pub fn precision<P: Process>(inner: P, digits: i32) -> Precision<P> {
    Precision {
        inner,
        scale: 10f64.powi(digits.clamp(0, 5)),
    }
}

impl<P: Process> Process for Precision<P> {
    fn advance(&mut self, rng: &mut impl Rng) {
        self.inner.advance(rng);
    }
    fn get(&self) -> f64 {
        // Go: float64(int(x*p)) / p — truncation toward zero.
        (self.inner.get() * self.scale) as i64 as f64 / self.scale
    }
}

/// Only advance `inner` when `motive` lands at or above `threshold`
/// (TSBS `LD`): a value that changes rarely.
#[derive(Clone, Debug)]
pub struct Lazy<M, P> {
    pub motive: M,
    pub inner: P,
    pub threshold: f64,
}

pub fn lazy<M: Process, P: Process>(motive: M, inner: P, threshold: f64) -> Lazy<M, P> {
    Lazy {
        motive,
        inner,
        threshold,
    }
}

impl<M: Process, P: Process> Process for Lazy<M, P> {
    fn advance(&mut self, rng: &mut impl Rng) {
        self.motive.advance(rng);
        if self.motive.get() < self.threshold {
            return;
        }
        self.inner.advance(rng);
    }
    fn get(&self) -> f64 {
        self.inner.get()
    }
}

// ============================================================================
// Series — a process as a column
// ============================================================================

/// Iterates a process: row `i` is the value after `i` advances.
#[derive(Clone, Debug)]
pub struct SeriesIter<P> {
    process: P,
    started: bool,
}

impl<P: Process> SampleIter for SeriesIter<P> {
    type Item = f64;
    fn next(&mut self, rng: &mut impl Rng) -> f64 {
        if self.started {
            self.process.advance(rng);
        }
        self.started = true;
        self.process.get()
    }
}

/// A process as a single `f64` column: row `i` is the value after `i` advances.
pub fn series<P: Process + Clone>(process: P) -> Leaf<SeriesIter<P>> {
    leaf(SeriesIter {
        process,
        started: false,
    })
}
