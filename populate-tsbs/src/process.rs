//! Stateful time-series processes (TSBS's `common.Distribution`).
//!
//! A [`Process`] has a current value ([`Process::get`], idempotent) and moves
//! one step forward on [`Process::advance`]. They compose like TSBS's
//! constructors:
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
//! All sampling goes through [`GoRandExt`], so with a
//! [`GoRand`](crate::gorand::GoRand) the draws match Go bit for bit, and with
//! any other RNG they are ordinary fast samplers.
//!
//! [`series`] turns any process into a populate-2 column distribution.

use crate::gorand::GoRandExt;
use populate_2::traits::{Backend, DefaultSink, Distribution, SampleIter, Sink};
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
pub struct Normal {
    pub mean: f64,
    pub std_dev: f64,
    value: f64,
}

pub fn normal(mean: f64, std_dev: f64) -> Normal {
    Normal {
        mean,
        std_dev,
        value: 0.0,
    }
}

impl Process for Normal {
    fn advance(&mut self, rng: &mut impl Rng) {
        self.value = rng.go_norm_float64() * self.std_dev + self.mean;
    }
    fn get(&self) -> f64 {
        self.value
    }
}

/// `U[low, high)`, redrawn on every advance.
#[derive(Clone, Debug)]
pub struct Uniform {
    pub low: f64,
    pub high: f64,
    value: f64,
}

pub fn uniform(low: f64, high: f64) -> Uniform {
    Uniform {
        low,
        high,
        value: 0.0,
    }
}

impl Process for Uniform {
    fn advance(&mut self, rng: &mut impl Rng) {
        let mut x = rng.go_float64();
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
/// TSBS's IoT `current_load` reads a *package-level* `UD` that every truck
/// advances, so all trucks observe the most recent draw by any truck. This
/// reproduces that coupling.
#[derive(Clone, Debug)]
pub struct SharedUniform {
    pub low: f64,
    pub high: f64,
    value: Rc<Cell<f64>>,
}

pub fn shared_uniform(low: f64, high: f64) -> SharedUniform {
    SharedUniform {
        low,
        high,
        value: Rc::new(Cell::new(0.0)),
    }
}

impl Process for SharedUniform {
    fn advance(&mut self, rng: &mut impl Rng) {
        let mut x = rng.go_float64();
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
// Series — a process as a populate-2 column
// ============================================================================

/// A process as a single `f64` column: row `i` is the value after `i` advances.
#[derive(Clone, Debug)]
pub struct Series<P>(pub P);

pub fn series<P: Process + Clone>(process: P) -> Series<P> {
    Series(process)
}

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

impl<B: Backend, P: Process + Clone> Distribution<B> for Series<P>
where
    <f64 as DefaultSink<B>>::Sink: Sink<f64>,
{
    type Item = f64;
    type RowIter = SeriesIter<P>;

    fn row_iter(&mut self) -> Self::RowIter {
        SeriesIter {
            process: self.0.clone(),
            started: false,
        }
    }

    fn collect_into(
        &mut self,
        mut sink: <f64 as DefaultSink<B>>::Sink,
        rng: &mut impl Rng,
        n: usize,
    ) -> <f64 as DefaultSink<B>>::Sink {
        let mut iter = <Self as Distribution<B>>::row_iter(self);
        for _ in 0..n {
            sink.push(iter.next(rng));
        }
        sink
    }
}
