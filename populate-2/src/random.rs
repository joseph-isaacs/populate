//! Random leaf columns, generic over a [`Sampler`] dialect.
//!
//! | leaf | value | R | Go |
//! |------|-------|---|----|
//! | [`choose`] | uniform pick from a fixed set | `sample(x, n, TRUE)` | `x[Intn(len(x))]` |
//! | [`uniform_int`] | uniform integer in `start..start + n` | `sample(n, N, TRUE)` (start 1) | `Intn(n)` (start 0) |
//! | [`uniform`] | uniform double in `(min, max)` | `runif(N, min, max)` | — |
//!
//! Each is a [`Leaf`] over a small clonable [`SampleIter`], so the row and
//! column paths draw the same stream.

use crate::dist::{Leaf, leaf};
use crate::sampler::Sampler;
use crate::traits::SampleIter;
use rand::Rng;
use std::sync::Arc;

/// Uniform pick from a fixed set of values.
#[derive(Clone, Debug)]
pub struct Choose<T, S> {
    pub values: Arc<[T]>,
    pub sampler: S,
}

impl<T: Clone, S: Sampler> SampleIter for Choose<T, S> {
    type Item = T;
    fn next(&mut self, rng: &mut impl Rng) -> T {
        self.values[self.sampler.index(rng, self.values.len())].clone()
    }
}

/// Uniform pick from `values`.
pub fn choose<T: Clone, S: Sampler>(sampler: S, values: impl Into<Arc<[T]>>) -> Leaf<Choose<T, S>> {
    leaf(Choose {
        values: values.into(),
        sampler,
    })
}

/// Uniform integer in `start..start + n`.
#[derive(Clone, Copy, Debug)]
pub struct UniformInt<S> {
    pub start: i32,
    pub n: usize,
    pub sampler: S,
}

impl<S: Sampler> SampleIter for UniformInt<S> {
    type Item = i32;
    fn next(&mut self, rng: &mut impl Rng) -> i32 {
        self.start + self.sampler.index(rng, self.n) as i32
    }
}

/// Uniform integer in `start..start + n` (R's `sample(n, N, TRUE)` is
/// `uniform_int(RSampler, 1, n)`).
pub fn uniform_int<S: Sampler>(sampler: S, start: i32, n: usize) -> Leaf<UniformInt<S>> {
    leaf(UniformInt { start, n, sampler })
}

/// Uniform double in `(min, max)` via [`Sampler::runif`].
#[derive(Clone, Copy, Debug)]
pub struct Uniform<S> {
    pub min: f64,
    pub max: f64,
    pub sampler: S,
}

impl<S: Sampler> SampleIter for Uniform<S> {
    type Item = f64;
    fn next(&mut self, rng: &mut impl Rng) -> f64 {
        self.sampler.runif(rng, self.min, self.max)
    }
}

/// Uniform double in `(min, max)`.
pub fn uniform<S: Sampler>(sampler: S, min: f64, max: f64) -> Leaf<Uniform<S>> {
    leaf(Uniform { min, max, sampler })
}

/// `"{prefix}{i:0width$}"` for `i` in `1..=n` (R: `sprintf("id%03d", 1:n)`):
/// a label set for [`choose`].
pub fn padded_labels(prefix: &str, width: usize, n: usize) -> Vec<String> {
    (1..=n).map(|i| format!("{prefix}{i:0width$}")).collect()
}
