//! Leaf distributions for H2O-style data, generic over a [`Sampler`].
//!
//! Each leaf is a [`SampleIter`] wrapped in [`Leaf`], which turns any
//! clonable `SampleIter` into a populate-2 [`Distribution`]. Collected
//! column by column, a tuple of these consumes the RNG in exactly the order
//! an R script building one column vector at a time does.

use crate::rng::r_round;
use crate::sampler::Sampler;
use populate_2::traits::{Backend, DefaultSink, Distribution, SampleIter, Sink};
use rand::Rng;
use std::sync::Arc;

// ============================================================================
// Leaf — any clonable SampleIter as a single-column Distribution
// ============================================================================

/// A single-column distribution backed by a clonable [`SampleIter`]:
/// `row_iter` clones the iterator, `collect_into` pushes `n` draws.
#[derive(Clone, Debug)]
pub struct Leaf<I>(pub I);

impl<B: Backend, I> Distribution<B> for Leaf<I>
where
    I: SampleIter + Clone,
    I::Item: DefaultSink<B>,
    <I::Item as DefaultSink<B>>::Sink: Sink<I::Item>,
{
    type Item = I::Item;
    type RowIter = I;

    fn row_iter(&mut self) -> I {
        self.0.clone()
    }

    fn collect_into(
        &mut self,
        mut sink: <I::Item as DefaultSink<B>>::Sink,
        rng: &mut impl Rng,
        n: usize,
    ) -> <I::Item as DefaultSink<B>>::Sink {
        let mut iter = self.0.clone();
        for _ in 0..n {
            sink.push(iter.next(rng));
        }
        sink
    }
}

// ============================================================================
// SampleInt — R's sample.int(n, size, replace = TRUE)
// ============================================================================

/// Uniform integer in `1..=n` (R: `sample(n, size, TRUE)`).
#[derive(Clone, Copy, Debug)]
pub struct SampleInt<S> {
    pub n: f64,
    pub sampler: S,
}

impl<S: Sampler> SampleIter for SampleInt<S> {
    type Item = i32;
    fn next(&mut self, rng: &mut impl Rng) -> i32 {
        self.sampler.index(rng, self.n) as i32 + 1
    }
}

/// Uniform integer in `1..=n`.
pub fn sample_int<S: Sampler>(sampler: S, n: usize) -> Leaf<SampleInt<S>> {
    Leaf(SampleInt {
        n: n as f64,
        sampler,
    })
}

// ============================================================================
// Choose — R's sample(x, size, replace = TRUE) for a vector x
// ============================================================================

/// Uniform pick from a fixed set of values (R: `sample(x, size, TRUE)`).
#[derive(Clone, Debug)]
pub struct Choose<T, S> {
    pub values: Arc<[T]>,
    pub sampler: S,
}

impl<T: Clone, S: Sampler> SampleIter for Choose<T, S> {
    type Item = T;
    fn next(&mut self, rng: &mut impl Rng) -> T {
        let i = self.sampler.index(rng, self.values.len() as f64) as usize;
        self.values[i].clone()
    }
}

/// Uniform pick from `values`.
pub fn choose<T, S: Sampler>(sampler: S, values: impl Into<Arc<[T]>>) -> Leaf<Choose<T, S>> {
    Leaf(Choose {
        values: values.into(),
        sampler,
    })
}

// ============================================================================
// Runif — R's runif(n, min, max), optionally rounded like round(x, digits)
// ============================================================================

/// Uniform double in `(min, max)`, rounded to `digits` decimals with R's
/// `round` when `digits` is `Some` (R: `round(runif(n, min, max), digits)`).
#[derive(Clone, Copy, Debug)]
pub struct Runif<S> {
    pub min: f64,
    pub max: f64,
    pub digits: Option<i32>,
    pub sampler: S,
}

impl<S: Sampler> SampleIter for Runif<S> {
    type Item = f64;
    fn next(&mut self, rng: &mut impl Rng) -> f64 {
        let x = self.sampler.runif(rng, self.min, self.max);
        match self.digits {
            Some(d) => r_round(x, d),
            None => x,
        }
    }
}

/// Uniform double in `(min, max)` rounded to `digits` decimals.
pub fn runif_rounded<S: Sampler>(sampler: S, min: f64, max: f64, digits: i32) -> Leaf<Runif<S>> {
    Leaf(Runif {
        min,
        max,
        digits: Some(digits),
        sampler,
    })
}

/// `"{prefix}{i:0width$}"` for `i` in `1..=n`: the label set R builds with
/// `sprintf("id%03d", 1:n)`.
pub fn padded_labels(prefix: &str, width: usize, n: usize) -> Vec<String> {
    (1..=n).map(|i| format!("{prefix}{i:0width$}")).collect()
}
