//! Random leaf columns, generic over a [`Sampler`] dialect.
//!
//! | leaf | value | R | Go |
//! |------|-------|---|----|
//! | [`choose`] | uniform pick from a fixed set | `sample(x, n, TRUE)` | `x[Intn(len(x))]` |
//! | [`choose_dict`] | the same picks as a dictionary column (codes) | `sample(x, n, TRUE)` | `x[Intn(len(x))]` |
//! | [`uniform_int`] | uniform integer in `start..start + n` | `sample(n, N, TRUE)` (start 1) | `Intn(n)` (start 0) |
//! | [`uniform`] | uniform double in `(min, max)` | `runif(N, min, max)` | — |
//!
//! Each draws one index per row from the sampler, so the row and column
//! paths draw the same stream, and `choose` and `choose_dict` draw the same
//! picks. Columnar, `choose` copies a borrowed value into its column
//! ([`PushRef`]: bytes for a string, no `String` per row) and `choose_dict`
//! writes only a `u32` code.

use crate::column::{DictCol, DictValue, PushRef};
use crate::dist::{Leaf, leaf};
use crate::sampler::Sampler;
use crate::traits::{Backend, DefaultSink, Distribution, HCons, HNil, SampleIter};
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

impl<B: Backend, T, S, C> Distribution<B> for Choose<T, S>
where
    T: Clone + DefaultSink<B, Sink = HCons<C, HNil>>,
    S: Sampler,
    C: PushRef<T>,
{
    type Item = T;
    type RowIter = Self;

    fn row_iter(&mut self) -> Self {
        self.clone()
    }

    fn collect_into(
        &mut self,
        mut sink: HCons<C, HNil>,
        rng: &mut impl Rng,
        n: usize,
    ) -> HCons<C, HNil> {
        for _ in 0..n {
            let i = self.sampler.index(rng, self.values.len());
            sink.0.push_ref(&self.values[i]);
        }
        sink
    }
}

/// Uniform pick from `values`.
pub fn choose<T: Clone, S: Sampler>(sampler: S, values: impl Into<Arc<[T]>>) -> Choose<T, S> {
    Choose {
        values: values.into(),
        sampler,
    }
}

/// Uniform pick from a fixed set, as a dictionary column: a `u32` code per
/// row into the shared value list.
#[derive(Clone, Debug)]
pub struct ChooseDict<T, S> {
    pub values: Arc<[T]>,
    pub sampler: S,
}

impl<T, S: Sampler> SampleIter for ChooseDict<T, S> {
    type Item = DictValue<T>;
    fn next(&mut self, rng: &mut impl Rng) -> DictValue<T> {
        DictValue {
            code: self.sampler.index(rng, self.values.len()) as u32,
            values: Arc::clone(&self.values),
        }
    }
}

impl<B: Backend, T, S: Sampler> Distribution<B> for ChooseDict<T, S> {
    type Item = DictValue<T>;
    type RowIter = Self;

    fn row_iter(&mut self) -> Self {
        ChooseDict {
            values: Arc::clone(&self.values),
            sampler: self.sampler,
        }
    }

    fn collect_into(
        &mut self,
        mut sink: HCons<DictCol<T>, HNil>,
        rng: &mut impl Rng,
        n: usize,
    ) -> HCons<DictCol<T>, HNil> {
        let col = &mut sink.0;
        col.set_values(&self.values);
        col.codes.reserve(n);
        for _ in 0..n {
            col.codes
                .push(self.sampler.index(rng, self.values.len()) as u32);
        }
        sink
    }
}

/// Uniform pick from `values` as a dictionary column (same draws as [`choose`]).
pub fn choose_dict<T, S: Sampler>(sampler: S, values: impl Into<Arc<[T]>>) -> ChooseDict<T, S> {
    let values = values.into();
    assert!(
        u32::try_from(values.len()).is_ok(),
        "choose_dict: more than u32::MAX values"
    );
    ChooseDict { values, sampler }
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
