use std::marker::PhantomData;

use populate_core::Rng;
use populate_core::{CollectColumnar, Distribution, SampleIter, SinkTuple};

use crate::{Range, RangeSampler};

// === PickOne: pick one element using an index distribution ===

/// Pick one random element from a slice using a custom index generator.
pub struct PickOne<'a, T, I> {
    values: &'a [T],
    indices: I,
}

impl<'a, T, I> PickOne<'a, T, I> {
    pub fn new(values: &'a [T], indices: I) -> Self {
        Self { values, indices }
    }
}

#[derive(Clone)]
pub struct PickOneIter<'a, T, II> {
    values: &'a [T],
    indices: II,
}

impl<T: Clone, II: SampleIter<Item = i64>> SampleIter for PickOneIter<'_, T, II> {
    type Item = T;

    fn next(&mut self, rng: &mut impl Rng) -> T {
        let idx = self.indices.next(rng) as usize;
        self.values[idx].clone()
    }
}

impl<T: Clone, II: SampleIter<Item = i64>, S: SinkTuple<T>> CollectColumnar<S>
    for PickOneIter<'_, T, II>
{
    const IS_COLUMNAR: bool = true;
    type Output = S::Output;

    fn collect_columnar(mut self, rng: &mut impl Rng, n: usize, mut sinks: S) -> Self::Output {
        for _ in 0..n {
            sinks.push_row(self.next(rng));
        }
        sinks.finish()
    }
}

impl<'a, T: Clone, I: Distribution<i64>> Distribution<T> for PickOne<'a, T, I> {
    type Iter = PickOneIter<'a, T, I::Iter>;

    fn sample(&mut self) -> Self::Iter {
        PickOneIter {
            values: self.values,
            indices: self.indices.sample(),
        }
    }
}

// === PickOneRef: pick one element by reference ===

/// Pick one random element from a slice by reference (no clone).
pub struct PickOneRef<'a, T, I> {
    values: &'a [T],
    indices: I,
}

impl<'a, T, I> PickOneRef<'a, T, I> {
    pub fn new(values: &'a [T], indices: I) -> Self {
        Self { values, indices }
    }
}

impl<'a, T, S: RangeSampler<i64>> PickOneRef<'a, T, Range<i64, S>> {
    pub fn with_sampler(values: &'a [T]) -> Self {
        Self::new(values, Range::with_sampler(0, values.len() as i64))
    }
}

pub struct PickOneRefIter<'a, T, II> {
    values: &'a [T],
    indices: II,
}

impl<T, II: Clone> Clone for PickOneRefIter<'_, T, II> {
    fn clone(&self) -> Self {
        Self {
            values: self.values,
            indices: self.indices.clone(),
        }
    }
}

impl<'a, T, II: SampleIter<Item = i64>> SampleIter for PickOneRefIter<'a, T, II> {
    type Item = &'a T;

    fn next(&mut self, rng: &mut impl Rng) -> &'a T {
        let idx = self.indices.next(rng) as usize;
        &self.values[idx]
    }
}

impl<'a, T, II: SampleIter<Item = i64>, S: SinkTuple<&'a T>> CollectColumnar<S>
    for PickOneRefIter<'a, T, II>
{
    const IS_COLUMNAR: bool = true;
    type Output = S::Output;

    fn collect_columnar(mut self, rng: &mut impl Rng, n: usize, mut sinks: S) -> Self::Output {
        for _ in 0..n {
            sinks.push_row(self.next(rng));
        }
        sinks.finish()
    }
}

impl<'a, T, I: Distribution<i64>> Distribution<&'a T> for PickOneRef<'a, T, I> {
    type Iter = PickOneRefIter<'a, T, I::Iter>;

    fn sample(&mut self) -> Self::Iter {
        PickOneRefIter {
            values: self.values,
            indices: self.indices.sample(),
        }
    }
}

// === Pick: pick N elements using an index distribution (with replacement) ===

/// Pick N random elements from a slice using a custom index generator (compile-time N).
/// This allows duplicates (sampling with replacement).
pub struct Pick<'a, T, I, const N: usize> {
    values: &'a [T],
    indices: I,
}

impl<'a, T, I, const N: usize> Pick<'a, T, I, N> {
    pub fn new(values: &'a [T], indices: I) -> Self {
        Self { values, indices }
    }
}

#[derive(Clone)]
pub struct PickIter<'a, T, II, const N: usize> {
    values: &'a [T],
    indices: II,
}

impl<T: Clone, II: SampleIter<Item = i64>, const N: usize> SampleIter for PickIter<'_, T, II, N> {
    type Item = [T; N];

    fn next(&mut self, rng: &mut impl Rng) -> [T; N] {
        std::array::from_fn(|_| {
            let idx = self.indices.next(rng) as usize;
            self.values[idx].clone()
        })
    }
}

impl<T: Clone, II: SampleIter<Item = i64>, const N: usize, S: SinkTuple<[T; N]>> CollectColumnar<S>
    for PickIter<'_, T, II, N>
{
    const IS_COLUMNAR: bool = true;
    type Output = S::Output;

    fn collect_columnar(mut self, rng: &mut impl Rng, n: usize, mut sinks: S) -> Self::Output {
        for _ in 0..n {
            sinks.push_row(self.next(rng));
        }
        sinks.finish()
    }
}

impl<'a, T: Clone, I: Distribution<i64>, const N: usize> Distribution<[T; N]>
    for Pick<'a, T, I, N>
{
    type Iter = PickIter<'a, T, I::Iter, N>;

    fn sample(&mut self) -> Self::Iter {
        PickIter {
            values: self.values,
            indices: self.indices.sample(),
        }
    }
}

// === PickStrategy: trait for unique picking strategies ===

/// Strategy for picking N unique elements from a slice.
/// Implementations define how indices are selected (Fisher-Yates, TPC-H, etc.)
pub trait PickStrategy: Clone {
    /// Generate N unique indices from range [0, len) using partial Fisher-Yates shuffle.
    fn pick_indices<const N: usize>(rng: &mut impl Rng, len: usize) -> [usize; N];
}

// === Unique: standard Fisher-Yates shuffle ===

/// Pick unique elements using Fisher-Yates shuffle with standard rand.
#[derive(Clone, Copy, Default)]
pub struct Unique;

impl PickStrategy for Unique {
    fn pick_indices<const N: usize>(rng: &mut impl Rng, len: usize) -> [usize; N] {
        let mut indices: Vec<usize> = (0..len).collect();
        for i in 0..N {
            let j = rng.random_range(i..len);
            indices.swap(i, j);
        }
        std::array::from_fn(|i| indices[i])
    }
}

// === PickN: pick N unique elements using a strategy ===

/// Pick N unique random elements from a slice using a PickStrategy.
/// Default strategy is `Unique` (Fisher-Yates without replacement).
pub struct PickN<'a, T, S = Unique, const N: usize = 1> {
    values: &'a [T],
    _phantom: PhantomData<S>,
}

impl<'a, T, const N: usize> PickN<'a, T, Unique, N> {
    pub fn new(values: &'a [T]) -> Self {
        Self {
            values,
            _phantom: PhantomData,
        }
    }
}

impl<'a, T, S, const N: usize> PickN<'a, T, S, N> {
    pub fn with_strategy(values: &'a [T]) -> Self {
        Self {
            values,
            _phantom: PhantomData,
        }
    }
}

#[derive(Clone)]
pub struct PickNIter<'a, T, S, const N: usize> {
    values: &'a [T],
    _phantom: PhantomData<S>,
}

impl<T: Clone, S: PickStrategy, const N: usize> SampleIter for PickNIter<'_, T, S, N> {
    type Item = [T; N];

    fn next(&mut self, rng: &mut impl Rng) -> [T; N] {
        let indices = S::pick_indices::<N>(rng, self.values.len());
        std::array::from_fn(|i| self.values[indices[i]].clone())
    }
}

impl<T: Clone, PS: PickStrategy, const N: usize, S: SinkTuple<[T; N]>> CollectColumnar<S>
    for PickNIter<'_, T, PS, N>
{
    const IS_COLUMNAR: bool = true;
    type Output = S::Output;

    fn collect_columnar(mut self, rng: &mut impl Rng, n: usize, mut sinks: S) -> Self::Output {
        for _ in 0..n {
            sinks.push_row(self.next(rng));
        }
        sinks.finish()
    }
}

impl<'a, T: Clone, S: PickStrategy, const N: usize> Distribution<[T; N]> for PickN<'a, T, S, N> {
    type Iter = PickNIter<'a, T, S, N>;

    fn sample(&mut self) -> Self::Iter {
        PickNIter {
            values: self.values,
            _phantom: PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::range;
    use rand::SeedableRng;
    use rand::rngs::StdRng;

    const COLORS: [&str; 5] = ["red", "green", "blue", "yellow", "purple"];

    #[test]
    fn test_pick_one() {
        let mut dist = PickOne::new(&COLORS, range(0i64, COLORS.len() as i64));
        let mut rng = StdRng::seed_from_u64(42);
        let mut iter = dist.sample();

        for _ in 0..10 {
            let color = iter.next(&mut rng);
            assert!(COLORS.contains(&color));
        }
    }

    #[test]
    fn test_pick_n() {
        let mut dist = Pick::<_, _, 3>::new(&COLORS, range(0i64, COLORS.len() as i64));
        let mut rng = StdRng::seed_from_u64(42);
        let mut iter = dist.sample();

        let picked: [&str; 3] = iter.next(&mut rng);
        assert_eq!(picked.len(), 3);
        for color in picked {
            assert!(COLORS.contains(&color));
        }
    }

    #[test]
    fn test_pick_unique() {
        let mut dist = PickN::<_, Unique, 5>::new(&COLORS);
        let mut rng = StdRng::seed_from_u64(42);
        let mut iter = dist.sample();

        let picked: [&str; 5] = iter.next(&mut rng);
        // All 5 should be unique (since we have exactly 5 colors)
        let mut seen = std::collections::HashSet::new();
        for color in picked {
            assert!(COLORS.contains(&color));
            assert!(seen.insert(color), "Duplicate found: {}", color);
        }
    }
}
