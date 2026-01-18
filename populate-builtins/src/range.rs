use std::marker::PhantomData;

use populate_core::{Distribution, SampleIter};

/// Trait for range sampling strategies.
pub trait RangeSampler<T>: Clone {
    fn sample_range(rng: &mut impl rand::Rng, min: T, max: T) -> T;
}

// === Standard sampling: () delegates to rand ===

macro_rules! impl_standard_sampler {
    ($t:ty) => {
        impl RangeSampler<$t> for () {
            fn sample_range(rng: &mut impl rand::Rng, min: $t, max: $t) -> $t {
                rng.random_range(min..max)
            }
        }
    };
}

impl_standard_sampler!(i8);
impl_standard_sampler!(i16);
impl_standard_sampler!(i32);
impl_standard_sampler!(i64);
impl_standard_sampler!(u8);
impl_standard_sampler!(u16);
impl_standard_sampler!(u32);
impl_standard_sampler!(u64);
impl_standard_sampler!(usize);
impl_standard_sampler!(f32);
impl_standard_sampler!(f64);

// === Range distribution ===

pub struct Range<T, S = ()> {
    min: T,
    max: T,
    _phantom: PhantomData<S>,
}

impl<T> Range<T, ()> {
    pub fn new(min: T, max: T) -> Self {
        Self {
            min,
            max,
            _phantom: PhantomData::<()>,
        }
    }
}

impl<T, S> Range<T, S> {
    pub fn with_sampler(min: T, max: T) -> Self {
        Self {
            min,
            max,
            _phantom: PhantomData::<S>,
        }
    }
}

/// Iterator that borrows min/max from the parent Range.
pub struct RangeIter<'a, T, S> {
    min: &'a T,
    max: &'a T,
    _phantom_data: PhantomData<S>,
}

impl<T: Copy, S: RangeSampler<T>> Distribution<T> for Range<T, S> {
    type Iter<'a>
        = RangeIter<'a, T, S>
    where
        Self: 'a;

    fn sample(&mut self) -> Self::Iter<'_> {
        RangeIter {
            min: &self.min,
            max: &self.max,
            _phantom_data: Default::default(),
        }
    }
}

impl<T: Copy, S: RangeSampler<T>> SampleIter for RangeIter<'_, T, S> {
    type Item = T;

    fn next(&mut self, rng: &mut impl rand::Rng) -> T {
        S::sample_range(rng, *self.min, *self.max)
    }

    fn reset(&mut self) {
        // No state to reset - stateless iterator
    }
}

// === Convenience function ===

pub fn range<T>(min: T, max: T) -> Range<T, ()> {
    Range::new(min, max)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;
    use rand::rngs::StdRng;

    #[test]
    fn test_range_i32() {
        let mut dist = range(0i32, 100);
        let mut rng = StdRng::seed_from_u64(0);
        let mut iter = dist.sample();

        for _ in 0..100 {
            let v = iter.next(&mut rng);
            assert!(v >= 0 && v < 100);
        }
    }

    #[test]
    fn test_range_i64() {
        let mut dist = range(0i64, 100);
        let mut rng = StdRng::seed_from_u64(0);
        let mut iter = dist.sample();

        for _ in 0..100 {
            let v = iter.next(&mut rng);
            assert!(v >= 0 && v < 100);
        }
    }
}
