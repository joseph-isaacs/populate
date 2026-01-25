use std::marker::PhantomData;

use populate_core::{ColumnarDistribution, Distribution, SampleIter};

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

impl<T: Clone> Range<T, ()> {
    pub fn new(min: T, max: T) -> Self {
        Self {
            min,
            max,
            _phantom: PhantomData::<()>,
        }
    }
}

impl<T: Clone, S> Range<T, S> {
    pub fn with_sampler(min: T, max: T) -> Self {
        Self {
            min,
            max,
            _phantom: PhantomData::<S>,
        }
    }
}

#[derive(Clone)]
pub struct RangeIter<T, S> {
    min: T,
    max: T,
    _phantom_data: PhantomData<S>,
}

impl<T: Clone, S: RangeSampler<T>> Distribution<T> for Range<T, S> {
    type Iter = RangeIter<T, S>;

    fn sample(&mut self) -> Self::Iter {
        RangeIter {
            min: self.min.clone(),
            max: self.max.clone(),
            _phantom_data: Default::default(),
        }
    }
}

impl<T: Clone, S: RangeSampler<T>> ColumnarDistribution<T> for Range<T, S> {
    type Cols = Self;

    fn into_cols(self) -> Self::Cols {
        self
    }
}

impl<T: Clone, S: RangeSampler<T>> SampleIter for RangeIter<T, S> {
    type Item = T;

    fn next(&mut self, rng: &mut impl rand::Rng) -> T {
        S::sample_range(rng, self.min.clone(), self.max.clone())
    }
}

// === Convenience function ===

pub fn range<T: Clone>(min: T, max: T) -> Range<T, ()> {
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
