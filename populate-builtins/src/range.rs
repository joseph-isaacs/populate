use populate_core::Sampler;
use populate_core::{Distribution, SampleIter};

/// A distribution that produces values uniformly in [min, max).
pub struct Range<T> {
    min: T,
    max: T,
}

impl<T: Clone> Range<T> {
    pub fn new(min: T, max: T) -> Self {
        Self { min, max }
    }
}

/// Iterator for range values.
#[derive(Clone)]
pub struct RangeIter<T> {
    min: T,
    max: T,
}

/// Macro for types that fit in i32 range.
macro_rules! impl_range_i32 {
    ($t:ty) => {
        impl Distribution<$t> for Range<$t> {
            type Iter = RangeIter<$t>;

            fn sample(&mut self) -> Self::Iter {
                RangeIter {
                    min: self.min,
                    max: self.max,
                }
            }
        }

        impl SampleIter for RangeIter<$t> {
            type Item = $t;

            fn next(&mut self, rng: &mut impl Sampler) -> $t {
                // [min, max) half-open range: use bounded_int(min, max-1)
                rng.bounded_int(self.min as i64, self.max as i64 - 1) as $t
            }
        }
    };
}

impl_range_i32!(i8);
impl_range_i32!(i16);
impl_range_i32!(i32);
impl_range_i32!(u8);
impl_range_i32!(u16);

// For larger types, we need bounded_int to work with the range
// For now, cast through i32 (limits range but works for most TPC-H cases)
impl Distribution<i64> for Range<i64> {
    type Iter = RangeIter<i64>;
    fn sample(&mut self) -> Self::Iter {
        RangeIter {
            min: self.min,
            max: self.max,
        }
    }
}

impl SampleIter for RangeIter<i64> {
    type Item = i64;
    fn next(&mut self, rng: &mut impl Sampler) -> i64 {
        rng.bounded_int(self.min, self.max - 1)
    }
}

impl Distribution<u32> for Range<u32> {
    type Iter = RangeIter<u32>;
    fn sample(&mut self) -> Self::Iter {
        RangeIter {
            min: self.min,
            max: self.max,
        }
    }
}

impl SampleIter for RangeIter<u32> {
    type Item = u32;
    fn next(&mut self, rng: &mut impl Sampler) -> u32 {
        rng.bounded_int(self.min as i64, self.max as i64 - 1) as u32
    }
}

impl Distribution<u64> for Range<u64> {
    type Iter = RangeIter<u64>;
    fn sample(&mut self) -> Self::Iter {
        RangeIter {
            min: self.min,
            max: self.max,
        }
    }
}

impl SampleIter for RangeIter<u64> {
    type Item = u64;
    fn next(&mut self, rng: &mut impl Sampler) -> u64 {
        rng.bounded_int(self.min as i64, self.max as i64 - 1) as u64
    }
}

impl Distribution<usize> for Range<usize> {
    type Iter = RangeIter<usize>;
    fn sample(&mut self) -> Self::Iter {
        RangeIter {
            min: self.min,
            max: self.max,
        }
    }
}

impl SampleIter for RangeIter<usize> {
    type Item = usize;
    fn next(&mut self, rng: &mut impl Sampler) -> usize {
        rng.bounded_int(self.min as i64, self.max as i64 - 1) as usize
    }
}

impl Distribution<f32> for Range<f32> {
    type Iter = RangeIter<f32>;
    fn sample(&mut self) -> Self::Iter {
        RangeIter {
            min: self.min,
            max: self.max,
        }
    }
}

impl SampleIter for RangeIter<f32> {
    type Item = f32;
    fn next(&mut self, rng: &mut impl Sampler) -> f32 {
        let t = populate_core::next_double(rng) as f32;
        self.min + t * (self.max - self.min)
    }
}

impl Distribution<f64> for Range<f64> {
    type Iter = RangeIter<f64>;
    fn sample(&mut self) -> Self::Iter {
        RangeIter {
            min: self.min,
            max: self.max,
        }
    }
}

impl SampleIter for RangeIter<f64> {
    type Item = f64;
    fn next(&mut self, rng: &mut impl Sampler) -> f64 {
        let t = populate_core::next_double(rng);
        self.min + t * (self.max - self.min)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;
    use rand::rngs::StdRng;

    #[test]
    fn test_range_i32() {
        let mut dist = Range::new(0i32, 100);
        let mut rng = StdRng::seed_from_u64(0);
        let mut iter = dist.sample();

        for _ in 0..100 {
            let v = iter.next(&mut rng);
            assert!(v >= 0 && v < 100);
        }
    }

    #[test]
    fn test_range_collect() {
        let mut dist = Range::new(0i32, 100);
        let mut rng = StdRng::seed_from_u64(0);
        let mut iter = dist.sample();

        let data: Vec<i32> = (0..10).map(|_| iter.next(&mut rng)).collect();
        assert_eq!(data.len(), 10);
        for v in data {
            assert!(v >= 0 && v < 100);
        }
    }
}
