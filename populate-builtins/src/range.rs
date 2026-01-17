use populate_core::{Distribution, SampleIter};
use rand::Rng;

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

macro_rules! impl_range {
    ($t:ty) => {
        impl Distribution for Range<$t> {
            type Item = $t;
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

            fn next(&mut self, rng: &mut impl Rng) -> $t {
                rng.random_range(self.min..self.max)
            }
        }
    };
}

impl_range!(i8);
impl_range!(i16);
impl_range!(i32);
impl_range!(i64);
impl_range!(u8);
impl_range!(u16);
impl_range!(u32);
impl_range!(u64);
impl_range!(usize);
impl_range!(f32);
impl_range!(f64);

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
