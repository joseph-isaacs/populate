use crate::{Distribution, SampleIter};
use rand::Rng;

/// A distribution that produces uniform random values in a range [min, max).
pub struct Range<T> {
    min: T,
    max: T,
}

impl<T> Range<T> {
    pub fn new(min: T, max: T) -> Self {
        Range { min, max }
    }
}

/// Iterator for Range distribution.
#[derive(Clone)]
pub struct RangeIter<T> {
    min: T,
    max: T,
}

macro_rules! impl_range {
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

            fn next(&mut self, rng: &mut impl Rng) -> $t {
                rng.gen_range(self.min..self.max)
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
    use crate::{collect::Vecs, collect::ElementCollector};
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    #[test]
    fn test_range_i32() {
        let mut dist = Range::new(0i32, 100);
        let mut rng = StdRng::seed_from_u64(42);
        let mut iter = dist.sample();

        for _ in 0..100 {
            let val = iter.next(&mut rng);
            assert!(val >= 0 && val < 100);
        }
    }

    #[test]
    fn test_range_collect() {
        let mut dist = Range::new(0i32, 1000);
        let mut rng = StdRng::seed_from_u64(42);

        let data: Vec<i32> = Vecs::collect_element(dist.sample(), &mut rng, 1000);
        assert_eq!(data.len(), 1000);
        assert!(data.iter().all(|&x| x >= 0 && x < 1000));
    }
}
