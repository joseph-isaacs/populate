use populate_core::Rng;
use populate_core::{Distribution, SampleIter};

/// A distribution that produces incrementing values starting from a given value.
pub struct Inc<T> {
    start: T,
}

impl<T: Clone> Inc<T> {
    pub fn new(start: T) -> Self {
        Self { start }
    }
}

#[derive(Clone)]
pub struct IncIter<T> {
    current: T,
}

macro_rules! impl_inc {
    ($t:ty) => {
        impl Distribution<$t> for Inc<$t> {
            type Iter = IncIter<$t>;

            fn sample(&mut self) -> Self::Iter {
                IncIter {
                    current: self.start,
                }
            }
        }

        impl SampleIter for IncIter<$t> {
            type Item = $t;

            fn next(&mut self, _rng: &mut impl Rng) -> $t {
                let v = self.current;
                self.current += 1;
                v
            }
        }
    };
}

impl_inc!(i8);
impl_inc!(i16);
impl_inc!(i32);
impl_inc!(i64);
impl_inc!(u8);
impl_inc!(u16);
impl_inc!(u32);
impl_inc!(u64);
impl_inc!(usize);

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;
    use rand::rngs::StdRng;

    #[test]
    fn test_inc() {
        let mut dist = Inc::new(0i64);
        let mut rng = StdRng::seed_from_u64(0);
        let mut iter = dist.sample();

        assert_eq!(iter.next(&mut rng), 0);
        assert_eq!(iter.next(&mut rng), 1);
        assert_eq!(iter.next(&mut rng), 2);
        assert_eq!(iter.next(&mut rng), 3);
    }

    #[test]
    fn test_inc_start_at() {
        let mut dist = Inc::new(100i32);
        let mut rng = StdRng::seed_from_u64(0);
        let mut iter = dist.sample();

        assert_eq!(iter.next(&mut rng), 100);
        assert_eq!(iter.next(&mut rng), 101);
        assert_eq!(iter.next(&mut rng), 102);
    }
}
