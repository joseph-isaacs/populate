use crate::{Distribution, SampleIter};
use rand::Rng;

/// A distribution that always produces the same value.
pub struct Constant<T>(pub T);

impl<T> Constant<T> {
    pub fn new(value: T) -> Self {
        Constant(value)
    }
}

/// Iterator for Constant distribution.
#[derive(Clone)]
pub struct ConstantIter<T>(T);

impl<T: Clone> Distribution<T> for Constant<T> {
    type Iter = ConstantIter<T>;

    fn sample(&mut self) -> Self::Iter {
        ConstantIter(self.0.clone())
    }
}

impl<T: Clone> SampleIter for ConstantIter<T> {
    type Item = T;

    fn next(&mut self, _rng: &mut impl Rng) -> T {
        self.0.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;
    use rand::rngs::StdRng;

    #[test]
    fn test_constant() {
        let mut dist = Constant::new(42i32);
        let mut rng = StdRng::seed_from_u64(0);
        let mut iter = dist.sample();

        assert_eq!(iter.next(&mut rng), 42);
        assert_eq!(iter.next(&mut rng), 42);
        assert_eq!(iter.next(&mut rng), 42);
    }
}
