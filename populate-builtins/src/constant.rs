use populate_core::{Distribution, SampleIter};
use rand::Rng;

/// A distribution that always produces the same value.
pub struct Constant<T> {
    value: T,
}

impl<T: Clone> Constant<T> {
    pub fn new(value: T) -> Self {
        Self { value }
    }
}

/// Iterator for constant values.
#[derive(Clone)]
pub struct ConstantIter<T>(T);

impl<T: Clone> SampleIter for ConstantIter<T> {
    type Item = T;

    fn next(&mut self, _rng: &mut impl Rng) -> T {
        self.0.clone()
    }
}

impl<T: Clone> Distribution for Constant<T> {
    type Item = T;
    type Iter = ConstantIter<T>;

    fn sample(&mut self) -> Self::Iter {
        ConstantIter(self.value.clone())
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
