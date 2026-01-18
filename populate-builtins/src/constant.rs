use populate_core::{Distribution, DynDistribution, DynSampleIter, Rng, RngCore, SampleIter};

/// A distribution that always produces the same value.
pub struct Constant<T> {
    value: T,
}

impl<T> Constant<T> {
    pub fn new(value: T) -> Self {
        Self { value }
    }
}

/// Iterator for constant values - borrows from the parent Constant.
pub struct ConstantIter<'a, T> {
    value: &'a T,
}

impl<T: Clone> SampleIter for ConstantIter<'_, T> {
    type Item = T;

    fn next(&mut self, _rng: &mut impl Rng) -> T {
        self.value.clone()
    }

    fn reset(&mut self) {
        // No state to reset
    }
}

impl<T: Clone> DynSampleIter for ConstantIter<'_, T> {
    type Item = T;

    fn next_dyn(&mut self, _rng: &mut dyn RngCore) -> T {
        self.value.clone()
    }

    fn reset_dyn(&mut self) {
        // No state to reset
    }
}

impl<T: Clone> Distribution<T> for Constant<T> {
    type Iter<'a>
        = ConstantIter<'a, T>
    where
        Self: 'a;

    fn sample(&mut self) -> Self::Iter<'_> {
        ConstantIter { value: &self.value }
    }
}

impl<T: Clone + 'static> DynDistribution<T> for Constant<T> {
    fn sample_boxed(&mut self) -> Box<dyn DynSampleIter<Item = T> + '_> {
        Box::new(self.sample())
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
