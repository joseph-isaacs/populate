//! Core distribution trait for generating values.

use rand::Rng;

/// A distribution that can produce values of type `T`.
///
/// This is the core abstraction for value generation. Distributions are
/// composable - they can be combined using combinators to create new
/// distributions with different statistical properties.
///
/// # Design Philosophy
///
/// Distributions know nothing about storage. They only know how to produce
/// values. This separation allows the same distribution to be used with
/// different storage backends (Vec, Arrow arrays, etc.)
///
/// # Example
///
/// ```
/// use data_comb::Distribution;
/// use rand::thread_rng;
///
/// struct Constant<T>(T);
///
/// impl<T: Clone> Distribution<T> for Constant<T> {
///     fn sample(&self, _rng: &mut impl Rng) -> T {
///         self.0.clone()
///     }
/// }
/// ```
pub trait Distribution<T> {
    /// Generate a single value from this distribution.
    fn sample(&self, rng: &mut impl Rng) -> T;

    /// Generate multiple values from this distribution.
    fn sample_iter<'a, R: Rng>(
        &'a self,
        rng: &'a mut R,
    ) -> DistributionIter<'a, T, Self, R>
    where
        Self: Sized,
    {
        DistributionIter {
            dist: self,
            rng,
            _marker: std::marker::PhantomData,
        }
    }
}

/// An iterator that samples from a distribution.
pub struct DistributionIter<'a, T, D: Distribution<T>, R: Rng> {
    dist: &'a D,
    rng: &'a mut R,
    _marker: std::marker::PhantomData<T>,
}

impl<'a, T, D: Distribution<T>, R: Rng> Iterator for DistributionIter<'a, T, D, R> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        Some(self.dist.sample(self.rng))
    }
}

// Implement Distribution for references to distributions
impl<T, D: Distribution<T> + ?Sized> Distribution<T> for &D {
    fn sample(&self, rng: &mut impl Rng) -> T {
        (*self).sample(rng)
    }
}

impl<T, D: Distribution<T> + ?Sized> Distribution<T> for &mut D {
    fn sample(&self, rng: &mut impl Rng) -> T {
        (**self).sample(rng)
    }
}

impl<T, D: Distribution<T> + ?Sized> Distribution<T> for Box<D> {
    fn sample(&self, rng: &mut impl Rng) -> T {
        (**self).sample(rng)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestDist(i32);

    impl Distribution<i32> for TestDist {
        fn sample(&self, _rng: &mut impl Rng) -> i32 {
            self.0
        }
    }

    #[test]
    fn test_sample_iter() {
        let dist = TestDist(42);
        let mut rng = rand::thread_rng();
        let samples: Vec<_> = dist.sample_iter(&mut rng).take(5).collect();
        assert_eq!(samples, vec![42, 42, 42, 42, 42]);
    }

    #[test]
    fn test_boxed_distribution() {
        let dist: Box<dyn Distribution<i32>> = Box::new(TestDist(42));
        let mut rng = rand::thread_rng();
        assert_eq!(dist.sample(&mut rng), 42);
    }
}
