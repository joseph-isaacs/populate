//! Optional combinator for nullable values.

use rand::Rng;

use crate::Distribution;

/// A combinator that wraps a distribution to produce `Option<T>` values.
///
/// This is useful for generating nullable columns where some fraction of
/// values should be `None`.
///
/// # Example
///
/// ```
/// use data_comb::{Distribution, Uniform, Optional};
/// use rand::thread_rng;
///
/// // 20% of values will be None
/// let dist = Optional::new(Uniform::new(0, 100), 0.2);
/// let mut rng = thread_rng();
/// let value: Option<i32> = dist.sample(&mut rng);
/// ```
#[derive(Debug, Clone, Copy)]
pub struct Optional<D> {
    inner: D,
    null_probability: f64,
}

impl<D> Optional<D> {
    /// Create a new optional distribution.
    ///
    /// # Arguments
    ///
    /// * `inner` - The underlying distribution to sample from
    /// * `null_probability` - Probability of producing `None` (must be in `[0, 1]`)
    ///
    /// # Panics
    ///
    /// Panics if `null_probability` is not in `[0, 1]`.
    pub fn new(inner: D, null_probability: f64) -> Self {
        assert!(
            (0.0..=1.0).contains(&null_probability),
            "null_probability must be in [0, 1], got {}",
            null_probability
        );
        Optional {
            inner,
            null_probability,
        }
    }

    /// Create a distribution that never produces `None`.
    pub fn always_some(inner: D) -> Self {
        Optional {
            inner,
            null_probability: 0.0,
        }
    }

    /// Create a distribution that always produces `None`.
    pub fn always_none(inner: D) -> Self {
        Optional {
            inner,
            null_probability: 1.0,
        }
    }

    /// Get the null probability.
    pub fn null_probability(&self) -> f64 {
        self.null_probability
    }

    /// Get a reference to the inner distribution.
    pub fn inner(&self) -> &D {
        &self.inner
    }
}

impl<T, D: Distribution<T>> Distribution<Option<T>> for Optional<D> {
    fn sample(&self, rng: &mut impl Rng) -> Option<T> {
        if rng.gen::<f64>() < self.null_probability {
            None
        } else {
            Some(self.inner.sample(rng))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Constant;

    #[test]
    fn test_always_some() {
        let dist = Optional::always_some(Constant::new(42));
        let mut rng = rand::thread_rng();
        for _ in 0..100 {
            assert_eq!(dist.sample(&mut rng), Some(42));
        }
    }

    #[test]
    fn test_always_none() {
        let dist = Optional::always_none(Constant::new(42));
        let mut rng = rand::thread_rng();
        for _ in 0..100 {
            assert_eq!(dist.sample(&mut rng), None);
        }
    }

    #[test]
    fn test_partial_null() {
        let dist = Optional::new(Constant::new(42), 0.5);
        let mut rng = rand::thread_rng();
        let samples: Vec<_> = (0..1000).map(|_| dist.sample(&mut rng)).collect();

        let null_count = samples.iter().filter(|v| v.is_none()).count();
        // Should be roughly 50% null (allow wide margin for randomness)
        assert!(null_count > 300 && null_count < 700);
    }

    #[test]
    #[should_panic(expected = "null_probability must be in [0, 1]")]
    fn test_invalid_null_probability() {
        let _ = Optional::new(Constant::new(42), 1.5);
    }
}
