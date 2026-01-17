//! Numeric distributions.

use rand::distributions::uniform::SampleUniform;
use rand::Rng;

use crate::Distribution;

/// A uniform distribution over integers in a range `[low, high)`.
///
/// # Example
///
/// ```
/// use data_comb::{Distribution, Uniform};
/// use rand::thread_rng;
///
/// let dist = Uniform::new(0, 100);
/// let mut rng = thread_rng();
/// let value = dist.sample(&mut rng);
/// assert!(value >= 0 && value < 100);
/// ```
#[derive(Debug, Clone, Copy)]
pub struct Uniform<T> {
    low: T,
    high: T,
}

impl<T> Uniform<T> {
    /// Create a new uniform distribution over `[low, high)`.
    ///
    /// # Panics
    ///
    /// Panics if `low >= high`.
    pub fn new(low: T, high: T) -> Self
    where
        T: PartialOrd,
    {
        assert!(
            low < high,
            "Uniform::new requires low < high"
        );
        Uniform { low, high }
    }
}

impl<T> Distribution<T> for Uniform<T>
where
    T: SampleUniform + Clone,
{
    fn sample(&self, rng: &mut impl Rng) -> T {
        rng.gen_range(self.low.clone()..self.high.clone())
    }
}

/// A uniform distribution over floating-point numbers in a range `[low, high)`.
///
/// # Example
///
/// ```
/// use data_comb::{Distribution, UniformFloat};
/// use rand::thread_rng;
///
/// let dist = UniformFloat::new(0.0, 1.0);
/// let mut rng = thread_rng();
/// let value = dist.sample(&mut rng);
/// assert!(value >= 0.0 && value < 1.0);
/// ```
#[derive(Debug, Clone, Copy)]
pub struct UniformFloat<T> {
    low: T,
    high: T,
}

impl<T> UniformFloat<T> {
    /// Create a new uniform distribution over `[low, high)`.
    ///
    /// # Panics
    ///
    /// Panics if `low >= high`.
    pub fn new(low: T, high: T) -> Self
    where
        T: PartialOrd,
    {
        assert!(
            low < high,
            "UniformFloat::new requires low < high"
        );
        UniformFloat { low, high }
    }
}

impl<T> Distribution<T> for UniformFloat<T>
where
    T: SampleUniform + Clone,
{
    fn sample(&self, rng: &mut impl Rng) -> T {
        rng.gen_range(self.low.clone()..self.high.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_uniform_i32() {
        let dist = Uniform::new(0, 100);
        let mut rng = rand::thread_rng();
        for _ in 0..1000 {
            let v = dist.sample(&mut rng);
            assert!(v >= 0 && v < 100);
        }
    }

    #[test]
    fn test_uniform_f64() {
        let dist = UniformFloat::new(0.0, 1.0);
        let mut rng = rand::thread_rng();
        for _ in 0..1000 {
            let v = dist.sample(&mut rng);
            assert!(v >= 0.0 && v < 1.0);
        }
    }

    #[test]
    #[should_panic(expected = "requires low < high")]
    fn test_uniform_invalid_range() {
        let _ = Uniform::new(100, 0);
    }
}
