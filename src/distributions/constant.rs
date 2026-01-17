//! A distribution that always returns the same value.

use rand::Rng;

use crate::Distribution;

/// A distribution that always produces the same value.
///
/// # Example
///
/// ```
/// use data_comb::{Distribution, Constant};
/// use rand::thread_rng;
///
/// let dist = Constant::new(42);
/// let mut rng = thread_rng();
/// assert_eq!(dist.sample(&mut rng), 42);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Constant<T>(T);

impl<T> Constant<T> {
    /// Create a new constant distribution that always returns `value`.
    pub fn new(value: T) -> Self {
        Constant(value)
    }

    /// Get a reference to the constant value.
    pub fn value(&self) -> &T {
        &self.0
    }

    /// Consume this distribution and return the inner value.
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T: Clone> Distribution<T> for Constant<T> {
    fn sample(&self, _rng: &mut impl Rng) -> T {
        self.0.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_constant_i32() {
        let dist = Constant::new(42);
        let mut rng = rand::thread_rng();
        for _ in 0..100 {
            assert_eq!(dist.sample(&mut rng), 42);
        }
    }

    #[test]
    fn test_constant_string() {
        let dist = Constant::new(String::from("hello"));
        let mut rng = rand::thread_rng();
        assert_eq!(dist.sample(&mut rng), "hello");
    }
}
