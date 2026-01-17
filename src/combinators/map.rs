//! Map combinator for transforming distribution output.

use rand::Rng;

use crate::Distribution;

/// A combinator that transforms the output of a distribution.
///
/// # Example
///
/// ```
/// use data_comb::{Distribution, Uniform, Map};
/// use rand::thread_rng;
///
/// let dist = Map::new(Uniform::new(0, 100), |x| x * 2);
/// let mut rng = thread_rng();
/// let value = dist.sample(&mut rng);
/// assert!(value >= 0 && value < 200);
/// assert_eq!(value % 2, 0); // Always even
/// ```
#[derive(Debug, Clone, Copy)]
pub struct Map<D, F> {
    inner: D,
    f: F,
}

impl<D, F> Map<D, F> {
    /// Create a new map combinator.
    pub fn new(inner: D, f: F) -> Self {
        Map { inner, f }
    }

    /// Get a reference to the inner distribution.
    pub fn inner(&self) -> &D {
        &self.inner
    }
}

impl<T, U, D, F> Distribution<U> for Map<D, F>
where
    D: Distribution<T>,
    F: Fn(T) -> U,
{
    fn sample(&self, rng: &mut impl Rng) -> U {
        (self.f)(self.inner.sample(rng))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Constant;

    #[test]
    fn test_map_double() {
        let dist = Map::new(Constant::new(21), |x| x * 2);
        let mut rng = rand::thread_rng();
        assert_eq!(dist.sample(&mut rng), 42);
    }

    #[test]
    fn test_map_type_change() {
        let dist = Map::new(Constant::new(42), |x: i32| x.to_string());
        let mut rng = rand::thread_rng();
        assert_eq!(dist.sample(&mut rng), "42");
    }
}
