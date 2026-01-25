use populate_core::{ColumnarDistribution, Distribution, Rng, SampleIter};
use std::marker::PhantomData;

/// Maps the output of a distribution through a function.
pub struct Map<D, F, T> {
    inner: D,
    f: F,
    _marker: PhantomData<T>,
}

impl<D, F, T> Map<D, F, T> {
    pub fn new(inner: D, f: F) -> Self {
        Self {
            inner,
            f,
            _marker: PhantomData,
        }
    }
}

#[derive(Clone)]
pub struct MapIter<I, F> {
    inner: I,
    f: F,
}

impl<I, F, T, U> SampleIter for MapIter<I, F>
where
    I: SampleIter<Item = T>,
    F: FnMut(T) -> U + Clone,
{
    type Item = U;

    fn next(&mut self, rng: &mut impl Rng) -> U {
        (self.f)(self.inner.next(rng))
    }
}

impl<D, F, T, U> Distribution<U> for Map<D, F, T>
where
    D: Distribution<T>,
    F: FnMut(T) -> U + Clone,
{
    type Iter = MapIter<D::Iter, F>;

    fn sample(&mut self) -> Self::Iter {
        MapIter {
            inner: self.inner.sample(),
            f: self.f.clone(),
        }
    }
}

impl<D, F, T, U> ColumnarDistribution<U> for Map<D, F, T>
where
    D: Distribution<T>,
    F: FnMut(T) -> U + Clone,
{
    // Map stays bundled - the output depends on the input
    type Cols = Self;

    fn into_cols(self) -> Self::Cols {
        self
    }
}

/// Extension trait to add .map() method to distributions.
pub trait MapExt<T>: Distribution<T> + Sized {
    fn map<F, U>(self, f: F) -> Map<Self, F, T>
    where
        F: FnMut(T) -> U + Clone,
    {
        Map::new(self, f)
    }
}

impl<D, T> MapExt<T> for D where D: Distribution<T> {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::constant;
    use rand::SeedableRng;
    use rand::rngs::StdRng;

    #[test]
    fn test_map() {
        let mut dist = constant(5i32).map(|x| x * 2);
        let mut rng = StdRng::seed_from_u64(0);
        let mut iter = dist.sample();

        assert_eq!(iter.next(&mut rng), 10);
        assert_eq!(iter.next(&mut rng), 10);
    }

    #[test]
    fn test_map_flatten_tuple() {
        let mut dist = (constant(1i32), constant((2i32, 3i32))).map(|(a, (b, c))| (a, b, c));
        let mut rng = StdRng::seed_from_u64(0);
        let mut iter = dist.sample();

        assert_eq!(iter.next(&mut rng), (1, 2, 3));
    }
}
