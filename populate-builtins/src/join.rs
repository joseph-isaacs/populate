use populate_core::Rng;
use populate_core::{Distribution, SampleIter};
use std::marker::PhantomData;

/// Joins array elements into a String with a separator.
pub struct Join<D, T, const N: usize> {
    inner: D,
    separator: &'static str,
    _marker: PhantomData<T>,
}

impl<D, T, const N: usize> Join<D, T, N> {
    pub fn new(inner: D, separator: &'static str) -> Self {
        Self {
            inner,
            separator,
            _marker: PhantomData,
        }
    }
}

#[derive(Clone)]
pub struct JoinIter<I> {
    inner: I,
    separator: &'static str,
}

impl<I, T, const N: usize> SampleIter for JoinIter<I>
where
    I: SampleIter<Item = [T; N]>,
    T: AsRef<str>,
{
    type Item = String;

    fn next(&mut self, rng: &mut (impl Rng + ?Sized)) -> String {
        let arr = self.inner.next(rng);
        arr.iter()
            .map(|s| s.as_ref())
            .collect::<Vec<_>>()
            .join(self.separator)
    }
}

impl<D, T, const N: usize> Distribution<String> for Join<D, T, N>
where
    D: Distribution<[T; N]>,
    D::Iter: SampleIter<Item = [T; N]>,
    T: AsRef<str>,
{
    type Iter = JoinIter<D::Iter>;

    fn sample(&mut self) -> Self::Iter {
        JoinIter {
            inner: self.inner.sample(),
            separator: self.separator,
        }
    }
}

/// Extension trait to add .join() method to distributions producing arrays.
pub trait JoinExt<T, const N: usize>: Sized {
    fn join(self, separator: &'static str) -> Join<Self, T, N> {
        Join::new(self, separator)
    }
}

impl<D, T, const N: usize> JoinExt<T, N> for D
where
    D: Distribution<[T; N]>,
    T: AsRef<str>,
{
}

/// Joins Vec elements into a String with a separator (runtime version).
pub struct JoinVec<D, T> {
    inner: D,
    separator: &'static str,
    _marker: PhantomData<T>,
}

impl<D, T> JoinVec<D, T> {
    pub fn new(inner: D, separator: &'static str) -> Self {
        Self {
            inner,
            separator,
            _marker: PhantomData,
        }
    }
}

#[derive(Clone)]
pub struct JoinVecIter<I> {
    inner: I,
    separator: &'static str,
}

impl<I, T> SampleIter for JoinVecIter<I>
where
    I: SampleIter<Item = Vec<T>>,
    T: AsRef<str>,
{
    type Item = String;

    fn next(&mut self, rng: &mut (impl Rng + ?Sized)) -> String {
        let vec = self.inner.next(rng);
        vec.iter()
            .map(|s| s.as_ref())
            .collect::<Vec<_>>()
            .join(self.separator)
    }
}

impl<D, T> Distribution<String> for JoinVec<D, T>
where
    D: Distribution<Vec<T>>,
    D::Iter: SampleIter<Item = Vec<T>>,
    T: AsRef<str>,
{
    type Iter = JoinVecIter<D::Iter>;

    fn sample(&mut self) -> Self::Iter {
        JoinVecIter {
            inner: self.inner.sample(),
            separator: self.separator,
        }
    }
}

/// Extension trait to add .join_vec() method to distributions producing Vec.
pub trait JoinVecExt<T>: Sized {
    fn join_vec(self, separator: &'static str) -> JoinVec<Self, T> {
        JoinVec::new(self, separator)
    }
}

impl<D, T> JoinVecExt<T> for D
where
    D: Distribution<Vec<T>>,
    T: AsRef<str>,
{
}

/// Joins 2-tuple elements into a String with a separator.
pub struct JoinTuple2<D, T1, T2> {
    inner: D,
    separator: &'static str,
    _marker: PhantomData<(T1, T2)>,
}

impl<D, T1, T2> JoinTuple2<D, T1, T2> {
    pub fn new(inner: D, separator: &'static str) -> Self {
        Self {
            inner,
            separator,
            _marker: PhantomData,
        }
    }
}

#[derive(Clone)]
pub struct JoinTuple2Iter<I> {
    inner: I,
    separator: &'static str,
}

impl<I, T1, T2> SampleIter for JoinTuple2Iter<I>
where
    I: SampleIter<Item = (T1, T2)>,
    T1: AsRef<str>,
    T2: AsRef<str>,
{
    type Item = String;

    fn next(&mut self, rng: &mut (impl Rng + ?Sized)) -> String {
        let (a, b) = self.inner.next(rng);
        format!("{}{}{}", a.as_ref(), self.separator, b.as_ref())
    }
}

impl<D, T1, T2> Distribution<String> for JoinTuple2<D, T1, T2>
where
    D: Distribution<(T1, T2)>,
    D::Iter: SampleIter<Item = (T1, T2)>,
    T1: AsRef<str>,
    T2: AsRef<str>,
{
    type Iter = JoinTuple2Iter<D::Iter>;

    fn sample(&mut self) -> Self::Iter {
        JoinTuple2Iter {
            inner: self.inner.sample(),
            separator: self.separator,
        }
    }
}

/// Joins 3-tuple elements into a String with a separator.
pub struct JoinTuple3<D, T1, T2, T3> {
    inner: D,
    separator: &'static str,
    _marker: PhantomData<(T1, T2, T3)>,
}

impl<D, T1, T2, T3> JoinTuple3<D, T1, T2, T3> {
    pub fn new(inner: D, separator: &'static str) -> Self {
        Self {
            inner,
            separator,
            _marker: PhantomData,
        }
    }
}

#[derive(Clone)]
pub struct JoinTuple3Iter<I> {
    inner: I,
    separator: &'static str,
}

impl<I, T1, T2, T3> SampleIter for JoinTuple3Iter<I>
where
    I: SampleIter<Item = (T1, T2, T3)>,
    T1: AsRef<str>,
    T2: AsRef<str>,
    T3: AsRef<str>,
{
    type Item = String;

    fn next(&mut self, rng: &mut (impl Rng + ?Sized)) -> String {
        let (a, b, c) = self.inner.next(rng);
        format!(
            "{}{}{}{}{}",
            a.as_ref(),
            self.separator,
            b.as_ref(),
            self.separator,
            c.as_ref()
        )
    }
}

impl<D, T1, T2, T3> Distribution<String> for JoinTuple3<D, T1, T2, T3>
where
    D: Distribution<(T1, T2, T3)>,
    D::Iter: SampleIter<Item = (T1, T2, T3)>,
    T1: AsRef<str>,
    T2: AsRef<str>,
    T3: AsRef<str>,
{
    type Iter = JoinTuple3Iter<D::Iter>;

    fn sample(&mut self) -> Self::Iter {
        JoinTuple3Iter {
            inner: self.inner.sample(),
            separator: self.separator,
        }
    }
}

/// Extension trait for joining 2-tuple distributions.
pub trait JoinTuple2Ext<T1, T2>: Sized {
    fn join2(self, separator: &'static str) -> JoinTuple2<Self, T1, T2> {
        JoinTuple2::new(self, separator)
    }
}

impl<D, T1, T2> JoinTuple2Ext<T1, T2> for D
where
    D: Distribution<(T1, T2)>,
    T1: AsRef<str>,
    T2: AsRef<str>,
{
}

/// Extension trait for joining 3-tuple distributions.
pub trait JoinTuple3Ext<T1, T2, T3>: Sized {
    fn join3(self, separator: &'static str) -> JoinTuple3<Self, T1, T2, T3> {
        JoinTuple3::new(self, separator)
    }
}

impl<D, T1, T2, T3> JoinTuple3Ext<T1, T2, T3> for D
where
    D: Distribution<(T1, T2, T3)>,
    T1: AsRef<str>,
    T2: AsRef<str>,
    T3: AsRef<str>,
{
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Pick, constant, list, pick_one, range};
    use rand::SeedableRng;
    use rand::rngs::StdRng;

    const COLORS: [&str; 5] = ["red", "green", "blue", "yellow", "purple"];

    #[test]
    fn test_join() {
        let mut dist = Pick::<_, _, 3>::new(&COLORS, range(0i64, COLORS.len() as i64)).join(" ");
        let mut rng = StdRng::seed_from_u64(42);
        let mut iter = dist.sample();

        let joined = iter.next(&mut rng);
        let parts: Vec<_> = joined.split(' ').collect();
        assert_eq!(parts.len(), 3);
        for part in parts {
            assert!(COLORS.contains(&part));
        }
    }

    #[test]
    fn test_join_vec() {
        // Runtime version: list(constant(3), pick_one(range, &COLORS)).join_vec(" ")
        let mut dist = list(
            constant(3usize),
            pick_one(range(0i64, COLORS.len() as i64), &COLORS),
        )
        .join_vec(" ");
        let mut rng = StdRng::seed_from_u64(42);
        let mut iter = dist.sample();

        let joined = iter.next(&mut rng);
        let parts: Vec<_> = joined.split(' ').collect();
        assert_eq!(parts.len(), 3);
        for part in parts {
            assert!(COLORS.contains(&part));
        }
    }
}
