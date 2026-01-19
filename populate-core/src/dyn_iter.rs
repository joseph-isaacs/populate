use std::any::Any;

use dyn_clone::DynClone;
use rand::{Rng, RngCore};

use crate::{Distribution, SampleIter};

/// Type-erased iterator trait.
///
/// This allows boxing iterators while still being able to push values
/// directly to typed sinks without boxing the values themselves.
pub trait ErasedIter: DynClone + Send {
    /// Generate next value and push directly to a typed sink.
    ///
    /// The sink must be downcastable to `Vec<Item>`.
    /// Panics if the sink type doesn't match.
    fn next_push(&mut self, rng: &mut dyn RngCore, sink: &mut dyn Any);

    /// Generate next value and return as Box<dyn Any>.
    ///
    /// This is less efficient (boxes each value) but useful for
    /// cases where you need the value directly.
    fn next_boxed(&mut self, rng: &mut dyn RngCore) -> Box<dyn Any + Send>;
}

dyn_clone::clone_trait_object!(ErasedIter);

/// A type-erased iterator that can be cloned and used dynamically.
///
/// Values are not boxed - they flow directly to typed sinks via downcast.
/// Only the iterator itself is boxed.
pub struct DynIter(Box<dyn ErasedIter>);

impl Clone for DynIter {
    fn clone(&self) -> Self {
        DynIter(dyn_clone::clone_box(&*self.0))
    }
}

impl DynIter {
    /// Create a new DynIter from any SampleIter.
    pub fn new<I>(iter: I) -> Self
    where
        I: SampleIter + Send + 'static,
        I::Item: Send + 'static,
    {
        DynIter(Box::new(ErasedIterWrapper(iter)))
    }

    /// Push next value directly to a Vec<T>.
    ///
    /// # Panics
    /// Panics if T doesn't match the iterator's item type.
    #[allow(clippy::ptr_arg)] // We need Vec for push(), not slice
    pub fn next_push<T: 'static>(&mut self, rng: &mut impl Rng, sink: &mut Vec<T>) {
        self.0.next_push(rng, sink)
    }

    /// Push next value to any dyn Any (must be Vec<T> where T matches).
    pub fn next_push_any(&mut self, rng: &mut impl Rng, sink: &mut dyn Any) {
        self.0.next_push(rng, sink)
    }

    /// Get next value as Box<dyn Any>.
    pub fn next_boxed(&mut self, rng: &mut impl Rng) -> Box<dyn Any + Send> {
        self.0.next_boxed(rng)
    }

    /// Collect n values into a Vec<T>.
    ///
    /// # Panics
    /// Panics if T doesn't match the iterator's item type.
    pub fn collect_vec<T: 'static>(&mut self, rng: &mut impl Rng, n: usize) -> Vec<T> {
        let mut out: Vec<T> = Vec::with_capacity(n);
        for _ in 0..n {
            self.0.next_push(rng, &mut out);
        }
        out
    }
}

/// Wrapper that implements ErasedIter for any SampleIter.
struct ErasedIterWrapper<I>(I);

impl<I: Clone> Clone for ErasedIterWrapper<I> {
    fn clone(&self) -> Self {
        ErasedIterWrapper(self.0.clone())
    }
}

impl<I> ErasedIter for ErasedIterWrapper<I>
where
    I: SampleIter + Send + Clone + 'static,
    I::Item: Send + 'static,
{
    fn next_push(&mut self, rng: &mut dyn RngCore, sink: &mut dyn Any) {
        let val = self.0.next(rng);
        // Downcast to Vec<T>
        if let Some(vec) = sink.downcast_mut::<Vec<I::Item>>() {
            vec.push(val);
            return;
        }
        panic!(
            "DynIter: sink type mismatch. Expected Vec<{}>",
            std::any::type_name::<I::Item>()
        );
    }

    fn next_boxed(&mut self, rng: &mut dyn RngCore) -> Box<dyn Any + Send> {
        Box::new(self.0.next(rng))
    }
}

// === DynDistribution ===

/// Type-erased distribution trait.
pub trait ErasedDistribution: Send {
    /// Create a type-erased iterator.
    fn sample_dyn(&mut self) -> DynIter;
}

/// A type-erased distribution.
pub struct DynDistribution(Box<dyn ErasedDistribution>);

impl DynDistribution {
    /// Create a new DynDistribution from any Distribution.
    pub fn new<D, T>(dist: D) -> Self
    where
        D: Distribution<T> + Send + 'static,
        D::Iter: Send + 'static,
        T: Send + 'static,
    {
        DynDistribution(Box::new(ErasedDistWrapper(dist, std::marker::PhantomData)))
    }

    /// Sample a new type-erased iterator.
    pub fn sample(&mut self) -> DynIter {
        self.0.sample_dyn()
    }

    /// Collect n values into a Vec<T>.
    ///
    /// # Panics
    /// Panics if T doesn't match the distribution's item type.
    pub fn collect_vec<T: 'static>(&mut self, rng: &mut impl Rng, n: usize) -> Vec<T> {
        self.sample().collect_vec(rng, n)
    }
}

/// Wrapper that implements ErasedDistribution for any Distribution.
struct ErasedDistWrapper<D, T>(D, std::marker::PhantomData<T>);

impl<D, T> ErasedDistribution for ErasedDistWrapper<D, T>
where
    D: Distribution<T> + Send + 'static,
    D::Iter: Send + 'static,
    T: Send + 'static,
{
    fn sample_dyn(&mut self) -> DynIter {
        DynIter::new(self.0.sample())
    }
}

// === Extension trait for easy conversion ===

/// Extension trait to convert any Distribution to DynDistribution.
pub trait IntoDyn<T>: Distribution<T> + Sized {
    fn into_dyn(self) -> DynDistribution
    where
        Self: Send + 'static,
        Self::Iter: Send + 'static,
        T: Send + 'static,
    {
        DynDistribution::new(self)
    }
}

impl<T, D: Distribution<T>> IntoDyn<T> for D {}

/// Extension trait to convert any SampleIter to DynIter.
pub trait IntoDynIter: SampleIter + Sized {
    fn into_dyn_iter(self) -> DynIter
    where
        Self: Send + 'static,
        Self::Item: Send + 'static,
    {
        DynIter::new(self)
    }
}

impl<I: SampleIter> IntoDynIter for I {}

#[cfg(test)]
mod tests {
    use super::*;

    // Simple test iterator
    #[derive(Clone)]
    struct Counter(i64);

    impl SampleIter for Counter {
        type Item = i64;
        fn next(&mut self, _rng: &mut (impl Rng + ?Sized)) -> i64 {
            let v = self.0;
            self.0 += 1;
            v
        }
    }

    struct CounterDist(i64);

    impl Distribution<i64> for CounterDist {
        type Iter = Counter;
        fn sample(&mut self) -> Counter {
            Counter(self.0)
        }
    }

    #[test]
    fn test_dyn_iter_collect_vec() {
        let mut rng = rand::rng();
        let mut dyn_iter = DynIter::new(Counter(0));
        let result: Vec<i64> = dyn_iter.collect_vec(&mut rng, 5);
        assert_eq!(result, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn test_dyn_iter_clone() {
        let mut rng = rand::rng();
        let dyn_iter = DynIter::new(Counter(10));

        let mut iter1 = dyn_iter.clone();
        let mut iter2 = dyn_iter.clone();

        let r1: Vec<i64> = iter1.collect_vec(&mut rng, 3);
        let r2: Vec<i64> = iter2.collect_vec(&mut rng, 3);

        // Both clones should produce same sequence
        assert_eq!(r1, vec![10, 11, 12]);
        assert_eq!(r2, vec![10, 11, 12]);
    }

    #[test]
    fn test_dyn_distribution() {
        let mut rng = rand::rng();
        let mut dyn_dist = DynDistribution::new(CounterDist(100));

        let result: Vec<i64> = dyn_dist.collect_vec(&mut rng, 3);
        assert_eq!(result, vec![100, 101, 102]);
    }

    #[test]
    fn test_into_dyn_extension() {
        let mut rng = rand::rng();
        let mut dyn_dist = CounterDist(50).into_dyn();

        let result: Vec<i64> = dyn_dist.collect_vec(&mut rng, 3);
        assert_eq!(result, vec![50, 51, 52]);
    }

    #[test]
    #[should_panic(expected = "sink type mismatch")]
    fn test_type_mismatch_panics() {
        let mut rng = rand::rng();
        let mut dyn_iter = DynIter::new(Counter(0));

        // Try to collect i64 into Vec<String> - should panic
        let _: Vec<String> = dyn_iter.collect_vec(&mut rng, 1);
    }
}
