use populate_core::Rng;
use populate_core::{Distribution, SampleIter};

/// A distribution that uses its own RNG instead of the passed-in one.
/// This allows subtrees to have independent random streams.
pub struct Seeded<R, D> {
    rng: R,
    dist: D,
}

impl<R, D> Seeded<R, D> {
    pub fn new(rng: R, dist: D) -> Self {
        Self { rng, dist }
    }
}

impl<R: Rng + Clone, T, D: Distribution<T>> Distribution<T> for Seeded<R, D> {
    type Iter = SeededIter<R, D::Iter>;
    type Sinks = D::Sinks;

    fn sample(&mut self) -> Self::Iter {
        SeededIter {
            rng: self.rng.clone(),
            iter: self.dist.sample(),
        }
    }

    fn collect_columns(&mut self, _rng: &mut impl Rng, n: usize) -> Self::Sinks {
        // Seeded uses its own RNG, ignore the passed-in one
        self.dist.collect_columns(&mut self.rng.clone(), n)
    }
}

#[derive(Clone)]
pub struct SeededIter<R, I> {
    rng: R,
    iter: I,
}

impl<R: Rng + Clone, I: SampleIter> SampleIter for SeededIter<R, I> {
    type Item = I::Item;

    fn next(&mut self, _rng: &mut impl Rng) -> Self::Item {
        self.iter.next(&mut self.rng)
    }
}
