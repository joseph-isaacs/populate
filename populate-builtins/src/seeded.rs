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
    type Iter<'a>
        = SeededIter<R, D::Iter<'a>>
    where
        Self: 'a;

    fn sample(&mut self) -> Self::Iter<'_> {
        SeededIter {
            rng: self.rng.clone(),
            iter: self.dist.sample(),
        }
    }
}

pub struct SeededIter<R, I> {
    rng: R,
    iter: I,
}

impl<R: Rng, I: SampleIter> SampleIter for SeededIter<R, I> {
    type Item = I::Item;

    fn next(&mut self, _rng: &mut impl Rng) -> Self::Item {
        self.iter.next(&mut self.rng)
    }

    fn reset(&mut self) {
        self.iter.reset();
    }
}
