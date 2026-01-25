use populate_core::Rng;
use populate_core::{CollectColumnar, Distribution, SampleIter, SinkTuple};

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

    fn sample(&mut self) -> Self::Iter {
        SeededIter {
            rng: self.rng.clone(),
            iter: self.dist.sample(),
        }
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

/// SeededIter preserves columnar capability from the inner iterator.
impl<R: Rng + Clone, I: SampleIter, S: SinkTuple<I::Item>> CollectColumnar<S> for SeededIter<R, I> {
    const IS_COLUMNAR: bool = false; // Can't delegate columnar since we use our own RNG
    type Output = S::Output;

    fn collect_columnar(mut self, _rng: &mut impl Rng, n: usize, mut sinks: S) -> Self::Output {
        // Use the seeded RNG, not the passed-in one
        for _ in 0..n {
            sinks.push_row(self.iter.next(&mut self.rng));
        }
        sinks.finish()
    }
}
