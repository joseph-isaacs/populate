use populate_core::{Distribution, SampleIter};

/// A distribution wrapper that prints a label each time a value is sampled.
pub struct Print<D> {
    label: &'static str,
    inner: D,
}

impl<D> Print<D> {
    pub fn new(label: &'static str, inner: D) -> Self {
        Self { label, inner }
    }
}

pub struct PrintIter<I> {
    label: &'static str,
    inner: I,
}

impl<I: Clone> Clone for PrintIter<I> {
    fn clone(&self) -> Self {
        Self {
            label: self.label,
            inner: self.inner.clone(),
        }
    }
}

impl<I: SampleIter> SampleIter for PrintIter<I> {
    type Item = I::Item;

    fn next(&mut self, rng: &mut impl rand::Rng) -> Self::Item {
        println!("{}", self.label);
        self.inner.next(rng)
    }
}

impl<T, D: Distribution<T>> Distribution<T> for Print<D> {
    type Iter = PrintIter<D::Iter>;

    fn sample(&mut self) -> Self::Iter {
        PrintIter {
            label: self.label,
            inner: self.inner.sample(),
        }
    }
}

/// Wrap a distribution to print a label each time it's sampled.
pub fn print<D>(label: &'static str, inner: D) -> Print<D> {
    Print::new(label, inner)
}
