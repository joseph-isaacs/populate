use populate_core::{Distribution, SampleIter};
use std::cell::RefCell;
use std::rc::Rc;

/// Shared log for recording sample order in tests.
pub type SharedLog = Rc<RefCell<Vec<&'static str>>>;

/// Create a new shared log.
pub fn shared_log() -> SharedLog {
    Rc::new(RefCell::new(Vec::new()))
}

/// A distribution wrapper that logs each sample to a shared log.
pub struct Log<D> {
    label: &'static str,
    log: SharedLog,
    inner: D,
}

impl<D> Log<D> {
    pub fn new(label: &'static str, log: SharedLog, inner: D) -> Self {
        Self { label, log, inner }
    }
}

#[derive(Clone)]
pub struct LogIter<I> {
    label: &'static str,
    log: SharedLog,
    inner: I,
}

impl<I: SampleIter> SampleIter for LogIter<I> {
    type Item = I::Item;

    fn next(&mut self, rng: &mut impl rand::Rng) -> Self::Item {
        self.log.borrow_mut().push(self.label);
        self.inner.next(rng)
    }
}

impl<T, D: Distribution<T>> Distribution<T> for Log<D> {
    type Iter = LogIter<D::Iter>;

    fn sample(&mut self) -> Self::Iter {
        LogIter {
            label: self.label,
            log: Rc::clone(&self.log),
            inner: self.inner.sample(),
        }
    }
}

/// Wrap a distribution to log each sample to a shared log.
pub fn log<D>(label: &'static str, log: SharedLog, inner: D) -> Log<D> {
    Log::new(label, log, inner)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MapExt, constant, inc};
    use populate_core::{Sink, TupleIter3, collect_struct3_into};
    use rand::SeedableRng;
    use rand::rngs::StdRng;

    /// Simple Vec sink for testing.
    struct VecSink<T>(Vec<T>);

    impl<T> VecSink<T> {
        fn new() -> Self {
            Self(Vec::new())
        }
    }

    impl<T> Sink<T> for VecSink<T> {
        type Output = Vec<T>;

        fn push(&mut self, value: T) {
            self.0.push(value);
        }

        fn finish(self) -> Self::Output {
            self.0
        }
    }

    #[test]
    fn test_column_by_column_execution_order() {
        // Create a shared log to record execution order
        let log_store = shared_log();

        // Create 3 distributions:
        // - col_a: independent (incrementing from 1)
        // - col_b: dependent on col_a (derived via map: a * 10)
        // - col_c: independent (constant 99)
        //
        // For column-by-column, we need them as separate distributions.
        // The dependency is conceptual - col_b's value depends on position,
        // just like col_a.

        // Column A: incrementing from 1
        let col_a = log("A", Rc::clone(&log_store), inc(1i64));

        // Column B: incrementing from 100 (dependent in the sense that
        // B[i] conceptually relates to row i, same as A[i])
        let col_b = log(
            "B",
            Rc::clone(&log_store),
            inc(1i64).map(|x| x * 10), // B depends on position
        );

        // Column C: independent constant
        let col_c = log("C", Rc::clone(&log_store), constant(99i64));

        // Create a 3-tuple distribution
        let mut dist = (col_a, col_b, col_c);

        // Sample and collect column-by-column
        let iter: TupleIter3<_, _, _> = dist.sample();
        let mut rng = StdRng::seed_from_u64(42);
        let n = 4;

        let (vec_a, vec_b, vec_c) = collect_struct3_into(
            iter,
            &mut rng,
            n,
            VecSink::new(),
            VecSink::new(),
            VecSink::new(),
        );

        // Verify data is correct
        assert_eq!(vec_a, vec![1, 2, 3, 4]);
        assert_eq!(vec_b, vec![10, 20, 30, 40]);
        assert_eq!(vec_c, vec![99, 99, 99, 99]);

        // Verify execution order: all A's, then all B's, then all C's
        let log_contents = log_store.borrow();
        assert_eq!(log_contents.len(), 12); // 4 samples * 3 columns

        // First 4 should be A (column 0)
        assert_eq!(&log_contents[0..4], &["A", "A", "A", "A"]);
        // Next 4 should be B (column 1)
        assert_eq!(&log_contents[4..8], &["B", "B", "B", "B"]);
        // Last 4 should be C (column 2)
        assert_eq!(&log_contents[8..12], &["C", "C", "C", "C"]);
    }

    #[test]
    fn test_row_by_row_execution_order() {
        // For comparison, show what row-by-row looks like
        let log_store = shared_log();

        let col_a = log("A", Rc::clone(&log_store), inc(1i64));
        let col_b = log("B", Rc::clone(&log_store), inc(1i64).map(|x| x * 10));
        let col_c = log("C", Rc::clone(&log_store), constant(99i64));

        let mut dist = (col_a, col_b, col_c);
        let mut iter = dist.sample();
        let mut rng = StdRng::seed_from_u64(42);

        // Row-by-row collection
        let mut results = Vec::new();
        for _ in 0..4 {
            results.push(iter.next(&mut rng));
        }

        // Verify data
        assert_eq!(results[0], (1, 10, 99));
        assert_eq!(results[1], (2, 20, 99));
        assert_eq!(results[2], (3, 30, 99));
        assert_eq!(results[3], (4, 40, 99));

        // Verify execution order: interleaved A, B, C for each row
        let log_contents = log_store.borrow();
        assert_eq!(log_contents.len(), 12);

        // Pattern should be [A, B, C, A, B, C, A, B, C, A, B, C]
        assert_eq!(
            &log_contents[..],
            &["A", "B", "C", "A", "B", "C", "A", "B", "C", "A", "B", "C"]
        );
    }
}
