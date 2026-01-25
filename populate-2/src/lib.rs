pub mod dist;
pub mod traits;

#[cfg(test)]
mod tests {
    use crate::dist::{constant, map, range};
    use crate::traits::{Distribution, SampleIter, Sink};
    use std::cell::RefCell;
    use std::rc::Rc;

    // ========================================================================
    // Helper functions returning impl Distribution
    // ========================================================================

    fn make_constant() -> impl Distribution<Item = i32> {
        constant(42)
    }

    fn make_range() -> impl Distribution<Item = i64> {
        range(0, 1)
    }

    fn make_tuple() -> impl Distribution<Item = (i32, i64)> {
        (constant(1), range(0, 10))
    }

    fn make_mapped() -> impl Distribution<Item = i64> {
        map(range(0, 1), |x| x * 2)
    }

    fn make_nested_tuple() -> impl Distribution<Item = ((i32, i64), i64)> {
        ((constant(1i32), range(0, 1)), range(100, 10))
    }

    // ========================================================================
    // Generic collect using row_iter + Sink
    // ========================================================================

    fn collect<D: Distribution, S: Sink<D::Item>>(
        dist: &mut D,
        sink: &mut S,
        n: usize,
        rng: &mut impl rand::Rng,
    ) {
        let mut iter = dist.row_iter();
        for _ in 0..n {
            sink.push(iter.next(rng));
        }
    }

    // ========================================================================
    // Tests using row_iter
    // ========================================================================

    #[test]
    fn test_constant_row_iter() {
        let mut dist = make_constant();
        let mut iter = dist.row_iter();
        let mut rng = rand::rng();
        let results: Vec<_> = (0..5).map(|_| iter.next(&mut rng)).collect();
        assert_eq!(results, vec![42, 42, 42, 42, 42]);
    }

    #[test]
    fn test_range_row_iter() {
        let mut dist = make_range();
        let mut iter = dist.row_iter();
        let mut rng = rand::rng();
        let results: Vec<_> = (0..5).map(|_| iter.next(&mut rng)).collect();
        assert_eq!(results, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn test_tuple_row_iter() {
        let mut dist = make_tuple();
        let mut iter = dist.row_iter();
        let mut rng = rand::rng();
        let results: Vec<_> = (0..4).map(|_| iter.next(&mut rng)).collect();
        assert_eq!(results, vec![(1, 0), (1, 10), (1, 20), (1, 30)]);
    }

    #[test]
    fn test_map_row_iter() {
        let mut dist = make_mapped();
        let mut iter = dist.row_iter();
        let mut rng = rand::rng();
        let results: Vec<_> = (0..5).map(|_| iter.next(&mut rng)).collect();
        assert_eq!(results, vec![0, 2, 4, 6, 8]);
    }

    #[test]
    fn test_nested_tuple_row_iter() {
        let mut dist = make_nested_tuple();
        let mut iter = dist.row_iter();
        let mut rng = rand::rng();
        let results: Vec<_> = (0..3).map(|_| iter.next(&mut rng)).collect();
        assert_eq!(results, vec![((1, 0), 100), ((1, 1), 110), ((1, 2), 120)]);
    }

    // ========================================================================
    // Tests using collect (row-by-row into Sink)
    // ========================================================================

    #[test]
    fn test_constant_collect() {
        let mut dist = make_constant();
        let mut sink = Vec::new();
        collect(&mut dist, &mut sink, 5, &mut rand::rng());
        assert_eq!(sink, vec![42, 42, 42, 42, 42]);
    }

    #[test]
    fn test_range_collect() {
        let mut dist = make_range();
        let mut sink = Vec::new();
        collect(&mut dist, &mut sink, 5, &mut rand::rng());
        assert_eq!(sink, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn test_tuple_collect() {
        let mut dist = make_tuple();
        let mut sink = (Vec::new(), Vec::new());
        collect(&mut dist, &mut sink, 4, &mut rand::rng());
        assert_eq!(sink.0, vec![1, 1, 1, 1]);
        assert_eq!(sink.1, vec![0, 10, 20, 30]);
    }

    #[test]
    fn test_map_collect() {
        let mut dist = make_mapped();
        let mut sink = Vec::new();
        collect(&mut dist, &mut sink, 5, &mut rand::rng());
        assert_eq!(sink, vec![0, 2, 4, 6, 8]);
    }

    #[test]
    fn test_nested_tuple_collect() {
        let mut dist = make_nested_tuple();
        let mut sink = ((Vec::new(), Vec::new()), Vec::new());
        collect(&mut dist, &mut sink, 3, &mut rand::rng());
        assert_eq!(sink.0 .0, vec![1, 1, 1]);
        assert_eq!(sink.0 .1, vec![0, 1, 2]);
        assert_eq!(sink.1, vec![100, 110, 120]);
    }

    // ========================================================================
    // Verify iteration order (row-by-row)
    // ========================================================================

    struct Tracked<D> {
        name: &'static str,
        inner: D,
        log: Rc<RefCell<Vec<&'static str>>>,
    }

    struct TrackedIter<I> {
        name: &'static str,
        inner: I,
        log: Rc<RefCell<Vec<&'static str>>>,
    }

    impl<I: SampleIter> SampleIter for TrackedIter<I> {
        type Item = I::Item;
        fn next(&mut self, rng: &mut impl rand::Rng) -> Self::Item {
            self.log.borrow_mut().push(self.name);
            self.inner.next(rng)
        }
    }

    impl<D: Distribution> Distribution for Tracked<D> {
        type Item = D::Item;
        type RowIter = TrackedIter<D::RowIter>;

        fn row_iter(&mut self) -> Self::RowIter {
            TrackedIter {
                name: self.name,
                inner: self.inner.row_iter(),
                log: Rc::clone(&self.log),
            }
        }
    }

    fn tracked<D: Distribution>(
        name: &'static str,
        inner: D,
        log: &Rc<RefCell<Vec<&'static str>>>,
    ) -> Tracked<D> {
        Tracked {
            name,
            inner,
            log: Rc::clone(log),
        }
    }

    fn make_tracked_tuple(
        log: &Rc<RefCell<Vec<&'static str>>>,
    ) -> impl Distribution<Item = (i64, i64)> {
        (
            tracked("COL1", range(0, 1), log),
            tracked("COL2", range(100, 1), log),
        )
    }

    #[test]
    fn test_row_order() {
        let log: Rc<RefCell<Vec<&'static str>>> = Rc::new(RefCell::new(Vec::new()));
        let mut dist = make_tracked_tuple(&log);
        let mut sink: (Vec<i64>, Vec<i64>) = (Vec::new(), Vec::new());

        collect(&mut dist, &mut sink, 3, &mut rand::rng());

        // Row-by-row: COL1, COL2, COL1, COL2, COL1, COL2
        assert_eq!(
            *log.borrow(),
            vec!["COL1", "COL2", "COL1", "COL2", "COL1", "COL2"]
        );
    }
}
