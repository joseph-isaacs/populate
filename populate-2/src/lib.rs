pub mod dist;
pub mod traits;

#[cfg(test)]
mod tests {
    use crate::dist::{NotUnnest, constant, map, range, unnest};
    use crate::traits::{ColumnDistribution, Distribution, SampleIter, Sink, Splittable};
    use rand::rng;
    use std::cell::RefCell;
    use std::rc::Rc;

    type Log = Rc<RefCell<Vec<&'static str>>>;

    // ========================================================================
    // Helper functions returning impl Distribution
    // ========================================================================

    fn make_constant() -> impl Distribution<Item = i32> {
        constant(42)
    }

    fn make_range() -> impl Distribution<Item = i64> {
        range(0, 1)
    }

    fn make_tuple() -> impl ColumnDistribution<Item = (i32, i64)> {
        (constant(1), range(0, 10))
    }

    fn make_mapped() -> impl Distribution<Item = i64> {
        map(range(0, 1), |x| x * 2)
    }

    fn make_nested_tuple() -> impl ColumnDistribution<Item = ((i32, i64), i64)> {
        ((constant(1i32), range(0, 1)), range(100, 10))
    }

    fn make_3tuple() -> impl ColumnDistribution<Item = (i32, i64, bool)> {
        (constant(1i32), range(0, 10), constant(true))
    }

    fn make_4tuple() -> impl ColumnDistribution<Item = (i32, i64, bool, f64)> {
        (
            constant(1i32),
            range(0, 10),
            constant(true),
            constant(2.5f64),
        )
    }

    fn make_tracked_3tuple(log: &Log) -> impl ColumnDistribution<Item = (i64, i64, i64)> {
        (
            tracked("COL1", range(0, 1), log),
            tracked("COL2", range(100, 1), log),
            tracked("COL3", range(1000, 1), log),
        )
    }

    fn make_tracked_4tuple(log: &Log) -> impl ColumnDistribution<Item = (i64, i64, i64, i64)> {
        (
            tracked("COL1", range(0, 1), log),
            tracked("COL2", range(100, 1), log),
            tracked("COL3", range(1000, 1), log),
            tracked("COL4", range(10000, 1), log),
        )
    }

    // ========================================================================
    // Generic collect using row_iter + Sink
    // ========================================================================

    fn collect<D: Distribution, S: Sink<D::Item>>(
        dist: &mut D,
        sink: &mut S,
        n: usize,
        rng: &mut impl rand::Rng,
    ) where
        D::Item: Splittable,
    {
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
        let mut rng = rng();
        let results: Vec<_> = (0..5).map(|_| iter.next(&mut rng)).collect();
        assert_eq!(results, vec![42, 42, 42, 42, 42]);
    }

    #[test]
    fn test_range_row_iter() {
        let mut dist = make_range();
        let mut iter = dist.row_iter();
        let mut rng = rng();
        let results: Vec<_> = (0..5).map(|_| iter.next(&mut rng)).collect();
        assert_eq!(results, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn test_tuple_row_iter() {
        let mut dist = make_tuple();
        let mut iter = dist.row_iter();
        let mut rng = rng();
        let results: Vec<_> = (0..4).map(|_| iter.next(&mut rng)).collect();
        assert_eq!(results, vec![(1, 0), (1, 10), (1, 20), (1, 30)]);
    }

    #[test]
    fn test_map_row_iter() {
        let mut dist = make_mapped();
        let mut iter = dist.row_iter();
        let mut rng = rng();
        let results: Vec<_> = (0..5).map(|_| iter.next(&mut rng)).collect();
        assert_eq!(results, vec![0, 2, 4, 6, 8]);
    }

    #[test]
    fn test_nested_tuple_row_iter() {
        let mut dist = make_nested_tuple();
        let mut iter = dist.row_iter();
        let mut rng = rng();
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
        collect(&mut dist, &mut sink, 5, &mut rng());
        assert_eq!(sink, vec![42, 42, 42, 42, 42]);
    }

    #[test]
    fn test_range_collect() {
        let mut dist = make_range();
        let mut sink = Vec::new();
        collect(&mut dist, &mut sink, 5, &mut rng());
        assert_eq!(sink, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn test_tuple_collect() {
        let mut dist = make_tuple();
        let mut sink = (Vec::new(), Vec::new());
        collect(&mut dist, &mut sink, 4, &mut rng());
        assert_eq!(sink.0, vec![1, 1, 1, 1]);
        assert_eq!(sink.1, vec![0, 10, 20, 30]);
    }

    #[test]
    fn test_map_collect() {
        let mut dist = make_mapped();
        let mut sink = Vec::new();
        collect(&mut dist, &mut sink, 5, &mut rng());
        assert_eq!(sink, vec![0, 2, 4, 6, 8]);
    }

    #[test]
    fn test_nested_tuple_collect() {
        let mut dist = make_nested_tuple();
        let mut sink = ((Vec::new(), Vec::new()), Vec::new());
        collect(&mut dist, &mut sink, 3, &mut rng());
        assert_eq!(sink.0.0, vec![1, 1, 1]);
        assert_eq!(sink.0.1, vec![0, 1, 2]);
        assert_eq!(sink.1, vec![100, 110, 120]);
    }

    // ========================================================================
    // Verify iteration order (row-by-row)
    // ========================================================================

    struct Tracked<D> {
        name: &'static str,
        inner: D,
        log: Log,
    }

    struct TrackedIter<I> {
        name: &'static str,
        inner: I,
        log: Log,
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

    impl<D: NotUnnest> NotUnnest for Tracked<D> {}

    impl<D: ColumnDistribution> ColumnDistribution for Tracked<D>
    where
        D::Item: Splittable,
    {
        fn collect_into<S: Sink<Self::Item>>(
            &mut self,
            rng: &mut impl rand::Rng,
            mut sink: S,
            n: usize,
        ) -> S
        where
            Self: Sized,
        {
            let mut sample_iter = self.row_iter();
            for _ in 0..n {
                sink.push(sample_iter.next(rng));
            }
            sink
        }
    }

    fn tracked<D: Distribution>(name: &'static str, inner: D, log: &Log) -> Tracked<D> {
        Tracked {
            name,
            inner,
            log: Rc::clone(log),
        }
    }

    fn make_tracked_tuple(log: &Log) -> impl ColumnDistribution<Item = (i64, i64)> {
        (
            tracked("COL1", range(0, 1), log),
            tracked("COL2", range(100, 1), log),
        )
    }

    #[test]
    fn test_row_order() {
        let log: Log = Rc::new(RefCell::new(Vec::new()));
        let mut dist = make_tracked_tuple(&log);
        let mut sink: (Vec<i64>, Vec<i64>) = (Vec::new(), Vec::new());

        collect(&mut dist, &mut sink, 3, &mut rng());

        // Assert sink values
        assert_eq!(sink.0, vec![0, 1, 2]);
        assert_eq!(sink.1, vec![100, 101, 102]);
        // Row-by-row: COL1, COL2, COL1, COL2, COL1, COL2
        assert_eq!(
            *log.borrow(),
            vec!["COL1", "COL2", "COL1", "COL2", "COL1", "COL2"]
        );
    }

    // ========================================================================
    // Col-by-col collection tests
    // ========================================================================

    #[test]
    fn test_col_by_col_basic() {
        let mut dist = make_tuple();
        let sink: (Vec<i32>, Vec<i64>) = (Vec::new(), Vec::new());

        let sink = dist.collect_into(&mut rng(), sink, 4);

        assert_eq!(sink.0, vec![1, 1, 1, 1]);
        assert_eq!(sink.1, vec![0, 10, 20, 30]);
    }

    #[test]
    fn test_col_by_col_order() {
        let log: Log = Log::default();
        let mut dist = make_tracked_tuple(&log);

        let sink = dist.collect_into(&mut rng(), (Vec::new(), Vec::new()), 3);

        // Assert sink values
        assert_eq!(sink.0, vec![0, 1, 2]);
        assert_eq!(sink.1, vec![100, 101, 102]);
        // Col-by-col: COL1, COL1, COL1, COL2, COL2, COL2
        assert_eq!(
            *log.borrow(),
            vec!["COL1", "COL1", "COL1", "COL2", "COL2", "COL2"]
        );
    }

    // ========================================================================
    // 3-tuple tests
    // ========================================================================

    #[test]
    fn test_3tuple_row_iter() {
        let mut dist = make_3tuple();
        let mut iter = dist.row_iter();
        let mut rng = rng();
        let results: Vec<_> = (0..3).map(|_| iter.next(&mut rng)).collect();
        assert_eq!(results, vec![(1, 0, true), (1, 10, true), (1, 20, true)]);
    }

    #[test]
    fn test_3tuple_col_by_col() {
        let mut dist = make_3tuple();
        let sink: (Vec<i32>, Vec<i64>, Vec<bool>) = (Vec::new(), Vec::new(), Vec::new());
        let sink = dist.collect_into(&mut rng(), sink, 3);
        assert_eq!(sink.0, vec![1, 1, 1]);
        assert_eq!(sink.1, vec![0, 10, 20]);
        assert_eq!(sink.2, vec![true, true, true]);
    }

    #[test]
    fn test_3tuple_col_by_col_order() {
        let log: Log = Rc::new(RefCell::new(Vec::new()));
        let mut dist = make_tracked_3tuple(&log);
        let sink = dist.collect_into(&mut rng(), (Vec::new(), Vec::new(), Vec::new()), 2);
        // Assert sink values
        assert_eq!(sink.0, vec![0, 1]);
        assert_eq!(sink.1, vec![100, 101]);
        assert_eq!(sink.2, vec![1000, 1001]);
        // Col-by-col: COL1, COL1, COL2, COL2, COL3, COL3
        assert_eq!(
            *log.borrow(),
            vec!["COL1", "COL1", "COL2", "COL2", "COL3", "COL3"]
        );
    }

    // ========================================================================
    // 4-tuple tests
    // ========================================================================

    #[test]
    fn test_4tuple_row_iter() {
        let mut dist = make_4tuple();
        let mut iter = dist.row_iter();
        let mut rng = rng();
        let results: Vec<_> = (0..2).map(|_| iter.next(&mut rng)).collect();
        assert_eq!(results, vec![(1, 0, true, 2.5), (1, 10, true, 2.5)]);
    }

    #[test]
    fn test_4tuple_col_by_col() {
        let mut dist = make_4tuple();
        let sink: (Vec<i32>, Vec<i64>, Vec<bool>, Vec<f64>) =
            (Vec::new(), Vec::new(), Vec::new(), Vec::new());
        let sink = dist.collect_into(&mut rng(), sink, 3);
        assert_eq!(sink.0, vec![1, 1, 1]);
        assert_eq!(sink.1, vec![0, 10, 20]);
        assert_eq!(sink.2, vec![true, true, true]);
        assert_eq!(sink.3, vec![2.5, 2.5, 2.5]);
    }

    #[test]
    fn test_4tuple_col_by_col_order() {
        let log: Log = Rc::new(RefCell::new(Vec::new()));
        let mut dist = make_tracked_4tuple(&log);
        let sink = dist.collect_into(
            &mut rng(),
            (Vec::new(), Vec::new(), Vec::new(), Vec::new()),
            2,
        );
        // Assert sink values
        assert_eq!(sink.0, vec![0, 1]);
        assert_eq!(sink.1, vec![100, 101]);
        assert_eq!(sink.2, vec![1000, 1001]);
        assert_eq!(sink.3, vec![10000, 10001]);
        // Col-by-col: COL1, COL1, COL2, COL2, COL3, COL3, COL4, COL4
        assert_eq!(
            *log.borrow(),
            vec![
                "COL1", "COL1", "COL2", "COL2", "COL3", "COL3", "COL4", "COL4"
            ]
        );
    }

    // ========================================================================
    // Nested tuple tests
    // ========================================================================

    fn make_nested_tracked(log: &Log) -> impl ColumnDistribution<Item = ((i64, i64), i64)> {
        (
            (
                tracked("COL1", range(0, 1), log),
                tracked("COL2", range(100, 1), log),
            ),
            tracked("COL3", range(1000, 1), log),
        )
    }

    #[test]
    fn test_nested_col_by_col() {
        let mut dist = make_nested_tuple();
        let ((inner1, inner2), outer) =
            dist.collect_into(&mut rng(), ((Vec::new(), Vec::new()), Vec::new()), 3);
        assert_eq!(inner1, vec![1, 1, 1]);
        assert_eq!(inner2, vec![0, 1, 2]);
        assert_eq!(outer, vec![100, 110, 120]);
    }

    #[test]
    fn test_nested_col_by_col_order() {
        let log: Log = Rc::new(RefCell::new(Vec::new()));
        let mut dist = make_nested_tracked(&log);
        let sink = dist.collect_into(&mut rng(), ((Vec::new(), Vec::new()), Vec::new()), 2);

        // Assert sink values
        assert_eq!(sink.0.0, vec![0, 1]);
        assert_eq!(sink.0.1, vec![100, 101]);
        assert_eq!(sink.1, vec![1000, 1001]);

        // Col-by-col order: COL1 (all), COL2 (all), COL3 (all)
        assert_eq!(
            *log.borrow(),
            vec!["COL1", "COL1", "COL2", "COL2", "COL3", "COL3"]
        );
    }

    // ========================================================================
    // Map with tuple tests (non-independent columns)
    // ========================================================================

    fn make_map_with_independent(log: &Log) -> impl ColumnDistribution<Item = (i64, i64, i64)> {
        // COL1 is independent, COL2 is mapped (depends on inner), COL3 is independent
        (
            tracked("COL1", range(0, 1), log),
            map(tracked("MAP_SRC", range(100, 1), log), |x| x * 2),
            tracked("COL3", range(1000, 1), log),
        )
    }

    #[test]
    fn test_map_in_tuple_values() {
        let log: Log = Rc::new(RefCell::new(Vec::new()));
        let mut dist = make_map_with_independent(&log);
        let sink = dist.collect_into(&mut rng(), (Vec::new(), Vec::new(), Vec::new()), 3);

        // Assert sink values
        assert_eq!(sink.0, vec![0, 1, 2]); // COL1: 0, 1, 2
        assert_eq!(sink.1, vec![200, 202, 204]); // MAP_SRC: 100, 101, 102 -> *2 = 200, 202, 204
        assert_eq!(sink.2, vec![1000, 1001, 1002]); // COL3: 1000, 1001, 1002
    }

    #[test]
    fn test_map_in_tuple_order() {
        let log: Log = Rc::new(RefCell::new(Vec::new()));
        let mut dist = make_map_with_independent(&log);
        let sink = dist.collect_into(&mut rng(), (Vec::new(), Vec::new(), Vec::new()), 3);

        // Assert sink values
        assert_eq!(sink.0, vec![0, 1, 2]);
        assert_eq!(sink.1, vec![200, 202, 204]);
        assert_eq!(sink.2, vec![1000, 1001, 1002]);

        // Order: COL1 (col-by-col), then MAP_SRC (row-by-row inside map), then COL3 (col-by-col)
        assert_eq!(
            *log.borrow(),
            vec![
                "COL1", "COL1", "COL1", // first column, all rows
                "MAP_SRC", "MAP_SRC", "MAP_SRC", // map iterates row-by-row
                "COL3", "COL3", "COL3" // third column, all rows
            ]
        );
    }

    // Map that takes a tuple and produces a tuple (coupled columns)
    fn make_map_tuple_to_tuple(
        log: &Log,
    ) -> impl ColumnDistribution<Item = (i64, (i64, i64), i64)> {
        // COL1 is independent, the map produces two coupled columns from two inputs
        (
            tracked("COL1", range(0, 1), log),
            map(
                (
                    tracked("MAP_A", range(100, 1), log),
                    tracked("MAP_B", range(1000, 1), log),
                ),
                |(a, b)| (a + b, a * b),
            ),
            tracked("COL2", range(0, 5), log),
        )
    }

    #[test]
    fn test_map_tuple_to_tuple_values() {
        let log: Log = Rc::new(RefCell::new(Vec::new()));
        let mut dist = make_map_tuple_to_tuple(&log);
        let sink = dist.collect_into(
            &mut rng(),
            (Vec::new(), (Vec::new(), Vec::new()), Vec::new()),
            3,
        );

        // Assert sink values
        assert_eq!(sink.0, vec![0, 1, 2]); // COL1
        // MAP_A: 100, 101, 102; MAP_B: 1000, 1001, 1002
        // sum: 1100, 1102, 1104; product: 100000, 101101, 102204
        assert_eq!(sink.1.0, vec![1100, 1102, 1104]);
        assert_eq!(sink.1.1, vec![100000, 101101, 102204]);
    }

    #[test]
    fn test_map_tuple_to_tuple_order() {
        let log: Log = Rc::new(RefCell::new(Vec::new()));
        let mut dist = make_map_tuple_to_tuple(&log);
        let (outer1, (inner1, inner2), outer2) = dist.collect_into(
            &mut rng(),
            (Vec::new(), (Vec::new(), Vec::new()), Vec::new()),
            3,
        );

        // Assert sink values
        assert_eq!(outer1, vec![0, 1, 2]);
        assert_eq!(inner1, vec![1100, 1102, 1104]);
        assert_eq!(inner2, vec![100000, 101101, 102204]);
        assert_eq!(outer2, vec![0, 5, 10]);

        // Order: COL1 (col-by-col), then the map which iterates row-by-row
        // Inside the map, each row samples MAP_A then MAP_B
        assert_eq!(
            *log.borrow(),
            vec![
                "COL1", "COL1", "COL1", // first column, all rows
                // Map iterates row-by-row, each row does MAP_A then MAP_B
                "MAP_A", "MAP_B", // row 0
                "MAP_A", "MAP_B", // row 1
                "MAP_A", "MAP_B", // row 2
                "COL2", "COL2", "COL2"
            ]
        );
    }

    // ========================================================================
    // Unnest tests
    // ========================================================================

    fn make_unnested(log: &Log) -> impl ColumnDistribution<Item = (i64, i64, i64, i64)> {
        (
            tracked("COL1", range(0, 1), log),
            unnest(map(
                (
                    tracked("MAP_A", range(100, 1), log),
                    tracked("MAP_B", range(1000, 1), log),
                ),
                |(a, b)| (a + b, a * b),
            )),
            tracked("COL4", range(0, 5), log),
        )
    }

    fn make_unnested_ind(log: &Log) -> impl ColumnDistribution<Item = (i64, i64, i64, i64)> {
        (
            tracked("COL1", range(0, 1), log),
            unnest((
                tracked("MAP_A", range(100, 1), log),
                tracked("MAP_B", range(1000, 1), log),
            )),
            tracked("COL4", range(0, 5), log),
        )
    }

    #[test]
    fn test_unnest_values() {
        let log: Log = Rc::new(RefCell::new(Vec::new()));
        let mut dist = make_unnested(&log);
        let sink = dist.collect_into(
            &mut rng(),
            (Vec::new(), Vec::new(), Vec::new(), Vec::new()),
            3,
        );

        // Assert unnested sink values
        assert_eq!(sink.0, vec![0, 1, 2]); // COL1
        assert_eq!(sink.1, vec![1100, 1102, 1104]); // sum
        assert_eq!(sink.2, vec![100000, 101101, 102204]); // product
        assert_eq!(sink.3, vec![0, 5, 10]); // COL4
    }

    #[test]
    fn test_unnest_order() {
        let log: Log = Rc::new(RefCell::new(Vec::new()));
        let mut dist = make_unnested(&log);
        let sink = dist.collect_into(
            &mut rng(),
            (Vec::new(), Vec::new(), Vec::new(), Vec::new()),
            3,
        );

        // Assert unnested sink values
        assert_eq!(sink.0, vec![0, 1, 2]);
        assert_eq!(sink.1, vec![1100, 1102, 1104]);
        assert_eq!(sink.2, vec![100000, 101101, 102204]);
        assert_eq!(sink.3, vec![0, 5, 10]);

        // COL1 fills col-by-col, then MAP fills row-by-row, then COL4 fills col-by-col
        assert_eq!(
            *log.borrow(),
            vec![
                "COL1", "COL1", "COL1", // col 1 (col-by-col)
                "MAP_A", "MAP_B", // row 0 of unnested map
                "MAP_A", "MAP_B", // row 1 of unnested map
                "MAP_A", "MAP_B", // row 2 of unnested map
                "COL4", "COL4", "COL4", // col 4 (col-by-col)
            ]
        );
    }

    #[test]
    fn test_unnest_order_ind() {
        let log: Log = Rc::new(RefCell::new(Vec::new()));
        let mut dist = make_unnested_ind(&log);
        let sink = dist.collect_into(
            &mut rng(),
            (Vec::new(), Vec::new(), Vec::new(), Vec::new()),
            3,
        );

        // Assert unnested sink values
        assert_eq!(sink.0, vec![0, 1, 2]);
        assert_eq!(sink.1, vec![100, 101, 102]);
        assert_eq!(sink.2, vec![1000, 1001, 1002]);
        assert_eq!(sink.3, vec![0, 5, 10]);

        // All columns fill col-by-col since inner tuple has independent distributions
        assert_eq!(
            *log.borrow(),
            vec![
                "COL1", "COL1", "COL1",   // col 1 (col-by-col)
                "MAP_A", "MAP_A", "MAP_A", // col 2 (col-by-col)
                "MAP_B", "MAP_B", "MAP_B", // col 3 (col-by-col)
                "COL4", "COL4", "COL4",   // col 4 (col-by-col)
            ]
        );
    }
}
