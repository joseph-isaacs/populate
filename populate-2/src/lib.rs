pub mod dist;
pub mod traits;

#[cfg(test)]
mod tests {
    use crate::dist::{constant, list, map, range};
    use crate::traits::{
        CollectInto, Distribution, HCons, HNil, HasWidth, IntoHList, IntoTuple, SampleIter, Sink,
        VecListSink,
    };
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

    fn make_tuple()
    -> impl Distribution<Item = (i32, i64)> + CollectInto<HCons<Vec<i32>, HCons<Vec<i64>, HNil>>>
    {
        (constant(1), range(0, 10))
    }

    fn make_mapped() -> impl Distribution<Item = i64> + CollectInto<Vec<i64>> {
        map(range(0, 1), |x| x * 2)
    }

    fn make_3tuple() -> impl Distribution<Item = (i32, i64, bool)>
    + CollectInto<HCons<Vec<i32>, HCons<Vec<i64>, HCons<Vec<bool>, HNil>>>> {
        (constant(1i32), range(0, 10), constant(true))
    }

    // ========================================================================
    // Generic collect using row_iter + Sink (row-by-row, unchanged)
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
    // Tests using row_iter (unchanged)
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

    // ========================================================================
    // Tracked distribution for verifying iteration order
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

    impl<D: HasWidth> HasWidth for Tracked<D> {
        type Width = D::Width;
    }

    impl<D: Distribution, S: Sink<D::Item>> CollectInto<S> for Tracked<D> {
        fn collect_into(&mut self, mut sink: S, rng: &mut impl rand::Rng, n: usize) -> S {
            let mut iter = self.row_iter();
            for _ in 0..n {
                sink.push(iter.next(rng));
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

    // ========================================================================
    // Col-by-col collection tests (via CollectInto with HList sinks)
    // ========================================================================

    #[test]
    fn test_col_by_col_basic() {
        let mut dist = make_tuple();
        let sink = (Vec::<i32>::new(), Vec::<i64>::new()).into_hlist();
        let (col1, col2) = dist.collect_into(sink, &mut rng(), 4).into_tuple();
        assert_eq!(col1, vec![1, 1, 1, 1]);
        assert_eq!(col2, vec![0, 10, 20, 30]);
    }

    #[test]
    fn test_col_by_col_order() {
        let log: Log = Log::default();
        let mut dist = (
            tracked("COL1", range(0, 1), &log),
            tracked("COL2", range(100, 1), &log),
        );
        let sink = (Vec::<i64>::new(), Vec::<i64>::new()).into_hlist();
        let (col1, col2) = dist.collect_into(sink, &mut rng(), 3).into_tuple();

        assert_eq!(col1, vec![0, 1, 2]);
        assert_eq!(col2, vec![100, 101, 102]);
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
        let sink = (Vec::<i32>::new(), Vec::<i64>::new(), Vec::<bool>::new()).into_hlist();
        let (col1, col2, col3) = dist.collect_into(sink, &mut rng(), 3).into_tuple();
        assert_eq!(col1, vec![1, 1, 1]);
        assert_eq!(col2, vec![0, 10, 20]);
        assert_eq!(col3, vec![true, true, true]);
    }

    #[test]
    fn test_3tuple_col_by_col_order() {
        let log: Log = Rc::new(RefCell::new(Vec::new()));
        let mut dist = (
            tracked("COL1", range(0, 1), &log),
            tracked("COL2", range(100, 1), &log),
            tracked("COL3", range(1000, 1), &log),
        );
        let sink = (Vec::<i64>::new(), Vec::<i64>::new(), Vec::<i64>::new()).into_hlist();
        let (col1, col2, col3) = dist.collect_into(sink, &mut rng(), 2).into_tuple();
        assert_eq!(col1, vec![0, 1]);
        assert_eq!(col2, vec![100, 101]);
        assert_eq!(col3, vec![1000, 1001]);
        assert_eq!(
            *log.borrow(),
            vec!["COL1", "COL1", "COL2", "COL2", "COL3", "COL3"]
        );
    }

    // ========================================================================
    // 4-tuple tests
    // ========================================================================

    #[test]
    fn test_4tuple_col_by_col() {
        let mut dist = (
            constant(1i32),
            range(0, 10),
            constant(true),
            constant(2.5f64),
        );
        let sink = (
            Vec::<i32>::new(),
            Vec::<i64>::new(),
            Vec::<bool>::new(),
            Vec::<f64>::new(),
        )
            .into_hlist();
        let (c1, c2, c3, c4) = dist.collect_into(sink, &mut rng(), 3).into_tuple();
        assert_eq!(c1, vec![1, 1, 1]);
        assert_eq!(c2, vec![0, 10, 20]);
        assert_eq!(c3, vec![true, true, true]);
        assert_eq!(c4, vec![2.5, 2.5, 2.5]);
    }

    #[test]
    fn test_4tuple_col_by_col_order() {
        let log: Log = Rc::new(RefCell::new(Vec::new()));
        let mut dist = (
            tracked("COL1", range(0, 1), &log),
            tracked("COL2", range(100, 1), &log),
            tracked("COL3", range(1000, 1), &log),
            tracked("COL4", range(10000, 1), &log),
        );
        let sink = (
            Vec::<i64>::new(),
            Vec::<i64>::new(),
            Vec::<i64>::new(),
            Vec::<i64>::new(),
        )
            .into_hlist();
        let (c1, c2, c3, c4) = dist.collect_into(sink, &mut rng(), 2).into_tuple();
        assert_eq!(c1, vec![0, 1]);
        assert_eq!(c2, vec![100, 101]);
        assert_eq!(c3, vec![1000, 1001]);
        assert_eq!(c4, vec![10000, 10001]);
        assert_eq!(
            *log.borrow(),
            vec![
                "COL1", "COL1", "COL2", "COL2", "COL3", "COL3", "COL4", "COL4"
            ]
        );
    }

    // ========================================================================
    // Map in tuple tests
    // ========================================================================

    #[test]
    fn test_map_in_tuple_values() {
        let log: Log = Rc::new(RefCell::new(Vec::new()));
        let mut dist = (
            tracked("COL1", range(0, 1), &log),
            map(tracked("MAP_SRC", range(100, 1), &log), |x| x * 2),
            tracked("COL3", range(1000, 1), &log),
        );
        let sink = (Vec::<i64>::new(), Vec::<i64>::new(), Vec::<i64>::new()).into_hlist();
        let (c1, c2, c3) = dist.collect_into(sink, &mut rng(), 3).into_tuple();

        assert_eq!(c1, vec![0, 1, 2]);
        assert_eq!(c2, vec![200, 202, 204]);
        assert_eq!(c3, vec![1000, 1001, 1002]);
    }

    #[test]
    fn test_map_in_tuple_order() {
        let log: Log = Rc::new(RefCell::new(Vec::new()));
        let mut dist = (
            tracked("COL1", range(0, 1), &log),
            map(tracked("MAP_SRC", range(100, 1), &log), |x| x * 2),
            tracked("COL3", range(1000, 1), &log),
        );
        let sink = (Vec::<i64>::new(), Vec::<i64>::new(), Vec::<i64>::new()).into_hlist();
        let (c1, c2, c3) = dist.collect_into(sink, &mut rng(), 3).into_tuple();

        assert_eq!(c1, vec![0, 1, 2]);
        assert_eq!(c2, vec![200, 202, 204]);
        assert_eq!(c3, vec![1000, 1001, 1002]);

        assert_eq!(
            *log.borrow(),
            vec![
                "COL1", "COL1", "COL1", // col-by-col
                "MAP_SRC", "MAP_SRC", "MAP_SRC", // map row-by-row
                "COL3", "COL3", "COL3" // col-by-col
            ]
        );
    }

    // ========================================================================
    // AUTO-FLATTENING: Map producing tuple → flat columns, no Unnest needed!
    // ========================================================================

    #[test]
    fn test_map_tuple_auto_flatten() {
        // Map produces (i64, i64) → Width = N2 → takes 2 sink slots
        // (d1, map(src, f), d3) with map returning (A, B) → 4 flat columns!
        let log: Log = Rc::new(RefCell::new(Vec::new()));
        let mut dist = (
            tracked("COL1", range(0, 1), &log),
            map(
                (
                    tracked("MAP_A", range(100, 1), &log),
                    tracked("MAP_B", range(1000, 1), &log),
                ),
                |(a, b)| (a + b, a * b),
            ),
            tracked("COL4", range(0, 5), &log),
        );

        // 4 flat columns — no nesting, no Unnest!
        let sink = (
            Vec::<i64>::new(),
            Vec::<i64>::new(),
            Vec::<i64>::new(),
            Vec::<i64>::new(),
        )
            .into_hlist();
        let (c1, c2, c3, c4) = dist.collect_into(sink, &mut rng(), 3).into_tuple();

        assert_eq!(c1, vec![0, 1, 2]); // COL1
        assert_eq!(c2, vec![1100, 1102, 1104]); // sum
        assert_eq!(c3, vec![100000, 101101, 102204]); // product
        assert_eq!(c4, vec![0, 5, 10]); // COL4
    }

    #[test]
    fn test_map_tuple_auto_flatten_order() {
        let log: Log = Rc::new(RefCell::new(Vec::new()));
        let mut dist = (
            tracked("COL1", range(0, 1), &log),
            map(
                (
                    tracked("MAP_A", range(100, 1), &log),
                    tracked("MAP_B", range(1000, 1), &log),
                ),
                |(a, b)| (a + b, a * b),
            ),
            tracked("COL4", range(0, 5), &log),
        );

        let sink = (
            Vec::<i64>::new(),
            Vec::<i64>::new(),
            Vec::<i64>::new(),
            Vec::<i64>::new(),
        )
            .into_hlist();
        let _ = dist.collect_into(sink, &mut rng(), 3);

        // COL1 col-by-col, then Map row-by-row (MAP_A, MAP_B per row), then COL4 col-by-col
        assert_eq!(
            *log.borrow(),
            vec![
                "COL1", "COL1", "COL1", "MAP_A", "MAP_B", "MAP_A", "MAP_B", "MAP_A", "MAP_B",
                "COL4", "COL4", "COL4",
            ]
        );
    }

    // ========================================================================
    // Independent tuple auto-flatten (previously needed Unnest)
    // ========================================================================

    #[test]
    fn test_independent_tuple_auto_flatten() {
        // Inner tuple (D2, D3) has Width = N2 → takes 2 sink slots
        // No Unnest needed — the width system handles it automatically!
        let log: Log = Rc::new(RefCell::new(Vec::new()));
        let mut dist = (
            tracked("COL1", range(0, 1), &log),
            (
                tracked("COL2", range(100, 1), &log),
                tracked("COL3", range(1000, 1), &log),
            ),
            tracked("COL4", range(0, 5), &log),
        );

        let sink = (
            Vec::<i64>::new(),
            Vec::<i64>::new(),
            Vec::<i64>::new(),
            Vec::<i64>::new(),
        )
            .into_hlist();
        let (c1, c2, c3, c4) = dist.collect_into(sink, &mut rng(), 3).into_tuple();

        assert_eq!(c1, vec![0, 1, 2]);
        assert_eq!(c2, vec![100, 101, 102]);
        assert_eq!(c3, vec![1000, 1001, 1002]);
        assert_eq!(c4, vec![0, 5, 10]);

        // All col-by-col!
        assert_eq!(
            *log.borrow(),
            vec![
                "COL1", "COL1", "COL1", "COL2", "COL2", "COL2", "COL3", "COL3", "COL3", "COL4",
                "COL4", "COL4",
            ]
        );
    }

    // ========================================================================
    // List tests
    // ========================================================================

    #[test]
    fn test_list_row_iter() {
        let mut dist = list(constant(3usize), constant(7i32));
        let mut iter = dist.row_iter();
        let mut rng = rng();
        let result = iter.next(&mut rng);
        assert_eq!(result, vec![7, 7, 7]);
    }

    #[test]
    fn test_list_collect_into_vec_list_sink() {
        let mut dist = list(constant(2usize), range(0, 1));
        let sink = VecListSink::new();
        let sink = dist.collect_into(sink, &mut rng(), 3);
        let (values, offsets) = sink.into_parts();
        assert_eq!(values, vec![0, 1, 0, 1, 0, 1]);
        assert_eq!(offsets, vec![0, 2, 4, 6]);
    }

    #[test]
    fn test_list_in_tuple() {
        // (scalar, list) — scalar takes 1 slot, list takes 1 slot
        let mut dist = (constant(42i32), list(constant(2usize), range(0, 1)));
        let sink = (Vec::<i32>::new(), VecListSink::<i64>::new()).into_hlist();
        let (scalars, list_sink) = dist.collect_into(sink, &mut rng(), 3).into_tuple();
        let (values, offsets) = list_sink.into_parts();

        assert_eq!(scalars, vec![42, 42, 42]);
        assert_eq!(values, vec![0, 1, 0, 1, 0, 1]);
        assert_eq!(offsets, vec![0, 2, 4, 6]);
    }

    // ========================================================================
    // Row-by-row iteration order test
    // ========================================================================

    #[test]
    fn test_row_order() {
        let log: Log = Rc::new(RefCell::new(Vec::new()));
        let mut dist = (
            tracked("COL1", range(0, 1), &log),
            tracked("COL2", range(100, 1), &log),
        );
        let mut sink: (Vec<i64>, Vec<i64>) = (Vec::new(), Vec::new());

        collect(&mut dist, &mut sink, 3, &mut rng());

        assert_eq!(sink.0, vec![0, 1, 2]);
        assert_eq!(sink.1, vec![100, 101, 102]);
        // Row-by-row: COL1, COL2, COL1, COL2, COL1, COL2
        assert_eq!(
            *log.borrow(),
            vec!["COL1", "COL2", "COL1", "COL2", "COL1", "COL2"]
        );
    }
}
