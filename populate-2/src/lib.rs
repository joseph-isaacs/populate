pub mod dist;
pub mod traits;

#[cfg(test)]
mod tests {
    use crate::dist::{constant, flatten, list, map, range};
    use crate::traits::{
        Backend, DefaultSink, Distribution, DistributionExt, HAppend, HCons, HNil, IntoTuple,
        SampleIter, Sink, VecBackend, VecListSink,
    };
    use rand::rng;
    use std::cell::RefCell;
    use std::rc::Rc;

    type Log = Rc<RefCell<Vec<&'static str>>>;

    // ========================================================================
    // Helper functions returning impl Distribution — NO extra bounds needed!
    // ========================================================================

    fn make_constant<B: Backend>() -> impl Distribution<B, Item = i32>
    where
        <i32 as DefaultSink<B>>::Sink: Sink<i32>,
    {
        constant(42)
    }

    fn make_range<B: Backend>() -> impl Distribution<B, Item = i64>
    where
        <i64 as DefaultSink<B>>::Sink: Sink<i64>,
    {
        range(0, 1)
    }

    fn make_tuple<B: Backend>() -> impl Distribution<B, Item = (i32, i64)>
    where
        <i32 as DefaultSink<B>>::Sink: Sink<i32>,
        <i64 as DefaultSink<B>>::Sink: Sink<i64>,
        <i32 as DefaultSink<B>>::Sink: HAppend<<i64 as DefaultSink<B>>::Sink>,
    {
        (constant(1), range(0, 10))
    }

    fn make_mapped() -> impl Distribution<VecBackend, Item = i64> {
        map(range(0, 1), |x| x * 2i64)
    }

    fn make_3tuple() -> impl Distribution<VecBackend, Item = (i32, i64, bool)> {
        (constant(1i32), range(0, 10), constant(true))
    }

    // ========================================================================
    // Generic collect using row_iter + Sink (row-by-row)
    // ========================================================================

    fn collect<B: Backend, D: Distribution<B>, S: Sink<D::Item>>(
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
        let mut dist = make_constant::<VecBackend>();
        let mut iter = dist.row_iter();
        let mut rng = rng();
        let results: Vec<_> = (0..5).map(|_| iter.next(&mut rng)).collect();
        assert_eq!(results, vec![42, 42, 42, 42, 42]);
    }

    #[test]
    fn test_range_row_iter() {
        let mut dist = make_range::<VecBackend>();
        let mut iter = dist.row_iter();
        let mut rng = rng();
        let results: Vec<_> = (0..5).map(|_| iter.next(&mut rng)).collect();
        assert_eq!(results, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn test_tuple_row_iter() {
        let mut dist = make_tuple::<VecBackend>();
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
        let mut dist = make_constant::<VecBackend>();
        let mut sink = Vec::new();
        collect(&mut dist, &mut sink, 5, &mut rng());
        assert_eq!(sink, vec![42, 42, 42, 42, 42]);
    }

    #[test]
    fn test_range_collect() {
        let mut dist = make_range::<VecBackend>();
        let mut sink = Vec::new();
        collect(&mut dist, &mut sink, 5, &mut rng());
        assert_eq!(sink, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn test_tuple_collect() {
        let mut dist = make_tuple::<VecBackend>();
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

    impl<B: Backend, D: Distribution<B>> Distribution<B> for Tracked<D>
    where
        <D::Item as DefaultSink<B>>::Sink: Sink<D::Item>,
    {
        type Item = D::Item;
        type RowIter = TrackedIter<D::RowIter>;

        fn row_iter(&mut self) -> Self::RowIter {
            TrackedIter {
                name: self.name,
                inner: self.inner.row_iter(),
                log: Rc::clone(&self.log),
            }
        }

        fn collect_into(
            &mut self,
            mut sink: <D::Item as DefaultSink<B>>::Sink,
            rng: &mut impl rand::Rng,
            n: usize,
        ) -> <D::Item as DefaultSink<B>>::Sink {
            let mut iter = self.row_iter();
            for _ in 0..n {
                sink.push(iter.next(rng));
            }
            sink
        }
    }

    fn tracked<D>(name: &'static str, inner: D, log: &Log) -> Tracked<D> {
        Tracked {
            name,
            inner,
            log: Rc::clone(log),
        }
    }

    // ========================================================================
    // Tracked helpers returning impl Distribution<VecBackend>
    // ========================================================================

    fn make_tracked_2tuple(log: &Log) -> impl Distribution<VecBackend, Item = (i64, i64)> {
        (
            tracked("COL1", range(0i64, 1), log),
            tracked("COL2", range(100i64, 1), log),
        )
    }

    fn make_tracked_3tuple(log: &Log) -> impl Distribution<VecBackend, Item = (i64, i64, i32)> {
        (
            tracked("COL1", range(0i64, 1), log),
            tracked("COL2", range(100i64, 1), log),
            tracked("COL3", range(1000i32, 1), log),
        )
    }

    fn make_4tuple() -> impl Distribution<VecBackend, Item = (i32, i64, bool, f64)> {
        (
            constant(1i32),
            range(0i64, 10),
            constant(true),
            constant(2.5f64),
        )
    }

    fn make_tracked_4tuple(
        log: &Log,
    ) -> impl Distribution<VecBackend, Item = (i64, i64, i64, i64)> {
        (
            tracked("COL1", range(0i64, 1), log),
            tracked("COL2", range(100i64, 1), log),
            tracked("COL3", range(1000i64, 1), log),
            tracked("COL4", range(10000i64, 1), log),
        )
    }

    fn make_map_in_tuple(log: &Log) -> impl Distribution<VecBackend, Item = (i64, i64, i64)> {
        (
            tracked("COL1", range(0i64, 1), log),
            map(tracked("MAP_SRC", range(100i64, 1), log), |x| x * 2),
            tracked("COL3", range(1000i64, 1), log),
        )
    }

    fn gen_col4<B: Backend>(log: &Log) -> impl Distribution<B, Item = (i16, i64, i64, i32)> {
        flatten((
            tracked("COL1", range(0i16, 1), log),
            map(
                (
                    tracked("MAP_A", range(100i64, 1), log),
                    tracked("MAP_B", range(1000, 1), log),
                ),
                |(a, b)| (a + b, a * b),
            ),
            tracked("COL4", range(0i32, 5), log),
        ))
    }

    fn make_flatten_order<B: Backend>(
        log: &Log,
    ) -> impl Distribution<B, Item = (i32, i32, i32, i32)> {
        flatten((
            tracked("COL1", range(0i32, 1), log),
            map(
                (
                    tracked("MAP_A", range(100i32, 1), log),
                    tracked("MAP_B", range(1000i32, 1), log),
                ),
                |(a, b)| (a + b, a * b),
            ),
            tracked("COL4", range(0i32, 5), log),
        ))
    }

    fn make_nested_tuple(
        log: &Log,
    ) -> impl Distribution<VecBackend, Item = (i32, (i32, i32), i32)> {
        (
            tracked("COL1", range(0i32, 1), log),
            (
                tracked("COL2", range(100i32, 1), log),
                tracked("COL3", range(1000i32, 1), log),
            ),
            tracked("COL4", range(0i32, 5), log),
        )
    }

    fn make_list_row_iter() -> impl Distribution<VecBackend, Item = Vec<i32>> {
        list(constant(3usize), constant(7i32))
    }

    fn gen_list() -> impl Distribution<VecBackend, Item = Vec<i64>> {
        list(range(0usize, 1), range(0, 1))
    }

    fn make_list_in_tuple() -> impl Distribution<VecBackend, Item = (i32, Vec<i64>)> {
        (constant(42i32), list(constant(2usize), range(0, 1)))
    }

    fn make_tracked_list_in_tuple(
        log: &Log,
    ) -> impl Distribution<VecBackend, Item = (i32, Vec<i64>)> {
        (
            tracked("COL1", constant(42i32), log),
            tracked("LIST", list(constant(2usize), range(0, 1)), log),
        )
    }

    fn make_row_order(log: &Log) -> impl Distribution<VecBackend, Item = (i64, i64)> {
        (
            tracked("COL1", range(0i64, 1), log),
            tracked("COL2", range(100i64, 1), log),
        )
    }

    // ========================================================================
    // Col-by-col collection tests (via collect_flat — no HCons visible!)
    // ========================================================================

    #[test]
    fn test_col_by_col_basic() {
        let mut dist = make_tuple::<VecBackend>();
        let (col1, col2) = dist.collect_flat((Vec::<i32>::new(), Vec::<i64>::new()), &mut rng(), 4);
        assert_eq!(col1, vec![1, 1, 1, 1]);
        assert_eq!(col2, vec![0, 10, 20, 30]);
    }

    #[test]
    fn test_col_by_col_order() {
        let log: Log = Log::default();
        let mut dist = make_tracked_2tuple(&log);
        let (col1, col2) = dist.collect_flat((Vec::<i64>::new(), Vec::<i64>::new()), &mut rng(), 3);

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
        let (col1, col2, col3) = dist.collect_flat(
            (Vec::<i32>::new(), Vec::<i64>::new(), Vec::<bool>::new()),
            &mut rng(),
            3,
        );
        assert_eq!(col1, vec![1, 1, 1]);
        assert_eq!(col2, vec![0, 10, 20]);
        assert_eq!(col3, vec![true, true, true]);
    }

    #[test]
    fn test_3tuple_col_by_col_order() {
        let log: Log = Rc::new(RefCell::new(Vec::new()));
        let mut dist = make_tracked_3tuple(&log);
        let (col1, col2, col3) = dist.collect_flat(
            (Vec::<i64>::new(), Vec::<i64>::new(), Vec::<i32>::new()),
            &mut rng(),
            2,
        );
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
        let mut dist = make_4tuple();
        let (c1, c2, c3, c4) = dist.collect_flat(
            (
                Vec::<i32>::new(),
                Vec::<i64>::new(),
                Vec::<bool>::new(),
                Vec::<f64>::new(),
            ),
            &mut rng(),
            3,
        );
        assert_eq!(c1, vec![1, 1, 1]);
        assert_eq!(c2, vec![0, 10, 20]);
        assert_eq!(c3, vec![true, true, true]);
        assert_eq!(c4, vec![2.5, 2.5, 2.5]);
    }

    #[test]
    fn test_4tuple_col_by_col_order() {
        let log: Log = Rc::new(RefCell::new(Vec::new()));
        let mut dist = make_tracked_4tuple(&log);
        let (c1, c2, c3, c4) = dist.collect_flat(
            (
                Vec::<i64>::new(),
                Vec::<i64>::new(),
                Vec::<i64>::new(),
                Vec::<i64>::new(),
            ),
            &mut rng(),
            2,
        );
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
        let mut dist = make_map_in_tuple(&log);
        let (c1, c2, c3) = dist.collect_flat(
            (Vec::<i64>::new(), Vec::<i64>::new(), Vec::<i64>::new()),
            &mut rng(),
            3,
        );

        assert_eq!(c1, vec![0, 1, 2]);
        assert_eq!(c2, vec![200, 202, 204]);
        assert_eq!(c3, vec![1000, 1001, 1002]);
    }

    #[test]
    fn test_map_in_tuple_order() {
        let log: Log = Rc::new(RefCell::new(Vec::new()));
        let mut dist = make_map_in_tuple(&log);
        let (c1, c2, c3) = dist.collect_flat(
            (Vec::<i64>::new(), Vec::<i64>::new(), Vec::<i64>::new()),
            &mut rng(),
            3,
        );

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
        let log = Log::default();
        let mut dist = gen_col4::<VecBackend>(&log);

        let (c1, c2, c3, c4) = dist.collect_flat(
            (
                Vec::<i16>::new(),
                Vec::<i64>::new(),
                Vec::<i64>::new(),
                Vec::<i32>::new(),
            ),
            &mut rng(),
            3,
        );

        assert_eq!(c1, vec![0, 1, 2]); // COL1
        assert_eq!(c2, vec![1100, 1102, 1104]); // sum
        assert_eq!(c3, vec![100000, 101101, 102204]); // product
        assert_eq!(c4, vec![0, 5, 10]); // COL4
    }

    #[test]
    fn test_map_tuple_auto_flatten_order() {
        let log: Log = Rc::new(RefCell::new(Vec::new()));
        let mut dist = make_flatten_order::<VecBackend>(&log);

        let (_, _, _, _) = dist.collect_flat(
            (
                Vec::<i32>::new(),
                Vec::<i32>::new(),
                Vec::<i32>::new(),
                Vec::<i32>::new(),
            ),
            &mut rng(),
            3,
        );

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
        let log: Log = Rc::new(RefCell::new(Vec::new()));
        let mut dist = make_nested_tuple(&log);

        let (c1, c2, c3, c4) = dist.collect_flat(
            (
                Vec::<i32>::new(),
                Vec::<i32>::new(),
                Vec::<i32>::new(),
                Vec::<i32>::new(),
            ),
            &mut rng(),
            3,
        );

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
        let mut dist = make_list_row_iter();
        let mut iter = dist.row_iter();
        let mut rng = rng();
        let result = iter.next(&mut rng);
        assert_eq!(result, vec![7, 7, 7]);
    }

    #[test]
    fn test_list_collect_into() {
        let mut dist = gen_list();
        let sink = HCons(VecListSink::new(), HNil);
        let (result,) = dist.collect_into(sink, &mut rng(), 3).into_tuple();
        let (values, offsets) = result.into_parts();
        assert_eq!(values, vec![0, 0, 1]);
        assert_eq!(offsets, vec![0, 0, 1, 3]);
    }

    #[test]
    fn test_list_in_tuple() {
        let mut dist = make_list_in_tuple();
        let (scalars, list_sink) = dist.collect_flat(
            (Vec::<i32>::new(), VecListSink::<i64>::new()),
            &mut rng(),
            3,
        );
        let (values, offsets) = list_sink.into_parts();

        assert_eq!(scalars, vec![42, 42, 42]);
        assert_eq!(values, vec![0, 1, 0, 1, 0, 1]);
        assert_eq!(offsets, vec![0, 2, 4, 6]);
    }

    // ========================================================================
    // Tracked<List> — exercises Sink<Vec<T>> through generic wrapper
    // ========================================================================

    #[test]
    fn test_tracked_list_in_tuple() {
        let log: Log = Rc::new(RefCell::new(Vec::new()));
        let mut dist = make_tracked_list_in_tuple(&log);
        let (scalars, list_sink) = dist.collect_flat(
            (Vec::<i32>::new(), VecListSink::<i64>::new()),
            &mut rng(),
            3,
        );
        let (values, offsets) = list_sink.into_parts();

        assert_eq!(scalars, vec![42, 42, 42]);
        assert_eq!(values, vec![0, 1, 0, 1, 0, 1]);
        assert_eq!(offsets, vec![0, 2, 4, 6]);
        // Col-by-col: COL1 x3, then LIST x3
        assert_eq!(
            *log.borrow(),
            vec!["COL1", "COL1", "COL1", "LIST", "LIST", "LIST"]
        );
    }

    // ========================================================================
    // Row-by-row iteration order test
    // ========================================================================

    #[test]
    fn test_row_order() {
        let log: Log = Rc::new(RefCell::new(Vec::new()));
        let mut dist = make_row_order(&log);
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

    // ========================================================================
    // Dual-backend tests: same generic helpers, two backends
    // ========================================================================

    use crate::traits::DequeBackend;
    use std::collections::VecDeque;

    #[test]
    fn test_constant_deque_backend() {
        let mut dist = make_constant::<DequeBackend>();
        let (col,) = dist.collect_flat((VecDeque::<i32>::new(),), &mut rng(), 5);
        assert_eq!(col, VecDeque::from([42, 42, 42, 42, 42]));
    }

    #[test]
    fn test_range_deque_backend() {
        let mut dist = make_range::<DequeBackend>();
        let (col,) = dist.collect_flat((VecDeque::<i64>::new(),), &mut rng(), 5);
        assert_eq!(col, VecDeque::from([0, 1, 2, 3, 4]));
    }

    #[test]
    fn test_tuple_deque_backend() {
        let mut dist = make_tuple::<DequeBackend>();
        let (c1, c2) = dist.collect_flat(
            (VecDeque::<i32>::new(), VecDeque::<i64>::new()),
            &mut rng(),
            4,
        );
        assert_eq!(c1, VecDeque::from([1, 1, 1, 1]));
        assert_eq!(c2, VecDeque::from([0, 10, 20, 30]));
    }
}
