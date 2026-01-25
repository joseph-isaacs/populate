use populate_builtins::List;
use populate_core::{
    Collector, ColumnarCollect, ColumnarDistribution, Distribution, ListSink, Rng, SampleIter, Sink,
};

// === Marker types for Vec collection ===

/// Marker for collecting scalar distributions into Vec<T>.
pub struct Col;

/// Marker for collecting tuple distributions into tuple of Vecs (columnar).
pub struct Cols;

/// Marker for collecting List distributions into (Vec<T>, Vec<usize>) - columnar.
pub struct ListCol;

/// Marker for collecting List distributions into Vec<Vec<T>> - row-oriented.
pub struct Rows;

/// Marker for columnar collection - generates each column fully before moving to next.
/// Uses the `Cols` associated type from `ColumnarDistribution` to determine column groups.
pub struct ColumnarCols;

// === Collector implementations ===

/// Collect any Distribution into Vec<T>
impl<T, D> Collector<D, T> for Col
where
    D: Distribution<T>,
{
    type Output = Vec<T>;

    fn collect(dist: &mut D, rng: &mut impl Rng, n: usize) -> Self::Output {
        let mut iter = dist.sample();
        (0..n).map(|_| iter.next(rng)).collect()
    }
}

/// Collect 2-tuple distribution into (Vec<T1>, Vec<T2>)
impl<T1, T2, D1, D2> Collector<(D1, D2), (T1, T2)> for Cols
where
    D1: Distribution<T1>,
    D2: Distribution<T2>,
{
    type Output = (Vec<T1>, Vec<T2>);

    fn collect(dist: &mut (D1, D2), rng: &mut impl Rng, n: usize) -> Self::Output {
        let mut iter = dist.sample();
        let mut v1 = Vec::with_capacity(n);
        let mut v2 = Vec::with_capacity(n);
        v1.extend((0..n).map(|_| iter.0.next(rng)));
        v2.extend((0..n).map(|_| iter.1.next(rng)));
        (v1, v2)
    }
}

/// Collect 3-tuple distribution into (Vec<T1>, Vec<T2>, Vec<T3>)
impl<T1, T2, T3, D1, D2, D3> Collector<(D1, D2, D3), (T1, T2, T3)> for Cols
where
    D1: Distribution<T1>,
    D2: Distribution<T2>,
    D3: Distribution<T3>,
{
    type Output = (Vec<T1>, Vec<T2>, Vec<T3>);

    fn collect(dist: &mut (D1, D2, D3), rng: &mut impl Rng, n: usize) -> Self::Output {
        let mut iter = dist.sample();
        let mut v1 = Vec::with_capacity(n);
        let mut v2 = Vec::with_capacity(n);
        let mut v3 = Vec::with_capacity(n);
        v1.extend((0..n).map(|_| iter.0.next(rng)));
        v2.extend((0..n).map(|_| iter.1.next(rng)));
        v3.extend((0..n).map(|_| iter.2.next(rng)));
        (v1, v2, v3)
    }
}

/// Collect List distribution into (Vec<T>, Vec<usize>) - columnar (values and offsets)
impl<T, L, E> Collector<List<L, E>, Vec<T>> for ListCol
where
    L: Distribution<usize>,
    E: Distribution<T>,
{
    type Output = (Vec<T>, Vec<usize>);

    fn collect(dist: &mut List<L, E>, rng: &mut impl Rng, n: usize) -> Self::Output {
        let iter = dist.sample();
        populate_core::collect_list_into(iter.lengths, iter.elements, rng, n, VecListSink::new())
    }
}

/// Collect List distribution into Vec<Vec<T>> - row-oriented
impl<T, L, E> Collector<List<L, E>, Vec<T>> for Rows
where
    L: Distribution<usize>,
    E: Distribution<T>,
{
    type Output = Vec<Vec<T>>;

    fn collect(dist: &mut List<L, E>, rng: &mut impl Rng, n: usize) -> Self::Output {
        let mut iter = dist.sample();
        (0..n)
            .map(|_| {
                let len = iter.lengths.next(rng);
                (0..len).map(|_| iter.elements.next(rng)).collect()
            })
            .collect()
    }
}

// === ColumnarCols implementation ===
// Uses ColumnarDistribution::into_cols() and ColumnarCollect::columnar_collect()
// to generate each column group fully before moving to the next.

/// Generic columnar collection using the ColumnarDistribution trait.
/// This works for any distribution that implements ColumnarDistribution,
/// including tuples of independent distributions and bundled distributions like Map.
impl<T, D> Collector<D, T> for ColumnarCols
where
    D: ColumnarDistribution<T> + Clone,
{
    type Output = <D::Cols as ColumnarCollect>::Output;

    fn collect(dist: &mut D, rng: &mut impl Rng, n: usize) -> Self::Output {
        let mut cols = dist.clone().into_cols();
        cols.columnar_collect(rng, n)
    }
}

// === Sink types ===

/// A simple Vec sink.
pub struct VecSink<T> {
    data: Vec<T>,
}

impl<T> VecSink<T> {
    pub fn new() -> Self {
        Self { data: Vec::new() }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            data: Vec::with_capacity(capacity),
        }
    }
}

impl<T> Default for VecSink<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Sink<T> for VecSink<T> {
    type Output = Vec<T>;

    fn push(&mut self, value: T) {
        self.data.push(value);
    }

    fn extend(&mut self, iter: impl Iterator<Item = T>) {
        self.data.extend(iter);
    }

    fn finish(self) -> Self::Output {
        self.data
    }
}

/// A Vec-based list sink that produces (values, offsets).
pub struct VecListSink<T> {
    values: Vec<T>,
    offsets: Vec<usize>,
}

impl<T> VecListSink<T> {
    pub fn new() -> Self {
        Self {
            values: Vec::new(),
            offsets: vec![0],
        }
    }
}

impl<T> Default for VecListSink<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> ListSink<T> for VecListSink<T> {
    type Output = (Vec<T>, Vec<usize>);

    fn start_list(&mut self) {
        // Nothing to do at start
    }

    fn push_element(&mut self, value: T) {
        self.values.push(value);
    }

    fn end_list(&mut self) {
        self.offsets.push(self.values.len());
    }

    fn finish(self) -> Self::Output {
        (self.values, self.offsets)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use populate_builtins::range;
    use populate_core::CollectExt;
    use rand::SeedableRng;
    use rand::rngs::StdRng;
    use std::cell::RefCell;
    use std::rc::Rc;

    #[test]
    fn test_vec_sink() {
        let mut sink = VecSink::new();
        sink.push(1);
        sink.push(2);
        sink.push(3);
        assert_eq!(sink.finish(), vec![1, 2, 3]);
    }

    #[test]
    fn test_vec_sink_extend() {
        let mut sink = VecSink::new();
        sink.extend(1..=5);
        assert_eq!(sink.finish(), vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_vec_list_sink() {
        let mut sink = VecListSink::new();
        sink.start_list();
        sink.push_element(1);
        sink.push_element(2);
        sink.end_list();
        sink.start_list();
        sink.push_element(3);
        sink.end_list();
        let (values, offsets) = sink.finish();
        assert_eq!(values, vec![1, 2, 3]);
        assert_eq!(offsets, vec![0, 2, 3]);
    }

    /// A distribution that records the order in which values are generated.
    /// Each value is tagged with a column ID.
    #[derive(Clone)]
    struct TrackedDist {
        col_id: u8,
        log: Rc<RefCell<Vec<u8>>>,
    }

    #[derive(Clone)]
    struct TrackedIter {
        col_id: u8,
        log: Rc<RefCell<Vec<u8>>>,
    }

    impl SampleIter for TrackedIter {
        type Item = u8;

        fn next(&mut self, _rng: &mut impl Rng) -> u8 {
            self.log.borrow_mut().push(self.col_id);
            self.col_id
        }
    }

    impl Distribution<u8> for TrackedDist {
        type Iter = TrackedIter;

        fn sample(&mut self) -> Self::Iter {
            TrackedIter {
                col_id: self.col_id,
                log: self.log.clone(),
            }
        }
    }

    impl ColumnarDistribution<u8> for TrackedDist {
        type Cols = populate_core::SingleCol<Self, u8>;

        fn into_cols(self) -> Self::Cols {
            populate_core::SingleCol::new(self)
        }
    }

    #[test]
    fn test_cols_generates_interleaved() {
        // Cols (the existing marker) generates columns in an interleaved pattern
        // but actually collects column-by-column from the tuple iterator
        let log = Rc::new(RefCell::new(Vec::new()));
        let mut dist = (
            TrackedDist {
                col_id: 1,
                log: log.clone(),
            },
            TrackedDist {
                col_id: 2,
                log: log.clone(),
            },
        );

        let mut rng = StdRng::seed_from_u64(42);
        let _result: (Vec<u8>, Vec<u8>) = dist.collect::<Cols>(&mut rng, 3);

        // Cols generates: all of col1, then all of col2
        // Pattern: [1,1,1, 2,2,2]
        let order = log.borrow().clone();
        assert_eq!(order, vec![1, 1, 1, 2, 2, 2]);
    }

    #[test]
    fn test_columnar_cols_generates_column_by_column() {
        // ColumnarCols should generate all of column 1, then all of column 2
        let log = Rc::new(RefCell::new(Vec::new()));
        let mut dist = (
            TrackedDist {
                col_id: 1,
                log: log.clone(),
            },
            TrackedDist {
                col_id: 2,
                log: log.clone(),
            },
        );

        let mut rng = StdRng::seed_from_u64(42);
        let _result: (Vec<u8>, Vec<u8>) = dist.collect::<ColumnarCols>(&mut rng, 3);

        // ColumnarCols generates: all of col1, then all of col2
        // Pattern: [1,1,1, 2,2,2]
        let order = log.borrow().clone();
        assert_eq!(order, vec![1, 1, 1, 2, 2, 2]);
    }

    #[test]
    fn test_col_generates_row_by_row() {
        // Col on a tuple generates row-by-row (interleaved)
        let log = Rc::new(RefCell::new(Vec::new()));
        let mut dist = (
            TrackedDist {
                col_id: 1,
                log: log.clone(),
            },
            TrackedDist {
                col_id: 2,
                log: log.clone(),
            },
        );

        let mut rng = StdRng::seed_from_u64(42);
        // Col collects the tuple as a single value, generating interleaved
        let _result: Vec<(u8, u8)> = dist.collect::<Col>(&mut rng, 3);

        // Col generates row-by-row: [1,2, 1,2, 1,2]
        let order = log.borrow().clone();
        assert_eq!(order, vec![1, 2, 1, 2, 1, 2]);
    }

    #[test]
    fn test_columnar_3_tuple() {
        let log = Rc::new(RefCell::new(Vec::new()));
        let mut dist = (
            TrackedDist {
                col_id: 1,
                log: log.clone(),
            },
            TrackedDist {
                col_id: 2,
                log: log.clone(),
            },
            TrackedDist {
                col_id: 3,
                log: log.clone(),
            },
        );

        let mut rng = StdRng::seed_from_u64(42);
        let _result: (Vec<u8>, Vec<u8>, Vec<u8>) = dist.collect::<ColumnarCols>(&mut rng, 2);

        // ColumnarCols generates: all of col1, then col2, then col3
        let order = log.borrow().clone();
        assert_eq!(order, vec![1, 1, 2, 2, 3, 3]);
    }

    #[test]
    fn test_basic_columnar_collection() {
        let mut dist = (range(0i32, 100), range(0i64, 1000));
        let mut rng = StdRng::seed_from_u64(42);
        let (v1, v2): (Vec<i32>, Vec<i64>) = dist.collect::<ColumnarCols>(&mut rng, 5);

        assert_eq!(v1.len(), 5);
        assert_eq!(v2.len(), 5);
    }

    #[test]
    fn test_map_stays_bundled() {
        // Map distributions should stay bundled because their output depends on input.
        // When a mapped distribution produces a tuple, the entire map is generated
        // as a single unit (row-by-row), not split into independent columns.
        use populate_builtins::MapExt;

        let log = Rc::new(RefCell::new(Vec::new()));

        // Create a map that produces (col1, col2) from a single source
        // The source tracks when it's called
        let source = TrackedDist {
            col_id: 0,
            log: log.clone(),
        };

        // Map produces a 2-tuple from each source value
        let mut dist = source.map(|v| (v, v + 10));

        let mut rng = StdRng::seed_from_u64(42);
        // Collecting into Vec of tuples should call source once per row
        let result: Vec<(u8, u8)> = dist.collect::<Col>(&mut rng, 3);

        // Source should be called 3 times (once per row)
        let order = log.borrow().clone();
        assert_eq!(order, vec![0, 0, 0]);
        assert_eq!(result.len(), 3);
    }
}
