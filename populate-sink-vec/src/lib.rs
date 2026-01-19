use populate_builtins::List;
use populate_core::{Collector, Distribution, IntoColumnIters, ListSink, Rng, SampleIter, Sink};

// === Marker types for Vec collection ===

/// Marker for collecting scalar distributions into Vec<T>.
pub struct Col;

/// Marker for collecting tuple distributions into tuple of Vecs (columnar).
pub struct Cols;

/// Marker for collecting List distributions into (Vec<T>, Vec<usize>) - columnar.
pub struct ListCol;

/// Marker for collecting List distributions into Vec<Vec<T>> - row-oriented.
pub struct Rows;

/// Marker for row-by-row collection that unzips tuples into columns.
/// Use when columns are coupled (e.g., from Map) and can't use ColumnIters.
pub struct Unzip;

/// Marker for cache-local grouped collection using IntoColumnIters.
/// Independent columns are filled first, then coupled groups are unzipped.
/// No intermediate allocations - values go directly into output Vecs.
pub struct GroupedCols;

// === GroupedCols: 2 independent columns, no intermediate allocations ===
impl<T1, T2, D, I1, I2> Collector<D, (T1, T2)> for GroupedCols
where
    D: Distribution<(T1, T2)>,
    D::Iter: IntoColumnIters<Columns = (I1, I2)>,
    I1: SampleIter<Item = T1>,
    I2: SampleIter<Item = T2>,
{
    type Output = (Vec<T1>, Vec<T2>);

    fn collect(dist: &mut D, rng: &mut impl Rng, n: usize) -> Self::Output {
        let iter = dist.sample();
        let (mut c1, mut c2) = iter.into_columns();

        let mut v1 = Vec::with_capacity(n);
        let mut v2 = Vec::with_capacity(n);

        // Fill column 1 entirely (cache-local)
        v1.extend((0..n).map(|_| c1.next(rng)));
        // Fill column 2 entirely
        v2.extend((0..n).map(|_| c2.next(rng)));

        (v1, v2)
    }
}

// === GroupedCols: 1 independent + 1 coupled pair → 3 output columns ===
// For (D1, Map<D2, F>) where Map produces (T2, T3)
// No intermediate Vec<(T2, T3)> - unzip directly into output Vecs
impl<T1, T2, T3, D, I1, ICoupled> Collector<D, (T1, T2, T3)> for GroupedCols
where
    D: Distribution<(T1, (T2, T3))>,
    D::Iter: IntoColumnIters<Columns = (I1, ICoupled)>,
    I1: SampleIter<Item = T1>,
    ICoupled: SampleIter<Item = (T2, T3)>,
{
    type Output = (Vec<T1>, Vec<T2>, Vec<T3>);

    fn collect(dist: &mut D, rng: &mut impl Rng, n: usize) -> Self::Output {
        let iter = dist.sample();
        let (mut c1, mut coupled) = iter.into_columns();

        let mut v1 = Vec::with_capacity(n);
        let mut v2 = Vec::with_capacity(n);
        let mut v3 = Vec::with_capacity(n);

        // Fill independent column entirely first (cache-local)
        v1.extend((0..n).map(|_| c1.next(rng)));

        // Unzip coupled column directly into output - NO intermediate allocation
        for _ in 0..n {
            let (a, b) = coupled.next(rng);
            v2.push(a);
            v3.push(b);
        }

        (v1, v2, v3)
    }
}

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

/// Collect any 2-column distribution into (Vec<T1>, Vec<T2>)
/// Uses Distribution::collect_columns for column-by-column collection.
impl<T1, T2, D> Collector<D, (T1, T2)> for Cols
where
    D: Distribution<(T1, T2)>,
{
    type Output = D::Sinks;

    fn collect(dist: &mut D, rng: &mut impl Rng, n: usize) -> Self::Output {
        dist.collect_columns(rng, n)
    }
}

/// Collect any 3-column distribution into (Vec<T1>, Vec<T2>, Vec<T3>)
/// Uses Distribution::collect_columns for column-by-column collection.
impl<T1, T2, T3, D> Collector<D, (T1, T2, T3)> for Cols
where
    D: Distribution<(T1, T2, T3)>,
{
    type Output = D::Sinks;

    fn collect(dist: &mut D, rng: &mut impl Rng, n: usize) -> Self::Output {
        dist.collect_columns(rng, n)
    }
}

/// Collect any 8-column distribution into 8 Vecs.
/// Uses Distribution::collect_columns for column-by-column collection.
impl<T1, T2, T3, T4, T5, T6, T7, T8, D> Collector<D, (T1, T2, T3, T4, T5, T6, T7, T8)> for Cols
where
    D: Distribution<(T1, T2, T3, T4, T5, T6, T7, T8)>,
{
    type Output = D::Sinks;

    fn collect(dist: &mut D, rng: &mut impl Rng, n: usize) -> Self::Output {
        dist.collect_columns(rng, n)
    }
}

// === Unzip: Row-by-row collection that unzips tuples into columns ===
// Use when columns are coupled (e.g., from Map) and can't use ColumnIters.

/// Collect 2-tuple distribution into (Vec<T1>, Vec<T2>) row-by-row
impl<T1, T2, D> Collector<D, (T1, T2)> for Unzip
where
    D: Distribution<(T1, T2)>,
{
    type Output = (Vec<T1>, Vec<T2>);

    fn collect(dist: &mut D, rng: &mut impl Rng, n: usize) -> Self::Output {
        let mut iter = dist.sample();
        let mut v1 = Vec::with_capacity(n);
        let mut v2 = Vec::with_capacity(n);
        for _ in 0..n {
            let (a, b) = iter.next(rng);
            v1.push(a);
            v2.push(b);
        }
        (v1, v2)
    }
}

/// Collect 3-tuple distribution into (Vec<T1>, Vec<T2>, Vec<T3>) row-by-row
impl<T1, T2, T3, D> Collector<D, (T1, T2, T3)> for Unzip
where
    D: Distribution<(T1, T2, T3)>,
{
    type Output = (Vec<T1>, Vec<T2>, Vec<T3>);

    fn collect(dist: &mut D, rng: &mut impl Rng, n: usize) -> Self::Output {
        let mut iter = dist.sample();
        let mut v1 = Vec::with_capacity(n);
        let mut v2 = Vec::with_capacity(n);
        let mut v3 = Vec::with_capacity(n);
        for _ in 0..n {
            let (a, b, c) = iter.next(rng);
            v1.push(a);
            v2.push(b);
            v3.push(c);
        }
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
    use populate_builtins::Range;
    use populate_core::{CollectExt, unnest};
    use rand::SeedableRng;
    use rand::rngs::StdRng;

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

    #[test]
    fn test_unnest_cols_collection() {
        let mut rng = StdRng::seed_from_u64(42);
        // Test (D1, Unnest<(D2, D3)>) -> (Vec<T1>, Vec<T2>, Vec<T3>)
        let mut dist = (
            Range::new(0i32, 100),
            unnest((Range::new(100i64, 200), Range::new(0.0f64, 1.0))),
        );

        let (v1, v2, v3): (Vec<i32>, Vec<i64>, Vec<f64>) = dist.collect::<Cols>(&mut rng, 10);

        assert_eq!(v1.len(), 10);
        assert_eq!(v2.len(), 10);
        assert_eq!(v3.len(), 10);
        // Values should be in expected ranges
        assert!(v1.iter().all(|&x| x >= 0 && x < 100));
        assert!(v2.iter().all(|&x| x >= 100 && x < 200));
        assert!(v3.iter().all(|&x| x >= 0.0 && x < 1.0));
    }

    #[test]
    fn test_grouped_cols_independent() {
        let mut rng = StdRng::seed_from_u64(42);
        // Test GroupedCols with 2 independent columns
        let mut dist = (Range::new(0i32, 100), Range::new(100i64, 200));

        // Uses IntoColumnIters - no intermediate allocations
        let (v1, v2): (Vec<i32>, Vec<i64>) = dist.collect::<GroupedCols>(&mut rng, 10);

        assert_eq!(v1.len(), 10);
        assert_eq!(v2.len(), 10);
        assert!(v1.iter().all(|&x| x >= 0 && x < 100));
        assert!(v2.iter().all(|&x| x >= 100 && x < 200));
    }

    #[test]
    fn test_unzip_2_tuple() {
        let mut rng = StdRng::seed_from_u64(42);
        // Test Unzip with 2-tuple - row-by-row collection
        let mut dist = (Range::new(0i32, 100), Range::new(100i64, 200));

        let (v1, v2): (Vec<i32>, Vec<i64>) = dist.collect::<Unzip>(&mut rng, 10);

        assert_eq!(v1.len(), 10);
        assert_eq!(v2.len(), 10);
        assert!(v1.iter().all(|&x| x >= 0 && x < 100));
        assert!(v2.iter().all(|&x| x >= 100 && x < 200));
    }

    #[test]
    fn test_unzip_3_tuple() {
        let mut rng = StdRng::seed_from_u64(42);
        // Test Unzip with 3-tuple - row-by-row collection
        let mut dist = (
            Range::new(0i32, 100),
            Range::new(100i64, 200),
            Range::new(0.0f64, 1.0),
        );

        let (v1, v2, v3): (Vec<i32>, Vec<i64>, Vec<f64>) = dist.collect::<Unzip>(&mut rng, 10);

        assert_eq!(v1.len(), 10);
        assert_eq!(v2.len(), 10);
        assert_eq!(v3.len(), 10);
        assert!(v1.iter().all(|&x| x >= 0 && x < 100));
        assert!(v2.iter().all(|&x| x >= 100 && x < 200));
        assert!(v3.iter().all(|&x| x >= 0.0 && x < 1.0));
    }
}
