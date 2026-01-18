use populate_builtins::List;
use populate_core::{
    Collector, ColumnIters2, ColumnIters3, Distribution, ListSink, Rng, SampleIter, Sink,
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

/// Marker for row-by-row collection that unzips tuples into columns.
/// Use when columns are coupled (e.g., from Map) and can't use ColumnIters.
pub struct Unzip;

/// Marker for cache-local grouped collection.
/// Independent columns are filled first, then coupled groups.
pub struct GroupedCols;

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
/// Works for flat tuples (D1, D2) and any type implementing ColumnIters2.
impl<T1, T2, D> Collector<D, (T1, T2)> for Cols
where
    D: ColumnIters2<T1, T2>,
{
    type Output = (Vec<T1>, Vec<T2>);

    fn collect(dist: &mut D, rng: &mut impl Rng, n: usize) -> Self::Output {
        let (mut i1, mut i2) = dist.column_iters();
        let mut v1 = Vec::with_capacity(n);
        let mut v2 = Vec::with_capacity(n);
        v1.extend((0..n).map(|_| i1.next(rng)));
        v2.extend((0..n).map(|_| i2.next(rng)));
        (v1, v2)
    }
}

/// Collect any 3-column distribution into (Vec<T1>, Vec<T2>, Vec<T3>)
/// Works for flat tuples (D1, D2, D3), unnested (D1, Unnest<(D2, D3)>), etc.
impl<T1, T2, T3, D> Collector<D, (T1, T2, T3)> for Cols
where
    D: ColumnIters3<T1, T2, T3>,
{
    type Output = (Vec<T1>, Vec<T2>, Vec<T3>);

    fn collect(dist: &mut D, rng: &mut impl Rng, n: usize) -> Self::Output {
        let (mut i1, mut i2, mut i3) = dist.column_iters();
        let mut v1 = Vec::with_capacity(n);
        let mut v2 = Vec::with_capacity(n);
        let mut v3 = Vec::with_capacity(n);
        v1.extend((0..n).map(|_| i1.next(rng)));
        v2.extend((0..n).map(|_| i2.next(rng)));
        v3.extend((0..n).map(|_| i3.next(rng)));
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
}
