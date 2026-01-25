//! Prototype: Distribution with Cols for columnar iteration
//!
//! Design:
//! - `sample()` returns row-by-row iterator
//! - `cols()` returns column iterators (tuple of iters for independent, single iter for dependent)
//! - `CollectCols` is a marker trait for valid Cols types

use rand::Rng;

// === Core traits ===

/// An infinite iterator that produces values using an RNG.
pub trait SampleIter: Clone {
    type Item;
    fn next(&mut self, rng: &mut impl Rng) -> Self::Item;
}

/// Marker trait for valid Cols types.
/// Either a SampleIter (atomic) or a tuple of CollectCols (splittable).
pub trait CollectCols {}

// Any SampleIter is a valid Cols
impl<I: SampleIter> CollectCols for I {}

// Tuples of CollectCols are valid Cols
impl<C1: CollectCols, C2: CollectCols> CollectCols for (C1, C2) {}
impl<C1: CollectCols, C2: CollectCols, C3: CollectCols> CollectCols for (C1, C2, C3) {}

/// A factory that produces sample iterators.
pub trait Distribution<T> {
    /// Row-by-row iterator
    type Iter: SampleIter<Item = T>;

    /// Column iterator(s) - either a single SampleIter or tuple of them
    type Cols: CollectCols;

    /// Get row-by-row iterator
    fn sample(&mut self) -> Self::Iter;

    /// Get column iterator(s)
    fn cols(&mut self) -> Self::Cols;
}

// === Constant distribution ===

#[derive(Clone)]
pub struct Constant<T> {
    value: T,
}

impl<T: Clone> Constant<T> {
    pub fn new(value: T) -> Self {
        Self { value }
    }
}

#[derive(Clone)]
pub struct ConstantIter<T>(T);

impl<T: Clone> SampleIter for ConstantIter<T> {
    type Item = T;
    fn next(&mut self, _rng: &mut impl Rng) -> T {
        self.0.clone()
    }
}

impl<T: Clone> Distribution<T> for Constant<T> {
    type Iter = ConstantIter<T>;
    type Cols = ConstantIter<T>;  // Atomic - same as Iter

    fn sample(&mut self) -> Self::Iter {
        ConstantIter(self.value.clone())
    }

    fn cols(&mut self) -> Self::Cols {
        self.sample()  // Same as sample() for atomic
    }
}

// === Range distribution ===

#[derive(Clone)]
pub struct Range<T> {
    start: T,
    end: T,
}

impl<T> Range<T> {
    pub fn new(start: T, end: T) -> Self {
        Self { start, end }
    }
}

#[derive(Clone)]
pub struct RangeIter<T> {
    start: T,
    end: T,
}

impl SampleIter for RangeIter<i64> {
    type Item = i64;
    fn next(&mut self, rng: &mut impl Rng) -> i64 {
        rng.random_range(self.start..self.end)
    }
}

impl SampleIter for RangeIter<i32> {
    type Item = i32;
    fn next(&mut self, rng: &mut impl Rng) -> i32 {
        rng.random_range(self.start..self.end)
    }
}

impl SampleIter for RangeIter<f64> {
    type Item = f64;
    fn next(&mut self, rng: &mut impl Rng) -> f64 {
        rng.random_range(self.start..self.end)
    }
}

impl<T: Clone> Distribution<T> for Range<T>
where
    RangeIter<T>: SampleIter<Item = T>,
{
    type Iter = RangeIter<T>;
    type Cols = RangeIter<T>;  // Atomic - same as Iter

    fn sample(&mut self) -> Self::Iter {
        RangeIter {
            start: self.start.clone(),
            end: self.end.clone(),
        }
    }

    fn cols(&mut self) -> Self::Cols {
        self.sample()  // Same as sample() for atomic
    }
}

// === Map distribution ===

#[derive(Clone)]
pub struct Map<D, F, T> {
    inner: D,
    f: F,
    _marker: std::marker::PhantomData<T>,
}

impl<D, F, T> Map<D, F, T> {
    pub fn new(inner: D, f: F) -> Self {
        Self { inner, f, _marker: std::marker::PhantomData }
    }
}

#[derive(Clone)]
pub struct MapIter<I, F> {
    inner: I,
    f: F,
}

impl<I, F, T, U> SampleIter for MapIter<I, F>
where
    I: SampleIter<Item = T>,
    F: FnMut(T) -> U + Clone,
{
    type Item = U;
    fn next(&mut self, rng: &mut impl Rng) -> U {
        (self.f)(self.inner.next(rng))
    }
}

impl<T, U, D, F> Distribution<U> for Map<D, F, T>
where
    D: Distribution<T>,
    F: FnMut(T) -> U + Clone,
{
    type Iter = MapIter<D::Iter, F>;
    type Cols = MapIter<D::Iter, F>;  // Atomic - stays bundled!

    fn sample(&mut self) -> Self::Iter {
        MapIter {
            inner: self.inner.sample(),
            f: self.f.clone(),
        }
    }

    fn cols(&mut self) -> Self::Cols {
        self.sample()  // Same as sample() - Map can't be split
    }
}

// === Tuple distributions ===

/// Trait for splitting a tuple distribution into its component distributions.
/// This returns the actual distributions, not iterators.
pub trait Split {
    type Parts;
    fn split(self) -> Self::Parts;
}

impl<D1, D2> Split for (D1, D2) {
    type Parts = (D1, D2);
    fn split(self) -> Self::Parts {
        self  // Just return the tuple of distributions
    }
}

impl<D1, D2, D3> Split for (D1, D2, D3) {
    type Parts = (D1, D2, D3);
    fn split(self) -> Self::Parts {
        self
    }
}

/// Row-by-row iterator for 2-tuples (newtype, implements SampleIter)
#[derive(Clone)]
pub struct TupleIter2<I1, I2>(pub I1, pub I2);

impl<I1: SampleIter, I2: SampleIter> SampleIter for TupleIter2<I1, I2> {
    type Item = (I1::Item, I2::Item);

    fn next(&mut self, rng: &mut impl Rng) -> Self::Item {
        (self.0.next(rng), self.1.next(rng))
    }
}

impl<T1, T2, D1, D2> Distribution<(T1, T2)> for (D1, D2)
where
    D1: Distribution<T1>,
    D2: Distribution<T2>,
{
    type Iter = TupleIter2<D1::Iter, D2::Iter>;
    type Cols = (D1::Cols, D2::Cols);  // Tuple of column iters - SPLITS!

    fn sample(&mut self) -> Self::Iter {
        TupleIter2(self.0.sample(), self.1.sample())
    }

    fn cols(&mut self) -> Self::Cols {
        (self.0.cols(), self.1.cols())  // Returns tuple of iters, NOT TupleIter2
    }
}

/// Row-by-row iterator for 3-tuples
#[derive(Clone)]
pub struct TupleIter3<I1, I2, I3>(pub I1, pub I2, pub I3);

impl<I1: SampleIter, I2: SampleIter, I3: SampleIter> SampleIter for TupleIter3<I1, I2, I3> {
    type Item = (I1::Item, I2::Item, I3::Item);

    fn next(&mut self, rng: &mut impl Rng) -> Self::Item {
        (self.0.next(rng), self.1.next(rng), self.2.next(rng))
    }
}

impl<T1, T2, T3, D1, D2, D3> Distribution<(T1, T2, T3)> for (D1, D2, D3)
where
    D1: Distribution<T1>,
    D2: Distribution<T2>,
    D3: Distribution<T3>,
{
    type Iter = TupleIter3<D1::Iter, D2::Iter, D3::Iter>;
    type Cols = (D1::Cols, D2::Cols, D3::Cols);

    fn sample(&mut self) -> Self::Iter {
        TupleIter3(self.0.sample(), self.1.sample(), self.2.sample())
    }

    fn cols(&mut self) -> Self::Cols {
        (self.0.cols(), self.1.cols(), self.2.cols())
    }
}

// === Collector trait and impls ===

pub trait Collector<D, T> {
    type Output;
    fn collect(dist: &mut D, rng: &mut impl Rng, n: usize) -> Self::Output;
}

/// Marker for row-by-row collection into Vec<T>
pub struct Col;

/// Marker for columnar collection using cols()
pub struct Cols;

// Col: collect any distribution into Vec<T> (row-by-row)
impl<T, D: Distribution<T>> Collector<D, T> for Col {
    type Output = Vec<T>;

    fn collect(dist: &mut D, rng: &mut impl Rng, n: usize) -> Self::Output {
        let mut iter = dist.sample();
        (0..n).map(|_| iter.next(rng)).collect()
    }
}

// Cols: collect 2-tuple distribution into (Vec<T1>, Vec<T2>)
impl<T1, T2, D1, D2> Collector<(D1, D2), (T1, T2)> for Cols
where
    D1: Distribution<T1>,
    D2: Distribution<T2>,
    D1::Cols: SampleIter<Item = T1>,
    D2::Cols: SampleIter<Item = T2>,
{
    type Output = (Vec<T1>, Vec<T2>);

    fn collect(dist: &mut (D1, D2), rng: &mut impl Rng, n: usize) -> Self::Output {
        let (mut c1, mut c2) = dist.cols();
        let v1 = (0..n).map(|_| c1.next(rng)).collect();
        let v2 = (0..n).map(|_| c2.next(rng)).collect();
        (v1, v2)
    }
}

// Cols: collect 3-tuple distribution into (Vec<T1>, Vec<T2>, Vec<T3>)
impl<T1, T2, T3, D1, D2, D3> Collector<(D1, D2, D3), (T1, T2, T3)> for Cols
where
    D1: Distribution<T1>,
    D2: Distribution<T2>,
    D3: Distribution<T3>,
    D1::Cols: SampleIter<Item = T1>,
    D2::Cols: SampleIter<Item = T2>,
    D3::Cols: SampleIter<Item = T3>,
{
    type Output = (Vec<T1>, Vec<T2>, Vec<T3>);

    fn collect(dist: &mut (D1, D2, D3), rng: &mut impl Rng, n: usize) -> Self::Output {
        let (mut c1, mut c2, mut c3) = dist.cols();
        let v1 = (0..n).map(|_| c1.next(rng)).collect();
        let v2 = (0..n).map(|_| c2.next(rng)).collect();
        let v3 = (0..n).map(|_| c3.next(rng)).collect();
        (v1, v2, v3)
    }
}

// === Extension trait for ergonomic collection ===

pub trait CollectExt<T>: Distribution<T> {
    fn collect<C>(&mut self, rng: &mut impl Rng, n: usize) -> C::Output
    where
        C: Collector<Self, T>,
        Self: Sized,
    {
        C::collect(self, rng, n)
    }
}

impl<T, D: Distribution<T>> CollectExt<T> for D {}

// === Helper constructors ===

pub fn constant<T: Clone>(value: T) -> Constant<T> {
    Constant::new(value)
}

pub fn range<T>(start: T, end: T) -> Range<T> {
    Range::new(start, end)
}

pub trait MapExt<T>: Distribution<T> + Sized {
    fn map<U, F: FnMut(T) -> U + Clone>(self, f: F) -> Map<Self, F, T> {
        Map::new(self, f)
    }
}

impl<T, D: Distribution<T>> MapExt<T> for D {}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;
    use rand::rngs::StdRng;
    use std::cell::RefCell;
    use std::rc::Rc;

    #[test]
    fn test_constant_sample_and_cols() {
        let mut dist = constant(42i32);
        let mut rng = StdRng::seed_from_u64(0);

        // sample() - row iterator
        let mut iter = dist.sample();
        assert_eq!(iter.next(&mut rng), 42);

        // cols() - same as sample for atomic
        let mut col_iter = dist.cols();
        assert_eq!(col_iter.next(&mut rng), 42);
    }

    #[test]
    fn test_range_sample_and_cols() {
        let mut dist = range(0i64, 100);
        let mut rng = StdRng::seed_from_u64(0);

        // sample() - row iterator
        let mut iter = dist.sample();
        let v1 = iter.next(&mut rng);
        assert!(v1 >= 0 && v1 < 100);

        // cols() - same as sample for atomic
        let mut col_iter = dist.cols();
        let v2 = col_iter.next(&mut rng);
        assert!(v2 >= 0 && v2 < 100);
    }

    #[test]
    fn test_tuple_sample_gives_pairs() {
        let mut dist = (range(0i64, 100), range(0.0f64, 1.0));
        let mut rng = StdRng::seed_from_u64(0);

        // sample() returns TupleIter2 which produces pairs
        let mut iter = dist.sample();
        let (a, b) = iter.next(&mut rng);
        assert!(a >= 0 && a < 100);
        assert!(b >= 0.0 && b < 1.0);
    }

    #[test]
    fn test_tuple_cols_gives_separate_iters() {
        let mut dist = (range(0i64, 100), range(0.0f64, 1.0));
        let mut rng = StdRng::seed_from_u64(0);

        // cols() returns (RangeIter<i64>, RangeIter<f64>) - separate iterators!
        let (mut c1, mut c2) = dist.cols();

        // Can iterate each independently
        let a = c1.next(&mut rng);
        let b = c2.next(&mut rng);
        assert!(a >= 0 && a < 100);
        assert!(b >= 0.0 && b < 1.0);
    }

    #[test]
    fn test_split_gives_independent_distributions() {
        // split() returns the actual distributions, not iterators
        let dist = (range(0i64, 100), range(0.0f64, 1.0));

        // Split into two independent distributions
        let (mut d1, mut d2) = dist.split();

        // Each distribution can be sampled independently
        let mut rng1 = StdRng::seed_from_u64(42);
        let mut rng2 = StdRng::seed_from_u64(99);

        // Get iterators from each distribution
        let mut iter1 = d1.sample();
        let mut iter2 = d2.sample();

        // Iterate them completely independently with different RNGs
        let vals1: Vec<i64> = (0..5).map(|_| iter1.next(&mut rng1)).collect();
        let vals2: Vec<f64> = (0..3).map(|_| iter2.next(&mut rng2)).collect();

        assert_eq!(vals1.len(), 5);
        assert_eq!(vals2.len(), 3);

        // Can also get fresh iterators
        let mut iter1b = d1.sample();
        let another_val = iter1b.next(&mut rng1);
        assert!(another_val >= 0 && another_val < 100);
    }

    #[test]
    fn test_split_3tuple() {
        let dist = (range(0i32, 10), constant(42i64), range(0.0f64, 1.0));

        let (mut d1, mut d2, mut d3) = dist.split();

        let mut rng = StdRng::seed_from_u64(0);

        // Each is an independent distribution
        let mut i1 = d1.sample();
        let mut i2 = d2.sample();
        let mut i3 = d3.sample();

        let v1 = i1.next(&mut rng);
        let v2 = i2.next(&mut rng);
        let v3 = i3.next(&mut rng);

        assert!(v1 >= 0 && v1 < 10);
        assert_eq!(v2, 42);
        assert!(v3 >= 0.0 && v3 < 1.0);
    }

    #[test]
    fn test_split_then_collect_each() {
        let dist = (range(0i32, 100), range(0i64, 1000));
        let (mut d1, mut d2) = dist.split();

        let mut rng = StdRng::seed_from_u64(0);

        // Collect each distribution independently using Col
        let v1: Vec<i32> = d1.collect::<Col>(&mut rng, 10);
        let v2: Vec<i64> = d2.collect::<Col>(&mut rng, 5);  // Different count!

        assert_eq!(v1.len(), 10);
        assert_eq!(v2.len(), 5);  // Can collect different amounts!
    }

    #[test]
    fn test_map_cols_stays_bundled() {
        let mut dist = (range(0i64, 100), range(0i64, 100)).map(|(a, b)| (a + b, a * b));
        let mut rng = StdRng::seed_from_u64(0);

        // cols() for Map returns the same as sample() - stays bundled
        let mut col_iter = dist.cols();
        let (sum, prod) = col_iter.next(&mut rng);
        // Just verify it returns a pair
        assert!(sum >= 0);
        assert!(prod >= 0);
    }

    #[test]
    fn test_col_collector_row_by_row() {
        let mut dist = (range(0i32, 100), range(0i32, 100));
        let mut rng = StdRng::seed_from_u64(0);

        // Col collects row-by-row into Vec<(T1, T2)>
        let result: Vec<(i32, i32)> = dist.collect::<Col>(&mut rng, 5);
        assert_eq!(result.len(), 5);
    }

    #[test]
    fn test_cols_collector_columnar() {
        let mut dist = (range(0i32, 100), range(0i64, 1000));
        let mut rng = StdRng::seed_from_u64(0);

        // Cols collects column-by-column into (Vec<T1>, Vec<T2>)
        let (v1, v2): (Vec<i32>, Vec<i64>) = dist.collect::<Cols>(&mut rng, 5);
        assert_eq!(v1.len(), 5);
        assert_eq!(v2.len(), 5);
    }

    #[test]
    fn test_cols_collector_3tuple() {
        let mut dist = (range(0i32, 100), range(0i64, 1000), constant(3.14f64));
        let mut rng = StdRng::seed_from_u64(0);

        let (v1, v2, v3): (Vec<i32>, Vec<i64>, Vec<f64>) = dist.collect::<Cols>(&mut rng, 5);
        assert_eq!(v1.len(), 5);
        assert_eq!(v2.len(), 5);
        assert_eq!(v3.len(), 5);
        assert!(v3.iter().all(|&x| x == 3.14));
    }

    /// Track generation order
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
        type Cols = TrackedIter;

        fn sample(&mut self) -> Self::Iter {
            TrackedIter {
                col_id: self.col_id,
                log: self.log.clone(),
            }
        }

        fn cols(&mut self) -> Self::Cols {
            self.sample()
        }
    }

    #[test]
    fn test_col_generates_row_by_row_interleaved() {
        let log = Rc::new(RefCell::new(Vec::new()));
        let mut dist = (
            TrackedDist { col_id: 1, log: log.clone() },
            TrackedDist { col_id: 2, log: log.clone() },
        );

        let mut rng = StdRng::seed_from_u64(0);
        let _: Vec<(u8, u8)> = dist.collect::<Col>(&mut rng, 3);

        // Row-by-row: [1,2, 1,2, 1,2]
        assert_eq!(*log.borrow(), vec![1, 2, 1, 2, 1, 2]);
    }

    #[test]
    fn test_cols_generates_column_by_column() {
        let log = Rc::new(RefCell::new(Vec::new()));
        let mut dist = (
            TrackedDist { col_id: 1, log: log.clone() },
            TrackedDist { col_id: 2, log: log.clone() },
        );

        let mut rng = StdRng::seed_from_u64(0);
        let _: (Vec<u8>, Vec<u8>) = dist.collect::<Cols>(&mut rng, 3);

        // Column-by-column: [1,1,1, 2,2,2]
        assert_eq!(*log.borrow(), vec![1, 1, 1, 2, 2, 2]);
    }

    #[test]
    fn test_map_with_cols_stays_bundled() {
        let log = Rc::new(RefCell::new(Vec::new()));

        // Map over tuple - produces (u8, u8) but stays bundled
        let mut dist = (
            TrackedDist { col_id: 1, log: log.clone() },
            TrackedDist { col_id: 2, log: log.clone() },
        ).map(|(a, b)| (a + 10, b + 20));

        let mut rng = StdRng::seed_from_u64(0);

        // Collect with Col - row by row
        let result: Vec<(u8, u8)> = dist.collect::<Col>(&mut rng, 3);

        // Map samples row-by-row from inner tuple: [1,2, 1,2, 1,2]
        assert_eq!(*log.borrow(), vec![1, 2, 1, 2, 1, 2]);
        assert_eq!(result.len(), 3);
    }
}
