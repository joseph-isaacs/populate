// Re-export rand::Rng
pub use rand::Rng;

// === Collection traits ===

/// Trait for a collector that can collect a distribution D into an output.
/// The collector type C is a marker (e.g., `Arrow`, `Vecs`) that determines
/// the output family. The associated `Output` type is the actual result.
/// T is the item type produced by the distribution.
pub trait Collector<D, T>: Sized {
    type Output;
    fn collect(dist: &mut D, rng: &mut impl Rng, n: usize) -> Self::Output;
}

/// Extension trait that adds `collect` method to all distributions.
pub trait CollectExt<T>: Distribution<T> {
    fn collect<C>(&mut self, rng: &mut impl Rng, n: usize) -> C::Output
    where
        C: Collector<Self, T>,
        Self: Sized,
    {
        C::collect(self, rng, n)
    }
}

// Blanket impl for all Distribution<T>
impl<T, D: Distribution<T>> CollectExt<T> for D {}

// === Core traits ===

/// An infinite iterator that produces values using an RNG.
///
/// # Clone semantics
///
/// Iterators must implement `Clone` such that cloning produces an iterator
/// with no accumulated internal state. A cloned iterator must produce the
/// same sequence (given the same RNG state) as a freshly created iterator.
///
/// This invariant is used by list collectors to ensure each list gets
/// independent element generation - the element iterator is cloned before
/// generating each list's values.
///
/// For stateless iterators (e.g., `Range`, `Constant`), this is trivially satisfied.
/// For stateful iterators (e.g., `Runs`), clone must reset any accumulated state
/// (e.g., the "current run value") to its initial condition.
pub trait SampleIter: Clone {
    type Item;
    fn next(&mut self, rng: &mut impl Rng) -> Self::Item;
}

/// A factory that produces sample iterators.
pub trait Distribution<T> {
    type Iter: SampleIter<Item = T>;
    fn sample(&mut self) -> Self::Iter;
}

/// A sink that consumes values and produces an output.
pub trait Sink<T> {
    type Output;

    fn push(&mut self, value: T);

    /// Bulk append. Default uses push, but implementations can override
    /// with optimized bulk operations.
    fn extend(&mut self, iter: impl Iterator<Item = T>) {
        for value in iter {
            self.push(value);
        }
    }

    fn finish(self) -> Self::Output;
}

/// A sink for list values that tracks offsets.
pub trait ListSink<T> {
    type Output;

    fn start_list(&mut self);
    fn push_element(&mut self, value: T);
    fn end_list(&mut self);
    fn finish(self) -> Self::Output;
}

// === Tuple iterators and Distribution impls ===
// These must be in the same crate as Distribution due to orphan rules.

/// Iterator for 2-tuple distributions.
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

    fn sample(&mut self) -> Self::Iter {
        TupleIter2(self.0.sample(), self.1.sample())
    }
}

/// Iterator for 3-tuple distributions.
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

    fn sample(&mut self) -> Self::Iter {
        TupleIter3(self.0.sample(), self.1.sample(), self.2.sample())
    }
}

/// Iterator for 4-tuple distributions.
#[derive(Clone)]
pub struct TupleIter4<I1, I2, I3, I4>(pub I1, pub I2, pub I3, pub I4);

impl<I1: SampleIter, I2: SampleIter, I3: SampleIter, I4: SampleIter> SampleIter
    for TupleIter4<I1, I2, I3, I4>
{
    type Item = (I1::Item, I2::Item, I3::Item, I4::Item);

    fn next(&mut self, rng: &mut impl Rng) -> Self::Item {
        (
            self.0.next(rng),
            self.1.next(rng),
            self.2.next(rng),
            self.3.next(rng),
        )
    }
}

impl<T1, T2, T3, T4, D1, D2, D3, D4> Distribution<(T1, T2, T3, T4)> for (D1, D2, D3, D4)
where
    D1: Distribution<T1>,
    D2: Distribution<T2>,
    D3: Distribution<T3>,
    D4: Distribution<T4>,
{
    type Iter = TupleIter4<D1::Iter, D2::Iter, D3::Iter, D4::Iter>;

    fn sample(&mut self) -> Self::Iter {
        TupleIter4(
            self.0.sample(),
            self.1.sample(),
            self.2.sample(),
            self.3.sample(),
        )
    }
}

/// Iterator for 5-tuple distributions.
#[derive(Clone)]
pub struct TupleIter5<I1, I2, I3, I4, I5>(pub I1, pub I2, pub I3, pub I4, pub I5);

impl<I1: SampleIter, I2: SampleIter, I3: SampleIter, I4: SampleIter, I5: SampleIter> SampleIter
    for TupleIter5<I1, I2, I3, I4, I5>
{
    type Item = (I1::Item, I2::Item, I3::Item, I4::Item, I5::Item);

    fn next(&mut self, rng: &mut impl Rng) -> Self::Item {
        (
            self.0.next(rng),
            self.1.next(rng),
            self.2.next(rng),
            self.3.next(rng),
            self.4.next(rng),
        )
    }
}

impl<T1, T2, T3, T4, T5, D1, D2, D3, D4, D5> Distribution<(T1, T2, T3, T4, T5)>
    for (D1, D2, D3, D4, D5)
where
    D1: Distribution<T1>,
    D2: Distribution<T2>,
    D3: Distribution<T3>,
    D4: Distribution<T4>,
    D5: Distribution<T5>,
{
    type Iter = TupleIter5<D1::Iter, D2::Iter, D3::Iter, D4::Iter, D5::Iter>;

    fn sample(&mut self) -> Self::Iter {
        TupleIter5(
            self.0.sample(),
            self.1.sample(),
            self.2.sample(),
            self.3.sample(),
            self.4.sample(),
        )
    }
}

/// Iterator for 6-tuple distributions.
#[derive(Clone)]
pub struct TupleIter6<I1, I2, I3, I4, I5, I6>(pub I1, pub I2, pub I3, pub I4, pub I5, pub I6);

impl<I1: SampleIter, I2: SampleIter, I3: SampleIter, I4: SampleIter, I5: SampleIter, I6: SampleIter>
    SampleIter for TupleIter6<I1, I2, I3, I4, I5, I6>
{
    type Item = (I1::Item, I2::Item, I3::Item, I4::Item, I5::Item, I6::Item);

    fn next(&mut self, rng: &mut impl Rng) -> Self::Item {
        (
            self.0.next(rng),
            self.1.next(rng),
            self.2.next(rng),
            self.3.next(rng),
            self.4.next(rng),
            self.5.next(rng),
        )
    }
}

impl<T1, T2, T3, T4, T5, T6, D1, D2, D3, D4, D5, D6> Distribution<(T1, T2, T3, T4, T5, T6)>
    for (D1, D2, D3, D4, D5, D6)
where
    D1: Distribution<T1>,
    D2: Distribution<T2>,
    D3: Distribution<T3>,
    D4: Distribution<T4>,
    D5: Distribution<T5>,
    D6: Distribution<T6>,
{
    type Iter = TupleIter6<D1::Iter, D2::Iter, D3::Iter, D4::Iter, D5::Iter, D6::Iter>;

    fn sample(&mut self) -> Self::Iter {
        TupleIter6(
            self.0.sample(),
            self.1.sample(),
            self.2.sample(),
            self.3.sample(),
            self.4.sample(),
            self.5.sample(),
        )
    }
}

/// Iterator for 7-tuple distributions.
#[derive(Clone)]
pub struct TupleIter7<I1, I2, I3, I4, I5, I6, I7>(
    pub I1,
    pub I2,
    pub I3,
    pub I4,
    pub I5,
    pub I6,
    pub I7,
);

impl<
    I1: SampleIter,
    I2: SampleIter,
    I3: SampleIter,
    I4: SampleIter,
    I5: SampleIter,
    I6: SampleIter,
    I7: SampleIter,
> SampleIter for TupleIter7<I1, I2, I3, I4, I5, I6, I7>
{
    type Item = (
        I1::Item,
        I2::Item,
        I3::Item,
        I4::Item,
        I5::Item,
        I6::Item,
        I7::Item,
    );

    fn next(&mut self, rng: &mut impl Rng) -> Self::Item {
        (
            self.0.next(rng),
            self.1.next(rng),
            self.2.next(rng),
            self.3.next(rng),
            self.4.next(rng),
            self.5.next(rng),
            self.6.next(rng),
        )
    }
}

impl<T1, T2, T3, T4, T5, T6, T7, D1, D2, D3, D4, D5, D6, D7>
    Distribution<(T1, T2, T3, T4, T5, T6, T7)> for (D1, D2, D3, D4, D5, D6, D7)
where
    D1: Distribution<T1>,
    D2: Distribution<T2>,
    D3: Distribution<T3>,
    D4: Distribution<T4>,
    D5: Distribution<T5>,
    D6: Distribution<T6>,
    D7: Distribution<T7>,
{
    type Iter = TupleIter7<D1::Iter, D2::Iter, D3::Iter, D4::Iter, D5::Iter, D6::Iter, D7::Iter>;

    fn sample(&mut self) -> Self::Iter {
        TupleIter7(
            self.0.sample(),
            self.1.sample(),
            self.2.sample(),
            self.3.sample(),
            self.4.sample(),
            self.5.sample(),
            self.6.sample(),
        )
    }
}

// === Unnest marker for tuple spreading ===

/// Marker wrapper that tells tuple combinators to spread the inner distribution's output.
///
/// When used in a tuple like `(D1, Unnest<D2>)` where D2 produces (B, C),
/// the result is flattened to (A, B, C) instead of (A, (B, C)).
pub struct Unnest<D>(pub D);

/// Convenience function to create an Unnest marker.
pub fn unnest<D>(inner: D) -> Unnest<D> {
    Unnest(inner)
}

/// Iterator for (A, Unnest<(B, C)>) -> (A, B, C).
#[derive(Clone)]
pub struct TupleUnnest2Iter<I1, I2>(pub I1, pub I2);

impl<I1, I2, A, B, C> SampleIter for TupleUnnest2Iter<I1, I2>
where
    I1: SampleIter<Item = A>,
    I2: SampleIter<Item = (B, C)>,
{
    type Item = (A, B, C);

    fn next(&mut self, rng: &mut impl Rng) -> (A, B, C) {
        let a = self.0.next(rng);
        let (b, c) = self.1.next(rng);
        (a, b, c)
    }
}

impl<D1, D2, A, B, C> Distribution<(A, B, C)> for (D1, Unnest<D2>)
where
    D1: Distribution<A>,
    D2: Distribution<(B, C)>,
{
    type Iter = TupleUnnest2Iter<D1::Iter, D2::Iter>;

    fn sample(&mut self) -> Self::Iter {
        TupleUnnest2Iter(self.0.sample(), self.1.0.sample())
    }
}

/// Iterator for 7-tuple with unnest at position 5: (A, B, C, D, E, Unnest<(F, G)>, H) -> 8-tuple
#[derive(Clone)]
pub struct TupleUnnest7At5Iter<I1, I2, I3, I4, I5, I6, I7>(
    pub I1,
    pub I2,
    pub I3,
    pub I4,
    pub I5,
    pub I6,
    pub I7,
);

impl<I1, I2, I3, I4, I5, I6, I7, T1, T2, T3, T4, T5, T6, T7, T8> SampleIter
    for TupleUnnest7At5Iter<I1, I2, I3, I4, I5, I6, I7>
where
    I1: SampleIter<Item = T1>,
    I2: SampleIter<Item = T2>,
    I3: SampleIter<Item = T3>,
    I4: SampleIter<Item = T4>,
    I5: SampleIter<Item = T5>,
    I6: SampleIter<Item = (T6, T7)>,
    I7: SampleIter<Item = T8>,
{
    type Item = (T1, T2, T3, T4, T5, T6, T7, T8);

    fn next(&mut self, rng: &mut impl Rng) -> Self::Item {
        let t1 = self.0.next(rng);
        let t2 = self.1.next(rng);
        let t3 = self.2.next(rng);
        let t4 = self.3.next(rng);
        let t5 = self.4.next(rng);
        let (t6, t7) = self.5.next(rng);
        let t8 = self.6.next(rng);
        (t1, t2, t3, t4, t5, t6, t7, t8)
    }
}

impl<D1, D2, D3, D4, D5, D6, D7, T1, T2, T3, T4, T5, T6, T7, T8>
    Distribution<(T1, T2, T3, T4, T5, T6, T7, T8)> for (D1, D2, D3, D4, D5, Unnest<D6>, D7)
where
    D1: Distribution<T1>,
    D2: Distribution<T2>,
    D3: Distribution<T3>,
    D4: Distribution<T4>,
    D5: Distribution<T5>,
    D6: Distribution<(T6, T7)>,
    D7: Distribution<T8>,
{
    type Iter =
        TupleUnnest7At5Iter<D1::Iter, D2::Iter, D3::Iter, D4::Iter, D5::Iter, D6::Iter, D7::Iter>;

    fn sample(&mut self) -> Self::Iter {
        TupleUnnest7At5Iter(
            self.0.sample(),
            self.1.sample(),
            self.2.sample(),
            self.3.sample(),
            self.4.sample(),
            self.5.0.sample(),
            self.6.sample(),
        )
    }
}

// === IntoColumnIters trait for iterator-level column splitting ===

/// Trait for iterators that can be split into independent column iterators.
/// This enables column-by-column collection with proper execution order.
///
/// - Independent columns: fully splits, each column generated separately
/// - Coupled columns (e.g., from Map): stays as tuple-producing iterator
pub trait IntoColumnIters: SampleIter {
    type Columns;
    fn into_columns(self) -> Self::Columns;
}

// TupleIter2 splits into 2 independent column iterators
impl<I1: SampleIter, I2: SampleIter> IntoColumnIters for TupleIter2<I1, I2> {
    type Columns = (I1, I2);

    fn into_columns(self) -> (I1, I2) {
        (self.0, self.1)
    }
}

// TupleIter3 splits into 3 independent column iterators
impl<I1: SampleIter, I2: SampleIter, I3: SampleIter> IntoColumnIters for TupleIter3<I1, I2, I3> {
    type Columns = (I1, I2, I3);

    fn into_columns(self) -> (I1, I2, I3) {
        (self.0, self.1, self.2)
    }
}

// TupleIter4 splits into 4 independent column iterators
impl<I1: SampleIter, I2: SampleIter, I3: SampleIter, I4: SampleIter> IntoColumnIters
    for TupleIter4<I1, I2, I3, I4>
{
    type Columns = (I1, I2, I3, I4);

    fn into_columns(self) -> (I1, I2, I3, I4) {
        (self.0, self.1, self.2, self.3)
    }
}

// TupleUnnest2Iter: when inner is TupleIter2, splits into 3 columns
impl<I1, I2, I3> IntoColumnIters for TupleUnnest2Iter<I1, TupleIter2<I2, I3>>
where
    I1: SampleIter,
    I2: SampleIter,
    I3: SampleIter,
{
    type Columns = (I1, I2, I3);

    fn into_columns(self) -> (I1, I2, I3) {
        (self.0, self.1.0, self.1.1)
    }
}

// TupleUnnest2Iter: when inner doesn't split (e.g., MapIter), keeps as (I1, I2)
// This is the default - coupled columns stay together
// Note: More specific impl above takes precedence for TupleIter2

// TupleUnnest7At5Iter: splits into 7 column groups
// Positions 0-4 and 6 are independent, position 5 is coupled (produces tuple)
impl<I1, I2, I3, I4, I5, I6, I7, T6, T7> IntoColumnIters
    for TupleUnnest7At5Iter<I1, I2, I3, I4, I5, I6, I7>
where
    I1: SampleIter,
    I2: SampleIter,
    I3: SampleIter,
    I4: SampleIter,
    I5: SampleIter,
    I6: SampleIter<Item = (T6, T7)>, // Coupled - produces (T6, T7)
    I7: SampleIter,
{
    type Columns = (I1, I2, I3, I4, I5, I6, I7);

    fn into_columns(self) -> Self::Columns {
        (self.0, self.1, self.2, self.3, self.4, self.5, self.6)
    }
}

// === CollectIntoSinks: compile-time structure drives runtime sink collection ===

/// Trait for iterators that can collect directly into sink tuples.
/// The generic type structure determines:
/// - How many sinks are created
/// - Which columns are collected column-by-column (independent)
/// - Which columns are collected row-by-row and unzipped (coupled)
///
/// This avoids intermediate tuple allocations for coupled columns.
pub trait CollectIntoSinks: SampleIter + Sized {
    /// The sink types - typically a tuple of Vec<T> for each output column
    type Sinks;

    /// Create sinks with the given capacity
    fn create_sinks(capacity: usize) -> Self::Sinks;

    /// Collect n elements into the sinks
    fn collect_into_sinks(self, rng: &mut impl Rng, n: usize, sinks: &mut Self::Sinks);

    /// Convenience: create sinks and collect in one call
    fn collect_to_vecs(self, rng: &mut impl Rng, n: usize) -> Self::Sinks {
        let mut sinks = Self::create_sinks(n);
        self.collect_into_sinks(rng, n, &mut sinks);
        sinks
    }
}

// TupleIter2: 2 independent columns -> 2 Vecs, column-by-column
impl<I1, I2> CollectIntoSinks for TupleIter2<I1, I2>
where
    I1: SampleIter,
    I2: SampleIter,
{
    type Sinks = (Vec<I1::Item>, Vec<I2::Item>);

    fn create_sinks(capacity: usize) -> Self::Sinks {
        (Vec::with_capacity(capacity), Vec::with_capacity(capacity))
    }

    fn collect_into_sinks(self, rng: &mut impl Rng, n: usize, sinks: &mut Self::Sinks) {
        let (mut c1, mut c2) = self.into_columns();
        sinks.0.extend((0..n).map(|_| c1.next(rng)));
        sinks.1.extend((0..n).map(|_| c2.next(rng)));
    }
}

// TupleIter3: 3 independent columns -> 3 Vecs, column-by-column
impl<I1, I2, I3> CollectIntoSinks for TupleIter3<I1, I2, I3>
where
    I1: SampleIter,
    I2: SampleIter,
    I3: SampleIter,
{
    type Sinks = (Vec<I1::Item>, Vec<I2::Item>, Vec<I3::Item>);

    fn create_sinks(capacity: usize) -> Self::Sinks {
        (
            Vec::with_capacity(capacity),
            Vec::with_capacity(capacity),
            Vec::with_capacity(capacity),
        )
    }

    fn collect_into_sinks(self, rng: &mut impl Rng, n: usize, sinks: &mut Self::Sinks) {
        let (mut c1, mut c2, mut c3) = self.into_columns();
        sinks.0.extend((0..n).map(|_| c1.next(rng)));
        sinks.1.extend((0..n).map(|_| c2.next(rng)));
        sinks.2.extend((0..n).map(|_| c3.next(rng)));
    }
}

// TupleIter4: 4 independent columns -> 4 Vecs, column-by-column
impl<I1, I2, I3, I4> CollectIntoSinks for TupleIter4<I1, I2, I3, I4>
where
    I1: SampleIter,
    I2: SampleIter,
    I3: SampleIter,
    I4: SampleIter,
{
    type Sinks = (Vec<I1::Item>, Vec<I2::Item>, Vec<I3::Item>, Vec<I4::Item>);

    fn create_sinks(capacity: usize) -> Self::Sinks {
        (
            Vec::with_capacity(capacity),
            Vec::with_capacity(capacity),
            Vec::with_capacity(capacity),
            Vec::with_capacity(capacity),
        )
    }

    fn collect_into_sinks(self, rng: &mut impl Rng, n: usize, sinks: &mut Self::Sinks) {
        let (mut c1, mut c2, mut c3, mut c4) = self.into_columns();
        sinks.0.extend((0..n).map(|_| c1.next(rng)));
        sinks.1.extend((0..n).map(|_| c2.next(rng)));
        sinks.2.extend((0..n).map(|_| c3.next(rng)));
        sinks.3.extend((0..n).map(|_| c4.next(rng)));
    }
}

// TupleUnnest2Iter with TupleIter2 inner: 3 independent columns -> 3 Vecs
impl<I1, I2, I3> CollectIntoSinks for TupleUnnest2Iter<I1, TupleIter2<I2, I3>>
where
    I1: SampleIter,
    I2: SampleIter,
    I3: SampleIter,
{
    type Sinks = (Vec<I1::Item>, Vec<I2::Item>, Vec<I3::Item>);

    fn create_sinks(capacity: usize) -> Self::Sinks {
        (
            Vec::with_capacity(capacity),
            Vec::with_capacity(capacity),
            Vec::with_capacity(capacity),
        )
    }

    fn collect_into_sinks(self, rng: &mut impl Rng, n: usize, sinks: &mut Self::Sinks) {
        let (mut c1, mut c2, mut c3) = self.into_columns();
        sinks.0.extend((0..n).map(|_| c1.next(rng)));
        sinks.1.extend((0..n).map(|_| c2.next(rng)));
        sinks.2.extend((0..n).map(|_| c3.next(rng)));
    }
}

// TupleUnnest7At5Iter: 6 independent + 1 coupled pair -> 8 Vecs
// Coupled pair at position 5 is unzipped directly into 2 separate Vecs (no tuple allocation)
impl<I1, I2, I3, I4, I5, I6, I7, T6, T7> CollectIntoSinks
    for TupleUnnest7At5Iter<I1, I2, I3, I4, I5, I6, I7>
where
    I1: SampleIter,
    I2: SampleIter,
    I3: SampleIter,
    I4: SampleIter,
    I5: SampleIter,
    I6: SampleIter<Item = (T6, T7)>,
    I7: SampleIter,
{
    type Sinks = (
        Vec<I1::Item>,
        Vec<I2::Item>,
        Vec<I3::Item>,
        Vec<I4::Item>,
        Vec<I5::Item>,
        Vec<T6>,
        Vec<T7>,
        Vec<I7::Item>,
    );

    fn create_sinks(capacity: usize) -> Self::Sinks {
        (
            Vec::with_capacity(capacity),
            Vec::with_capacity(capacity),
            Vec::with_capacity(capacity),
            Vec::with_capacity(capacity),
            Vec::with_capacity(capacity),
            Vec::with_capacity(capacity),
            Vec::with_capacity(capacity),
            Vec::with_capacity(capacity),
        )
    }

    fn collect_into_sinks(self, rng: &mut impl Rng, n: usize, sinks: &mut Self::Sinks) {
        let (mut c1, mut c2, mut c3, mut c4, mut c5, mut c6, mut c7) = self.into_columns();

        // Independent columns: column-by-column
        sinks.0.extend((0..n).map(|_| c1.next(rng)));
        sinks.1.extend((0..n).map(|_| c2.next(rng)));
        sinks.2.extend((0..n).map(|_| c3.next(rng)));
        sinks.3.extend((0..n).map(|_| c4.next(rng)));
        sinks.4.extend((0..n).map(|_| c5.next(rng)));

        // Coupled column: row-by-row, unzip directly into separate vecs
        // No intermediate (T6, T7) tuple stored - destructured immediately
        for _ in 0..n {
            let (a, b) = c6.next(rng);
            sinks.5.push(a);
            sinks.6.push(b);
        }

        // Last independent column
        sinks.7.extend((0..n).map(|_| c7.next(rng)));
    }
}

// === Collection functions using Sink ===

/// Collect elements into a sink (row-oriented).
pub fn collect_into<I, S>(mut iter: I, rng: &mut impl Rng, n: usize, mut sink: S) -> S::Output
where
    I: SampleIter,
    S: Sink<I::Item>,
{
    for _ in 0..n {
        sink.push(iter.next(rng));
    }
    sink.finish()
}

/// Collect elements using column-oriented bulk extend.
pub fn collect_into_column<I, S>(
    mut iter: I,
    rng: &mut impl Rng,
    n: usize,
    mut sink: S,
) -> S::Output
where
    I: SampleIter,
    S: Sink<I::Item>,
{
    sink.extend((0..n).map(|_| iter.next(rng)));
    sink.finish()
}

/// Collect 2-tuple into separate sinks (column-oriented).
pub fn collect_struct2_into<I1, I2, S1, S2>(
    mut iter: TupleIter2<I1, I2>,
    rng: &mut impl Rng,
    n: usize,
    mut sink1: S1,
    mut sink2: S2,
) -> (S1::Output, S2::Output)
where
    I1: SampleIter,
    I2: SampleIter,
    S1: Sink<I1::Item>,
    S2: Sink<I2::Item>,
{
    sink1.extend((0..n).map(|_| iter.0.next(rng)));
    sink2.extend((0..n).map(|_| iter.1.next(rng)));
    (sink1.finish(), sink2.finish())
}

/// Collect 3-tuple into separate sinks (column-oriented).
pub fn collect_struct3_into<I1, I2, I3, S1, S2, S3>(
    mut iter: TupleIter3<I1, I2, I3>,
    rng: &mut impl Rng,
    n: usize,
    mut sink1: S1,
    mut sink2: S2,
    mut sink3: S3,
) -> (S1::Output, S2::Output, S3::Output)
where
    I1: SampleIter,
    I2: SampleIter,
    I3: SampleIter,
    S1: Sink<I1::Item>,
    S2: Sink<I2::Item>,
    S3: Sink<I3::Item>,
{
    sink1.extend((0..n).map(|_| iter.0.next(rng)));
    sink2.extend((0..n).map(|_| iter.1.next(rng)));
    sink3.extend((0..n).map(|_| iter.2.next(rng)));
    (sink1.finish(), sink2.finish(), sink3.finish())
}

/// Collect list into a list sink.
pub fn collect_list_into<L, E, S>(
    mut lengths: L,
    elements: E,
    rng: &mut impl Rng,
    n: usize,
    mut sink: S,
) -> S::Output
where
    L: SampleIter<Item = usize>,
    E: SampleIter,
    S: ListSink<E::Item>,
{
    for _ in 0..n {
        sink.start_list();
        let mut elem_iter = elements.clone();
        let len = lengths.next(rng);
        for _ in 0..len {
            sink.push_element(elem_iter.next(rng));
        }
        sink.end_list();
    }
    sink.finish()
}

// === StructCollect trait for struct-level collection ===

/// Trait for output structs that can be collected from a distribution.
/// The derive macro generates this, hiding iteration order inside.
pub trait StructCollect: Sized {
    type Dist;

    /// Collect n instances from the distribution with optimal iteration order.
    /// The implementation (generated by derive) handles:
    /// - Splitting into column iterators via IntoColumnIters
    /// - Iterating independent columns first (cache-local)
    /// - Iterating coupled columns together (row-by-row within groups)
    /// - Mapping results to struct fields
    fn collect_from(dist: &mut Self::Dist, rng: &mut impl Rng, n: usize) -> Vec<Self>;
}

/// Extension trait to add collect_struct method to distributions.
pub trait CollectStructExt<S: StructCollect<Dist = Self>>: Sized {
    fn collect_struct(&mut self, rng: &mut impl Rng, n: usize) -> Vec<S> {
        S::collect_from(self, rng, n)
    }
}

impl<D, S: StructCollect<Dist = D>> CollectStructExt<S> for D {}

// === ColumnIters traits for column-by-column collection ===

/// Trait for distributions that can produce individual column iterators (2 columns).
/// This enables column-by-column collection for both flat tuples and unnested tuples.
pub trait ColumnIters2<T1, T2> {
    type I1: SampleIter<Item = T1>;
    type I2: SampleIter<Item = T2>;

    fn column_iters(&mut self) -> (Self::I1, Self::I2);
}

/// Trait for distributions that can produce individual column iterators (3 columns).
pub trait ColumnIters3<T1, T2, T3> {
    type I1: SampleIter<Item = T1>;
    type I2: SampleIter<Item = T2>;
    type I3: SampleIter<Item = T3>;

    fn column_iters(&mut self) -> (Self::I1, Self::I2, Self::I3);
}

// Implement ColumnIters2 for flat 2-tuple
impl<T1, T2, D1, D2> ColumnIters2<T1, T2> for (D1, D2)
where
    D1: Distribution<T1>,
    D2: Distribution<T2>,
{
    type I1 = D1::Iter;
    type I2 = D2::Iter;

    fn column_iters(&mut self) -> (Self::I1, Self::I2) {
        (self.0.sample(), self.1.sample())
    }
}

// Implement ColumnIters3 for flat 3-tuple
impl<T1, T2, T3, D1, D2, D3> ColumnIters3<T1, T2, T3> for (D1, D2, D3)
where
    D1: Distribution<T1>,
    D2: Distribution<T2>,
    D3: Distribution<T3>,
{
    type I1 = D1::Iter;
    type I2 = D2::Iter;
    type I3 = D3::Iter;

    fn column_iters(&mut self) -> (Self::I1, Self::I2, Self::I3) {
        (self.0.sample(), self.1.sample(), self.2.sample())
    }
}

// Implement ColumnIters3 for (D1, Unnest<(D2, D3)>)
impl<T1, T2, T3, D1, D2, D3> ColumnIters3<T1, T2, T3> for (D1, Unnest<(D2, D3)>)
where
    D1: Distribution<T1>,
    D2: Distribution<T2>,
    D3: Distribution<T3>,
{
    type I1 = D1::Iter;
    type I2 = D2::Iter;
    type I3 = D3::Iter;

    fn column_iters(&mut self) -> (Self::I1, Self::I2, Self::I3) {
        (
            self.0.sample(),
            (self.1).0.0.sample(),
            (self.1).0.1.sample(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::rc::Rc;

    /// A tracking distribution that records when values are generated.
    /// Used to verify execution order.
    #[derive(Clone)]
    struct TrackingDist {
        id: &'static str,
        log: Rc<RefCell<Vec<String>>>,
    }

    impl TrackingDist {
        fn new(id: &'static str, log: Rc<RefCell<Vec<String>>>) -> Self {
            Self { id, log }
        }
    }

    #[derive(Clone)]
    struct TrackingIter {
        id: &'static str,
        counter: usize,
        log: Rc<RefCell<Vec<String>>>,
    }

    impl SampleIter for TrackingIter {
        type Item = usize;

        fn next(&mut self, _rng: &mut impl Rng) -> usize {
            let value = self.counter;
            self.log.borrow_mut().push(format!("{}:{}", self.id, value));
            self.counter += 1;
            value
        }
    }

    impl Distribution<usize> for TrackingDist {
        type Iter = TrackingIter;

        fn sample(&mut self) -> TrackingIter {
            TrackingIter {
                id: self.id,
                counter: 0,
                log: self.log.clone(),
            }
        }
    }

    #[test]
    fn test_into_column_iters_execution_order_2_tuple() {
        let log = Rc::new(RefCell::new(Vec::new()));
        let mut rng = rand::rng();

        // Create a 2-tuple distribution
        let mut dist = (
            TrackingDist::new("A", log.clone()),
            TrackingDist::new("B", log.clone()),
        );

        // Get iterator and split into columns
        let iter = dist.sample();
        let (mut col_a, mut col_b) = iter.into_columns();

        // Generate column A entirely first
        for _ in 0..3 {
            col_a.next(&mut rng);
        }
        // Then generate column B entirely
        for _ in 0..3 {
            col_b.next(&mut rng);
        }

        let log = log.borrow();
        assert_eq!(
            *log,
            vec!["A:0", "A:1", "A:2", "B:0", "B:1", "B:2"],
            "Expected column-by-column order: all A, then all B"
        );
    }

    #[test]
    fn test_into_column_iters_execution_order_3_tuple() {
        let log = Rc::new(RefCell::new(Vec::new()));
        let mut rng = rand::rng();

        // Create a 3-tuple distribution
        let mut dist = (
            TrackingDist::new("A", log.clone()),
            TrackingDist::new("B", log.clone()),
            TrackingDist::new("C", log.clone()),
        );

        // Get iterator and split into columns
        let iter = dist.sample();
        let (mut col_a, mut col_b, mut col_c) = iter.into_columns();

        // Generate all columns in order
        for _ in 0..2 {
            col_a.next(&mut rng);
        }
        for _ in 0..2 {
            col_b.next(&mut rng);
        }
        for _ in 0..2 {
            col_c.next(&mut rng);
        }

        let log = log.borrow();
        assert_eq!(
            *log,
            vec!["A:0", "A:1", "B:0", "B:1", "C:0", "C:1"],
            "Expected column-by-column order: all A, then all B, then all C"
        );
    }

    #[test]
    fn test_into_column_iters_unnest_splits_inner() {
        let log = Rc::new(RefCell::new(Vec::new()));
        let mut rng = rand::rng();

        // Create (D1, Unnest<(D2, D3)>) distribution
        let mut dist = (
            TrackingDist::new("A", log.clone()),
            Unnest((
                TrackingDist::new("B", log.clone()),
                TrackingDist::new("C", log.clone()),
            )),
        );

        // Get iterator - it's TupleUnnest2Iter<TrackingIter, TupleIter2<TrackingIter, TrackingIter>>
        let iter = dist.sample();

        // Split into 3 columns (because inner is TupleIter2)
        let (mut col_a, mut col_b, mut col_c) = iter.into_columns();

        // Generate all columns in order
        for _ in 0..2 {
            col_a.next(&mut rng);
        }
        for _ in 0..2 {
            col_b.next(&mut rng);
        }
        for _ in 0..2 {
            col_c.next(&mut rng);
        }

        let log = log.borrow();
        assert_eq!(
            *log,
            vec!["A:0", "A:1", "B:0", "B:1", "C:0", "C:1"],
            "Unnest should split inner tuple: all A, then all B, then all C"
        );
    }

    #[test]
    fn test_row_by_row_vs_column_by_column() {
        let log_row = Rc::new(RefCell::new(Vec::new()));
        let log_col = Rc::new(RefCell::new(Vec::new()));
        let mut rng = rand::rng();

        // Row-by-row: interleaved access pattern
        {
            let mut dist = (
                TrackingDist::new("A", log_row.clone()),
                TrackingDist::new("B", log_row.clone()),
            );
            let mut iter = dist.sample();
            for _ in 0..3 {
                iter.next(&mut rng); // produces (A, B) tuple
            }
        }

        // Column-by-column: sequential access pattern
        {
            let mut dist = (
                TrackingDist::new("A", log_col.clone()),
                TrackingDist::new("B", log_col.clone()),
            );
            let iter = dist.sample();
            let (mut col_a, mut col_b) = iter.into_columns();
            for _ in 0..3 {
                col_a.next(&mut rng);
            }
            for _ in 0..3 {
                col_b.next(&mut rng);
            }
        }

        assert_eq!(
            *log_row.borrow(),
            vec!["A:0", "B:0", "A:1", "B:1", "A:2", "B:2"],
            "Row-by-row: interleaved A,B,A,B,A,B"
        );
        assert_eq!(
            *log_col.borrow(),
            vec!["A:0", "A:1", "A:2", "B:0", "B:1", "B:2"],
            "Column-by-column: sequential A,A,A,B,B,B"
        );
    }

    /// A "Map-like" coupled distribution that produces tuples atomically.
    /// Simulates Map<D, F> where F: T -> (U1, U2)
    /// Cannot be split into independent column iterators.
    #[derive(Clone)]
    struct CoupledDist {
        id1: &'static str,
        id2: &'static str,
        log: Rc<RefCell<Vec<String>>>,
    }

    impl CoupledDist {
        fn new(id1: &'static str, id2: &'static str, log: Rc<RefCell<Vec<String>>>) -> Self {
            Self { id1, id2, log }
        }
    }

    /// Iterator for CoupledDist - produces (usize, usize) tuples atomically.
    /// Both values are generated together (like Map output).
    #[derive(Clone)]
    struct CoupledIter {
        id1: &'static str,
        id2: &'static str,
        counter: usize,
        log: Rc<RefCell<Vec<String>>>,
    }

    impl SampleIter for CoupledIter {
        type Item = (usize, usize);

        fn next(&mut self, _rng: &mut impl Rng) -> (usize, usize) {
            let v1 = self.counter;
            let v2 = self.counter * 10;
            // Log both atomically - they're coupled
            self.log
                .borrow_mut()
                .push(format!("({},{}):({},{})", self.id1, self.id2, v1, v2));
            self.counter += 1;
            (v1, v2)
        }
    }

    impl Distribution<(usize, usize)> for CoupledDist {
        type Iter = CoupledIter;

        fn sample(&mut self) -> CoupledIter {
            CoupledIter {
                id1: self.id1,
                id2: self.id2,
                counter: 0,
                log: self.log.clone(),
            }
        }
    }

    /// Test struct-like scenario:
    /// struct Order { id: i32, customer_id: i32, customer_name: String }
    /// Where id is independent, but customer_id and customer_name come from same Map.
    #[test]
    fn test_struct_with_independent_and_coupled_columns() {
        let log = Rc::new(RefCell::new(Vec::new()));
        let mut rng = rand::rng();

        // Simulates: (id_dist, customer_map) where customer_map produces (customer_id, customer_name)
        let mut dist = (
            TrackingDist::new("id", log.clone()),
            CoupledDist::new("cust_id", "cust_name", log.clone()),
        );

        // Get iterator: TupleIter2<TrackingIter, CoupledIter>
        let iter = dist.sample();

        // Split into columns - but CoupledIter stays together!
        // Returns (TrackingIter, CoupledIter) - NOT (TrackingIter, Iter1, Iter2)
        let (mut col_id, mut col_customer) = iter.into_columns();

        // Generate independent column entirely first (cache-local)
        for _ in 0..3 {
            col_id.next(&mut rng);
        }

        // Then generate coupled column - values come out together row-by-row
        for _ in 0..3 {
            let (_cust_id, _cust_name) = col_customer.next(&mut rng);
        }

        let log = log.borrow();
        // Expected: all id values first, then all coupled (cust_id, cust_name) together
        assert_eq!(
            *log,
            vec![
                "id:0",
                "id:1",
                "id:2",
                "(cust_id,cust_name):(0,0)",
                "(cust_id,cust_name):(1,10)",
                "(cust_id,cust_name):(2,20)",
            ],
            "Independent column first, then coupled columns together"
        );
    }

    /// Test that a sink can unzip coupled columns after independent ones are done.
    /// This simulates the full collection pattern for a struct.
    #[test]
    fn test_struct_collection_with_unzip() {
        let log = Rc::new(RefCell::new(Vec::new()));
        let mut rng = rand::rng();

        let mut dist = (
            TrackingDist::new("id", log.clone()),
            CoupledDist::new("cust_id", "cust_name", log.clone()),
        );

        let iter = dist.sample();
        let (mut col_id, mut col_customer) = iter.into_columns();

        // Collect into separate output columns
        let mut ids: Vec<usize> = Vec::new();
        let mut cust_ids: Vec<usize> = Vec::new();
        let mut cust_names: Vec<usize> = Vec::new();

        // Fill independent column (cache-local)
        for _ in 0..3 {
            ids.push(col_id.next(&mut rng));
        }

        // Fill coupled columns - unzip as we go
        for _ in 0..3 {
            let (cid, cname) = col_customer.next(&mut rng);
            cust_ids.push(cid);
            cust_names.push(cname);
        }

        // Verify output
        assert_eq!(ids, vec![0, 1, 2]);
        assert_eq!(cust_ids, vec![0, 1, 2]);
        assert_eq!(cust_names, vec![0, 10, 20]);

        // Verify execution order: all independent, then all coupled
        let log = log.borrow();
        assert_eq!(log.len(), 6); // 3 id + 3 coupled
        assert!(log[0].starts_with("id:"));
        assert!(log[1].starts_with("id:"));
        assert!(log[2].starts_with("id:"));
        assert!(log[3].starts_with("(cust_id,cust_name)"));
        assert!(log[4].starts_with("(cust_id,cust_name)"));
        assert!(log[5].starts_with("(cust_id,cust_name)"));
    }

    /// Test that OUTPUT struct field order doesn't affect GENERATION order.
    /// The struct is just the target shape - iteration order is determined by source coupling.
    #[test]
    fn test_struct_field_order_independent_of_generation() {
        let log = Rc::new(RefCell::new(Vec::new()));
        let mut rng = rand::rng();

        // Distribution sources - this determines coupling
        let mut dist = (
            TrackingDist::new("id", log.clone()),
            CoupledDist::new("cust_id", "cust_name", log.clone()),
        );

        let iter = dist.sample();
        let (mut col_id, mut col_customer) = iter.into_columns();

        // Output struct with DIFFERENT field order than sources:
        // struct Order { customer_name, id, customer_id }
        // But we still generate optimally based on SOURCE coupling
        struct Order {
            customer_name: usize,
            id: usize,
            customer_id: usize,
        }

        let mut orders: Vec<Order> = Vec::new();

        // Collect into intermediate buffers with optimal generation order
        let mut ids = Vec::new();
        let mut cust_ids = Vec::new();
        let mut cust_names = Vec::new();

        // Fill independent first (cache-local)
        for _ in 0..3 {
            ids.push(col_id.next(&mut rng));
        }

        // Fill coupled together (row-by-row within group)
        for _ in 0..3 {
            let (cid, cname) = col_customer.next(&mut rng);
            cust_ids.push(cid);
            cust_names.push(cname);
        }

        // Now assemble structs - order matches struct definition, not generation
        for i in 0..3 {
            orders.push(Order {
                customer_name: cust_names[i],
                id: ids[i],
                customer_id: cust_ids[i],
            });
        }

        // Verify structs are correct
        assert_eq!(orders[0].id, 0);
        assert_eq!(orders[0].customer_id, 0);
        assert_eq!(orders[0].customer_name, 0);
        assert_eq!(orders[2].id, 2);
        assert_eq!(orders[2].customer_id, 2);
        assert_eq!(orders[2].customer_name, 20);

        // Verify generation order was optimal (all ind, then all coupled)
        let log = log.borrow();
        assert_eq!(
            *log,
            vec![
                "id:0",
                "id:1",
                "id:2",
                "(cust_id,cust_name):(0,0)",
                "(cust_id,cust_name):(1,10)",
                "(cust_id,cust_name):(2,20)",
            ]
        );
    }

    /// Test StructCollect trait - struct hides iteration order completely.
    /// This is what the derive macro would generate.
    #[test]
    fn test_struct_collect_hides_order() {
        let log = Rc::new(RefCell::new(Vec::new()));
        let mut rng = rand::rng();

        // === Output struct - user just defines fields, any order ===
        #[derive(Debug, PartialEq)]
        struct Order {
            customer_name: usize, // Field order doesn't matter
            id: usize,
            customer_id: usize,
        }

        // === Distribution type (would be derived or hand-written) ===
        type OrderDist = (TrackingDist, CoupledDist);

        // === StructCollect impl (generated by derive macro) ===
        // This hides all the iteration order logic from the user
        impl StructCollect for Order {
            type Dist = OrderDist;

            fn collect_from(dist: &mut Self::Dist, rng: &mut impl Rng, n: usize) -> Vec<Self> {
                // Get iterator and split into columns
                let iter = dist.sample();
                let (mut col_id, mut col_customer) = iter.into_columns();

                // === Iteration order is hidden here ===
                // Independent columns first (cache-local)
                let ids: Vec<_> = (0..n).map(|_| col_id.next(rng)).collect();

                // Coupled columns together
                let customers: Vec<_> = (0..n).map(|_| col_customer.next(rng)).collect();

                // Map to output struct (field order from struct definition)
                (0..n)
                    .map(|i| Order {
                        customer_name: customers[i].1,
                        id: ids[i],
                        customer_id: customers[i].0,
                    })
                    .collect()
            }
        }

        // === User code - clean API, order is hidden ===
        let mut dist = (
            TrackingDist::new("id", log.clone()),
            CoupledDist::new("cust_id", "cust_name", log.clone()),
        );

        // Just call collect_struct - no manual iteration
        let orders = Order::collect_from(&mut dist, &mut rng, 3);
        // Or equivalently: let orders: Vec<Order> = dist.collect_struct(&mut rng, 3);

        // Verify output
        assert_eq!(orders.len(), 3);
        assert_eq!(
            orders[0],
            Order {
                customer_name: 0,
                id: 0,
                customer_id: 0
            }
        );
        assert_eq!(
            orders[2],
            Order {
                customer_name: 20,
                id: 2,
                customer_id: 2
            }
        );

        // Verify optimal generation order happened internally
        let log = log.borrow();
        assert_eq!(
            *log,
            vec![
                "id:0",
                "id:1",
                "id:2", // All independent first
                "(cust_id,cust_name):(0,0)",
                "(cust_id,cust_name):(1,10)",
                "(cust_id,cust_name):(2,20)", // Then coupled
            ]
        );
    }

    /// Print execution order to stdout for manual verification.
    /// Run with: cargo test -p populate-core print_execution_order -- --nocapture
    #[test]
    fn print_execution_order_validation() {
        let mut rng = rand::rng();

        println!("\n========================================");
        println!("EXECUTION ORDER VALIDATION");
        println!("========================================\n");

        // Test 1: Flat tuple - all independent
        println!("--- Test 1: Flat tuple (A, B, C) - all independent ---");
        {
            let log = Rc::new(RefCell::new(Vec::new()));
            let mut dist = (
                TrackingDist::new("A", log.clone()),
                TrackingDist::new("B", log.clone()),
                TrackingDist::new("C", log.clone()),
            );
            let iter = dist.sample();
            let (mut a, mut b, mut c) = iter.into_columns();

            for _ in 0..3 {
                a.next(&mut rng);
            }
            for _ in 0..3 {
                b.next(&mut rng);
            }
            for _ in 0..3 {
                c.next(&mut rng);
            }

            println!("Log: {:?}", log.borrow());
            println!("Expected: A:0, A:1, A:2, B:0, B:1, B:2, C:0, C:1, C:2");
            assert_eq!(
                *log.borrow(),
                vec![
                    "A:0", "A:1", "A:2", "B:0", "B:1", "B:2", "C:0", "C:1", "C:2"
                ]
            );
            println!("✓ PASSED\n");
        }

        // Test 2: Unnest/merge - inner tuple splits
        println!("--- Test 2: (A, Unnest<(B, C)>) - merge splits inner ---");
        {
            let log = Rc::new(RefCell::new(Vec::new()));
            let mut dist = (
                TrackingDist::new("A", log.clone()),
                Unnest((
                    TrackingDist::new("B", log.clone()),
                    TrackingDist::new("C", log.clone()),
                )),
            );
            let iter = dist.sample();
            let (mut a, mut b, mut c) = iter.into_columns();

            for _ in 0..3 {
                a.next(&mut rng);
            }
            for _ in 0..3 {
                b.next(&mut rng);
            }
            for _ in 0..3 {
                c.next(&mut rng);
            }

            println!("Log: {:?}", log.borrow());
            println!("Expected: A:0, A:1, A:2, B:0, B:1, B:2, C:0, C:1, C:2");
            assert_eq!(
                *log.borrow(),
                vec![
                    "A:0", "A:1", "A:2", "B:0", "B:1", "B:2", "C:0", "C:1", "C:2"
                ]
            );
            println!("✓ PASSED\n");
        }

        // Test 3: With coupled (Map-like)
        println!("--- Test 3: (A, Coupled<B,C>) - A independent, BC coupled ---");
        {
            let log = Rc::new(RefCell::new(Vec::new()));
            let mut dist = (
                TrackingDist::new("A", log.clone()),
                CoupledDist::new("B", "C", log.clone()),
            );
            let iter = dist.sample();
            let (mut a, mut bc) = iter.into_columns();

            // A filled first (independent)
            for _ in 0..3 {
                a.next(&mut rng);
            }
            // BC filled together (coupled - can't split)
            for _ in 0..3 {
                bc.next(&mut rng);
            }

            println!("Log: {:?}", log.borrow());
            println!("Expected: A:0, A:1, A:2, (B,C):(0,0), (B,C):(1,10), (B,C):(2,20)");
            assert_eq!(
                *log.borrow(),
                vec![
                    "A:0",
                    "A:1",
                    "A:2",
                    "(B,C):(0,0)",
                    "(B,C):(1,10)",
                    "(B,C):(2,20)"
                ]
            );
            println!("✓ PASSED\n");
        }

        // Test 4: Row-by-row comparison
        println!("--- Test 4: Row-by-row vs Column-by-column comparison ---");
        {
            let log_row = Rc::new(RefCell::new(Vec::new()));
            let log_col = Rc::new(RefCell::new(Vec::new()));

            // Row-by-row (traditional)
            let mut dist = (
                TrackingDist::new("A", log_row.clone()),
                TrackingDist::new("B", log_row.clone()),
            );
            let mut iter = dist.sample();
            for _ in 0..3 {
                iter.next(&mut rng);
            }

            // Column-by-column
            let mut dist = (
                TrackingDist::new("A", log_col.clone()),
                TrackingDist::new("B", log_col.clone()),
            );
            let iter = dist.sample();
            let (mut a, mut b) = iter.into_columns();
            for _ in 0..3 {
                a.next(&mut rng);
            }
            for _ in 0..3 {
                b.next(&mut rng);
            }

            println!("Row-by-row:       {:?}", log_row.borrow());
            println!("Column-by-column: {:?}", log_col.borrow());
            println!("Row-by-row interleaves: A,B,A,B,A,B");
            println!("Column-by-column groups: A,A,A,B,B,B");
            assert_eq!(
                *log_row.borrow(),
                vec!["A:0", "B:0", "A:1", "B:1", "A:2", "B:2"]
            );
            assert_eq!(
                *log_col.borrow(),
                vec!["A:0", "A:1", "A:2", "B:0", "B:1", "B:2"]
            );
            println!("✓ PASSED\n");
        }

        println!("========================================");
        println!("ALL VALIDATION TESTS PASSED");
        println!("========================================\n");
    }

    /// Test CollectIntoSinks for TupleIter2 - 2 independent columns
    #[test]
    fn test_collect_into_sinks_tuple2() {
        let log = Rc::new(RefCell::new(Vec::new()));
        let mut rng = rand::rng();

        let mut dist = (
            TrackingDist::new("A", log.clone()),
            TrackingDist::new("B", log.clone()),
        );

        let iter = dist.sample();
        let (col_a, col_b) = iter.collect_to_vecs(&mut rng, 3);

        // Verify output
        assert_eq!(col_a, vec![0, 1, 2]);
        assert_eq!(col_b, vec![0, 1, 2]);

        // Verify column-by-column execution order
        assert_eq!(
            *log.borrow(),
            vec!["A:0", "A:1", "A:2", "B:0", "B:1", "B:2"],
            "CollectIntoSinks should collect column-by-column"
        );
    }

    /// Test CollectIntoSinks for TupleIter3 - 3 independent columns
    #[test]
    fn test_collect_into_sinks_tuple3() {
        let log = Rc::new(RefCell::new(Vec::new()));
        let mut rng = rand::rng();

        let mut dist = (
            TrackingDist::new("A", log.clone()),
            TrackingDist::new("B", log.clone()),
            TrackingDist::new("C", log.clone()),
        );

        let iter = dist.sample();
        let (col_a, col_b, col_c) = iter.collect_to_vecs(&mut rng, 3);

        // Verify output
        assert_eq!(col_a, vec![0, 1, 2]);
        assert_eq!(col_b, vec![0, 1, 2]);
        assert_eq!(col_c, vec![0, 1, 2]);

        // Verify column-by-column execution order
        assert_eq!(
            *log.borrow(),
            vec![
                "A:0", "A:1", "A:2", "B:0", "B:1", "B:2", "C:0", "C:1", "C:2"
            ],
            "CollectIntoSinks should collect column-by-column"
        );
    }

    /// Test CollectIntoSinks with coupled columns - unzips directly into separate vecs
    #[test]
    fn test_collect_into_sinks_with_coupled_unzip() {
        let log = Rc::new(RefCell::new(Vec::new()));
        let mut rng = rand::rng();

        // (independent, coupled_pair) -> should produce 3 vecs
        let mut dist = (
            TrackingDist::new("A", log.clone()),
            CoupledDist::new("B", "C", log.clone()),
        );

        let iter = dist.sample();
        // Note: TupleIter2<TrackingIter, CoupledIter> where CoupledIter: Item=(usize, usize)
        // This collects into (Vec<usize>, Vec<(usize, usize)>)
        let (col_a, col_bc) = iter.collect_to_vecs(&mut rng, 3);

        // Verify output
        assert_eq!(col_a, vec![0, 1, 2]);
        assert_eq!(col_bc, vec![(0, 0), (1, 10), (2, 20)]);

        // Verify column-by-column for independent, row-by-row for coupled
        let log = log.borrow();
        assert_eq!(log[0], "A:0");
        assert_eq!(log[1], "A:1");
        assert_eq!(log[2], "A:2");
        assert!(log[3].starts_with("(B,C)"));
        assert!(log[4].starts_with("(B,C)"));
        assert!(log[5].starts_with("(B,C)"));
    }

    /// Test CollectIntoSinks for TupleUnnest2Iter - inner tuple splits into 3 vecs
    #[test]
    fn test_collect_into_sinks_unnest2() {
        let log = Rc::new(RefCell::new(Vec::new()));
        let mut rng = rand::rng();

        let mut dist = (
            TrackingDist::new("A", log.clone()),
            Unnest((
                TrackingDist::new("B", log.clone()),
                TrackingDist::new("C", log.clone()),
            )),
        );

        let iter = dist.sample();
        let (col_a, col_b, col_c) = iter.collect_to_vecs(&mut rng, 3);

        // Verify output
        assert_eq!(col_a, vec![0, 1, 2]);
        assert_eq!(col_b, vec![0, 1, 2]);
        assert_eq!(col_c, vec![0, 1, 2]);

        // Verify column-by-column execution order
        assert_eq!(
            *log.borrow(),
            vec![
                "A:0", "A:1", "A:2", "B:0", "B:1", "B:2", "C:0", "C:1", "C:2"
            ],
            "CollectIntoSinks with Unnest should split inner and collect column-by-column"
        );
    }

    /// Test CollectIntoSinks for TupleUnnest7At5Iter - 8 vecs with coupled pair unzipped
    #[test]
    fn test_collect_into_sinks_unnest7at5() {
        use crate::TupleUnnest7At5Iter;

        let log = Rc::new(RefCell::new(Vec::new()));
        let mut rng = rand::rng();

        // Create iterators directly to form TupleUnnest7At5Iter
        let iter = TupleUnnest7At5Iter(
            TrackingIter {
                id: "A",
                counter: 0,
                log: log.clone(),
            },
            TrackingIter {
                id: "B",
                counter: 0,
                log: log.clone(),
            },
            TrackingIter {
                id: "C",
                counter: 0,
                log: log.clone(),
            },
            TrackingIter {
                id: "D",
                counter: 0,
                log: log.clone(),
            },
            TrackingIter {
                id: "E",
                counter: 0,
                log: log.clone(),
            },
            // Coupled iterator at position 5 - produces (usize, usize)
            CoupledIter {
                id1: "F",
                id2: "G",
                counter: 0,
                log: log.clone(),
            },
            TrackingIter {
                id: "H",
                counter: 0,
                log: log.clone(),
            },
        );

        // Collect into 8 vecs (coupled pair F,G unzipped into 2 separate vecs)
        let (a, b, c, d, e, f, g, h) = iter.collect_to_vecs(&mut rng, 2);

        // Verify output shapes
        assert_eq!(a, vec![0, 1]);
        assert_eq!(b, vec![0, 1]);
        assert_eq!(c, vec![0, 1]);
        assert_eq!(d, vec![0, 1]);
        assert_eq!(e, vec![0, 1]);
        assert_eq!(f, vec![0, 1]); // First element of coupled pair
        assert_eq!(g, vec![0, 10]); // Second element of coupled pair (counter * 10)
        assert_eq!(h, vec![0, 1]);

        // Verify execution order: independent cols first, then coupled, then last independent
        let log = log.borrow();
        assert_eq!(
            *log,
            vec![
                "A:0",
                "A:1",
                "B:0",
                "B:1",
                "C:0",
                "C:1",
                "D:0",
                "D:1",
                "E:0",
                "E:1",
                "(F,G):(0,0)",
                "(F,G):(1,10)", // Coupled pair generated row-by-row
                "H:0",
                "H:1",
            ],
            "TupleUnnest7At5Iter: independent columns first, coupled pair row-by-row, then last column"
        );
    }
}
