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

/// Extension trait that adds `collect_columnar` method to distributions.
///
/// This allows calling `dist.collect_columnar(rng, n, sinks)` directly on a distribution
/// without needing to call `.sample()` first. Useful for `impl Distribution<T>` return types.
pub trait DistributionCollectColumnarExt<T>: Distribution<T> + Sized {
    fn collect_columnar<S>(
        &mut self,
        rng: &mut impl Rng,
        n: usize,
        sinks: S,
    ) -> <Self::Iter as CollectColumnar<S>>::Output
    where
        Self::Iter: CollectColumnar<S>;
}

impl<T, D: Distribution<T>> DistributionCollectColumnarExt<T> for D {
    fn collect_columnar<S>(
        &mut self,
        rng: &mut impl Rng,
        n: usize,
        sinks: S,
    ) -> <Self::Iter as CollectColumnar<S>>::Output
    where
        Self::Iter: CollectColumnar<S>,
    {
        let iter = self.sample();
        CollectColumnar::collect_columnar(iter, rng, n, sinks)
    }
}

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

    /// Generate n values and process each with the given function.
    ///
    /// This is zero-allocation: values are generated and processed one at a time.
    /// Useful for column-by-column collection where all values from one column
    /// are processed before moving to the next.
    fn next_n_with<F>(&mut self, n: usize, rng: &mut impl Rng, mut f: F)
    where
        F: FnMut(Self::Item),
    {
        for _ in 0..n {
            f(self.next(rng));
        }
    }
}

/// A factory that produces sample iterators.
pub trait Distribution<T> {
    type Iter: SampleIter<Item = T>;
    fn sample(&mut self) -> Self::Iter;

    /// Collect n values into sinks using columnar collection.
    ///
    /// This method is provided on the Distribution trait directly so that
    /// `impl Distribution<T>` return types can use columnar collection.
    fn collect_columnar<S>(
        &mut self,
        rng: &mut impl Rng,
        n: usize,
        sinks: S,
    ) -> <Self::Iter as CollectColumnar<S>>::Output
    where
        Self::Iter: CollectColumnar<S>,
    {
        let iter = self.sample();
        CollectColumnar::collect_columnar(iter, rng, n, sinks)
    }
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

// === Column Schema trait ===

/// Trait for structs that can describe their column schema.
/// Typically derived via `#[derive(ColumnSchema)]` from populate-derive.
pub trait ColumnSchema {
    /// The tuple type that matches this struct's fields.
    type Row;

    /// Get field names for schema generation.
    fn field_names() -> &'static [&'static str];

    /// Get the number of fields/columns.
    fn field_count() -> usize;
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

/// Iterator for 8-tuple distributions.
#[derive(Clone)]
pub struct TupleIter8<I1, I2, I3, I4, I5, I6, I7, I8>(
    pub I1,
    pub I2,
    pub I3,
    pub I4,
    pub I5,
    pub I6,
    pub I7,
    pub I8,
);

impl<
    I1: SampleIter,
    I2: SampleIter,
    I3: SampleIter,
    I4: SampleIter,
    I5: SampleIter,
    I6: SampleIter,
    I7: SampleIter,
    I8: SampleIter,
> SampleIter for TupleIter8<I1, I2, I3, I4, I5, I6, I7, I8>
{
    type Item = (
        I1::Item,
        I2::Item,
        I3::Item,
        I4::Item,
        I5::Item,
        I6::Item,
        I7::Item,
        I8::Item,
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
            self.7.next(rng),
        )
    }
}

impl<T1, T2, T3, T4, T5, T6, T7, T8, D1, D2, D3, D4, D5, D6, D7, D8>
    Distribution<(T1, T2, T3, T4, T5, T6, T7, T8)> for (D1, D2, D3, D4, D5, D6, D7, D8)
where
    D1: Distribution<T1>,
    D2: Distribution<T2>,
    D3: Distribution<T3>,
    D4: Distribution<T4>,
    D5: Distribution<T5>,
    D6: Distribution<T6>,
    D7: Distribution<T7>,
    D8: Distribution<T8>,
{
    type Iter =
        TupleIter8<D1::Iter, D2::Iter, D3::Iter, D4::Iter, D5::Iter, D6::Iter, D7::Iter, D8::Iter>;

    fn sample(&mut self) -> Self::Iter {
        TupleIter8(
            self.0.sample(),
            self.1.sample(),
            self.2.sample(),
            self.3.sample(),
            self.4.sample(),
            self.5.sample(),
            self.6.sample(),
            self.7.sample(),
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
    iter.0.next_n_with(n, rng, |v| sink1.push(v));
    iter.1.next_n_with(n, rng, |v| sink2.push(v));
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
    iter.0.next_n_with(n, rng, |v| sink1.push(v));
    iter.1.next_n_with(n, rng, |v| sink2.push(v));
    iter.2.next_n_with(n, rng, |v| sink3.push(v));
    (sink1.finish(), sink2.finish(), sink3.finish())
}

/// Collect 4-tuple into separate sinks (column-oriented).
pub fn collect_struct4_into<I1, I2, I3, I4, S1, S2, S3, S4>(
    mut iter: TupleIter4<I1, I2, I3, I4>,
    rng: &mut impl Rng,
    n: usize,
    mut sink1: S1,
    mut sink2: S2,
    mut sink3: S3,
    mut sink4: S4,
) -> (S1::Output, S2::Output, S3::Output, S4::Output)
where
    I1: SampleIter,
    I2: SampleIter,
    I3: SampleIter,
    I4: SampleIter,
    S1: Sink<I1::Item>,
    S2: Sink<I2::Item>,
    S3: Sink<I3::Item>,
    S4: Sink<I4::Item>,
{
    iter.0.next_n_with(n, rng, |v| sink1.push(v));
    iter.1.next_n_with(n, rng, |v| sink2.push(v));
    iter.2.next_n_with(n, rng, |v| sink3.push(v));
    iter.3.next_n_with(n, rng, |v| sink4.push(v));
    (
        sink1.finish(),
        sink2.finish(),
        sink3.finish(),
        sink4.finish(),
    )
}

/// Collect 5-tuple into separate sinks (column-oriented).
#[allow(clippy::too_many_arguments, clippy::type_complexity)]
pub fn collect_struct5_into<I1, I2, I3, I4, I5, S1, S2, S3, S4, S5>(
    mut iter: TupleIter5<I1, I2, I3, I4, I5>,
    rng: &mut impl Rng,
    n: usize,
    mut sink1: S1,
    mut sink2: S2,
    mut sink3: S3,
    mut sink4: S4,
    mut sink5: S5,
) -> (S1::Output, S2::Output, S3::Output, S4::Output, S5::Output)
where
    I1: SampleIter,
    I2: SampleIter,
    I3: SampleIter,
    I4: SampleIter,
    I5: SampleIter,
    S1: Sink<I1::Item>,
    S2: Sink<I2::Item>,
    S3: Sink<I3::Item>,
    S4: Sink<I4::Item>,
    S5: Sink<I5::Item>,
{
    iter.0.next_n_with(n, rng, |v| sink1.push(v));
    iter.1.next_n_with(n, rng, |v| sink2.push(v));
    iter.2.next_n_with(n, rng, |v| sink3.push(v));
    iter.3.next_n_with(n, rng, |v| sink4.push(v));
    iter.4.next_n_with(n, rng, |v| sink5.push(v));
    (
        sink1.finish(),
        sink2.finish(),
        sink3.finish(),
        sink4.finish(),
        sink5.finish(),
    )
}

/// Collect 6-tuple into separate sinks (column-oriented).
#[allow(clippy::too_many_arguments, clippy::type_complexity)]
pub fn collect_struct6_into<I1, I2, I3, I4, I5, I6, S1, S2, S3, S4, S5, S6>(
    mut iter: TupleIter6<I1, I2, I3, I4, I5, I6>,
    rng: &mut impl Rng,
    n: usize,
    mut sink1: S1,
    mut sink2: S2,
    mut sink3: S3,
    mut sink4: S4,
    mut sink5: S5,
    mut sink6: S6,
) -> (
    S1::Output,
    S2::Output,
    S3::Output,
    S4::Output,
    S5::Output,
    S6::Output,
)
where
    I1: SampleIter,
    I2: SampleIter,
    I3: SampleIter,
    I4: SampleIter,
    I5: SampleIter,
    I6: SampleIter,
    S1: Sink<I1::Item>,
    S2: Sink<I2::Item>,
    S3: Sink<I3::Item>,
    S4: Sink<I4::Item>,
    S5: Sink<I5::Item>,
    S6: Sink<I6::Item>,
{
    iter.0.next_n_with(n, rng, |v| sink1.push(v));
    iter.1.next_n_with(n, rng, |v| sink2.push(v));
    iter.2.next_n_with(n, rng, |v| sink3.push(v));
    iter.3.next_n_with(n, rng, |v| sink4.push(v));
    iter.4.next_n_with(n, rng, |v| sink5.push(v));
    iter.5.next_n_with(n, rng, |v| sink6.push(v));
    (
        sink1.finish(),
        sink2.finish(),
        sink3.finish(),
        sink4.finish(),
        sink5.finish(),
        sink6.finish(),
    )
}

/// Collect 7-tuple into separate sinks (column-oriented).
#[allow(clippy::too_many_arguments, clippy::type_complexity)]
pub fn collect_struct7_into<I1, I2, I3, I4, I5, I6, I7, S1, S2, S3, S4, S5, S6, S7>(
    mut iter: TupleIter7<I1, I2, I3, I4, I5, I6, I7>,
    rng: &mut impl Rng,
    n: usize,
    mut sink1: S1,
    mut sink2: S2,
    mut sink3: S3,
    mut sink4: S4,
    mut sink5: S5,
    mut sink6: S6,
    mut sink7: S7,
) -> (
    S1::Output,
    S2::Output,
    S3::Output,
    S4::Output,
    S5::Output,
    S6::Output,
    S7::Output,
)
where
    I1: SampleIter,
    I2: SampleIter,
    I3: SampleIter,
    I4: SampleIter,
    I5: SampleIter,
    I6: SampleIter,
    I7: SampleIter,
    S1: Sink<I1::Item>,
    S2: Sink<I2::Item>,
    S3: Sink<I3::Item>,
    S4: Sink<I4::Item>,
    S5: Sink<I5::Item>,
    S6: Sink<I6::Item>,
    S7: Sink<I7::Item>,
{
    iter.0.next_n_with(n, rng, |v| sink1.push(v));
    iter.1.next_n_with(n, rng, |v| sink2.push(v));
    iter.2.next_n_with(n, rng, |v| sink3.push(v));
    iter.3.next_n_with(n, rng, |v| sink4.push(v));
    iter.4.next_n_with(n, rng, |v| sink5.push(v));
    iter.5.next_n_with(n, rng, |v| sink6.push(v));
    iter.6.next_n_with(n, rng, |v| sink7.push(v));
    (
        sink1.finish(),
        sink2.finish(),
        sink3.finish(),
        sink4.finish(),
        sink5.finish(),
        sink6.finish(),
        sink7.finish(),
    )
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

// === Columnar collection traits ===

/// A tuple of sinks that can receive values from a tuple row.
/// This enables collecting tuple-producing iterators into separate column sinks.
pub trait SinkTuple<Row>: Sized {
    type Output;

    /// Push a row, destructuring it into the individual sinks.
    fn push_row(&mut self, row: Row);

    /// Finish all sinks and return the outputs.
    fn finish(self) -> Self::Output;
}

// SinkTuple implementations for tuples of sinks

impl<S1: Sink<T1>, T1> SinkTuple<(T1,)> for (S1,) {
    type Output = (S1::Output,);

    fn push_row(&mut self, (a,): (T1,)) {
        self.0.push(a);
    }

    fn finish(self) -> Self::Output {
        (self.0.finish(),)
    }
}

impl<S1: Sink<T1>, S2: Sink<T2>, T1, T2> SinkTuple<(T1, T2)> for (S1, S2) {
    type Output = (S1::Output, S2::Output);

    fn push_row(&mut self, (a, b): (T1, T2)) {
        self.0.push(a);
        self.1.push(b);
    }

    fn finish(self) -> Self::Output {
        (self.0.finish(), self.1.finish())
    }
}

/// Trait for 2-column sink tuples with column-wise access.
pub trait SinkTuple2<T1, T2>: SinkTuple<(T1, T2)> {
    fn push0(&mut self, v: T1);
    fn push1(&mut self, v: T2);
}

impl<S1: Sink<T1>, S2: Sink<T2>, T1, T2> SinkTuple2<T1, T2> for (S1, S2) {
    fn push0(&mut self, v: T1) {
        self.0.push(v);
    }
    fn push1(&mut self, v: T2) {
        self.1.push(v);
    }
}

impl<S1: Sink<T1>, S2: Sink<T2>, S3: Sink<T3>, T1, T2, T3> SinkTuple<(T1, T2, T3)>
    for (S1, S2, S3)
{
    type Output = (S1::Output, S2::Output, S3::Output);

    fn push_row(&mut self, (a, b, c): (T1, T2, T3)) {
        self.0.push(a);
        self.1.push(b);
        self.2.push(c);
    }

    fn finish(self) -> Self::Output {
        (self.0.finish(), self.1.finish(), self.2.finish())
    }
}

/// Trait for 3-column sink tuples with column-wise access.
pub trait SinkTuple3<T1, T2, T3>: SinkTuple<(T1, T2, T3)> {
    fn push0(&mut self, v: T1);
    fn push1(&mut self, v: T2);
    fn push2(&mut self, v: T3);
}

impl<S1: Sink<T1>, S2: Sink<T2>, S3: Sink<T3>, T1, T2, T3> SinkTuple3<T1, T2, T3> for (S1, S2, S3) {
    fn push0(&mut self, v: T1) {
        self.0.push(v);
    }
    fn push1(&mut self, v: T2) {
        self.1.push(v);
    }
    fn push2(&mut self, v: T3) {
        self.2.push(v);
    }
}

impl<S1: Sink<T1>, S2: Sink<T2>, S3: Sink<T3>, S4: Sink<T4>, T1, T2, T3, T4>
    SinkTuple<(T1, T2, T3, T4)> for (S1, S2, S3, S4)
{
    type Output = (S1::Output, S2::Output, S3::Output, S4::Output);

    fn push_row(&mut self, (a, b, c, d): (T1, T2, T3, T4)) {
        self.0.push(a);
        self.1.push(b);
        self.2.push(c);
        self.3.push(d);
    }

    fn finish(self) -> Self::Output {
        (
            self.0.finish(),
            self.1.finish(),
            self.2.finish(),
            self.3.finish(),
        )
    }
}

/// Trait for 4-column sink tuples with column-wise access.
pub trait SinkTuple4<T1, T2, T3, T4>: SinkTuple<(T1, T2, T3, T4)> {
    fn push0(&mut self, v: T1);
    fn push1(&mut self, v: T2);
    fn push2(&mut self, v: T3);
    fn push3(&mut self, v: T4);
}

impl<S1: Sink<T1>, S2: Sink<T2>, S3: Sink<T3>, S4: Sink<T4>, T1, T2, T3, T4>
    SinkTuple4<T1, T2, T3, T4> for (S1, S2, S3, S4)
{
    fn push0(&mut self, v: T1) {
        self.0.push(v);
    }
    fn push1(&mut self, v: T2) {
        self.1.push(v);
    }
    fn push2(&mut self, v: T3) {
        self.2.push(v);
    }
    fn push3(&mut self, v: T4) {
        self.3.push(v);
    }
}

/// Trait for 5-column sink tuples with column-wise access.
pub trait SinkTuple5<T1, T2, T3, T4, T5>: SinkTuple<(T1, T2, T3, T4, T5)> {
    fn push0(&mut self, v: T1);
    fn push1(&mut self, v: T2);
    fn push2(&mut self, v: T3);
    fn push3(&mut self, v: T4);
    fn push4(&mut self, v: T5);
}

/// Trait for 6-column sink tuples with column-wise access.
pub trait SinkTuple6<T1, T2, T3, T4, T5, T6>: SinkTuple<(T1, T2, T3, T4, T5, T6)> {
    fn push0(&mut self, v: T1);
    fn push1(&mut self, v: T2);
    fn push2(&mut self, v: T3);
    fn push3(&mut self, v: T4);
    fn push4(&mut self, v: T5);
    fn push5(&mut self, v: T6);
}

/// Trait for 7-column sink tuples with column-wise access.
pub trait SinkTuple7<T1, T2, T3, T4, T5, T6, T7>: SinkTuple<(T1, T2, T3, T4, T5, T6, T7)> {
    fn push0(&mut self, v: T1);
    fn push1(&mut self, v: T2);
    fn push2(&mut self, v: T3);
    fn push3(&mut self, v: T4);
    fn push4(&mut self, v: T5);
    fn push5(&mut self, v: T6);
    fn push6(&mut self, v: T7);
}

/// Trait for 8-column sink tuples with column-wise access.
pub trait SinkTuple8<T1, T2, T3, T4, T5, T6, T7, T8>: SinkTuple<(T1, T2, T3, T4, T5, T6, T7, T8)> {
    fn push0(&mut self, v: T1);
    fn push1(&mut self, v: T2);
    fn push2(&mut self, v: T3);
    fn push3(&mut self, v: T4);
    fn push4(&mut self, v: T5);
    fn push5(&mut self, v: T6);
    fn push6(&mut self, v: T7);
    fn push7(&mut self, v: T8);
}

impl<S1: Sink<T1>, S2: Sink<T2>, S3: Sink<T3>, S4: Sink<T4>, S5: Sink<T5>, T1, T2, T3, T4, T5>
    SinkTuple<(T1, T2, T3, T4, T5)> for (S1, S2, S3, S4, S5)
{
    type Output = (S1::Output, S2::Output, S3::Output, S4::Output, S5::Output);

    fn push_row(&mut self, (a, b, c, d, e): (T1, T2, T3, T4, T5)) {
        self.0.push(a);
        self.1.push(b);
        self.2.push(c);
        self.3.push(d);
        self.4.push(e);
    }

    fn finish(self) -> Self::Output {
        (
            self.0.finish(),
            self.1.finish(),
            self.2.finish(),
            self.3.finish(),
            self.4.finish(),
        )
    }
}

impl<S1: Sink<T1>, S2: Sink<T2>, S3: Sink<T3>, S4: Sink<T4>, S5: Sink<T5>, T1, T2, T3, T4, T5>
    SinkTuple5<T1, T2, T3, T4, T5> for (S1, S2, S3, S4, S5)
{
    fn push0(&mut self, v: T1) {
        self.0.push(v);
    }
    fn push1(&mut self, v: T2) {
        self.1.push(v);
    }
    fn push2(&mut self, v: T3) {
        self.2.push(v);
    }
    fn push3(&mut self, v: T4) {
        self.3.push(v);
    }
    fn push4(&mut self, v: T5) {
        self.4.push(v);
    }
}

impl<
    S1: Sink<T1>,
    S2: Sink<T2>,
    S3: Sink<T3>,
    S4: Sink<T4>,
    S5: Sink<T5>,
    S6: Sink<T6>,
    T1,
    T2,
    T3,
    T4,
    T5,
    T6,
> SinkTuple<(T1, T2, T3, T4, T5, T6)> for (S1, S2, S3, S4, S5, S6)
{
    type Output = (
        S1::Output,
        S2::Output,
        S3::Output,
        S4::Output,
        S5::Output,
        S6::Output,
    );

    fn push_row(&mut self, (a, b, c, d, e, f): (T1, T2, T3, T4, T5, T6)) {
        self.0.push(a);
        self.1.push(b);
        self.2.push(c);
        self.3.push(d);
        self.4.push(e);
        self.5.push(f);
    }

    fn finish(self) -> Self::Output {
        (
            self.0.finish(),
            self.1.finish(),
            self.2.finish(),
            self.3.finish(),
            self.4.finish(),
            self.5.finish(),
        )
    }
}

impl<
    S1: Sink<T1>,
    S2: Sink<T2>,
    S3: Sink<T3>,
    S4: Sink<T4>,
    S5: Sink<T5>,
    S6: Sink<T6>,
    T1,
    T2,
    T3,
    T4,
    T5,
    T6,
> SinkTuple6<T1, T2, T3, T4, T5, T6> for (S1, S2, S3, S4, S5, S6)
{
    fn push0(&mut self, v: T1) {
        self.0.push(v);
    }
    fn push1(&mut self, v: T2) {
        self.1.push(v);
    }
    fn push2(&mut self, v: T3) {
        self.2.push(v);
    }
    fn push3(&mut self, v: T4) {
        self.3.push(v);
    }
    fn push4(&mut self, v: T5) {
        self.4.push(v);
    }
    fn push5(&mut self, v: T6) {
        self.5.push(v);
    }
}

impl<
    S1: Sink<T1>,
    S2: Sink<T2>,
    S3: Sink<T3>,
    S4: Sink<T4>,
    S5: Sink<T5>,
    S6: Sink<T6>,
    S7: Sink<T7>,
    T1,
    T2,
    T3,
    T4,
    T5,
    T6,
    T7,
> SinkTuple<(T1, T2, T3, T4, T5, T6, T7)> for (S1, S2, S3, S4, S5, S6, S7)
{
    type Output = (
        S1::Output,
        S2::Output,
        S3::Output,
        S4::Output,
        S5::Output,
        S6::Output,
        S7::Output,
    );

    fn push_row(&mut self, (a, b, c, d, e, f, g): (T1, T2, T3, T4, T5, T6, T7)) {
        self.0.push(a);
        self.1.push(b);
        self.2.push(c);
        self.3.push(d);
        self.4.push(e);
        self.5.push(f);
        self.6.push(g);
    }

    fn finish(self) -> Self::Output {
        (
            self.0.finish(),
            self.1.finish(),
            self.2.finish(),
            self.3.finish(),
            self.4.finish(),
            self.5.finish(),
            self.6.finish(),
        )
    }
}

impl<
    S1: Sink<T1>,
    S2: Sink<T2>,
    S3: Sink<T3>,
    S4: Sink<T4>,
    S5: Sink<T5>,
    S6: Sink<T6>,
    S7: Sink<T7>,
    T1,
    T2,
    T3,
    T4,
    T5,
    T6,
    T7,
> SinkTuple7<T1, T2, T3, T4, T5, T6, T7> for (S1, S2, S3, S4, S5, S6, S7)
{
    fn push0(&mut self, v: T1) {
        self.0.push(v);
    }
    fn push1(&mut self, v: T2) {
        self.1.push(v);
    }
    fn push2(&mut self, v: T3) {
        self.2.push(v);
    }
    fn push3(&mut self, v: T4) {
        self.3.push(v);
    }
    fn push4(&mut self, v: T5) {
        self.4.push(v);
    }
    fn push5(&mut self, v: T6) {
        self.5.push(v);
    }
    fn push6(&mut self, v: T7) {
        self.6.push(v);
    }
}

impl<
    S1: Sink<T1>,
    S2: Sink<T2>,
    S3: Sink<T3>,
    S4: Sink<T4>,
    S5: Sink<T5>,
    S6: Sink<T6>,
    S7: Sink<T7>,
    S8: Sink<T8>,
    T1,
    T2,
    T3,
    T4,
    T5,
    T6,
    T7,
    T8,
> SinkTuple<(T1, T2, T3, T4, T5, T6, T7, T8)> for (S1, S2, S3, S4, S5, S6, S7, S8)
{
    type Output = (
        S1::Output,
        S2::Output,
        S3::Output,
        S4::Output,
        S5::Output,
        S6::Output,
        S7::Output,
        S8::Output,
    );

    fn push_row(&mut self, (a, b, c, d, e, f, g, h): (T1, T2, T3, T4, T5, T6, T7, T8)) {
        self.0.push(a);
        self.1.push(b);
        self.2.push(c);
        self.3.push(d);
        self.4.push(e);
        self.5.push(f);
        self.6.push(g);
        self.7.push(h);
    }

    fn finish(self) -> Self::Output {
        (
            self.0.finish(),
            self.1.finish(),
            self.2.finish(),
            self.3.finish(),
            self.4.finish(),
            self.5.finish(),
            self.6.finish(),
            self.7.finish(),
        )
    }
}

impl<
    S1: Sink<T1>,
    S2: Sink<T2>,
    S3: Sink<T3>,
    S4: Sink<T4>,
    S5: Sink<T5>,
    S6: Sink<T6>,
    S7: Sink<T7>,
    S8: Sink<T8>,
    T1,
    T2,
    T3,
    T4,
    T5,
    T6,
    T7,
    T8,
> SinkTuple8<T1, T2, T3, T4, T5, T6, T7, T8> for (S1, S2, S3, S4, S5, S6, S7, S8)
{
    fn push0(&mut self, v: T1) {
        self.0.push(v);
    }
    fn push1(&mut self, v: T2) {
        self.1.push(v);
    }
    fn push2(&mut self, v: T3) {
        self.2.push(v);
    }
    fn push3(&mut self, v: T4) {
        self.3.push(v);
    }
    fn push4(&mut self, v: T5) {
        self.4.push(v);
    }
    fn push5(&mut self, v: T6) {
        self.5.push(v);
    }
    fn push6(&mut self, v: T7) {
        self.6.push(v);
    }
    fn push7(&mut self, v: T8) {
        self.7.push(v);
    }
}

/// Trait for joining sink tuple outputs after collection.
pub trait JoinOutputs<Other> {
    type Output;
    fn join(self, other: Other) -> Self::Output;
}

impl<A, B> JoinOutputs<(B,)> for (A,) {
    type Output = (A, B);
    fn join(self, other: (B,)) -> (A, B) {
        (self.0, other.0)
    }
}

impl<A, B, C> JoinOutputs<(C,)> for (A, B) {
    type Output = (A, B, C);
    fn join(self, other: (C,)) -> (A, B, C) {
        (self.0, self.1, other.0)
    }
}

impl<A, B, C> JoinOutputs<(B, C)> for (A,) {
    type Output = (A, B, C);
    fn join(self, other: (B, C)) -> (A, B, C) {
        (self.0, other.0, other.1)
    }
}

impl<A, B, C, D> JoinOutputs<(D,)> for (A, B, C) {
    type Output = (A, B, C, D);
    fn join(self, other: (D,)) -> (A, B, C, D) {
        (self.0, self.1, self.2, other.0)
    }
}

impl<A, B, C, D> JoinOutputs<(C, D)> for (A, B) {
    type Output = (A, B, C, D);
    fn join(self, other: (C, D)) -> (A, B, C, D) {
        (self.0, self.1, other.0, other.1)
    }
}

impl<A, B, C, D> JoinOutputs<(B, C, D)> for (A,) {
    type Output = (A, B, C, D);
    fn join(self, other: (B, C, D)) -> (A, B, C, D) {
        (self.0, other.0, other.1, other.2)
    }
}

impl<A, B, C, D, E> JoinOutputs<(E,)> for (A, B, C, D) {
    type Output = (A, B, C, D, E);
    fn join(self, other: (E,)) -> (A, B, C, D, E) {
        (self.0, self.1, self.2, self.3, other.0)
    }
}

impl<A, B, C, D, E> JoinOutputs<(D, E)> for (A, B, C) {
    type Output = (A, B, C, D, E);
    fn join(self, other: (D, E)) -> (A, B, C, D, E) {
        (self.0, self.1, self.2, other.0, other.1)
    }
}

impl<A, B, C, D, E> JoinOutputs<(C, D, E)> for (A, B) {
    type Output = (A, B, C, D, E);
    fn join(self, other: (C, D, E)) -> (A, B, C, D, E) {
        (self.0, self.1, other.0, other.1, other.2)
    }
}

impl<A, B, C, D, E> JoinOutputs<(B, C, D, E)> for (A,) {
    type Output = (A, B, C, D, E);
    fn join(self, other: (B, C, D, E)) -> (A, B, C, D, E) {
        (self.0, other.0, other.1, other.2, other.3)
    }
}

impl<A, B, C, D, E, F> JoinOutputs<(F,)> for (A, B, C, D, E) {
    type Output = (A, B, C, D, E, F);
    fn join(self, other: (F,)) -> (A, B, C, D, E, F) {
        (self.0, self.1, self.2, self.3, self.4, other.0)
    }
}

impl<A, B, C, D, E, F> JoinOutputs<(E, F)> for (A, B, C, D) {
    type Output = (A, B, C, D, E, F);
    fn join(self, other: (E, F)) -> (A, B, C, D, E, F) {
        (self.0, self.1, self.2, self.3, other.0, other.1)
    }
}

impl<A, B, C, D, E, F> JoinOutputs<(D, E, F)> for (A, B, C) {
    type Output = (A, B, C, D, E, F);
    fn join(self, other: (D, E, F)) -> (A, B, C, D, E, F) {
        (self.0, self.1, self.2, other.0, other.1, other.2)
    }
}

impl<A, B, C, D, E, F> JoinOutputs<(C, D, E, F)> for (A, B) {
    type Output = (A, B, C, D, E, F);
    fn join(self, other: (C, D, E, F)) -> (A, B, C, D, E, F) {
        (self.0, self.1, other.0, other.1, other.2, other.3)
    }
}

impl<A, B, C, D, E, F> JoinOutputs<(B, C, D, E, F)> for (A,) {
    type Output = (A, B, C, D, E, F);
    fn join(self, other: (B, C, D, E, F)) -> (A, B, C, D, E, F) {
        (self.0, other.0, other.1, other.2, other.3, other.4)
    }
}

impl<A, B, C, D, E, F, G> JoinOutputs<(G,)> for (A, B, C, D, E, F) {
    type Output = (A, B, C, D, E, F, G);
    fn join(self, other: (G,)) -> (A, B, C, D, E, F, G) {
        (self.0, self.1, self.2, self.3, self.4, self.5, other.0)
    }
}

impl<A, B, C, D, E, F, G> JoinOutputs<(F, G)> for (A, B, C, D, E) {
    type Output = (A, B, C, D, E, F, G);
    fn join(self, other: (F, G)) -> (A, B, C, D, E, F, G) {
        (self.0, self.1, self.2, self.3, self.4, other.0, other.1)
    }
}

impl<A, B, C, D, E, F, G> JoinOutputs<(E, F, G)> for (A, B, C, D) {
    type Output = (A, B, C, D, E, F, G);
    fn join(self, other: (E, F, G)) -> (A, B, C, D, E, F, G) {
        (self.0, self.1, self.2, self.3, other.0, other.1, other.2)
    }
}

impl<A, B, C, D, E, F, G> JoinOutputs<(D, E, F, G)> for (A, B, C) {
    type Output = (A, B, C, D, E, F, G);
    fn join(self, other: (D, E, F, G)) -> (A, B, C, D, E, F, G) {
        (self.0, self.1, self.2, other.0, other.1, other.2, other.3)
    }
}

impl<A, B, C, D, E, F, G> JoinOutputs<(C, D, E, F, G)> for (A, B) {
    type Output = (A, B, C, D, E, F, G);
    fn join(self, other: (C, D, E, F, G)) -> (A, B, C, D, E, F, G) {
        (self.0, self.1, other.0, other.1, other.2, other.3, other.4)
    }
}

impl<A, B, C, D, E, F, G> JoinOutputs<(B, C, D, E, F, G)> for (A,) {
    type Output = (A, B, C, D, E, F, G);
    fn join(self, other: (B, C, D, E, F, G)) -> (A, B, C, D, E, F, G) {
        (self.0, other.0, other.1, other.2, other.3, other.4, other.5)
    }
}

impl<A, B, C, D, E, F, G, H> JoinOutputs<(H,)> for (A, B, C, D, E, F, G) {
    type Output = (A, B, C, D, E, F, G, H);
    fn join(self, other: (H,)) -> (A, B, C, D, E, F, G, H) {
        (
            self.0, self.1, self.2, self.3, self.4, self.5, self.6, other.0,
        )
    }
}

impl<A, B, C, D, E, F, G, H> JoinOutputs<(G, H)> for (A, B, C, D, E, F) {
    type Output = (A, B, C, D, E, F, G, H);
    fn join(self, other: (G, H)) -> (A, B, C, D, E, F, G, H) {
        (
            self.0, self.1, self.2, self.3, self.4, self.5, other.0, other.1,
        )
    }
}

impl<A, B, C, D, E, F, G, H> JoinOutputs<(F, G, H)> for (A, B, C, D, E) {
    type Output = (A, B, C, D, E, F, G, H);
    fn join(self, other: (F, G, H)) -> (A, B, C, D, E, F, G, H) {
        (
            self.0, self.1, self.2, self.3, self.4, other.0, other.1, other.2,
        )
    }
}

impl<A, B, C, D, E, F, G, H> JoinOutputs<(E, F, G, H)> for (A, B, C, D) {
    type Output = (A, B, C, D, E, F, G, H);
    fn join(self, other: (E, F, G, H)) -> (A, B, C, D, E, F, G, H) {
        (
            self.0, self.1, self.2, self.3, other.0, other.1, other.2, other.3,
        )
    }
}

impl<A, B, C, D, E, F, G, H> JoinOutputs<(D, E, F, G, H)> for (A, B, C) {
    type Output = (A, B, C, D, E, F, G, H);
    fn join(self, other: (D, E, F, G, H)) -> (A, B, C, D, E, F, G, H) {
        (
            self.0, self.1, self.2, other.0, other.1, other.2, other.3, other.4,
        )
    }
}

impl<A, B, C, D, E, F, G, H> JoinOutputs<(C, D, E, F, G, H)> for (A, B) {
    type Output = (A, B, C, D, E, F, G, H);
    fn join(self, other: (C, D, E, F, G, H)) -> (A, B, C, D, E, F, G, H) {
        (
            self.0, self.1, other.0, other.1, other.2, other.3, other.4, other.5,
        )
    }
}

impl<A, B, C, D, E, F, G, H> JoinOutputs<(B, C, D, E, F, G, H)> for (A,) {
    type Output = (A, B, C, D, E, F, G, H);
    fn join(self, other: (B, C, D, E, F, G, H)) -> (A, B, C, D, E, F, G, H) {
        (
            self.0, other.0, other.1, other.2, other.3, other.4, other.5, other.6,
        )
    }
}

/// Trait for iterators that support columnar collection.
///
/// This enables true column-by-column collection for tuple iterators,
/// where each column is fully generated before moving to the next.
///
/// The `Sinks` type parameter specifies what sink type this iterator can collect into.
/// `IS_COLUMNAR` indicates whether this iterator can be collected column-by-column.
pub trait CollectColumnar<Sinks>: SampleIter + Sized {
    /// Whether this iterator supports true column-by-column collection.
    /// - `true`: Each column can be generated independently
    /// - `false`: Columns have dependencies and must be generated row-by-row
    const IS_COLUMNAR: bool;

    type Output;

    /// Collect n values into the provided sinks.
    /// For columnar iterators, this collects each column separately (column-by-column).
    /// For non-columnar iterators, this collects row-by-row.
    fn collect_columnar(self, rng: &mut impl Rng, n: usize, sinks: Sinks) -> Self::Output;
}

// === CollectColumnar implementations for TupleIterN ===

impl<I1, I2, S> CollectColumnar<S> for TupleIter2<I1, I2>
where
    I1: SampleIter,
    I2: SampleIter,
    S: SinkTuple2<I1::Item, I2::Item>,
{
    const IS_COLUMNAR: bool = true;
    type Output = S::Output;

    fn collect_columnar(mut self, rng: &mut impl Rng, n: usize, mut sinks: S) -> Self::Output {
        // True column-by-column: collect all of column 0, then all of column 1
        self.0.next_n_with(n, rng, |v| sinks.push0(v));
        self.1.next_n_with(n, rng, |v| sinks.push1(v));
        sinks.finish()
    }
}

impl<I1, I2, I3, S> CollectColumnar<S> for TupleIter3<I1, I2, I3>
where
    I1: SampleIter,
    I2: SampleIter,
    I3: SampleIter,
    S: SinkTuple3<I1::Item, I2::Item, I3::Item>,
{
    const IS_COLUMNAR: bool = true;
    type Output = S::Output;

    fn collect_columnar(mut self, rng: &mut impl Rng, n: usize, mut sinks: S) -> Self::Output {
        self.0.next_n_with(n, rng, |v| sinks.push0(v));
        self.1.next_n_with(n, rng, |v| sinks.push1(v));
        self.2.next_n_with(n, rng, |v| sinks.push2(v));
        sinks.finish()
    }
}

impl<I1, I2, I3, I4, S> CollectColumnar<S> for TupleIter4<I1, I2, I3, I4>
where
    I1: SampleIter,
    I2: SampleIter,
    I3: SampleIter,
    I4: SampleIter,
    S: SinkTuple4<I1::Item, I2::Item, I3::Item, I4::Item>,
{
    const IS_COLUMNAR: bool = true;
    type Output = S::Output;

    fn collect_columnar(mut self, rng: &mut impl Rng, n: usize, mut sinks: S) -> Self::Output {
        self.0.next_n_with(n, rng, |v| sinks.push0(v));
        self.1.next_n_with(n, rng, |v| sinks.push1(v));
        self.2.next_n_with(n, rng, |v| sinks.push2(v));
        self.3.next_n_with(n, rng, |v| sinks.push3(v));
        sinks.finish()
    }
}

impl<I1, I2, I3, I4, I5, S> CollectColumnar<S> for TupleIter5<I1, I2, I3, I4, I5>
where
    I1: SampleIter,
    I2: SampleIter,
    I3: SampleIter,
    I4: SampleIter,
    I5: SampleIter,
    S: SinkTuple5<I1::Item, I2::Item, I3::Item, I4::Item, I5::Item>,
{
    const IS_COLUMNAR: bool = true;
    type Output = S::Output;

    fn collect_columnar(mut self, rng: &mut impl Rng, n: usize, mut sinks: S) -> Self::Output {
        self.0.next_n_with(n, rng, |v| sinks.push0(v));
        self.1.next_n_with(n, rng, |v| sinks.push1(v));
        self.2.next_n_with(n, rng, |v| sinks.push2(v));
        self.3.next_n_with(n, rng, |v| sinks.push3(v));
        self.4.next_n_with(n, rng, |v| sinks.push4(v));
        sinks.finish()
    }
}

impl<I1, I2, I3, I4, I5, I6, S> CollectColumnar<S> for TupleIter6<I1, I2, I3, I4, I5, I6>
where
    I1: SampleIter,
    I2: SampleIter,
    I3: SampleIter,
    I4: SampleIter,
    I5: SampleIter,
    I6: SampleIter,
    S: SinkTuple6<I1::Item, I2::Item, I3::Item, I4::Item, I5::Item, I6::Item>,
{
    const IS_COLUMNAR: bool = true;
    type Output = S::Output;

    fn collect_columnar(mut self, rng: &mut impl Rng, n: usize, mut sinks: S) -> Self::Output {
        self.0.next_n_with(n, rng, |v| sinks.push0(v));
        self.1.next_n_with(n, rng, |v| sinks.push1(v));
        self.2.next_n_with(n, rng, |v| sinks.push2(v));
        self.3.next_n_with(n, rng, |v| sinks.push3(v));
        self.4.next_n_with(n, rng, |v| sinks.push4(v));
        self.5.next_n_with(n, rng, |v| sinks.push5(v));
        sinks.finish()
    }
}

impl<I1, I2, I3, I4, I5, I6, I7, S> CollectColumnar<S> for TupleIter7<I1, I2, I3, I4, I5, I6, I7>
where
    I1: SampleIter,
    I2: SampleIter,
    I3: SampleIter,
    I4: SampleIter,
    I5: SampleIter,
    I6: SampleIter,
    I7: SampleIter,
    S: SinkTuple7<I1::Item, I2::Item, I3::Item, I4::Item, I5::Item, I6::Item, I7::Item>,
{
    const IS_COLUMNAR: bool = true;
    type Output = S::Output;

    fn collect_columnar(mut self, rng: &mut impl Rng, n: usize, mut sinks: S) -> Self::Output {
        self.0.next_n_with(n, rng, |v| sinks.push0(v));
        self.1.next_n_with(n, rng, |v| sinks.push1(v));
        self.2.next_n_with(n, rng, |v| sinks.push2(v));
        self.3.next_n_with(n, rng, |v| sinks.push3(v));
        self.4.next_n_with(n, rng, |v| sinks.push4(v));
        self.5.next_n_with(n, rng, |v| sinks.push5(v));
        self.6.next_n_with(n, rng, |v| sinks.push6(v));
        sinks.finish()
    }
}

impl<I1, I2, I3, I4, I5, I6, I7, I8, S> CollectColumnar<S>
    for TupleIter8<I1, I2, I3, I4, I5, I6, I7, I8>
where
    I1: SampleIter,
    I2: SampleIter,
    I3: SampleIter,
    I4: SampleIter,
    I5: SampleIter,
    I6: SampleIter,
    I7: SampleIter,
    I8: SampleIter,
    S: SinkTuple8<I1::Item, I2::Item, I3::Item, I4::Item, I5::Item, I6::Item, I7::Item, I8::Item>,
{
    const IS_COLUMNAR: bool = true;
    type Output = S::Output;

    fn collect_columnar(mut self, rng: &mut impl Rng, n: usize, mut sinks: S) -> Self::Output {
        self.0.next_n_with(n, rng, |v| sinks.push0(v));
        self.1.next_n_with(n, rng, |v| sinks.push1(v));
        self.2.next_n_with(n, rng, |v| sinks.push2(v));
        self.3.next_n_with(n, rng, |v| sinks.push3(v));
        self.4.next_n_with(n, rng, |v| sinks.push4(v));
        self.5.next_n_with(n, rng, |v| sinks.push5(v));
        self.6.next_n_with(n, rng, |v| sinks.push6(v));
        self.7.next_n_with(n, rng, |v| sinks.push7(v));
        sinks.finish()
    }
}

/// Extension trait providing `collect_columnar` as a method on any `SampleIter`.
///
/// This allows calling `iter.collect_columnar(rng, n, sinks)` where the concrete
/// iterator type implements `CollectColumnar<S>` for the given sink type `S`.
pub trait CollectColumnarExt: SampleIter + Sized {
    fn collect_columnar<S>(self, rng: &mut impl Rng, n: usize, sinks: S) -> <Self as CollectColumnar<S>>::Output
    where
        Self: CollectColumnar<S>;
}

impl<I: SampleIter + Sized> CollectColumnarExt for I {
    fn collect_columnar<S>(self, rng: &mut impl Rng, n: usize, sinks: S) -> <Self as CollectColumnar<S>>::Output
    where
        Self: CollectColumnar<S>,
    {
        <Self as CollectColumnar<S>>::collect_columnar(self, rng, n, sinks)
    }
}
