use crate::traits::{
    ColumnDistribution, Distribution, SampleIter, Sink, Splittable, SplittableExt,
};
use rand::Rng;
use std::marker::PhantomData;

// ============================================================================
// Constant
// ============================================================================

pub struct Constant<T> {
    pub value: T,
}

pub fn constant<T>(value: T) -> Constant<T> {
    Constant { value }
}

pub struct ConstantIter<T> {
    value: T,
}

impl<T: Copy> SampleIter for ConstantIter<T> {
    type Item = T;
    fn next(&mut self, _rng: &mut impl Rng) -> Self::Item {
        self.value
    }
}

impl<T: Copy + Splittable> Distribution for Constant<T> {
    type Item = T;
    type RowIter = ConstantIter<T>;

    fn row_iter(&mut self) -> Self::RowIter {
        ConstantIter { value: self.value }
    }
}

impl<T: Copy + Splittable> ColumnDistribution for Constant<T> {
    fn collect_into<S: Sink<Self::Item>>(&mut self, rng: &mut impl Rng, mut sink: S, n: usize) -> S
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

// ============================================================================
// Range
// ============================================================================

pub struct Range {
    pub start: i64,
    pub step: i64,
}

pub fn range(start: i64, step: i64) -> Range {
    Range { start, step }
}

pub struct RangeIter {
    pub current: i64,
    pub step: i64,
}

impl SampleIter for RangeIter {
    type Item = i64;
    fn next(&mut self, _rng: &mut impl Rng) -> Self::Item {
        let v = self.current;
        self.current += self.step;
        v
    }
}

impl Distribution for Range {
    type Item = i64;
    type RowIter = RangeIter;

    fn row_iter(&mut self) -> Self::RowIter {
        RangeIter {
            current: self.start,
            step: self.step,
        }
    }
}

impl ColumnDistribution for Range {
    fn collect_into<S: Sink<Self::Item>>(&mut self, rng: &mut impl Rng, mut sink: S, n: usize) -> S
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

// ============================================================================
// Tuple (D1, D2) - col-by-col collection using split/unsplit
// ============================================================================

pub struct TupleIter2<I1, I2>(pub I1, pub I2);

impl<I1: SampleIter, I2: SampleIter> SampleIter for TupleIter2<I1, I2> {
    type Item = (I1::Item, I2::Item);

    fn next(&mut self, rng: &mut impl Rng) -> Self::Item {
        (self.0.next(rng), self.1.next(rng))
    }
}

impl<D1: Distribution, D2: Distribution> Distribution for (D1, D2) {
    type Item = (D1::Item, D2::Item);
    type RowIter = TupleIter2<D1::RowIter, D2::RowIter>;

    fn row_iter(&mut self) -> Self::RowIter {
        TupleIter2(self.0.row_iter(), self.1.row_iter())
    }
}

impl<D1: ColumnDistribution, D2: ColumnDistribution> ColumnDistribution for (D1, D2)
where
    D1::Item: Splittable,
    D2::Item: Splittable,
{
    /// Col-by-col collection: fills all of column 0, then all of column 1
    fn collect_into<S: Sink<(D1::Item, D2::Item)>>(
        &mut self,
        rng: &mut impl Rng,
        sink: S,
        n: usize,
    ) -> S
    where
        Self: Sized,
    {
        let (head_sink, tail_sink) = sink.split();
        let head_sink = self.0.collect_into(rng, head_sink, n);
        let tail_sink = self.1.collect_into(rng, tail_sink, n);
        S::unsplit(head_sink, tail_sink)
    }
}

// ============================================================================
// Marker trait for distributions that can be used directly in tuples
// (Unnest does not implement this, so it uses special tuple impls)
// ============================================================================

pub trait NotUnnest {}

impl<T: Copy + Splittable> NotUnnest for Constant<T> {}
impl NotUnnest for Range {}
impl<D, F, In, Out: Splittable> NotUnnest for Map<D, F, In, Out> {}
impl<D1: NotUnnest, D2: NotUnnest> NotUnnest for (D1, D2) {}
impl<D1: NotUnnest, D2: NotUnnest, D3: NotUnnest> NotUnnest for (D1, D2, D3) {}
impl<D1: NotUnnest, D2: NotUnnest, D3: NotUnnest, D4: NotUnnest> NotUnnest for (D1, D2, D3, D4) {}

// ============================================================================
// Tuple (D1, D2, D3) - col-by-col collection
// ============================================================================

pub struct TupleIter3<I1, I2, I3>(pub I1, pub I2, pub I3);

impl<I1: SampleIter, I2: SampleIter, I3: SampleIter> SampleIter for TupleIter3<I1, I2, I3> {
    type Item = (I1::Item, I2::Item, I3::Item);

    fn next(&mut self, rng: &mut impl Rng) -> Self::Item {
        (self.0.next(rng), self.1.next(rng), self.2.next(rng))
    }
}

impl<D1: Distribution, D2: Distribution + NotUnnest, D3: Distribution> Distribution
    for (D1, D2, D3)
{
    type Item = (D1::Item, D2::Item, D3::Item);
    type RowIter = TupleIter3<D1::RowIter, D2::RowIter, D3::RowIter>;

    fn row_iter(&mut self) -> Self::RowIter {
        TupleIter3(self.0.row_iter(), self.1.row_iter(), self.2.row_iter())
    }
}

impl<D1: ColumnDistribution, D2: ColumnDistribution + NotUnnest, D3: ColumnDistribution>
    ColumnDistribution for (D1, D2, D3)
where
    D1::Item: Splittable,
    D2::Item: Splittable,
    D3::Item: Splittable,
{
    /// Col-by-col collection: fills columns in order
    fn collect_into<S: Sink<(D1::Item, D2::Item, D3::Item)>>(
        &mut self,
        rng: &mut impl Rng,
        sink: S,
        n: usize,
    ) -> S
    where
        Self: Sized,
    {
        let (s1, tail) = sink.split();
        let s1 = self.0.collect_into(rng, s1, n);
        let (s2, s3) = tail.split();
        let s2 = self.1.collect_into(rng, s2, n);
        let s3 = self.2.collect_into(rng, s3, n);
        let tail = S::TailSink::unsplit(s2, s3);
        S::unsplit(s1, tail)
    }
}

// ============================================================================
// Tuple (D1, D2, D3, D4) - col-by-col collection
// ============================================================================

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

impl<D1: Distribution, D2: Distribution, D3: Distribution, D4: Distribution> Distribution
    for (D1, D2, D3, D4)
{
    type Item = (D1::Item, D2::Item, D3::Item, D4::Item);
    type RowIter = TupleIter4<D1::RowIter, D2::RowIter, D3::RowIter, D4::RowIter>;

    fn row_iter(&mut self) -> Self::RowIter {
        TupleIter4(
            self.0.row_iter(),
            self.1.row_iter(),
            self.2.row_iter(),
            self.3.row_iter(),
        )
    }
}

impl<D1: ColumnDistribution, D2: ColumnDistribution, D3: ColumnDistribution, D4: ColumnDistribution>
    ColumnDistribution for (D1, D2, D3, D4)
where
    D1::Item: Splittable,
    D2::Item: Splittable,
    D3::Item: Splittable,
    D4::Item: Splittable,
{
    /// Col-by-col collection: fills columns in order
    fn collect_into<S: Sink<(D1::Item, D2::Item, D3::Item, D4::Item)>>(
        &mut self,
        rng: &mut impl Rng,
        sink: S,
        n: usize,
    ) -> S
    where
        Self: Sized,
    {
        let (s1, tail) = sink.split();
        let s1 = self.0.collect_into(rng, s1, n);
        let (s2, tail2) = tail.split();
        let s2 = self.1.collect_into(rng, s2, n);
        let (s3, s4) = tail2.split();
        let s3 = self.2.collect_into(rng, s3, n);
        let s4 = self.3.collect_into(rng, s4, n);
        let tail2 =
            <S::TailSink as Sink<(D2::Item, D3::Item, D4::Item)>>::TailSink::unsplit(s3, s4);
        let tail = S::TailSink::unsplit(s2, tail2);
        S::unsplit(s1, tail)
    }
}

// ============================================================================
// Map
// ============================================================================

pub struct Map<D, F, In, Out> {
    pub source: D,
    pub f: F,
    pub _phantom: PhantomData<(In, Out)>,
}

pub fn map<D, F, In, Out>(source: D, f: F) -> Map<D, F, In, Out>
where
    F: Fn(In) -> Out,
{
    Map {
        source,
        f,
        _phantom: PhantomData,
    }
}

pub struct MapIter<I, F> {
    source: I,
    f: F,
}

impl<I, F, Out> SampleIter for MapIter<I, F>
where
    I: SampleIter,
    F: Fn(I::Item) -> Out,
{
    type Item = Out;

    fn next(&mut self, rng: &mut impl Rng) -> Self::Item {
        (self.f)(self.source.next(rng))
    }
}

impl<D, F, In, Out: Splittable> Distribution for Map<D, F, In, Out>
where
    D: Distribution<Item = In>,
    F: Fn(In) -> Out + Clone,
{
    type Item = Out;
    type RowIter = MapIter<D::RowIter, F>;

    fn row_iter(&mut self) -> Self::RowIter {
        MapIter {
            source: self.source.row_iter(),
            f: self.f.clone(),
        }
    }
}

impl<D, F, In, Out: Splittable> ColumnDistribution for Map<D, F, In, Out>
where
    D: Distribution<Item = In>,
    F: Fn(In) -> Out + Clone,
{
    /// Map must use row-by-row collection since the function couples input columns
    fn collect_into<S: Sink<Self::Item>>(&mut self, rng: &mut impl Rng, mut sink: S, n: usize) -> S
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

// ============================================================================
// Unnest - marker wrapper that tells tuples to expand the inner distribution
// ============================================================================

pub struct Unnest<D>(pub D);

pub fn unnest<D>(d: D) -> Unnest<D> {
    Unnest(d)
}

/// Unnest's iterator - just delegates to inner
pub struct UnnestIter<I>(I);

impl<I: SampleIter> SampleIter for UnnestIter<I> {
    type Item = I::Item;

    fn next(&mut self, rng: &mut impl Rng) -> Self::Item {
        self.0.next(rng)
    }
}

impl<D: Distribution> Distribution for Unnest<D> {
    type Item = D::Item;
    type RowIter = UnnestIter<D::RowIter>;

    fn row_iter(&mut self) -> Self::RowIter {
        UnnestIter(self.0.row_iter())
    }
}

impl<D: ColumnDistribution> ColumnDistribution for Unnest<D>
where
    D::Item: SplittableExt,
{
    /// Delegates to inner's collect_into - preserves col-by-col for independent columns
    fn collect_into<S: Sink<Self::Item>>(&mut self, rng: &mut impl Rng, sink: S, n: usize) -> S
    where
        Self: Sized,
    {
        self.0.collect_into(rng, sink, n)
    }
}

// ============================================================================
// (D1, Unnest<D2>, D3) where D2 produces (Head, Tail) -> produces (D1::Item, Head, Tail, D3::Item)
// ============================================================================

pub struct UnnestTupleIter3<I1, I2, I3> {
    i1: I1,
    i2: I2,
    i3: I3,
}

impl<I1, I2, I3> SampleIter for UnnestTupleIter3<I1, I2, I3>
where
    I1: SampleIter,
    I2: SampleIter,
    I2::Item: SplittableExt,
    I3: SampleIter,
{
    type Item = (
        I1::Item,
        <I2::Item as Splittable>::Head,
        <I2::Item as Splittable>::Tail,
        I3::Item,
    );

    fn next(&mut self, rng: &mut impl Rng) -> Self::Item {
        let v1 = self.i1.next(rng);
        let (head, tail) = self.i2.next(rng).split_head_tail();
        let v3 = self.i3.next(rng);
        (v1, head, tail, v3)
    }
}

impl<D1, D2, D3> Distribution for (D1, Unnest<D2>, D3)
where
    D1: Distribution,
    D2: Distribution,
    D2::Item: SplittableExt,
    D3: Distribution,
{
    type Item = (
        D1::Item,
        <D2::Item as Splittable>::Head,
        <D2::Item as Splittable>::Tail,
        D3::Item,
    );
    type RowIter = UnnestTupleIter3<D1::RowIter, D2::RowIter, D3::RowIter>;

    fn row_iter(&mut self) -> Self::RowIter {
        UnnestTupleIter3 {
            i1: self.0.row_iter(),
            i2: self.1.0.row_iter(),
            i3: self.2.row_iter(),
        }
    }
}

impl<D1, D2, H, T, D3> ColumnDistribution for (D1, Unnest<D2>, D3)
where
    D1: ColumnDistribution,
    D2: ColumnDistribution<Item = (H, T)>,
    H: Splittable,
    T: Splittable,
    D3: ColumnDistribution,
    D1::Item: Splittable,
    D3::Item: Splittable,
{
    fn collect_into<S: Sink<Self::Item>>(&mut self, rng: &mut impl Rng, sink: S, n: usize) -> S
    where
        Self: Sized,
    {
        // Split: S -> (S1, TailSink) where TailSink: Sink<(H, T, D3::Item)>
        let (s1, tail) = sink.split();

        // Fill D1 column-by-column
        let s1 = self.0.collect_into(rng, s1, n);

        // Split tail: TailSink -> (S2, Tail2) where S2: Sink<H>, Tail2: Sink<(T, D3::Item)>
        let (s2, tail2) = tail.split();

        // Split tail2: Tail2 -> (S3, S4) where S3: Sink<T>, S4: Sink<D3::Item>
        let (s3, s4) = tail2.split();

        // Unsplit (s2, s3) into a sink for (H, T) and pass to Unnest's collect_into
        let middle_sink = self.1.collect_into(rng, (s2, s3), n);
        let (s2, s3) = middle_sink;

        // Fill D3 column-by-column
        let s4 = self.2.collect_into(rng, s4, n);

        // Reconstruct the sink hierarchy
        let tail2 = <S::TailSink as Sink<(H, T, D3::Item)>>::TailSink::unsplit(s3, s4);
        let tail = S::TailSink::unsplit(s2, tail2);
        S::unsplit(s1, tail)
    }
}
