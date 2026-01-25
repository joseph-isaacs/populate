use crate::traits::{Distribution, SampleIter, Sink, Splittable};
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

impl<T: Copy + 'static> SampleIter for ConstantIter<T> {
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

// ============================================================================
// Tuple (D1, D2) - col-by-col collection using split/unsplit
// ============================================================================

pub struct TupleIter2<I1, I2>(pub I1, pub I2);

impl<I1: SampleIter, I2: SampleIter> SampleIter for TupleIter2<I1, I2>
where
    I1::Item: 'static,
    I2::Item: 'static,
{
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
// Tuple (D1, D2, D3) - col-by-col collection
// ============================================================================

pub struct TupleIter3<I1, I2, I3>(pub I1, pub I2, pub I3);

impl<I1: SampleIter, I2: SampleIter, I3: SampleIter> SampleIter for TupleIter3<I1, I2, I3>
where
    I1::Item: 'static,
    I2::Item: 'static,
    I3::Item: 'static,
{
    type Item = (I1::Item, I2::Item, I3::Item);

    fn next(&mut self, rng: &mut impl Rng) -> Self::Item {
        (self.0.next(rng), self.1.next(rng), self.2.next(rng))
    }
}

impl<D1: Distribution, D2: Distribution, D3: Distribution> Distribution for (D1, D2, D3) {
    type Item = (D1::Item, D2::Item, D3::Item);
    type RowIter = TupleIter3<D1::RowIter, D2::RowIter, D3::RowIter>;

    fn row_iter(&mut self) -> Self::RowIter {
        TupleIter3(self.0.row_iter(), self.1.row_iter(), self.2.row_iter())
    }

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
where
    I1::Item: 'static,
    I2::Item: 'static,
    I3::Item: 'static,
    I4::Item: 'static,
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

impl<I, F, Out: 'static> SampleIter for MapIter<I, F>
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
