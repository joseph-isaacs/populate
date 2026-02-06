use crate::traits::{
    CollectInto, Distribution, HSplitAt, HasWidth, ListSink, N1, SampleIter, Sink,
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

#[derive(Clone)]
pub struct ConstantIter<T> {
    value: T,
}

impl<T: Copy> SampleIter for ConstantIter<T> {
    type Item = T;
    fn next(&mut self, _rng: &mut impl Rng) -> Self::Item {
        self.value
    }
}

impl<T: Copy> Distribution for Constant<T> {
    type Item = T;
    type RowIter = ConstantIter<T>;

    fn row_iter(&mut self) -> Self::RowIter {
        ConstantIter { value: self.value }
    }
}

impl<T: Copy + HasWidth> HasWidth for Constant<T> {
    type Width = T::Width;
}

impl<T: Copy, S: Sink<T>> CollectInto<S> for Constant<T> {
    fn collect_into(&mut self, mut sink: S, rng: &mut impl Rng, n: usize) -> S {
        let mut iter = self.row_iter();
        for _ in 0..n {
            sink.push(iter.next(rng));
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

#[derive(Clone)]
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

impl HasWidth for Range {
    type Width = N1;
}

impl<S: Sink<i64>> CollectInto<S> for Range {
    fn collect_into(&mut self, mut sink: S, rng: &mut impl Rng, n: usize) -> S {
        let mut iter = self.row_iter();
        for _ in 0..n {
            sink.push(iter.next(rng));
        }
        sink
    }
}

// ============================================================================
// Tuple (D1, D2) — col-by-col using HList sink + HSplitAt
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

impl<D1, D2, S> CollectInto<S> for (D1, D2)
where
    D1: HasWidth,
    S: HSplitAt<D1::Width>,
    D1: CollectInto<<S as HSplitAt<D1::Width>>::Left>,
    D2: CollectInto<<S as HSplitAt<D1::Width>>::Right>,
{
    fn collect_into(&mut self, sink: S, rng: &mut impl Rng, n: usize) -> S {
        let (s1, s2) = sink.split();
        let s1 = self.0.collect_into(s1, rng, n);
        let s2 = self.1.collect_into(s2, rng, n);
        S::unsplit(s1, s2)
    }
}

// ============================================================================
// Tuple (D1, D2, D3) — col-by-col
// ============================================================================

pub struct TupleIter3<I1, I2, I3>(pub I1, pub I2, pub I3);

impl<I1: SampleIter, I2: SampleIter, I3: SampleIter> SampleIter for TupleIter3<I1, I2, I3> {
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
}

impl<D1, D2, D3, S> CollectInto<S> for (D1, D2, D3)
where
    D1: HasWidth,
    D2: HasWidth,
    S: HSplitAt<D1::Width>,
    <S as HSplitAt<D1::Width>>::Right: HSplitAt<D2::Width>,
    D1: CollectInto<<S as HSplitAt<D1::Width>>::Left>,
    D2: CollectInto<<<S as HSplitAt<D1::Width>>::Right as HSplitAt<D2::Width>>::Left>,
    D3: CollectInto<<<S as HSplitAt<D1::Width>>::Right as HSplitAt<D2::Width>>::Right>,
{
    fn collect_into(&mut self, sink: S, rng: &mut impl Rng, n: usize) -> S {
        let (s1, rest) = sink.split();
        let (s2, s3) = rest.split();
        let s1 = self.0.collect_into(s1, rng, n);
        let s2 = self.1.collect_into(s2, rng, n);
        let s3 = self.2.collect_into(s3, rng, n);
        let rest = <<S as HSplitAt<D1::Width>>::Right>::unsplit(s2, s3);
        S::unsplit(s1, rest)
    }
}

// ============================================================================
// Tuple (D1, D2, D3, D4) — col-by-col
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

impl<D1, D2, D3, D4, S> CollectInto<S> for (D1, D2, D3, D4)
where
    D1: HasWidth,
    D2: HasWidth,
    D3: HasWidth,
    S: HSplitAt<D1::Width>,
    <S as HSplitAt<D1::Width>>::Right: HSplitAt<D2::Width>,
    <<S as HSplitAt<D1::Width>>::Right as HSplitAt<D2::Width>>::Right: HSplitAt<D3::Width>,
    D1: CollectInto<<S as HSplitAt<D1::Width>>::Left>,
    D2: CollectInto<<<S as HSplitAt<D1::Width>>::Right as HSplitAt<D2::Width>>::Left>,
    D3: CollectInto<
        <<<S as HSplitAt<D1::Width>>::Right as HSplitAt<D2::Width>>::Right as HSplitAt<
            D3::Width,
        >>::Left,
    >,
    D4: CollectInto<
        <<<S as HSplitAt<D1::Width>>::Right as HSplitAt<D2::Width>>::Right as HSplitAt<
            D3::Width,
        >>::Right,
    >,
{
    fn collect_into(&mut self, sink: S, rng: &mut impl Rng, n: usize) -> S {
        let (s1, rest) = sink.split();
        let (s2, rest) = rest.split();
        let (s3, s4) = rest.split();
        let s1 = self.0.collect_into(s1, rng, n);
        let s2 = self.1.collect_into(s2, rng, n);
        let s3 = self.2.collect_into(s3, rng, n);
        let s4 = self.3.collect_into(s4, rng, n);
        let rest =
            <<<S as HSplitAt<D1::Width>>::Right as HSplitAt<D2::Width>>::Right>::unsplit(s3, s4);
        let rest = <<S as HSplitAt<D1::Width>>::Right>::unsplit(s2, rest);
        S::unsplit(s1, rest)
    }
}

// ============================================================================
// Map — row-by-row collection via Sink<Out>::push
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

impl<D, F, In, Out> Distribution for Map<D, F, In, Out>
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

impl<D, F, In, Out: HasWidth> HasWidth for Map<D, F, In, Out> {
    type Width = Out::Width;
}

impl<D, F, In, Out, S: Sink<Out>> CollectInto<S> for Map<D, F, In, Out>
where
    D: Distribution<Item = In>,
    F: Fn(In) -> Out + Clone,
{
    fn collect_into(&mut self, mut sink: S, rng: &mut impl Rng, n: usize) -> S {
        let mut iter = self.row_iter();
        for _ in 0..n {
            sink.push(iter.next(rng));
        }
        sink
    }
}

// ============================================================================
// List<L, E> — list/array column distribution
// ============================================================================

pub struct List<L, E> {
    pub lengths: L,
    pub elements: E,
}

pub fn list<L, E>(lengths: L, elements: E) -> List<L, E> {
    List { lengths, elements }
}

pub struct ListIter<L, E> {
    pub lengths: L,
    pub elements: E,
}

impl<L, E> SampleIter for ListIter<L, E>
where
    L: SampleIter<Item = usize>,
    E: SampleIter + Clone,
{
    type Item = Vec<E::Item>;

    fn next(&mut self, rng: &mut impl Rng) -> Vec<E::Item> {
        let len = self.lengths.next(rng);
        let mut elems = self.elements.clone();
        (0..len).map(|_| elems.next(rng)).collect()
    }
}

impl<L, E> Distribution for List<L, E>
where
    L: Distribution<Item = usize>,
    E: Distribution,
    E::RowIter: Clone,
{
    type Item = Vec<E::Item>;
    type RowIter = ListIter<L::RowIter, E::RowIter>;

    fn row_iter(&mut self) -> Self::RowIter {
        ListIter {
            lengths: self.lengths.row_iter(),
            elements: self.elements.row_iter(),
        }
    }
}

impl<L, E> HasWidth for List<L, E> {
    type Width = N1;
}

impl<L, E, S> CollectInto<S> for List<L, E>
where
    L: Distribution<Item = usize>,
    E: Distribution,
    E::RowIter: Clone,
    S: ListSink<E::Item>,
{
    fn collect_into(&mut self, mut sink: S, rng: &mut impl Rng, n: usize) -> S {
        let mut len_iter = self.lengths.row_iter();
        let elem_base = self.elements.row_iter();
        for _ in 0..n {
            let len = len_iter.next(rng);
            let mut elems = elem_base.clone();
            sink.push_list(len, (0..len).map(|_| elems.next(rng)));
        }
        sink
    }
}
