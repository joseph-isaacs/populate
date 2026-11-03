use crate::traits::{
    Backend, DefaultSink, Distribution, Flatten, HAppend, HCons, HNil, ListCol, SampleIter, Sink,
};
use rand::Rng;
use std::marker::PhantomData;
use std::ops::Add;
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

impl<T: Clone> SampleIter for ConstantIter<T> {
    type Item = T;
    fn next(&mut self, _rng: &mut impl Rng) -> Self::Item {
        self.value.clone()
    }
}

impl<B: Backend, T: Clone + DefaultSink<B>> Distribution<B> for Constant<T>
where
    <T as DefaultSink<B>>::Sink: Sink<T>,
{
    type Item = T;
    type RowIter = ConstantIter<T>;

    fn row_iter(&mut self) -> Self::RowIter {
        ConstantIter {
            value: self.value.clone(),
        }
    }

    fn collect_into(
        &mut self,
        mut sink: <T as DefaultSink<B>>::Sink,
        rng: &mut impl Rng,
        n: usize,
    ) -> <T as DefaultSink<B>>::Sink {
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

pub struct Range<T> {
    pub start: T,
    pub step: T,
}

pub fn range<T: Add>(start: T, step: T) -> Range<T> {
    Range { start, step }
}

#[derive(Clone)]
pub struct RangeIter<T> {
    pub current: T,
    pub step: T,
}

impl<T: Add<Output = T> + Copy> SampleIter for RangeIter<T> {
    type Item = T;
    fn next(&mut self, _rng: &mut impl Rng) -> Self::Item {
        let v = self.current;
        self.current = self.current + self.step;
        v
    }
}

impl<B: Backend, T: DefaultSink<B> + Add<Output = T> + Copy> Distribution<B> for Range<T>
where
    <T as DefaultSink<B>>::Sink: Sink<T>,
{
    type Item = T;
    type RowIter = RangeIter<T>;

    fn row_iter(&mut self) -> Self::RowIter {
        RangeIter {
            current: self.start,
            step: self.step,
        }
    }

    fn collect_into(
        &mut self,
        mut sink: <T as DefaultSink<B>>::Sink,
        rng: &mut impl Rng,
        n: usize,
    ) -> <T as DefaultSink<B>>::Sink {
        let mut iter = self.row_iter();
        for _ in 0..n {
            sink.push(iter.next(rng));
        }
        sink
    }
}

// ============================================================================
// Tuple (D1, D2) — col-by-col using HAppend
// ============================================================================

pub struct TupleIter2<I1, I2>(pub I1, pub I2);

impl<I1: SampleIter, I2: SampleIter> SampleIter for TupleIter2<I1, I2> {
    type Item = (I1::Item, I2::Item);

    fn next(&mut self, rng: &mut impl Rng) -> Self::Item {
        (self.0.next(rng), self.1.next(rng))
    }
}

impl<B: Backend, D1: Distribution<B>, D2: Distribution<B>> Distribution<B> for (D1, D2)
where
    <D1::Item as DefaultSink<B>>::Sink: HAppend<<D2::Item as DefaultSink<B>>::Sink>,
{
    type Item = (D1::Item, D2::Item);
    type RowIter = TupleIter2<D1::RowIter, D2::RowIter>;

    fn row_iter(&mut self) -> Self::RowIter {
        TupleIter2(self.0.row_iter(), self.1.row_iter())
    }

    fn collect_into(
        &mut self,
        sink: <(D1::Item, D2::Item) as DefaultSink<B>>::Sink,
        rng: &mut impl Rng,
        n: usize,
    ) -> <(D1::Item, D2::Item) as DefaultSink<B>>::Sink {
        type S1<B, D1> = <<D1 as Distribution<B>>::Item as DefaultSink<B>>::Sink;
        type S2<B, D2> = <<D2 as Distribution<B>>::Item as DefaultSink<B>>::Sink;
        let (s1, s2) = <S1<B, D1> as HAppend<S2<B, D2>>>::unappend(sink);
        let s1 = self.0.collect_into(s1, rng, n);
        let s2 = self.1.collect_into(s2, rng, n);
        s1.append(s2)
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

impl<B: Backend, D1: Distribution<B>, D2: Distribution<B>, D3: Distribution<B>> Distribution<B>
    for (D1, D2, D3)
where
    <D1::Item as DefaultSink<B>>::Sink: HAppend<<D2::Item as DefaultSink<B>>::Sink>,
    <<D1::Item as DefaultSink<B>>::Sink as HAppend<<D2::Item as DefaultSink<B>>::Sink>>::Output:
        HAppend<<D3::Item as DefaultSink<B>>::Sink>,
{
    type Item = (D1::Item, D2::Item, D3::Item);
    type RowIter = TupleIter3<D1::RowIter, D2::RowIter, D3::RowIter>;

    fn row_iter(&mut self) -> Self::RowIter {
        TupleIter3(self.0.row_iter(), self.1.row_iter(), self.2.row_iter())
    }

    fn collect_into(
        &mut self,
        sink: <(D1::Item, D2::Item, D3::Item) as DefaultSink<B>>::Sink,
        rng: &mut impl Rng,
        n: usize,
    ) -> <(D1::Item, D2::Item, D3::Item) as DefaultSink<B>>::Sink {
        type S1<B, D1> = <<D1 as Distribution<B>>::Item as DefaultSink<B>>::Sink;
        type S2<B, D2> = <<D2 as Distribution<B>>::Item as DefaultSink<B>>::Sink;
        type S3<B, D3> = <<D3 as Distribution<B>>::Item as DefaultSink<B>>::Sink;
        type S12<B, D1, D2> = <S1<B, D1> as HAppend<S2<B, D2>>>::Output;
        let (s12, s3) = <S12<B, D1, D2> as HAppend<S3<B, D3>>>::unappend(sink);
        let (s1, s2) = <S1<B, D1> as HAppend<S2<B, D2>>>::unappend(s12);
        let s1 = self.0.collect_into(s1, rng, n);
        let s2 = self.1.collect_into(s2, rng, n);
        let s3 = self.2.collect_into(s3, rng, n);
        s1.append(s2).append(s3)
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

impl<
        B: Backend,
        D1: Distribution<B>,
        D2: Distribution<B>,
        D3: Distribution<B>,
        D4: Distribution<B>,
    > Distribution<B> for (D1, D2, D3, D4)
where
    <D1::Item as DefaultSink<B>>::Sink: HAppend<<D2::Item as DefaultSink<B>>::Sink>,
    <<D1::Item as DefaultSink<B>>::Sink as HAppend<<D2::Item as DefaultSink<B>>::Sink>>::Output:
        HAppend<<D3::Item as DefaultSink<B>>::Sink>,
    <<<D1::Item as DefaultSink<B>>::Sink as HAppend<<D2::Item as DefaultSink<B>>::Sink>>::Output
        as HAppend<<D3::Item as DefaultSink<B>>::Sink>>::Output:
        HAppend<<D4::Item as DefaultSink<B>>::Sink>,
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

    fn collect_into(
        &mut self,
        sink: <(D1::Item, D2::Item, D3::Item, D4::Item) as DefaultSink<B>>::Sink,
        rng: &mut impl Rng,
        n: usize,
    ) -> <(D1::Item, D2::Item, D3::Item, D4::Item) as DefaultSink<B>>::Sink {
        type S1<B, D1> = <<D1 as Distribution<B>>::Item as DefaultSink<B>>::Sink;
        type S2<B, D2> = <<D2 as Distribution<B>>::Item as DefaultSink<B>>::Sink;
        type S3<B, D3> = <<D3 as Distribution<B>>::Item as DefaultSink<B>>::Sink;
        type S4<B, D4> = <<D4 as Distribution<B>>::Item as DefaultSink<B>>::Sink;
        type S12<B, D1, D2> = <S1<B, D1> as HAppend<S2<B, D2>>>::Output;
        type S123<B, D1, D2, D3> = <S12<B, D1, D2> as HAppend<S3<B, D3>>>::Output;
        let (s123, s4) = <S123<B, D1, D2, D3> as HAppend<S4<B, D4>>>::unappend(sink);
        let (s12, s3) = <S12<B, D1, D2> as HAppend<S3<B, D3>>>::unappend(s123);
        let (s1, s2) = <S1<B, D1> as HAppend<S2<B, D2>>>::unappend(s12);
        let s1 = self.0.collect_into(s1, rng, n);
        let s2 = self.1.collect_into(s2, rng, n);
        let s3 = self.2.collect_into(s3, rng, n);
        let s4 = self.3.collect_into(s4, rng, n);
        s1.append(s2).append(s3).append(s4)
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

impl<B: Backend, D, F, In, Out: DefaultSink<B>> Distribution<B> for Map<D, F, In, Out>
where
    D: Distribution<B, Item = In>,
    F: Fn(In) -> Out + Clone,
    <Out as DefaultSink<B>>::Sink: Sink<Out>,
{
    type Item = Out;
    type RowIter = MapIter<D::RowIter, F>;

    fn row_iter(&mut self) -> Self::RowIter {
        MapIter {
            source: self.source.row_iter(),
            f: self.f.clone(),
        }
    }

    fn collect_into(
        &mut self,
        mut sink: <Out as DefaultSink<B>>::Sink,
        rng: &mut impl Rng,
        n: usize,
    ) -> <Out as DefaultSink<B>>::Sink {
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
    E: SampleIter,
{
    type Item = Vec<E::Item>;

    // Element stream continues across rows (same semantics as columnar path).
    fn next(&mut self, rng: &mut impl Rng) -> Vec<E::Item> {
        let len = self.lengths.next(rng);
        (0..len).map(|_| self.elements.next(rng)).collect()
    }
}

impl<B: Backend, L, E> Distribution<B> for List<L, E>
where
    L: Distribution<B, Item = usize>,
    E: Distribution<B>,
{
    type Item = Vec<E::Item>;
    type RowIter = ListIter<L::RowIter, E::RowIter>;

    fn row_iter(&mut self) -> Self::RowIter {
        ListIter {
            lengths: self.lengths.row_iter(),
            elements: self.elements.row_iter(),
        }
    }

    // Fully columnar:
    //   pass 1: n lengths -> offsets column (+ total element count)
    //   pass 2: hand the *whole* child sink to the element distribution for
    //           `total` rows, so tuples/lists inside recurse col-by-col.
    fn collect_into(
        &mut self,
        sink: <Vec<E::Item> as DefaultSink<B>>::Sink,
        rng: &mut impl Rng,
        n: usize,
    ) -> <Vec<E::Item> as DefaultSink<B>>::Sink {
        let HCons(
            ListCol {
                mut offsets,
                values,
                mut len,
            },
            HNil,
        ) = sink;
        let mut lengths = self.lengths.row_iter();
        let start = len;
        for _ in 0..n {
            len += lengths.next(rng);
            offsets.push(len);
        }
        let values = self.elements.collect_into(values, rng, len - start);
        HCons(
            ListCol {
                offsets,
                values,
                len,
            },
            HNil,
        )
    }
}

// ============================================================================
// Flatten — free function wrapper (avoids UFCS for generic-B contexts)
// ============================================================================

pub fn flatten<D>(d: D) -> Flatten<D> {
    Flatten(d)
}

// ============================================================================
// Leaf — any clonable SampleIter as a single-column Distribution
// ============================================================================

/// A single-column distribution backed by a clonable [`SampleIter`]:
/// `row_iter` clones the iterator, `collect_into` pushes `n` draws. This is
/// the one-liner way to turn a custom sampler into a column.
#[derive(Clone, Debug)]
pub struct Leaf<I>(pub I);

pub fn leaf<I: SampleIter + Clone>(iter: I) -> Leaf<I> {
    Leaf(iter)
}

impl<B: Backend, I> Distribution<B> for Leaf<I>
where
    I: SampleIter + Clone,
    I::Item: DefaultSink<B>,
    <I::Item as DefaultSink<B>>::Sink: Sink<I::Item>,
{
    type Item = I::Item;
    type RowIter = I;

    fn row_iter(&mut self) -> I {
        self.0.clone()
    }

    fn collect_into(
        &mut self,
        mut sink: <I::Item as DefaultSink<B>>::Sink,
        rng: &mut impl Rng,
        n: usize,
    ) -> <I::Item as DefaultSink<B>>::Sink {
        let mut iter = self.0.clone();
        for _ in 0..n {
            sink.push(iter.next(rng));
        }
        sink
    }
}
