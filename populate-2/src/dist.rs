use crate::traits::{DefaultSink, Distribution, HAppend, HCons, HNil, SampleIter, Sink};
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

impl<T: Copy> SampleIter for ConstantIter<T> {
    type Item = T;
    fn next(&mut self, _rng: &mut impl Rng) -> Self::Item {
        self.value
    }
}

impl<T: Copy + DefaultSink> Distribution for Constant<T>
where
    T::Sink: Sink<T>,
{
    type Item = T;
    type RowIter = ConstantIter<T>;

    fn row_iter(&mut self) -> Self::RowIter {
        ConstantIter { value: self.value }
    }

    fn collect_into(
        &mut self,
        mut sink: <T as DefaultSink>::Sink,
        rng: &mut impl Rng,
        n: usize,
    ) -> <T as DefaultSink>::Sink {
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

impl<T: DefaultSink + Add<Output = T> + Copy> Distribution for Range<T>
where
    T::Sink: Sink<T>,
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
        mut sink: <T as DefaultSink>::Sink,
        rng: &mut impl Rng,
        n: usize,
    ) -> <T as DefaultSink>::Sink {
        let mut iter = self.row_iter();
        for _ in 0..n {
            sink.push(iter.next(rng));
        }
        sink
    }
}

//
// impl Distribution for Range {
//     type Item = i64;
//     type RowIter = RangeIter;
//
//     fn row_iter(&mut self) -> Self::RowIter {
//         RangeIter {
//             current: self.start,
//             step: self.step,
//         }
//     }
//
//     fn collect_into(
//         &mut self,
//         mut sink: HCons<Vec<i64>, HNil>,
//         rng: &mut impl Rng,
//         n: usize,
//     ) -> HCons<Vec<i64>, HNil> {
//         let mut iter = self.row_iter();
//         for _ in 0..n {
//             sink.push(iter.next(rng));
//         }
//         sink
//     }
// }

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

impl<D1: Distribution, D2: Distribution> Distribution for (D1, D2)
where
    <D1::Item as DefaultSink>::Sink: HAppend<<D2::Item as DefaultSink>::Sink>,
{
    type Item = (D1::Item, D2::Item);
    type RowIter = TupleIter2<D1::RowIter, D2::RowIter>;

    fn row_iter(&mut self) -> Self::RowIter {
        TupleIter2(self.0.row_iter(), self.1.row_iter())
    }

    fn collect_into(
        &mut self,
        sink: <(D1::Item, D2::Item) as DefaultSink>::Sink,
        rng: &mut impl Rng,
        n: usize,
    ) -> <(D1::Item, D2::Item) as DefaultSink>::Sink {
        type S1<D1> = <<D1 as Distribution>::Item as DefaultSink>::Sink;
        type S2<D2> = <<D2 as Distribution>::Item as DefaultSink>::Sink;
        let (s1, s2) = <S1<D1> as HAppend<S2<D2>>>::unappend(sink);
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

impl<D1: Distribution, D2: Distribution, D3: Distribution> Distribution for (D1, D2, D3)
where
    <D1::Item as DefaultSink>::Sink: HAppend<<D2::Item as DefaultSink>::Sink>,
    <<D1::Item as DefaultSink>::Sink as HAppend<<D2::Item as DefaultSink>::Sink>>::Output:
        HAppend<<D3::Item as DefaultSink>::Sink>,
{
    type Item = (D1::Item, D2::Item, D3::Item);
    type RowIter = TupleIter3<D1::RowIter, D2::RowIter, D3::RowIter>;

    fn row_iter(&mut self) -> Self::RowIter {
        TupleIter3(self.0.row_iter(), self.1.row_iter(), self.2.row_iter())
    }

    fn collect_into(
        &mut self,
        sink: <(D1::Item, D2::Item, D3::Item) as DefaultSink>::Sink,
        rng: &mut impl Rng,
        n: usize,
    ) -> <(D1::Item, D2::Item, D3::Item) as DefaultSink>::Sink {
        type S1<D1> = <<D1 as Distribution>::Item as DefaultSink>::Sink;
        type S2<D2> = <<D2 as Distribution>::Item as DefaultSink>::Sink;
        type S3<D3> = <<D3 as Distribution>::Item as DefaultSink>::Sink;
        type S12<D1, D2> = <S1<D1> as HAppend<S2<D2>>>::Output;
        let (s12, s3) = <S12<D1, D2> as HAppend<S3<D3>>>::unappend(sink);
        let (s1, s2) = <S1<D1> as HAppend<S2<D2>>>::unappend(s12);
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

impl<D1: Distribution, D2: Distribution, D3: Distribution, D4: Distribution> Distribution
    for (D1, D2, D3, D4)
where
    <D1::Item as DefaultSink>::Sink: HAppend<<D2::Item as DefaultSink>::Sink>,
    <<D1::Item as DefaultSink>::Sink as HAppend<<D2::Item as DefaultSink>::Sink>>::Output:
        HAppend<<D3::Item as DefaultSink>::Sink>,
    <<<D1::Item as DefaultSink>::Sink as HAppend<<D2::Item as DefaultSink>::Sink>>::Output
        as HAppend<<D3::Item as DefaultSink>::Sink>>::Output:
        HAppend<<D4::Item as DefaultSink>::Sink>,
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
        sink: <(D1::Item, D2::Item, D3::Item, D4::Item) as DefaultSink>::Sink,
        rng: &mut impl Rng,
        n: usize,
    ) -> <(D1::Item, D2::Item, D3::Item, D4::Item) as DefaultSink>::Sink {
        type S1<D1> = <<D1 as Distribution>::Item as DefaultSink>::Sink;
        type S2<D2> = <<D2 as Distribution>::Item as DefaultSink>::Sink;
        type S3<D3> = <<D3 as Distribution>::Item as DefaultSink>::Sink;
        type S4<D4> = <<D4 as Distribution>::Item as DefaultSink>::Sink;
        type S12<D1, D2> = <S1<D1> as HAppend<S2<D2>>>::Output;
        type S123<D1, D2, D3> = <S12<D1, D2> as HAppend<S3<D3>>>::Output;
        let (s123, s4) = <S123<D1, D2, D3> as HAppend<S4<D4>>>::unappend(sink);
        let (s12, s3) = <S12<D1, D2> as HAppend<S3<D3>>>::unappend(s123);
        let (s1, s2) = <S1<D1> as HAppend<S2<D2>>>::unappend(s12);
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

impl<D, F, In, Out: DefaultSink> Distribution for Map<D, F, In, Out>
where
    D: Distribution<Item = In>,
    F: Fn(In) -> Out + Clone,
    Out::Sink: Sink<Out>,
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
        mut sink: <Out as DefaultSink>::Sink,
        rng: &mut impl Rng,
        n: usize,
    ) -> <Out as DefaultSink>::Sink {
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

    fn collect_into(
        &mut self,
        mut sink: HCons<crate::traits::VecListSink<E::Item>, HNil>,
        rng: &mut impl Rng,
        n: usize,
    ) -> HCons<crate::traits::VecListSink<E::Item>, HNil> {
        let mut len_iter = self.lengths.row_iter();
        let elem_base = self.elements.row_iter();
        for _ in 0..n {
            let len = len_iter.next(rng);
            let mut elems = elem_base.clone();
            sink.0.push_list(len, (0..len).map(|_| elems.next(rng)));
        }
        sink
    }
}
