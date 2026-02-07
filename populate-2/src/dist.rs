use crate::traits::{
    Backend, Distribution, HAppend, IntoTuple, ListSink, SampleIter, Sink, SinkFor, ValueHList,
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

impl<T: Copy> SampleIter for ConstantIter<T> {
    type Item = T;
    fn next(&mut self, _rng: &mut impl Rng) -> Self::Item {
        self.value
    }
}

impl<B: Backend, T: Copy + SinkFor<B>> Distribution<B> for Constant<T> {
    type Value = T;
    type Sink = <T as SinkFor<B>>::Sink;
    type Iter = ConstantIter<T>;

    fn row_iter(&mut self) -> Self::Iter {
        ConstantIter { value: self.value }
    }

    fn default_sink(&self) -> Self::Sink {
        Default::default()
    }

    fn collect_into(&mut self, mut sink: Self::Sink, rng: &mut impl Rng, n: usize) -> Self::Sink {
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

impl<B: Backend, T: SinkFor<B> + Add<Output = T> + Copy> Distribution<B> for Range<T> {
    type Value = T;
    type Sink = <T as SinkFor<B>>::Sink;
    type Iter = RangeIter<T>;

    fn row_iter(&mut self) -> Self::Iter {
        RangeIter {
            current: self.start,
            step: self.step,
        }
    }

    fn default_sink(&self) -> Self::Sink {
        Default::default()
    }

    fn collect_into(&mut self, mut sink: Self::Sink, rng: &mut impl Rng, n: usize) -> Self::Sink {
        let mut iter = self.row_iter();
        for _ in 0..n {
            sink.push(iter.next(rng));
        }
        sink
    }
}

// ============================================================================
// Tuple (D1, D2) — col-by-col with auto-flatten
// ============================================================================

pub struct TupleIter2<I1, I2>(pub I1, pub I2);

impl<I1: SampleIter, I2: SampleIter> SampleIter for TupleIter2<I1, I2>
where
    I1::Item: ValueHList,
    I2::Item: ValueHList,
    <I1::Item as ValueHList>::HList: HAppend<<I2::Item as ValueHList>::HList>,
    <<I1::Item as ValueHList>::HList as HAppend<<I2::Item as ValueHList>::HList>>::Output:
        IntoTuple,
{
    type Item = <<<I1::Item as ValueHList>::HList as HAppend<<I2::Item as ValueHList>::HList>>::Output as IntoTuple>::Tuple;

    fn next(&mut self, rng: &mut impl Rng) -> Self::Item {
        self.0
            .next(rng)
            .into_hlist()
            .append(self.1.next(rng).into_hlist())
            .into_tuple()
    }
}

impl<B: Backend, D1: Distribution<B>, D2: Distribution<B>> Distribution<B> for (D1, D2)
where
    D1::Value: ValueHList,
    D2::Value: ValueHList,
    D1::Sink: HAppend<D2::Sink>,
    <D1::Value as ValueHList>::HList: HAppend<<D2::Value as ValueHList>::HList>,
    <<D1::Value as ValueHList>::HList as HAppend<<D2::Value as ValueHList>::HList>>::Output:
        IntoTuple,
{
    type Value = <<<D1::Value as ValueHList>::HList as HAppend<
        <D2::Value as ValueHList>::HList,
    >>::Output as IntoTuple>::Tuple;
    type Sink = <D1::Sink as HAppend<D2::Sink>>::Output;
    type Iter = TupleIter2<D1::Iter, D2::Iter>;

    fn row_iter(&mut self) -> Self::Iter {
        TupleIter2(self.0.row_iter(), self.1.row_iter())
    }

    fn default_sink(&self) -> Self::Sink {
        self.0.default_sink().append(self.1.default_sink())
    }

    fn collect_into(&mut self, sink: Self::Sink, rng: &mut impl Rng, n: usize) -> Self::Sink {
        let (s1, s2) = <D1::Sink as HAppend<D2::Sink>>::unappend(sink);
        let s1 = self.0.collect_into(s1, rng, n);
        let s2 = self.1.collect_into(s2, rng, n);
        s1.append(s2)
    }
}

// ============================================================================
// Tuple (D1, D2, D3) — col-by-col with auto-flatten
// ============================================================================

pub struct TupleIter3<I1, I2, I3>(pub I1, pub I2, pub I3);

impl<I1: SampleIter, I2: SampleIter, I3: SampleIter> SampleIter for TupleIter3<I1, I2, I3>
where
    I1::Item: ValueHList,
    I2::Item: ValueHList,
    I3::Item: ValueHList,
    <I1::Item as ValueHList>::HList: HAppend<<I2::Item as ValueHList>::HList>,
    <<I1::Item as ValueHList>::HList as HAppend<<I2::Item as ValueHList>::HList>>::Output:
        HAppend<<I3::Item as ValueHList>::HList>,
    <<<I1::Item as ValueHList>::HList as HAppend<<I2::Item as ValueHList>::HList>>::Output
        as HAppend<<I3::Item as ValueHList>::HList>>::Output: IntoTuple,
{
    type Item = <<<<I1::Item as ValueHList>::HList as HAppend<
        <I2::Item as ValueHList>::HList,
    >>::Output as HAppend<<I3::Item as ValueHList>::HList>>::Output as IntoTuple>::Tuple;

    fn next(&mut self, rng: &mut impl Rng) -> Self::Item {
        self.0
            .next(rng)
            .into_hlist()
            .append(self.1.next(rng).into_hlist())
            .append(self.2.next(rng).into_hlist())
            .into_tuple()
    }
}

impl<B: Backend, D1: Distribution<B>, D2: Distribution<B>, D3: Distribution<B>> Distribution<B>
    for (D1, D2, D3)
where
    D1::Value: ValueHList,
    D2::Value: ValueHList,
    D3::Value: ValueHList,
    D1::Sink: HAppend<D2::Sink>,
    <D1::Sink as HAppend<D2::Sink>>::Output: HAppend<D3::Sink>,
    <D1::Value as ValueHList>::HList: HAppend<<D2::Value as ValueHList>::HList>,
    <<D1::Value as ValueHList>::HList as HAppend<<D2::Value as ValueHList>::HList>>::Output:
        HAppend<<D3::Value as ValueHList>::HList>,
    <<<D1::Value as ValueHList>::HList as HAppend<<D2::Value as ValueHList>::HList>>::Output
        as HAppend<<D3::Value as ValueHList>::HList>>::Output: IntoTuple,
{
    type Value = <<<<D1::Value as ValueHList>::HList as HAppend<
        <D2::Value as ValueHList>::HList,
    >>::Output as HAppend<<D3::Value as ValueHList>::HList>>::Output
        as IntoTuple>::Tuple;
    type Sink =
        <<D1::Sink as HAppend<D2::Sink>>::Output as HAppend<D3::Sink>>::Output;
    type Iter = TupleIter3<D1::Iter, D2::Iter, D3::Iter>;

    fn row_iter(&mut self) -> Self::Iter {
        TupleIter3(self.0.row_iter(), self.1.row_iter(), self.2.row_iter())
    }

    fn default_sink(&self) -> Self::Sink {
        self.0
            .default_sink()
            .append(self.1.default_sink())
            .append(self.2.default_sink())
    }

    fn collect_into(
        &mut self,
        sink: Self::Sink,
        rng: &mut impl Rng,
        n: usize,
    ) -> Self::Sink {
        type S12<B, D1, D2> =
            <<D1 as Distribution<B>>::Sink as HAppend<<D2 as Distribution<B>>::Sink>>::Output;
        let (s12, s3) = <S12<B, D1, D2> as HAppend<D3::Sink>>::unappend(sink);
        let (s1, s2) = <D1::Sink as HAppend<D2::Sink>>::unappend(s12);
        let s1 = self.0.collect_into(s1, rng, n);
        let s2 = self.1.collect_into(s2, rng, n);
        let s3 = self.2.collect_into(s3, rng, n);
        s1.append(s2).append(s3)
    }
}

// ============================================================================
// Tuple (D1, D2, D3, D4) — col-by-col with auto-flatten
// ============================================================================

pub struct TupleIter4<I1, I2, I3, I4>(pub I1, pub I2, pub I3, pub I4);

impl<I1: SampleIter, I2: SampleIter, I3: SampleIter, I4: SampleIter> SampleIter
    for TupleIter4<I1, I2, I3, I4>
where
    I1::Item: ValueHList,
    I2::Item: ValueHList,
    I3::Item: ValueHList,
    I4::Item: ValueHList,
    <I1::Item as ValueHList>::HList: HAppend<<I2::Item as ValueHList>::HList>,
    <<I1::Item as ValueHList>::HList as HAppend<<I2::Item as ValueHList>::HList>>::Output:
        HAppend<<I3::Item as ValueHList>::HList>,
    <<<I1::Item as ValueHList>::HList as HAppend<<I2::Item as ValueHList>::HList>>::Output
        as HAppend<<I3::Item as ValueHList>::HList>>::Output:
        HAppend<<I4::Item as ValueHList>::HList>,
    <<<<I1::Item as ValueHList>::HList as HAppend<<I2::Item as ValueHList>::HList>>::Output
        as HAppend<<I3::Item as ValueHList>::HList>>::Output
        as HAppend<<I4::Item as ValueHList>::HList>>::Output: IntoTuple,
{
    type Item = <<<<<I1::Item as ValueHList>::HList as HAppend<
        <I2::Item as ValueHList>::HList,
    >>::Output as HAppend<<I3::Item as ValueHList>::HList>>::Output
        as HAppend<<I4::Item as ValueHList>::HList>>::Output
        as IntoTuple>::Tuple;

    fn next(&mut self, rng: &mut impl Rng) -> Self::Item {
        self.0
            .next(rng)
            .into_hlist()
            .append(self.1.next(rng).into_hlist())
            .append(self.2.next(rng).into_hlist())
            .append(self.3.next(rng).into_hlist())
            .into_tuple()
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
    D1::Value: ValueHList,
    D2::Value: ValueHList,
    D3::Value: ValueHList,
    D4::Value: ValueHList,
    D1::Sink: HAppend<D2::Sink>,
    <D1::Sink as HAppend<D2::Sink>>::Output: HAppend<D3::Sink>,
    <<D1::Sink as HAppend<D2::Sink>>::Output as HAppend<D3::Sink>>::Output:
        HAppend<D4::Sink>,
    <D1::Value as ValueHList>::HList: HAppend<<D2::Value as ValueHList>::HList>,
    <<D1::Value as ValueHList>::HList as HAppend<<D2::Value as ValueHList>::HList>>::Output:
        HAppend<<D3::Value as ValueHList>::HList>,
    <<<D1::Value as ValueHList>::HList as HAppend<<D2::Value as ValueHList>::HList>>::Output
        as HAppend<<D3::Value as ValueHList>::HList>>::Output:
        HAppend<<D4::Value as ValueHList>::HList>,
    <<<<D1::Value as ValueHList>::HList as HAppend<<D2::Value as ValueHList>::HList>>::Output
        as HAppend<<D3::Value as ValueHList>::HList>>::Output
        as HAppend<<D4::Value as ValueHList>::HList>>::Output: IntoTuple,
{
    type Value = <<<<<D1::Value as ValueHList>::HList as HAppend<
        <D2::Value as ValueHList>::HList,
    >>::Output as HAppend<<D3::Value as ValueHList>::HList>>::Output
        as HAppend<<D4::Value as ValueHList>::HList>>::Output
        as IntoTuple>::Tuple;
    type Sink = <<<D1::Sink as HAppend<D2::Sink>>::Output as HAppend<D3::Sink>>::Output
        as HAppend<D4::Sink>>::Output;
    type Iter = TupleIter4<D1::Iter, D2::Iter, D3::Iter, D4::Iter>;

    fn row_iter(&mut self) -> Self::Iter {
        TupleIter4(
            self.0.row_iter(),
            self.1.row_iter(),
            self.2.row_iter(),
            self.3.row_iter(),
        )
    }

    fn default_sink(&self) -> Self::Sink {
        self.0
            .default_sink()
            .append(self.1.default_sink())
            .append(self.2.default_sink())
            .append(self.3.default_sink())
    }

    fn collect_into(
        &mut self,
        sink: Self::Sink,
        rng: &mut impl Rng,
        n: usize,
    ) -> Self::Sink {
        type S12<B, D1, D2> =
            <<D1 as Distribution<B>>::Sink as HAppend<<D2 as Distribution<B>>::Sink>>::Output;
        type S123<B, D1, D2, D3> =
            <S12<B, D1, D2> as HAppend<<D3 as Distribution<B>>::Sink>>::Output;
        let (s123, s4) = <S123<B, D1, D2, D3> as HAppend<D4::Sink>>::unappend(sink);
        let (s12, s3) = <S12<B, D1, D2> as HAppend<D3::Sink>>::unappend(s123);
        let (s1, s2) = <D1::Sink as HAppend<D2::Sink>>::unappend(s12);
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

impl<B: Backend, D, F, In, Out: SinkFor<B>> Distribution<B> for Map<D, F, In, Out>
where
    D: Distribution<B, Value = In>,
    F: Fn(In) -> Out + Clone,
{
    type Value = Out;
    type Sink = <Out as SinkFor<B>>::Sink;
    type Iter = MapIter<D::Iter, F>;

    fn row_iter(&mut self) -> Self::Iter {
        MapIter {
            source: self.source.row_iter(),
            f: self.f.clone(),
        }
    }

    fn default_sink(&self) -> Self::Sink {
        Default::default()
    }

    fn collect_into(&mut self, mut sink: Self::Sink, rng: &mut impl Rng, n: usize) -> Self::Sink {
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

impl<B: Backend, L, E> Distribution<B> for List<L, E>
where
    L: Distribution<B, Value = usize>,
    E: Distribution<B>,
    E::Iter: Clone,
{
    type Value = Vec<E::Value>;
    type Sink = <Vec<E::Value> as SinkFor<B>>::Sink;
    type Iter = ListIter<L::Iter, E::Iter>;

    fn row_iter(&mut self) -> Self::Iter {
        ListIter {
            lengths: self.lengths.row_iter(),
            elements: self.elements.row_iter(),
        }
    }

    fn default_sink(&self) -> Self::Sink {
        Default::default()
    }

    fn collect_into(&mut self, mut sink: Self::Sink, rng: &mut impl Rng, n: usize) -> Self::Sink {
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
