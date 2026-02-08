use crate::traits::{
    Backend, Distribution, GroupSink, HAppend, HCons, HNil, IntoTuple, ListSink, NamedDefault,
    Nested, SampleIter, SinkFor, SinkOf, StepMul, ValueHList,
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
    type Iter = ConstantIter<T>;

    fn row_iter(&mut self) -> Self::Iter {
        ConstantIter { value: self.value }
    }
}

// ============================================================================
// Uniform — random values in [lo, hi)
// ============================================================================

pub struct Uniform<T> {
    pub lo: T,
    pub hi: T,
}

pub fn uniform<T>(lo: T, hi: T) -> Uniform<T> {
    Uniform { lo, hi }
}

#[derive(Clone)]
pub struct UniformIter<T> {
    lo: T,
    hi: T,
}

impl SampleIter for UniformIter<i64> {
    type Item = i64;
    fn next(&mut self, rng: &mut impl Rng) -> i64 {
        rng.random_range(self.lo..self.hi)
    }
}

impl SampleIter for UniformIter<i32> {
    type Item = i32;
    fn next(&mut self, rng: &mut impl Rng) -> i32 {
        rng.random_range(self.lo..self.hi)
    }
}

impl SampleIter for UniformIter<f64> {
    type Item = f64;
    fn next(&mut self, rng: &mut impl Rng) -> f64 {
        rng.random_range(self.lo..self.hi)
    }
}

impl SampleIter for UniformIter<usize> {
    type Item = usize;
    fn next(&mut self, rng: &mut impl Rng) -> usize {
        rng.random_range(self.lo..self.hi)
    }
}

macro_rules! impl_uniform_dist {
    ($($t:ty),*) => {
        $(impl<B: Backend> Distribution<B> for Uniform<$t>
        where
            $t: SinkFor<B>,
        {
            type Value = $t;
            type Iter = UniformIter<$t>;

            fn row_iter(&mut self) -> Self::Iter {
                UniformIter { lo: self.lo, hi: self.hi }
            }

            fn skip(&mut self, rng: &mut impl Rng, n: usize) {
                for _ in 0..n {
                    rng.random_range(self.lo..self.hi);
                }
            }
        })*
    };
}

impl_uniform_dist!(i32, i64, f64, usize);

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

impl<B: Backend, T: SinkFor<B> + Add<Output = T> + StepMul + Copy> Distribution<B> for Range<T> {
    type Value = T;
    type Iter = RangeIter<T>;

    fn row_iter(&mut self) -> Self::Iter {
        RangeIter {
            current: self.start,
            step: self.step,
        }
    }

    fn skip(&mut self, _rng: &mut impl Rng, n: usize) {
        self.start = self.start + self.step.step_mul(n);
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
    SinkOf<B, D1::Value>: HAppend<SinkOf<B, D2::Value>>,
    <D1::Value as ValueHList>::HList: HAppend<<D2::Value as ValueHList>::HList>,
    <<D1::Value as ValueHList>::HList as HAppend<<D2::Value as ValueHList>::HList>>::Output:
        IntoTuple,
    <<<D1::Value as ValueHList>::HList as HAppend<<D2::Value as ValueHList>::HList>>::Output
        as IntoTuple>::Tuple:
        SinkFor<B, Sink = <SinkOf<B, D1::Value> as HAppend<SinkOf<B, D2::Value>>>::Output>,
{
    type Value = <<<D1::Value as ValueHList>::HList as HAppend<
        <D2::Value as ValueHList>::HList,
    >>::Output as IntoTuple>::Tuple;
    type Iter = TupleIter2<D1::Iter, D2::Iter>;

    fn row_iter(&mut self) -> Self::Iter {
        TupleIter2(self.0.row_iter(), self.1.row_iter())
    }

    fn default_sink(&self) -> SinkOf<B, Self::Value> {
        self.0.default_sink().append(self.1.default_sink())
    }

    fn skip(&mut self, rng: &mut impl Rng, n: usize) {
        self.0.skip(rng, n);
        self.1.skip(rng, n);
    }

    fn collect_into(
        &mut self,
        sink: SinkOf<B, Self::Value>,
        rng: &mut impl Rng,
        n: usize,
    ) -> SinkOf<B, Self::Value> {
        let (s1, s2) =
            <SinkOf<B, D1::Value> as HAppend<SinkOf<B, D2::Value>>>::unappend(sink);
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
    SinkOf<B, D1::Value>: HAppend<SinkOf<B, D2::Value>>,
    <SinkOf<B, D1::Value> as HAppend<SinkOf<B, D2::Value>>>::Output:
        HAppend<SinkOf<B, D3::Value>>,
    <D1::Value as ValueHList>::HList: HAppend<<D2::Value as ValueHList>::HList>,
    <<D1::Value as ValueHList>::HList as HAppend<<D2::Value as ValueHList>::HList>>::Output:
        HAppend<<D3::Value as ValueHList>::HList>,
    <<<D1::Value as ValueHList>::HList as HAppend<<D2::Value as ValueHList>::HList>>::Output
        as HAppend<<D3::Value as ValueHList>::HList>>::Output: IntoTuple,
    <<<<D1::Value as ValueHList>::HList as HAppend<<D2::Value as ValueHList>::HList>>::Output
        as HAppend<<D3::Value as ValueHList>::HList>>::Output as IntoTuple>::Tuple:
        SinkFor<
            B,
            Sink = <<SinkOf<B, D1::Value> as HAppend<SinkOf<B, D2::Value>>>::Output as HAppend<
                SinkOf<B, D3::Value>,
            >>::Output,
        >,
{
    type Value = <<<<D1::Value as ValueHList>::HList as HAppend<
        <D2::Value as ValueHList>::HList,
    >>::Output as HAppend<<D3::Value as ValueHList>::HList>>::Output
        as IntoTuple>::Tuple;
    type Iter = TupleIter3<D1::Iter, D2::Iter, D3::Iter>;

    fn row_iter(&mut self) -> Self::Iter {
        TupleIter3(self.0.row_iter(), self.1.row_iter(), self.2.row_iter())
    }

    fn default_sink(&self) -> SinkOf<B, Self::Value> {
        self.0
            .default_sink()
            .append(self.1.default_sink())
            .append(self.2.default_sink())
    }

    fn skip(&mut self, rng: &mut impl Rng, n: usize) {
        self.0.skip(rng, n);
        self.1.skip(rng, n);
        self.2.skip(rng, n);
    }

    fn collect_into(
        &mut self,
        sink: SinkOf<B, Self::Value>,
        rng: &mut impl Rng,
        n: usize,
    ) -> SinkOf<B, Self::Value> {
        type S12<B, D1, D2> = <SinkOf<B, <D1 as Distribution<B>>::Value> as HAppend<
            SinkOf<B, <D2 as Distribution<B>>::Value>,
        >>::Output;
        let (s12, s3) =
            <S12<B, D1, D2> as HAppend<SinkOf<B, D3::Value>>>::unappend(sink);
        let (s1, s2) =
            <SinkOf<B, D1::Value> as HAppend<SinkOf<B, D2::Value>>>::unappend(s12);
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
    SinkOf<B, D1::Value>: HAppend<SinkOf<B, D2::Value>>,
    <SinkOf<B, D1::Value> as HAppend<SinkOf<B, D2::Value>>>::Output:
        HAppend<SinkOf<B, D3::Value>>,
    <<SinkOf<B, D1::Value> as HAppend<SinkOf<B, D2::Value>>>::Output as HAppend<
        SinkOf<B, D3::Value>,
    >>::Output: HAppend<SinkOf<B, D4::Value>>,
    <D1::Value as ValueHList>::HList: HAppend<<D2::Value as ValueHList>::HList>,
    <<D1::Value as ValueHList>::HList as HAppend<<D2::Value as ValueHList>::HList>>::Output:
        HAppend<<D3::Value as ValueHList>::HList>,
    <<<D1::Value as ValueHList>::HList as HAppend<<D2::Value as ValueHList>::HList>>::Output
        as HAppend<<D3::Value as ValueHList>::HList>>::Output:
        HAppend<<D4::Value as ValueHList>::HList>,
    <<<<D1::Value as ValueHList>::HList as HAppend<<D2::Value as ValueHList>::HList>>::Output
        as HAppend<<D3::Value as ValueHList>::HList>>::Output
        as HAppend<<D4::Value as ValueHList>::HList>>::Output: IntoTuple,
    <<<<<D1::Value as ValueHList>::HList as HAppend<<D2::Value as ValueHList>::HList>>::Output
        as HAppend<<D3::Value as ValueHList>::HList>>::Output
        as HAppend<<D4::Value as ValueHList>::HList>>::Output as IntoTuple>::Tuple:
        SinkFor<
            B,
            Sink = <<<SinkOf<B, D1::Value> as HAppend<SinkOf<B, D2::Value>>>::Output as HAppend<
                SinkOf<B, D3::Value>,
            >>::Output as HAppend<SinkOf<B, D4::Value>>>::Output,
        >,
{
    type Value = <<<<<D1::Value as ValueHList>::HList as HAppend<
        <D2::Value as ValueHList>::HList,
    >>::Output as HAppend<<D3::Value as ValueHList>::HList>>::Output
        as HAppend<<D4::Value as ValueHList>::HList>>::Output
        as IntoTuple>::Tuple;
    type Iter = TupleIter4<D1::Iter, D2::Iter, D3::Iter, D4::Iter>;

    fn row_iter(&mut self) -> Self::Iter {
        TupleIter4(
            self.0.row_iter(),
            self.1.row_iter(),
            self.2.row_iter(),
            self.3.row_iter(),
        )
    }

    fn default_sink(&self) -> SinkOf<B, Self::Value> {
        self.0
            .default_sink()
            .append(self.1.default_sink())
            .append(self.2.default_sink())
            .append(self.3.default_sink())
    }

    fn skip(&mut self, rng: &mut impl Rng, n: usize) {
        self.0.skip(rng, n);
        self.1.skip(rng, n);
        self.2.skip(rng, n);
        self.3.skip(rng, n);
    }

    fn collect_into(
        &mut self,
        sink: SinkOf<B, Self::Value>,
        rng: &mut impl Rng,
        n: usize,
    ) -> SinkOf<B, Self::Value> {
        type S12<B, D1, D2> = <SinkOf<B, <D1 as Distribution<B>>::Value> as HAppend<
            SinkOf<B, <D2 as Distribution<B>>::Value>,
        >>::Output;
        type S123<B, D1, D2, D3> =
            <S12<B, D1, D2> as HAppend<SinkOf<B, <D3 as Distribution<B>>::Value>>>::Output;
        let (s123, s4) =
            <S123<B, D1, D2, D3> as HAppend<SinkOf<B, D4::Value>>>::unappend(sink);
        let (s12, s3) =
            <S12<B, D1, D2> as HAppend<SinkOf<B, D3::Value>>>::unappend(s123);
        let (s1, s2) =
            <SinkOf<B, D1::Value> as HAppend<SinkOf<B, D2::Value>>>::unappend(s12);
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
    type Iter = MapIter<D::Iter, F>;

    fn row_iter(&mut self) -> Self::Iter {
        MapIter {
            source: self.source.row_iter(),
            f: self.f.clone(),
        }
    }

    fn skip(&mut self, rng: &mut impl Rng, n: usize) {
        self.source.skip(rng, n);
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
    Vec<E::Value>: SinkFor<B>,
    SinkOf<B, Vec<E::Value>>: ListSink<E::Value>,
{
    type Value = Vec<E::Value>;
    type Iter = ListIter<L::Iter, E::Iter>;

    fn row_iter(&mut self) -> Self::Iter {
        ListIter {
            lengths: self.lengths.row_iter(),
            elements: self.elements.row_iter(),
        }
    }

    fn skip(&mut self, rng: &mut impl Rng, n: usize) {
        #[cfg(not(feature = "disjoint-list-skip"))]
        {
            let mut len_iter = self.lengths.row_iter();
            for _ in 0..n {
                let len = len_iter.next(rng);
                self.elements.skip(rng, len);
            }
        }
        self.lengths.skip(rng, n);
    }

    fn collect_into(
        &mut self,
        mut sink: SinkOf<B, Self::Value>,
        rng: &mut impl Rng,
        n: usize,
    ) -> SinkOf<B, Self::Value> {
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

// ============================================================================
// Named — attach a column name (used with NamedVecBackend)
// ============================================================================

pub struct Named<D> {
    pub name: &'static str,
    pub inner: D,
}

pub fn named<D>(name: &'static str, inner: D) -> Named<D> {
    Named { name, inner }
}

impl<B: Backend, D: Distribution<B>> Distribution<B> for Named<D>
where
    SinkOf<B, D::Value>: NamedDefault,
{
    type Value = D::Value;
    type Iter = D::Iter;

    fn row_iter(&mut self) -> Self::Iter {
        self.inner.row_iter()
    }

    fn default_sink(&self) -> SinkOf<B, Self::Value> {
        let mut sink = self.inner.default_sink();
        sink.set_name(self.name);
        sink
    }

    fn skip(&mut self, rng: &mut impl Rng, n: usize) {
        self.inner.skip(rng, n);
    }

    fn collect_into(
        &mut self,
        sink: SinkOf<B, Self::Value>,
        rng: &mut impl Rng,
        n: usize,
    ) -> SinkOf<B, Self::Value> {
        self.inner.collect_into(sink, rng, n)
    }
}

// ============================================================================
// NestedDist — struct column: wraps sub-distribution in Column<S>
// ============================================================================

pub struct NestedDist<D> {
    pub inner: D,
}

pub fn nested<D>(inner: D) -> NestedDist<D> {
    NestedDist { inner }
}

pub struct NestedIter<I>(pub I);

impl<I: SampleIter> SampleIter for NestedIter<I> {
    type Item = Nested<I::Item>;
    fn next(&mut self, rng: &mut impl Rng) -> Self::Item {
        Nested(self.0.next(rng))
    }
}

impl<B: Backend, D: Distribution<B>> Distribution<B> for NestedDist<D>
where
    Nested<D::Value>: SinkFor<B, Sink = HCons<B::Column<SinkOf<B, D::Value>>, HNil>>,
{
    type Value = Nested<D::Value>;
    type Iter = NestedIter<D::Iter>;

    fn row_iter(&mut self) -> Self::Iter {
        NestedIter(self.inner.row_iter())
    }

    fn default_sink(&self) -> SinkOf<B, Self::Value> {
        HCons(GroupSink::new("", self.inner.default_sink()), HNil)
    }

    fn skip(&mut self, rng: &mut impl Rng, n: usize) {
        self.inner.skip(rng, n);
    }

    fn collect_into(
        &mut self,
        sink: SinkOf<B, Self::Value>,
        rng: &mut impl Rng,
        n: usize,
    ) -> SinkOf<B, Self::Value> {
        let column = sink.0;
        let filled = column.map_inner(|inner| self.inner.collect_into(inner, rng, n));
        HCons(filled, HNil)
    }
}
