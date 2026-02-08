use crate::traits::{
    Backend, Distribution, GroupSink, HAppend, HCons, HNil, IntoTuple, ListSink, NamedDefault,
    Nested, SampleIter, Sink, SinkFor, SinkOf, ValueHList,
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

    fn collect_into(&mut self, mut sink: SinkOf<B, Self::Value>, rng: &mut impl Rng, n: usize) -> SinkOf<B, Self::Value> {
        self.collect_sink(&mut sink, rng, n);
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
    type Iter = RangeIter<T>;

    fn row_iter(&mut self) -> Self::Iter {
        RangeIter {
            current: self.start,
            step: self.step,
        }
    }

    fn collect_into(&mut self, mut sink: SinkOf<B, Self::Value>, rng: &mut impl Rng, n: usize) -> SinkOf<B, Self::Value> {
        self.collect_sink(&mut sink, rng, n);
        sink
    }
}

// ============================================================================
// Tuple (D1, D2) — col-by-col with auto-flatten
// ============================================================================

pub struct PairIter<I1, I2>(pub I1, pub I2);

impl<I1: SampleIter, I2: SampleIter> SampleIter for PairIter<I1, I2>
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
    type Iter = PairIter<D1::Iter, D2::Iter>;

    fn row_iter(&mut self) -> Self::Iter {
        PairIter(self.0.row_iter(), self.1.row_iter())
    }

    fn default_sink(&self) -> SinkOf<B, Self::Value> {
        self.0.default_sink().append(self.1.default_sink())
    }

    fn collect_into(
        &mut self,
        sink: SinkOf<B, Self::Value>,
        rng: &mut impl Rng,
        n: usize,
    ) -> SinkOf<B, Self::Value> {
        let (left, right) = <SinkOf<B, D1::Value>>::unappend(sink);
        let left = self.0.collect_into(left, rng, n);
        let right = self.1.collect_into(right, rng, n);
        left.append(right)
    }
}

// ============================================================================
// Extend<D, E> — col-by-col pair with take-split-rejoin
// ============================================================================

pub struct Extend<D, E> {
    pub base: D,
    pub extra: E,
}

impl<B: Backend, D: Distribution<B>, E: Distribution<B>> Distribution<B> for Extend<D, E>
where
    D::Value: ValueHList,
    E::Value: ValueHList,
    SinkOf<B, D::Value>: HAppend<SinkOf<B, E::Value>>,
    <D::Value as ValueHList>::HList: HAppend<<E::Value as ValueHList>::HList>,
    <<D::Value as ValueHList>::HList as HAppend<<E::Value as ValueHList>::HList>>::Output:
        IntoTuple,
    <<<D::Value as ValueHList>::HList as HAppend<<E::Value as ValueHList>::HList>>::Output
        as IntoTuple>::Tuple:
        SinkFor<B, Sink = <SinkOf<B, D::Value> as HAppend<SinkOf<B, E::Value>>>::Output>,
{
    type Value = <<<D::Value as ValueHList>::HList as HAppend<
        <E::Value as ValueHList>::HList,
    >>::Output as IntoTuple>::Tuple;
    type Iter = PairIter<D::Iter, E::Iter>;

    fn row_iter(&mut self) -> Self::Iter {
        PairIter(self.base.row_iter(), self.extra.row_iter())
    }

    fn default_sink(&self) -> SinkOf<B, Self::Value> {
        self.base.default_sink().append(self.extra.default_sink())
    }

    fn collect_into(
        &mut self,
        sink: SinkOf<B, Self::Value>,
        rng: &mut impl Rng,
        n: usize,
    ) -> SinkOf<B, Self::Value> {
        let (left, right) = <SinkOf<B, D::Value>>::unappend(sink);
        let left = self.base.collect_into(left, rng, n);
        let right = self.extra.collect_into(right, rng, n);
        left.append(right)
    }
}

// ============================================================================
// IntoCols — convert tuples into Extend chains
// ============================================================================

pub trait IntoCols {
    type Dist;
    fn into_cols(self) -> Self::Dist;
}

pub fn cols<T: IntoCols>(t: T) -> T::Dist {
    t.into_cols()
}

impl<A> IntoCols for (A,) {
    type Dist = A;
    fn into_cols(self) -> A {
        self.0
    }
}

impl<A, B> IntoCols for (A, B) {
    type Dist = Extend<A, B>;
    fn into_cols(self) -> Extend<A, B> {
        Extend {
            base: self.0,
            extra: self.1,
        }
    }
}

impl<A, B, C> IntoCols for (A, B, C) {
    type Dist = Extend<A, Extend<B, C>>;
    fn into_cols(self) -> Extend<A, Extend<B, C>> {
        Extend {
            base: self.0,
            extra: Extend {
                base: self.1,
                extra: self.2,
            },
        }
    }
}

impl<A, B, C, D> IntoCols for (A, B, C, D) {
    type Dist = Extend<A, Extend<B, Extend<C, D>>>;
    fn into_cols(self) -> Extend<A, Extend<B, Extend<C, D>>> {
        Extend {
            base: self.0,
            extra: Extend {
                base: self.1,
                extra: Extend {
                    base: self.2,
                    extra: self.3,
                },
            },
        }
    }
}

impl<A, B, C, D, E> IntoCols for (A, B, C, D, E) {
    type Dist = Extend<A, Extend<B, Extend<C, Extend<D, E>>>>;
    fn into_cols(self) -> Extend<A, Extend<B, Extend<C, Extend<D, E>>>> {
        Extend {
            base: self.0,
            extra: Extend {
                base: self.1,
                extra: Extend {
                    base: self.2,
                    extra: Extend {
                        base: self.3,
                        extra: self.4,
                    },
                },
            },
        }
    }
}

// ============================================================================
// DistBuilder — extend/append available on any distribution
// ============================================================================

pub trait DistBuilder: Sized {
    /// Add flat columns from `f` into this chain.
    fn extend<F>(self, f: F) -> Extend<Self, F> {
        Extend {
            base: self,
            extra: f,
        }
    }

    /// Add `f` as a single nested struct column.
    fn append<F>(self, f: F) -> Extend<Self, NestedDist<F>> {
        Extend {
            base: self,
            extra: NestedDist { inner: f },
        }
    }
}

impl<T> DistBuilder for T {}

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

    fn collect_into(&mut self, mut sink: SinkOf<B, Self::Value>, rng: &mut impl Rng, n: usize) -> SinkOf<B, Self::Value> {
        self.collect_sink(&mut sink, rng, n);
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
    Vec<E::Value>: SinkFor<B, Sink = HCons<B::ListCol<E::Value>, HNil>>,
    B::ListCol<E::Value>: ListSink<E::Value>,
{
    type Value = Vec<E::Value>;
    type Iter = ListIter<L::Iter, E::Iter>;

    fn row_iter(&mut self) -> Self::Iter {
        ListIter {
            lengths: self.lengths.row_iter(),
            elements: self.elements.row_iter(),
        }
    }

    fn collect_into(&mut self, mut sink: SinkOf<B, Self::Value>, rng: &mut impl Rng, n: usize) -> SinkOf<B, Self::Value> {
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

impl<B: Backend, D: Distribution<B>> Distribution<B> for Named<D> {
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

    fn collect_into(&mut self, sink: SinkOf<B, Self::Value>, rng: &mut impl Rng, n: usize) -> SinkOf<B, Self::Value> {
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
    B::Column<SinkOf<B, D::Value>>: Sink<Nested<D::Value>> + Default + NamedDefault,
{
    type Value = Nested<D::Value>;
    type Iter = NestedIter<D::Iter>;

    fn row_iter(&mut self) -> Self::Iter {
        NestedIter(self.inner.row_iter())
    }

    fn default_sink(&self) -> SinkOf<B, Self::Value> {
        HCons(GroupSink::new("", self.inner.default_sink()), HNil)
    }

    fn collect_into(&mut self, sink: SinkOf<B, Self::Value>, rng: &mut impl Rng, n: usize) -> SinkOf<B, Self::Value> {
        HCons(sink.0.map_inner(|inner| self.inner.collect_into(inner, rng, n)), HNil)
    }
}
