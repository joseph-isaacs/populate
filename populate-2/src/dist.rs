use crate::traits::{CollectInto, Distribution, ListSink, SampleIter, Sink};
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
// Marker trait for distributions that can be used directly in tuples
// (Unnest does not implement this, so it uses special tuple impls)
// ============================================================================

pub trait NotUnnest {}

impl<T: Copy> NotUnnest for Constant<T> {}
impl NotUnnest for Range {}
impl<D, F, In, Out> NotUnnest for Map<D, F, In, Out> {}
impl<L, E> NotUnnest for List<L, E> {}

// ============================================================================
// Tuple (D1, D2) — col-by-col: fill each sub-sink independently
// ============================================================================

pub struct TupleIter2<I1, I2>(pub I1, pub I2);

impl<I1: SampleIter, I2: SampleIter> SampleIter for TupleIter2<I1, I2> {
    type Item = (I1::Item, I2::Item);

    fn next(&mut self, rng: &mut impl Rng) -> Self::Item {
        (self.0.next(rng), self.1.next(rng))
    }
}

impl<D1: NotUnnest, D2: NotUnnest> NotUnnest for (D1, D2) {}
impl<D1: NotUnnest, D2: NotUnnest, D3: NotUnnest> NotUnnest for (D1, D2, D3) {}
impl<D1: NotUnnest, D2: NotUnnest, D3: NotUnnest, D4: NotUnnest> NotUnnest for (D1, D2, D3, D4) {}

impl<D1: Distribution, D2: Distribution + NotUnnest> Distribution for (D1, D2) {
    type Item = (D1::Item, D2::Item);
    type RowIter = TupleIter2<D1::RowIter, D2::RowIter>;

    fn row_iter(&mut self) -> Self::RowIter {
        TupleIter2(self.0.row_iter(), self.1.row_iter())
    }
}

impl<D1, D2, S1, S2> CollectInto<(S1, S2)> for (D1, D2)
where
    D1: CollectInto<S1>,
    D2: CollectInto<S2>,
{
    fn collect_into(&mut self, sink: (S1, S2), rng: &mut impl Rng, n: usize) -> (S1, S2) {
        let s1 = self.0.collect_into(sink.0, rng, n);
        let s2 = self.1.collect_into(sink.1, rng, n);
        (s1, s2)
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

impl<D1: Distribution, D2: Distribution + NotUnnest, D3: Distribution> Distribution
    for (D1, D2, D3)
{
    type Item = (D1::Item, D2::Item, D3::Item);
    type RowIter = TupleIter3<D1::RowIter, D2::RowIter, D3::RowIter>;

    fn row_iter(&mut self) -> Self::RowIter {
        TupleIter3(self.0.row_iter(), self.1.row_iter(), self.2.row_iter())
    }
}

impl<D1, D2, D3, S1, S2, S3> CollectInto<(S1, S2, S3)> for (D1, D2, D3)
where
    D1: CollectInto<S1>,
    D2: CollectInto<S2>,
    D3: CollectInto<S3>,
{
    fn collect_into(&mut self, sink: (S1, S2, S3), rng: &mut impl Rng, n: usize) -> (S1, S2, S3) {
        let s1 = self.0.collect_into(sink.0, rng, n);
        let s2 = self.1.collect_into(sink.1, rng, n);
        let s3 = self.2.collect_into(sink.2, rng, n);
        (s1, s2, s3)
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

impl<D1, D2, D3, D4, S1, S2, S3, S4> CollectInto<(S1, S2, S3, S4)> for (D1, D2, D3, D4)
where
    D1: CollectInto<S1>,
    D2: CollectInto<S2>,
    D3: CollectInto<S3>,
    D4: CollectInto<S4>,
{
    fn collect_into(
        &mut self,
        sink: (S1, S2, S3, S4),
        rng: &mut impl Rng,
        n: usize,
    ) -> (S1, S2, S3, S4) {
        let s1 = self.0.collect_into(sink.0, rng, n);
        let s2 = self.1.collect_into(sink.1, rng, n);
        let s3 = self.2.collect_into(sink.2, rng, n);
        let s4 = self.3.collect_into(sink.3, rng, n);
        (s1, s2, s3, s4)
    }
}

// ============================================================================
// Map — row-by-row collection via Sink<Out>::push
//
// Map couples its input columns, so it must iterate row-by-row.
// Uses Sink<Out> which handles both scalar and tuple outputs:
// - If Out = f64, Sink<f64> for Vec<f64> pushes scalars
// - If Out = (T1, T2), Sink<(T1,T2)> for (S1,S2) destructures and pushes
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

/// Map uses row-by-row collection: Sink<Out>::push handles both scalar and
/// tuple outputs. For tuple outputs like (T1, T2), the tuple sink's push
/// destructures and pushes to each sub-sink.
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
//
// Produces variable-length lists. The list sink receives (len, element_iter)
// directly — no intermediate Vec<T> allocation.
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

/// List fills a ListSink: for each row, push_list with (len, element_iter).
/// Zero allocation — elements flow directly from the element iterator
/// into the sink's internal storage.
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

// ============================================================================
// Unnest — marker wrapper that tells tuples to spread the inner distribution
//
// With owned sinks, Unnest is trivial: destructure the flat sink tuple,
// regroup the middle sinks into a sub-tuple for D2, and reassemble.
// No Splittable/split/unsplit needed.
// ============================================================================

pub struct Unnest<D>(pub D);

pub fn unnest<D>(d: D) -> Unnest<D> {
    Unnest(d)
}

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

/// Unnest delegates to inner's CollectInto — if D2 is a tuple distribution,
/// it goes col-by-col. If D2 is a Map, it goes row-by-row. Either way,
/// the inner distribution handles it.
impl<D: CollectInto<S>, S> CollectInto<S> for Unnest<D> {
    fn collect_into(&mut self, sink: S, rng: &mut impl Rng, n: usize) -> S {
        self.0.collect_into(sink, rng, n)
    }
}

// ============================================================================
// (D1, Unnest<D2>) where D2 produces (H, T) → flat output (D1::Item, H, T)
// ============================================================================

pub struct UnnestTupleIter2<I1, I2> {
    i1: I1,
    i2: I2,
}

impl<I1, I2, H, T> SampleIter for UnnestTupleIter2<I1, I2>
where
    I1: SampleIter,
    I2: SampleIter<Item = (H, T)>,
{
    type Item = (I1::Item, H, T);

    fn next(&mut self, rng: &mut impl Rng) -> Self::Item {
        let v1 = self.i1.next(rng);
        let (h, t) = self.i2.next(rng);
        (v1, h, t)
    }
}

impl<D1, D2, H, T> Distribution for (D1, Unnest<D2>)
where
    D1: Distribution,
    D2: Distribution<Item = (H, T)>,
{
    type Item = (D1::Item, H, T);
    type RowIter = UnnestTupleIter2<D1::RowIter, D2::RowIter>;

    fn row_iter(&mut self) -> Self::RowIter {
        UnnestTupleIter2 {
            i1: self.0.row_iter(),
            i2: self.1.0.row_iter(),
        }
    }
}

/// (D1, Unnest<D2>) col-by-col into (S1, SH, ST):
/// - D1 fills S1
/// - D2 fills (SH, ST) — regrouped from the flat sink
impl<D1, D2, S1, SH, ST> CollectInto<(S1, SH, ST)> for (D1, Unnest<D2>)
where
    D1: CollectInto<S1>,
    D2: CollectInto<(SH, ST)>,
{
    fn collect_into(&mut self, sink: (S1, SH, ST), rng: &mut impl Rng, n: usize) -> (S1, SH, ST) {
        let s1 = self.0.collect_into(sink.0, rng, n);
        let (sh, st) = self.1.0.collect_into((sink.1, sink.2), rng, n);
        (s1, sh, st)
    }
}

// ============================================================================
// (D1, Unnest<D2>, D3) where D2 produces (H, T) → flat output (D1::Item, H, T, D3::Item)
// ============================================================================

pub struct UnnestTupleIter3<I1, I2, I3> {
    i1: I1,
    i2: I2,
    i3: I3,
}

impl<I1, I2, I3, H, T> SampleIter for UnnestTupleIter3<I1, I2, I3>
where
    I1: SampleIter,
    I2: SampleIter<Item = (H, T)>,
    I3: SampleIter,
{
    type Item = (I1::Item, H, T, I3::Item);

    fn next(&mut self, rng: &mut impl Rng) -> Self::Item {
        let v1 = self.i1.next(rng);
        let (h, t) = self.i2.next(rng);
        let v3 = self.i3.next(rng);
        (v1, h, t, v3)
    }
}

impl<D1, D2, D3, H, T> Distribution for (D1, Unnest<D2>, D3)
where
    D1: Distribution,
    D2: Distribution<Item = (H, T)>,
    D3: Distribution,
{
    type Item = (D1::Item, H, T, D3::Item);
    type RowIter = UnnestTupleIter3<D1::RowIter, D2::RowIter, D3::RowIter>;

    fn row_iter(&mut self) -> Self::RowIter {
        UnnestTupleIter3 {
            i1: self.0.row_iter(),
            i2: self.1.0.row_iter(),
            i3: self.2.row_iter(),
        }
    }
}

/// (D1, Unnest<D2>, D3) col-by-col into (S1, SH, ST, S3):
/// - D1 fills S1
/// - D2 fills (SH, ST) — regrouped from the flat sink
/// - D3 fills S3
impl<D1, D2, D3, S1, SH, ST, S3> CollectInto<(S1, SH, ST, S3)> for (D1, Unnest<D2>, D3)
where
    D1: CollectInto<S1>,
    D2: CollectInto<(SH, ST)>,
    D3: CollectInto<S3>,
{
    fn collect_into(
        &mut self,
        sink: (S1, SH, ST, S3),
        rng: &mut impl Rng,
        n: usize,
    ) -> (S1, SH, ST, S3) {
        let s1 = self.0.collect_into(sink.0, rng, n);
        let (sh, st) = self.1.0.collect_into((sink.1, sink.2), rng, n);
        let s3 = self.2.collect_into(sink.3, rng, n);
        (s1, sh, st, s3)
    }
}
