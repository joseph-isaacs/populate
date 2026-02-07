use rand::Rng;
use std::collections::VecDeque;

// Re-export HList types so existing `use crate::traits::{HCons, ...}` keeps working
pub use crate::hlist::{HAppend, HCons, HNil, IntoHList, IntoTuple, ValueHList};

// ============================================================================
// Backend — selects concrete sink types for each column kind
// ============================================================================

pub trait Backend: Sized {
    type Prim<T>: Sink<T> + Default;
    type List<T>: ListSink<T> + Sink<Vec<T>> + Default;
    type Column<S>: GroupSink<Inner = S>;
}

pub struct VecBackend;

impl Backend for VecBackend {
    type Prim<T> = Vec<T>;
    type List<T> = Vec<Vec<T>>;
    type Column<S> = Transparent<S>;
}

pub struct DequeBackend;

impl Backend for DequeBackend {
    type Prim<T> = VecDeque<T>;
    type List<T> = DequeListSink<T>;
    type Column<S> = Transparent<S>;
}

// ============================================================================
// GroupSink — wrap/unwrap for column groups (used by Named columns)
// ============================================================================

pub trait GroupSink: Sized {
    type Inner;
    fn new(name: &'static str, inner: Self::Inner) -> Self;
    fn into_inner(self) -> Self::Inner;
}

// ============================================================================
// Transparent — Vec/Deque ignore column names, delegate HAppend to inner
// ============================================================================

pub struct Transparent<S>(pub S);

impl<S> GroupSink for Transparent<S> {
    type Inner = S;
    fn new(_name: &'static str, inner: S) -> Self {
        Transparent(inner)
    }
    fn into_inner(self) -> S {
        self.0
    }
}

impl<S: HAppend<R>, R> HAppend<R> for Transparent<S> {
    type Output = <S as HAppend<R>>::Output;
    fn append(self, right: R) -> Self::Output {
        self.0.append(right)
    }
    fn unappend(combined: Self::Output) -> (Self, R) {
        let (s, r) = S::unappend(combined);
        (Transparent(s), r)
    }
}

// ============================================================================
// SinkFor<B> — maps (value type, backend) to HList sink type
// Replaces DefaultSink. Only required by Map (not a bound on Distribution::Value).
// ============================================================================

pub trait SinkFor<B: Backend>: Sized {
    type Sink: Sink<Self> + Default;
}

macro_rules! for_all_primitives {
    ($macro:ident) => {
        $macro!(i16, i32, i64, f64, bool, usize);
    };
}

macro_rules! impl_sink_for_scalar {
    ($($t:ty),*) => {
        $(impl<B: Backend> SinkFor<B> for $t {
            type Sink = HCons<B::Prim<$t>, HNil>;
        })*
    };
}

macro_rules! impl_value_hlist_scalar {
    ($($t:ty),*) => {
        $(impl ValueHList for $t {
            type HList = HCons<$t, HNil>;
            fn into_hlist(self) -> Self::HList { HCons(self, HNil) }
        })*
    };
}

for_all_primitives!(impl_sink_for_scalar);
for_all_primitives!(impl_value_hlist_scalar);

impl<B: Backend, T> SinkFor<B> for Vec<T> {
    type Sink = HCons<B::List<T>, HNil>;
}

// SinkFor tuple impls — used by Map to resolve sink from output type

impl<B: Backend, A: SinkFor<B>, C: SinkFor<B>> SinkFor<B> for (A, C)
where
    A::Sink: HAppend<C::Sink>,
    <A::Sink as HAppend<C::Sink>>::Output: Sink<(A, C)> + Default,
{
    type Sink = <A::Sink as HAppend<C::Sink>>::Output;
}

impl<B: Backend, A: SinkFor<B>, C: SinkFor<B>, E: SinkFor<B>> SinkFor<B> for (A, C, E)
where
    A::Sink: HAppend<C::Sink>,
    <A::Sink as HAppend<C::Sink>>::Output: HAppend<E::Sink>,
    <<A::Sink as HAppend<C::Sink>>::Output as HAppend<E::Sink>>::Output: Sink<(A, C, E)> + Default,
{
    type Sink = <<A::Sink as HAppend<C::Sink>>::Output as HAppend<E::Sink>>::Output;
}

impl<B: Backend, A: SinkFor<B>, C: SinkFor<B>, E: SinkFor<B>, G: SinkFor<B>> SinkFor<B>
    for (A, C, E, G)
where
    A::Sink: HAppend<C::Sink>,
    <A::Sink as HAppend<C::Sink>>::Output: HAppend<E::Sink>,
    <<A::Sink as HAppend<C::Sink>>::Output as HAppend<E::Sink>>::Output: HAppend<G::Sink>,
    <<<A::Sink as HAppend<C::Sink>>::Output as HAppend<E::Sink>>::Output as HAppend<G::Sink>>::Output:
        Sink<(A, C, E, G)> + Default,
{
    type Sink = <<<A::Sink as HAppend<C::Sink>>::Output as HAppend<E::Sink>>::Output as HAppend<
        G::Sink,
    >>::Output;
}

// ============================================================================
// SampleIter and Distribution
// ============================================================================

pub trait SampleIter {
    type Item;
    fn next(&mut self, rng: &mut impl Rng) -> Self::Item;
}

pub trait Distribution<B: Backend> {
    type Value;
    type Sink;
    type Iter: SampleIter<Item = Self::Value>;

    fn row_iter(&mut self) -> Self::Iter;
    fn default_sink(&self) -> Self::Sink;
    fn collect_into(&mut self, sink: Self::Sink, rng: &mut impl Rng, n: usize) -> Self::Sink;
}

// ============================================================================
// Sink<T> — accepts values via push
// ============================================================================

pub trait Sink<T> {
    fn push(&mut self, value: T);

    fn push_n(&mut self, value: impl Iterator<Item = T>) {
        value.for_each(|v| self.push(v));
    }
}

// Vec<T> is a primitive sink
impl<T> Sink<T> for Vec<T> {
    fn push(&mut self, value: T) {
        Vec::push(self, value);
    }

    fn push_n(&mut self, value: impl Iterator<Item = T>) {
        Vec::extend(self, value)
    }
}

// VecDeque<T> is a primitive sink
impl<T> Sink<T> for VecDeque<T> {
    fn push(&mut self, value: T) {
        self.push_back(value);
    }
}

// HList Sink impls — bridge between leaf distributions and HList sinks

// Scalar into 1-element HList
impl<T, S: Sink<T>> Sink<T> for HCons<S, HNil> {
    fn push(&mut self, value: T) {
        self.0.push(value);
    }
}

// 2-tuple into 2-element HList
impl<A, B, SA: Sink<A>, SB: Sink<B>> Sink<(A, B)> for HCons<SA, HCons<SB, HNil>> {
    fn push(&mut self, (a, b): (A, B)) {
        self.0.push(a);
        self.1.0.push(b);
    }
}

// 3-tuple into 3-element HList
impl<A, B, C, SA: Sink<A>, SB: Sink<B>, SC: Sink<C>> Sink<(A, B, C)>
    for HCons<SA, HCons<SB, HCons<SC, HNil>>>
{
    fn push(&mut self, (a, b, c): (A, B, C)) {
        self.0.push(a);
        self.1.0.push(b);
        self.1.1.0.push(c);
    }
}

// 4-tuple into 4-element HList
impl<A, B, C, D, SA: Sink<A>, SB: Sink<B>, SC: Sink<C>, SD: Sink<D>> Sink<(A, B, C, D)>
    for HCons<SA, HCons<SB, HCons<SC, HCons<SD, HNil>>>>
{
    fn push(&mut self, (a, b, c, d): (A, B, C, D)) {
        self.0.push(a);
        self.1.0.push(b);
        self.1.1.0.push(c);
        self.1.1.1.0.push(d);
    }
}

// Tuple sinks — for row-by-row collection (collect helper in tests uses these)
impl<T1, T2, S1: Sink<T1>, S2: Sink<T2>> Sink<(T1, T2)> for (S1, S2) {
    fn push(&mut self, (a, b): (T1, T2)) {
        self.0.push(a);
        self.1.push(b);
    }
}

impl<T1, T2, T3, S1: Sink<T1>, S2: Sink<T2>, S3: Sink<T3>> Sink<(T1, T2, T3)> for (S1, S2, S3) {
    fn push(&mut self, (a, b, c): (T1, T2, T3)) {
        self.0.push(a);
        self.1.push(b);
        self.2.push(c);
    }
}

impl<T1, T2, T3, T4, S1: Sink<T1>, S2: Sink<T2>, S3: Sink<T3>, S4: Sink<T4>> Sink<(T1, T2, T3, T4)>
    for (S1, S2, S3, S4)
{
    fn push(&mut self, (a, b, c, d): (T1, T2, T3, T4)) {
        self.0.push(a);
        self.1.push(b);
        self.2.push(c);
        self.3.push(d);
    }
}

// ============================================================================
// ListSink<T> — sink trait for list/array columns
// ============================================================================

pub trait ListSink<T> {
    fn push_list(&mut self, len: usize, elems: impl Iterator<Item = T>);
}

// HList ListSink — wrap a ListSink in a 1-element HList
impl<T, S: ListSink<T>> ListSink<T> for HCons<S, HNil> {
    fn push_list(&mut self, len: usize, elems: impl Iterator<Item = T>) {
        self.0.push_list(len, elems);
    }
}

// ============================================================================
// VecListSink<T> — columnar sink for list/array columns
// ============================================================================

pub struct VecListSink<T> {
    pub values: Vec<T>,
    pub offsets: Vec<usize>,
}

impl<T> VecListSink<T> {
    pub fn new() -> Self {
        Self {
            values: Vec::new(),
            offsets: vec![0],
        }
    }

    pub fn into_parts(self) -> (Vec<T>, Vec<usize>) {
        (self.values, self.offsets)
    }
}

impl<T> Default for VecListSink<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> ListSink<T> for VecListSink<T> {
    fn push_list(&mut self, _len: usize, elems: impl Iterator<Item = T>) {
        self.values.extend(elems);
        self.offsets.push(self.values.len());
    }
}

impl<T> Sink<Vec<T>> for VecListSink<T> {
    fn push(&mut self, value: Vec<T>) {
        let len = value.len();
        self.push_list(len, value.into_iter());
    }
}

// Vec<Vec<T>> as a list sink — the natural representation for VecBackend
impl<T> ListSink<T> for Vec<Vec<T>> {
    fn push_list(&mut self, _len: usize, elems: impl Iterator<Item = T>) {
        self.push(elems.collect());
    }
}

// ============================================================================
// DequeListSink<T> — columnar sink for list/array columns using VecDeque
// ============================================================================

pub struct DequeListSink<T> {
    pub values: VecDeque<T>,
    pub offsets: VecDeque<usize>,
}

impl<T> DequeListSink<T> {
    pub fn new() -> Self {
        let mut offsets = VecDeque::new();
        offsets.push_back(0);
        Self {
            values: VecDeque::new(),
            offsets,
        }
    }

    pub fn into_parts(self) -> (VecDeque<T>, VecDeque<usize>) {
        (self.values, self.offsets)
    }
}

impl<T> Default for DequeListSink<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> ListSink<T> for DequeListSink<T> {
    fn push_list(&mut self, _len: usize, elems: impl Iterator<Item = T>) {
        self.values.extend(elems);
        self.offsets.push_back(self.values.len());
    }
}

impl<T> Sink<Vec<T>> for DequeListSink<T> {
    fn push(&mut self, value: Vec<T>) {
        let len = value.len();
        self.push_list(len, value.into_iter());
    }
}

// ============================================================================
// DistributionExt — ergonomic methods (collect, collect_flat, collect_flat_into)
// ============================================================================

pub trait DistributionExt<B: Backend>: Distribution<B> {
    /// Collect using default_sink, return native Sink type (HList)
    fn collect(&mut self, rng: &mut impl Rng, n: usize) -> Self::Sink {
        let sink = self.default_sink();
        self.collect_into(sink, rng, n)
    }

    /// Collect using default_sink, return flat tuple
    fn collect_flat(&mut self, rng: &mut impl Rng, n: usize) -> <Self::Sink as IntoTuple>::Tuple
    where
        Self::Sink: IntoTuple,
    {
        self.collect(rng, n).into_tuple()
    }

    /// Collect with explicit sink tuple, return flat tuple
    fn collect_flat_into<T>(&mut self, sink: T, rng: &mut impl Rng, n: usize) -> T
    where
        T: IntoHList<HList = Self::Sink>,
        Self::Sink: IntoTuple<Tuple = T>,
    {
        self.collect_into(sink.into_hlist(), rng, n).into_tuple()
    }
}

impl<B: Backend, D: Distribution<B>> DistributionExt<B> for D {}

// ============================================================================
// DistributionScalar — convenience for single-column distributions
// ============================================================================

pub trait DistributionScalar<B: Backend>: Distribution<B> {
    fn collect_col<S>(&mut self, sink: S, rng: &mut impl Rng, n: usize) -> S
    where
        Self: Distribution<B, Sink = HCons<S, HNil>>,
    {
        self.collect_into(HCons(sink, HNil), rng, n).0
    }
}

impl<B: Backend, D: Distribution<B>> DistributionScalar<B> for D {}
