use rand::Rng;
use std::collections::VecDeque;

// Re-export HList types so existing `use crate::traits::{HCons, ...}` keeps working
pub use crate::hlist::{HAppend, HCons, HNil, IntoHList, IntoTuple, ValueHList};

// ============================================================================
// Backend — selects concrete sink types for each column kind
// ============================================================================

pub trait Backend {
    type Prim<T>: Sink<T>;
    type List<T>: ListSink<T>;
}

pub struct VecBackend;

impl Backend for VecBackend {
    type Prim<T> = Vec<T>;
    type List<T> = VecListSink<T>;
}

pub struct DequeBackend;

impl Backend for DequeBackend {
    type Prim<T> = VecDeque<T>;
    type List<T> = DequeListSink<T>;
}

// ============================================================================
// DefaultSink<B> — maps (value type, backend) to HList sink type
// ============================================================================

pub trait DefaultSink<B: Backend> {
    type Sink;
}

macro_rules! for_all_primitives {
    ($macro:ident) => {
        $macro!(i16, i32, i64, f64, bool, usize);
    };
}

macro_rules! impl_default_sink_scalar {
    ($($t:ty),*) => {
        $(impl<B: Backend> DefaultSink<B> for $t {
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

for_all_primitives!(impl_default_sink_scalar);
for_all_primitives!(impl_value_hlist_scalar);

impl<B: Backend, T> DefaultSink<B> for Vec<T> {
    type Sink = HCons<B::List<T>, HNil>;
}

macro_rules! impl_default_sink_tuple {
    // Terminal: emit the impl
    (@accum [$($all:ident),+] ($($tuple:ident),+) $acc:ty; [$($bounds:tt)*]) => {
        impl<B: Backend, $($all: DefaultSink<B>),+> DefaultSink<B> for ($($tuple),+)
        where $($bounds)*
        {
            type Sink = $acc;
        }
    };
    // Recursive: fold in next type's sink via HAppend
    (@accum [$($all:ident),+] ($($tuple:ident),+) $acc:ty; [$($bounds:tt)*] $next:ident $(, $rest:ident)*) => {
        impl_default_sink_tuple!(@accum
            [$($all),+]
            ($($tuple),+)
            <$acc as HAppend<<$next as DefaultSink<B>>::Sink>>::Output;
            [$($bounds)* $acc: HAppend<<$next as DefaultSink<B>>::Sink>,]
            $($rest),*
        );
    };
    // Entry: start accumulating from first type's sink
    ($first:ident $(, $rest:ident)+) => {
        impl_default_sink_tuple!(@accum
            [$first $(, $rest)+]
            ($first $(, $rest)+)
            <$first as DefaultSink<B>>::Sink;
            []
            $($rest),+
        );
    };
}

impl_default_sink_tuple!(T1, T2);
impl_default_sink_tuple!(T1, T2, T3);
impl_default_sink_tuple!(T1, T2, T3, T4);

// ============================================================================
// SampleIter and Distribution
// ============================================================================

pub trait SampleIter {
    type Item;
    fn next(&mut self, rng: &mut impl Rng) -> Self::Item;
}

pub trait Distribution<B: Backend> {
    type Item: DefaultSink<B>;
    type RowIter: SampleIter<Item = Self::Item>;

    fn row_iter(&mut self) -> Self::RowIter;

    fn collect_into(
        &mut self,
        sink: <Self::Item as DefaultSink<B>>::Sink,
        rng: &mut impl Rng,
        n: usize,
    ) -> <Self::Item as DefaultSink<B>>::Sink;
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

    // fn push_n_list(&mut self, len: usize, elems: impl Iterator<Item = T>);
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
// DistributionExt — ergonomic methods (collect_flat, flatten)
// ============================================================================

pub trait DistributionExt<B: Backend>: Distribution<B> {
    fn collect_flat<T>(&mut self, sink: T, rng: &mut impl Rng, n: usize) -> T
    where
        T: IntoHList<HList = <Self::Item as DefaultSink<B>>::Sink>,
        <Self::Item as DefaultSink<B>>::Sink: IntoTuple<Tuple = T>,
    {
        self.collect_into(sink.into_hlist(), rng, n).into_tuple()
    }

    fn flatten(self) -> Flatten<Self>
    where
        Self: Sized,
        Self::Item: ValueHList,
        <Self::Item as ValueHList>::HList: IntoTuple,
        <<Self::Item as ValueHList>::HList as IntoTuple>::Tuple:
            DefaultSink<B, Sink = <Self::Item as DefaultSink<B>>::Sink>,
    {
        Flatten(self)
    }
}

impl<B: Backend, D: Distribution<B>> DistributionExt<B> for D {}

// ============================================================================
// Flatten<D> — distribution wrapper that flattens nested Item types
// ============================================================================

pub struct Flatten<D>(pub D);

pub struct FlattenIter<I>(pub I);

impl<I: SampleIter> SampleIter for FlattenIter<I>
where
    I::Item: ValueHList,
    <I::Item as ValueHList>::HList: IntoTuple,
{
    type Item = <<I::Item as ValueHList>::HList as IntoTuple>::Tuple;
    fn next(&mut self, rng: &mut impl Rng) -> Self::Item {
        self.0.next(rng).into_hlist().into_tuple()
    }
}

impl<B: Backend, D: Distribution<B>> Distribution<B> for Flatten<D>
where
    D::Item: ValueHList,
    <D::Item as ValueHList>::HList: IntoTuple,
    <<D::Item as ValueHList>::HList as IntoTuple>::Tuple:
        DefaultSink<B, Sink = <D::Item as DefaultSink<B>>::Sink>,
{
    type Item = <<D::Item as ValueHList>::HList as IntoTuple>::Tuple;
    type RowIter = FlattenIter<D::RowIter>;

    fn row_iter(&mut self) -> Self::RowIter {
        FlattenIter(self.0.row_iter())
    }

    fn collect_into(
        &mut self,
        sink: <Self::Item as DefaultSink<B>>::Sink,
        rng: &mut impl Rng,
        n: usize,
    ) -> <Self::Item as DefaultSink<B>>::Sink {
        self.0.collect_into(sink, rng, n)
    }
}
