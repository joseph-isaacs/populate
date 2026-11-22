use rand::Rng;
use std::collections::VecDeque;

// Re-export HList types so existing `use crate::traits::{HCons, ...}` keeps working
pub use crate::hlist::{HAppend, HCons, HNil, IntoHList, IntoTuple, ValueHList};
use crate::hlist::{hlist_ty, hlist_val};

// ============================================================================
// Backend — selects concrete sink types for each column kind
// ============================================================================

pub trait Backend {
    type Prim<T>: Sink<T>;
    /// Sink for list offsets. Receives the running end offset of each row.
    type Offsets: Sink<usize>;
}

pub struct VecBackend;

impl Backend for VecBackend {
    type Prim<T> = Vec<T>;
    type Offsets = Vec<usize>;
}

pub struct DequeBackend;

impl Backend for DequeBackend {
    type Prim<T> = VecDeque<T>;
    type Offsets = VecDeque<usize>;
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

// A list column = offsets column + the element type's *own* columnar sink.
// Recurses: Vec<(A, B)> -> ListCol<offsets, HCons<A-col, HCons<B-col, HNil>>>
//           Vec<Vec<T>> -> ListCol<offsets, HCons<ListCol<offsets, ..>, HNil>>
impl<B: Backend, T: DefaultSink<B>> DefaultSink<B> for Vec<T> {
    type Sink = HCons<ListCol<B::Offsets, T::Sink>, HNil>;
}

/// Columnar list sink: end-offsets per row plus child column sinks.
#[derive(Debug, Default, PartialEq)]
pub struct ListCol<O, V> {
    pub offsets: O,
    pub values: V,
    /// running number of elements written (== last offset)
    pub len: usize,
}

impl<O, V> ListCol<O, V> {
    pub fn new(offsets: O, values: V) -> Self {
        Self {
            offsets,
            values,
            len: 0,
        }
    }
}

// Row-mode fallback (e.g. a Map that returns Vec<T>): explode into columns.
impl<T, O: Sink<usize>, V: Sink<T>> Sink<Vec<T>> for ListCol<O, V> {
    fn push(&mut self, value: Vec<T>) {
        self.len += value.len();
        self.values.push_n(value.into_iter());
        self.offsets.push(self.len);
    }

    // Only the offsets count is known up front (one per row).
    fn reserve(&mut self, additional: usize) {
        self.offsets.reserve(additional);
    }
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

/// Row-at-a-time collection: pull `n` items from `iter` and push them into
/// `sink`. The `collect_into` of every leaf (and of `Map`) is this.
pub fn collect_rows<I: SampleIter, S: Sink<I::Item>>(
    mut iter: I,
    mut sink: S,
    rng: &mut impl Rng,
    n: usize,
) -> S {
    sink.reserve(n);
    for _ in 0..n {
        sink.push(iter.next(rng));
    }
    sink
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

    /// Hint that `additional` more values are about to be pushed. Columnar
    /// collection always knows the exact count, so buffers are allocated once
    /// at their final size and never grown (no realloc copies).
    fn reserve(&mut self, _additional: usize) {}

    fn push_n(&mut self, value: impl Iterator<Item = T>) {
        value.for_each(|v| self.push(v));
    }
}

// Vec<T> is a primitive sink
impl<T> Sink<T> for Vec<T> {
    fn push(&mut self, value: T) {
        Vec::push(self, value);
    }

    fn reserve(&mut self, additional: usize) {
        Vec::reserve(self, additional);
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

    fn reserve(&mut self, additional: usize) {
        VecDeque::reserve(self, additional);
    }
}

// HList Sink impls — bridge between leaf distributions and HList sinks

// Scalar into 1-element HList
impl<T, S: Sink<T>> Sink<T> for HCons<S, HNil> {
    fn push(&mut self, value: T) {
        self.0.push(value);
    }

    fn reserve(&mut self, additional: usize) {
        self.0.reserve(additional);
    }

    fn push_n(&mut self, value: impl Iterator<Item = T>) {
        self.0.push_n(value);
    }
}

// Tuples into an HList of sinks (columnar) or a tuple of sinks (row tests).
macro_rules! impl_tuple_sinks {
    ($($T:ident $S:ident $v:ident $s:ident),+) => {
        impl<$($T, $S: Sink<$T>),+> Sink<($($T,)+)> for hlist_ty!($($S),+) {
            fn push(&mut self, ($($v,)+): ($($T,)+)) {
                let hlist_val!($($s),+) = self;
                $($s.push($v);)+
            }

            fn reserve(&mut self, n: usize) {
                let hlist_val!($($s),+) = self;
                $($s.reserve(n);)+
            }
        }

        impl<$($T, $S: Sink<$T>),+> Sink<($($T,)+)> for ($($S,)+) {
            fn push(&mut self, ($($v,)+): ($($T,)+)) {
                let ($($s,)+) = self;
                $($s.push($v);)+
            }

            fn reserve(&mut self, n: usize) {
                let ($($s,)+) = self;
                $($s.reserve(n);)+
            }
        }
    };
}

impl_tuple_sinks!(A SA a sa, B SB b sb);
impl_tuple_sinks!(A SA a sa, B SB b sb, C SC c sc);
impl_tuple_sinks!(A SA a sa, B SB b sb, C SC c sc, D SD d sd);

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
// DistributionScalar — convenience for single-column distributions
// ============================================================================

pub trait DistributionScalar<B: Backend>: Distribution<B> {
    fn collect_col<S>(&mut self, sink: S, rng: &mut impl Rng, n: usize) -> S
    where
        Self::Item: DefaultSink<B, Sink = HCons<S, HNil>>,
    {
        self.collect_into(HCons(sink, HNil), rng, n).0
    }
}

impl<B: Backend, D: Distribution<B>> DistributionScalar<B> for D {}

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
