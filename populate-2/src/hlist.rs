// ============================================================================
// HList — type-level linked list
// ============================================================================

#[derive(Debug, Default, PartialEq)]
pub struct HNil;
#[derive(Debug, Default, PartialEq)]
pub struct HCons<H, T>(pub H, pub T);

// ============================================================================
// HAppend — concatenate two HLists, and split them back apart
// ============================================================================

pub trait HAppend<R> {
    type Output;
    fn append(self, right: R) -> Self::Output;
    fn unappend(combined: Self::Output) -> (Self, R)
    where
        Self: Sized;
}

impl<R> HAppend<R> for HNil {
    type Output = R;

    fn append(self, right: R) -> R {
        right
    }

    fn unappend(combined: R) -> (HNil, R) {
        (HNil, combined)
    }
}

impl<H, T: HAppend<R>, R> HAppend<R> for HCons<H, T> {
    type Output = HCons<H, T::Output>;

    fn append(self, right: R) -> HCons<H, T::Output> {
        HCons(self.0, self.1.append(right))
    }

    fn unappend(combined: HCons<H, T::Output>) -> (HCons<H, T>, R) {
        let (tail, right) = T::unappend(combined.1);
        (HCons(combined.0, tail), right)
    }
}

// ============================================================================
// IntoHList / IntoTuple — conversion between flat tuples and HLists
// ============================================================================

pub trait IntoHList {
    type HList;
    fn into_hlist(self) -> Self::HList;
}

pub trait IntoTuple {
    type Tuple;
    fn into_tuple(self) -> Self::Tuple;
}

// 1-tuple
impl<A> IntoHList for (A,) {
    type HList = HCons<A, HNil>;

    fn into_hlist(self) -> Self::HList {
        HCons(self.0, HNil)
    }
}

impl<A> IntoTuple for HCons<A, HNil> {
    type Tuple = (A,);

    fn into_tuple(self) -> (A,) {
        (self.0,)
    }
}

// 2-tuple
impl<A, B> IntoHList for (A, B) {
    type HList = HCons<A, HCons<B, HNil>>;

    fn into_hlist(self) -> Self::HList {
        HCons(self.0, HCons(self.1, HNil))
    }
}

impl<A, B> IntoTuple for HCons<A, HCons<B, HNil>> {
    type Tuple = (A, B);

    fn into_tuple(self) -> (A, B) {
        (self.0, self.1.0)
    }
}

// 3-tuple
impl<A, B, C> IntoHList for (A, B, C) {
    type HList = HCons<A, HCons<B, HCons<C, HNil>>>;

    fn into_hlist(self) -> Self::HList {
        HCons(self.0, HCons(self.1, HCons(self.2, HNil)))
    }
}

impl<A, B, C> IntoTuple for HCons<A, HCons<B, HCons<C, HNil>>> {
    type Tuple = (A, B, C);

    fn into_tuple(self) -> (A, B, C) {
        (self.0, self.1.0, self.1.1.0)
    }
}

// 4-tuple
impl<A, B, C, D> IntoHList for (A, B, C, D) {
    type HList = HCons<A, HCons<B, HCons<C, HCons<D, HNil>>>>;

    fn into_hlist(self) -> Self::HList {
        HCons(self.0, HCons(self.1, HCons(self.2, HCons(self.3, HNil))))
    }
}

impl<A, B, C, D> IntoTuple for HCons<A, HCons<B, HCons<C, HCons<D, HNil>>>> {
    type Tuple = (A, B, C, D);

    fn into_tuple(self) -> (A, B, C, D) {
        (self.0, self.1.0, self.1.1.0, self.1.1.1.0)
    }
}

// 5-tuple
impl<A, B, C, D, E> IntoHList for (A, B, C, D, E) {
    type HList = HCons<A, HCons<B, HCons<C, HCons<D, HCons<E, HNil>>>>>;

    fn into_hlist(self) -> Self::HList {
        HCons(
            self.0,
            HCons(self.1, HCons(self.2, HCons(self.3, HCons(self.4, HNil)))),
        )
    }
}

impl<A, B, C, D, E> IntoTuple for HCons<A, HCons<B, HCons<C, HCons<D, HCons<E, HNil>>>>> {
    type Tuple = (A, B, C, D, E);

    fn into_tuple(self) -> (A, B, C, D, E) {
        (self.0, self.1.0, self.1.1.0, self.1.1.1.0, self.1.1.1.1.0)
    }
}

// ============================================================================
// ValueHList — recursive value-to-HList (mirrors DefaultSink but for values)
// ============================================================================

pub trait ValueHList {
    type HList;
    fn into_hlist(self) -> Self::HList;
}

impl<T> ValueHList for Vec<T> {
    type HList = HCons<Vec<T>, HNil>;
    fn into_hlist(self) -> Self::HList {
        HCons(self, HNil)
    }
}

macro_rules! impl_value_hlist_tuple {
    // Terminal: emit the impl; generate body from accumulated indices
    (@accum [$($all:ident),+] ($($tuple:ident),+) $acc:ty; [$($bounds:tt)*] [$first_idx:tt $(, $rest_idx:tt)*]) => {
        impl<$($all: ValueHList),+> ValueHList for ($($tuple),+)
        where $($bounds)*
        {
            type HList = $acc;
            fn into_hlist(self) -> Self::HList {
                self.$first_idx.into_hlist() $(.append(self.$rest_idx.into_hlist()))*
            }
        }
    };
    // Recursive: fold in next type's HList via HAppend, collect index
    (@accum [$($all:ident),+] ($($tuple:ident),+) $acc:ty; [$($bounds:tt)*] [$($indices:tt)*] $idx:tt $next:ident $(, $rest_idx:tt $rest:ident)*) => {
        impl_value_hlist_tuple!(@accum
            [$($all),+]
            ($($tuple),+)
            <$acc as HAppend<<$next as ValueHList>::HList>>::Output;
            [$($bounds)* $acc: HAppend<<$next as ValueHList>::HList>,]
            [$($indices)*, $idx]
            $($rest_idx $rest),*
        );
    };
    // Entry: start accumulating from first type
    ($first_idx:tt $first:ident $(, $rest_idx:tt $rest:ident)+) => {
        impl_value_hlist_tuple!(@accum
            [$first $(, $rest)+]
            ($first $(, $rest)+)
            <$first as ValueHList>::HList;
            []
            [$first_idx]
            $($rest_idx $rest),+
        );
    };
}

impl_value_hlist_tuple!(0 A, 1 B);
impl_value_hlist_tuple!(0 A, 1 B, 2 C);
impl_value_hlist_tuple!(0 A, 1 B, 2 C, 3 D);

// Scalar ValueHList impls — generated via for_all_primitives! in traits.rs
