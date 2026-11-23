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

// `hlist_ty!(A, B)` = `HCons<A, HCons<B, HNil>>`; `hlist_val!(a, b)` is the
// matching value / pattern. Used to generate the per-arity impls below and the
// tuple sink impls in `traits`.
macro_rules! hlist_ty {
    () => { HNil };
    ($h:ident $(, $t:ident)*) => { HCons<$h, hlist_ty!($($t),*)> };
}

macro_rules! hlist_val {
    () => { HNil };
    ($h:ident $(, $t:ident)*) => { HCons($h, hlist_val!($($t),*)) };
}

pub(crate) use {hlist_ty, hlist_val};

macro_rules! impl_tuple_hlist {
    ($($T:ident $v:ident),+) => {
        impl<$($T),+> IntoHList for ($($T,)+) {
            type HList = hlist_ty!($($T),+);

            fn into_hlist(self) -> Self::HList {
                let ($($v,)+) = self;
                hlist_val!($($v),+)
            }
        }

        impl<$($T),+> IntoTuple for hlist_ty!($($T),+) {
            type Tuple = ($($T,)+);

            fn into_tuple(self) -> Self::Tuple {
                let hlist_val!($($v),+) = self;
                ($($v,)+)
            }
        }
    };
}

impl_tuple_hlist!(A a);
impl_tuple_hlist!(A a, B b);
impl_tuple_hlist!(A a, B b, C c);
impl_tuple_hlist!(A a, B b, C c, D d);
impl_tuple_hlist!(A a, B b, C c, D d, E e);
impl_tuple_hlist!(A a, B b, C c, D d, E e, F f);
impl_tuple_hlist!(A a, B b, C c, D d, E e, F f, G g);
impl_tuple_hlist!(A a, B b, C c, D d, E e, F f, G g, H h);
impl_tuple_hlist!(A a, B b, C c, D d, E e, F f, G g, H h, I i);
impl_tuple_hlist!(A a, B b, C c, D d, E e, F f, G g, H h, I i, J j);
impl_tuple_hlist!(A a, B b, C c, D d, E e, F f, G g, H h, I i, J j, K k);
impl_tuple_hlist!(A a, B b, C c, D d, E e, F f, G g, H h, I i, J j, K k, L l);

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
impl_value_hlist_tuple!(0 A, 1 B, 2 C, 3 D, 4 E);
impl_value_hlist_tuple!(0 A, 1 B, 2 C, 3 D, 4 E, 5 F);
impl_value_hlist_tuple!(0 A, 1 B, 2 C, 3 D, 4 E, 5 F, 6 G);
impl_value_hlist_tuple!(0 A, 1 B, 2 C, 3 D, 4 E, 5 F, 6 G, 7 H);
impl_value_hlist_tuple!(0 A, 1 B, 2 C, 3 D, 4 E, 5 F, 6 G, 7 H, 8 I);
impl_value_hlist_tuple!(0 A, 1 B, 2 C, 3 D, 4 E, 5 F, 6 G, 7 H, 8 I, 9 J);
impl_value_hlist_tuple!(0 A, 1 B, 2 C, 3 D, 4 E, 5 F, 6 G, 7 H, 8 I, 9 J, 10 K);
impl_value_hlist_tuple!(0 A, 1 B, 2 C, 3 D, 4 E, 5 F, 6 G, 7 H, 8 I, 9 J, 10 K, 11 L);

// Scalar ValueHList impls — generated via for_all_primitives! in traits.rs
