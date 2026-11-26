//! [`RowRefs`]: reading collected columns a row at a time, in place, for
//! the closures of [`derive`](crate::derive()) and
//! [`flat_map`](crate::flat_map).

use populate_core::Nullable;

/// Columns whose rows can be read in place, without copying them out:
/// a row of a `Vec<T>` is a `&T`, a row of a tuple of columns a tuple of
/// such rows, so a row too big to copy cheaply (a fixed block of draws
/// per row) is never copied onto the stack.
pub trait RowRefs {
    /// One row, borrowed from the columns.
    type Ref<'a>: Copy
    where
        Self: 'a;

    /// Every row, in order.
    fn row_refs(&self) -> impl Iterator<Item = Self::Ref<'_>> + '_;

    /// Drop every row, keeping the buffers to collect into again.
    fn clear(&mut self);
}

impl<T> RowRefs for Vec<T> {
    type Ref<'a>
        = &'a T
    where
        T: 'a;

    fn row_refs(&self) -> impl Iterator<Item = &T> + '_ {
        self.iter()
    }

    fn clear(&mut self) {
        Vec::clear(self);
    }
}

/// A row of `Option` columns is an `Option` of the row of its values.
impl<C: RowRefs> RowRefs for Nullable<C> {
    type Ref<'a>
        = Option<C::Ref<'a>>
    where
        C: 'a;

    fn row_refs(&self) -> impl Iterator<Item = Self::Ref<'_>> + '_ {
        self.values
            .row_refs()
            .zip(self.valid.iter())
            .map(|(v, ok)| ok.then_some(v))
    }

    fn clear(&mut self) {
        self.values.clear();
        self.valid.clear();
    }
}

macro_rules! tuple_refs {
    ($($T:ident $v:ident),+) => {
        impl<$($T: RowRefs),+> RowRefs for ($($T,)+) {
            type Ref<'a> = ($($T::Ref<'a>,)+) where Self: 'a;

            fn row_refs(&self) -> impl Iterator<Item = Self::Ref<'_>> + '_ {
                let ($($v,)+) = self;
                zip_refs!($($v),+)
            }

            fn clear(&mut self) {
                let ($($v,)+) = self;
                $($v.clear();)+
            }
        }
    };
}

// `a.row_refs().zip(b.row_refs()).zip(..).map(|((a, b), ..)| (a, b, ..))`
macro_rules! zip_refs {
    (@acc $iter:expr, $pat:pat, [$($done:ident),*];) => {
        $iter.map(|$pat| ($($done,)*))
    };
    (@acc $iter:expr, $pat:pat, [$($done:ident),*]; $next:ident $(, $rest:ident)*) => {
        zip_refs!(@acc $iter.zip($next.row_refs()), ($pat, $next),
            [$($done,)* $next]; $($rest),*)
    };
    ($first:ident $(, $rest:ident)*) => {
        zip_refs!(@acc $first.row_refs(), $first, [$first]; $($rest),*)
    };
}

tuple_refs!(A a);
tuple_refs!(A a, B b);
tuple_refs!(A a, B b, C c);
tuple_refs!(A a, B b, C c, D d);
tuple_refs!(A a, B b, C c, D d, E e);
tuple_refs!(A a, B b, C c, D d, E e, F f);
tuple_refs!(A a, B b, C c, D d, E e, F f, G g);
tuple_refs!(A a, B b, C c, D d, E e, F f, G g, H h);
tuple_refs!(A a, B b, C c, D d, E e, F f, G g, H h, I i);
tuple_refs!(A a, B b, C c, D d, E e, F f, G g, H h, I i, J j);
tuple_refs!(A a, B b, C c, D d, E e, F f, G g, H h, I i, J j, K k);
