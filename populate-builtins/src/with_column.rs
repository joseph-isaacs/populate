//! [`with_column`]: add a column computed from the columns before it, columnar.
//!
//! `with_column(parent, f)` yields every column of `parent` followed by one new
//! column, `f(parent_row, rng)`. It collects column by column: `parent` fills
//! its own columns first (at full columnar speed, whatever it is), then the
//! new column is computed in one pass over those just-written buffers. Rows
//! are never materialised and the parent is never sampled row by row.
//!
//! The output is flat, so `with_column`s chain:
//!
//! ```
//! use populate_builtins::{uniform, with_column};
//! use populate_core::Distribution;
//! use rand::Rng;
//!
//! let receipt = with_column(uniform(8035i32, 10_591), |ship, rng| {
//!     ship + rng.random_range(1..=30)
//! });
//! let mut d = with_column(receipt, |(ship, receipt), _| receipt - ship > 14);
//!
//! let (ship, receipt, late): (Vec<i32>, Vec<i32>, Vec<bool>) = d.collect(&mut rand::rng(), 100);
//! assert!(ship.iter().zip(&receipt).all(|(s, r)| (s + 1..=s + 30).contains(r)));
//! assert!(ship.iter().zip(&receipt).zip(&late).all(|((s, r), l)| *l == (r - s > 14)));
//! ```
//!
//! The closure's RNG is a concrete [`ColumnRng`] seeded from the caller's
//! RNG, so draws inline instead of going through `dyn RngCore`.
//!
//! Parents must be made of scalar columns (up to 11 before the new one), so
//! their rows can be read back from the columns.

use populate_core::{Columnar, Columns, Distribution, Nullable, Sink};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use std::mem::take;

// ============================================================================
// Reading back what was collected
// ============================================================================

/// Columns whose rows can be read back: a `Vec` of `Copy` values, or a tuple
/// of them. `Row` is the row type the columns were collected from.
pub trait ReadRows {
    type Row;
    /// Number of rows written so far.
    fn rows(&self) -> usize;
    /// Rows from index `start` on (zipped column iterators, no per-row
    /// bounds checks).
    fn rows_from(&self, start: usize) -> impl Iterator<Item = Self::Row> + '_;
}

impl<T: Copy> ReadRows for Vec<T> {
    type Row = T;
    fn rows(&self) -> usize {
        self.len()
    }
    fn rows_from(&self, start: usize) -> impl Iterator<Item = T> + '_ {
        self[start..].iter().copied()
    }
}

// `a.rows_from(s).zip(b.rows_from(s)).zip(..).map(|((a, b), ..)| (a, b, ..))`:
// nested zips stay `TrustedLen`, so `Vec::extend` still knows the length.
macro_rules! zip_rows {
    (@acc $start:ident, $iter:expr, $pat:pat, [$($done:ident),*];) => {
        $iter.map(|$pat| ($($done,)*))
    };
    (@acc $start:ident, $iter:expr, $pat:pat, [$($done:ident),*]; $next:ident $(, $rest:ident)*) => {
        zip_rows!(@acc $start, $iter.zip($next.rows_from($start)), ($pat, $next),
            [$($done,)* $next]; $($rest),*)
    };
    ($start:ident; $first:ident $(, $rest:ident)*) => {
        zip_rows!(@acc $start, $first.rows_from($start), $first, [$first]; $($rest),*)
    };
}

/// Columns whose rows can be read in place, without copying them out:
/// a row of a `Vec<T>` is a `&T`, a row of a tuple of columns a tuple of
/// such rows. For rows too big to copy cheaply (a fixed block of draws
/// per row), where [`ReadRows`] would copy every row onto the stack.
pub trait RowRefs {
    type Ref<'a>: Copy
    where
        Self: 'a;

    /// Every row, in order.
    fn row_refs(&self) -> impl Iterator<Item = Self::Ref<'_>> + '_;
}

impl<T> RowRefs for Vec<T> {
    type Ref<'a>
        = &'a T
    where
        T: 'a;

    fn row_refs(&self) -> impl Iterator<Item = &T> + '_ {
        self.iter()
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
            .zip(&self.valid)
            .map(|(v, &ok)| ok.then_some(v))
    }
}

impl<C: ReadRows> ReadRows for Nullable<C> {
    type Row = Option<C::Row>;
    fn rows(&self) -> usize {
        self.valid.len()
    }
    fn rows_from(&self, start: usize) -> impl Iterator<Item = Self::Row> + '_ {
        self.values
            .rows_from(start)
            .zip(&self.valid[start..])
            .map(|(v, &ok)| ok.then_some(v))
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

// ============================================================================
// Flat row + one more value
// ============================================================================

/// A row with a value `U` appended, kept flat: `i32 + U = (i32, U)`,
/// `(A, B) + U = (A, B, U)`. The columns follow: [`split`](Self::split) and
/// [`join`](Self::join) move the buffers between the columns of `Out` and
/// those of the two parts (nothing is copied).
pub trait RowAppend<U: Columnar>: Columnar {
    type Out: Columnar;
    fn with(self, u: U) -> Self::Out;
    fn split(cols: Columns<Self::Out>) -> (Columns<Self>, Columns<U>);
    fn join(cols: Columns<Self>, col: Columns<U>) -> Columns<Self::Out>;
}

macro_rules! row_append_scalar {
    ($($t:ty),*) => {
        $(impl<U: Columnar> RowAppend<U> for $t {
            type Out = ($t, U);
            fn with(self, u: U) -> Self::Out {
                (self, u)
            }
            fn split(cols: Columns<Self::Out>) -> (Vec<$t>, Columns<U>) {
                cols
            }
            fn join(cols: Vec<$t>, col: Columns<U>) -> Columns<Self::Out> {
                (cols, col)
            }
        })*
    };
}

row_append_scalar!(i8, i16, i32, i64, u8, u16, u32, u64, f32, f64, bool, usize);

macro_rules! tuple_rows {
    ($($T:ident $v:ident),+) => {
        impl<$($T: ReadRows),+> ReadRows for ($($T,)+) {
            type Row = ($($T::Row,)+);
            fn rows(&self) -> usize {
                self.0.rows()
            }
            fn rows_from(&self, start: usize) -> impl Iterator<Item = Self::Row> + '_ {
                let ($($v,)+) = self;
                zip_rows!(start; $($v),+)
            }
        }

        impl<$($T: Columnar,)+ U: Columnar> RowAppend<U> for ($($T,)+) {
            type Out = ($($T,)+ U);
            fn with(self, u: U) -> Self::Out {
                let ($($v,)+) = self;
                ($($v,)+ u)
            }
            fn split(cols: Columns<Self::Out>) -> (Columns<Self>, Columns<U>) {
                let ($($v,)+ u) = cols;
                (($($v,)+), u)
            }
            fn join(cols: Columns<Self>, col: Columns<U>) -> Columns<Self::Out> {
                let ($($v,)+) = cols;
                ($($v,)+ col)
            }
        }
    };
}

tuple_rows!(A a, B b);
tuple_rows!(A a, B b, C c);
tuple_rows!(A a, B b, C c, D d);
tuple_rows!(A a, B b, C c, D d, E e);
tuple_rows!(A a, B b, C c, D d, E e, F f);
tuple_rows!(A a, B b, C c, D d, E e, F f, G g);
tuple_rows!(A a, B b, C c, D d, E e, F f, G g, H h);
tuple_rows!(A a, B b, C c, D d, E e, F f, G g, H h, I i);
tuple_rows!(A a, B b, C c, D d, E e, F f, G g, H h, I i, J j);
tuple_rows!(A a, B b, C c, D d, E e, F f, G g, H h, I i, J j, K k);

// ============================================================================
// WithColumn
// ============================================================================

#[derive(Clone)]
pub struct WithColumn<P, F> {
    pub parent: P,
    pub f: F,
    /// The closure's RNG when sampling row by row, seeded at the first row.
    row_rng: Option<ColumnRng>,
}

/// The RNG a [`with_column`] closure gets: a concrete, small, fast generator, so
/// the closure's draws inline (no `dyn` call per value). It's seeded from the
/// caller's RNG once per `collect_into` (or at the first `sample`), so output
/// stays deterministic for a seeded caller RNG, but the new column draws
/// from this child stream rather than the caller's RNG directly.
pub type ColumnRng = SmallRng;

/// Every column of `parent`, then `f(row, rng)` as one more column.
/// See the module docs.
///
/// The bound here (rather than only on the `Distribution` impl) is what makes
/// a plain closure `|row, rng| ..` higher-ranked over the RNG borrow.
pub fn with_column<P, R, U, F>(parent: P, f: F) -> WithColumn<P, F>
where
    F: Fn(R, &mut ColumnRng) -> U,
{
    WithColumn {
        parent,
        f,
        row_rng: None,
    }
}

impl<P, F, U> Distribution for WithColumn<P, F>
where
    P: Distribution,
    P::Item: RowAppend<U> + Copy,
    Columns<P::Item>: ReadRows<Row = P::Item>,
    U: Columnar,
    F: Fn(P::Item, &mut ColumnRng) -> U,
{
    type Item = <P::Item as RowAppend<U>>::Out;

    fn sample(&mut self, rng: &mut impl Rng) -> Self::Item {
        let row = self.parent.sample(rng);
        let child = self.row_rng.get_or_insert_with(|| ColumnRng::from_rng(rng));
        let u = (self.f)(row, child);
        row.with(u)
    }

    fn collect_into(&mut self, cols: &mut Columns<Self::Item>, rng: &mut impl Rng, n: usize) {
        let (mut parent, mut col) = P::Item::split(take(cols));
        let start = parent.rows();
        // 1. the parent's columns, however it collects them
        self.parent.collect_into(&mut parent, rng, n);
        // 2. one pass over what it just wrote
        col.reserve(n);
        let f = &self.f;
        let mut child = ColumnRng::from_rng(rng);
        col.push_n(parent.rows_from(start).map(|row| f(row, &mut child)));
        *cols = P::Item::join(parent, col);
    }

    fn skip(&mut self, n: u64) {
        self.parent.skip(n);
    }
}
