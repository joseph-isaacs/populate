//! [`with_column`]: add a column computed from the columns before it, columnar.
//!
//! `with_column(parent, f)` yields every column of `parent` followed by one new
//! column, `f(parent_row, rng)`. It collects column by column: `parent` fills
//! its own columns first (at full columnar speed, whatever it is), then the
//! new column is computed in one pass over those just-written buffers. Rows
//! are never materialised as tuples and nothing goes through `collect_rows`.
//!
//! The output is flat, so `with_column`s chain:
//!
//! ```
//! use populate_builtins::{uniform, with_column};
//! use populate_core::traits::{DistributionExt, VecBackend};
//! use rand::Rng;
//!
//! let receipt = with_column(uniform(8035i32, 10_591), |ship, rng| {
//!     ship + rng.random_range(1..=30)
//! });
//! let mut d = with_column(receipt, |(ship, receipt), _| receipt - ship > 14);
//!
//! let (ship, receipt, late): (Vec<i32>, Vec<i32>, Vec<bool>) =
//!     DistributionExt::<VecBackend>::collect_flat(
//!         &mut d,
//!         Default::default(),
//!         &mut rand::rng(),
//!         100,
//!     );
//! assert!(ship.iter().zip(&receipt).all(|(s, r)| (s + 1..=s + 30).contains(r)));
//! assert!(ship.iter().zip(&receipt).zip(&late).all(|((s, r), l)| *l == (r - s > 14)));
//! ```
//!
//! The closure's RNG is a concrete [`ColumnRng`] seeded from the caller's
//! RNG, so draws inline instead of going through `dyn RngCore`.
//!
//! Parents must be made of scalar columns (up to 11 before the new one), so
//! their rows can be read back from the sink.

use populate_core::traits::{
    Backend, DefaultSink, Distribution, HAppend, HCons, HNil, SampleIter, Sink,
};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use std::collections::VecDeque;

// ============================================================================
// Reading back what a sink wrote
// ============================================================================

/// A scalar column sink whose values can be read back.
pub trait Column {
    type Elem: Copy;
    /// Number of values written so far.
    fn count(&self) -> usize;
    /// Values from index `start` on.
    fn iter_from(&self, start: usize) -> impl Iterator<Item = Self::Elem> + '_;
}

impl<T: Copy> Column for Vec<T> {
    type Elem = T;
    fn count(&self) -> usize {
        Vec::len(self)
    }
    fn iter_from(&self, start: usize) -> impl Iterator<Item = T> + '_ {
        self[start..].iter().copied()
    }
}

impl<T: Copy> Column for VecDeque<T> {
    type Elem = T;
    fn count(&self) -> usize {
        VecDeque::len(self)
    }
    fn iter_from(&self, start: usize) -> impl Iterator<Item = T> + '_ {
        self.range(start..).copied()
    }
}

/// An HList of scalar column sinks, read back as rows. `Row` is the same flat
/// value the distribution's `Item` is.
pub trait ReadRows {
    type Row;
    fn rows(&self) -> usize;
    /// Rows from index `start` on (zipped column iterators, no per-row
    /// bounds checks).
    fn rows_from(&self, start: usize) -> impl Iterator<Item = Self::Row> + '_;
}

impl<A: Column> ReadRows for HCons<A, HNil> {
    type Row = A::Elem;
    fn rows(&self) -> usize {
        self.0.count()
    }
    fn rows_from(&self, start: usize) -> impl Iterator<Item = Self::Row> + '_ {
        self.0.iter_from(start)
    }
}

macro_rules! hlist_ty {
    () => { HNil };
    ($h:ident $(, $t:ident)*) => { HCons<$h, hlist_ty!($($t),*)> };
}

macro_rules! hlist_pat {
    () => { HNil };
    ($h:ident $(, $t:ident)*) => { HCons($h, hlist_pat!($($t),*)) };
}

// `a.iter_from(s).zip(b.iter_from(s)).zip(..).map(|((a, b), ..)| (a, b, ..))`:
// nested zips stay `TrustedLen`, so `Vec::extend` still knows the length.
macro_rules! zip_rows {
    (@acc $start:ident, $iter:expr, $pat:pat, [$($done:ident),*];) => {
        $iter.map(|$pat| ($($done,)*))
    };
    (@acc $start:ident, $iter:expr, $pat:pat, [$($done:ident),*]; $next:ident $(, $rest:ident)*) => {
        zip_rows!(@acc $start, $iter.zip($next.iter_from($start)), ($pat, $next),
            [$($done,)* $next]; $($rest),*)
    };
    ($start:ident; $first:ident $(, $rest:ident)*) => {
        zip_rows!(@acc $start, $first.iter_from($start), $first, [$first]; $($rest),*)
    };
}

macro_rules! read_rows {
    ($($T:ident $v:ident),+) => {
        impl<$($T: Column),+> ReadRows for hlist_ty!($($T),+) {
            type Row = ($($T::Elem,)+);
            fn rows(&self) -> usize {
                self.0.count()
            }
            fn rows_from(&self, start: usize) -> impl Iterator<Item = Self::Row> + '_ {
                let hlist_pat!($($v),+) = self;
                zip_rows!(start; $($v),+)
            }
        }
    };
}

read_rows!(A a, B b);
read_rows!(A a, B b, C c);
read_rows!(A a, B b, C c, D d);
read_rows!(A a, B b, C c, D d, E e);
read_rows!(A a, B b, C c, D d, E e, F f);
read_rows!(A a, B b, C c, D d, E e, F f, G g);
read_rows!(A a, B b, C c, D d, E e, F f, G g, H h);
read_rows!(A a, B b, C c, D d, E e, F f, G g, H h, I i);
read_rows!(A a, B b, C c, D d, E e, F f, G g, H h, I i, J j);
read_rows!(A a, B b, C c, D d, E e, F f, G g, H h, I i, J j, K k);

// ============================================================================
// Flat row + one more value
// ============================================================================

/// `T` with a value `U` appended, kept flat: `i32 + U = (i32, U)`,
/// `(A, B) + U = (A, B, U)`.
pub trait RowAppend<U> {
    type Out;
    fn with(self, u: U) -> Self::Out;
}

macro_rules! impl_row_append_scalar {
    ($($t:ty),*) => {
        $(impl<U> RowAppend<U> for $t {
            type Out = ($t, U);
            fn with(self, u: U) -> Self::Out {
                (self, u)
            }
        })*
    };
}

impl_row_append_scalar!(i8, i16, i32, i64, u8, u16, u32, u64, f32, f64, bool, usize);

macro_rules! row_append_tuple {
    ($($T:ident $v:ident),+) => {
        impl<$($T,)+ U> RowAppend<U> for ($($T,)+) {
            type Out = ($($T,)+ U);
            fn with(self, u: U) -> Self::Out {
                let ($($v,)+) = self;
                ($($v,)+ u)
            }
        }
    };
}

row_append_tuple!(A a, B b);
row_append_tuple!(A a, B b, C c);
row_append_tuple!(A a, B b, C c, D d);
row_append_tuple!(A a, B b, C c, D d, E e);
row_append_tuple!(A a, B b, C c, D d, E e, F f);
row_append_tuple!(A a, B b, C c, D d, E e, F f, G g);
row_append_tuple!(A a, B b, C c, D d, E e, F f, G g, H h);
row_append_tuple!(A a, B b, C c, D d, E e, F f, G g, H h, I i);
row_append_tuple!(A a, B b, C c, D d, E e, F f, G g, H h, I i, J j);
row_append_tuple!(A a, B b, C c, D d, E e, F f, G g, H h, I i, J j, K k);

// ============================================================================
// WithColumn
// ============================================================================

pub struct WithColumn<P, F> {
    pub parent: P,
    pub f: F,
}

/// The RNG a [`with_column`] closure gets: a concrete, small, fast generator, so
/// the closure's draws inline (no `dyn` call per value). It's seeded from the
/// caller's RNG once per `collect_into` (or once per row iterator), so output
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
    WithColumn { parent, f }
}

pub struct WithColumnIter<I, F> {
    parent: I,
    f: F,
    rng: Option<ColumnRng>,
}

impl<I, F, U> SampleIter for WithColumnIter<I, F>
where
    I: SampleIter,
    I::Item: RowAppend<U> + Copy,
    F: Fn(I::Item, &mut ColumnRng) -> U,
{
    type Item = <I::Item as RowAppend<U>>::Out;

    fn next(&mut self, rng: &mut impl Rng) -> Self::Item {
        let row = self.parent.next(rng);
        let child = self.rng.get_or_insert_with(|| ColumnRng::from_rng(rng));
        let u = (self.f)(row, child);
        row.with(u)
    }
}

type SinkOf<B, T> = <T as DefaultSink<B>>::Sink;

impl<B, P, F, U> Distribution<B> for WithColumn<P, F>
where
    B: Backend,
    P: Distribution<B>,
    P::Item: RowAppend<U> + Copy,
    U: DefaultSink<B>,
    SinkOf<B, U>: Sink<U>,
    SinkOf<B, P::Item>: ReadRows<Row = P::Item> + HAppend<SinkOf<B, U>>,
    <P::Item as RowAppend<U>>::Out:
        DefaultSink<B, Sink = <SinkOf<B, P::Item> as HAppend<SinkOf<B, U>>>::Output>,
    F: Fn(P::Item, &mut ColumnRng) -> U + Clone,
{
    type Item = <P::Item as RowAppend<U>>::Out;
    type RowIter = WithColumnIter<P::RowIter, F>;

    fn row_iter(&mut self) -> Self::RowIter {
        WithColumnIter {
            parent: self.parent.row_iter(),
            f: self.f.clone(),
            rng: None,
        }
    }

    fn collect_into(
        &mut self,
        sink: SinkOf<B, Self::Item>,
        rng: &mut impl Rng,
        n: usize,
    ) -> SinkOf<B, Self::Item> {
        let (parent, mut col) = <SinkOf<B, P::Item> as HAppend<SinkOf<B, U>>>::unappend(sink);
        let start = parent.rows();
        // 1. the parent's columns, however it collects them
        let parent = self.parent.collect_into(parent, rng, n);
        // 2. one pass over what it just wrote
        col.reserve(n);
        let f = &self.f;
        let mut child = ColumnRng::from_rng(rng);
        col.push_n(parent.rows_from(start).map(|row| f(row, &mut child)));
        parent.append(col)
    }
}
