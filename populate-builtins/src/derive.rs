//! [`derive`]: add a column computed from the columns before it, columnar.
//!
//! `derive(parent, f)` yields every column of `parent` followed by one new
//! column, `f(parent_row, rng)`. It collects column by column: `parent` fills
//! its own columns first (at full columnar speed, whatever it is), then the
//! new column is computed in one pass over those just-written buffers. Rows
//! are never materialised as tuples and nothing goes through `collect_rows`.
//!
//! The output is flat, so derives chain:
//!
//! ```ignore
//! let d = derive(derive(ship, |s, rng| s + rng.random_range(1..=30)),
//!                |(s, r), _| r - s > 14);       // Item = (i32, i32, bool)
//! ```
//!
//! Parents must be made of scalar columns (up to 3 before the new one), so
//! their rows can be read back from the sink.

use populate_core::traits::{
    Backend, DefaultSink, Distribution, HAppend, HCons, HNil, SampleIter, Sink,
};
use rand::{Rng, RngCore};
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

impl<A: Column, B: Column> ReadRows for HCons<A, HCons<B, HNil>> {
    type Row = (A::Elem, B::Elem);
    fn rows(&self) -> usize {
        self.0.count()
    }
    fn rows_from(&self, start: usize) -> impl Iterator<Item = Self::Row> + '_ {
        self.0.iter_from(start).zip(self.1.0.iter_from(start))
    }
}

impl<A: Column, B: Column, C: Column> ReadRows for HCons<A, HCons<B, HCons<C, HNil>>> {
    type Row = (A::Elem, B::Elem, C::Elem);
    fn rows(&self) -> usize {
        self.0.count()
    }
    fn rows_from(&self, start: usize) -> impl Iterator<Item = Self::Row> + '_ {
        self.0
            .iter_from(start)
            .zip(self.1.0.iter_from(start))
            .zip(self.1.1.0.iter_from(start))
            .map(|((a, b), c)| (a, b, c))
    }
}

// ============================================================================
// Flat row + one more value
// ============================================================================

/// `T` with a value `U` appended, kept flat: `i32 + U = (i32, U)`,
/// `(A, B) + U = (A, B, U)`.
pub trait WithColumn<U> {
    type Out;
    fn with(self, u: U) -> Self::Out;
}

macro_rules! impl_with_column_scalar {
    ($($t:ty),*) => {
        $(impl<U> WithColumn<U> for $t {
            type Out = ($t, U);
            fn with(self, u: U) -> Self::Out {
                (self, u)
            }
        })*
    };
}

impl_with_column_scalar!(i16, i32, i64, f64, bool, usize);

impl<A, B, U> WithColumn<U> for (A, B) {
    type Out = (A, B, U);
    fn with(self, u: U) -> Self::Out {
        (self.0, self.1, u)
    }
}

impl<A, B, C, U> WithColumn<U> for (A, B, C) {
    type Out = (A, B, C, U);
    fn with(self, u: U) -> Self::Out {
        (self.0, self.1, self.2, u)
    }
}

// ============================================================================
// Derive
// ============================================================================

pub struct Derive<P, F> {
    pub parent: P,
    pub f: F,
}

/// Every column of `parent`, then `f(row, rng)` as one more column.
/// See the module docs.
///
/// The bound here (rather than only on the `Distribution` impl) is what makes
/// a plain closure `|row, rng| ..` higher-ranked over the RNG borrow.
pub fn derive<P, R, U, F>(parent: P, f: F) -> Derive<P, F>
where
    F: Fn(R, &mut dyn RngCore) -> U,
{
    Derive { parent, f }
}

pub struct DeriveIter<I, F> {
    parent: I,
    f: F,
}

impl<I, F, U> SampleIter for DeriveIter<I, F>
where
    I: SampleIter,
    I::Item: WithColumn<U> + Copy,
    F: Fn(I::Item, &mut dyn RngCore) -> U,
{
    type Item = <I::Item as WithColumn<U>>::Out;

    fn next(&mut self, rng: &mut impl Rng) -> Self::Item {
        let row = self.parent.next(rng);
        let u = (self.f)(row, rng);
        row.with(u)
    }
}

type SinkOf<B, T> = <T as DefaultSink<B>>::Sink;

impl<B, P, F, U> Distribution<B> for Derive<P, F>
where
    B: Backend,
    P: Distribution<B>,
    P::Item: WithColumn<U> + Copy,
    U: DefaultSink<B>,
    SinkOf<B, U>: Sink<U>,
    SinkOf<B, P::Item>: ReadRows<Row = P::Item> + HAppend<SinkOf<B, U>>,
    <P::Item as WithColumn<U>>::Out:
        DefaultSink<B, Sink = <SinkOf<B, P::Item> as HAppend<SinkOf<B, U>>>::Output>,
    F: Fn(P::Item, &mut dyn RngCore) -> U + Clone,
{
    type Item = <P::Item as WithColumn<U>>::Out;
    type RowIter = DeriveIter<P::RowIter, F>;

    fn row_iter(&mut self) -> Self::RowIter {
        DeriveIter {
            parent: self.parent.row_iter(),
            f: self.f.clone(),
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
        col.push_n(parent.rows_from(start).map(|row| f(row, rng)));
        parent.append(col)
    }
}
