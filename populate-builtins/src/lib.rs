//! Built-in distributions: [`constant`], [`range`], [`map`], [`list`],
//! [`draws`], [`derive()`], [`children`], [`flat_map`], [`fan_out`], [`named`],
//! [`column`], [`checkpointed`],
//! [`with_column`], and the random leaves in [`random`] ([`uniform`], [`choose`],
//! [`weighted`], [`text`], [`chars`]). Tuples of distributions are implemented in
//! `populate-core` (orphan rule).

use populate_core::{Columnar, Columns, Distribution, ListCol, in_local};
use rand::Rng;
use std::ops::Add;

mod compose;
mod stored;
mod with_column;
pub use compose::{
    ChildSlots, ChildTables, Derive, DeriveColumns, Draws, FanOut, FlatMap, Format, Named,
    children, derive, derive_columns, draws, draws_reserving, fan_out, flat_map, format, named,
    named_all,
};
pub use stored::{Checkpointed, Column, checkpointed, column, column_as};
pub use with_column::{ColumnRng, ReadRows, RowAppend, RowRefs, WithColumn, with_column};

pub mod bounded;
pub mod random;
pub use random::{
    Chars, Choose, Text, Uniform, UniformValue, Weighted, chars, choose, text, uniform, weighted,
};

// ============================================================================
// Constant
// ============================================================================

/// The same value in every row.
#[derive(Clone)]
pub struct Constant<T> {
    pub value: T,
}

pub fn constant<T>(value: T) -> Constant<T> {
    Constant { value }
}

impl<T: Copy + Columnar> Distribution for Constant<T> {
    type Item = T;

    fn sample(&mut self, _rng: &mut impl Rng) -> T {
        self.value
    }

    fn skip(&mut self, _n: u64) {}
}

// ============================================================================
// Range
// ============================================================================

/// `start`, `start + step`, `start + 2 * step`, ...
#[derive(Clone)]
pub struct Range<T> {
    /// The value of the next row.
    pub next: T,
    pub step: T,
}

pub fn range<T: Add>(start: T, step: T) -> Range<T> {
    Range { next: start, step }
}

/// A number a [`Range`] can count in: `self + step * n` in one go, for
/// [`Distribution::skip`].
pub trait RangeValue: Add<Output = Self> + Copy {
    /// `self + step * n`. Integers wrap; a float may differ in its last
    /// bits from adding `step` `n` times.
    fn advance(self, step: Self, n: u64) -> Self;
}

macro_rules! range_int {
    ($($t:ty),*) => {$(
        impl RangeValue for $t {
            fn advance(self, step: Self, n: u64) -> Self {
                // `n as $t` wraps, and so does the arithmetic: the result is
                // right modulo 2^bits, which is all a wrapped count can be
                self.wrapping_add(step.wrapping_mul(n as $t))
            }
        }
    )*};
}

range_int!(
    i8, i16, i32, i64, i128, isize, u8, u16, u32, u64, u128, usize
);

macro_rules! range_float {
    ($($t:ty),*) => {$(
        impl RangeValue for $t {
            fn advance(self, step: Self, n: u64) -> Self {
                self + step * n as $t
            }
        }
    )*};
}

range_float!(f32, f64);

impl<T: Columnar + RangeValue> Distribution for Range<T> {
    type Item = T;

    fn sample(&mut self, _rng: &mut impl Rng) -> T {
        let v = self.next;
        self.next = v + self.step;
        v
    }

    fn skip(&mut self, n: u64) {
        self.next = self.next.advance(self.step, n);
    }
}

// ============================================================================
// Map
// ============================================================================

/// `f` of every row of `source`. Collects row by row: `source` is sampled,
/// not collected, so prefer [`with_column`] or a tuple where it matters.
#[derive(Clone)]
pub struct Map<D, F> {
    pub source: D,
    pub f: F,
}

pub fn map<D, F, Out>(source: D, f: F) -> Map<D, F>
where
    D: Distribution,
    F: Fn(D::Item) -> Out,
{
    Map { source, f }
}

impl<D, F, Out> Distribution for Map<D, F>
where
    D: Distribution,
    F: Fn(D::Item) -> Out,
    Out: Columnar,
{
    type Item = Out;

    fn sample(&mut self, rng: &mut impl Rng) -> Out {
        (self.f)(self.source.sample(rng))
    }

    fn skip(&mut self, n: u64) {
        self.source.skip(n);
    }
}

// ============================================================================
// List
// ============================================================================

/// A list per row: its length from `lengths`, its elements from `elements`.
#[derive(Clone)]
pub struct List<L, E> {
    pub lengths: L,
    pub elements: E,
}

pub fn list<L, E>(lengths: L, elements: E) -> List<L, E> {
    List { lengths, elements }
}

impl<L, E> Distribution for List<L, E>
where
    L: Distribution<Item = usize>,
    E: Distribution,
{
    type Item = Vec<E::Item>;

    fn sample(&mut self, rng: &mut impl Rng) -> Vec<E::Item> {
        let len = self.lengths.sample(rng);
        (0..len).map(|_| self.elements.sample(rng)).collect()
    }

    // Fully columnar:
    //   pass 1: n lengths -> offsets column (and the total element count)
    //   pass 2: hand the *whole* element columns to the element distribution
    //           for `total` rows, so tuples/lists inside recurse col-by-col.
    fn collect_into(&mut self, cols: &mut ListCol<Columns<E::Item>>, rng: &mut impl Rng, n: usize) {
        cols.reserve_rows(n);
        let total = in_local(&mut self.lengths, |lengths| {
            cols.push_lengths((0..n).map(|_| lengths.sample(rng)))
        });
        self.elements.collect_into(&mut cols.values, rng, total);
    }

    fn columns(&self) -> ListCol<Columns<E::Item>> {
        let mut cols = ListCol::default();
        cols.values = self.elements.columns();
        cols
    }

    /// Skips `n` lengths. The elements stay where they are: how many of
    /// them `n` rows hold is random, so there is no row `n` to land on.
    fn skip(&mut self, n: u64) {
        self.lengths.skip(n);
    }
}
