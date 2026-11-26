//! Built-in distributions: [`constant`], [`range`], [`list`], [`draws`],
//! [`derive()`], [`derive_columns`], [`children`], [`flat_map`], [`named`],
//! [`column`], [`checkpointed`],
//! and the random leaves in [`random`] ([`uniform`], [`choose`],
//! [`weighted`], [`text`], [`chars`]). Tuples of distributions are implemented in
//! `populate-core` (orphan rule).

use populate_core::{Columnar, Columns, Distribution, ListCol, Sink, in_local};
use rand::Rng;
use std::ops::Add;

mod compose;
mod row_refs;
mod stored;
pub use compose::{
    ChildSlots, ChildTables, Derive, DeriveColumns, Draws, FlatMap, Format, Named, children,
    derive, derive_columns, draws, draws_reserving, flat_map, format, named, named_all,
};
pub use row_refs::RowRefs;
pub use stored::{Checkpointed, Column, checkpointed, column, column_as};

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

impl<T: Copy + Columnar<Columns: Sink<T>>> Distribution for Constant<T> {
    type Item = T;

    fn collect_into(&mut self, cols: &mut Columns<T>, _rng: &mut impl Rng, n: usize) {
        cols.reserve(n);
        cols.push_n(std::iter::repeat_n(self.value, n));
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

    fn collect_into(&mut self, cols: &mut Columns<T>, _rng: &mut impl Rng, n: usize) {
        cols.reserve(n);
        // `in_local` keeps the counter in a register across the loop
        in_local(&mut self.next, |next| {
            let step = self.step;
            cols.push_n((0..n).map(|_| {
                let v = *next;
                *next = v + step;
                v
            }));
        });
    }

    fn skip(&mut self, n: u64) {
        self.next = self.next.advance(self.step, n);
    }
}

// ============================================================================
// List
// ============================================================================

/// A list per row: its length from `lengths`, its elements from `elements`.
#[derive(Clone)]
pub struct List<L: Distribution, E> {
    pub lengths: L,
    pub elements: E,
    /// Lengths of a type other than `i32`, collected before they become
    /// offsets ([`ListLength`]); empty between collects.
    scratch: Vec<L::Item>,
}

/// A list per row: `lengths` sets each row's length, `elements` fills them.
/// `i32` lengths are collected straight into the offsets buffer; other
/// integer types go through a buffer the list keeps.
pub fn list<L: Distribution, E>(lengths: L, elements: E) -> List<L, E> {
    List {
        lengths,
        elements,
        scratch: Vec::new(),
    }
}

/// A type a [`list`]'s lengths can be: how its column of lengths becomes
/// a list's offsets.
pub trait ListLength: Columnar<Columns = Vec<Self>> + Copy {
    /// Append `n` rows to `cols`, their lengths collected from `lengths`
    /// (through `scratch` if need be), and return the sum of the lengths.
    fn collect_lengths<D: Distribution<Item = Self>, V>(
        lengths: &mut D,
        cols: &mut ListCol<V>,
        scratch: &mut Vec<Self>,
        rng: &mut impl Rng,
        n: usize,
    ) -> usize;
}

impl ListLength for i32 {
    fn collect_lengths<D: Distribution<Item = i32>, V>(
        lengths: &mut D,
        cols: &mut ListCol<V>,
        _scratch: &mut Vec<i32>,
        rng: &mut impl Rng,
        n: usize,
    ) -> usize {
        cols.push_lengths_in_place(|offsets| lengths.collect_into(offsets, rng, n))
    }
}

macro_rules! list_length {
    ($($t:ty),*) => {$(
        impl ListLength for $t {
            fn collect_lengths<D: Distribution<Item = $t>, V>(
                lengths: &mut D,
                cols: &mut ListCol<V>,
                scratch: &mut Vec<$t>,
                rng: &mut impl Rng,
                n: usize,
            ) -> usize {
                scratch.clear();
                lengths.collect_into(scratch, rng, n);
                cols.push_lengths(
                    scratch
                        .iter()
                        .map(|&l| usize::try_from(l).expect("list length fits in usize")),
                )
            }
        }
    )*};
}

list_length!(u8, u16, u32, u64, usize, i64);

impl<L, E> Distribution for List<L, E>
where
    L: Distribution<Item: ListLength>,
    E: Distribution,
{
    type Item = Vec<E::Item>;

    // Fully columnar:
    //   pass 1: n lengths -> offsets column (and the total element count)
    //   pass 2: hand the *whole* element columns to the element distribution
    //           for `total` rows, so tuples/lists inside recurse col-by-col.
    fn collect_into(&mut self, cols: &mut ListCol<Columns<E::Item>>, rng: &mut impl Rng, n: usize) {
        cols.reserve_rows(n);
        let total = L::Item::collect_lengths(&mut self.lengths, cols, &mut self.scratch, rng, n);
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
