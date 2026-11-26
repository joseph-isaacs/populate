#![warn(missing_docs)]
//! The built-in distributions: leaves that make a column from nothing, and
//! combinators that make one from other distributions. A tuple of
//! distributions is a distribution too (implemented in `populate-core`), so
//! a row of several columns is a tuple of these.
//!
//! ```
//! use populate_builtins::{choose, derive, list, named, range, uniform};
//! use populate_core::Distribution;
//!
//! let mut orders = (
//!     named("id", range(1i64, 1)),
//!     named("status", choose(&["open", "shipped"])),
//!     named("qty", uniform(1i64, 10)),
//!     // a list of 0 to 3 line ids per row
//!     named("lines", list(uniform(0i32, 3), range(0i64, 1))),
//!     named("even", derive(range(1i64, 1), |&id: &i64| id % 2 == 0)),
//! );
//! let (ids, status, qty, lines, even) = orders.collect(&mut rand::rng(), 4);
//! assert_eq!(ids, [1, 2, 3, 4]);
//! assert_eq!(status.len(), 4);
//! assert!(qty.iter().all(|q| (1..=10).contains(q)));
//! assert_eq!(lines.len(), 4);
//! assert_eq!(even, [false, true, false, true]);
//! ```
//!
//! # Which one to use
//!
//! | to make                                 | use                                  | `skip(n)`                      |
//! |-----------------------------------------|--------------------------------------|--------------------------------|
//! | the same value in every row             | [`constant`]                         | nothing                        |
//! | a counter                               | [`range`]                            | adds `n * step`                |
//! | each row's own number, as a `u64`       | [`row_numbers`]                      | moves the start                |
//! | uniform numbers, picks from a list      | [`uniform`], [`choose`], [`weighted`] | nothing                       |
//! | random strings                          | [`text`], [`chars`]                  | nothing                        |
//! | a value that depends only on its row    | [`draws`] over a [`SeekRng`](populate_core::SeekRng) | jumps the stream |
//! | a column already in memory              | [`column()`], [`column_as`]            | moves the cursor               |
//! | a stream that can't jump                | [`checkpointed`]                     | from the last saved state      |
//! | a list per row                          | [`list`]                             | skips the lengths only         |
//! | one column from a row of others         | [`derive()`]                         | passes `n` on                  |
//! | a string from a row of others           | [`format()`]                         | passes `n` on                  |
//! | several columns that share work         | [`derive_columns`]                   | passes `n` on                  |
//! | child rows, one stream per column       | [`children`]                         | passes `n` on                  |
//! | child rows written by a closure, or several child tables | [`flat_map`]        | passes `n` on                  |
//! | column names, for a schema              | [`named`], [`named_all`]             | passes `n` on                  |
//!
//! A table starts each batch by cloning its distribution and skipping to
//! the batch's first row, so the `skip` column is what a batch costs to
//! start and whether it starts in the right place. Random leaves don't
//! depend on their position, so they skip by doing nothing. A [`list`]
//! can't know how many elements `n` rows hold without drawing their
//! lengths, so it leaves its elements where they are: use elements that
//! don't depend on their position, or [`flat_map`] with a
//! [`draws`]-based source.

use populate_core::{Columnar, Columns, Distribution, ListCol, Sink, in_local};
use rand::Rng;
use std::ops::Add;

mod compose;
mod row_refs;
mod rows;
mod stored;
pub use compose::{
    ChildSlots, ChildTables, Derive, DeriveColumns, Draws, FlatMap, Format, Named, children,
    derive, derive_columns, draws, draws_reserving, flat_map, format, named, named_all,
};
pub use row_refs::RowRefs;
pub use rows::{Col, Row, RowNumbers, Rows, row_numbers};
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
    /// The value.
    pub value: T,
}

/// `value` in every row.
///
/// ```
/// use populate_builtins::constant;
/// use populate_core::Distribution;
///
/// assert_eq!(constant(7u8).collect(&mut rand::rng(), 3), [7, 7, 7]);
/// ```
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
    /// Added after every row.
    pub step: T,
}

/// `start`, `start + step`, `start + 2 * step`, ...: a row number when
/// `step` is 1, for any integer or float type ([`RangeValue`]).
///
/// ```
/// use populate_builtins::range;
/// use populate_core::Distribution;
///
/// let mut ids = range(10i64, 5);
/// assert_eq!(ids.collect(&mut rand::rng(), 3), [10, 15, 20]);
/// ids.skip(2);
/// assert_eq!(ids.collect(&mut rand::rng(), 1), [35]);
/// ```
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
    /// Each row's length.
    pub lengths: L,
    /// Every element of every row, end to end.
    pub elements: E,
    /// Lengths of a type other than `i32`, collected before they become
    /// offsets ([`ListLength`]); empty between collects.
    scratch: Vec<L::Item>,
}

/// A list per row: `lengths` sets each row's length, `elements` fills them.
/// `i32` lengths are collected straight into the offsets buffer; other
/// integer types go through a buffer the list keeps.
///
/// All `n` lengths are collected first, then `elements` collects their sum
/// in one go, so nested lists and lists of tuples stay columnar all the way
/// down. Skipping skips the lengths only (see the [crate docs](crate)).
///
/// ```
/// use populate_builtins::{constant, list, range};
/// use populate_core::Distribution;
///
/// let mut pairs = list(constant(2i32), range(0i64, 1));
/// let (offsets, values) = pairs.collect(&mut rand::rng(), 3).into_parts();
/// assert_eq!(offsets, [0, 2, 4, 6]);
/// assert_eq!(values, [0, 1, 2, 3, 4, 5]);
/// ```
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

    /// Advances the length source by `n` rows and leaves the element source
    /// in place. A future implementation could build a skip index from
    /// generated lengths, recording cumulative element counts at row
    /// checkpoints so a seek can start from the nearest checkpoint.
    fn skip(&mut self, n: u64) {
        self.lengths.skip(n);
    }
}
