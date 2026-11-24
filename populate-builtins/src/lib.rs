//! Built-in distributions: [`constant`], [`range`], [`map`], [`list`],
//! [`with_column`], and the random leaves in [`random`] ([`uniform`], [`choose`],
//! [`weighted`], [`text`], [`chars`]). Tuples of distributions are implemented in
//! `populate-core` (orphan rule).

use populate_core::{Columnar, Columns, Distribution, ListCol, in_local};
use rand::Rng;
use std::ops::Add;

mod with_column;
pub use with_column::{ColumnRng, ReadRows, RowAppend, WithColumn, with_column};

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

impl<T: Columnar + Add<Output = T> + Copy> Distribution for Range<T> {
    type Item = T;

    fn sample(&mut self, _rng: &mut impl Rng) -> T {
        let v = self.next;
        self.next = v + self.step;
        v
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
}
