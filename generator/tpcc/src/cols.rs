//! The leaves TPC-C's tables are made of, and its string column type.
//!
//! Every TPC-C value is a function of its row number (each column has its
//! own stateless stream, [`Draws`](crate::rng::Draws)), so a leaf is a row
//! counter and a function: [`per_row`] for values, [`text`] for strings
//! written straight into their column. Skipping is moving the counter.

use crate::strings::Views;
use arrow_array::ArrayRef;
use arrow_schema::DataType;
use populate_core::RowCount;
use populate_core::{Columnar, Distribution, Sink};
use populate_sink_arrow::IntoArrowArray;
use rand::Rng;

/// A string (ASCII): a row of a [`TextCol`].
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct Text(pub(crate) Vec<u8>);

/// A `Utf8View` column of [`Text`].
#[derive(Default)]
pub(crate) struct TextCol(pub(crate) Views);

impl Columnar for Text {
    type Columns = TextCol;
}

impl Sink<Text> for TextCol {
    fn push(&mut self, t: Text) {
        self.0.push(&t.0);
    }

    fn reserve(&mut self, n: usize) {
        self.0.reserve(n, 0);
    }
}

impl RowCount for TextCol {
    fn rows(&self) -> usize {
        self.0.len()
    }
}

impl IntoArrowArray for TextCol {
    fn data_type() -> DataType {
        DataType::Utf8View
    }

    fn into_array(self) -> ArrayRef {
        self.0.finish()
    }
}

/// `f(row)` for every row. See [`per_row`].
#[derive(Clone)]
pub(crate) struct PerRow<F> {
    next: u64,
    f: F,
}

/// A column whose value at row `r` is `f(r)`.
pub(crate) fn per_row<T, F: Fn(u64) -> T>(f: F) -> PerRow<F> {
    PerRow { next: 0, f }
}

impl<T: Columnar, F: Fn(u64) -> T> Distribution for PerRow<F> {
    type Item = T;

    fn collect_into(&mut self, cols: &mut T::Columns, _rng: &mut impl Rng, n: usize) {
        let first = self.next;
        self.next += n as u64;
        cols.reserve(n);
        cols.push_n((first..self.next).map(&self.f));
    }

    fn skip(&mut self, n: u64) {
        self.next += n;
    }
}

/// A string per row written by `f(row, column)`. See [`text`].
#[derive(Clone)]
pub(crate) struct TextRows<F> {
    next: u64,
    /// Bytes to reserve per row (0 for strings that fit in their view).
    avg: usize,
    f: F,
}

/// A string column: `f(r, col)` appends row `r`'s string to `col`
/// (with [`Views::push_with`] or [`Views::push`]), straight into the
/// column. `avg` is the bytes per row to reserve: the average length of
/// strings over 12 bytes, 0 for shorter ones (they live in their view).
pub(crate) fn text<F: Fn(u64, &mut Views)>(avg: usize, f: F) -> TextRows<F> {
    TextRows { next: 0, avg, f }
}

impl<F: Fn(u64, &mut Views)> Distribution for TextRows<F> {
    type Item = Text;

    fn collect_into(&mut self, cols: &mut TextCol, _rng: &mut impl Rng, n: usize) {
        cols.0.reserve(n, n * self.avg);
        for r in self.next..self.next + n as u64 {
            (self.f)(r, &mut cols.0);
        }
        self.next += n as u64;
    }

    fn skip(&mut self, n: u64) {
        self.next += n;
    }
}
