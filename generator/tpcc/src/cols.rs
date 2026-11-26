//! The leaves TPC-C's tables are made of, and its string column type.
//!
//! Every TPC-C value is a function of its row number (each column has its
//! own stateless stream, [`Draws`](crate::rng::Draws)), so a leaf is a row
//! counter and a function: [`per_row`] for values, [`text`] for strings
//! written straight into their column. Skipping is moving the counter.

use populate_combinators::views::{Str, Unshared, Views};
use populate_core::{Columnar, Distribution, Sink, StrBuf, StrSink};
use rand::Rng;

/// A string (ASCII): every TPC-C string is written per row.
pub(crate) type Text = Str<Unshared>;

/// A `Utf8View` column of [`Text`].
pub(crate) type TextCol = Views<Unshared>;

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

/// A string column: `f(r, out)` writes row `r`'s string straight into
/// the column. `avg` is the bytes per row to reserve: the average length of
/// strings over 12 bytes, 0 for shorter ones (they live in their view).
pub(crate) fn text<F: Fn(u64, &mut StrBuf<'_>)>(avg: usize, f: F) -> TextRows<F> {
    TextRows { next: 0, avg, f }
}

impl<F: Fn(u64, &mut StrBuf<'_>)> Distribution for TextRows<F> {
    type Item = Text;

    fn collect_into(&mut self, cols: &mut TextCol, _rng: &mut impl Rng, n: usize) {
        cols.reserve_bytes(n, n * self.avg);
        for r in self.next..self.next + n as u64 {
            cols.push_with(|out| (self.f)(r, out));
        }
        self.next += n as u64;
    }

    fn skip(&mut self, n: u64) {
        self.next += n;
    }
}
