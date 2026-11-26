//! The leaves TPC-C's tables are made of, and its string column type.
//!
//! Every TPC-C value is a function of its row number (each column has its
//! own stateless stream, [`Draws`](crate::rng::Draws)), so a leaf derives
//! from [`row_numbers`]: `derive(row_numbers(), f)` for values, [`text`]
//! for strings written straight into their column. Skipping is moving the
//! counter.

use populate_builtins::{Col, Rows, derive_columns, row_numbers};
use populate_combinators::views::{Str, Unshared, Views};
use populate_core::{StrBuf, StrSink};

/// A string (ASCII): every TPC-C string is written per row.
pub(crate) type Text = Str<Unshared>;

/// A `Utf8View` column of [`Text`].
pub(crate) type TextCol = Views<Unshared>;

/// A string column: `f(r, out)` writes row `r`'s string straight into
/// the column. `avg` is the bytes per row to reserve: the average length of
/// strings over 12 bytes, 0 for shorter ones (they live in their view).
pub(crate) fn text<F>(avg: usize, f: F) -> impl Col<Text>
where
    F: Fn(u64, &mut StrBuf<'_>) + Clone + Send + Sync + 'static,
{
    derive_columns(row_numbers(), move |rows: &Rows, col: &mut TextCol| {
        col.reserve_bytes(rows.len(), rows.len() * avg);
        for r in rows.iter() {
            col.push_with(|out| f(r, out));
        }
    })
}
