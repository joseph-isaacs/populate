//! The core of `populate`: three small traits.
//!
//! - A [`Distribution`] is a stream of rows. It hands them out a block at
//!   a time, column by column, with
//!   [`collect_into`](Distribution::collect_into).
//! - [`Columnar`] says how many rows of a type are stored: which column
//!   buffers hold them.
//! - A [`Sink`] is something values are appended to. Every column buffer is
//!   one.
//!
//! | row type (`Columnar`)       | its columns                                   |
//! |-----------------------------|-----------------------------------------------|
//! | `i32`, `f64`, `bool`, ...   | `Vec<T>`                                      |
//! | `String`, `&'static str`    | [`StrCol`]: offsets and bytes, as Arrow `Utf8` |
//! | `Vec<T>`                    | [`ListCol`]: offsets and `T`'s columns        |
//! | [`Date32`], [`Decimal`], [`Timestamp`] | `Vec` of them: the storage values |
//! | `Option<T>`                 | [`Nullable`]: `T`'s columns and validity      |
//! | `[T; N]`                    | `Vec<[T; N]>`: `N` values per row, end to end |
//! | [`Spread<T, N>`](Spread)    | [`SpreadCols`]: `N` sets of `T`'s columns     |
//! | [`Children<T>`](Children)   | [`ChildCols`]: `T`'s columns, children end to end |
//! | [`Tables<T>`](Tables)       | [`TableCols`]: one [`ChildCols`] per table    |
//! | `(A, B, ...)`               | `(A`'s columns`, B`'s columns`, ...)`         |
//!
//! A tuple of distributions is a distribution of tuples (see [`tuple`]), so
//! a row of several columns is written as a tuple:
//!
//! ```
//! use populate_core::Distribution;
//! use rand::Rng;
//!
//! /// 0, 1, 2, ...
//! struct Counter(i64);
//!
//! impl Distribution for Counter {
//!     type Item = i64;
//!
//!     fn collect_into(&mut self, cols: &mut Vec<i64>, _rng: &mut impl Rng, n: usize) {
//!         cols.extend(self.0..self.0 + n as i64);
//!         self.0 += n as i64;
//!     }
//!
//!     fn skip(&mut self, n: u64) {
//!         self.0 += n as i64;
//!     }
//! }
//!
//! /// Fair coin flips.
//! struct Coin;
//!
//! impl Distribution for Coin {
//!     type Item = bool;
//!
//!     fn collect_into(&mut self, cols: &mut Vec<bool>, rng: &mut impl Rng, n: usize) {
//!         cols.extend((0..n).map(|_| rng.random::<bool>()));
//!     }
//!
//!     // flips don't depend on their position
//!     fn skip(&mut self, _n: u64) {}
//! }
//!
//! let mut rows = (Counter(0), Coin);
//! let (ids, flips): (Vec<i64>, Vec<bool>) = rows.collect(&mut rand::rng(), 4);
//! assert_eq!(ids, [0, 1, 2, 3]);
//! assert_eq!(flips.len(), 4);
//! ```

mod children;
mod columnar;
mod list;
mod local;
mod logical;
mod nullable;
mod rows;
mod seek;
mod sink;
mod spread;
pub mod string;
pub mod tuple;
mod wide;

pub use children::{ChildCols, Children, TableCols, Tables};
pub use columnar::{Columnar, Columns};
pub use list::ListCol;
pub use local::in_local;
pub use logical::{Date32, Decimal, Timestamp, time_unit};
pub use nullable::{Nullable, Validity};
use rand::Rng;
pub use rows::{Append, RowCount};
pub use seek::SeekRng;
pub use sink::Sink;
pub use spread::{Spread, SpreadCols};
pub use string::{Padded, StrBuf, StrCol, StrSink};
pub use wide::{Wide, WideCols};

/// A stream of rows of type [`Item`](Self::Item), produced a block at a
/// time and column by column: there is no way to ask for one row, so every
/// combinator works on whole columns.
///
/// A distribution is a stream, not a recipe: collecting moves it on, and
/// what comes next may depend on what came before (a counter, or random
/// bits left over from the last draw). Clone it to start again from the
/// same point, and [`skip`](Self::skip) to start further on.
pub trait Distribution: Sized {
    /// One row.
    type Item: Columnar;

    /// Append the next `n` rows to `cols`, column by column. A tuple fills
    /// its first column completely before it starts the second.
    fn collect_into(&mut self, cols: &mut Columns<Self::Item>, rng: &mut impl Rng, n: usize);

    /// Move past the next `n` rows without producing them, so the stream
    /// carries on from row `n`. This is how one chunk of a table starts at
    /// its own first row without generating the chunks before it: do
    /// nothing if rows don't depend on their position, add `n` to a
    /// counter, pass `n` on to an inner distribution.
    ///
    /// No caller RNG is involved. State that only holds randomness (bits
    /// left over from the last draw, say) can stay as it is.
    fn skip(&mut self, n: u64);

    /// Append the names of this distribution's columns to `out`, one per
    /// top-level column of [`Item`](Self::Item), if it names them.
    ///
    /// With the column types, which come from the
    /// [`Columns`](Columnar::Columns) the rows are collected into, this is
    /// a table's schema, read off the distribution without generating a
    /// row. Leaves name nothing; `populate_builtins::named` names what it
    /// wraps, and a tuple gives its fields' names in order.
    fn column_names(&self, _out: &mut Vec<String>) {}

    /// Empty columns for this distribution's rows. The default is
    /// `Default`; a distribution whose rows have a width chosen when it is
    /// built ([`Wide`]) returns columns of that width, so they and the
    /// schema read off them match the rows.
    fn columns(&self) -> Columns<Self::Item> {
        Columns::<Self::Item>::default()
    }

    /// The next `n` rows as new columns.
    fn collect(&mut self, rng: &mut impl Rng, n: usize) -> Columns<Self::Item> {
        let mut cols = self.columns();
        self.collect_into(&mut cols, rng, n);
        cols
    }
}

/// A distribution of `T` rows that can be cloned and shared between
/// threads, as every column of a table is: helpers that build a column
/// return `impl Dist<T>`.
pub trait Dist<T>: Distribution<Item = T> + Clone + Send + Sync + 'static {}

impl<T, D: Distribution<Item = T> + Clone + Send + Sync + 'static> Dist<T> for D {}
