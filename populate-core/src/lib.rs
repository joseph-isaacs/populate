//! The core of `populate`: three small traits.
//!
//! - A [`Distribution`] is a stream of rows. It hands them out one at a time
//!   with [`sample`](Distribution::sample), or many at once, column by
//!   column, with [`collect_into`](Distribution::collect_into).
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
//! | `(A, B, ...)`               | `(A`'s columns`, B`'s columns`, ...)`         |
//!
//! A tuple of distributions is a distribution of tuples (see [`tuple`]), so
//! a row of several columns is written as a tuple:
//!
//! ```
//! use populate_core::{Distribution, Sink};
//! use rand::Rng;
//!
//! /// 0, 1, 2, ...
//! struct Counter(i64);
//!
//! impl Distribution for Counter {
//!     type Item = i64;
//!
//!     fn sample(&mut self, _rng: &mut impl Rng) -> i64 {
//!         self.0 += 1;
//!         self.0 - 1
//!     }
//! }
//!
//! /// Fair coin flips.
//! struct Coin;
//!
//! impl Distribution for Coin {
//!     type Item = bool;
//!
//!     fn sample(&mut self, rng: &mut impl Rng) -> bool {
//!         rng.random()
//!     }
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
mod seek;
mod sink;
mod spread;
pub mod string;
pub mod tuple;
mod wide;

pub use children::{ChildCols, Children};
pub use columnar::{Columnar, Columns};
pub use list::ListCol;
pub use local::in_local;
pub use logical::{Date32, Decimal, Timestamp, time_unit};
pub use nullable::{Nullable, Validity};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
pub use seek::SeekRng;
pub use sink::Sink;
pub use spread::{Spread, SpreadCols};
pub use string::{StrBuf, StrCol, StrSink};
pub use wide::{Wide, WideCols};

/// A stream of rows of type [`Item`](Self::Item).
///
/// Only [`sample`](Self::sample) has to be written. A distribution that can
/// fill a whole column faster than one value at a time (most leaves can)
/// also overrides [`collect_into`](Self::collect_into).
///
/// A distribution is a stream, not a recipe: sampling moves it on, and what
/// comes next may depend on what came before (a counter, or random bits left
/// over from the last draw). Clone it to start again from the same point,
/// and [`skip`](Self::skip) to start further on.
pub trait Distribution: Sized {
    /// One row.
    type Item: Columnar;

    /// The next row.
    fn sample(&mut self, rng: &mut impl Rng) -> Self::Item;

    /// Append the next `n` rows to `cols`, column by column.
    ///
    /// The rows are as valid as `n` calls of [`sample`](Self::sample), but
    /// need not be the same ones: a tuple fills its first column completely
    /// before it starts the second, so it draws from `rng` in another order.
    fn collect_into(&mut self, cols: &mut Columns<Self::Item>, rng: &mut impl Rng, n: usize) {
        cols.reserve(n);
        // `push_n` over a `Range`-driven iterator: a `Vec` turns this into
        // `extend`, which knows the exact length (no per-value capacity
        // check). `in_local` keeps the distribution's state in registers.
        in_local(self, |d| cols.push_n((0..n).map(|_| d.sample(rng))));
    }

    /// Move past the next `n` rows without producing them, so the stream
    /// carries on from row `n`. This is how one chunk of a table starts at
    /// its own first row without generating the chunks before it.
    ///
    /// The default samples `n` rows and drops them, which takes time
    /// proportional to `n`. Override it wherever the state after `n` rows
    /// can be computed directly: do nothing if rows don't depend on their
    /// position, add `n` to a counter, pass `n` on to an inner distribution.
    ///
    /// No caller RNG is involved. State that only holds randomness (bits
    /// left over from the last draw, say) can stay as it is. The default
    /// draws from a throwaway RNG, so it lands exactly on row `n` only for
    /// state that doesn't depend on the values drawn.
    fn skip(&mut self, n: u64) {
        let mut rng = SmallRng::seed_from_u64(0);
        in_local(self, |d| {
            for _ in 0..n {
                d.sample(&mut rng);
            }
        });
    }

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
