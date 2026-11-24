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

mod columnar;
mod list;
mod local;
mod sink;
pub mod string;
pub mod tuple;

pub use columnar::{Columnar, Columns};
pub use list::ListCol;
pub use local::in_local;
use rand::Rng;
pub use sink::Sink;
pub use string::{StrBuf, StrCol};

/// A stream of rows of type [`Item`](Self::Item).
///
/// Only [`sample`](Self::sample) has to be written. A distribution that can
/// fill a whole column faster than one value at a time (most leaves can)
/// also overrides [`collect_into`](Self::collect_into).
///
/// A distribution is a stream, not a recipe: sampling moves it on, and what
/// comes next may depend on what came before (a counter, or random bits left
/// over from the last draw). Clone it to start again from the same point.
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

    /// The next `n` rows as new columns.
    fn collect(&mut self, rng: &mut impl Rng, n: usize) -> Columns<Self::Item> {
        let mut cols = Columns::<Self::Item>::default();
        self.collect_into(&mut cols, rng, n);
        cols
    }
}
