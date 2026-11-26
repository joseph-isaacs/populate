#![warn(missing_docs)]
//! Generate tabular test data column by column, straight into Arrow
//! (and optionally Parquet) buffers.
//!
//! This is the facade crate: it re-exports the pieces that live in their own
//! crates so users only depend on `populate`. A row is described by
//! combining distributions; collecting writes each column into the buffer
//! it will be stored in, and the sinks move those buffers into Arrow arrays
//! or encode them as Parquet without copying.
//!
//! ```
//! # #[cfg(feature = "arrow")] {
//! use populate::arrow::DistributionArrow;
//! use populate::dist::{constant, list, range};
//!
//! // id: i64, tags: list<i64> (every row has 3 tags)
//! let mut dist = (range(0i64, 1), list(constant(3usize), range(0i64, 1)));
//!
//! let batch = dist.collect_record_batch(&mut rand::rng(), 1_000).unwrap();
//! assert_eq!(batch.num_rows(), 1_000);
//! assert_eq!(batch.num_columns(), 2);
//! # }
//! ```
//!
//! # Where to start
//!
//! - [`Distribution`] is the one trait everything implements: what a
//!   stream of rows must do, and what its output is deterministic in.
//! - The [crate-root items](#reexports) are the row types and the columns
//!   that hold them, with a table of which row type is stored how.
//! - [`dist`] has the built-in leaves and combinators, and a table of which
//!   one to use.
//! - [`arrow`] turns columns into Arrow arrays and reads a schema off a
//!   distribution; [`parquet`] writes them as Parquet.
//! - [`table`] runs a distribution as a table of any size, in batches on
//!   all cores, with the same output as one thread.
//!
//! `DESIGN.md` in this crate describes how the pieces fit together.
//!
//! # Crates and features
//!
//! | item        | crate                   | feature              | contents                                   |
//! |-------------|-------------------------|----------------------|--------------------------------------------|
//! | crate root  | `populate-core`         | always               | [`Distribution`], [`Columnar`], [`Sink`], row types and columns |
//! | [`dist`]    | `populate-builtins`     | always               | `constant`, `range`, `list`, `derive`, `flat_map`, random leaves |
//! | [`arrow`]   | `populate-sink-arrow`   | `arrow` (default)    | Arrow arrays, `RecordBatch`es, schemas     |
//! | [`parquet`] | `populate-sink-parquet` | `parquet`            | Parquet straight from the columns          |
//! | [`table`]   | `populate-table`        | `table` (default)    | tables, parallel batches, Parquet and text files |
//!
//! `parquet` turns on `arrow`, and with `table` it also turns on the
//! table writers' Parquet output.

pub use populate_builtins as dist;
pub use populate_core::{
    Append, ChildCols, Children, Columnar, Columns, Date32, Decimal, Dist, Distribution, ListCol,
    Nullable, Padded, RowCount, SeekRng, Sink, Spread, SpreadCols, StrBuf, StrCol, StrSink,
    TableCols, Tables, Timestamp, Validity, Wide, WideCols, in_local, string, time_unit, tuple,
};
#[cfg(feature = "arrow")]
pub use populate_sink_arrow as arrow;
#[cfg(feature = "parquet")]
pub use populate_sink_parquet as parquet;
#[cfg(feature = "table")]
pub use populate_table as table;

#[cfg(test)]
mod test;

/// The book's chapters (`book/` at the repository root), so their examples
/// are compiled and run as doctests.
#[cfg(all(doctest, feature = "table"))]
mod book {
    macro_rules! chapters {
        ($($name:ident = $file:literal),+ $(,)?) => {
            $(#[doc = include_str!(concat!("../../book/src/", $file))]
            mod $name {})+
        };
    }

    chapters!(
        model = "model.md",
        first_table = "first-table.md",
        row_writers = "row-writers.md",
        child_tables = "child-tables.md",
        leaf = "leaf.md",
    );
}
