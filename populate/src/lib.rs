//! Sampling combinators for columnar data generation.
//!
//! This is the facade crate: it re-exports the pieces that live in their own
//! crates so users only depend on `populate`.
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
//! | item        | crate                   | contents                                    |
//! |-------------|-------------------------|---------------------------------------------|
//! | crate root  | `populate-core`         | [`Distribution`], [`Columnar`], [`Sink`],   |
//! |             |                         | and the columns [`StrCol`] and [`ListCol`]  |
//! | [`dist`]    | `populate-builtins`     | `constant`, `range`, `map`, `list`, ...     |
//! | [`arrow`]   | `populate-sink-arrow`   | Arrow arrays and `RecordBatch` output       |
//! | [`parquet`] | `populate-sink-parquet` | Parquet encoder (`parquet` feature)         |
//! | [`table`]   | `populate-table`        | named, typed tables; parallel batches       |

pub use populate_builtins as dist;
pub use populate_core::{
    ChildCols, Children, Columnar, Columns, Date32, Decimal, Distribution, ListCol, SeekRng, Sink,
    StrBuf, StrCol, in_local,
};
#[cfg(feature = "arrow")]
pub use populate_sink_arrow as arrow;
#[cfg(feature = "parquet")]
pub use populate_sink_parquet as parquet;
#[cfg(feature = "table")]
pub use populate_table as table;

#[cfg(test)]
mod test;
