//! Sampling combinators for columnar data generation.
//!
//! This is the facade crate: it re-exports the pieces that live in their own
//! crates so users only depend on `populate`.
//!
//! ```
//! # #[cfg(feature = "arrow")] {
//! use populate::arrow::{ArrowBackend, DistributionArrow};
//! use populate::dist::{constant, list, range};
//!
//! // id: i64, tags: list<i64> (every row has 3 tags)
//! let mut dist = (range(0i64, 1), list(constant(3usize), range(0i64, 1)));
//!
//! let batch = DistributionArrow::<ArrowBackend>::collect_record_batch(&mut dist, &mut rand::rng(), 1_000)
//!     .unwrap();
//! assert_eq!(batch.num_rows(), 1_000);
//! assert_eq!(batch.num_columns(), 2);
//! # }
//! ```
//!
//! | module      | crate                   | contents                                    |
//! |-------------|-------------------------|---------------------------------------------|
//! | [`traits`]  | `populate-core`         | `Distribution`, `SampleIter`, `Sink`, ...   |
//! | [`hlist`]   | `populate-core`         | HList sink plumbing                         |
//! | [`tuple`]   | `populate-core`         | tuple-of-distributions combinators          |
//! | [`dist`]    | `populate-builtins`     | `constant`, `range`, `map`, `list`, ...     |
//! | [`arrow`]   | `populate-sink-arrow`   | `ArrowBackend`, `RecordBatch` output        |
//! | [`parquet`] | `populate-sink-parquet` | zero-copy Parquet writer (`parquet` feature)|
//! | [`table`]   | `populate-table`        | named, typed tables; parallel batches       |

pub use populate_builtins as dist;
pub use populate_core::{hlist, traits, tuple};
#[cfg(feature = "arrow")]
pub use populate_sink_arrow as arrow;
#[cfg(feature = "parquet")]
pub use populate_sink_parquet as parquet;
#[cfg(feature = "table")]
pub use populate_table as table;

#[cfg(test)]
mod test;
