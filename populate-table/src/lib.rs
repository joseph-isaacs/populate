//! Declare a table once — named, typed columns of any width, each from a
//! populate distribution, a row function or other columns — and generate it
//! as Arrow `RecordBatch`es, sequentially or on all cores, deterministically.
//!
//! - [`Table`]: the builder and generator (see [`table`] for an example).
//! - [`DistTable`]: a table that is one distribution, its schema read
//!   off the distribution (names and column types), not declared;
//!   [`DynTable`] the same with the distribution's type erased.
//!   [`DistTable::raw`] exposes chunked native columns for custom backends;
//!   [`populate_core::DistChunks`] also works without this crate or Arrow.
//! - [`Stream`]: stateless per-row randomness, for values that several
//!   tables must agree on (a return and the sale it refers to).
//! - [`Chunks`]: any generator split into independent chunks gets the same
//!   multi-core drivers ([`chunks::for_each_batch`], and
//!   [`chunks::for_each_batch_many`] for several sources on one pool,
//!   biggest work first; both run on [`par_sources`]).
//! - [`text`]: the TPC `.dat` / `.tbl` text formats from Arrow batches,
//!   and a parallel writer for them.
//! - `write_parquet` / `Table::write_parquet` (feature `parquet`): parallel
//!   generation *and* encoding streamed into one Parquet file.
//! - [`write_dist_parquet`] (feature `parquet`): the same, straight from a
//!   distribution's collected buffers, with no Arrow in between.
//! - `cli` (feature `parquet`): the flags, files and report the
//!   generators' command-line tools share.

pub mod chunks;
pub mod dist;
pub mod sched;
pub mod stream;
pub mod table;
pub mod text;

#[cfg(feature = "parquet")]
pub mod cli;
#[cfg(feature = "parquet")]
pub mod direct;
#[cfg(feature = "parquet")]
mod parquet;

pub use chunks::{Chunks, Jobs, MultiChunks};
#[cfg(feature = "parquet")]
pub use direct::{ParquetOptions, write_dist_parquet};
pub use dist::{DEFAULT_BATCH_ROWS, DEFAULT_CHUNK_ROWS, DistTable, DistTables, DynTable};
#[cfg(feature = "parquet")]
pub use parquet::{write_parquet, write_parquet_many, write_parquet_many_in_batches};
pub use sched::{Schedule, par_map, par_ordered, par_sources, resolve_threads};
pub use stream::{Stream, fnv1a64, hash_str, mix64};
pub use table::{BatchCtx, DecimalRepr, Table};
