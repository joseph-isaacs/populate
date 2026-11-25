//! Declare a table once — named, typed columns of any width, each from a
//! populate distribution, a row function or other columns — and generate it
//! as Arrow `RecordBatch`es, sequentially or on all cores, deterministically.
//!
//! - [`Table`]: the builder and generator (see [`table`] for an example).
//! - [`Stream`]: stateless per-row randomness, for values that several
//!   tables must agree on (a return and the sale it refers to).
//! - [`Chunks`]: any generator split into independent chunks gets the same
//!   multi-core drivers ([`chunks::for_each_batch`], [`par_ordered`], and
//!   [`par_sources`] for several sources on one pool, biggest work first).
//! - [`text`]: the TPC `.dat` / `.tbl` text formats from Arrow batches,
//!   and a parallel writer for them.
//! - `write_parquet` / `Table::write_parquet` (feature `parquet`): parallel
//!   generation *and* encoding streamed into one Parquet file.

pub mod chunks;
pub mod sched;
pub mod stream;
pub mod table;
pub mod text;

#[cfg(feature = "parquet")]
mod parquet;
#[cfg(feature = "parquet")]
mod parquet_direct;

pub use chunks::{Chunks, Jobs, MultiChunks};
#[cfg(feature = "parquet")]
pub use parquet::{write_parquet, write_parquet_many};
pub use sched::{Schedule, par_ordered, par_sources};
pub use stream::{Stream, hash_str, mix64};
pub use table::{BatchCtx, DecimalRepr, Table};
