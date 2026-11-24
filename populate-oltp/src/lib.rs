//! OLTP benchmark data: the TPC-C initial database and the YCSB load, on
//! all cores, as Arrow `RecordBatch`es, Parquet, or text.
//!
//! - [`Tpcc`]: all nine TPC-C tables for any number of warehouses,
//!   following the population rules of TPC-C 5.11 clause 4.3.3.1 (NURand
//!   last names, `ORIGINAL` data, the order / new-order / order-line
//!   relationships). There are no reference bytes for TPC-C: every
//!   reference loader (go-tpc, BenchBase, tpcc-mysql) seeds from the clock.
//!   The tests check row counts, keys, ranges and distributions against
//!   the spec instead.
//! - [`Ycsb`]: the `usertable` of `ycsb load`, byte-identical to YCSB
//!   0.17.0: its keys always, its values with `dataintegrity=true`, and its
//!   random values given the loading thread's random seed.
//!
//! Each table is split into chunks of rows that are generated independently
//! (every random value is a function of its row), so output is the same
//! for any chunk size and thread count; chunks of several tables run on one
//! pool through [`populate_table`]'s drivers.
//!
//! ```
//! use populate_oltp::{Tpcc, TpccTableName};
//!
//! let stock = Tpcc::new(2).table(TpccTableName::Stock);
//! let mut rows = 0;
//! stock.for_each_batch(0, |b| rows += b.num_rows());
//! assert_eq!(rows, 200_000);
//! ```

mod engine;
mod rng;
mod strings;
mod text;
pub mod tpcc;
pub mod ycsb;

pub use engine::{
    DEFAULT_BATCH_SIZE, DEFAULT_CHUNK_ROWS, for_each_batch_many, write_parquet_many,
    write_text_many,
};
pub use rng::JavaTlr;
pub use tpcc::{Tpcc, TpccTable, TpccTableName};
pub use ycsb::Ycsb;
