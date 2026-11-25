//! The TPC-C initial database, on all cores, as Arrow `RecordBatch`es,
//! Parquet, or CSV.
//!
//! [`Tpcc`] generates all nine TPC-C tables for any number of warehouses,
//! following the population rules of TPC-C 5.11 clause 4.3.3.1 (NURand
//! last names, `ORIGINAL` data, the order / new-order / order-line
//! relationships). There are no reference bytes for TPC-C: every reference
//! loader (go-tpc, BenchBase, tpcc-mysql) seeds from the clock. The tests
//! check row counts, keys, ranges and distributions against the spec
//! instead.
//!
//! Each table is split into chunks of rows that are generated independently
//! (every random value is a function of its row), so output is the same
//! for any chunk size and thread count; chunks of several tables run on one
//! pool through [`populate_table`]'s drivers.
//!
//! ```
//! use populate_tpcc::{Tpcc, TpccTableName};
//!
//! let stock = Tpcc::new(2).table(TpccTableName::Stock);
//! let mut rows = 0;
//! stock.for_each_batch(0, |b| rows += b.num_rows());
//! assert_eq!(rows, 200_000);
//! ```

mod cols;
mod engine;
mod rng;
mod strings;
mod tables;
mod text;
pub mod tpcc;

pub use engine::{
    DEFAULT_BATCH_SIZE, DEFAULT_CHUNK_ROWS, for_each_batch_many, write_parquet_many,
    write_text_many,
};
pub use tpcc::{Tpcc, TpccTable, TpccTableName};
