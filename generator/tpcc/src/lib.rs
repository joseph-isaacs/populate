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
//! pool through `populate_table`'s drivers.
//!
//! ```
//! # #[cfg(feature = "formats")] {
//! use populate_gen_tpcc::{Tpcc, TpccTableName};
//!
//! let stock = Tpcc::new(2).table(TpccTableName::Stock);
//! let mut rows = 0;
//! stock.for_each_batch(0, |b| rows += b.num_rows());
//! assert_eq!(rows, 200_000);
//! # }
//! ```

mod cols;
mod rng;
mod tables;
#[cfg(feature = "formats")]
mod text;
pub mod tpcc;

pub use tpcc::{DEFAULT_CHUNK_ROWS, Tpcc, TpccTableName};
#[cfg(feature = "formats")]
pub use tpcc::{TpccTable, for_each_batch_many, write_parquet_many, write_text_many};

/// Native TPC-C row types and string columns.
pub mod raw {
    pub use crate::cols::{Text, TextCol};
    pub use crate::tables::{
        Address, Customer, District, History, Item, Line, Micros, NewOrder, OrderLine, Orders,
        Stock, Warehouse,
    };
}
