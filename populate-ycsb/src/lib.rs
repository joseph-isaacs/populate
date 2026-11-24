//! YCSB benchmark data: the YCSB load and the YCSB run's operations, on all
//! cores, as Arrow `RecordBatch`es, Parquet, or TSV.
//!
//! - [`Ycsb`]: the `usertable` of `ycsb load`, byte-identical to YCSB
//!   0.17.0: its keys always, its values with `dataintegrity=true`, and its
//!   random values given the loading thread's random seed.
//! - [`YcsbRun`]: the operations of `ycsb run` (workloads A to F, or any
//!   proportions and request distribution), byte-identical to YCSB 0.17.0
//!   given the client thread's random seed.
//!
//! Each table is split into chunks of rows that are generated independently
//! (every random value is a function of its row), so output is the same
//! for any chunk size and thread count; chunks run on one pool through
//! [`populate_table`]'s drivers.
//!
//! ```
//! use populate_ycsb::Ycsb;
//!
//! let load = Ycsb::new(1000).data_integrity(true);
//! let mut rows = 0;
//! load.for_each_batch(0, |b| rows += b.num_rows());
//! assert_eq!(rows, 1000);
//! ```

mod engine;
mod rng;
mod strings;
pub mod ycsb;
mod ycsb_run;

pub use engine::{
    DEFAULT_BATCH_SIZE, DEFAULT_CHUNK_ROWS, for_each_batch_many, write_parquet_many,
    write_text_many,
};
pub use rng::JavaTlr;
pub use ycsb::Ycsb;
pub use ycsb_run::{RequestDistribution, YcsbRun};
