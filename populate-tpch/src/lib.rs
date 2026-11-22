//! TPC-H data, byte-identical to dbgen and tpchgen, on all cores.
//!
//! All eight tables, every column, at any scale factor up to 10,000, as
//! Arrow `RecordBatch`es, Parquet, or dbgen's `.tbl` text. The values are
//! dbgen's (as ported by [`tpchgen`], whose distributions and grammar this
//! crate uses); the engine is this crate's:
//!
//! - every table is split into small, independent ranges of *source rows*
//!   (orders for `lineitem`, parts for `partsupp`, rows elsewhere). dbgen's
//!   streams reserve a fixed number of draws per row, so each stream jumps
//!   straight to any row (`16807^k mod (2^31 - 1)`), and the chunks
//!   concatenate to the sequential output for any chunk size and thread
//!   count;
//! - columns are generated directly: each stream's draws for a whole block
//!   of rows at once, by independent LCG lanes that vectorise;
//! - text is never copied: comments are `Utf8View`s into the shared text
//!   pool and distribution values views into a shared dictionary;
//! - chunks of every table run on one pool of threads through
//!   [`populate_table`]'s ordered drivers, Parquet row groups are encoded
//!   on the workers, and `.tbl` text is formatted and written by them.
//!
//! ```
//! use populate_tpch::{TableName, Tpch};
//!
//! let h = Tpch::new(0.01);
//! let nation = h.table(TableName::Nation);
//! assert_eq!(nation.source_rows(), 25);
//!
//! // dbgen's `nation.tbl`, byte for byte
//! let tbl = nation.write_tbl(Vec::new(), 0).unwrap();
//! assert!(tbl.starts_with(b"0|ALGERIA|0| haggle. carefully final deposits"));
//!
//! // all cores, batches in order
//! let mut rows = 0;
//! h.table(TableName::LineItem).for_each_batch(0, |b| rows += b.num_rows());
//! assert_eq!(rows, 60_175);
//! ```

mod engine;
mod rng;
mod tables;
mod text;

pub use engine::{
    DEFAULT_BATCH_SIZE, DEFAULT_CHUNK_ROWS, TpchTable, for_each_batch_many, parquet_props,
    write_parquet_many, write_tbl_many,
};

use std::fmt;
use std::str::FromStr;

/// Every TPC-H table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum TableName {
    Nation,
    Region,
    Part,
    Supplier,
    PartSupp,
    Customer,
    Orders,
    LineItem,
}

impl TableName {
    /// All tables, in dbgen's order.
    pub const ALL: [TableName; 8] = {
        use TableName::*;
        [
            Nation, Region, Part, Supplier, PartSupp, Customer, Orders, LineItem,
        ]
    };

    /// SQL name, e.g. `lineitem`.
    pub fn as_str(self) -> &'static str {
        use TableName::*;
        match self {
            Nation => "nation",
            Region => "region",
            Part => "part",
            Supplier => "supplier",
            PartSupp => "partsupp",
            Customer => "customer",
            Orders => "orders",
            LineItem => "lineitem",
        }
    }
}

impl fmt::Display for TableName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for TableName {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        TableName::ALL
            .into_iter()
            .find(|t| t.as_str() == s)
            .ok_or_else(|| format!("unknown TPC-H table `{s}`"))
    }
}

/// The largest supported scale factor. Above ~10,737 dbgen's part keys
/// overflow 32 bits, and from 30,000 it switches to a 64-bit generator
/// whose partitioned output differs from its sequential output.
pub const MAX_SCALE_FACTOR: f64 = 10_000.0;

/// A TPC-H data set at one scale factor.
#[derive(Debug, Clone, Copy)]
pub struct Tpch {
    scale: tables::Scale,
}

impl Tpch {
    /// Scale factor `sf` (1 = about 1 GB of `.tbl` text), in
    /// `(0, MAX_SCALE_FACTOR]`. Fractional scale factors scale every table
    /// but `nation` and `region`, as in dbgen.
    pub fn new(sf: f64) -> Self {
        assert!(
            sf > 0.0 && sf <= MAX_SCALE_FACTOR,
            "scale factor must be in (0, {MAX_SCALE_FACTOR}]"
        );
        Tpch {
            scale: tables::Scale::new(sf),
        }
    }

    pub fn sf(&self) -> f64 {
        self.scale.sf
    }

    /// Source rows of `t`: orders for `lineitem`, parts for `partsupp`, the
    /// row count elsewhere.
    pub fn source_rows(&self, t: TableName) -> u64 {
        let s = &self.scale;
        match t {
            TableName::Nation => 25,
            TableName::Region => 5,
            TableName::Part | TableName::PartSupp => s.parts as u64,
            TableName::Supplier => s.suppliers as u64,
            TableName::Customer => s.customers as u64,
            TableName::Orders | TableName::LineItem => s.orders as u64,
        }
    }

    /// Table `t`, ready to generate.
    pub fn table(&self, t: TableName) -> TpchTable {
        TpchTable::new(t, self.scale, self.source_rows(t))
    }

    /// Build the text pool every table's comments are cut from (300 MiB,
    /// ~0.4 s on 4 cores), on `threads` threads (0 = all cores). It is
    /// built once per process, on first use otherwise.
    pub fn prepare(threads: usize) {
        text::Text::init(threads);
    }

    /// Every table, in [`TableName::ALL`] order.
    pub fn tables(&self) -> impl Iterator<Item = TpchTable> + '_ {
        TableName::ALL.into_iter().map(|t| self.table(t))
    }
}
