//! Star Schema Benchmark (SSB) data, byte-identical to `ssb-dbgen`, on all
//! cores.
//!
//! The five SSB tables (`part`, `supplier`, `customer`, `date`,
//! `lineorder`), plus the denormalised `lineorder_flat`, as Arrow
//! `RecordBatch`es, Parquet, or ssb-dbgen's `.tbl` text. The `.tbl` files
//! are the same bytes `dbgen -s <sf>` (eyalroz/ssb-dbgen, default build)
//! writes; this crate is a clean reimplementation of its behaviour, on the
//! dbgen random streams `populate-tpch` uses.
//!
//! - Every table is split into small, independent ranges of source rows
//!   (orders for `lineorder`). dbgen's streams reserve a fixed number of
//!   draws per row, so each stream jumps straight to any row, and the
//!   chunks concatenate to the sequential output for any chunk size and
//!   thread count.
//! - Columns are generated directly: each stream's draws for a whole block
//!   of rows at once, by independent LCG lanes that vectorise.
//! - Text that is not unique per row (cities, nations, part names, brands,
//!   distribution values, the date table's names) is never copied: it is a
//!   `Utf8View` into one shared dictionary.
//!
//! ```
//! use populate_ssb::{Ssb, TableName};
//!
//! let s = Ssb::new(0.01);
//! let date = s.table(TableName::Date);
//! let tbl = date.write_tbl(Vec::new(), 0).unwrap();
//! assert!(tbl.starts_with(b"19920101|January 1, 1992|Thursday|January|1992|"));
//!
//! let mut rows = 0;
//! s.table(TableName::LineOrder).for_each_batch(0, |b| rows += b.num_rows());
//! assert_eq!(rows, 60_176);
//! ```
//!
//! The sequential run generates the tables in the order part, supplier,
//! customer, date, lineorder, and some later tables' streams start where
//! the earlier ones left them (see `tables.rs`); this crate reproduces the
//! default run of all five tables. (`dbgen -T c` alone, say, gives other
//! customer cities and phone numbers.)

mod cols;
mod dict;
mod engine;
mod flat;
mod tables;

pub use engine::{
    DEFAULT_BATCH_SIZE, DEFAULT_CHUNK_ROWS, SsbTable, for_each_batch_many, parquet_props,
    write_parquet_many, write_tbl_many,
};

use std::fmt;
use std::str::FromStr;
use std::sync::{Arc, OnceLock};
use tables::TableGen;

/// Every SSB table, and `lineorder_flat`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum TableName {
    Part,
    Supplier,
    Customer,
    Date,
    LineOrder,
    /// `lineorder` joined with `customer`, `supplier` and `part` (SSB-flat).
    /// Not made by ssb-dbgen. The dimension tables are built once per
    /// [`Ssb`] and kept; each batch's name, address and phone columns are
    /// views into (and keep alive) the dimension's whole buffer, which
    /// matters only when sending the Arrow batches on (IPC, FFI) rather
    /// than writing Parquet or `.tbl`.
    LineOrderFlat,
}

impl TableName {
    /// The five SSB tables, in ssb-dbgen's order (not `lineorder_flat`).
    pub const ALL: [TableName; 5] = [
        TableName::Part,
        TableName::Supplier,
        TableName::Customer,
        TableName::Date,
        TableName::LineOrder,
    ];

    /// SQL name, e.g. `lineorder`.
    pub fn as_str(self) -> &'static str {
        match self {
            TableName::Part => "part",
            TableName::Supplier => "supplier",
            TableName::Customer => "customer",
            TableName::Date => "date",
            TableName::LineOrder => "lineorder",
            TableName::LineOrderFlat => "lineorder_flat",
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
            .chain([TableName::LineOrderFlat])
            .find(|t| t.as_str() == s)
            .ok_or_else(|| format!("unknown SSB table `{s}`"))
    }
}

/// The largest supported scale factor: customer names print keys in 9
/// digits (ssb-dbgen's `%09ld`), which holds below 10^9 customers (SF
/// 33,333). Every 32-bit column fits well past it. (ssb-dbgen itself warns
/// above SF 1000.)
pub const MAX_SCALE_FACTOR: f64 = 30_000.0;

/// An SSB data set at one scale factor.
#[derive(Debug, Clone)]
pub struct Ssb {
    scale: tables::Scale,
    /// `lineorder_flat`'s distribution, built (with the dimension tables)
    /// on first use.
    flat: Arc<OnceLock<Arc<dyn TableGen>>>,
}

impl Ssb {
    /// Scale factor `sf`, as ssb-dbgen's `-s`: below 1 every table is
    /// scaled by `sf` (the date table too); from 1 the scale is `sf`
    /// truncated to an integer. From SF 1 `part` has
    /// `200,000 * floor(1 + log2(sf))` rows (below, `200,000 * sf`).
    pub fn new(sf: f64) -> Self {
        assert!(
            sf > 0.0 && sf <= MAX_SCALE_FACTOR,
            "scale factor must be in (0, {MAX_SCALE_FACTOR}]"
        );
        Ssb {
            scale: tables::Scale::new(sf),
            flat: Arc::default(),
        }
    }

    pub fn sf(&self) -> f64 {
        self.scale.sf
    }

    /// Source rows of `t`: orders for the line orders, the row count
    /// elsewhere.
    pub fn source_rows(&self, t: TableName) -> u64 {
        let s = &self.scale;
        match t {
            TableName::Part => s.parts,
            TableName::Supplier => s.suppliers,
            TableName::Customer => s.customers,
            TableName::Date => s.dates,
            TableName::LineOrder | TableName::LineOrderFlat => s.orders,
        }
    }

    /// Table `t`, ready to generate.
    pub fn table(&self, t: TableName) -> SsbTable {
        SsbTable::new(t, self.scale, self.source_rows(t), self.flat.clone())
    }

    /// The five SSB tables, in [`TableName::ALL`] order.
    pub fn tables(&self) -> impl Iterator<Item = SsbTable> + '_ {
        TableName::ALL.into_iter().map(|t| self.table(t))
    }
}
