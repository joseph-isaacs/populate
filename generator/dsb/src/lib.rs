//! DSB data (Microsoft's skewed, correlated TPC-DS), byte-identical to
//! DSB's `dsdgen`, on all cores.
//!
//! The values come from [`dsbgen`]: tpcdsgen (the Apache-2.0 Rust port of
//! TPC-DS dsdgen from the tpcgen-rs project) with DSB's changes to dsdgen
//! ported in: exponentially skewed, permuted values and keys, correlated
//! attributes, skewed sales dates and joint item/customer draws. This
//! crate adds the parallel engine:
//!
//! - every table is one populate
//!   [`Distribution`](populate_core::Distribution) over dsbgen's row
//!   generator (a sales table and its returns, one distribution of both),
//!   and its schema is read off the distribution's column types;
//! - every table is split into small, independent ranges of *source rows*
//!   (a ticket, an order, a customer). dsbgen's random streams skip
//!   exactly to any row, so the chunks concatenate to the sequential output
//!   for any chunk size and thread count;
//! - chunks run on all cores through [`populate_table`]'s ordered drivers,
//!   whatever the scale factor;
//! - Parquet row groups are generated *and* encoded on the worker threads;
//! - `.dat` output is the exact dsdgen text format, for diffing.
//!
//! ```
//! use populate_gen_dsb::{Encoding, TableName, Dsb};
//!
//! let ds = Dsb::new(1.0);
//! let ship_mode = ds.table(TableName::ShipMode);
//! assert_eq!(ship_mode.source_rows(), 20);
//!
//! // all cores, batches in order
//! let mut rows = 0;
//! ship_mode.for_each_batch(0, |batch| rows += batch.num_rows());
//! assert_eq!(rows, 20);
//!
//! // dsdgen's `ship_mode.dat`, byte for byte
//! let dat = ship_mode.write_dat(Vec::new(), 0, Encoding::Utf8).unwrap();
//! assert!(dat.starts_with(b"1|AAAAAAAABAAAAAAA|"));
//! ```

pub mod dat;
mod dist;
mod gen_tables;

pub use dat::Encoding;
pub use gen_tables::{
    DEFAULT_BATCH_SIZE, DsTable, for_each_batch_many, parquet_props, write_dat_many,
    write_parquet_many,
};

use dsbgen::config::{Session, SessionBuilder};
use std::fmt;
use std::str::FromStr;

/// Every DSB table (TPC-DS's 24).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum TableName {
    CallCenter,
    CatalogPage,
    CatalogReturns,
    CatalogSales,
    Customer,
    CustomerAddress,
    CustomerDemographics,
    DateDim,
    HouseholdDemographics,
    IncomeBand,
    Inventory,
    Item,
    Promotion,
    Reason,
    ShipMode,
    Store,
    StoreReturns,
    StoreSales,
    TimeDim,
    Warehouse,
    WebPage,
    WebReturns,
    WebSales,
    WebSite,
}

impl TableName {
    pub const ALL: [TableName; 24] = {
        use TableName::*;
        [
            CallCenter,
            CatalogPage,
            CatalogReturns,
            CatalogSales,
            Customer,
            CustomerAddress,
            CustomerDemographics,
            DateDim,
            HouseholdDemographics,
            IncomeBand,
            Inventory,
            Item,
            Promotion,
            Reason,
            ShipMode,
            Store,
            StoreReturns,
            StoreSales,
            TimeDim,
            Warehouse,
            WebPage,
            WebReturns,
            WebSales,
            WebSite,
        ]
    };

    /// SQL name, e.g. `store_sales`.
    pub fn as_str(self) -> &'static str {
        use TableName::*;
        match self {
            CallCenter => "call_center",
            CatalogPage => "catalog_page",
            CatalogReturns => "catalog_returns",
            CatalogSales => "catalog_sales",
            Customer => "customer",
            CustomerAddress => "customer_address",
            CustomerDemographics => "customer_demographics",
            DateDim => "date_dim",
            HouseholdDemographics => "household_demographics",
            IncomeBand => "income_band",
            Inventory => "inventory",
            Item => "item",
            Promotion => "promotion",
            Reason => "reason",
            ShipMode => "ship_mode",
            Store => "store",
            StoreReturns => "store_returns",
            StoreSales => "store_sales",
            TimeDim => "time_dim",
            Warehouse => "warehouse",
            WebPage => "web_page",
            WebReturns => "web_returns",
            WebSales => "web_sales",
            WebSite => "web_site",
        }
    }

    /// Fact (sales, returns, inventory) rather than dimension table.
    pub fn is_fact(self) -> bool {
        use TableName::*;
        matches!(
            self,
            CatalogReturns
                | CatalogSales
                | Inventory
                | StoreReturns
                | StoreSales
                | WebReturns
                | WebSales
        )
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
            .ok_or_else(|| format!("unknown DSB table `{s}`"))
    }
}

/// A DSB data set at one scale factor.
#[derive(Debug, Clone)]
pub struct Dsb {
    sf: f64,
    session: Session,
}

impl Dsb {
    /// Scale factor `sf` (1 = about 1 GB of `.dat` text): a whole number,
    /// at least 1, as DSB's dsdgen takes.
    pub fn new(sf: f64) -> Self {
        assert!(
            sf >= 1.0 && sf.fract() == 0.0,
            "scale factor must be a whole number, at least 1"
        );
        let session = SessionBuilder::new()
            .with_scale_factor(sf)
            .build()
            .expect("valid DSB session");
        Dsb { sf, session }
    }

    pub fn sf(&self) -> f64 {
        self.sf
    }

    /// Source rows of `t` (tickets for `store_sales`, orders for
    /// `catalog_sales`, ...; equal to the row count for other tables).
    pub fn source_rows(&self, t: TableName) -> u64 {
        self.session.get_scaling().get_row_count(t.ds())
    }

    /// Table `t`, ready to generate.
    pub fn table(&self, t: TableName) -> DsTable {
        DsTable::new(t, self.session.clone())
    }

    /// Every table, in [`TableName::ALL`] order.
    pub fn tables(&self) -> impl Iterator<Item = DsTable> + '_ {
        TableName::ALL.into_iter().map(|t| self.table(t))
    }
}
