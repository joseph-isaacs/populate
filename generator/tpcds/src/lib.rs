//! TPC-DS data, byte-identical to the reference `dsdgen`, on all cores.
//!
//! The values come from [`tpcdsgen`], the Apache-2.0 Rust port of dsdgen
//! from the tpcgen-rs project (its default `CompatMode::C` reproduces
//! dsdgen's output, bugs included). This crate adds the parallel engine:
//!
//! - every table is split into small, independent ranges of *source rows*
//!   (a ticket, an order, a customer). tpcdsgen's random streams skip
//!   exactly to any row, so the chunks concatenate to the sequential output
//!   for any chunk size and thread count;
//! - chunks run on all cores through `populate_table`'s ordered drivers,
//!   whatever the scale factor (dsdgen and tpcgen-cli only split tables
//!   above 1M source rows, so at SF 1 `store_sales` runs on one core);
//! - Parquet row groups are generated *and* encoded on the worker threads;
//! - `.dat` output is the exact dsdgen text format, for diffing.
//!
//! ```
//! # #[cfg(feature = "formats")] {
//! use populate_gen_tpcds::{Encoding, TableName, Tpcds};
//!
//! let ds = Tpcds::new(1.0);
//! let ship_mode = ds.table(TableName::ShipMode);
//! assert_eq!(ship_mode.source_rows(), 20);
//!
//! // all cores, batches in order
//! let mut rows = 0;
//! ship_mode.for_each_batch(0, |batch| rows += batch.num_rows());
//! assert_eq!(rows, 20);
//!
//! // dsdgen's `ship_mode.dat`, byte for byte
//! let dat = ship_mode.write_dat(Vec::new(), 0, Encoding::Latin1).unwrap();
//! assert!(dat.starts_with(b"1|AAAAAAAABAAAAAAA|"));
//! # }
//! ```

mod channels;
#[cfg(feature = "formats")]
pub mod dat;
mod dims;
#[cfg(feature = "formats")]
mod gen_tables;
mod inventory;
pub mod masked;
mod seeds;

#[cfg(feature = "formats")]
pub use dat::Encoding;
#[cfg(feature = "formats")]
pub use gen_tables::{
    DEFAULT_BATCH_SIZE, DEFAULT_CHUNK_ROWS, DsTable, for_each_batch_many, parquet_props,
    write_dat_many, write_parquet_many,
};

use populate_core::{DistChunks, Distribution};
use std::fmt;
use std::str::FromStr;
use tpcdsgen::config::{Session, SessionBuilder};

/// Every TPC-DS table.
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
            .ok_or_else(|| format!("unknown TPC-DS table `{s}`"))
    }
}

/// A TPC-DS data set at one scale factor.
#[derive(Debug, Clone)]
pub struct Tpcds {
    sf: f64,
    session: Session,
}

impl Tpcds {
    /// Scale factor `sf` (1 = about 1 GB of `.dat` text). dsdgen's scale
    /// factors are 1, 10, 100, 300, 1000, ...; others are interpolated the
    /// way dsdgen does.
    pub fn new(sf: f64) -> Self {
        assert!(sf > 0.0, "scale factor must be > 0");
        let session = SessionBuilder::new()
            .with_scale_factor(sf)
            .build()
            .expect("valid TPC-DS session");
        Tpcds { sf, session }
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
    #[cfg(feature = "formats")]
    pub fn table(&self, t: TableName) -> DsTable {
        DsTable::new(t, &self.session)
    }

    /// Every table, in [`TableName::ALL`] order.
    #[cfg(feature = "formats")]
    pub fn tables(&self) -> impl Iterator<Item = DsTable> + '_ {
        TableName::ALL.into_iter().map(|t| self.table(t))
    }
}

macro_rules! tables {
    ($($t:ident, $per_source:expr;)*) => {
        impl TableName {
            /// The tpcdsgen table.
            pub(crate) fn ds(self) -> tpcdsgen::config::Table {
                match self { $(TableName::$t => tpcdsgen::config::Table::$t,)* }
            }

            /// Average output rows per source row (line items per ticket or
            /// order; returned share of them; 1 otherwise).
            pub fn rows_per_source_row(self) -> f64 {
                match self { $(TableName::$t => $per_source,)* }
            }
        }
    };
}

tables! {
    CallCenter, 1.0;
    CatalogPage, 1.0;
    CatalogReturns, 0.9;
    CatalogSales, 9.0;
    Customer, 1.0;
    CustomerAddress, 1.0;
    CustomerDemographics, 1.0;
    DateDim, 1.0;
    HouseholdDemographics, 1.0;
    IncomeBand, 1.0;
    Inventory, 1.0;
    Item, 1.0;
    Promotion, 1.0;
    Reason, 1.0;
    ShipMode, 1.0;
    Store, 1.0;
    StoreReturns, 1.2;
    StoreSales, 12.0;
    TimeDim, 1.0;
    Warehouse, 1.0;
    WebPage, 1.0;
    WebReturns, 1.2;
    WebSales, 12.0;
    WebSite, 1.0;
}

/// Typed raw distributions and their row types.
pub mod raw {
    pub use crate::channels::{
        CatalogTables, StoreTables, WebTables, catalog as catalog_sales_returns,
        store as store_sales_returns, web as web_sales_returns,
    };
    pub use crate::dims::*;
    pub use crate::inventory::{DemoRow, customer_demographics, inventory};
    pub use tpcdsgen::config::{Session, SessionBuilder};
}

impl Tpcds {
    /// Raw `call_center` columns, in independently addressable source chunks.
    pub fn call_center(
        &self,
    ) -> DistChunks<impl Distribution<Item = dims::CallCenter> + Clone + Sync + use<>> {
        DistChunks::new(
            self.source_rows(TableName::CallCenter),
            dims::call_center(&self.session),
        )
    }
    /// Raw `catalog_page` columns, in independently addressable source chunks.
    pub fn catalog_page(
        &self,
    ) -> DistChunks<impl Distribution<Item = dims::CatalogPage> + Clone + Sync + use<>> {
        DistChunks::new(
            self.source_rows(TableName::CatalogPage),
            dims::catalog_page(&self.session),
        )
    }
    /// Raw `customer` columns, in independently addressable source chunks.
    pub fn customer(
        &self,
    ) -> DistChunks<impl Distribution<Item = dims::Customer> + Clone + Sync + use<>> {
        DistChunks::new(
            self.source_rows(TableName::Customer),
            dims::customer(&self.session),
        )
    }
    /// Raw `customer_address` columns, in independently addressable source chunks.
    pub fn customer_address(
        &self,
    ) -> DistChunks<impl Distribution<Item = dims::CustomerAddress> + Clone + Sync + use<>> {
        DistChunks::new(
            self.source_rows(TableName::CustomerAddress),
            dims::customer_address(&self.session),
        )
    }
    /// Raw `date_dim` columns, in independently addressable source chunks.
    pub fn date_dim(
        &self,
    ) -> DistChunks<impl Distribution<Item = dims::DateDim> + Clone + Sync + use<>> {
        DistChunks::new(
            self.source_rows(TableName::DateDim),
            dims::date_dim(&self.session),
        )
    }
    /// Raw `household_demographics` columns, in independently addressable source chunks.
    pub fn household_demographics(
        &self,
    ) -> DistChunks<impl Distribution<Item = dims::HouseholdDemographics> + Clone + Sync + use<>>
    {
        DistChunks::new(
            self.source_rows(TableName::HouseholdDemographics),
            dims::household_demographics(&self.session),
        )
    }
    /// Raw `income_band` columns, in independently addressable source chunks.
    pub fn income_band(
        &self,
    ) -> DistChunks<impl Distribution<Item = dims::IncomeBand> + Clone + Sync + use<>> {
        DistChunks::new(
            self.source_rows(TableName::IncomeBand),
            dims::income_band(&self.session),
        )
    }
    /// Raw `item` columns, in independently addressable source chunks.
    pub fn item(&self) -> DistChunks<impl Distribution<Item = dims::Item> + Clone + Sync + use<>> {
        DistChunks::new(self.source_rows(TableName::Item), dims::item(&self.session))
    }
    /// Raw `promotion` columns, in independently addressable source chunks.
    pub fn promotion(
        &self,
    ) -> DistChunks<impl Distribution<Item = dims::Promotion> + Clone + Sync + use<>> {
        DistChunks::new(
            self.source_rows(TableName::Promotion),
            dims::promotion(&self.session),
        )
    }
    /// Raw `reason` columns, in independently addressable source chunks.
    pub fn reason(
        &self,
    ) -> DistChunks<impl Distribution<Item = dims::Reason> + Clone + Sync + use<>> {
        DistChunks::new(
            self.source_rows(TableName::Reason),
            dims::reason(&self.session),
        )
    }
    /// Raw `ship_mode` columns, in independently addressable source chunks.
    pub fn ship_mode(
        &self,
    ) -> DistChunks<impl Distribution<Item = dims::ShipMode> + Clone + Sync + use<>> {
        DistChunks::new(
            self.source_rows(TableName::ShipMode),
            dims::ship_mode(&self.session),
        )
    }
    /// Raw `store` columns, in independently addressable source chunks.
    pub fn store(
        &self,
    ) -> DistChunks<impl Distribution<Item = dims::Store> + Clone + Sync + use<>> {
        DistChunks::new(
            self.source_rows(TableName::StoreSales),
            dims::store(&self.session),
        )
    }
    /// Raw `time_dim` columns, in independently addressable source chunks.
    pub fn time_dim(
        &self,
    ) -> DistChunks<impl Distribution<Item = dims::TimeDim> + Clone + Sync + use<>> {
        DistChunks::new(
            self.source_rows(TableName::TimeDim),
            dims::time_dim(&self.session),
        )
    }
    /// Raw `warehouse` columns, in independently addressable source chunks.
    pub fn warehouse(
        &self,
    ) -> DistChunks<impl Distribution<Item = dims::Warehouse> + Clone + Sync + use<>> {
        DistChunks::new(
            self.source_rows(TableName::Warehouse),
            dims::warehouse(&self.session),
        )
    }
    /// Raw `web_page` columns, in independently addressable source chunks.
    pub fn web_page(
        &self,
    ) -> DistChunks<impl Distribution<Item = dims::WebPage> + Clone + Sync + use<>> {
        DistChunks::new(
            self.source_rows(TableName::WebPage),
            dims::web_page(&self.session),
        )
    }
    /// Raw `web_site` columns, in independently addressable source chunks.
    pub fn web_site(
        &self,
    ) -> DistChunks<impl Distribution<Item = dims::WebSite> + Clone + Sync + use<>> {
        DistChunks::new(
            self.source_rows(TableName::WebSite),
            dims::web_site(&self.session),
        )
    }
    /// Raw `inventory` columns, in independently addressable source chunks.
    pub fn inventory(
        &self,
    ) -> DistChunks<impl Distribution<Item = ((i32, i32, i32), Option<i32>)> + Clone + Sync + use<>>
    {
        DistChunks::new(
            self.source_rows(TableName::Inventory),
            inventory::inventory(&self.session),
        )
    }
    /// Raw `customer_demographics` columns, in independently addressable source chunks.
    pub fn customer_demographics(
        &self,
    ) -> DistChunks<impl Distribution<Item = inventory::DemoRow> + Clone + Sync + use<>> {
        DistChunks::new(
            self.source_rows(TableName::CustomerDemographics),
            inventory::customer_demographics(),
        )
    }
    /// Raw `store` columns, in independently addressable source chunks.
    pub fn store_sales_returns(
        &self,
    ) -> DistChunks<impl Distribution<Item = channels::StoreTables> + Clone + Sync + use<>> {
        DistChunks::new(
            self.source_rows(TableName::StoreSales),
            channels::store(&self.session),
        )
    }
    /// Raw `catalog` columns, in independently addressable source chunks.
    pub fn catalog_sales_returns(
        &self,
    ) -> DistChunks<impl Distribution<Item = channels::CatalogTables> + Clone + Sync + use<>> {
        DistChunks::new(
            self.source_rows(TableName::CatalogSales),
            channels::catalog(&self.session),
        )
    }
    /// Raw `web` columns, in independently addressable source chunks.
    pub fn web_sales_returns(
        &self,
    ) -> DistChunks<impl Distribution<Item = channels::WebTables> + Clone + Sync + use<>> {
        DistChunks::new(
            self.source_rows(TableName::WebSales),
            channels::web(&self.session),
        )
    }
}
