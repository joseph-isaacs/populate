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
//! - chunks run on all cores through `populate_table`'s ordered drivers,
//!   whatever the scale factor;
//! - Parquet row groups are generated *and* encoded on the worker threads;
//! - `.dat` output is the exact dsdgen text format, for diffing.
//!
//! ```
//! # #[cfg(feature = "formats")] {
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
//! # }
//! ```

#[cfg(feature = "formats")]
pub mod dat;
mod dist;
#[cfg(feature = "formats")]
mod gen_tables;

#[cfg(feature = "formats")]
pub use dat::Encoding;
#[cfg(feature = "formats")]
pub use gen_tables::{
    DEFAULT_BATCH_SIZE, DsTable, for_each_batch_many, parquet_props, write_dat_many,
    write_parquet_many,
};

use dsbgen::config::{Session, SessionBuilder, Table as Ds};
use populate_core::{Children, DistChunks, Distribution, Tables};
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
    #[cfg(feature = "formats")]
    pub fn table(&self, t: TableName) -> DsTable {
        DsTable::new(t, self.session.clone())
    }

    /// Every table, in [`TableName::ALL`] order.
    #[cfg(feature = "formats")]
    pub fn tables(&self) -> impl Iterator<Item = DsTable> + '_ {
        TableName::ALL.into_iter().map(|t| self.table(t))
    }
    /// Native `call_center` columns, in independent source chunks.
    pub fn call_center(
        &self,
    ) -> DistChunks<impl Distribution<Item = raw::CallCenter> + Clone + Send + Sync + use<>> {
        dist::raw_call_center(&self.session).chunks(self.source_rows(TableName::CallCenter))
    }

    /// Native `catalog_page` columns, in independent source chunks.
    pub fn catalog_page(
        &self,
    ) -> DistChunks<impl Distribution<Item = raw::CatalogPage> + Clone + Send + Sync + use<>> {
        dist::raw_catalog_page(&self.session).chunks(self.source_rows(TableName::CatalogPage))
    }

    /// Native `customer` columns, in independent source chunks.
    pub fn customer(
        &self,
    ) -> DistChunks<impl Distribution<Item = raw::Customer> + Clone + Send + Sync + use<>> {
        dist::raw_customer(&self.session).chunks(self.source_rows(TableName::Customer))
    }

    /// Native `customer_address` columns, in independent source chunks.
    pub fn customer_address(
        &self,
    ) -> DistChunks<impl Distribution<Item = raw::CustomerAddress> + Clone + Send + Sync + use<>>
    {
        dist::raw_customer_address(&self.session)
            .chunks(self.source_rows(TableName::CustomerAddress))
    }

    /// Native `customer_demographics` columns, in independent source chunks.
    pub fn customer_demographics(
        &self,
    ) -> DistChunks<
        impl Distribution<Item = raw::CustomerDemographicsRows> + Clone + Send + Sync + use<>,
    > {
        dist::raw_customer_demographics(&self.session)
            .chunks(self.source_rows(TableName::CustomerDemographics))
    }

    /// Native `date_dim` columns, in independent source chunks.
    pub fn date_dim(
        &self,
    ) -> DistChunks<impl Distribution<Item = raw::DateDim> + Clone + Send + Sync + use<>> {
        dist::raw_date_dim(&self.session).chunks(self.source_rows(TableName::DateDim))
    }

    /// Native `household_demographics` columns, in independent source chunks.
    pub fn household_demographics(
        &self,
    ) -> DistChunks<
        impl Distribution<Item = raw::HouseholdDemographics> + Clone + Send + Sync + use<>,
    > {
        dist::raw_household_demographics(&self.session)
            .chunks(self.source_rows(TableName::HouseholdDemographics))
    }

    /// Native `income_band` columns, in independent source chunks.
    pub fn income_band(
        &self,
    ) -> DistChunks<impl Distribution<Item = raw::IncomeBand> + Clone + Send + Sync + use<>> {
        dist::raw_income_band(&self.session).chunks(self.source_rows(TableName::IncomeBand))
    }

    /// Native `item` columns, in independent source chunks.
    pub fn item(
        &self,
    ) -> DistChunks<impl Distribution<Item = raw::Item> + Clone + Send + Sync + use<>> {
        dist::raw_item(&self.session).chunks(self.source_rows(TableName::Item))
    }

    /// Native `promotion` columns, in independent source chunks.
    pub fn promotion(
        &self,
    ) -> DistChunks<impl Distribution<Item = raw::Promotion> + Clone + Send + Sync + use<>> {
        dist::raw_promotion(&self.session).chunks(self.source_rows(TableName::Promotion))
    }

    /// Native `reason` columns, in independent source chunks.
    pub fn reason(
        &self,
    ) -> DistChunks<impl Distribution<Item = raw::Reason> + Clone + Send + Sync + use<>> {
        dist::raw_reason(&self.session).chunks(self.source_rows(TableName::Reason))
    }

    /// Native `ship_mode` columns, in independent source chunks.
    pub fn ship_mode(
        &self,
    ) -> DistChunks<impl Distribution<Item = raw::ShipMode> + Clone + Send + Sync + use<>> {
        dist::raw_ship_mode(&self.session).chunks(self.source_rows(TableName::ShipMode))
    }

    /// Native `store` columns, in independent source chunks.
    pub fn store(
        &self,
    ) -> DistChunks<impl Distribution<Item = raw::Store> + Clone + Send + Sync + use<>> {
        dist::raw_store(&self.session).chunks(self.source_rows(TableName::Store))
    }

    /// Native `time_dim` columns, in independent source chunks.
    pub fn time_dim(
        &self,
    ) -> DistChunks<impl Distribution<Item = raw::TimeDim> + Clone + Send + Sync + use<>> {
        dist::raw_time_dim(&self.session).chunks(self.source_rows(TableName::TimeDim))
    }

    /// Native `warehouse` columns, in independent source chunks.
    pub fn warehouse(
        &self,
    ) -> DistChunks<impl Distribution<Item = raw::Warehouse> + Clone + Send + Sync + use<>> {
        dist::raw_warehouse(&self.session).chunks(self.source_rows(TableName::Warehouse))
    }

    /// Native `web_page` columns, in independent source chunks.
    pub fn web_page(
        &self,
    ) -> DistChunks<impl Distribution<Item = raw::WebPage> + Clone + Send + Sync + use<>> {
        dist::raw_web_page(&self.session).chunks(self.source_rows(TableName::WebPage))
    }

    /// Native `web_site` columns, in independent source chunks.
    pub fn web_site(
        &self,
    ) -> DistChunks<impl Distribution<Item = raw::WebSite> + Clone + Send + Sync + use<>> {
        dist::raw_web_site(&self.session).chunks(self.source_rows(TableName::WebSite))
    }

    /// Native `inventory` columns, in independent source chunks.
    pub fn inventory(
        &self,
    ) -> DistChunks<impl Distribution<Item = raw::InventoryRow> + Clone + Send + Sync + use<>> {
        dist::raw_inventory(&self.session).chunks(self.source_rows(TableName::Inventory))
    }

    /// Native `store_sales` columns, in independent source chunks.
    pub fn store_sales(
        &self,
    ) -> DistChunks<
        impl Distribution<Item = Children<raw::StoreSalesCols>> + Clone + Send + Sync + use<>,
    > {
        dist::raw_store_sales(&self.session).chunks(self.source_rows(TableName::StoreSales))
    }

    /// Native `store_returns` columns, in independent source chunks.
    pub fn store_returns(
        &self,
    ) -> DistChunks<
        impl Distribution<Item = Children<raw::StoreReturnsCols>> + Clone + Send + Sync + use<>,
    > {
        dist::raw_store_returns(&self.session).chunks(self.source_rows(TableName::StoreReturns))
    }

    /// Native `store_sales_returns` columns, in independent source chunks.
    pub fn store_sales_returns(
        &self,
    ) -> DistChunks<
        impl Distribution<
            Item = Tables<(
                Children<raw::StoreSalesCols>,
                Children<raw::StoreReturnsCols>,
            )>,
        > + Clone
        + Send
        + Sync
        + use<>,
    > {
        dist::raw_store_sales_returns(&self.session).chunks(self.source_rows(TableName::StoreSales))
    }

    /// Native `catalog_sales` columns, in independent source chunks.
    pub fn catalog_sales(
        &self,
    ) -> DistChunks<
        impl Distribution<Item = Children<raw::CatalogSalesCols>> + Clone + Send + Sync + use<>,
    > {
        dist::raw_catalog_sales(&self.session).chunks(self.source_rows(TableName::CatalogSales))
    }

    /// Native `catalog_returns` columns, in independent source chunks.
    pub fn catalog_returns(
        &self,
    ) -> DistChunks<
        impl Distribution<Item = Children<raw::CatalogReturnsCols>> + Clone + Send + Sync + use<>,
    > {
        dist::raw_catalog_returns(&self.session).chunks(self.source_rows(TableName::CatalogReturns))
    }

    /// Native `catalog_sales_returns` columns, in independent source chunks.
    pub fn catalog_sales_returns(
        &self,
    ) -> DistChunks<
        impl Distribution<
            Item = Tables<(
                Children<raw::CatalogSalesCols>,
                Children<raw::CatalogReturnsCols>,
            )>,
        > + Clone
        + Send
        + Sync
        + use<>,
    > {
        dist::raw_catalog_sales_returns(&self.session)
            .chunks(self.source_rows(TableName::CatalogSales))
    }

    /// Native `web_sales` columns, in independent source chunks.
    pub fn web_sales(
        &self,
    ) -> DistChunks<
        impl Distribution<Item = Children<raw::WebSalesCols>> + Clone + Send + Sync + use<>,
    > {
        dist::raw_web_sales(&self.session).chunks(self.source_rows(TableName::WebSales))
    }

    /// Native `web_returns` columns, in independent source chunks.
    pub fn web_returns(
        &self,
    ) -> DistChunks<
        impl Distribution<Item = Children<raw::WebReturnsCols>> + Clone + Send + Sync + use<>,
    > {
        dist::raw_web_returns(&self.session).chunks(self.source_rows(TableName::WebReturns))
    }

    /// Native `web_sales_returns` columns, in independent source chunks.
    pub fn web_sales_returns(
        &self,
    ) -> DistChunks<
        impl Distribution<Item = Tables<(Children<raw::WebSalesCols>, Children<raw::WebReturnsCols>)>>
        + Clone
        + Send
        + Sync
        + use<>,
    > {
        dist::raw_web_sales_returns(&self.session).chunks(self.source_rows(TableName::WebSales))
    }
}

/// Native row shapes and constructors, independent of format adapters.
pub mod raw {
    pub use crate::dist::{
        CallCenter, CatalogPage, CatalogReturnsCols, CatalogSalesCols, Customer, CustomerAddress,
        CustomerDemographicsRows, DateDim, DemoRow, HouseholdDemographics, IncomeBand,
        InventoryRow, Item, Promotion, Reason, ShipMode, Store, StoreReturnsCols, StoreSalesCols,
        Str, TimeDim, Warehouse, WebPage, WebReturnsCols, WebSalesCols, WebSite, raw_call_center,
        raw_catalog_page, raw_catalog_returns, raw_catalog_sales, raw_catalog_sales_returns,
        raw_customer, raw_customer_address, raw_customer_demographics,
        raw_customer_demographics_rows, raw_date_dim, raw_household_demographics, raw_income_band,
        raw_inventory, raw_item, raw_promotion, raw_reason, raw_ship_mode, raw_store,
        raw_store_returns, raw_store_sales, raw_store_sales_returns, raw_time_dim, raw_warehouse,
        raw_web_page, raw_web_returns, raw_web_sales, raw_web_sales_returns, raw_web_site,
    };
    pub use dsbgen::config::{Session, SessionBuilder, Table as Ds};
}

impl TableName {
    pub(crate) fn ds(self) -> Ds {
        match self {
            Self::CallCenter => Ds::CallCenter,
            Self::CatalogPage => Ds::CatalogPage,
            Self::CatalogReturns => Ds::CatalogReturns,
            Self::CatalogSales => Ds::CatalogSales,
            Self::Customer => Ds::Customer,
            Self::CustomerAddress => Ds::CustomerAddress,
            Self::CustomerDemographics => Ds::CustomerDemographics,
            Self::DateDim => Ds::DateDim,
            Self::HouseholdDemographics => Ds::HouseholdDemographics,
            Self::IncomeBand => Ds::IncomeBand,
            Self::Inventory => Ds::Inventory,
            Self::Item => Ds::Item,
            Self::Promotion => Ds::Promotion,
            Self::Reason => Ds::Reason,
            Self::ShipMode => Ds::ShipMode,
            Self::Store => Ds::Store,
            Self::StoreReturns => Ds::StoreReturns,
            Self::StoreSales => Ds::StoreSales,
            Self::TimeDim => Ds::TimeDim,
            Self::Warehouse => Ds::Warehouse,
            Self::WebPage => Ds::WebPage,
            Self::WebReturns => Ds::WebReturns,
            Self::WebSales => Ds::WebSales,
            Self::WebSite => Ds::WebSite,
        }
    }
    /// Average output rows per source ticket, order, or dimension row.
    pub fn rows_per_source_row(self) -> f64 {
        match self {
            Self::CallCenter => 1.0,
            Self::CatalogPage => 1.0,
            Self::CatalogReturns => 0.9,
            Self::CatalogSales => 9.0,
            Self::Customer => 1.0,
            Self::CustomerAddress => 1.0,
            Self::CustomerDemographics => 1.0,
            Self::DateDim => 1.0,
            Self::HouseholdDemographics => 1.0,
            Self::IncomeBand => 1.0,
            Self::Inventory => 1.0,
            Self::Item => 1.0,
            Self::Promotion => 1.0,
            Self::Reason => 1.0,
            Self::ShipMode => 1.0,
            Self::Store => 1.0,
            Self::StoreReturns => 1.2,
            Self::StoreSales => 12.0,
            Self::TimeDim => 1.0,
            Self::Warehouse => 1.0,
            Self::WebPage => 1.0,
            Self::WebReturns => 1.2,
            Self::WebSales => 12.0,
            Self::WebSite => 1.0,
        }
    }
}
