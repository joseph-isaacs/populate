// Modified from tpcdsgen-arrow 4.0.0 for the populate DSB variant.
// Licensed under Apache-2.0; see the crate LICENSE and generator/dsb/NOTICE.md.

//! Generate DSB data as Arrow RecordBatches
//!
//! This crate provides generators for DSB tables that directly produce
//! Arrow [`RecordBatch`](arrow::array::RecordBatch)es. This is significantly faster than generating DAT or CSV
//! files and then parsing them into Arrow.
//!
//! # Example
//! ```
//! # use dsbgen::config::SessionBuilder;
//! # use dsbgen_arrow::StoreSalesArrow;
//! # use dsbgen_arrow::arrow::array::AsArray;
//! # use dsbgen_arrow::arrow::datatypes::Int32Type;
//! // Create a SF=1 session for the store_sales table
//! let session = SessionBuilder::new()
//!   .with_scale_factor(1.0)
//!   .build()
//!   .unwrap();
//! // Create the appropriate generator to Arrow
//! let mut arrow_generator = StoreSalesArrow::new(session)
//!   .with_batch_size(10);
//! // The generator is a Rust iterator, producing RecordBatch
//! let batch = arrow_generator.next().unwrap().unwrap();
//! assert_eq!(batch.num_rows(), 10);
//! // DSB dsdgen's first store_sales row starts `2450815|65495|17881|`
//! let col = |i: usize| batch.column(i).as_primitive::<Int32Type>().value(0);
//! assert_eq!((col(0), col(1), col(2)), (2450815, 65495, 17881));
//! ```
//!
//! # Feature Flags
//!
//! * `arrow_60` - build against [Arrow 60.x] (default)
//! * `arrow_59` - build against [Arrow 59.x]
//!
//! Pick the one that matches your project's arrow version. For arrow 59, also
//! disable the default features so that arrow 60 is not compiled as well:
//!
//! ```toml
//! # arrow 60
//! dsbgen-arrow = "..."
//! # arrow 59
//! dsbgen-arrow = { version = "...", default-features = false, features = ["arrow_59"] }
//! ```
//!
//! If both are enabled, arrow 59 is used. The selected version is re-exported
//! as [`arrow`].
//!
//! [Arrow 59.x]: https://docs.rs/arrow/59
//! [Arrow 60.x]: https://docs.rs/arrow/60

// Alias the selected arrow version as `arrow`.
pub extern crate arrow_60 as arrow;

pub mod conversions;
mod tables;

pub use tables::{
    CallCenterArrow, CatalogPageArrow, CatalogReturnsArrow, CatalogSalesArrow,
    CustomerAddressArrow, CustomerArrow, CustomerDemographicsArrow, DateDimArrow,
    DbgenVersionArrow, HouseholdDemographicsArrow, IncomeBandArrow, InventoryArrow, ItemArrow,
    PromotionArrow, ReasonArrow, ShipModeArrow, StoreArrow, StoreReturnsArrow, StoreSalesArrow,
    TimeDimArrow, WarehouseArrow, WebPageArrow, WebReturnsArrow, WebSalesArrow, WebSiteArrow,
};

/// Default number of rows per [`RecordBatch`](arrow::array::RecordBatch).
pub const DEFAULT_BATCH_SIZE: usize = 8_000;
