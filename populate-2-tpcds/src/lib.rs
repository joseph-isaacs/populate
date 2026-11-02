//! TPC-DS data generation on populate-2 combinators, emitting Arrow.
//!
//! Output is value-for-value identical to `tpcdsgen-arrow` (see tests).
//! Shares the random streams, column adapter and Arrow sinks with
//! `populate-2-tpch`; TPC-DS uses the same Park–Miller generator but draws
//! uniform ints with an integer modulo instead of float scaling.
//!
//! `tpcdsgen` is used only for reference data: scaling (row counts per scale
//! factor), table null rates, and the demographics value lists.

pub mod columns;
pub mod tables;

pub use tables::{CustomerDemographics, Inventory, Table, batches};
