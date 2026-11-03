//! TPC-H data generation on populate-2 combinators, emitting Arrow.
//!
//! Output is value-for-value identical to `tpchgen-arrow` (see tests), but
//! generated one column at a time straight into Arrow buffers.
//!
//! `tpchgen` is used only for TPC-H's reference data: the `dists.dss` word
//! lists and the generated text pool. The random streams, columns and tables
//! are implemented here.

pub mod column;
pub mod rng;
pub mod tables;
pub mod values;

pub use tables::{
    Customer, Nation, Part, PartSupp, Region, Supplier, Table, batches, batches_copied,
};
