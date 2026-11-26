//! Distributions shared by the benchmark generators, built on
//! `populate-core` and `populate-builtins`: pieces several reference tools
//! have in common but that are too specific to one family of tools for
//! the builtins.
//!
//! - [`lehmer`]: the Park–Miller stream of dbgen and dsdgen (TPC-H, SSB,
//!   TPC-DS, DSB), positioned anywhere in `O(log n)` and drawn in lanes.
//! - [`text`]: dsdgen's text and GMT offset columns (TPC-DS, DSB), as
//!   Arrow `Utf8View` and `.dat` fields.
//! - [`views`]: `Utf8View` columns over buffers shared between batches
//!   (a dictionary, a text pool), dbgen's weighted picks as views into
//!   them, and strings written per row (TPC-H, SSB).

pub mod lehmer;
pub mod text;
pub mod views;
