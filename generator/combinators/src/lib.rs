//! Distributions shared by the benchmark generators, built on
//! `populate-core` and `populate-builtins`: pieces several reference tools
//! have in common but that are too specific to one family of tools for
//! the builtins.
//!
//! - [`lehmer`]: the Park–Miller stream of dbgen and dsdgen (TPC-H, SSB,
//!   TPC-DS, DSB), positioned anywhere in `O(log n)` and drawn in lanes.
//! - [`dsdgen`]: dsdgen's GMT offset column (TPC-DS, DSB).
//! - [`views`]: `Utf8View` columns over buffers shared between batches
//!   (a dictionary, a text pool), dbgen's weighted picks as views into
//!   them, strings written per row and strings from fixed lists (TPC-H,
//!   SSB, TPC-C, YCSB, TPC-DS, DSB), also written as text fields.

pub mod dsdgen;
pub mod lehmer;
pub mod views;
