//! Python bindings for populate.
//!
//! The crate has two layers:
//!
//! - [`dynamic`] (always built, plain Rust): distributions whose row types
//!   are chosen at run time, and tables of them that run on
//!   `populate-table`'s multi-core drivers.
//! - the `populate` Python module (feature `python`, the only code that
//!   uses PyO3): builders for those distributions, the generators, and
//!   Arrow output handed to Python through the Arrow C data interface, so
//!   pyarrow, polars or DuckDB read the buffers populate wrote without a
//!   copy.
//!
//! Without `python` the crate has no PyO3 dependency; `maturin` turns the
//! feature on (see `pyproject.toml`).

pub mod dynamic;

#[cfg(feature = "python")]
pub mod py;
