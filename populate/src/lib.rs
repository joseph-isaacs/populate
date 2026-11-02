//! Sampling combinators for columnar data generation.
//!
//! This crate provides a trait-based system for generating test data with composable
//! distributions that can output to various storage formats.
//!
//! # Architecture
//!
//! The library is split into three main components:
//!
//! - **Core traits** (`populate-core`): `Distribution`, `SampleIter`, `Sink`, `ListSink`
//! - **Built-in combinators** (`populate-builtins`): `Constant`, `Range`, `List`, tuples
//! - **Sink implementations**: `populate-sink-vec` (Vec), `populate-sink-json` (JSON)
//!
//! # Example
//!
//! ```
//! use populate::{Distribution, collect_into, range, VecSink};
//! use rand::SeedableRng;
//!
//! let mut rng = rand::rngs::StdRng::seed_from_u64(42);
//! let mut dist = range(0i32, 100);
//!
//! let data: Vec<i32> = collect_into(dist.sample(), &mut rng, 10, VecSink::new());
//! assert_eq!(data.len(), 10);
//! ```

// Re-export core traits
pub use populate_core::{
    CollectExt, Collector, Distribution, ListSink, SampleIter, Sink, TupleIter2, TupleIter3,
    collect_into, collect_into_column, collect_list_into, collect_struct2_into,
    collect_struct3_into,
};

// Re-export built-in combinators
pub use populate_builtins::{
    Constant, ConstantIter, List, ListIter, Range, RangeIter, constant, list, range,
};

// Re-export Vec collectors and sinks
pub use populate_sink_vec::{Col, Cols, ListCol, Rows, VecListSink, VecSink};

// Also expose the sub-crates as modules for direct access
pub use populate_builtins as builtins;
pub use populate_core as core;
pub use populate_sink_vec as sink_vec;

// Re-export JSON collectors and sinks when feature is enabled
#[cfg(feature = "json")]
pub use populate_sink_json::{JsonRows, ToJsonValue, pretty_print};

#[cfg(feature = "json")]
pub use populate_sink_json as sink_json;
