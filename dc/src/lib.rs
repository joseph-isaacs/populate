//! Sampling combinators for columnar data generation.
//!
//! This crate provides a trait-based system for generating test data with composable
//! distributions that can output to various storage formats.
//!
//! # Architecture
//!
//! The library is split into three main components:
//!
//! - **Core traits** (`dc-core`): `Distribution`, `SampleIter`, `Sink`, `ListSink`
//! - **Built-in combinators** (`dc-builtins`): `Constant`, `Range`, `List`, tuples
//! - **Sink implementations**: `dc-sink-vec` (Vec), `dc-sink-arrow` (Arrow)
//!
//! # Example
//!
//! ```
//! use dc::{Constant, Range, Distribution, collect_into, VecSink};
//! use rand::SeedableRng;
//!
//! let mut rng = rand::rngs::StdRng::seed_from_u64(42);
//! let mut dist = Range::new(0i32, 100);
//!
//! let data: Vec<i32> = collect_into(dist.sample(), &mut rng, 10, VecSink::new());
//! assert_eq!(data.len(), 10);
//! ```

// Re-export core traits
pub use dc_core::{
    collect_into, collect_into_column, collect_list_into, collect_struct2_into,
    collect_struct3_into, Distribution, ListSink, SampleIter, Sink, TupleIter2, TupleIter3,
};

// Re-export built-in combinators
pub use dc_builtins::{Constant, ConstantIter, List, ListIter, Range, RangeIter};

// Re-export Vec sinks
pub use dc_sink_vec::{VecListSink, VecSink};

// Re-export Arrow sinks when feature is enabled
#[cfg(feature = "arrow")]
pub use dc_sink_arrow::{
    collect_record_batch2, collect_record_batch3, ArrowListSink, ArrowListSinkType, ArrowSinkType,
    BooleanSink, PrimitiveSink, StringSink,
};

// Also expose the sub-crates as modules for direct access
pub use dc_builtins as builtins;
pub use dc_core as core;
pub use dc_sink_vec as sink_vec;

#[cfg(feature = "arrow")]
pub use dc_sink_arrow as sink_arrow;
