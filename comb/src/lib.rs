//! # Comb - Synthetic Data Combinator Library
//!
//! A library for generating synthetic data with:
//! - Zero intermediate allocation during generation
//! - Type-safe boundaries with runtime flexibility
//! - Automatic columnar/row-wise strategy selection based on dependencies
//! - Two-tier system: fully typed (Tier 1) and erased/dynamic (Tier 2)

mod analysis;
mod engine;
mod generator;
mod generators;
mod graph;
mod schema;
mod sink;
mod value;

pub use engine::Engine;
pub use generator::{ErasedGen, Gen};
pub use generators::*;
pub use graph::{Col, Graph, NodeId};
pub use schema::*;
pub use sink::{ErasedSink, Sink, VecSink};
pub use value::{ErasedValue, TypeTag};
