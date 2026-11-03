//! H2O.ai db-benchmark data generators (`groupby-datagen.R`,
//! `join-datagen.R`) built on populate-2 combinators.
//!
//! With [`RRng`] + [`RSampler`] the output is identical to the R reference
//! scripts; with any other `Rng` + [`FastSampler`] it has the same
//! distribution, generated with `rand`'s native sampling.

pub mod csv;
pub mod dist;
pub mod groupby;
pub mod join;
pub mod ops;
pub mod rng;
pub mod sampler;
#[cfg(test)]
mod test;

pub use crate::groupby::{GroupByConfig, GroupByTable};
pub use crate::join::{JoinConfig, JoinTables};
pub use crate::rng::RRng;
pub use crate::sampler::{FastSampler, RSampler, Sampler};
