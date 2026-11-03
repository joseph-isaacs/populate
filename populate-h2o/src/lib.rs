//! H2O.ai db-benchmark data generators (`groupby-datagen.R`,
//! `join-datagen.R`) built on populate-2 combinators.
//!
//! With [`RRng`] + [`RSampler`] the output is identical to the R reference
//! scripts; with any other `Rng` + [`FastSampler`] it has the same
//! distribution, generated with `rand`'s native sampling. The R dialect
//! (`RRng`, `RSampler`, `r_round`), the leaf columns and the whole-table ops
//! (NA injection, covering samples, key splits, `setkey` sorting) are generic
//! populate-2 modules; only the table definitions live here.

pub mod csv;
pub mod groupby;
pub mod join;
#[cfg(test)]
mod test;

use populate_2::dist::map;
use populate_2::random::uniform;
use populate_2::sampler::r_round;
use populate_2::traits::{Distribution, VecBackend};

pub use crate::groupby::{GroupByConfig, GroupByTable};
pub use crate::join::{JoinConfig, JoinTables};
pub use populate_2::sampler::{FastSampler, RRng, RSampler, Sampler};

/// `round(runif(N, min, max), digits)`.
pub fn runif_rounded<S: Sampler>(
    sampler: S,
    min: f64,
    max: f64,
    digits: i32,
) -> impl Distribution<VecBackend, Item = f64> {
    map(uniform(sampler, min, max), move |x: f64| r_round(x, digits))
}
