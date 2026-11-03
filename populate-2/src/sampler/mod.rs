//! Sampling dialects: how a uniform double, a uniform index, a normal draw or
//! a sample without replacement is computed from an `Rng`.
//!
//! Reference generators (R scripts, Go tools, C `dbgen`s) each have their own
//! RNG *and* their own algorithms for turning raw random bits into values.
//! Matching a reference byte for byte needs both. A [`Sampler`] is the
//! algorithm half; pair it with the matching RNG for exact output, or with any
//! `rand::Rng` for the same distribution at full speed:
//!
//! | sampler | algorithms | exact RNG |
//! |---------|------------|-----------|
//! | [`FastSampler`] | `rand`'s native uniform sampling | — |
//! | [`RSampler`] | R >= 3.6 (`unif_rand`, rejection `R_unif_index`, `sample`) | [`RRng`] (`set.seed`) |
//! | [`GoSampler`] | Go `math/rand` (`Float64`, `Intn`, `NormFloat64`, `Perm`) | [`GoRand`] (`rand.Seed`) |
//!
//! Every leaf distribution in [`random`](crate::random), every
//! [`process`](crate::process) and every [`table`](crate::table) op is
//! generic over the sampler, so one generator graph serves both purposes.

pub mod go;
mod go_tables;
pub mod r;

pub use go::{GoRand, GoRandExt, GoSampler};
pub use r::{RRng, RSampler, r_round, r_unif_from_u32};

use rand::Rng;
use std::f64::consts::TAU;

pub trait Sampler: Copy {
    /// Uniform double in `[0, 1)` (or `(0, 1)`, as the dialect defines it).
    fn unif(self, rng: &mut impl Rng) -> f64;

    /// Uniform integer in `[0, n)`.
    fn index(self, rng: &mut impl Rng, n: usize) -> usize;

    /// Standard normal draw. Default: Box–Muller over [`unif`](Self::unif).
    fn normal(self, rng: &mut impl Rng) -> f64 {
        let u1 = loop {
            let u = self.unif(rng);
            if u > 0.0 {
                break u;
            }
        };
        let u2 = self.unif(rng);
        (-2.0 * u1.ln()).sqrt() * (TAU * u2).cos()
    }

    /// Uniform double in `(min, max)`, like R's `runif` (no draw when
    /// `min == max`).
    fn runif(self, rng: &mut impl Rng, min: f64, max: f64) -> f64 {
        if min == max {
            return min;
        }
        min + (max - min) * self.unif(rng)
    }

    /// `k` indices in `[0, n)` drawn with replacement.
    fn sample_with_replacement(self, rng: &mut impl Rng, n: usize, k: usize) -> Vec<usize> {
        (0..k).map(|_| self.index(rng, n)).collect()
    }

    /// `k` distinct indices in `[0, n)`, in draw order. Default: partial
    /// Fisher–Yates by swap-remove (draw `j` in the remaining pool, take it,
    /// move the last element into its place).
    fn sample_without_replacement(self, rng: &mut impl Rng, n: usize, k: usize) -> Vec<usize> {
        swap_remove_sample(self, rng, n, k)
    }

    /// A random permutation of `0..n`.
    fn permutation(self, rng: &mut impl Rng, n: usize) -> Vec<usize> {
        self.sample_without_replacement(rng, n, n)
    }
}

/// Partial Fisher–Yates by swap-remove: draw `j` in the remaining pool, take
/// `pool[j]`, move the pool's last element into its place.
pub fn swap_remove_sample(
    sampler: impl Sampler,
    rng: &mut impl Rng,
    n: usize,
    k: usize,
) -> Vec<usize> {
    assert!(k <= n, "cannot take a sample larger than the population");
    let mut pool: Vec<usize> = (0..n).collect();
    let mut remaining = n;
    let mut out = Vec::with_capacity(k);
    for _ in 0..k {
        let j = sampler.index(rng, remaining);
        out.push(pool[j]);
        remaining -= 1;
        pool[j] = pool[remaining];
    }
    out
}

/// `rand`'s native uniform sampling, for any `Rng`.
#[derive(Clone, Copy, Debug, Default)]
pub struct FastSampler;

impl Sampler for FastSampler {
    fn unif(self, rng: &mut impl Rng) -> f64 {
        rng.random()
    }

    fn index(self, rng: &mut impl Rng, n: usize) -> usize {
        if n <= 1 {
            return 0;
        }
        rng.random_range(0..n)
    }
}
