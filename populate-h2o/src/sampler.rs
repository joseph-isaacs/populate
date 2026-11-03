//! Sampling strategies: how a uniform double or a uniform index is drawn
//! from an `Rng`.
//!
//! Every distribution and table op in this crate is generic over a
//! [`Sampler`], so the same generator graph runs in two modes:
//!
//! * [`RSampler`] — R's algorithms (`unif_rand`, `R_unif_index` with
//!   rejection sampling, `sample`'s swap-remove without replacement). Paired
//!   with [`RRng`](crate::rng::RRng) it reproduces R's output exactly.
//! * [`FastSampler`] — `rand`'s own uniform sampling, for any `Rng`.

use crate::rng::r_unif_from_u32;
use rand::Rng;
use std::collections::HashSet;

pub trait Sampler: Copy {
    /// Uniform double in the open interval `(0, 1)`.
    fn unif(self, rng: &mut impl Rng) -> f64;

    /// Uniform integer in `[0, n)`, returned as `f64` (R works in doubles so
    /// that a non-integral `n` behaves as it does in R).
    fn index(self, rng: &mut impl Rng, n: f64) -> f64;

    /// Uniform double in `(min, max)`, like R's `runif`.
    fn runif(self, rng: &mut impl Rng, min: f64, max: f64) -> f64 {
        if min == max {
            return min;
        }
        min + (max - min) * self.unif(rng)
    }
}

/// R's sampling algorithms (R >= 3.6, `sample.kind = "Rejection"`).
#[derive(Clone, Copy, Debug, Default)]
pub struct RSampler;

impl RSampler {
    /// R's `rbits`: `bits` random bits built from 16-bit chunks of `unif_rand`.
    fn rbits(self, rng: &mut impl Rng, bits: u32) -> f64 {
        let mut v: i64 = 0;
        let mut n = 0;
        while n <= bits {
            let v1 = (self.unif(rng) * 65536.0).floor() as i64;
            v = 65536 * v + v1;
            n += 16;
        }
        (v & ((1i64 << bits) - 1)) as f64
    }
}

impl Sampler for RSampler {
    fn unif(self, rng: &mut impl Rng) -> f64 {
        r_unif_from_u32(rng.next_u32())
    }

    /// `R_unif_index`: rejection sampling below the next power of two.
    fn index(self, rng: &mut impl Rng, n: f64) -> f64 {
        if n <= 0.0 {
            return 0.0;
        }
        let bits = n.log2().ceil() as u32;
        loop {
            let dv = self.rbits(rng, bits);
            if n > dv {
                return dv;
            }
        }
    }
}

/// `rand`'s native uniform sampling.
#[derive(Clone, Copy, Debug, Default)]
pub struct FastSampler;

impl Sampler for FastSampler {
    fn unif(self, rng: &mut impl Rng) -> f64 {
        loop {
            let u: f64 = rng.random();
            if u > 0.0 {
                return u;
            }
        }
    }

    fn index(self, rng: &mut impl Rng, n: f64) -> f64 {
        if n <= 1.0 {
            return 0.0;
        }
        // `n` may be non-integral (as in R); the valid indices are 0..ceil(n).
        rng.random_range(0..n.ceil() as u64) as f64
    }
}

// ============================================================================
// Index sampling (R's `sample.int`)
// ============================================================================

/// `k` indices in `[0, n)` drawn with replacement (R: `sample.int(n, k, TRUE) - 1`).
pub fn sample_with_replacement(
    sampler: impl Sampler,
    rng: &mut impl Rng,
    n: usize,
    k: usize,
) -> Vec<usize> {
    let dn = n as f64;
    (0..k).map(|_| sampler.index(rng, dn) as usize).collect()
}

/// `k` distinct indices in `[0, n)`, in draw order
/// (R: `sample.int(n, k) - 1`, including its choice of algorithm: the
/// hash-rejection variant when `n > 1e7 && k <= n/2`, else partial
/// Fisher-Yates by swap-remove).
pub fn sample_without_replacement(
    sampler: impl Sampler,
    rng: &mut impl Rng,
    n: usize,
    k: usize,
) -> Vec<usize> {
    assert!(k <= n, "cannot take a sample larger than the population");
    if n > 10_000_000 && (k as f64) <= n as f64 / 2.0 {
        // do_sample2: draw with replacement, skip duplicates.
        let dn = n as f64;
        let mut seen = HashSet::with_capacity(k);
        let mut out = Vec::with_capacity(k);
        while out.len() < k {
            let v = sampler.index(rng, dn) as usize;
            if seen.insert(v) {
                out.push(v);
            }
        }
        return out;
    }
    if k < 2 {
        // R draws a single sample without allocating the pool.
        return sample_with_replacement(sampler, rng, n, k);
    }
    let mut pool: Vec<usize> = (0..n).collect();
    let mut remaining = n;
    let mut out = Vec::with_capacity(k);
    for _ in 0..k {
        let j = sampler.index(rng, remaining as f64) as usize;
        out.push(pool[j]);
        remaining -= 1;
        pool[j] = pool[remaining];
    }
    out
}

/// A random permutation of `0..n` (R: `sample.int(n) - 1`).
pub fn permutation(sampler: impl Sampler, rng: &mut impl Rng, n: usize) -> Vec<usize> {
    sample_without_replacement(sampler, rng, n, n)
}
