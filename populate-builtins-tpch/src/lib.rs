//! TPC-H specific implementations for populate-builtins.
//!
//! This crate provides TPC-H benchmark compatible random number generation
//! and sampling strategies.

use populate_builtins::{PickStrategy, RangeSampler};
use rand::RngCore;

// Re-export common types from populate-builtins for convenience
pub use populate_builtins::{PickN, PickNIter, Range, RangeIter, Seeded, SeededIter, seeded};

// === TpchRng: TPC-H Linear Congruential Generator ===

/// TPC-H specification Linear Congruential Generator (MINSTD).
///
/// This implements the exact RNG algorithm used by the TPC-H benchmark spec:
/// - Multiplier: 16807
/// - Modulus: 2^31 - 1 (2147483647)
/// - Formula: seed = (seed * 16807) % 2147483647
#[derive(Clone, Debug)]
pub struct TpchRng {
    seed: i64,
}

impl TpchRng {
    pub const MULTIPLIER: i64 = 16807;
    pub const MODULUS: i64 = 2147483647;

    pub fn new(seed: i64) -> Self {
        Self { seed }
    }

    /// Advance the seed and return the new value.
    fn next_rand(&mut self) -> i64 {
        self.seed = (self.seed * Self::MULTIPLIER) % Self::MODULUS;
        self.seed
    }

    /// Generate a bounded integer using the TPC-H formula.
    ///
    /// This advances the seed once, then computes:
    /// `floor((seed / MODULUS) * range) + min`
    pub fn bounded_int(&mut self, min: i64, max: i64) -> i64 {
        let seed = self.next_rand();
        let range = (max - min).wrapping_add(1) as f64;
        let value = ((seed as f64 / Self::MODULUS as f64) * range) as i64;
        min + value
    }
}

impl RngCore for TpchRng {
    fn next_u32(&mut self) -> u32 {
        self.next_rand() as u32
    }

    fn next_u64(&mut self) -> u64 {
        // Return the raw LCG value - TpcHRange will handle scaling
        self.next_rand() as u64
    }

    fn fill_bytes(&mut self, dest: &mut [u8]) {
        rand::Fill::fill(dest, self)
    }
}

// === TpcHRange: RangeSampler for TPC-H ===

/// TPC-H range sampling strategy.
/// Uses the TPC-H formula: value = floor((seed / MODULUS) * range) + min
#[derive(Clone, Copy, Default)]
pub struct TpcHRange;

impl RangeSampler<i64> for TpcHRange {
    fn sample_range(rng: &mut impl rand::Rng, min: i64, max: i64) -> i64 {
        let seed = rng.next_u64();
        let range = (max - min) as f64;
        let value = ((seed as f64 / TpchRng::MODULUS as f64) * range) as i64;
        min + value
    }
}

// === TpchUnique: PickStrategy for TPC-H ===

/// TPC-H unique picking strategy with seed accounting.
/// Uses partial Fisher-Yates shuffle with TPC-H formula, then advances
/// RNG by remaining seeds to match TPC-H's seeds_per_row behavior.
#[derive(Clone, Copy, Default)]
pub struct TpchUnique;

impl PickStrategy for TpchUnique {
    fn pick_indices<const N: usize>(rng: &mut impl rand::Rng, len: usize) -> [usize; N] {
        let mut indices: Vec<usize> = (0..len).collect();

        // Partial Fisher-Yates shuffle using TPC-H formula
        for i in 0..N {
            let seed = rng.next_u64();
            let range = (len - i) as f64;
            let j = i + ((seed as f64 / TpchRng::MODULUS as f64) * range) as usize;
            indices.swap(i, j);
        }

        // Advance RNG by remaining seeds (TPC-H seeds_per_row accounting)
        for _ in N..len {
            let _ = rng.next_u64();
        }

        std::array::from_fn(|i| indices[i])
    }
}

// === PickNBuffered: pick N unique elements with reusable buffer ===

use populate_core::{Distribution, SampleIter};

/// Pick N unique elements from a slice, reusing the shuffle buffer across calls.
/// Uses TPC-H's Fisher-Yates with seed accounting.
pub struct PickNBuffered<'a, T, const N: usize> {
    values: &'a [T],
}

impl<'a, T, const N: usize> PickNBuffered<'a, T, N> {
    pub fn new(values: &'a [T]) -> Self {
        Self { values }
    }
}

/// Iterator that holds a reusable shuffle buffer.
pub struct PickNBufferedIter<'a, T, const N: usize> {
    values: &'a [T],
    buffer: Vec<usize>,
}

impl<T, const N: usize> Clone for PickNBufferedIter<'_, T, N> {
    fn clone(&self) -> Self {
        // Clone resets to fresh buffer (per SampleIter contract)
        Self {
            values: self.values,
            buffer: (0..self.values.len()).collect(),
        }
    }
}

impl<'a, T: Clone, const N: usize> SampleIter for PickNBufferedIter<'a, T, N> {
    type Item = [T; N];

    fn next(&mut self, rng: &mut impl rand::Rng) -> [T; N] {
        let len = self.values.len();

        // Reset buffer in place
        for (i, slot) in self.buffer.iter_mut().enumerate() {
            *slot = i;
        }

        // Partial Fisher-Yates shuffle using TPC-H formula
        for i in 0..N {
            let seed = rng.next_u64();
            let range = (len - i) as f64;
            let j = i + ((seed as f64 / TpchRng::MODULUS as f64) * range) as usize;
            self.buffer.swap(i, j);
        }

        // Advance RNG by remaining seeds (TPC-H seeds_per_row accounting)
        for _ in N..len {
            let _ = rng.next_u64();
        }

        std::array::from_fn(|i| self.values[self.buffer[i]].clone())
    }
}

impl<'a, T: Clone, const N: usize> Distribution<[T; N]> for PickNBuffered<'a, T, N> {
    type Iter = PickNBufferedIter<'a, T, N>;
    type Sinks = Vec<[T; N]>;

    fn sample(&mut self) -> Self::Iter {
        PickNBufferedIter {
            values: self.values,
            buffer: (0..self.values.len()).collect(),
        }
    }

    fn collect_columns(&mut self, rng: &mut impl rand::Rng, n: usize) -> Self::Sinks {
        let mut iter = self.sample();
        (0..n).map(|_| iter.next(rng)).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use populate_builtins::seeded;
    use populate_core::{Distribution, SampleIter};
    use rand::SeedableRng;

    #[test]
    fn test_tpch_rng_sequence() {
        let mut rng = TpchRng::new(1193163244);
        let v1 = rng.bounded_int(1, 50);
        assert_eq!(v1, 7, "First p_size value should be 7");
    }

    #[test]
    fn test_tpch_rng_deterministic() {
        let mut rng1 = TpchRng::new(12345);
        let mut rng2 = TpchRng::new(12345);

        for _ in 0..100 {
            assert_eq!(rng1.bounded_int(1, 100), rng2.bounded_int(1, 100));
        }
    }

    #[test]
    fn test_tpch_range() {
        let mut dist = Range::<_, TpcHRange>::with_sampler(1, 51);
        let mut rng = TpchRng::new(1193163244);
        let mut iter = dist.sample();

        let v1 = iter.next(&mut rng);
        assert_eq!(v1, 7, "First p_size value should be 7");
    }

    #[test]
    fn test_tpch_unique() {
        const COLORS: [&str; 5] = ["red", "green", "blue", "yellow", "purple"];

        let mut dist = seeded(
            TpchRng::new(12345),
            PickN::<_, TpchUnique, 5>::with_strategy(&COLORS),
        );
        let mut rng = rand::rngs::StdRng::seed_from_u64(0); // ignored due to seeded
        let mut iter = dist.sample();

        let picked = iter.next(&mut rng);
        // All 5 should be unique
        let mut seen = std::collections::HashSet::new();
        for color in picked {
            assert!(COLORS.contains(&color));
            assert!(seen.insert(color), "Duplicate found: {}", color);
        }
    }
}
