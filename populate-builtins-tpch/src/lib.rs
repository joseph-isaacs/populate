//! TPC-H specific implementations for populate-builtins.
//!
//! This crate provides TPC-H benchmark compatible random number generation
//! and sampling strategies.

use populate_builtins::{PickStrategy, RangeSampler};
use rand::RngCore;

// Re-export common types from populate-builtins for convenience
pub use populate_builtins::{
    PickN, PickNIter, PickOneRef, Range, RangeIter, Seeded, SeededIter, seeded,
};

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
    pub fn next_rand(&mut self) -> i64 {
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

    /// Skip n iterations using O(log n) binary exponentiation.
    ///
    /// For LCG: seed' = seed * multiplier^n mod modulus
    /// This is equivalent to calling next_rand() n times, but in O(log n).
    pub fn skip(&mut self, n: usize) {
        if n == 0 {
            return;
        }
        // Compute multiplier^n mod modulus using binary exponentiation
        let mult_pow_n = Self::mod_pow(Self::MULTIPLIER, n as u64, Self::MODULUS);
        self.seed = (self.seed * mult_pow_n) % Self::MODULUS;
    }

    /// Compute base^exp mod modulus using binary exponentiation.
    fn mod_pow(base: i64, mut exp: u64, modulus: i64) -> i64 {
        let mut result = 1i64;
        let mut base = base % modulus;
        while exp > 0 {
            if exp & 1 == 1 {
                result = (result * base) % modulus;
            }
            exp >>= 1;
            base = (base * base) % modulus;
        }
        result
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

impl RangeSampler<i32> for TpcHRange {
    fn sample_range(rng: &mut impl rand::Rng, min: i32, max: i32) -> i32 {
        let seed = rng.next_u32();
        let range = (max - min) as f64;
        let value = ((seed as f64 / TpchRng::MODULUS as f64) * range) as i32;
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

// === TpchPickN: pick N unique elements with O(log n) seed accounting ===

use populate_core::{CollectColumnar, Distribution, SampleIter, SinkTuple};

/// Pick N unique elements with owned TpchRng and O(log n) seed accounting.
///
/// Unlike `PickNBuffered` which does O(n) RNG calls per row for TPC-H seed
/// accounting, this uses binary exponentiation to skip in O(log n).
///
/// Performance: For 92 colors, this reduces from 92 RNG calls to 5 + O(log 87).
pub struct TpchPickN<'a, T, const N: usize> {
    values: &'a [T],
    rng: TpchRng,
}

impl<'a, T, const N: usize> TpchPickN<'a, T, N> {
    pub fn new(values: &'a [T], rng: TpchRng) -> Self {
        assert!(
            values.len() <= 255,
            "TpchPickN only supports up to 255 elements"
        );
        assert!(N <= values.len(), "N must be <= values.len()");
        Self { values, rng }
    }
}

/// Iterator for TpchPickN with reusable shuffle buffer and owned RNG.
pub struct TpchPickNIter<'a, T, const N: usize> {
    values: &'a [T],
    buffer: Vec<u8>,
    rng: TpchRng,
}

impl<T, const N: usize> Clone for TpchPickNIter<'_, T, N> {
    fn clone(&self) -> Self {
        Self {
            values: self.values,
            buffer: (0..self.values.len() as u8).collect(),
            rng: self.rng.clone(),
        }
    }
}

impl<'a, T: Clone, const N: usize> SampleIter for TpchPickNIter<'a, T, N> {
    type Item = [T; N];

    fn next(&mut self, _rng: &mut impl rand::Rng) -> [T; N] {
        let len = self.values.len();

        // Track swaps for restoration
        let mut swaps = [(0u8, 0u8); N];

        // Only N RNG calls for actual picks
        #[allow(clippy::needless_range_loop)]
        for i in 0..N {
            let seed = self.rng.next_rand();
            let range = (len - i) as f64;
            let j = i + ((seed as f64 / TpchRng::MODULUS as f64) * range) as usize;
            swaps[i] = (i as u8, j as u8);
            self.buffer.swap(i, j);
        }

        // O(log n) skip for seed accounting instead of O(n) linear calls
        self.rng.skip(len - N);

        // Collect results
        let result = std::array::from_fn(|i| self.values[self.buffer[i] as usize].clone());

        // Restore swapped elements
        for i in (0..N).rev() {
            let (a, b) = swaps[i];
            self.buffer.swap(a as usize, b as usize);
        }

        result
    }
}

impl<'a, T: Clone, const N: usize, S: SinkTuple<[T; N]>> CollectColumnar<S>
    for TpchPickNIter<'a, T, N>
{
    const IS_COLUMNAR: bool = true;
    type Output = S::Output;

    fn collect_columnar(mut self, rng: &mut impl rand::Rng, n: usize, mut sinks: S) -> Self::Output {
        for _ in 0..n {
            sinks.push_row(self.next(rng));
        }
        sinks.finish()
    }
}

impl<'a, T: Clone, const N: usize> Distribution<[T; N]> for TpchPickN<'a, T, N> {
    type Iter = TpchPickNIter<'a, T, N>;

    fn sample(&mut self) -> Self::Iter {
        TpchPickNIter {
            values: self.values,
            buffer: (0..self.values.len() as u8).collect(),
            rng: self.rng.clone(),
        }
    }
}

// === PickNBuffered: pick N unique elements with reusable buffer (legacy) ===

/// Pick N unique elements from a slice, reusing the shuffle buffer across calls.
/// Uses TPC-H's Fisher-Yates with seed accounting.
///
/// NOTE: This does O(n) RNG calls per row for seed accounting.
/// For better performance with TpchRng, use `TpchPickN` instead.
///
/// Optimizations:
/// - Uses u8 indices (max 255 elements) to reduce memory bandwidth
/// - Restores only swapped elements instead of resetting full buffer
pub struct PickNBuffered<'a, T, const N: usize> {
    values: &'a [T],
}

impl<'a, T, const N: usize> PickNBuffered<'a, T, N> {
    pub fn new(values: &'a [T]) -> Self {
        assert!(
            values.len() <= 255,
            "PickNBuffered only supports up to 255 elements"
        );
        Self { values }
    }
}

/// Iterator that holds a reusable shuffle buffer with u8 indices.
pub struct PickNBufferedIter<'a, T, const N: usize> {
    values: &'a [T],
    buffer: Vec<u8>,
}

impl<T, const N: usize> Clone for PickNBufferedIter<'_, T, N> {
    fn clone(&self) -> Self {
        // Clone resets to fresh buffer (per SampleIter contract)
        Self {
            values: self.values,
            buffer: (0..self.values.len() as u8).collect(),
        }
    }
}

impl<'a, T: Clone, const N: usize> SampleIter for PickNBufferedIter<'a, T, N> {
    type Item = [T; N];

    fn next(&mut self, rng: &mut impl rand::Rng) -> [T; N] {
        let len = self.values.len();

        // Track swaps so we can restore instead of full reset
        let mut swaps = [(0u8, 0u8); N];

        // Partial Fisher-Yates shuffle using TPC-H formula
        #[allow(clippy::needless_range_loop)]
        for i in 0..N {
            let seed = rng.next_u64();
            let range = (len - i) as f64;
            let j = i + ((seed as f64 / TpchRng::MODULUS as f64) * range) as usize;
            swaps[i] = (i as u8, j as u8);
            self.buffer.swap(i, j);
        }

        // Advance RNG by remaining seeds (TPC-H seeds_per_row accounting)
        for _ in N..len {
            let _ = rng.next_u64();
        }

        // Collect results before restoring
        let result = std::array::from_fn(|i| self.values[self.buffer[i] as usize].clone());

        // Restore swapped elements in reverse order
        for i in (0..N).rev() {
            let (a, b) = swaps[i];
            self.buffer.swap(a as usize, b as usize);
        }

        result
    }
}

impl<'a, T: Clone, const N: usize, S: SinkTuple<[T; N]>> CollectColumnar<S>
    for PickNBufferedIter<'a, T, N>
{
    const IS_COLUMNAR: bool = true;
    type Output = S::Output;

    fn collect_columnar(mut self, rng: &mut impl rand::Rng, n: usize, mut sinks: S) -> Self::Output {
        for _ in 0..n {
            sinks.push_row(self.next(rng));
        }
        sinks.finish()
    }
}

impl<'a, T: Clone, const N: usize> Distribution<[T; N]> for PickNBuffered<'a, T, N> {
    type Iter = PickNBufferedIter<'a, T, N>;

    fn sample(&mut self) -> Self::Iter {
        PickNBufferedIter {
            values: self.values,
            buffer: (0..self.values.len() as u8).collect(),
        }
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
