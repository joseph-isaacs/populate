//! Stateless per-row randomness.
//!
//! A [`Stream`] maps a row number to a random-looking `u64` with a fixed
//! hash (SplitMix64's finaliser), so a value depends only on `(stream, row)`.
//! That makes it the tool for columns that must agree *across tables* or
//! *across batches*: a return row can recompute the exact item, customer and
//! date of the sale it refers to, without the two tables sharing any state or
//! being generated in the same order. It is also branch-free and has no
//! loop-carried state, so a `(start..end).map(|r| s.below(r, n))` loop
//! vectorises.
//!
//! For ordinary independent columns use a populate distribution instead
//! (`Table::col`); those draw from the per-batch RNG.

pub use populate_core::chunks::mix64;

/// FNV-1a, for turning names into seeds.
pub const fn hash_str(s: &str) -> u64 {
    fnv1a64(s.as_bytes())
}

/// FNV-1a, 64-bit: the hash the generators' tests check output bytes
/// against.
pub const fn fnv1a64(b: &[u8]) -> u64 {
    let mut h = 0xcbf2_9ce4_8422_2325u64;
    let mut i = 0;
    while i < b.len() {
        h ^= b[i] as u64;
        h = h.wrapping_mul(0x0100_0000_01b3);
        i += 1;
    }
    h
}

/// A named, seeded source of per-row random values. `Copy`, 8 bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Stream(u64);

impl Stream {
    /// Stream identified by `name` under `seed`. Same `(seed, name)`, same
    /// values, in any table.
    pub const fn new(seed: u64, name: &str) -> Self {
        Stream(mix64(seed ^ hash_str(name)))
    }

    /// An independent child stream (e.g. one per list element).
    pub const fn sub(self, i: u64) -> Self {
        Stream(mix64(self.0 ^ mix64(i)))
    }

    /// Random `u64` for `row`.
    #[inline]
    pub const fn at(self, row: u64) -> u64 {
        mix64(self.0 ^ row.wrapping_mul(0xD6E8_FEB8_6659_FD93))
    }

    /// Uniform in `0..n` (Lemire's multiply-shift; `n > 0`).
    #[inline]
    pub const fn below(self, row: u64, n: u64) -> u64 {
        ((self.at(row) as u128 * n as u128) >> 64) as u64
    }

    /// Uniform in `lo..=hi`.
    #[inline]
    pub const fn range(self, row: u64, lo: i64, hi: i64) -> i64 {
        lo + self.below(row, (hi - lo) as u64 + 1) as i64
    }

    /// Uniform pick from a non-empty slice.
    #[inline]
    pub fn pick<T: Copy>(self, row: u64, values: &[T]) -> T {
        values[self.below(row, values.len() as u64) as usize]
    }

    /// `true` with probability `p`.
    #[inline]
    pub fn chance(self, row: u64, p: f64) -> bool {
        (self.at(row) >> 11) as f64 * (1.0 / (1u64 << 53) as f64) < p
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_and_in_range() {
        let s = Stream::new(1, "x");
        assert_eq!(s.at(5), Stream::new(1, "x").at(5));
        assert_ne!(s.at(5), Stream::new(2, "x").at(5));
        assert_ne!(s.at(5), Stream::new(1, "y").at(5));
        for r in 0..10_000 {
            assert!((3..=9).contains(&s.range(r, 3, 9)));
            assert!(s.below(r, 7) < 7);
        }
    }

    #[test]
    fn roughly_uniform() {
        let s = Stream::new(7, "u");
        let mut counts = [0u32; 10];
        for r in 0..100_000 {
            counts[s.below(r, 10) as usize] += 1;
        }
        assert!(
            counts.iter().all(|&c| (9_000..11_000).contains(&c)),
            "{counts:?}"
        );
        let hits = (0..100_000).filter(|&r| s.chance(r, 0.25)).count();
        assert!((24_000..26_000).contains(&hits), "{hits}");
    }
}
