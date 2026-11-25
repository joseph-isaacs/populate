//! Randomness: stateless per-row streams, and random-string fills.

use populate_table::Stream;

/// SplitMix64's increment.
pub(crate) const GOLDEN_GAMMA: u64 = 0x9e37_79b9_7f4a_7c15;

/// SplitMix64's finaliser (Stafford's mix 13).
#[inline(always)]
pub(crate) fn mix(mut z: u64) -> u64 {
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// The draws of one row of one column: a SplitMix64 sequence started at
/// `stream.at(row)`, so any row's draws are available without generating
/// the rows before it.
#[derive(Clone, Copy)]
pub(crate) struct Draws(u64);

impl Draws {
    #[inline(always)]
    pub(crate) fn new(stream: Stream, row: u64) -> Self {
        Draws(stream.at(row))
    }

    #[inline(always)]
    pub(crate) fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(GOLDEN_GAMMA);
        mix(self.0)
    }

    /// Uniform in `lo..=hi` (Lemire's multiply-shift, bias below 2^-40 for
    /// TPC-C's ranges).
    #[inline(always)]
    pub(crate) fn int(&mut self, lo: i64, hi: i64) -> i64 {
        below(self.next(), (hi - lo + 1) as u64) as i64 + lo
    }
}

/// `x` mapped to `0..n`.
#[inline(always)]
pub(crate) fn below(x: u64, n: u64) -> u64 {
    ((x as u128 * n as u128) >> 64) as u64
}

/// TPC-C's `NURand(A, x, y)` (clause 2.1.6) with run-time constant `c`,
/// from two draws.
#[inline(always)]
pub(crate) fn nurand(d: &mut Draws, a: i64, x: i64, y: i64, c: i64) -> i64 {
    let r1 = d.int(0, a);
    let r2 = d.int(x, y);
    (((r1 | r2) + c) % (y - x + 1)) + x
}

/// A character set for random strings.
#[derive(Clone, Copy)]
pub(crate) struct Charset(&'static [u8]);

/// TPC-C's a-strings: letters and digits.
pub(crate) const ALNUM: Charset =
    Charset(b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789");
/// Upper-case letters (states).
pub(crate) const LETTERS: Charset = Charset(b"ABCDEFGHIJKLMNOPQRSTUVWXYZ");
/// n-strings: digits.
pub(crate) const DIGITS: Charset = Charset(b"0123456789");

impl Charset {
    /// Fill `out` with random characters, 8 per draw: each draw is used as
    /// a fixed-point fraction, multiplied out one character at a time
    /// (the 8th character still has 16+ bits of the draw behind it).
    #[inline(always)]
    pub(crate) fn fill(self, d: &mut Draws, out: &mut [u8]) {
        let n = self.0.len() as u128;
        for chunk in out.chunks_mut(8) {
            let mut x = d.next();
            for b in chunk {
                let m = x as u128 * n;
                *b = self.0[(m >> 64) as usize];
                x = m as u64;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn charset_fill_is_uniform() {
        let mut d = Draws::new(Stream::new(1, "t"), 0);
        let mut counts = [0u32; 62];
        let mut buf = [0u8; 64];
        for _ in 0..10_000 {
            ALNUM.fill(&mut d, &mut buf);
            for b in buf {
                let i = ALNUM.0.iter().position(|&c| c == b).expect("in set");
                counts[i] += 1;
            }
        }
        // 640k draws over 62 symbols: expect ~10,323 each (sd ~100)
        assert!(
            counts.iter().all(|&c| (9_800..10_850).contains(&c)),
            "{counts:?}"
        );
    }
}
