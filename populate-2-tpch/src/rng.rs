//! TPC-H random streams, bit-exact with dbgen / `tpchgen`.
//!
//! Every TPC-H column has its own Park–Miller stream
//! (`seed = seed * 16807 mod 2^31-1`) and reserves a fixed number of draws per
//! row (`seeds_per_row`). After a row, the stream jumps to the start of the
//! next row's block whether or not it used all of its draws.
//!
//! `tpchgen` tracks that with a usage counter and a square-and-multiply skip
//! on every row (`row_finished`). Here a column keeps only the *row start*
//! seed and multiplies it by the precomputed constant `16807^seeds_per_row`
//! once per row. Same numbers, one multiply instead of a loop.

pub const MULTIPLIER: i64 = 16807;
pub const MODULUS: i64 = 2147483647;

/// `x mod (2^31 - 1)` for `0 <= x < 2^62`, using `2^31 ≡ 1`: fold the high
/// bits onto the low bits instead of a multiply-high division sequence.
#[inline(always)]
pub fn mod_m(x: i64) -> i64 {
    const M: u64 = MODULUS as u64;
    let x = x as u64;
    let s = (x & M) + (x >> 31); // < 2^32
    let s = (s & M) + (s >> 31); // <= 2^31
    (if s >= M { s - M } else { s }) as i64
}

/// `16807^count mod (2^31 - 1)`.
pub fn pow_mod(mut count: i64) -> i64 {
    let mut result = 1;
    let mut multiplier = MULTIPLIER;
    while count > 0 {
        if count % 2 != 0 {
            result = (result * multiplier) % MODULUS;
        }
        count /= 2;
        multiplier = (multiplier * multiplier) % MODULUS;
    }
    result
}

/// `16807^k mod (2^31 - 1)` for `k` in `0..32`, at compile time.
const POW: [i64; 32] = {
    let mut t = [1i64; 32];
    let mut k = 1;
    while k < 32 {
        t[k] = t[k - 1] * MULTIPLIER % MODULUS;
        k += 1;
    }
    t
};

/// Uniform int in `[lo, hi]` from an already-advanced seed (dbgen's scaling).
#[inline(always)]
pub fn scale(seed: i64, lo: i32, hi: i32) -> i32 {
    let range = (hi - lo).wrapping_add(1) as f64;
    let scaled = (seed as f64 / MODULUS as f64) * range;
    // SAFETY: 0 <= seed/M < 1 and |range| <= 2^31, so `scaled` is finite
    // and |scaled| < 2^31: in i32 range, where `as` would not saturate.
    // Skips the saturating-cast checks (`as i32` emits ~5 extra ops).
    lo + unsafe { scaled.to_int_unchecked::<i32>() }
}

/// Draws within one row.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Rand {
    pub seed: i64,
}

impl Rand {
    #[inline(always)]
    pub fn next_rand(&mut self) -> i64 {
        self.seed = mod_m(self.seed * MULTIPLIER);
        self.seed
    }

    /// Uniform int in `[lo, hi]`. Same float arithmetic as dbgen, including the
    /// wrapping range for `hi = i32::MAX` used by alphanumeric strings.
    #[inline(always)]
    pub fn next_int(&mut self, lo: i32, hi: i32) -> i32 {
        self.next_rand();
        scale(self.seed, lo, hi)
    }

    /// The next `N` seeds, computed independently from the current one
    /// (`seed * 16807^k`) instead of as a serial chain; advances by `N`.
    /// Feed each to [`scale`]. Same values as `N` calls to `next_rand`.
    #[inline(always)]
    pub fn ahead<const N: usize>(&mut self) -> [i64; N] {
        const { assert!(N < 32) };
        let base = self.seed;
        let seeds = std::array::from_fn(|k| mod_m(base * POW[k + 1]));
        self.seed = seeds[N - 1];
        seeds
    }
}

/// Row lanes kept in flight. Row `r`'s start seed is row `r-1`'s times
/// `J = 16807^seeds_per_row`: one long serial multiply chain. Keeping four
/// consecutive row seeds and advancing each by `J^4` gives four independent
/// chains the CPU can overlap.
const LANES: usize = 4;

/// A column's stream, positioned at a row boundary. `group` items share one
/// TPC-H row (partsupp emits 4 rows per part, all drawing from the part's
/// block); a new block starts every `group` items.
#[derive(Clone, Copy, Debug)]
pub struct Stream {
    lanes: [i64; LANES],
    jump: i64,
    cur: Rand,
    k: u32,
    group: u32,
}

impl Stream {
    /// `seeds_per_row` is per TPC-H row (per group). `start_item` must be a
    /// multiple of `group`.
    pub fn new(seed: i64, seeds_per_row: i32, group: u32, start_item: i64) -> Self {
        assert_eq!(
            start_item % group as i64,
            0,
            "start must align to the row group"
        );
        let start_row = start_item / group as i64;
        let row_jump = pow_mod(seeds_per_row as i64);
        let mut lanes = [0; LANES];
        lanes[0] = mod_m(seed * pow_mod(seeds_per_row as i64 * start_row));
        for i in 1..LANES {
            lanes[i] = mod_m(lanes[i - 1] * row_jump);
        }
        Self {
            lanes,
            jump: pow_mod(seeds_per_row as i64 * LANES as i64),
            cur: Rand { seed: 0 },
            k: 0,
            group,
        }
    }

    pub fn ungrouped(seed: i64, seeds_per_row: i32, start: i64) -> Self {
        Self::new(seed, seeds_per_row, 1, start)
    }

    /// The next row's start seed (rotates the lanes).
    #[inline(always)]
    fn next_row(&mut self) -> i64 {
        let [s, a, b, c] = self.lanes;
        self.lanes = [a, b, c, mod_m(s * self.jump)];
        s
    }

    /// The draw state for the next item.
    #[inline(always)]
    pub fn next_item(&mut self) -> &mut Rand {
        if self.k == 0 {
            self.cur = Rand {
                seed: self.next_row(),
            };
        }
        self.k += 1;
        if self.k == self.group {
            self.k = 0;
        }
        &mut self.cur
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tpchgen::random::RowRandomInt;

    /// Our row jump == tpchgen's usage counting + row_finished, for rows that
    /// use a varying number of their reserved draws.
    #[test]
    fn row_jump_matches_tpchgen() {
        let mut theirs = RowRandomInt::new(709314158, 7);
        let mut ours = Stream::ungrouped(709314158, 7, 0);
        for row in 0..1000 {
            let r = ours.next_item();
            for _ in 0..(row % 8).min(7) {
                assert_eq!(r.next_int(0, 1000), theirs.next_int(0, 1000));
            }
            theirs.row_finished();
        }
    }

    #[test]
    fn ahead_matches_sequential() {
        let mut a = Rand { seed: 709314158 };
        let mut b = a;
        for _ in 0..1000 {
            let seeds = a.ahead::<7>();
            for s in seeds {
                assert_eq!(s, b.next_rand());
            }
            assert_eq!(a, b);
        }
    }

    #[test]
    fn mod_m_matches_remainder() {
        let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
        let edge = [
            0,
            1,
            MODULUS - 1,
            MODULUS,
            MODULUS + 1,
            2 * MODULUS,
            (1 << 62) - 1,
        ];
        for v in edge {
            assert_eq!(mod_m(v), v % MODULUS, "{v}");
        }
        for _ in 0..10_000_000 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let v = (x >> 2) as i64; // [0, 2^62)
            assert_eq!(mod_m(v), v % MODULUS, "{v}");
        }
    }

    #[test]
    fn start_offset_matches_advance_rows() {
        let mut theirs = RowRandomInt::new(1335826707, 2);
        theirs.advance_rows(12345);
        let mut ours = Stream::ungrouped(1335826707, 2, 12345);
        let r = ours.next_item();
        assert_eq!(r.next_int(0, 99), theirs.next_int(0, 99));
        assert_eq!(r.next_int(0, i32::MAX), theirs.next_int(0, i32::MAX));
    }
}
