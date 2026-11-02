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

/// Draws within one row.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Rand {
    pub seed: i64,
}

impl Rand {
    #[inline(always)]
    pub fn next_rand(&mut self) -> i64 {
        self.seed = (self.seed * MULTIPLIER) % MODULUS;
        self.seed
    }

    /// Uniform int in `[lo, hi]`. Same float arithmetic as dbgen, including the
    /// wrapping range for `hi = i32::MAX` used by alphanumeric strings.
    #[inline(always)]
    pub fn next_int(&mut self, lo: i32, hi: i32) -> i32 {
        self.next_rand();
        let range = (hi - lo).wrapping_add(1) as f64;
        lo + ((self.seed as f64 / MODULUS as f64) * range) as i32
    }
}

/// A column's stream, positioned at a row boundary. `group` items share one
/// TPC-H row (partsupp emits 4 rows per part, all drawing from the part's
/// block); a new block starts every `group` items.
#[derive(Clone, Copy, Debug)]
pub struct Stream {
    row_seed: i64,
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
        Self {
            row_seed: seed * pow_mod(seeds_per_row as i64 * start_row) % MODULUS,
            jump: pow_mod(seeds_per_row as i64),
            cur: Rand { seed: 0 },
            k: 0,
            group,
        }
    }

    pub fn ungrouped(seed: i64, seeds_per_row: i32, start: i64) -> Self {
        Self::new(seed, seeds_per_row, 1, start)
    }

    /// The draw state for the next item.
    #[inline(always)]
    pub fn next_item(&mut self) -> &mut Rand {
        if self.k == 0 {
            self.cur = Rand {
                seed: self.row_seed,
            };
            self.row_seed = self.row_seed * self.jump % MODULUS;
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
    fn start_offset_matches_advance_rows() {
        let mut theirs = RowRandomInt::new(1335826707, 2);
        theirs.advance_rows(12345);
        let mut ours = Stream::ungrouped(1335826707, 2, 12345);
        let r = ours.next_item();
        assert_eq!(r.next_int(0, 99), theirs.next_int(0, 99));
        assert_eq!(r.next_int(0, i32::MAX), theirs.next_int(0, i32::MAX));
    }
}
