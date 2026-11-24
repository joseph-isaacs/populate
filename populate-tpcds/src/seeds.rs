//! Fast end-of-row seed skipping for tpcdsgen's random streams.
//!
//! dsdgen gives every column a fixed budget of random draws per source row
//! and, at the end of each row, burns the unused ones so the next row starts
//! at a fixed position. tpcdsgen does this the same way, one draw at a time
//! (`consume_remaining_seeds_for_row`): ~100 wasted Lehmer steps per
//! `store_sales` ticket. The stream is a Lehmer generator
//! (`seed' = 16807 * seed mod (2^31 - 1)`), so `k` steps are one
//! multiplication by `16807^k`. [`finish_row`] does that instead: one draw
//! to read the current seed, one multiply, and a fresh stream starting at
//! the result. The streams' values are unchanged.

use tpcdsgen::generator::GeneratorColumn;
use tpcdsgen::random::RandomNumberStream;
use tpcdsgen::row::AbstractRowGenerator;

const M: u64 = (1 << 31) - 1;
const A: u64 = 16807;
/// tpcdsgen's per-column seed spacing: `initial = base + column * STRIDE`.
const STRIDE: i64 = i32::MAX as i64 / 799;
/// Largest per-row seed budget of any TPC-DS column.
const MAX_SEEDS: usize = 256;

#[inline]
fn mulmod(a: u64, b: u64) -> u64 {
    let p = a * b;
    let x = (p & M) + (p >> 31);
    if x >= M { x - M } else { x }
}

/// One Lehmer step: the stream's next draw after `seed`.
#[inline]
pub(crate) fn lehmer(seed: u64) -> u64 {
    mulmod(seed, A)
}

/// Two Lehmer steps.
#[inline]
pub(crate) fn lehmer2(seed: u64) -> u64 {
    mulmod(seed, A * A % M)
}

/// `A^k mod M` for `k < MAX_SEEDS`.
static POW: [u64; MAX_SEEDS] = {
    let mut p = [1u64; MAX_SEEDS];
    let mut k = 1;
    while k < MAX_SEEDS {
        p[k] = (p[k - 1] * A) % M;
        k += 1;
    }
    p
};

/// Burn every stream's unused draws for this row, as
/// `AbstractRowGenerator::consume_remaining_seeds_for_row` does.
pub(crate) fn finish_row<C: GeneratorColumn>(g: &mut AbstractRowGenerator<C>) {
    for c in C::values() {
        let s = g.get_random_number_stream(c);
        let (used, per) = (s.get_seeds_used(), s.get_seeds_per_row());
        if used >= per {
            s.reset_seeds_used();
            continue;
        }
        let left = (per - used) as usize;
        if left == 1 || left >= MAX_SEEDS {
            // one draw, or a budget too big for the table: the slow way
            for _ in 0..left {
                s.next_random();
            }
            s.reset_seeds_used();
            continue;
        }
        let seed = mulmod(s.next_random() as u64, POW[left - 1]) as i64;
        let col = c.get_global_column_number();
        // a stream whose initial seed is `seed`: base = seed - col * STRIDE
        let base = seed - col as i64 * STRIDE;
        *s = RandomNumberStream::new_with_base(col, base as i32, per);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tpcdsgen::generator::StoreSalesGeneratorColumn as S;

    #[test]
    fn same_streams_as_consuming_draws() {
        let mut fast = AbstractRowGenerator::<S>::new(100);
        let mut slow = AbstractRowGenerator::<S>::new(100);
        for row in 0..50 {
            for (i, c) in S::values().iter().enumerate() {
                // use a varying number of draws per column and row
                for _ in 0..(row * 7 + i) % 20 {
                    fast.get_random_number_stream(c).next_random();
                    slow.get_random_number_stream(c).next_random();
                }
            }
            finish_row(&mut fast);
            slow.consume_remaining_seeds_for_row();
            for c in S::values() {
                let (f, s) = (
                    fast.get_random_number_stream(c),
                    slow.get_random_number_stream(c),
                );
                assert_eq!(f.get_seeds_used(), s.get_seeds_used());
                assert_eq!(
                    f.clone().next_random(),
                    s.clone().next_random(),
                    "row {row}"
                );
            }
        }
    }
}
