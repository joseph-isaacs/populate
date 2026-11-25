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

use populate_core::Distribution;
use rand::Rng;
use std::array::from_fn;
use std::marker::PhantomData;
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

/// tpcdsgen's default seed base: column `c`'s stream starts at
/// `SEED_BASE + c * STRIDE` (not reduced mod `M`).
const SEED_BASE: i64 = 19_620_718;

/// `A^n mod M`.
fn pow(mut n: u64) -> u64 {
    let (mut r, mut a) = (1, A);
    while n > 0 {
        if n & 1 == 1 {
            r = r * a % M;
        }
        a = a * a % M;
        n >>= 1;
    }
    r
}

/// Where every stream of a table's generator columns `C` (`N` of them, in
/// `C::values()` order) stands at the start of each source row: the seed
/// its next draw is made from.
///
/// dsdgen reserves a fixed number of draws per row for every column, so
/// these are a function of the row number alone, and a row's draws are
/// made from [`Streams`] started at them. Skipping `n` rows is one
/// multiplication per stream.
#[derive(Debug)]
pub(crate) struct RowSeeds<C, const N: usize> {
    seeds: [u64; N],
    /// `A^per_row` of each stream (`1` for a stream that draws nothing).
    step: [u64; N],
    per_row: [u64; N],
    _c: PhantomData<fn() -> C>,
}

impl<C, const N: usize> Clone for RowSeeds<C, N> {
    fn clone(&self) -> Self {
        RowSeeds { ..*self }
    }
}

impl<C: GeneratorColumn, const N: usize> RowSeeds<C, N> {
    /// The streams at source row 1.
    pub(crate) fn new() -> Self {
        let cols = C::values();
        assert_eq!(cols.len(), N, "one seed per generator column");
        let per_row = from_fn(|i| cols[i].get_seeds_per_row() as u64);
        RowSeeds {
            seeds: from_fn(|i| {
                (SEED_BASE + cols[i].get_global_column_number() as i64 * STRIDE) as u64
            }),
            step: per_row.map(pow),
            per_row,
            _c: PhantomData,
        }
    }
}

impl<C: GeneratorColumn, const N: usize> Distribution for RowSeeds<C, N> {
    type Item = [u32; N];

    #[inline]
    fn sample(&mut self, _rng: &mut impl Rng) -> [u32; N] {
        let row = self.seeds.map(|s| s as u32);
        for (s, &a) in self.seeds.iter_mut().zip(&self.step) {
            // a stream that never draws keeps its unreduced initial seed,
            // as tpcdsgen's does
            if a != 1 {
                *s = *s * a % M;
            }
        }
        row
    }

    fn skip(&mut self, n: u64) {
        for (s, &per) in self.seeds.iter_mut().zip(&self.per_row) {
            if per > 0 && n > 0 {
                *s = *s % M * pow(per * n) % M;
            }
        }
    }
}

/// A row's streams of generator columns `C`, started at their
/// [`RowSeeds`]: what `AbstractRowGenerator::get_random_number_stream`
/// gives for that row, without the generator.
pub(crate) struct Streams<C, const N: usize> {
    streams: [RandomNumberStream; N],
    base: i32,
    _c: PhantomData<fn() -> C>,
}

impl<C: GeneratorColumn, const N: usize> Streams<C, N> {
    #[inline(always)]
    pub(crate) fn at(seeds: &[u32; N]) -> Self {
        let cols = C::values();
        let base = cols[0].get_global_column_number();
        Streams {
            streams: from_fn(|i| {
                let col = base + i as i32;
                // a stream whose initial seed is `seeds[i]`
                let b = seeds[i] as i64 - col as i64 * STRIDE;
                RandomNumberStream::new_with_base(col, b as i32, cols[i].get_seeds_per_row())
            }),
            base,
            _c: PhantomData,
        }
    }

    #[inline(always)]
    pub(crate) fn get(&mut self, c: &C) -> &mut RandomNumberStream {
        &mut self.streams[(c.get_global_column_number() - self.base) as usize]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tpcdsgen::generator::StoreSalesGeneratorColumn as S;

    #[test]
    fn row_seeds_match_the_generator() {
        let mut g = AbstractRowGenerator::<S>::new(1000);
        let mut seeds = RowSeeds::<S, 26>::new();
        let mut rng = rand::rng();
        let mut skipped = RowSeeds::<S, 26>::new();
        skipped.skip(37);
        for row in 0..50 {
            let at = seeds.sample(&mut rng);
            if row == 37 {
                assert_eq!(skipped.sample(&mut rng), at);
            }
            let mut st = Streams::<S, 26>::at(&at);
            for c in S::values() {
                // within the row's budget, as dsdgen's columns stay
                for _ in 0..c.get_seeds_per_row().min(3) {
                    assert_eq!(
                        st.get(c).next_random(),
                        g.get_random_number_stream(c).next_random(),
                        "row {row}"
                    );
                }
            }
            g.consume_remaining_seeds_for_row();
        }
    }

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
