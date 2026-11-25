//! dsdgen's random streams, by source row.
//!
//! dsdgen gives every column its own Lehmer stream
//! (`seed' = 16807 * seed mod (2^31 - 1)`) and a fixed budget of draws per
//! source row: at the end of each row it burns the unused ones, so row `r`
//! of a column starts `r * budget` draws into its stream. tpcdsgen does
//! this the same way, one draw at a time
//! (`consume_remaining_seeds_for_row`). Here a row's streams start at
//! their seeds for that row ([`RowSeeds`]), which `k` rows on is one
//! multiplication by `16807^(k * budget)`.

use populate_combinators::lehmer::{Lcg, M, pow};
use populate_core::Distribution;
use rand::Rng;
use std::array::from_fn;
use std::marker::PhantomData;
use tpcdsgen::generator::GeneratorColumn;
use tpcdsgen::random::RandomNumberStream;

/// tpcdsgen's per-column seed spacing: `initial = base + column * STRIDE`.
const STRIDE: i64 = i32::MAX as i64 / 799;
/// tpcdsgen's default seed base: column `c`'s stream starts at
/// `SEED_BASE + c * STRIDE` (not reduced mod `M`).
const SEED_BASE: i64 = 19_620_718;

/// dsdgen's random stream of generator column `c` at source row 1: the
/// stream of its initial seed.
pub(crate) fn stream(c: &impl GeneratorColumn) -> Lcg {
    Lcg::new(SEED_BASE + c.get_global_column_number() as i64 * STRIDE)
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
    use tpcdsgen::row::AbstractRowGenerator;
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
}
