//! The Lehmer generator of dbgen and dsdgen (TPC-H, SSB, TPC-DS),
//! positioned anywhere in O(log n).
//!
//! dbgen gives every TPC-H column its own stream: the Park–Miller LCG
//! `x' = 16807 * x mod (2^31 - 1)` from a fixed seed. A stream reserves a
//! fixed number of draws per row (`per_row`) and skips the ones a row
//! doesn't use, so draw `k` of row `r` is always draw `r * per_row + k + 1`
//! of the stream: `seed * 16807^(r * per_row + k + 1) mod m`. That is what
//! lets any chunk of rows start exactly where the sequential generator
//! would be ([`Rng::at`]), and what lets [`draws`] generate a stream with
//! independent lanes.

use populate_core::{Distribution, SeekRng};

/// The LCG multiplier.
pub const A: u64 = 16807;
/// The LCG modulus, `2^31 - 1`.
pub const M: u64 = 2_147_483_647;
/// Independent chains [`draws`] runs at once.
const LANES: usize = 16;

/// `a * b mod (2^31 - 1)` for `a, b` in `[1, m)`, with no division and no
/// compare (so it vectorises).
///
/// `2^31 = 1 (mod m)`, so folding the high bits onto the low ones keeps the
/// residue. The first fold leaves `r <= m + 2^31 - 2`, the second `r < m`
/// or `r = m`, and `r = m` would mean `a * b = 0 (mod m)`, impossible for a
/// prime `m`.
#[inline(always)]
pub fn mulmod(a: u64, b: u64) -> u64 {
    let p = a * b;
    let r = (p & M) + (p >> 31);
    (r & M) + (r >> 31)
}

/// The multiplier that steps a stream `n` draws: `A^n mod m`.
#[inline]
pub fn pow(n: u64) -> u64 {
    powmod(A, n)
}

/// A seed as a residue in `[0, m)`. dsdgen starts some streams above `m`
/// (`SEED_BASE + column * (i32::MAX / 799)` is not reduced); a step
/// reduces it, so reducing it first makes the same draws, and keeps
/// [`mulmod`]'s inputs in range.
#[inline]
fn residue(seed: i64) -> u64 {
    seed as u64 % M
}

/// `base^exp mod m`.
pub fn powmod(mut base: u64, mut exp: u64) -> u64 {
    let mut acc = 1;
    while exp > 0 {
        if exp & 1 == 1 {
            acc = mulmod(acc, base);
        }
        base = mulmod(base, base);
        exp >>= 1;
    }
    acc
}

/// dbgen's bounded value of draw `x`: `lo + (x / m * range)` in `f64`,
/// truncated, with the range computed in wrapping `i32` as dbgen does
/// (`RandomAlphaNumeric` asks for `0..=i32::MAX`, a range of `-2^31`).
#[inline(always)]
pub fn bounded(x: u32, lo: i32, hi: i32) -> i32 {
    let range = (hi.wrapping_sub(lo)).wrapping_add(1) as f64;
    lo.wrapping_add(((x as f64 / M as f64) * range) as i32)
}

/// Ranges below this map a draw exactly with integers ([`scale`]).
const INT_RANGE: u32 = 1 << 21;

/// `floor(x * range / m)` without a division: equal to the `f64` formula
/// of [`bounded`] for every `x` in `[1, m)` when `range < 2^21`.
///
/// Why: `x * range` is never a multiple of the prime `m`, so the exact
/// quotient is at least `1/m` away from an integer on both sides. The two
/// `f64` roundings (the divide, then the multiply) are off by at most
/// `range * 2^-52 < 2^-31 < 1/m`, so they never cross an integer and the
/// truncation is the floor. (`exhaustive_scale_matches_f64` checks some
/// ranges draw by draw.)
///
/// The floor itself: `p = q0 * 2^31 + r0 = q0 * m + t` with
/// `t = q0 + r0 < 2m` (`p < 2^52`), so it is `q0 + (t >= m)`, and
/// `t >= m` is `(t + 1) >> 31`.
#[inline(always)]
pub fn scale(x: u32, range: u32) -> u32 {
    let p = x as u64 * range as u64;
    let q0 = p >> 31;
    let t = (p & M) + q0;
    (q0 + ((t + 1) >> 31)) as u32
}

/// The `n` draws `start + 1 ..= start + n` of the stream seeded `seed`, as
/// `LANES` independent chains (lane `l` holds draws `l, l + LANES, ...`),
/// so the multiply chain vectorises.
pub fn draws(seed: i64, start: u64, out: &mut [u32]) {
    fill_after(mulmod(residue(seed), pow(start)), out)
}

/// The `out.len()` draws after state `x` (a seed, or the last draw made).
fn fill_after(x: u64, out: &mut [u32]) {
    #[cfg(target_arch = "x86_64")]
    if std::arch::is_x86_feature_detected!("avx2") {
        // SAFETY: the CPU has AVX2
        return unsafe { fill_avx2(x, out) };
    }
    fill_any(x, out)
}

/// [`fill_after`] compiled for AVX2: 4 lanes per multiply instead of 2.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn fill_avx2(x: u64, out: &mut [u32]) {
    fill_any(x, out)
}

#[cfg(test)]
fn draws_any(seed: i64, start: u64, out: &mut [u32]) {
    fill_any(mulmod(residue(seed), pow(start)), out)
}

#[inline(always)]
fn fill_any(mut x: u64, out: &mut [u32]) {
    if out.len() < LANES {
        // too few for the lanes to pay for their start
        for o in out {
            x = mulmod(x, A);
            *o = x as u32;
        }
        return;
    }
    let mut lanes = [0u64; LANES];
    for l in &mut lanes {
        x = mulmod(x, A);
        *l = x;
    }
    let step = pow(LANES as u64);
    // as u32 lanes: every value is below 2^31, so each product is one
    // 32 x 32 -> 64 bit multiply (`pmuludq`), and the store is a plain copy
    let mut lanes = lanes.map(|x| x as u32);
    let step = step as u32;
    let (chunks, rest) = out.as_chunks_mut::<LANES>();
    for c in chunks {
        *c = lanes;
        for x in &mut lanes {
            *x = mulmod(*x as u64, step as u64) as u32;
        }
    }
    let n = rest.len();
    rest.copy_from_slice(&lanes[..n]);
}

/// dbgen's bounded value of draw `x` in `lo..=hi`: [`bound_all`] of one
/// draw.
#[inline(always)]
pub fn bound(x: u32, lo: i32, hi: i32) -> i32 {
    let range = (hi as i64 - lo as i64 + 1) as u64;
    if range < INT_RANGE as u64 {
        // equal to the `f64` formula for these ranges (see `scale`)
        lo + scale(x, range as u32) as i32
    } else {
        bounded(x, lo, hi)
    }
}

/// Map draws to dbgen's bounded values in `lo..=hi`, in place.
pub fn bound_all(v: &mut [u32], lo: i32, hi: i32) {
    #[cfg(target_arch = "x86_64")]
    if std::arch::is_x86_feature_detected!("avx2") {
        // SAFETY: the CPU has AVX2
        return unsafe { bound_all_avx2(v, lo, hi) };
    }
    bound_all_any(v, lo, hi)
}

/// [`bound_all`] compiled for AVX2.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn bound_all_avx2(v: &mut [u32], lo: i32, hi: i32) {
    bound_all_any(v, lo, hi)
}

#[inline(always)]
fn bound_all_any(v: &mut [u32], lo: i32, hi: i32) {
    let range = (hi as i64 - lo as i64 + 1) as u64;
    if range < INT_RANGE as u64 {
        let range = range as u32;
        for x in v {
            *x = (lo + scale(*x, range) as i32) as u32;
        }
    } else {
        for x in v {
            *x = bounded(*x, lo, hi) as u32;
        }
    }
}

/// One dbgen stream as a [`SeekRng`]: the draws after a state, made in
/// lanes by [`fill`](SeekRng::fill), jumped over in `O(log n)` by
/// [`advance`](SeekRng::advance). `populate_builtins::draws` makes it a
/// column that reserves a fixed number of draws per row.
#[derive(Debug, Clone, Copy)]
pub struct Lcg {
    /// The last draw made (the seed before the first).
    x: u64,
}

impl Lcg {
    /// The stream seeded `seed`, before its first draw.
    pub fn new(seed: i64) -> Self {
        Lcg { x: residue(seed) }
    }
}

impl SeekRng for Lcg {
    type Draw = u32;

    #[inline(always)]
    fn next_draw(&mut self) -> u32 {
        self.x = mulmod(self.x, A);
        self.x as u32
    }

    #[inline]
    fn advance(&mut self, n: u64) {
        if n > 0 {
            self.x = mulmod(self.x, pow(n));
        }
    }

    fn fill(&mut self, out: &mut [u32]) {
        fill_after(self.x, out);
        if let Some(&last) = out.last() {
            self.x = last as u64;
        }
    }
}

/// `K` bounded values of a stream per row, dbgen's `lo..=hi` of each draw:
/// the column most streams make. Collected, the draws are filled and
/// bounded in place, both vectorised ([`draws`], [`bound_all`]).
#[derive(Debug, Clone, Copy)]
pub struct Ints<const K: usize> {
    stream: Lcg,
    lo: i32,
    hi: i32,
}

impl<const K: usize> Ints<K> {
    /// The stream seeded `seed`, mapped to `lo..=hi`.
    pub fn new(seed: i64, lo: i32, hi: i32) -> Self {
        Ints {
            stream: Lcg::new(seed),
            lo,
            hi,
        }
    }

    /// `out.len()` values.
    fn fill(&mut self, out: &mut [i32]) {
        // SAFETY: `i32` and `u32` have the same size, alignment and valid
        // bit patterns
        let raw = unsafe { std::slice::from_raw_parts_mut(out.as_mut_ptr().cast(), out.len()) };
        self.stream.fill(raw);
        bound_all(raw, self.lo, self.hi);
    }
}

impl<const K: usize> Distribution for Ints<K> {
    type Item = [i32; K];

    fn collect_into(&mut self, cols: &mut Vec<[i32; K]>, _rng: &mut impl rand::Rng, n: usize) {
        let start = cols.len();
        cols.resize(start + n, [0; K]);
        self.fill(cols[start..].as_flattened_mut());
    }

    fn skip(&mut self, n: u64) {
        self.stream.advance(n * K as u64);
    }
}

/// One bounded value per row: [`Ints<1>`] as a plain `i32` column.
#[derive(Debug, Clone, Copy)]
pub struct Int(pub Ints<1>);

impl Distribution for Int {
    type Item = i32;

    fn collect_into(&mut self, cols: &mut Vec<i32>, _rng: &mut impl rand::Rng, n: usize) {
        let start = cols.len();
        cols.resize(start + n, 0);
        self.0.fill(&mut cols[start..]);
    }

    fn skip(&mut self, n: u64) {
        self.0.skip(n);
    }
}

/// One dbgen stream (tpchgen's `RowRandomInt`), drawn serially.
#[derive(Debug, Clone, Copy)]
pub struct Rng {
    seed: u64,
    usage: u32,
    per_row: u32,
}

impl Rng {
    /// The stream seeded `seed` with `per_row` draws per row, at the start
    /// of row `row`.
    pub fn at(seed: i64, per_row: u32, row: u64) -> Self {
        Rng {
            seed: mulmod(residue(seed), pow(per_row as u64 * row)),
            usage: 0,
            per_row,
        }
    }

    /// The next raw draw, in `[1, m)`.
    #[inline(always)]
    pub fn next_rand(&mut self) -> u32 {
        self.seed = mulmod(self.seed, A);
        self.usage += 1;
        self.seed as u32
    }

    /// The next value in `lo..=hi`, as dbgen maps it.
    #[inline(always)]
    pub fn next_int(&mut self, lo: i32, hi: i32) -> i32 {
        let x = self.next_rand();
        bounded(x, lo, hi)
    }

    /// Skip to the start of the next row.
    #[inline(always)]
    pub fn row_finished(&mut self) {
        if self.usage < self.per_row {
            self.seed = mulmod(self.seed, pow((self.per_row - self.usage) as u64));
        }
        self.usage = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tpchgen::random::RowRandomInt;

    #[test]
    fn rng_matches_tpchgen_anywhere() {
        for (seed, per_row) in [(1i64, 1u32), (933588178, 7), (1095462486, 14)] {
            let mut want = RowRandomInt::new(seed, per_row as i32);
            for row in 0..200u64 {
                let mut got = Rng::at(seed, per_row, row);
                let used = (row % per_row as u64) as u32 + 1;
                for _ in 0..used {
                    assert_eq!(got.next_int(3, 99), want.next_int(3, 99));
                }
                // and through `row_finished` from an earlier row
                want.row_finished();
            }
        }
        // wrapping range, as RandomAlphaNumeric uses
        let mut a = Rng::at(77, 9, 0);
        let mut b = RowRandomInt::new(77, 9);
        for _ in 0..9 {
            assert_eq!(a.next_int(0, i32::MAX), b.next_int(0, i32::MAX));
        }
    }

    /// dsdgen's seeds above `m`, stepped as tpcdsgen steps them.
    #[test]
    fn unreduced_seeds_step_like_dsdgen() {
        const SEED_BASE: i64 = 19_620_718;
        for column in [0i64, 1, 500, 797, 798] {
            let seed = SEED_BASE + column * (i32::MAX as i64 / 799);
            let mut naive = seed as u64;
            let mut serial = Lcg::new(seed);
            let mut filled = vec![0u32; 100];
            Lcg::new(seed).fill(&mut filled);
            for (k, &f) in filled.iter().enumerate() {
                naive = naive * A % M;
                assert_eq!(serial.next_draw() as u64, naive);
                assert_eq!(f as u64, naive);
                let mut jumped = Lcg::new(seed);
                jumped.advance(k as u64);
                assert_eq!(jumped.next_draw() as u64, naive);
            }
            assert!(column < 798 || seed as u64 > M);
        }
    }

    #[test]
    fn pow_steps_n_draws() {
        let mut x = 12345u64;
        for _ in 0..1000 {
            x = x * A % M;
        }
        assert_eq!(mulmod(12345, pow(1000)), x);
    }

    #[test]
    fn mulmod_matches_modulo() {
        for (a, b) in [(1u64, 1u64), (M - 1, M - 1), (A, 123_456_789), (M - 1, 2)] {
            assert_eq!(mulmod(a, b), (a * b) % M);
        }
    }

    #[test]
    fn draws_match_serial() {
        for start in [0u64, 1, 17, 1_000_003] {
            for n in [0usize, 1, 15, 16, 17, 100] {
                // the dispatched version and the portable one
                for f in [draws, draws_any] {
                    let mut v = vec![0; n];
                    f(1808217256, start, &mut v);
                    let mut r = Rng::at(1808217256, 1, start);
                    for &x in &v {
                        assert_eq!(x, r.next_rand());
                    }
                }
            }
        }
    }

    #[test]
    fn bound_all_dispatch_matches_portable() {
        let mut v = vec![0; 1000];
        draws(77, 5, &mut v);
        for (lo, hi) in [(1, 7), (100, 100_000), (0, i32::MAX), (-5, 3_000_000)] {
            let (mut a, mut b) = (v.clone(), v.clone());
            bound_all(&mut a, lo, hi);
            bound_all_any(&mut b, lo, hi);
            assert_eq!(a, b);
        }
    }

    fn f64_scale(x: u32, range: u32) -> u32 {
        ((x as f64 / M as f64) * range as f64) as i32 as u32
    }

    #[test]
    fn scale_matches_f64_sampled() {
        for r in [1, 2, 7, 25, 2406, 9999, 99_901, 1_100_000, INT_RANGE - 1] {
            for x in (1..M as u32).step_by(9973).chain([1, M as u32 - 1]) {
                assert_eq!(scale(x, r), f64_scale(x, r), "x {x} range {r}");
            }
        }
    }

    #[test]
    #[ignore = "exhaustive, ~2^31 values per range; run with --release --ignored"]
    fn exhaustive_scale_matches_f64() {
        for r in [7, 50, 2406, 200_000, 2_000_000] {
            for x in 1..M as u32 {
                assert_eq!(scale(x, r), f64_scale(x, r), "x {x} range {r}");
            }
        }
    }
}
