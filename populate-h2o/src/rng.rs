//! R's random numbers: the Mersenne Twister as `set.seed()` seeds it, and
//! the `sample()` / `runif()` draws the H2O datagen scripts make from it.
//!
//! Everything here follows R 4.x (`src/main/RNG.c`, `src/main/random.c`,
//! `src/main/unique.c`) with the defaults `RNGkind("Mersenne-Twister",
//! sample.kind = "Rejection")`:
//!
//! - `unif_rand()` is `fixup(y / 2^32)` of the tempered 32-bit output `y`;
//! - `R_unif_index(n)` draws `bits = ceil(log2(n))` bits, 16 per draw (the
//!   top 16 bits of `y`), as `bits / 16 + 1` draws, and rejects values
//!   `>= n`. So every attempt for one `n` takes the same number of draws.

#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::{_MM_HINT_T0, _mm_prefetch};
use std::fmt::{self, Debug, Formatter};

/// MT19937 state size.
pub const N: usize = 624;
const M: usize = 397;
const MATRIX_A: u32 = 0x9908_b0df;
const UPPER: u32 = 0x8000_0000;
const LOWER: u32 = 0x7fff_ffff;

/// R's Mersenne Twister. `Clone` is a snapshot: 2.5 KB.
#[derive(Clone)]
pub struct RMt {
    mt: [u32; N],
    mti: usize,
}

impl Debug for RMt {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("RMt").field("mti", &self.mti).finish()
    }
}

#[inline(always)]
fn temper(mut y: u32) -> u32 {
    y ^= y >> 11;
    y ^= (y << 7) & 0x9d2c_5680;
    y ^= (y << 15) & 0xefc6_0000;
    y ^ (y >> 18)
}

#[inline(always)]
fn twist(a: u32, b: u32, m: u32) -> u32 {
    let y = (a & UPPER) | (b & LOWER);
    m ^ (y >> 1) ^ ((y & 1).wrapping_neg() & MATRIX_A)
}

impl RMt {
    /// The state after R's `set.seed(seed)`.
    pub fn set_seed(seed: u32) -> Self {
        let mut s = seed;
        for _ in 0..50 {
            s = s.wrapping_mul(69069).wrapping_add(1);
        }
        // `i_seed[0]` (R's `mti` slot) takes the first value, then is reset
        // to 624 by FixupSeeds.
        s = s.wrapping_mul(69069).wrapping_add(1);
        let mut mt = [0u32; N];
        for v in &mut mt {
            s = s.wrapping_mul(69069).wrapping_add(1);
            *v = s;
        }
        RMt { mt, mti: N }
    }

    /// Generate the next 624 words.
    fn regen(&mut self) {
        let mt = &mut self.mt;
        for kk in 0..N - M {
            mt[kk] = twist(mt[kk], mt[kk + 1], mt[kk + M]);
        }
        for kk in N - M..N - 1 {
            mt[kk] = twist(mt[kk], mt[kk + 1], mt[kk + M - N]);
        }
        mt[N - 1] = twist(mt[N - 1], mt[0], mt[M - 1]);
        self.mti = 0;
    }

    /// The next tempered 32-bit output.
    #[inline(always)]
    pub fn next_u32(&mut self) -> u32 {
        if self.mti >= N {
            self.regen();
        }
        let y = self.mt[self.mti];
        self.mti += 1;
        temper(y)
    }

    /// Skip `n` outputs.
    pub fn skip(&mut self, mut n: u64) {
        let left = (N - self.mti) as u64;
        if n <= left {
            self.mti += n as usize;
            return;
        }
        n -= left;
        self.mti = N;
        while n > N as u64 {
            self.regen();
            n -= N as u64;
        }
        self.regen();
        self.mti = n as usize;
    }

    /// Pass the remaining raw (untempered) state words of the current
    /// block to `f`, without copying; `f` returns how many it used.
    #[inline(always)]
    fn with_block(&mut self, f: impl FnOnce(&[u32]) -> usize) {
        if self.mti >= N {
            self.regen();
        }
        let used = f(&self.mt[self.mti..]);
        self.mti += used;
    }

    /// R's `unif_rand()`.
    #[inline(always)]
    pub fn unif_rand(&mut self) -> f64 {
        unif(self.next_u32())
    }

    /// R's `R_unif_index(n)` (`n >= 1`): uniform in `0..n`.
    #[inline]
    pub fn unif_index(&mut self, n: u64) -> u64 {
        let s = Sampler::new(n);
        loop {
            let v = s.attempt(self);
            if v < n {
                return v;
            }
        }
    }

    /// `n` draws of `R_unif_index(range)` into `out`, as
    /// `sample.int(range, n, replace = TRUE) - 1` makes them.
    pub fn fill_index(&mut self, range: u64, out: &mut [u32]) {
        let s = Sampler::new(range);
        let n = range as u32;
        let mut i = 0;
        match s.draws {
            1 => {
                let (shift, mask) = (16, s.mask as u32);
                while i < out.len() {
                    let o = &mut out[i..];
                    self.with_block(|ys| {
                        let mut vals = [0u32; N];
                        for (v, &y) in vals.iter_mut().zip(ys) {
                            *v = (temper(y) >> shift) & mask;
                        }
                        let vals = &vals[..ys.len()];
                        let mut k = 0;
                        // branch-free store, advance only on accept
                        let used = if o.len() > vals.len() {
                            // room for every value: no exit test
                            for &v in vals {
                                o[k] = v;
                                k += (v < n) as usize;
                            }
                            vals.len()
                        } else {
                            let mut used = 0;
                            for &v in vals {
                                used += 1;
                                o[k] = v;
                                k += (v < n) as usize;
                                if k == o.len() {
                                    break;
                                }
                            }
                            used
                        };
                        i += k;
                        used
                    });
                }
            }
            2 => {
                let mask = s.mask as u32;
                while i < out.len() {
                    if self.mti >= N {
                        self.regen();
                    }
                    if N - self.mti < 2 {
                        // an attempt straddles two blocks
                        out[i] = self.unif_index(range) as u32;
                        i += 1;
                        continue;
                    }
                    let o = &mut out[i..];
                    let ys = &self.mt[self.mti..];
                    let pairs = ys.len() / 2;
                    // the block's attempt values, tempered in one pass
                    let mut t = [0u32; N];
                    for (t, &y) in t.iter_mut().zip(ys) {
                        *t = temper(y);
                    }
                    let mut vals = [0u32; N / 2];
                    for (v, p) in vals.iter_mut().zip(t.chunks_exact(2)) {
                        *v = ((p[0] & 0xffff_0000) | (p[1] >> 16)) & mask;
                    }
                    let vals = &vals[..pairs];
                    let mut k = 0;
                    let used = if o.len() > pairs {
                        // room for every value: no exit test
                        for &v in vals {
                            o[k] = v;
                            k += (v < n) as usize;
                        }
                        2 * pairs
                    } else {
                        let mut used = 0;
                        for &v in vals {
                            used += 2;
                            o[k] = v;
                            k += (v < n) as usize;
                            if k == o.len() {
                                break;
                            }
                        }
                        used
                    };
                    self.mti += used;
                    i += k;
                }
            }
            _ => {
                for o in out {
                    *o = self.unif_index(range) as u32;
                }
            }
        }
    }

    /// Skip `n` draws of `R_unif_index(range)`, returning nothing: the
    /// same stream position as [`fill_index`](Self::fill_index).
    pub fn skip_index(&mut self, range: u64, mut n: u64) {
        let s = Sampler::new(range);
        if s.draws == 2 {
            let mask = s.mask as u32;
            let r = range as u32;
            while n > 0 {
                if self.mti >= N {
                    self.regen();
                }
                if N - self.mti < 2 {
                    self.unif_index(range);
                    n -= 1;
                    continue;
                }
                let ys = &self.mt[self.mti..];
                let pairs = ys.len() / 2;
                if (pairs as u64) < n {
                    // can't run out in this block: count accepts, no exit test
                    let mut t = [0u32; N];
                    for (t, &y) in t.iter_mut().zip(ys) {
                        *t = temper(y);
                    }
                    let hit = t[..2 * pairs]
                        .chunks_exact(2)
                        .map(|p| ((((p[0] & 0xffff_0000) | (p[1] >> 16)) & mask) < r) as u32)
                        .sum::<u32>() as u64;
                    if hit < n {
                        n -= hit;
                        self.mti += 2 * pairs;
                        continue;
                    }
                }
                let mut used = 0;
                for p in ys.chunks_exact(2) {
                    used += 2;
                    let v = (((temper(p[0]) >> 16) << 16) | (temper(p[1]) >> 16)) & mask;
                    n -= (v < r) as u64;
                    if n == 0 {
                        break;
                    }
                }
                self.mti += used;
            }
            return;
        }
        if s.draws != 1 {
            for _ in 0..n {
                self.unif_index(range);
            }
            return;
        }
        let mask = s.mask as u32;
        let r = range as u32;
        while n > 0 {
            if self.mti >= N {
                self.regen();
            }
            let ys = &self.mt[self.mti..];
            if (ys.len() as u64) < n {
                // can't run out in this block: count accepts, no exit test
                let hit = ys
                    .iter()
                    .map(|&y| (((temper(y) >> 16) & mask) < r) as u32)
                    .sum::<u32>() as u64;
                if hit < n {
                    n -= hit;
                    self.mti = N;
                    continue;
                }
            }
            self.with_block(|ys| {
                let mut used = 0;
                for &y in ys {
                    used += 1;
                    n -= (((temper(y) >> 16) & mask) < r) as u64;
                    if n == 0 {
                        break;
                    }
                }
                used
            });
        }
    }

    /// `n` draws of `unif_rand()` into `out`.
    pub fn fill_unif(&mut self, out: &mut [f64]) {
        let mut i = 0;
        while i < out.len() {
            let o = &mut out[i..];
            self.with_block(|ys| {
                let k = ys.len().min(o.len());
                for (d, &y) in o[..k].iter_mut().zip(ys) {
                    *d = unif(temper(y));
                }
                i += k;
                k
            });
        }
    }
}

/// `fixup(y * 2^-32)`: in (0, 1).
#[inline(always)]
pub fn unif(y: u32) -> f64 {
    const I2_32M1: f64 = 2.328_306_437_080_797e-10;
    let x = y as f64 * 2.328_306_436_538_696_3e-10;
    if x <= 0.0 {
        0.5 * I2_32M1
    } else {
        // `1 - x <= 0` cannot happen: x <= 1 - 2^-32
        x
    }
}

/// `R_unif_index(n)`'s attempt shape for one `n`.
#[derive(Debug, Clone, Copy)]
pub struct Sampler {
    /// Draws per attempt.
    pub draws: u32,
    pub mask: u64,
}

impl Sampler {
    pub fn new(n: u64) -> Self {
        assert!(n >= 1, "R_unif_index needs n >= 1");
        // ceil(log2(n)), exact for integers
        let bits = 64 - (n - 1).leading_zeros();
        Sampler {
            draws: bits / 16 + 1,
            mask: if bits >= 64 {
                u64::MAX
            } else {
                (1u64 << bits) - 1
            },
        }
    }

    /// One attempt: R's `rbits(bits)`.
    #[inline(always)]
    pub fn attempt(self, rng: &mut RMt) -> u64 {
        let mut v = 0u64;
        for _ in 0..self.draws {
            v = (v << 16) | (rng.next_u32() >> 16) as u64;
        }
        v & self.mask
    }
}

/// `sample.int(n, k)` without replacement and without hashing (R's
/// `do_sample`): a partial Fisher-Yates shuffle over `0..n`, 1-based
/// values. With `k == n` this is `sample.int(n)`, a permutation.
pub fn sample_noreplace(rng: &mut RMt, n: u64, k: u64) -> Vec<u32> {
    assert!(k <= n && n <= u32::MAX as u64);
    let mut out = Vec::with_capacity(k as usize);
    if k < 2 {
        for _ in 0..k {
            out.push((rng.unif_index(n) + 1) as u32);
        }
        return out;
    }
    let mut x: Vec<u32> = (0..n as u32).collect();
    let mut m = n;
    for _ in 0..k {
        let j = rng.unif_index(m) as usize;
        out.push(x[j] + 1);
        m -= 1;
        x[j] = x[m as usize];
    }
    out
}

/// `v <- v[sample.int(length(v))]`: `sample.int(n)` is R's partial
/// Fisher-Yates loop (`do_sample`) with `k == n`, which takes `x[j]` and
/// moves the last live element into its place. Done on the values
/// themselves, as swaps, the taken values collect at the end in reverse.
pub fn shuffle(rng: &mut RMt, v: &mut [u32]) {
    let js = shuffle_draws(rng, v.len());
    apply_shuffle(v, &js);
}

/// The draws of `sample.int(n)`: `R_unif_index(m)` for `m = n, n-1, ..,
/// 1`. They depend only on the generator, so they can be made first and
/// the (memory-bound) swaps applied later, on another thread.
pub fn shuffle_draws(rng: &mut RMt, n: usize) -> Vec<u32> {
    if n < 2 {
        // k < 2: one draw, no reordering
        if n == 1 {
            rng.unif_index(1);
        }
        return vec![];
    }
    // draw `i` is for m = n - i
    let mut out = vec![0u32; n];
    let mut m = n as u64;
    while m > 0 {
        // every m down to the next power of two draws the same bits
        let s = Sampler::new(m);
        let low = if m == 1 { 0 } else { (s.mask >> 1) + 1 };
        if s.draws != 2 {
            for mm in (low + 1..=m).rev() {
                out[n - mm as usize] = rng.unif_index(mm) as u32;
            }
            m = low;
            continue;
        }
        let mask = s.mask as u32;
        while m > low {
            if rng.mti >= N {
                rng.regen();
            }
            if N - rng.mti < 2 {
                // an attempt straddles two blocks
                out[n - m as usize] = rng.unif_index(m) as u32;
                m -= 1;
                continue;
            }
            // temper the rest of the block, then take pairs branch-free:
            // an accepted value is kept and moves on to the next m
            let ys = &rng.mt[rng.mti..];
            let mut t = [0u32; N];
            for (t, &y) in t.iter_mut().zip(ys) {
                *t = temper(y);
            }
            let mut used = 0;
            for p in t[..ys.len() / 2 * 2].chunks_exact(2) {
                used += 2;
                let v = ((p[0] & 0xffff_0000) | (p[1] >> 16)) & mask;
                let hit = (v as u64) < m;
                out[n - m as usize] = v;
                m -= hit as u64;
                if m == low {
                    break;
                }
            }
            rng.mti += used;
        }
    }
    out
}

/// Apply [`shuffle_draws`] to `v` (of the same length, or length < 2).
pub fn apply_shuffle(v: &mut [u32], js: &[u32]) {
    let n = v.len();
    if n < 2 {
        return;
    }
    assert_eq!(js.len(), n);
    // prefetch the elements a few swaps ahead: the swaps are random
    // accesses into an array much larger than cache
    const AHEAD: usize = 16;
    for (t, &j) in js.iter().enumerate() {
        if let Some(&a) = js.get(t + AHEAD) {
            prefetch(&v[a as usize]);
        }
        v.swap(j as usize, n - t - 1);
    }
    v.reverse();
}

/// Hint that `*p` will be read soon.
#[inline(always)]
fn prefetch<T>(p: &T) {
    #[cfg(target_arch = "x86_64")]
    // SAFETY: prefetching is a hint; any address is allowed
    unsafe {
        _mm_prefetch(p as *const T as *const i8, _MM_HINT_T0)
    };
    #[cfg(not(target_arch = "x86_64"))]
    let _ = p;
}

/// `sample.int(n, k)` by R's hashing algorithm (`do_sample2`), used when
/// `n > 1e7` and `k <= n / 2`: draw, and redraw duplicates (at most 100
/// tries per value).
pub fn sample_hash(rng: &mut RMt, n: u64, k: u64) -> Vec<u32> {
    assert!(2 * k <= n && n <= u32::MAX as u64);
    let mut seen = vec![0u64; (n as usize).div_ceil(64) + 1];
    let mut out = Vec::with_capacity(k as usize);
    for _ in 0..k {
        let mut v = 0;
        for _ in 0..100 {
            v = (rng.unif_index(n) + 1) as u32;
            let (w, b) = (v as usize / 64, v % 64);
            if seen[w] >> b & 1 == 0 {
                seen[w] |= 1 << b;
                break;
            }
        }
        out.push(v);
    }
    out
}

/// R's `sample.int(n, k)` without replacement, as `sample()` on a vector
/// of length `n` (or `sample(n, k)` for a number) chooses its algorithm.
pub fn sample_int(rng: &mut RMt, n: u64, k: u64) -> Vec<u32> {
    if n > 10_000_000 && 2 * k <= n {
        sample_hash(rng, n, k)
    } else {
        sample_noreplace(rng, n, k)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // set.seed(108); sprintf("%.17g", runif(3))
    #[test]
    fn runif_matches_r() {
        let mut r = RMt::set_seed(108);
        let got: Vec<String> = (0..3).map(|_| format!("{:.17}", r.unif_rand())).collect();
        assert_eq!(
            got,
            [
                "0.45505803870037198",
                "0.40396105591207743",
                "0.35129767190665007"
            ]
        );
    }

    // set.seed(108); sample(100, 10, TRUE)
    #[test]
    fn sample_replace_matches_r() {
        let mut r = RMt::set_seed(108);
        let mut v = vec![0u32; 10];
        r.fill_index(100, &mut v);
        let v: Vec<u32> = v.iter().map(|x| x + 1).collect();
        assert_eq!(v, [16, 39, 47, 43, 54, 29, 47, 91, 90, 70]);
        // set.seed(108); sample(1e5, 5, TRUE): two draws per attempt
        let mut r = RMt::set_seed(108);
        let mut v = vec![0u32; 5];
        r.fill_index(100_000, &mut v);
        let v: Vec<u32> = v.iter().map(|x| x + 1).collect();
        assert_eq!(v, [26474, 43536, 1517, 15275, 96541]);
    }

    // set.seed(108); sample.int(11); sample.int(2e7, 10)
    #[test]
    fn sample_noreplace_matches_r() {
        let mut r = RMt::set_seed(108);
        assert_eq!(
            sample_int(&mut r, 11, 11),
            [10, 7, 6, 5, 11, 8, 9, 3, 4, 2, 1]
        );
        let mut r = RMt::set_seed(108);
        assert_eq!(
            sample_int(&mut r, 20_000_000, 10),
            [
                8284010, 10880493, 3504413, 19815661, 15518939, 6713818, 9649742, 9945141, 7788307,
                13987989
            ]
        );
    }

    #[test]
    fn shuffle_is_sample() {
        for n in [1u64, 2, 3, 10, 1000, 65_537, 300_000] {
            let mut a = RMt::set_seed(3);
            let mut b = a.clone();
            let perm = sample_noreplace(&mut a, n, n);
            let mut v: Vec<u32> = (1..=n as u32).map(|x| x * 7).collect();
            shuffle(&mut b, &mut v);
            let want: Vec<u32> = perm.iter().map(|&p| p * 7).collect();
            assert_eq!(v, want, "n = {n}");
            assert_eq!(a.next_u32(), b.next_u32());
        }
    }

    #[test]
    fn fast_fills_match_unif_index() {
        for range in [
            2u64,
            100,
            65_536,
            65_537,
            100_000,
            10_000_000,
            2_147_483_647,
        ] {
            for offset in [0u64, 1, 623] {
                let mut a = RMt::set_seed(11);
                a.skip(offset);
                let mut b = a.clone();
                let mut v = vec![0u32; 3001];
                a.fill_index(range, &mut v);
                let want: Vec<u32> = (0..3001).map(|_| b.unif_index(range) as u32).collect();
                assert_eq!(v, want, "range {range} offset {offset}");
                assert_eq!(a.next_u32(), b.next_u32());
            }
        }
    }

    #[test]
    fn skip_matches_draws() {
        for range in [2u64, 5, 100, 1000, 65_536, 100_000, 10_000_000] {
            let mut a = RMt::set_seed(7);
            let mut b = a.clone();
            let mut v = vec![0u32; 5000];
            a.fill_index(range, &mut v);
            b.skip_index(range, 5000);
            assert_eq!(a.next_u32(), b.next_u32(), "range {range}");
        }
        let mut a = RMt::set_seed(7);
        let mut b = a.clone();
        for _ in 0..3000 {
            a.next_u32();
        }
        b.skip(3000);
        assert_eq!(a.next_u32(), b.next_u32());
    }
}
