//! Unbiased random indices in `0..n`, several from each `u64` the RNG
//! makes.
//!
//! A uniform `u64` `r` gives `k` uniform digits in `0..n` at once, read off
//! as the integer parts of `r * n`, then `frac * n`, and so on: `(d, r) =
//! (r * n) >> 64, (r * n) mod 2^64`. That is Lemire's nearly-divisionless
//! method applied to the product range `n^k` (Brackett-Rozinsky and Lemire,
//! "Batched Ranged Random Integer Generation", 2024): the digits are
//! exactly uniform when `r` is rejected whenever `r * n^k mod 2^64 <
//! 2^64 mod n^k`, and that one test is made before any digit is used.
//!
//! `k` is the largest power with `n^k <= 2^56`, so a draw is rejected with
//! probability under 2^-8. For `choose` over 7 values that is 19 indices per
//! RNG call instead of one; for `n > 2^56` it is plain Lemire.

use rand::Rng;

/// Largest product of a batch: a rejection costs at most `2^-8`.
const MAX_PRODUCT: u64 = 1 << 56;

/// Draws in `0..n` (see the module docs). Copy-cheap: it is the range, the
/// batch shape, and the unread rest of the current `u64`.
#[derive(Clone, Copy, Debug)]
pub struct Bounded {
    n: u64,
    /// Digits per `u64`.
    k: u32,
    /// `n^k`.
    product: u64,
    /// `2^64 mod n^k`: a `u64` whose `r * n^k mod 2^64` is below this is
    /// rejected.
    thresh: u64,
    /// The current `u64`, with the digits already read multiplied out.
    r: u64,
    /// Digits left in `r`.
    left: u32,
}

impl Bounded {
    /// Indices in `0..n`. Panics if `n == 0`.
    pub fn new(n: u64) -> Self {
        assert!(n > 0, "Bounded: empty range");
        let (mut k, mut product) = (1u32, n);
        if n > 1 {
            while let Some(p) = product.checked_mul(n).filter(|&p| p <= MAX_PRODUCT) {
                product = p;
                k += 1;
            }
        } else {
            // every digit is 0; take many per (unused) draw
            k = 64;
        }
        Bounded {
            n,
            k,
            product,
            thresh: product.wrapping_neg() % product,
            r: 0,
            left: 0,
        }
    }

    /// Indices in `0..n` for `n` up to `2^64` inclusive: `span` is the
    /// number of values, and `0` means all `2^64` of them.
    pub fn span(span: u64) -> Self {
        if span == 0 {
            // every u64 is a value: one digit per draw, never rejected
            Bounded {
                n: 0,
                k: 1,
                product: 0,
                thresh: 0,
                r: 0,
                left: 0,
            }
        } else {
            Self::new(span)
        }
    }

    /// The next index in `0..n`.
    #[inline(always)]
    pub fn next(&mut self, rng: &mut impl Rng) -> u64 {
        if self.n == 0 {
            return rng.next_u64();
        }
        if self.left == 0 {
            self.refill(rng);
        }
        self.digit()
    }

    /// Fill `out` with indices: the same values, in the same order, as
    /// `out.len()` calls of [`Self::next`], but each `u64`'s digits are read
    /// in one tight loop (a multiply and a store each) instead of one call
    /// per index with its own bookkeeping.
    #[inline]
    pub fn fill(&mut self, rng: &mut impl Rng, out: &mut [u64]) {
        if self.n == 0 {
            out.iter_mut().for_each(|o| *o = rng.next_u64());
            return;
        }
        let (n, k) = (self.n, self.k as usize);
        let mut i = 0;
        // digits left over from an earlier call
        while self.left > 0 && i < out.len() {
            out[i] = self.digit();
            i += 1;
        }
        // whole batches: one accepted `u64`, then `k` multiplies
        while out.len() - i >= k {
            let mut r = self.accept(rng);
            for o in &mut out[i..i + k] {
                let p = r as u128 * n as u128;
                r = p as u64;
                *o = (p >> 64) as u64;
            }
            i += k;
        }
        // the rest from a new batch, keeping its unread digits
        if i < out.len() {
            self.refill(rng);
            for o in &mut out[i..] {
                *o = self.digit();
            }
        }
    }

    /// The next digit of the current batch (`left > 0`).
    #[inline(always)]
    fn digit(&mut self) -> u64 {
        self.left -= 1;
        let p = self.r as u128 * self.n as u128;
        self.r = p as u64;
        (p >> 64) as u64
    }

    /// A `u64` that passes the rejection test.
    #[inline(always)]
    fn accept(&self, rng: &mut impl Rng) -> u64 {
        loop {
            let r = rng.next_u64();
            if r.wrapping_mul(self.product) >= self.thresh {
                return r;
            }
        }
    }

    #[inline]
    fn refill(&mut self, rng: &mut impl Rng) {
        self.r = self.accept(rng);
        self.left = self.k;
    }

    /// Indices per RNG draw.
    pub fn per_draw(&self) -> u32 {
        self.k
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;
    use rand::rngs::SmallRng;

    #[test]
    fn fill_matches_next() {
        for n in [0u64, 1, 3, 7, 2405, 1 << 40] {
            let (mut a, mut b) = (Bounded::span(n), Bounded::span(n));
            let (mut ra, mut rb) = (SmallRng::seed_from_u64(4), SmallRng::seed_from_u64(4));
            // uneven pieces, so batches straddle them
            for len in [1usize, 5, 19, 20, 0, 300, 7] {
                let mut got = vec![0; len];
                a.fill(&mut ra, &mut got);
                let want: Vec<u64> = (0..len).map(|_| b.next(&mut rb)).collect();
                assert_eq!(got, want, "{n}, {len}");
            }
        }
    }

    #[test]
    fn batch_shapes() {
        assert_eq!(Bounded::new(7).per_draw(), 19); // 7^19 < 2^56 < 7^20
        assert_eq!(Bounded::new(2).per_draw(), 56);
        assert_eq!(Bounded::new(1 << 28).per_draw(), 2);
        assert_eq!(Bounded::new((1 << 28) + 1).per_draw(), 1);
        assert_eq!(Bounded::new(u64::MAX).per_draw(), 1);
    }

    #[test]
    fn stays_in_range_and_covers_it() {
        let mut rng = SmallRng::seed_from_u64(1);
        for n in [
            1u64,
            2,
            3,
            7,
            10,
            36,
            1000,
            2405,
            10_494_850,
            1 << 40,
            u64::MAX,
        ] {
            let mut b = Bounded::new(n);
            let v: Vec<u64> = (0..20_000).map(|_| b.next(&mut rng)).collect();
            assert!(v.iter().all(|&x| x < n), "{n}");
            if n <= 1000 {
                let mut seen = vec![false; n as usize];
                v.iter().for_each(|&x| seen[x as usize] = true);
                assert!(seen.iter().all(|&s| s), "{n}: every value drawn");
            }
        }
        let mut all = Bounded::span(0);
        assert_ne!(all.next(&mut rng), all.next(&mut rng));
    }

    /// Chi-square of single digits over every value of `n`.
    #[test]
    fn digits_are_uniform() {
        let mut rng = SmallRng::seed_from_u64(2);
        for n in [3u64, 7, 36, 2405] {
            let mut b = Bounded::new(n);
            let draws = 200_000usize;
            let mut counts = vec![0usize; n as usize];
            for _ in 0..draws {
                counts[b.next(&mut rng) as usize] += 1;
            }
            let e = draws as f64 / n as f64;
            let chi: f64 = counts.iter().map(|&c| (c as f64 - e).powi(2) / e).sum();
            // mean n-1, sd sqrt(2(n-1)); 6 sd is a generous bound
            let df = (n - 1) as f64;
            assert!(chi < df + 6.0 * (2.0 * df).sqrt(), "{n}: chi^2 {chi}");
        }
    }

    /// Consecutive digits of one draw are independent: pairs of a 6-way
    /// draw are uniform over all 36 pairs.
    #[test]
    fn pairs_are_uniform() {
        let mut rng = SmallRng::seed_from_u64(3);
        let mut b = Bounded::new(6);
        let mut counts = [0usize; 36];
        let pairs = 360_000;
        for _ in 0..pairs {
            let (x, y) = (b.next(&mut rng), b.next(&mut rng));
            counts[(x * 6 + y) as usize] += 1;
        }
        let e = pairs as f64 / 36.0;
        let chi: f64 = counts.iter().map(|&c| (c as f64 - e).powi(2) / e).sum();
        assert!(chi < 35.0 + 6.0 * 70f64.sqrt(), "chi^2 {chi}");
    }
}
