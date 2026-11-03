//! Go `math/rand` compatibility.
//!
//! [`GoRand`] is a bit-exact port of Go's `rngSource` (the additive lagged
//! Fibonacci generator behind `rand.Seed` / `rand.NewSource`). [`GoRandExt`]
//! ports the `*rand.Rand` methods TSBS calls (`Float64`, `Intn`, `Int63n`,
//! `NormFloat64`, ...) as *algorithms over any [`RngCore`]*:
//!
//! - with a [`GoRand`] they reproduce Go's output exactly (validated against a
//!   Go golden dump in `validate/`);
//! - with any other `rand` RNG they are simply fast, well-distributed samplers
//!   ("fast mode"), so every TSBS distribution works with any `rand::Rng`.

use crate::sampler::Sampler;
use crate::sampler::go_tables::{FN, KN, RNG_COOKED, WN};
use rand::{Rng, RngCore};

const RNG_LEN: usize = 607;
const RNG_TAP: usize = 273;
const RNG_MASK: u64 = (1 << 63) - 1;
const INT32_MAX: i64 = (1 << 31) - 1;

/// Bit-exact port of Go's `math/rand.rngSource`.
#[derive(Clone)]
pub struct GoRand {
    tap: usize,
    feed: usize,
    vec: [i64; RNG_LEN],
}

fn seedrand(x: i32) -> i32 {
    const A: i32 = 48271;
    const Q: i32 = 44488;
    const R: i32 = 3399;
    let hi = x / Q;
    let lo = x % Q;
    let mut x = A.wrapping_mul(lo).wrapping_sub(R.wrapping_mul(hi));
    if x < 0 {
        x = x.wrapping_add(INT32_MAX as i32);
    }
    x
}

impl GoRand {
    /// Equivalent to Go's `rand.Seed(seed)` / `rand.NewSource(seed)`.
    pub fn new(seed: i64) -> Self {
        let mut seed = seed % INT32_MAX;
        if seed < 0 {
            seed += INT32_MAX;
        }
        if seed == 0 {
            seed = 89482311;
        }
        let mut vec = [0i64; RNG_LEN];
        let mut x = seed as i32;
        for i in -20..RNG_LEN as i64 {
            x = seedrand(x);
            if i >= 0 {
                let mut u = i64::from(x) << 40;
                x = seedrand(x);
                u ^= i64::from(x) << 20;
                x = seedrand(x);
                u ^= i64::from(x);
                u ^= RNG_COOKED[i as usize];
                vec[i as usize] = u;
            }
        }
        GoRand {
            tap: 0,
            feed: RNG_LEN - RNG_TAP,
            vec,
        }
    }

    /// Go's `rngSource.Uint64`.
    fn uint64(&mut self) -> u64 {
        self.tap = if self.tap == 0 {
            RNG_LEN - 1
        } else {
            self.tap - 1
        };
        self.feed = if self.feed == 0 {
            RNG_LEN - 1
        } else {
            self.feed - 1
        };
        let x = self.vec[self.feed].wrapping_add(self.vec[self.tap]);
        self.vec[self.feed] = x;
        x as u64
    }
}

impl RngCore for GoRand {
    fn next_u32(&mut self) -> u32 {
        // Go's Rand.Uint32: the top 32 of the 63 bits.
        ((self.uint64() & RNG_MASK) >> 31) as u32
    }

    fn next_u64(&mut self) -> u64 {
        self.uint64()
    }

    fn fill_bytes(&mut self, dst: &mut [u8]) {
        for chunk in dst.chunks_mut(8) {
            let v = self.uint64().to_le_bytes();
            chunk.copy_from_slice(&v[..chunk.len()]);
        }
    }
}

/// Go `*rand.Rand` sampling algorithms over any [`RngCore`].
///
/// Everything is derived from `next_u64() & (2^63-1)` (Go's `Int63`), so a
/// [`GoRand`] gives Go's exact streams.
pub trait GoRandExt: RngCore {
    /// Go `Int63`.
    fn go_int63(&mut self) -> i64 {
        (self.next_u64() & RNG_MASK) as i64
    }

    /// Go `Uint32`.
    fn go_uint32(&mut self) -> u32 {
        (self.go_int63() >> 31) as u32
    }

    /// Go `Int31`.
    fn go_int31(&mut self) -> i32 {
        (self.go_int63() >> 32) as i32
    }

    /// Go `Int63n` (rejection sampling, power-of-two fast path).
    fn go_int63n(&mut self, n: i64) -> i64 {
        assert!(n > 0, "invalid argument to Int63n");
        if n & (n - 1) == 0 {
            return self.go_int63() & (n - 1);
        }
        let max = ((1u64 << 63) - 1 - (1u64 << 63) % n as u64) as i64;
        let mut v = self.go_int63();
        while v > max {
            v = self.go_int63();
        }
        v % n
    }

    /// Go `Int31n` (rejection sampling, power-of-two fast path).
    fn go_int31n(&mut self, n: i32) -> i32 {
        assert!(n > 0, "invalid argument to Int31n");
        if n & (n - 1) == 0 {
            return self.go_int31() & (n - 1);
        }
        let max = ((1u32 << 31) - 1 - (1u32 << 31) % n as u32) as i32;
        let mut v = self.go_int31();
        while v > max {
            v = self.go_int31();
        }
        v % n
    }

    /// Go `Intn`: `Int31n` when `n` fits in 31 bits, else `Int63n`.
    fn go_intn(&mut self, n: usize) -> usize {
        assert!(n > 0, "invalid argument to Intn");
        if n <= i32::MAX as usize {
            self.go_int31n(n as i32) as usize
        } else {
            self.go_int63n(n as i64) as usize
        }
    }

    /// Go `Float64`: `Int63 / 2^63`, resampling the (never seen) value 1.0.
    fn go_float64(&mut self) -> f64 {
        loop {
            let f = self.go_int63() as f64 / (1u64 << 63) as f64;
            if f != 1.0 {
                return f;
            }
        }
    }

    /// Go `NormFloat64`: Marsaglia–Tsang ziggurat with Go's tables.
    fn go_norm_float64(&mut self) -> f64 {
        const RN: f64 = 3.442619855899;
        loop {
            let j = self.go_uint32() as i32;
            let i = (j & 0x7F) as usize;
            let mut x = f64::from(j) * f64::from(WN[i]);
            if j.unsigned_abs() < KN[i] {
                return x;
            }
            if i == 0 {
                loop {
                    x = -self.go_float64().ln() * (1.0 / RN);
                    let y = -self.go_float64().ln();
                    if y + y >= x * x {
                        break;
                    }
                }
                return if j > 0 { RN + x } else { -RN - x };
            }
            if FN[i] + (self.go_float64() as f32) * (FN[i - 1] - FN[i])
                < ((-0.5 * x * x).exp()) as f32
            {
                return x;
            }
        }
    }

    /// `s[Intn(len(s))]` — TSBS's `RandomStringSliceChoice` and friends.
    fn go_choice<'a, T>(&mut self, s: &'a [T]) -> &'a T {
        &s[self.go_intn(s.len())]
    }
}

impl<R: RngCore + ?Sized> GoRandExt for R {}

/// Go `math/rand` algorithms as a [`Sampler`]: `unif` is `Float64`, `index` is
/// `Intn`, `normal` is `NormFloat64`, `permutation` is `Perm`. With a
/// [`GoRand`] the draws match Go's exactly.
#[derive(Clone, Copy, Debug, Default)]
pub struct GoSampler;

impl Sampler for GoSampler {
    fn unif(self, rng: &mut impl Rng) -> f64 {
        rng.go_float64()
    }

    fn index(self, rng: &mut impl Rng, n: usize) -> usize {
        rng.go_intn(n)
    }

    fn normal(self, rng: &mut impl Rng) -> f64 {
        rng.go_norm_float64()
    }

    /// Go `Perm(n)`: inside-out Fisher–Yates with `Intn(i + 1)`.
    fn permutation(self, rng: &mut impl Rng, n: usize) -> Vec<usize> {
        let mut m = vec![0; n];
        for i in 0..n {
            let j = rng.go_intn(i + 1);
            m[i] = m[j];
            m[j] = i;
        }
        m
    }
}
