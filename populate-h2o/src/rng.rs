//! R-compatible random numbers.
//!
//! [`RRng`] is a port of R's default generator (`RNGkind()` =
//! `"Mersenne-Twister", "Inversion", "Rejection"`), including `set.seed`'s
//! seed scrambling, so a seeded `RRng` produces exactly the stream R does.
//! It implements [`RngCore`]: `next_u32` is the tempered 32-bit MT output that
//! R's `unif_rand()` scales to `[0, 1)`.
//!
//! [`r_round`] is R (>= 4.0.0)'s `round(x, digits)`.

use rand::RngCore;

const N: usize = 624;
const M: usize = 397;
const MATRIX_A: u32 = 0x9908_b0df;
const UPPER_MASK: u32 = 0x8000_0000;
const LOWER_MASK: u32 = 0x7fff_ffff;

/// `2^-32`, R's scale from a 32-bit MT output to `[0, 1)`.
pub const MT_SCALE: f64 = 2.328_306_436_538_696_3e-10;
/// `1 / (2^32 - 1)`, used by R's `fixup` to keep `unif_rand` inside `(0, 1)`.
const I2_32M1: f64 = 2.328_306_437_080_797e-10;

/// R's Mersenne-Twister, seeded like `set.seed(seed)`.
#[derive(Clone)]
pub struct RRng {
    mt: [u32; N],
    mti: usize,
}

impl RRng {
    /// Equivalent of R's `set.seed(seed)` with the default `RNGkind`.
    pub fn new(seed: i32) -> Self {
        // RNG_Init: initial scrambling, then fill the 625-word seed vector
        // (dummy[0] is mti, dummy[1..] is mt) from the same LCG.
        let mut s = seed as u32;
        for _ in 0..50 {
            s = s.wrapping_mul(69069).wrapping_add(1);
        }
        let mut mt = [0u32; N];
        // dummy[0] is overwritten by FixupSeeds (mti = 624), but still consumes an LCG step.
        s = s.wrapping_mul(69069).wrapping_add(1);
        for slot in mt.iter_mut() {
            s = s.wrapping_mul(69069).wrapping_add(1);
            *slot = s;
        }
        Self { mt, mti: N }
    }

    fn twist(&mut self) {
        let mag01 = |y: u32| if y & 1 == 0 { 0 } else { MATRIX_A };
        let mt = &mut self.mt;
        for kk in 0..N - M {
            let y = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
            mt[kk] = mt[kk + M] ^ (y >> 1) ^ mag01(y);
        }
        for kk in N - M..N - 1 {
            let y = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
            mt[kk] = mt[kk + M - N] ^ (y >> 1) ^ mag01(y);
        }
        let y = (mt[N - 1] & UPPER_MASK) | (mt[0] & LOWER_MASK);
        mt[N - 1] = mt[M - 1] ^ (y >> 1) ^ mag01(y);
        self.mti = 0;
    }
}

impl RngCore for RRng {
    fn next_u32(&mut self) -> u32 {
        if self.mti >= N {
            self.twist();
        }
        let mut y = self.mt[self.mti];
        self.mti += 1;
        y ^= y >> 11;
        y ^= (y << 7) & 0x9d2c_5680;
        y ^= (y << 15) & 0xefc6_0000;
        y ^= y >> 18;
        y
    }

    fn next_u64(&mut self) -> u64 {
        let lo = u64::from(self.next_u32());
        let hi = u64::from(self.next_u32());
        (hi << 32) | lo
    }

    fn fill_bytes(&mut self, dst: &mut [u8]) {
        for chunk in dst.chunks_mut(4) {
            let bytes = self.next_u32().to_le_bytes();
            chunk.copy_from_slice(&bytes[..chunk.len()]);
        }
    }
}

/// R's `unif_rand()` given a raw 32-bit MT output: scale and keep inside `(0, 1)`.
pub fn r_unif_from_u32(y: u32) -> f64 {
    let x = f64::from(y) * MT_SCALE;
    if x <= 0.0 {
        0.5 * I2_32M1
    } else if 1.0 - x <= 0.0 {
        1.0 - 0.5 * I2_32M1
    } else {
        x
    }
}

/// R (>= 4.0.0) `round(x, digits)` for `digits > 0`: pick the nearer of the
/// two decimal candidates `floor(x*10^d)/10^d` and `ceil(x*10^d)/10^d`,
/// ties to the even last digit.
pub fn r_round(x: f64, digits: i32) -> f64 {
    if x.is_nan() || !x.is_finite() || x == 0.0 {
        return x;
    }
    if digits == 0 {
        return x.round_ties_even();
    }
    let (sgn, x) = if x < 0.0 { (-1.0, -x) } else { (1.0, x) };
    // Enough digits that no rounding can happen (R: DBL_DIG = 15).
    if f64::from(digits) + x.log10() > 15.0 {
        return sgn * x;
    }
    let p10 = 10f64.powi(digits);
    let x10 = p10 * x;
    let i10 = x10.floor();
    let xd = i10 / p10;
    let xu = x10.ceil() / p10;
    let du = xu - x;
    let dd = x - xd;
    sgn * if du < dd || (du == dd && i10 % 2.0 == 1.0) {
        xu
    } else {
        xd
    }
}
