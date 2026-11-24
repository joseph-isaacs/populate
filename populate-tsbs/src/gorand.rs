//! Go's `math/rand` (v1), bit for bit: the additive lagged Fibonacci source
//! that `rand.Seed` installs, the `Rand` methods TSBS calls, `NormFloat64`'s
//! ziggurat and `Zipf`. Plus `math.Exp` and `math.Log` as Go's amd64
//! assembly computes them, since their last bits reach the output.
//!
//! Ported from the Go 1.24 sources (BSD-style licence, The Go Authors).

use crate::gotables::{FN, KN, RNG_COOKED, WN};
use std::sync::OnceLock;

const RNG_LEN: usize = 607;
const RNG_TAP: usize = 273;
const INT32_MAX: i64 = (1 << 31) - 1;

/// Go's `rngSource` wrapped in a `Rand`: `rand.Seed(seed)` followed by the
/// top-level `rand.*` functions.
#[derive(Clone)]
pub struct GoRand {
    tap: usize,
    feed: usize,
    vec: Box<[i64; RNG_LEN]>,
}

impl std::fmt::Debug for GoRand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GoRand")
            .field("tap", &self.tap)
            .field("feed", &self.feed)
            .finish_non_exhaustive()
    }
}

/// `seedrand`: x[n+1] = 48271 * x[n] mod (2**31 - 1).
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
    /// `rand.New(rand.NewSource(seed))`.
    pub fn new(seed: i64) -> Self {
        let mut seed = seed % INT32_MAX;
        if seed < 0 {
            seed += INT32_MAX;
        }
        if seed == 0 {
            seed = 89_482_311;
        }
        let mut x = seed as i32;
        let mut vec = Box::new([0i64; RNG_LEN]);
        for i in -20i32..RNG_LEN as i32 {
            x = seedrand(x);
            if i >= 0 {
                let mut u = (x as i64) << 40;
                x = seedrand(x);
                u ^= (x as i64) << 20;
                x = seedrand(x);
                u ^= x as i64;
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

    /// `Uint64`: one step of the generator.
    #[inline(always)]
    pub fn uint64(&mut self) -> u64 {
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

    /// `Int63`.
    #[inline(always)]
    pub fn int63(&mut self) -> i64 {
        (self.uint64() & (u64::MAX >> 1)) as i64
    }

    /// `Uint32`.
    #[inline(always)]
    pub fn uint32(&mut self) -> u32 {
        (self.int63() >> 31) as u32
    }

    /// `Int31`.
    #[inline(always)]
    pub fn int31(&mut self) -> i32 {
        (self.int63() >> 32) as i32
    }

    /// `Int63n`.
    pub fn int63n(&mut self, n: i64) -> i64 {
        assert!(n > 0, "invalid argument to Int63n");
        if n & (n - 1) == 0 {
            return self.int63() & (n - 1);
        }
        let max = (i64::MAX as u64 - (1u64 << 63) % n as u64) as i64;
        let mut v = self.int63();
        while v > max {
            v = self.int63();
        }
        v % n
    }

    /// `Int31n`.
    pub fn int31n(&mut self, n: i32) -> i32 {
        assert!(n > 0, "invalid argument to Int31n");
        if n & (n - 1) == 0 {
            return self.int31() & (n - 1);
        }
        let max = (i32::MAX as u32 - (1u32 << 31) % n as u32) as i32;
        let mut v = self.int31();
        while v > max {
            v = self.int31();
        }
        v % n
    }

    /// `Intn`.
    pub fn intn(&mut self, n: usize) -> usize {
        assert!(n > 0, "invalid argument to Intn");
        if n <= i32::MAX as usize {
            self.int31n(n as i32) as usize
        } else {
            self.int63n(n as i64) as usize
        }
    }

    /// `Float64`.
    #[inline(always)]
    pub fn float64(&mut self) -> f64 {
        loop {
            let f = self.int63() as f64 / (1u64 << 63) as f64;
            if f != 1.0 {
                return f;
            }
        }
    }

    /// `NormFloat64` (Marsaglia & Tsang's ziggurat).
    #[inline(always)]
    pub fn norm_float64(&mut self) -> f64 {
        let j = self.uint32() as i32;
        let i = (j & 0x7F) as usize;
        let x = j as f64 * WN[i] as f64;
        if j.unsigned_abs() < KN[i] {
            return x;
        }
        self.norm_slow(j, i, x)
    }

    #[cold]
    fn norm_slow(&mut self, mut j: i32, mut i: usize, mut x: f64) -> f64 {
        const RN: f64 = 3.442619855899;
        // the constant expression `1.0 / rn`, rounded once
        const RN_INV: f64 = 0.290_476_451_614_743_17;
        loop {
            if i == 0 {
                loop {
                    x = -go_log(self.float64()) * RN_INV;
                    let y = -go_log(self.float64());
                    if y + y >= x * x {
                        break;
                    }
                }
                return if j > 0 { RN + x } else { -RN - x };
            }
            if FN[i] + (self.float64() as f32) * (FN[i - 1] - FN[i]) < go_exp(-0.5 * x * x) as f32 {
                return x;
            }
            j = self.uint32() as i32;
            i = (j & 0x7F) as usize;
            x = j as f64 * WN[i] as f64;
            if j.unsigned_abs() < KN[i] {
                return x;
            }
        }
    }
}

/// Go's `rand.Zipf`.
#[derive(Debug, Clone)]
pub struct Zipf {
    imax: f64,
    v: f64,
    q: f64,
    s: f64,
    oneminus_q: f64,
    oneminus_qinv: f64,
    hxm: f64,
    hx0_minus_hxm: f64,
}

impl Zipf {
    /// `rand.NewZipf(r, s, v, imax)`; `None` where Go returns nil.
    pub fn new(s: f64, v: f64, imax: u64) -> Option<Self> {
        if s <= 1.0 || v < 1.0 {
            return None;
        }
        let mut z = Zipf {
            imax: imax as f64,
            v,
            q: s,
            s: 0.0,
            oneminus_q: 1.0 - s,
            oneminus_qinv: 0.0,
            hxm: 0.0,
            hx0_minus_hxm: 0.0,
        };
        z.oneminus_qinv = 1.0 / z.oneminus_q;
        z.hxm = z.h(z.imax + 0.5);
        z.hx0_minus_hxm = z.h(0.5) - go_exp(go_log(z.v) * (-z.q)) - z.hxm;
        z.s = 1.0 - z.hinv(z.h(1.5) - go_exp(-z.q * go_log(z.v + 1.0)));
        Some(z)
    }

    fn h(&self, x: f64) -> f64 {
        go_exp(self.oneminus_q * go_log(self.v + x)) * self.oneminus_qinv
    }

    fn hinv(&self, x: f64) -> f64 {
        go_exp(self.oneminus_qinv * go_log(self.oneminus_q * x)) - self.v
    }

    /// `Uint64`, drawing from `r`.
    pub fn uint64(&self, r: &mut GoRand) -> u64 {
        let mut k;
        loop {
            let rr = r.float64();
            let ur = self.hxm + rr * self.hx0_minus_hxm;
            let x = self.hinv(ur);
            k = (x + 0.5).floor();
            if k - x <= self.s {
                break;
            }
            if ur >= self.h(k + 0.5) - go_exp(-go_log(k + self.v) * self.q) {
                break;
            }
        }
        go_f64_to_u64(k)
    }
}

/// Go's `int64(f)` on amd64 (`CVTTSD2SQ`): truncation, and
/// `0x8000000000000000` for NaN and out-of-range values.
#[inline(always)]
pub fn go_f64_to_i64(f: f64) -> i64 {
    if (-9_223_372_036_854_775_808.0..9_223_372_036_854_775_808.0).contains(&f) {
        f as i64
    } else {
        i64::MIN
    }
}

/// Go's `uint64(f)` on amd64: below 2**63 a signed conversion, otherwise
/// a signed conversion of `f - 2**63` with the top bit flipped.
pub fn go_f64_to_u64(f: f64) -> u64 {
    const TWO63: f64 = 9_223_372_036_854_775_808.0;
    if f < TWO63 {
        go_f64_to_i64(f) as u64
    } else {
        (go_f64_to_i64(f - TWO63) as u64) ^ (1 << 63)
    }
}

fn use_fma() -> bool {
    static FMA: OnceLock<bool> = OnceLock::new();
    *FMA.get_or_init(|| {
        #[cfg(target_arch = "x86_64")]
        {
            std::arch::is_x86_feature_detected!("avx") && std::arch::is_x86_feature_detected!("fma")
        }
        #[cfg(not(target_arch = "x86_64"))]
        {
            false
        }
    })
}

/// Go's `math.Exp` as `exp_amd64.s` computes it (Shibata's method), with
/// the FMA variant on CPUs with AVX and FMA, as Go picks at run time.
// the constants are exp_amd64.s's, digit for digit
#[allow(clippy::excessive_precision, clippy::approx_constant)]
pub fn go_exp(x: f64) -> f64 {
    const LN2U: f64 = 0.693_147_180_559_662_956_511_601_805_686_950_683_593_75;
    const LN2L: f64 = 0.282_352_905_630_315_771_225_884_481_750_134_360_255_254_120_68e-12;
    const LOG2E: f64 = 1.442_695_040_888_963_407_359_924_681_001_892_0;
    const OVERFLOW: f64 = 7.097_827_128_933_84e+02;
    const C: [f64; 9] = [
        0.5,
        1.0,
        2.0,
        1.666_666_666_666_666_666_7e-1,
        4.166_666_666_666_666_666_7e-2,
        8.333_333_333_333_333_333_3e-3,
        1.388_888_888_888_888_888_9e-3,
        1.984_126_984_126_984_127_0e-4,
        2.480_158_730_158_730_158_7e-5,
    ];
    const POS_INF: u64 = 0x7FF0_0000_0000_0000;
    const NEG_INF: u64 = 0xFFF0_0000_0000_0000;
    let bits = x.to_bits();
    if (bits & !(1 << 63)) as i64 >= POS_INF as i64 {
        return if bits == NEG_INF { 0.0 } else { x };
    }
    if x > OVERFLOW {
        return f64::INFINITY;
    }
    // CVTSD2SL: round to nearest even; "integer indefinite" out of range
    let t = (LOG2E * x).round_ties_even();
    let mut bx: i32 = if (-2_147_483_648.0..2_147_483_648.0).contains(&t) {
        t as i32
    } else {
        i32::MIN
    };
    let k = bx as f64;
    let mut x0 = x;
    if use_fma() {
        x0 = (-LN2U).mul_add(k, x0);
        x0 = (-LN2L).mul_add(k, x0);
        x0 *= 0.0625;
        let mut x1 = C[8];
        for c in [C[7], C[6], C[5], C[4], C[3], C[0], C[1]] {
            x1 = x1.mul_add(x0, c);
        }
        x0 *= x1;
        for _ in 0..3 {
            let x1 = x0 + C[2];
            x0 *= x1;
        }
        let x1 = x0 + C[2];
        x0 = x0.mul_add(x1, C[1]);
    } else {
        x0 -= LN2U * k;
        x0 -= LN2L * k;
        x0 *= 0.0625;
        let mut x1 = C[8];
        for c in [C[7], C[6], C[5], C[4], C[3], C[0], C[1]] {
            x1 *= x0;
            x1 += c;
        }
        x0 *= x1;
        for _ in 0..4 {
            let x1 = C[2] + x0;
            x0 *= x1;
        }
        x0 += C[1];
    }
    // ldexp
    bx = bx.wrapping_add(0x3FF);
    if bx <= 0 {
        if bx < -52 {
            return 0.0;
        }
        bx += 0x3FE;
        x0 *= f64::from_bits((bx as u32 as u64) << 52);
        bx = 1;
    } else if bx >= 0x7FF {
        return f64::INFINITY;
    }
    x0 * f64::from_bits((bx as u32 as u64) << 52)
}

/// Go's `math.Log` as `log_amd64.s` computes it.
#[allow(clippy::excessive_precision, clippy::approx_constant)]
pub fn go_log(x: f64) -> f64 {
    const HSQRT2: f64 = 7.071_067_811_865_475_244_01e-01;
    let ln2hi = f64::from_bits(0x3fe6_2e42_fee0_0000);
    let ln2lo = f64::from_bits(0x3dea_39ef_3579_3c76);
    let l1 = f64::from_bits(0x3FE5_5555_5555_5593);
    let l2 = f64::from_bits(0x3FD9_9999_9997_FA04);
    let l3 = f64::from_bits(0x3FD2_4924_9422_9359);
    let l4 = f64::from_bits(0x3FCC_71C5_1D8E_78AF);
    let l5 = f64::from_bits(0x3FC7_4664_96CB_03DE);
    let l6 = f64::from_bits(0x3FC3_9A09_D078_C69F);
    let l7 = f64::from_bits(0x3FC2_F112_DF3E_5244);
    let bits = x.to_bits();
    if bits & !(1 << 63) == 0 {
        return f64::NEG_INFINITY;
    }
    if (bits as i64) < 0 {
        return f64::from_bits(0x7FF8_0000_0000_0001);
    }
    if bits >= 0x7FF0_0000_0000_0000 {
        return x;
    }
    let mut f1 = f64::from_bits((bits & 0x000F_FFFF_FFFF_FFFF) | 0x3FE0_0000_0000_0000);
    let mut k = (((bits >> 52) & 0x7FF) as i32 - 0x3FE) as f64;
    // CMPSD NLT: adjust unless sqrt(2)/2 < f1
    let adj = if HSQRT2 < f1 { 0.0 } else { 1.0 };
    k -= adj;
    f1 *= adj + 1.0;
    let f = f1 - 1.0;
    let s = f / (2.0 + f);
    let s2 = s * s;
    let s4 = s2 * s2;
    let t1 = s2 * (((l7 * s4 + l5) * s4 + l3) * s4 + l1);
    let t2 = s4 * ((l6 * s4 + l4) * s4 + l2);
    let r = t1 + t2;
    let hfsq = 0.5 * f * f;
    let a = s * (r + hfsq) + ln2lo * k;
    let b = (hfsq - a) - f;
    k * ln2hi - b
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exp_log_basics() {
        assert_eq!(go_exp(0.0), 1.0);
        assert_eq!(go_log(1.0), 0.0);
        assert!((go_exp(1.0) - std::f64::consts::E).abs() < 1e-15);
        assert!((go_log(10.0) - std::f64::consts::LN_10).abs() < 1e-15);
        assert_eq!(go_exp(-1e300), 0.0);
        assert_eq!(go_exp(1000.0), f64::INFINITY);
    }
}
