//! `StrictMath.log1p` (fdlibm `s_log1p.c`), which `Math.log1p` delegates
//! to: the geometric name distribution floors a quotient of two of them.

#![allow(clippy::excessive_precision)] // the constants exactly as fdlibm writes them
const LN2_HI: f64 = 6.931_471_803_691_238_164_90e-01;
const LN2_LO: f64 = 1.908_214_929_270_587_700_02e-10;
const TWO54: f64 = 1.801_439_850_948_198_400_00e+16;
const LP1: f64 = 6.666_666_666_666_735_130e-01;
const LP2: f64 = 3.999_999_999_940_941_908e-01;
const LP3: f64 = 2.857_142_874_366_239_149e-01;
const LP4: f64 = 2.222_219_843_214_978_396e-01;
const LP5: f64 = 1.818_357_216_161_805_012e-01;
const LP6: f64 = 1.531_383_769_920_937_332e-01;
const LP7: f64 = 1.479_819_860_511_658_591e-01;

fn hi(x: f64) -> i32 {
    (x.to_bits() >> 32) as i32
}

fn with_hi(x: f64, h: i32) -> f64 {
    f64::from_bits(((h as u32 as u64) << 32) | (x.to_bits() & 0xFFFF_FFFF))
}

/// fdlibm `log1p`.
#[allow(clippy::excessive_precision)]
pub fn log1p(x: f64) -> f64 {
    let hx = hi(x);
    let ax = hx & 0x7fff_ffff;
    let mut k = 1i32;
    let mut f = 0.0;
    let mut hu = 0i32;
    let mut c = 0.0;
    if hx < 0x3FDA_827A {
        // x < 0.41422
        if ax >= 0x3ff0_0000 {
            // x <= -1.0
            if x == -1.0 {
                return f64::NEG_INFINITY;
            }
            return f64::NAN;
        }
        if ax < 0x3e20_0000 {
            // |x| < 2**-29
            if TWO54 + x > 0.0 && ax < 0x3c90_0000 {
                return x;
            }
            return x - x * x * 0.5;
        }
        if hx > 0 || hx <= 0xbfd2_bec3_u32 as i32 {
            // -0.2929 < x < 0.41422
            k = 0;
            f = x;
            hu = 1;
        }
    }
    if hx >= 0x7ff0_0000 {
        return x + x;
    }
    if k != 0 {
        let u;
        if hx < 0x4340_0000 {
            u = 1.0 + x;
            hu = hi(u);
            k = (hu >> 20) - 1023;
            c = if k > 0 { 1.0 - (u - x) } else { x - (u - 1.0) };
            c /= u;
        } else {
            u = x;
            hu = hi(u);
            k = (hu >> 20) - 1023;
            c = 0.0;
        }
        hu &= 0x000f_ffff;
        let u = if hu < 0x6_a09e {
            with_hi(u, hu | 0x3ff0_0000)
        } else {
            k += 1;
            let u = with_hi(u, hu | 0x3fe0_0000);
            hu = (0x0010_0000 - hu) >> 2;
            u
        };
        f = u - 1.0;
    }
    let hfsq = 0.5 * f * f;
    let kf = k as f64;
    if hu == 0 {
        // |f| < 2**-20
        if f == 0.0 {
            if k == 0 {
                return 0.0;
            }
            c += kf * LN2_LO;
            return kf * LN2_HI + c;
        }
        let r = hfsq * (1.0 - 0.666_666_666_666_666_66 * f);
        if k == 0 {
            return f - r;
        }
        return kf * LN2_HI - ((r - (kf * LN2_LO + c)) - f);
    }
    let s = f / (2.0 + f);
    let z = s * s;
    let r = z * (LP1 + z * (LP2 + z * (LP3 + z * (LP4 + z * (LP5 + z * (LP6 + z * LP7))))));
    if k == 0 {
        f - (hfsq - s * (hfsq + r))
    } else {
        kf * LN2_HI - ((hfsq - (s * (hfsq + r) + (kf * LN2_LO + c))) - f)
    }
}

/// `(int) Math.floor(a / b)`.
pub fn floor_div(a: f64, b: f64) -> i32 {
    (a / b).floor() as i32
}

#[cfg(test)]
mod tests {
    #[test]
    fn close_to_libm() {
        let mut x = -0.999;
        while x < 10.0 {
            let a = super::log1p(x);
            let b = f64::ln_1p(x);
            assert!(
                (a - b).abs() <= 2.0 * f64::EPSILON * b.abs().max(1e-300),
                "{x}: {a} {b}"
            );
            x += 0.000_37;
        }
    }
}
