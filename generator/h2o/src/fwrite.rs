// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
// writeFloat64 is ported from data.table (src/fwrite.c); round6 follows
// R 4.x's fround (GPL-2.0-or-later). See ../NOTICE.md.

//! The text R writes: `round(x, 6)` (R 4.x's `fround`) and data.table
//! `fwrite`'s number formatting.
//!
//! [`write_f64`] is `writeFloat64` from data.table's `fwrite.c` (15
//! significant digits, built from its own power tables, so not always the
//! shortest round-trip form). The datagen scripts only ever write
//! `round(u, 6)` with `u` in `[0, 100]`, i.e. `k / 1e6` for an integer
//! `k <= 1e8`; for those [`write_micros`] prints `k` with six implied
//! decimals, which the tests check equals `write_f64(k / 1e6)` for every
//! such `k`.

use std::ptr::copy_nonoverlapping;

use crate::fwrite_tables::{EXPPOW, EXPSIG};

/// R's `round(x, 6)` for finite `x > 0` with `log10(x) + 6 <= 15`, as the
/// integer `k` with `round(x, 6) == k as f64 / 1e6`.
#[inline(always)]
pub fn round6_micros(x: f64) -> u64 {
    const P: f64 = 1e6;
    let x10 = x * P;
    let i10 = x10.floor();
    let c10 = x10.ceil();
    let xd = i10 / P;
    let xu = c10 / P;
    let du = xu - x;
    let dd = x - xd;
    // R: (du < dd || (fmod(i10, 2.) == 1 && du == dd)) ? xu : xd
    let up = du < dd || (du == dd && (i10 as u64) & 1 == 1);
    if up { c10 as u64 } else { i10 as u64 }
}

const NUM_SF: i32 = 15;
const SIZE_SF: f64 = 1e15;

/// data.table's `writeFloat64` (`fwrite` defaults: `dec = "."`,
/// `scipen = 0`, `na = ""`), appending to `out`.
pub fn write_f64(x: f64, out: &mut Vec<u8>) {
    if x.is_nan() {
        return;
    }
    if x.is_infinite() {
        if x < 0.0 {
            out.push(b'-');
        }
        out.extend_from_slice(b"Inf");
        return;
    }
    if x == 0.0 {
        out.push(b'0');
        return;
    }
    let mut x = x;
    let neg = x < 0.0;
    if neg {
        x = -x;
    }
    let bits = x.to_bits();
    let mut fraction = bits & 0xF_FFFF_FFFF_FFFF;
    let exponent = ((bits >> 52) & 0x7FF) as usize;
    let mut acc = 0.0f64;
    let mut i = 52i32;
    if fraction != 0 {
        while fraction & 0xFF == 0 {
            fraction >>= 8;
            i -= 8;
        }
        while fraction != 0 {
            if fraction & 1 == 1 {
                // sigparts[i] = 2^-i, exactly
                acc += f64::from_bits(((1023 - i) as u64) << 52);
            }
            i -= 1;
            fraction >>= 1;
        }
    }
    let mut y = (1.0 + acc) * f64::from_bits(EXPSIG[exponent]);
    let mut exp = EXPPOW[exponent] as i32;
    if y >= 9.999_999_999_999_99 {
        y /= 10.0;
        exp += 1;
    }
    let mut l = (y * SIZE_SF) as u64;
    if l % 10 >= 5 {
        l += 10;
    }
    l /= 10;
    if l == 0 {
        out.push(b'0');
        return;
    }
    if neg {
        out.push(b'-');
    }
    let mut trail = 0;
    while l.is_multiple_of(10) {
        l /= 10;
        trail += 1;
    }
    let mut sf = NUM_SF - trail;
    if sf == 0 {
        sf = 1;
        exp += 1;
    }
    let mut dr = sf - exp - 1;
    let width;
    let mut dl0 = 0;
    if dr <= 0 {
        dl0 = -dr;
        dr = 0;
        width = sf + dl0;
    } else if sf > dr {
        width = sf + 1;
    } else {
        dl0 = 1;
        width = dr + 1 + dl0;
    }
    let digits = |mut l: u64, sf: i32| -> Vec<u8> {
        let mut d = vec![0u8; sf as usize];
        for k in (0..sf as usize).rev() {
            d[k] = b'0' + (l % 10) as u8;
            l /= 10;
        }
        d
    };
    let ex = if exp.abs() > 99 { 3 } else { 2 };
    if width <= sf + (sf > 1) as i32 + 2 + ex {
        // decimal: [sf digits][dl0 zeros] or [int digits].[frac], or
        // 0.[zeros][digits]
        let mut buf = vec![0u8; width as usize];
        let mut p = width as usize; // write right to left
        let mut l = l;
        let mut sf = sf;
        if dr > 0 {
            while dr > 0 && sf > 0 {
                p -= 1;
                buf[p] = b'0' + (l % 10) as u8;
                l /= 10;
                dr -= 1;
                sf -= 1;
            }
            while dr > 0 {
                p -= 1;
                buf[p] = b'0';
                dr -= 1;
            }
            p -= 1;
            buf[p] = b'.';
        }
        while dl0 > 0 {
            p -= 1;
            buf[p] = b'0';
            dl0 -= 1;
        }
        while sf > 0 {
            p -= 1;
            buf[p] = b'0' + (l % 10) as u8;
            l /= 10;
            sf -= 1;
        }
        debug_assert_eq!(p, 0);
        out.extend_from_slice(&buf);
    } else {
        let d = digits(l, sf);
        out.push(d[0]);
        if sf > 1 {
            out.push(b'.');
            out.extend_from_slice(&d[1..]);
        }
        out.push(b'e');
        let mut e = exp;
        if e < 0 {
            out.push(b'-');
            e = -e;
        } else {
            out.push(b'+');
        }
        if e < 100 {
            out.push(b'0' + (e / 10) as u8);
            out.push(b'0' + (e % 10) as u8);
        } else {
            out.push(b'0' + (e / 100) as u8);
            out.push(b'0' + (e / 10 % 10) as u8);
            out.push(b'0' + (e % 10) as u8);
        }
    }
}

/// `fwrite` of `k / 1e6` for `k <= 1e8`: `k` with six implied decimals,
/// trailing zeros (and a bare point) dropped. Values below 0.001, which
/// `fwrite` may print in scientific notation, take [`write_f64`]. Writes
/// at most 16 bytes at `out` and returns how many.
///
/// # Safety
/// `out` must be valid for 16 bytes of writes.
#[inline(always)]
pub unsafe fn write_micros(k: u64, out: *mut u8) -> usize {
    if k < 1000 {
        return unsafe { write_micros_small(k, out) };
    }
    let int = k / 1_000_000;
    let mut frac = (k % 1_000_000) as u32;
    unsafe {
        let mut n = write_u32(int as u32, out);
        if frac != 0 {
            // six digits, then drop trailing zeros
            let mut nd = 6;
            while frac.is_multiple_of(10) {
                frac /= 10;
                nd -= 1;
            }
            *out.add(n) = b'.';
            n += 1;
            let end = n + nd;
            let mut p = end;
            while p > n {
                p -= 1;
                *out.add(p) = b'0' + (frac % 10) as u8;
                frac /= 10;
            }
            n = end;
        }
        n
    }
}

#[cold]
unsafe fn write_micros_small(k: u64, out: *mut u8) -> usize {
    let mut v = Vec::with_capacity(16);
    write_f64(k as f64 / 1e6, &mut v);
    unsafe { copy_nonoverlapping(v.as_ptr(), out, v.len()) };
    v.len()
}

/// Decimal digits of `v`. Writes at most 10 bytes at `out`, returns how
/// many.
///
/// # Safety
/// `out` must be valid for 10 bytes of writes.
#[inline(always)]
pub unsafe fn write_u32(mut v: u32, out: *mut u8) -> usize {
    let n = digits(v);
    unsafe {
        let mut p = n;
        while v >= 100 {
            let d = (v % 100) as usize * 2;
            v /= 100;
            p -= 2;
            copy_nonoverlapping(PAIRS.as_ptr().add(d), out.add(p), 2);
        }
        if v >= 10 {
            let d = v as usize * 2;
            copy_nonoverlapping(PAIRS.as_ptr().add(d), out, 2);
        } else {
            *out = b'0' + v as u8;
        }
    }
    n
}

/// `v` zero-padded to `width` digits (more if it needs them), as
/// `sprintf("%0<width>d", v)`. Returns the length.
///
/// # Safety
/// `out` must be valid for `max(width, 10)` bytes of writes.
#[inline(always)]
pub unsafe fn write_u32_padded(mut v: u32, width: usize, out: *mut u8) -> usize {
    let n = digits(v).max(width);
    unsafe {
        let mut p = n;
        while p >= 2 && v > 0 {
            let d = (v % 100) as usize * 2;
            v /= 100;
            p -= 2;
            copy_nonoverlapping(PAIRS.as_ptr().add(d), out.add(p), 2);
        }
        while p > 0 {
            p -= 1;
            *out.add(p) = b'0' + (v % 10) as u8;
            v /= 10;
        }
    }
    n
}

/// Number of decimal digits of `v` (1 for 0).
#[inline(always)]
pub fn digits(v: u32) -> usize {
    // floor(log10) via the bit length, then one correction
    const POW: [u32; 10] = [
        1,
        10,
        100,
        1_000,
        10_000,
        100_000,
        1_000_000,
        10_000_000,
        100_000_000,
        1_000_000_000,
    ];
    let bits = 32 - (v | 1).leading_zeros();
    let guess = ((bits * 1233) >> 12) as usize; // ~ bits * log10(2)
    guess + (guess < 10 && (v | 1) >= POW[guess]) as usize
}

static PAIRS: [u8; 200] = {
    let mut t = [0u8; 200];
    let mut i = 0;
    while i < 100 {
        t[2 * i] = b'0' + (i / 10) as u8;
        t[2 * i + 1] = b'0' + (i % 10) as u8;
        i += 1;
    }
    t
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rng::RMt;

    fn fw(x: f64) -> String {
        let mut v = Vec::new();
        write_f64(x, &mut v);
        String::from_utf8(v).unwrap()
    }

    fn micros(k: u64) -> String {
        let mut b = [0u8; 16];
        let n = unsafe { write_micros(k, b.as_mut_ptr()) };
        String::from_utf8(b[..n].to_vec()).unwrap()
    }

    // fwrite(data.table(x = c(...))) in R 4.3 / data.table 1.14
    #[test]
    fn fwrite_numbers() {
        assert_eq!(fw(47.662102), "47.662102");
        assert_eq!(fw(0.5), "0.5");
        assert_eq!(fw(100.0), "100");
        assert_eq!(fw(1e-6), "1e-06");
        assert_eq!(fw(1.5e-5), "1.5e-05");
        assert_eq!(fw(0.0001), "1e-04");
        assert_eq!(fw(0.001), "0.001");
        assert_eq!(fw(123456.0), "123456");
        assert_eq!(fw(-2.25), "-2.25");
        assert_eq!(fw(1.0 / 3.0), "0.333333333333333");
    }

    #[test]
    fn digits_and_ints() {
        for v in [0u32, 1, 9, 10, 99, 100, 999_999, 1_000_000, u32::MAX] {
            assert_eq!(digits(v), v.to_string().len(), "{v}");
            let mut b = [0u8; 16];
            let n = unsafe { write_u32(v, b.as_mut_ptr()) };
            assert_eq!(&b[..n], v.to_string().as_bytes());
            let n = unsafe { write_u32_padded(v, 10, b.as_mut_ptr()) };
            assert_eq!(&b[..n], format!("{v:010}").as_bytes());
            let n = unsafe { write_u32_padded(v, 3, b.as_mut_ptr()) };
            assert_eq!(&b[..n], format!("{v:03}").as_bytes());
        }
    }

    #[test]
    fn round6_matches_r() {
        // set.seed(108); sprintf("%.17g", round(runif(3, max=100), 6))
        let mut r = RMt::set_seed(108);
        let got: Vec<u64> = (0..3)
            .map(|_| round6_micros(100.0 * r.unif_rand()))
            .collect();
        assert_eq!(got, [45_505_804, 40_396_106, 35_129_767]);
    }

    /// Every `k` the scripts can write: `cargo test -p populate-h2o --release
    /// -- --ignored exhaustive_micros` (~10 s).
    #[test]
    #[ignore]
    fn exhaustive_micros() {
        let mut a = Vec::with_capacity(32);
        for k in 0..=100_000_000u64 {
            a.clear();
            write_f64(k as f64 / 1e6, &mut a);
            let mut b = [0u8; 16];
            let n = unsafe { write_micros(k, b.as_mut_ptr()) };
            assert_eq!(&b[..n], &a[..], "k = {k}");
        }
    }

    /// `write_micros(k) == fwrite(k / 1e6)` on a sample of `k` (the
    /// `exhaustive_micros` test checks all 1e8 + 1).
    #[test]
    fn micros_sample() {
        let mut k = 0u64;
        while k <= 100_000_000 {
            assert_eq!(micros(k), fw(k as f64 / 1e6), "k = {k}");
            k += 9_973;
        }
        for k in [0, 1, 10, 100, 1000, 10_000, 99_999_999, 100_000_000] {
            assert_eq!(micros(k), fw(k as f64 / 1e6), "k = {k}");
        }
    }
}
