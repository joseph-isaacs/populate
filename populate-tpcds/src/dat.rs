//! dsdgen's `.dat` text format, written straight from Arrow batches.
//!
//! One line per row: every field followed by `|`, NULL as an empty field,
//! decimals with exactly `scale` digits (`-0.05`), dates as `YYYY-MM-DD`.
//! This is ~10x faster than formatting tpcdsgen's rows with `Display`, and
//! the test suite checks it against that `Display` output for every table.

use arrow_array::cast::AsArray;
use arrow_array::types::{Date32Type, Decimal128Type, Int32Type, Int64Type};
use arrow_array::{Array, RecordBatch, StringViewArray};
use arrow_buffer::NullBuffer;
use arrow_schema::DataType;

/// Text encoding of `.dat` output.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Encoding {
    /// ISO-8859-1, one byte per character: what dsdgen writes, since it
    /// copies its (Latin-1) distribution files byte for byte. Only
    /// `customer.c_birth_country` has non-ASCII values (`CÔTE D'IVOIRE`,
    /// `RÉUNION`).
    #[default]
    Latin1,
    /// UTF-8, as `tpcgen-cli` writes.
    Utf8,
}

enum Col<'a> {
    I32(&'a [i32]),
    I64(&'a [i64]),
    /// Values, their scale, and the scale dsdgen prints.
    Dec(&'a [i128], u32, u32),
    Date(&'a [i32]),
    Str(&'a StringViewArray),
}

/// Append `batch` to `out` as `.dat` lines.
pub fn write_batch(batch: &RecordBatch, encoding: Encoding, out: &mut Vec<u8>) {
    let rows = batch.num_rows();
    let mut bound = rows; // newlines
    let cols: Vec<(Col<'_>, Option<Valid>)> = batch
        .columns()
        .iter()
        .zip(batch.schema_ref().fields())
        .map(|(a, f)| {
            let c = match a.data_type() {
                DataType::Int32 => Col::I32(a.as_primitive::<Int32Type>().values()),
                DataType::Int64 => Col::I64(a.as_primitive::<Int64Type>().values()),
                DataType::Decimal128(_, s) => {
                    // dsdgen keeps GMT offsets as decimal(5,2) but prints them
                    // with no decimals (`-5`)
                    let print = if f.name().ends_with("gmt_offset") {
                        0
                    } else {
                        *s as u32
                    };
                    Col::Dec(
                        a.as_primitive::<Decimal128Type>().values(),
                        *s as u32,
                        print,
                    )
                }
                DataType::Date32 => Col::Date(a.as_primitive::<Date32Type>().values()),
                DataType::Utf8View => Col::Str(a.as_string_view()),
                t => panic!("no .dat format for {t}"),
            };
            bound += rows
                * match &c {
                    Col::Str(a) => {
                        bound += a.views().iter().map(|v| *v as u32 as usize).sum::<usize>();
                        1 + 12 // separator, inline-copy slack
                    }
                    _ => MAX_NUMBER + 1,
                };
            (c, a.nulls().filter(|n| n.null_count() > 0).map(Valid::new))
        })
        .collect();
    out.reserve(bound);
    let start = out.len();
    let mut w = Cursor(out.spare_capacity_mut().as_mut_ptr().cast::<u8>());
    let base = w.0;
    for r in 0..rows {
        for (c, nulls) in &cols {
            // SAFETY: `r` is a row of the batch.
            if nulls.as_ref().is_none_or(|n| unsafe { n.get(r) }) {
                // SAFETY: `bound` reserved room for the widest number and the
                // full bytes of every string (Latin-1 never needs more bytes
                // than UTF-8), plus every separator and newline.
                // Every column has `rows` values, so `r` is in bounds.
                unsafe {
                    match c {
                        Col::I32(v) => w.int(*v.get_unchecked(r) as i64),
                        Col::I64(v) => w.int(*v.get_unchecked(r)),
                        Col::Dec(v, 2, 2) => w.decimal2(*v.get_unchecked(r) as i64),
                        Col::Dec(v, s, p) => {
                            w.decimal(*v.get_unchecked(r) as i64 / 10i64.pow(s - p), *p)
                        }
                        Col::Date(v) => w.date(*v.get_unchecked(r)),
                        Col::Str(a) => w.view(a, r, encoding),
                    }
                }
            }
            unsafe { w.byte(b'|') };
        }
        unsafe { w.byte(b'\n') };
    }
    // SAFETY: every byte in `start..start + written` was initialised above.
    unsafe {
        let written = w.0.offset_from(base) as usize;
        debug_assert!(written <= bound);
        out.set_len(start + written);
    }
}

/// Append `v` and its `|`, as [`write_batch`] writes an integer field.
#[inline]
pub(crate) fn int_field(out: &mut Vec<u8>, v: i64) {
    field(out, |w| unsafe { w.int(v) });
}

/// Append the decimal `v / 100` and its `|`, as [`write_batch`] writes a
/// decimal(_, 2) field.
#[inline]
pub(crate) fn decimal2_field(out: &mut Vec<u8>, v: i64) {
    field(out, |w| unsafe { w.decimal2(v) });
}

/// Run `f` (which writes at most one number) on a cursor at the end of
/// `out`, then append `|`.
#[inline(always)]
fn field(out: &mut Vec<u8>, f: impl FnOnce(&mut Cursor)) {
    out.reserve(MAX_NUMBER + 1);
    let len = out.len();
    // SAFETY: `f` writes at most `MAX_NUMBER` bytes (with the 8-byte stores
    // of `uint`, which fit in it too), then one `|`, into reserved
    // capacity; all of it is initialised before `set_len`.
    unsafe {
        let mut w = Cursor(out.as_mut_ptr().add(len));
        f(&mut w);
        w.byte(b'|');
        let n = w.0.offset_from(out.as_ptr()) as usize;
        out.set_len(n);
    }
}

/// A validity bitmap as a raw pointer and bit offset: `NullBuffer::is_valid`
/// bounds-checks and re-derives both on every call, which was a quarter of
/// the formatting time.
struct Valid(*const u8, usize);

impl Valid {
    fn new(n: &NullBuffer) -> Self {
        Valid(n.inner().values().as_ptr(), n.offset())
    }

    /// # Safety
    /// `r` must be below the bitmap's length.
    #[inline(always)]
    unsafe fn get(&self, r: usize) -> bool {
        let i = r + self.1;
        unsafe { *self.0.add(i / 8) >> (i % 8) & 1 != 0 }
    }
}

/// Widest number we print: `-` and 19 digits, `.`, scale digits.
const MAX_NUMBER: usize = 24;

const DIGITS: &[u8; 200] = b"\
0001020304050607080910111213141516171819\
2021222324252627282930313233343536373839\
4041424344454647484950515253545556575859\
6061626364656667686970717273747576777879\
8081828384858687888990919293949596979899";

/// The 8 decimal digits of `x < 10^8`, with leading zeros, as ASCII bytes
/// of a little-endian `u64` (most significant digit first). Splits into
/// 4-digit, 2-digit and 1-digit lanes with multiply-shift divisions that
/// are exact in those ranges, all lanes at once.
#[inline(always)]
fn eight(x: u32) -> u64 {
    let v = (x / 10_000) as u64 | (((x % 10_000) as u64) << 32);
    let hi = ((v * 10_486) >> 20) & 0x0000_007F_0000_007F; // lane / 100
    let v = hi | ((v - hi * 100) << 16);
    let hi = ((v * 103) >> 10) & 0x000F_000F_000F_000F; // lane / 10
    let v = hi | ((v - hi * 10) << 8);
    v + ZEROS
}

/// `"00000000"` as a `u64`.
const ZEROS: u64 = 0x3030_3030_3030_3030;

/// Write cursor into reserved capacity. Every method is `unsafe`: the
/// caller guarantees room for what it writes.
struct Cursor(*mut u8);

impl Cursor {
    #[inline(always)]
    unsafe fn byte(&mut self, b: u8) {
        unsafe {
            self.0.write(b);
            self.0 = self.0.add(1);
        }
    }

    #[inline(always)]
    unsafe fn bytes(&mut self, b: &[u8]) {
        unsafe {
            std::ptr::copy_nonoverlapping(b.as_ptr(), self.0, b.len());
            self.0 = self.0.add(b.len());
        }
    }

    #[inline(always)]
    unsafe fn pair(&mut self, d: usize) {
        unsafe {
            std::ptr::copy_nonoverlapping(DIGITS.as_ptr().add(d * 2), self.0, 2);
            self.0 = self.0.add(2);
        }
    }

    /// Decimal digits of `v`. Up to 8 digits are made at once in one
    /// `u64` ([`eight`]) and stored as 8 bytes, of which all but the leading
    /// zeros are kept (the reservation has room for 8 per number).
    #[inline]
    unsafe fn uint(&mut self, v: u64) {
        unsafe {
            if v < 100_000_000 {
                // leading zero digits are the low zero bytes (at most 7)
                let d = eight(v as u32);
                let zeros = ((d - ZEROS) | (1 << 56)).trailing_zeros() / 8;
                self.0
                    .cast::<[u8; 8]>()
                    .write_unaligned((d >> (8 * zeros)).to_le_bytes());
                self.0 = self.0.add(8 - zeros as usize);
            } else {
                self.big(v);
            }
        }
    }

    /// [`Self::uint`] of `v >= 10^8`: the high digits, then the low 8.
    #[cold]
    #[inline(never)]
    unsafe fn big(&mut self, v: u64) {
        unsafe {
            self.uint(v / 100_000_000);
            let d = eight((v % 100_000_000) as u32);
            self.0.cast::<[u8; 8]>().write_unaligned(d.to_le_bytes());
            self.0 = self.0.add(8);
        }
    }

    #[inline]
    unsafe fn int(&mut self, v: i64) {
        unsafe {
            if v < 0 {
                self.byte(b'-');
            }
            self.uint(v.unsigned_abs());
        }
    }

    /// `%.{scale}f` of `v / 10^scale` (what dsdgen's `print_decimal`
    /// prints; `-0.05` keeps its sign).
    unsafe fn decimal(&mut self, v: i64, scale: u32) {
        unsafe {
            if scale == 0 {
                return self.int(v);
            }
            if v < 0 {
                self.byte(b'-');
            }
            let p = 10u64.pow(scale);
            let a = v.unsigned_abs();
            self.uint(a / p);
            self.byte(b'.');
            let mut digits = [0u8; 19];
            let mut frac = a % p;
            for d in digits[..scale as usize].iter_mut().rev() {
                *d = b'0' + (frac % 10) as u8;
                frac /= 10;
            }
            self.bytes(&digits[..scale as usize]);
        }
    }

    /// [`Self::decimal`] at scale 2, with constant divisors.
    #[inline]
    unsafe fn decimal2(&mut self, v: i64) {
        unsafe {
            if v < 0 {
                self.byte(b'-');
            }
            let a = v.unsigned_abs();
            self.uint(a / 100);
            self.byte(b'.');
            self.pair((a % 100) as usize);
        }
    }

    /// Days since 1970-01-01 as `YYYY-MM-DD` (Hinnant's civil_from_days).
    #[inline]
    unsafe fn date(&mut self, days: i32) {
        let z = days as i64 + 719_468;
        let era = z.div_euclid(146_097);
        let doe = z.rem_euclid(146_097);
        let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
        let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        let mp = (5 * doy + 2) / 153;
        let d = (doy - (153 * mp + 2) / 5 + 1) as usize;
        let m = if mp < 10 { mp + 3 } else { mp - 9 } as usize;
        let y = (yoe + era * 400 + (m <= 2) as i64) as usize;
        unsafe {
            self.pair(y / 100);
            self.pair(y % 100);
            self.byte(b'-');
            self.pair(m);
            self.byte(b'-');
            self.pair(d);
        }
    }

    /// String `r` of a view array. Strings of up to 12 bytes live in the
    /// view itself: copy all 12 and advance by the length (the reservation
    /// has 12 bytes of slack per string for this).
    #[inline]
    unsafe fn view(&mut self, a: &StringViewArray, r: usize, encoding: Encoding) {
        // SAFETY: the caller passes a row of `a`.
        let v = unsafe { *a.views().get_unchecked(r) };
        let len = v as u32 as usize;
        unsafe {
            if len <= 12 {
                let inline = (v >> 32).to_le_bytes();
                if encoding == Encoding::Utf8 || inline.is_ascii() {
                    std::ptr::copy_nonoverlapping(inline.as_ptr(), self.0, 12);
                    self.0 = self.0.add(len);
                    return;
                }
            }
            self.text(a.value(r), encoding)
        }
    }

    #[inline]
    unsafe fn text(&mut self, s: &str, encoding: Encoding) {
        unsafe {
            if encoding == Encoding::Utf8 || s.is_ascii() {
                self.bytes(s.as_bytes());
            } else {
                // every TPC-DS character is in Latin-1
                for c in s.chars() {
                    self.byte(u8::try_from(c).unwrap_or(b'?'));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fmt(f: impl Fn(&mut Cursor)) -> String {
        let mut v = vec![0u8; 64];
        let mut c = Cursor(v.as_mut_ptr());
        f(&mut c);
        let n = unsafe { c.0.offset_from(v.as_ptr()) } as usize;
        v.truncate(n);
        String::from_utf8(v).unwrap()
    }

    #[test]
    fn eight_digits() {
        for x in (0..100_000_000u32)
            .step_by(9_973)
            .chain([0, 9, 10, 99_999_999, 12_345_678])
        {
            assert_eq!(eight(x).to_le_bytes(), *format!("{x:08}").as_bytes(), "{x}");
        }
        for v in [
            0u64,
            7,
            10,
            99_999_999,
            100_000_000,
            123_456_789_012,
            u64::MAX,
        ] {
            assert_eq!(fmt(|o| unsafe { o.uint(v) }), v.to_string());
        }
    }

    #[test]
    fn numbers_and_dates() {
        unsafe {
            assert_eq!(fmt(|o| o.int(0)), "0");
            assert_eq!(fmt(|o| o.int(-12345)), "-12345");
            assert_eq!(fmt(|o| o.int(i64::MIN)), i64::MIN.to_string());
            assert_eq!(fmt(|o| o.decimal(-5, 2)), "-0.05");
            assert_eq!(fmt(|o| o.decimal(0, 2)), "0.00");
            assert_eq!(fmt(|o| o.decimal(123456, 2)), "1234.56");
            assert_eq!(fmt(|o| o.decimal(-77973, 2)), "-779.73");
            assert_eq!(fmt(|o| o.decimal(-77973, 3)), "-77.973");
            assert_eq!(fmt(|o| o.decimal(-500, 0)), "-500");
            for v in [-100_001, -77973, -5, 0, 7, 99, 100, 123_456_789] {
                assert_eq!(fmt(|o| o.decimal2(v)), fmt(|o| o.decimal(v, 2)));
            }
            assert_eq!(fmt(|o| o.date(0)), "1970-01-01");
            assert_eq!(fmt(|o| o.date(-25567)), "1900-01-01");
            assert_eq!(fmt(|o| o.date(47482)), "2100-01-01");
            assert_eq!(fmt(|o| o.date(11016)), "2000-02-29");
        }
    }
}
