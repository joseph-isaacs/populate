//! Delimited text (CSV for TPC-C, TSV for YCSB) straight from Arrow batches.

use arrow_array::cast::AsArray;
use arrow_array::types::{Decimal128Type, Int32Type, Int64Type, TimestampMicrosecondType};
use arrow_array::{Array, RecordBatch, StringViewArray};
use arrow_buffer::NullBuffer;
use arrow_schema::{DataType, TimeUnit};

/// How a batch is written as text.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Delimited {
    /// Field separator.
    pub(crate) sep: u8,
    /// Written for a NULL value.
    pub(crate) null: &'static [u8],
}

enum Col<'a> {
    I32(&'a [i32]),
    I64(&'a [i64]),
    Dec(&'a [i128], u32),
    Ts(&'a [i64]),
    Str(&'a StringViewArray),
}

impl Delimited {
    /// Append `batch`, one line per row, fields separated by `sep`.
    pub(crate) fn write(&self, batch: &RecordBatch, out: &mut Vec<u8>) {
        let cols: Vec<(Col<'_>, Option<&NullBuffer>)> = batch
            .columns()
            .iter()
            .map(|a| {
                let c = match a.data_type() {
                    DataType::Int32 => Col::I32(a.as_primitive::<Int32Type>().values()),
                    DataType::Int64 => Col::I64(a.as_primitive::<Int64Type>().values()),
                    DataType::Decimal128(_, s) if *s >= 0 => {
                        Col::Dec(a.as_primitive::<Decimal128Type>().values(), *s as u32)
                    }
                    DataType::Timestamp(TimeUnit::Microsecond, None) => {
                        Col::Ts(a.as_primitive::<TimestampMicrosecondType>().values())
                    }
                    DataType::Utf8View => Col::Str(a.as_string_view()),
                    t => panic!("text output: unsupported column type {t}"),
                };
                (c, a.nulls())
            })
            .collect();
        let mut buf = itoa::Buffer::new();
        for r in 0..batch.num_rows() {
            for (k, (c, nulls)) in cols.iter().enumerate() {
                if k > 0 {
                    out.push(self.sep);
                }
                if nulls.is_some_and(|n| n.is_null(r)) {
                    out.extend_from_slice(self.null);
                    continue;
                }
                match c {
                    Col::I32(v) => out.extend_from_slice(buf.format(v[r]).as_bytes()),
                    Col::I64(v) => out.extend_from_slice(buf.format(v[r]).as_bytes()),
                    Col::Dec(v, s) => decimal(out, v[r], *s, &mut buf),
                    Col::Ts(v) => timestamp(out, v[r], &mut buf),
                    Col::Str(a) => out.extend_from_slice(a.value(r).as_bytes()),
                }
            }
            out.push(b'\n');
        }
    }
}

/// `v / 10^s` with exactly `s` decimals.
fn decimal(out: &mut Vec<u8>, v: i128, s: u32, buf: &mut itoa::Buffer) {
    if s == 0 {
        out.extend_from_slice(buf.format(v).as_bytes());
        return;
    }
    if v < 0 {
        out.push(b'-');
    }
    let p = 10u128.pow(s);
    let a = v.unsigned_abs();
    out.extend_from_slice(buf.format(a / p).as_bytes());
    out.push(b'.');
    let frac = buf.format(a % p).as_bytes();
    out.extend(std::iter::repeat_n(b'0', s as usize - frac.len()));
    out.extend_from_slice(frac);
}

/// `YYYY-MM-DD HH:MM:SS`, then `.ffffff` if there are microseconds.
fn timestamp(out: &mut Vec<u8>, us: i64, buf: &mut itoa::Buffer) {
    let secs = us.div_euclid(1_000_000);
    let micros = us.rem_euclid(1_000_000);
    let (days, sod) = (secs.div_euclid(86_400), secs.rem_euclid(86_400));
    let (y, m, d) = civil(days);
    let two = |out: &mut Vec<u8>, v: i64| {
        out.push(b'0' + (v / 10) as u8);
        out.push(b'0' + (v % 10) as u8);
    };
    let year = buf.format(y.abs()).as_bytes();
    if y < 0 {
        out.push(b'-');
    }
    out.extend(std::iter::repeat_n(b'0', 4usize.saturating_sub(year.len())));
    out.extend_from_slice(year);
    out.push(b'-');
    two(out, m);
    out.push(b'-');
    two(out, d);
    out.push(b' ');
    two(out, sod / 3600);
    out.push(b':');
    two(out, sod / 60 % 60);
    out.push(b':');
    two(out, sod % 60);
    if micros != 0 {
        out.push(b'.');
        let f = buf.format(micros).as_bytes();
        out.extend(std::iter::repeat_n(b'0', 6 - f.len()));
        out.extend_from_slice(f);
    }
}

/// Days since 1970-01-01 to `(year, month, day)` (Howard Hinnant's
/// `civil_from_days`).
pub(crate) fn civil(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats() {
        let mut b = itoa::Buffer::new();
        let mut out = Vec::new();
        decimal(&mut out, -5, 2, &mut b);
        out.push(b' ');
        decimal(&mut out, 30_000_000, 2, &mut b);
        out.push(b' ');
        decimal(&mut out, 1234, 4, &mut b);
        out.push(b' ');
        timestamp(&mut out, 1_767_225_600_000_000, &mut b);
        out.push(b' ');
        timestamp(&mut out, 951_782_400_000_001, &mut b);
        assert_eq!(
            String::from_utf8(out).expect("ascii"),
            "-0.05 300000.00 0.1234 2026-01-01 00:00:00 2000-02-29 00:00:00.000001"
        );
    }
}
