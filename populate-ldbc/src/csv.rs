//! The datagen's CSV output (`--mode raw --format csv`), byte for byte:
//! Spark's CSV writer with `sep = |`, a header line, timestamps as
//! `yyyy-MM-dd'T'HH:mm:ss.SSS+00:00` and dates as `yyyy-MM-dd`.
//!
//! Spark's quirks are reproduced: dates go through `(ms / 1000.0)` seconds
//! cast to microseconds with truncation (so some milliseconds print one
//! lower), and string values are trimmed of leading and trailing
//! whitespace (univocity's `ignoreLeading/TrailingWhiteSpace`, on by
//! default when writing).

use crate::dict::push_u64;
use crate::java::civil_from_days;
use crate::rows::{Col, Kind, Rows, TableDef};

/// Spark's microseconds of epoch milliseconds stored as a `Long`, divided
/// by 1000 and cast to a timestamp.
#[inline]
pub(crate) fn spark_micros(ms: i64) -> i64 {
    ((ms as f64 / 1000.0) * 1_000_000.0) as i64
}

/// Two ASCII digits of `v < 100`.
#[inline]
fn two(v: u32) -> [u8; 2] {
    [b'0' + (v / 10) as u8, b'0' + (v % 10) as u8]
}

/// `yyyy-MM-dd` of a day since the epoch.
#[inline]
fn ymd(days: i64) -> [u8; 10] {
    let (y, m, d) = civil_from_days(days);
    // four-digit years only (the datagen's are 1960s to 2019)
    debug_assert!((0..=9999).contains(&y), "year {y}");
    let y = y as u32;
    let (hi, lo, m, d) = (two(y / 100 % 100), two(y % 100), two(m), two(d));
    [
        hi[0], hi[1], lo[0], lo[1], b'-', m[0], m[1], b'-', d[0], d[1],
    ]
}

/// The last day formatted, since neighbouring values often share one.
#[derive(Clone, Copy)]
pub struct DayCache {
    day: i64,
    text: [u8; 10],
}

impl Default for DayCache {
    fn default() -> Self {
        DayCache {
            day: i64::MIN,
            text: [0; 10],
        }
    }
}

impl DayCache {
    #[inline]
    fn get(&mut self, day: i64) -> [u8; 10] {
        if day != self.day {
            self.day = day;
            self.text = ymd(day);
        }
        self.text
    }
}

/// A timestamp column value.
#[inline]
pub fn timestamp(out: &mut Vec<u8>, ms: i64) {
    timestamp_cached(out, ms, &mut DayCache::default());
}

#[inline]
fn timestamp_cached(out: &mut Vec<u8>, ms: i64, days: &mut DayCache) {
    let us = spark_micros(ms);
    let secs = us.div_euclid(1_000_000);
    let milli = (us.rem_euclid(1_000_000) / 1000) as u32;
    let sod = secs.rem_euclid(86_400) as u32;
    let mut b = *b"yyyy-MM-ddTHH:mm:ss.SSS+00:00";
    b[..10].copy_from_slice(&days.get(secs.div_euclid(86_400)));
    b[11..13].copy_from_slice(&two(sod / 3600));
    b[14..16].copy_from_slice(&two(sod / 60 % 60));
    b[17..19].copy_from_slice(&two(sod % 60));
    b[20] = b'0' + (milli / 100) as u8;
    b[21..23].copy_from_slice(&two(milli % 100));
    out.extend_from_slice(&b);
}

/// A date column value.
#[inline]
pub fn date(out: &mut Vec<u8>, ms: i64) {
    out.extend_from_slice(&ymd(spark_micros(ms).div_euclid(86_400_000_000)));
}

#[inline]
fn int(out: &mut Vec<u8>, v: i64) {
    if v < 0 {
        out.push(b'-');
    }
    push_u64(out, v.unsigned_abs());
}

/// A string value as univocity writes it: trimmed, quoted if it holds the
/// separator, a quote or a line break, `""` if empty.
#[inline]
pub fn string(out: &mut Vec<u8>, s: &[u8]) {
    let mut a = 0;
    let mut b = s.len();
    while a < b && s[a] <= b' ' {
        a += 1;
    }
    while b > a && s[b - 1] <= b' ' {
        b -= 1;
    }
    let s = &s[a..b];
    if s.is_empty() {
        out.extend_from_slice(b"\"\"");
        return;
    }
    if needs_quotes(s) {
        out.push(b'"');
        for &c in s {
            if c == b'"' || c == b'\\' {
                out.push(b'\\');
            }
            out.push(c);
        }
        out.push(b'"');
    } else {
        out.extend_from_slice(s);
    }
}

/// Whether `s` holds the separator, a quote or a line break: 32 bytes at
/// a time without early exits inside a block, so it vectorises.
#[inline]
fn needs_quotes(s: &[u8]) -> bool {
    let special = |c: u8| (c == b'|') | (c == b'"') | (c == b'\n') | (c == b'\r');
    let (blocks, rest) = s.as_chunks::<32>();
    blocks
        .iter()
        .any(|b| b.iter().fold(false, |acc, &c| acc | special(c)))
        || rest.iter().any(|&c| special(c))
}

/// The header line.
pub fn header(def: &TableDef, out: &mut Vec<u8>) {
    for (i, (n, _)) in def.cols.iter().enumerate() {
        if i > 0 {
            out.push(b'|');
        }
        out.extend_from_slice(n.as_bytes());
    }
    out.push(b'\n');
}

/// Append `rows` as CSV lines.
pub fn write(def: &TableDef, rows: &Rows, out: &mut Vec<u8>) {
    let n = rows.len();
    out.reserve(n * 16 * def.cols.len());
    let mut days = vec![DayCache::default(); def.cols.len()];
    for r in 0..n {
        for (c, (col, &(_, kind))) in rows.cols.iter().zip(def.cols).enumerate() {
            if c > 0 {
                out.push(b'|');
            }
            match col {
                Col::I64(v) => match kind {
                    Kind::Ts => timestamp_cached(out, v[r], &mut days[c]),
                    Kind::Date => date(out, v[r]),
                    _ => int(out, v[r]),
                },
                Col::I32(v) => int(out, v[r] as i64),
                Col::Bool(v) => out.extend_from_slice(if v[r] { b"true" } else { b"false" }),
                Col::Str(d, o) => {
                    let s = if r == 0 { 0 } else { o[r - 1] as usize };
                    string(out, &d[s..o[r] as usize]);
                }
                Col::OptI64(v, valid) => {
                    if valid[r] {
                        int(out, v[r]);
                    }
                }
                Col::OptI32(v, valid) => {
                    if valid[r] {
                        int(out, v[r] as i64);
                    }
                }
                Col::OptStr(d, o, valid) => {
                    if valid[r] {
                        let s = if r == 0 { 0 } else { o[r - 1] as usize };
                        string(out, &d[s..o[r] as usize]);
                    }
                }
            }
        }
        out.push(b'\n');
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::java::ms_of_ymd;

    #[test]
    fn spark_dates() {
        let mut v = Vec::new();
        timestamp(&mut v, 1_262_531_431_499);
        assert_eq!(
            String::from_utf8(v).unwrap(),
            "2010-01-03T15:10:31.499+00:00"
        );
        let mut v = Vec::new();
        date(&mut v, ms_of_ymd(1984, 3, 11));
        assert_eq!(String::from_utf8(v).unwrap(), "1984-03-11");
    }

    #[test]
    fn ints() {
        for v in [0i64, 7, 10, 99, 100, 12345, -1, -100, i64::MAX, i64::MIN] {
            let mut out = b"x".to_vec();
            int(&mut out, v);
            assert_eq!(out, format!("x{v}").into_bytes());
        }
    }

    #[test]
    fn strings() {
        let mut v = Vec::new();
        string(&mut v, b"  a b. ");
        assert_eq!(v, b"a b.");
    }
}
