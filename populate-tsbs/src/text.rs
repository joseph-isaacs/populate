//! TSBS's text serializers, byte for byte: InfluxDB line protocol (also
//! VictoriaMetrics and QuestDB), TimescaleDB's two-line format (also
//! ClickHouse and Timestream), CrateDB's TSV and Cassandra's CSV.

use crate::config::civil_from_days;
use crate::model::{NO_TAG, Point, Statics, TagVal, Type, Val};
use std::fmt;
use std::str::FromStr;

/// A `--format` of `tsbs_generate_data`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    /// `influx`: line protocol.
    Influx,
    /// `victoriametrics`: the influx serializer.
    VictoriaMetrics,
    /// `questdb`: the influx serializer's twin.
    QuestDb,
    /// `timescaledb`: a header, then a `tags` line and a values line per
    /// point.
    TimescaleDb,
    /// `clickhouse`: the timescaledb serializer and header.
    ClickHouse,
    /// `timestream`: the timescaledb serializer, without the header.
    Timestream,
    /// `cratedb`: the header, then tab-separated values with JSON tags.
    CrateDb,
    /// `cassandra`: one CSV line per field.
    Cassandra,
}

impl Format {
    /// Every text format.
    pub const ALL: [Format; 8] = [
        Format::Influx,
        Format::VictoriaMetrics,
        Format::QuestDb,
        Format::TimescaleDb,
        Format::ClickHouse,
        Format::Timestream,
        Format::CrateDb,
        Format::Cassandra,
    ];

    /// The `--format` name.
    pub fn name(self) -> &'static str {
        match self {
            Format::Influx => "influx",
            Format::VictoriaMetrics => "victoriametrics",
            Format::QuestDb => "questdb",
            Format::TimescaleDb => "timescaledb",
            Format::ClickHouse => "clickhouse",
            Format::Timestream => "timestream",
            Format::CrateDb => "cratedb",
            Format::Cassandra => "cassandra",
        }
    }

    /// Whether the output starts with `writeHeader`'s tag and field lists.
    pub fn has_header(self) -> bool {
        matches!(
            self,
            Format::TimescaleDb | Format::ClickHouse | Format::CrateDb
        )
    }

    /// Whether every point of `st` can be written (Go panics on non-string
    /// tags in cassandra).
    pub fn check(self, st: &Statics) -> Result<(), String> {
        if self == Format::Cassandra && st.tag_types.iter().any(|&t| t != Type::Str) {
            return Err("non-string tags not implemented for cassandra".into());
        }
        Ok(())
    }
}

impl FromStr for Format {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        Format::ALL
            .into_iter()
            .find(|f| f.name() == s)
            .ok_or_else(|| format!("invalid format specified: '{s}'"))
    }
}

impl fmt::Display for Format {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// `strconv.AppendInt(buf, v, 10)`.
#[inline]
pub fn push_i64(out: &mut Vec<u8>, v: i64) {
    let mut buf = [0u8; 20];
    let mut i = buf.len();
    let mut n = v.unsigned_abs();
    loop {
        i -= 1;
        buf[i] = b'0' + (n % 10) as u8;
        n /= 10;
        if n == 0 {
            break;
        }
    }
    if v < 0 {
        out.push(b'-');
    }
    out.extend_from_slice(&buf[i..]);
}

/// `strconv.AppendFloat(buf, v, 'f', -1, 64)`: the shortest digits that
/// round-trip (exact ties to even, as Ryu and Go pick them), never an
/// exponent.
#[inline]
pub fn push_f64(out: &mut Vec<u8>, v: f64) {
    if !v.is_finite() {
        return push_special(out, v.is_nan(), v > 0.0);
    }
    let mut b = ryu::Buffer::new();
    push_fixed(out, b.format_finite(v).as_bytes());
}

/// `strconv.AppendFloat(buf, float64(v), 'f', -1, 32)`.
#[inline]
pub fn push_f32(out: &mut Vec<u8>, v: f32) {
    if !v.is_finite() {
        return push_special(out, v.is_nan(), v > 0.0);
    }
    let mut b = ryu::Buffer::new();
    push_fixed(out, b.format_finite(v).as_bytes());
}

#[cold]
fn push_special(out: &mut Vec<u8>, nan: bool, pos: bool) {
    out.extend_from_slice(match (nan, pos) {
        (true, _) => b"NaN",
        (false, true) => b"+Inf",
        (false, false) => b"-Inf",
    });
}

/// Ryu's output (`28.5`, `50.0`, `1e21`, `-1.5e-7`) in Go's `'f'` form
/// (`28.5`, `50`, `1000000000000000000000`, `-0.00000015`).
#[inline]
fn push_fixed(out: &mut Vec<u8>, s: &[u8]) {
    let Some(e) = s.iter().position(|&c| c == b'e') else {
        let s = s.strip_suffix(b".0").unwrap_or(s);
        out.extend_from_slice(s);
        return;
    };
    push_exp(out, &s[..e], &s[e + 1..]);
}

#[cold]
fn push_exp(out: &mut Vec<u8>, mant: &[u8], exp: &[u8]) {
    let (neg, mant) = match mant.split_first() {
        Some((b'-', m)) => (true, m),
        _ => (false, mant),
    };
    let exp: i64 = std::str::from_utf8(exp)
        .ok()
        .and_then(|e| e.parse().ok())
        .expect("ryu exponent");
    let (int, frac) = match mant.iter().position(|&c| c == b'.') {
        Some(d) => (&mant[..d], &mant[d + 1..]),
        None => (mant, &[][..]),
    };
    let digits: Vec<u8> = int.iter().chain(frac).copied().collect();
    // value = 0.digits * 10^point
    let point = int.len() as i64 + exp;
    if neg {
        out.push(b'-');
    }
    if point <= 0 {
        out.extend_from_slice(b"0.");
        out.extend(std::iter::repeat_n(b'0', (-point) as usize));
        out.extend_from_slice(&digits);
    } else if point as usize >= digits.len() {
        out.extend_from_slice(&digits);
        out.extend(std::iter::repeat_n(b'0', point as usize - digits.len()));
    } else {
        out.extend_from_slice(&digits[..point as usize]);
        out.push(b'.');
        out.extend_from_slice(&digits[point as usize..]);
    }
}

/// `FastFormatAppend` of a field.
#[inline]
fn push_val(out: &mut Vec<u8>, v: Val) {
    match v {
        Val::Nil => {}
        Val::I(i) => push_i64(out, i),
        Val::F(f) => push_f64(out, f),
    }
}

/// `FastFormatAppend` of a tag (nil when zeroed).
#[inline]
fn push_tag(out: &mut Vec<u8>, v: Option<&TagVal>) {
    match v {
        None => {}
        Some(TagVal::Str(s)) => out.extend_from_slice(s.as_bytes()),
        Some(TagVal::F32(f)) => push_f32(out, *f),
    }
}

/// The point's tags in order: the generator's, then the measurement's.
fn tags<'a>(
    st: &'a Statics,
    p: &'a Point,
) -> impl Iterator<Item = (&'a str, Option<&'a TagVal>, Option<&'a str>)> + 'a {
    let g = p.source as usize;
    let m = p.meas as usize;
    let gens = st
        .tag_keys
        .iter()
        .zip(&st.gen_tags[g])
        .enumerate()
        .map(move |(i, (k, v))| {
            let v = if i == p.zero_tag as usize && p.zero_tag != NO_TAG {
                None
            } else {
                Some(v)
            };
            (&**k, v, None)
        });
    let extra = st.measurements[m]
        .extra_tags
        .iter()
        .zip(&st.extra_tags[g][m])
        .map(|(k, v)| (&**k, None, Some(&**v)));
    gens.chain(extra)
}

/// `writeHeader`: tag keys and types, then each measurement's fields,
/// sorted by name, then a blank line.
pub fn write_header(st: &Statics, out: &mut Vec<u8>) {
    out.extend_from_slice(b"tags");
    for (k, t) in st.tag_keys.iter().zip(&st.tag_types) {
        out.push(b',');
        out.extend_from_slice(k.as_bytes());
        out.push(b' ');
        out.extend_from_slice(t.go_name().as_bytes());
    }
    out.push(b'\n');
    let mut ms: Vec<usize> = (0..st.measurements.len()).collect();
    ms.sort_by(|&a, &b| st.measurements[a].name.cmp(&st.measurements[b].name));
    ms.dedup_by(|a, b| st.measurements[*a].name == st.measurements[*b].name);
    for m in ms {
        out.extend_from_slice(st.measurements[m].name.as_bytes());
        for f in &st.header_fields[m] {
            out.push(b',');
            out.extend_from_slice(f.as_bytes());
        }
        out.push(b'\n');
    }
    out.push(b'\n');
}

/// Serialize one point as `format`.
pub fn write_point(format: Format, st: &Statics, p: &Point, out: &mut Vec<u8>) {
    match format {
        Format::Influx | Format::VictoriaMetrics | Format::QuestDb => influx(st, p, out),
        Format::TimescaleDb | Format::ClickHouse | Format::Timestream => timescale(st, p, out),
        Format::CrateDb => crate_db(st, p, out),
        Format::Cassandra => cassandra(st, p, out),
    }
}

fn influx(st: &Statics, p: &Point, out: &mut Vec<u8>) {
    let start = out.len();
    let m = &st.measurements[p.meas as usize];
    out.extend_from_slice(m.name.as_bytes());
    let mut fake = false;
    for (k, v, extra) in tags(st, p) {
        match (v, extra) {
            (Some(TagVal::Str(s)), _) => {
                out.push(b',');
                out.extend_from_slice(k.as_bytes());
                out.push(b'=');
                out.extend_from_slice(s.as_bytes());
            }
            (None, Some(s)) => {
                out.push(b',');
                out.extend_from_slice(k.as_bytes());
                out.push(b'=');
                out.extend_from_slice(s.as_bytes());
            }
            (Some(TagVal::F32(_)), _) => fake = true,
            (None, None) => {}
        }
    }
    if fake || !p.fields.is_empty() {
        out.push(b' ');
    }
    let mut first = false;
    if fake {
        for (k, v, _) in tags(st, p) {
            if let Some(TagVal::F32(f)) = v {
                if first {
                    out.push(b',');
                }
                first = true;
                out.extend_from_slice(k.as_bytes());
                out.push(b'=');
                push_f32(out, *f);
            }
        }
    }
    for (k, &v) in m.fields.iter().zip(&p.fields) {
        if v == Val::Nil {
            continue;
        }
        if first {
            out.push(b',');
        }
        first = true;
        out.extend_from_slice(k.as_bytes());
        out.push(b'=');
        push_val(out, v);
        if let Val::I(_) = v {
            out.push(b'i');
        }
    }
    if !first {
        out.truncate(start);
        return;
    }
    out.push(b' ');
    push_i64(out, p.ts);
    out.push(b'\n');
}

fn timescale(st: &Statics, p: &Point, out: &mut Vec<u8>) {
    out.extend_from_slice(b"tags");
    for (k, v, extra) in tags(st, p) {
        out.push(b',');
        out.extend_from_slice(k.as_bytes());
        out.push(b'=');
        match extra {
            Some(s) => out.extend_from_slice(s.as_bytes()),
            None => push_tag(out, v),
        }
    }
    out.push(b'\n');
    out.extend_from_slice(st.measurements[p.meas as usize].name.as_bytes());
    out.push(b',');
    push_i64(out, p.ts);
    for &v in &p.fields {
        out.push(b',');
        push_val(out, v);
    }
    out.push(b'\n');
}

fn crate_db(st: &Statics, p: &Point, out: &mut Vec<u8>) {
    out.extend_from_slice(st.measurements[p.meas as usize].name.as_bytes());
    out.push(b'\t');
    let mut any = false;
    for (k, v, extra) in tags(st, p) {
        out.push(if any { b',' } else { b'{' });
        any = true;
        out.push(b'"');
        out.extend_from_slice(k.as_bytes());
        out.extend_from_slice(b"\":\"");
        match extra {
            Some(s) => out.extend_from_slice(s.as_bytes()),
            None => push_tag(out, v),
        }
        out.push(b'"');
    }
    if any {
        out.push(b'}');
    } else {
        out.extend_from_slice(b"null");
    }
    out.push(b'\t');
    push_i64(out, p.ts);
    for &v in &p.fields {
        out.push(b'\t');
        push_val(out, v);
    }
    out.push(b'\n');
}

/// `YYYY-MM-DD` (Go's `Format("2006-01-02")`) of the point's true time,
/// UTC. Points carry `UnixNano`, which wraps outside 1678..2262; the true
/// time is the run's start plus the point's (non-negative, under 584
/// years) offset from it.
fn push_date(out: &mut Vec<u8>, start: i128, ts: i64) {
    let t = start + i128::from(ts.wrapping_sub(start as i64) as u64);
    let (y, m, d) = civil_from_days(t.div_euclid(86_400_000_000_000) as i64);
    let two = |out: &mut Vec<u8>, v: i64| {
        out.push(b'0' + (v / 10) as u8);
        out.push(b'0' + (v % 10) as u8);
    };
    if y < 0 {
        out.push(b'-');
    }
    let y = y.unsigned_abs();
    if y >= 10_000 {
        out.extend_from_slice((y / 10_000).to_string().as_bytes());
    }
    let y = (y % 10_000) as i64;
    two(out, y / 100);
    two(out, y % 100);
    out.push(b'-');
    two(out, m);
    out.push(b'-');
    two(out, d);
}

fn cassandra(st: &Statics, p: &Point, out: &mut Vec<u8>) {
    let m = &st.measurements[p.meas as usize];
    let mut prefix = Vec::with_capacity(256);
    prefix.extend_from_slice(m.name.as_bytes());
    for (k, v, extra) in tags(st, p) {
        prefix.push(b',');
        prefix.extend_from_slice(k.as_bytes());
        prefix.push(b'=');
        match extra {
            Some(s) => prefix.extend_from_slice(s.as_bytes()),
            None => push_tag(&mut prefix, v),
        }
    }
    let mut date = Vec::with_capacity(10);
    push_date(&mut date, st.start, p.ts);
    for (k, &v) in m.fields.iter().zip(&p.fields) {
        let table: &[u8] = match v {
            Val::Nil => continue,
            Val::I(_) => b"series_bigint",
            Val::F(_) => b"series_double",
        };
        out.extend_from_slice(table);
        out.push(b',');
        out.extend_from_slice(&prefix);
        out.push(b',');
        out.extend_from_slice(k.as_bytes());
        out.push(b',');
        out.extend_from_slice(&date);
        out.push(b',');
        push_i64(out, p.ts);
        out.push(b',');
        push_val(out, v);
        out.push(b'\n');
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn go_number_formats() {
        let s = |f: &dyn Fn(&mut Vec<u8>)| {
            let mut v = Vec::new();
            f(&mut v);
            String::from_utf8(v).unwrap()
        };
        assert_eq!(s(&|o| push_i64(o, i64::MIN)), "-9223372036854775808");
        assert_eq!(s(&|o| push_i64(o, 0)), "0");
        assert_eq!(
            s(&|o| push_f64(o, 15.607654908671975)),
            "15.607654908671975"
        );
        assert_eq!(s(&|o| push_f64(o, 1e21)), "1000000000000000000000");
        assert_eq!(s(&|o| push_f64(o, 1e-7)), "0.0000001");
        assert_eq!(s(&|o| push_f64(o, -0.0)), "-0");
        assert_eq!(s(&|o| push_f64(o, 50.0)), "50");
        assert_eq!(s(&|o| push_f32(o, 2000.0)), "2000");
        // an exact tie between two 17-digit candidates: Go picks the even one
        assert_eq!(
            s(&|o| push_f64(o, f64::from_bits(0x403C_E23D_0000_0000))),
            "28.883743286132812"
        );
        assert_eq!(s(&|o| push_f64(o, -1.5e-7)), "-0.00000015");
        assert_eq!(s(&|o| push_f64(o, 1.25e17)), "125000000000000000");
        assert_eq!(s(&|o| push_f64(o, 0.0)), "0");
        assert_eq!(s(&|o| push_f32(o, 1.5e-6)), "0.0000015");
        assert_eq!(
            s(&|o| push_date(o, 1_451_606_400_000_000_000, 1_451_606_400_000_000_000)),
            "2016-01-01"
        );
        // 1600 wraps as UnixNano; the date is still the true one
        let t = -11_676_096_000_000_000_000i128;
        assert_eq!(
            s(&|o| push_date(o, t, t as i64 + 86_400_000_000_000)),
            "1600-01-02"
        );
        let t = 253_402_300_800_000_000_000i128; // 10000-01-01
        assert_eq!(s(&|o| push_date(o, t, t as i64)), "10000-01-01");
    }
}
