//! `tsbs_generate_data`'s configuration: use case, scale, time range, log
//! interval, seed and the rest, with Go's defaults, validation and its
//! RFC 3339 and duration syntax.

use std::fmt;
use std::ops::Range;
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

/// A TSBS use case (`--use-case`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UseCase {
    /// `devops`: nine measurements per host (cpu, diskio, disk, kernel,
    /// mem, net, nginx, postgresl, redis).
    Devops,
    /// `cpu-only`: the cpu measurement alone.
    CpuOnly,
    /// `cpu-single`: the cpu measurement with one field.
    CpuSingle,
    /// `devops-generic`: one `generic_metrics` measurement with a
    /// Zipf-distributed number of fields per host, and hosts that come
    /// and go.
    DevopsGeneric,
    /// `iot`: trucks reporting readings and diagnostics, with missing,
    /// out-of-order and zeroed entries.
    Iot,
}

impl UseCase {
    /// Every use case, in Go's order.
    pub const ALL: [UseCase; 5] = [
        UseCase::CpuOnly,
        UseCase::CpuSingle,
        UseCase::Devops,
        UseCase::Iot,
        UseCase::DevopsGeneric,
    ];

    /// The `--use-case` name.
    pub fn name(self) -> &'static str {
        match self {
            UseCase::Devops => "devops",
            UseCase::CpuOnly => "cpu-only",
            UseCase::CpuSingle => "cpu-single",
            UseCase::DevopsGeneric => "devops-generic",
            UseCase::Iot => "iot",
        }
    }
}

impl FromStr for UseCase {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        UseCase::ALL
            .into_iter()
            .find(|u| u.name() == s)
            .ok_or_else(|| format!("invalid use case specified: '{s}'"))
    }
}

impl fmt::Display for UseCase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// Everything that decides the generated points. The defaults are
/// `tsbs_generate_data`'s.
#[derive(Debug, Clone, PartialEq)]
pub struct Config {
    /// `--use-case`.
    pub use_case: UseCase,
    /// `--scale`: hosts or trucks.
    pub scale: u64,
    /// `--initial-scale`: hosts in the first epoch, growing linearly to
    /// `scale` (0 = `scale`).
    pub initial_scale: u64,
    /// `--timestamp-start`, Unix nanoseconds. Wider than `i64`, like Go's
    /// `time.Time`: points carry the time as `UnixNano` (wrapping outside
    /// 1678..2262), but cassandra's date is the true one.
    pub start: i128,
    /// `--timestamp-end`, Unix nanoseconds.
    pub end: i128,
    /// `--log-interval`, nanoseconds.
    pub interval: i64,
    /// `--seed`. Go replaces 0 with the clock's nanoseconds.
    pub seed: i64,
    /// `--max-data-points` (0 = no limit).
    pub limit: u64,
    /// `--max-metric-count` (devops-generic).
    pub max_metric_count: u64,
    /// `--interleaved-generation-group-id`.
    pub group_id: u64,
    /// `--interleaved-generation-groups`.
    pub groups: u64,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            use_case: UseCase::Devops,
            scale: 1,
            initial_scale: 0,
            start: parse_rfc3339("2016-01-01T00:00:00Z").expect("default start"),
            end: parse_rfc3339("2016-01-02T00:00:00Z").expect("default end"),
            interval: 10_000_000_000,
            seed: 0,
            limit: 0,
            max_metric_count: 100,
            group_id: 0,
            groups: 1,
        }
    }
}

impl Config {
    /// A config for `use_case` at `scale` hosts, with the other defaults
    /// and seed `seed`.
    pub fn new(use_case: UseCase, scale: u64, seed: i64) -> Self {
        Config {
            use_case,
            scale,
            seed,
            ..Config::default()
        }
    }

    /// Go's validation and defaulting (`DataGeneratorConfig.Validate` and
    /// `GetSimulatorConfig`): a zero seed becomes the clock's nanoseconds,
    /// a zero initial scale becomes the scale (half of it for
    /// devops-generic).
    pub fn validate(mut self) -> Result<Self, String> {
        if self.scale == 0 {
            return Err("scale cannot be 0".into());
        }
        if self.seed == 0 {
            self.seed = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(1, |d| d.subsec_nanos() as i64);
        }
        if self.initial_scale == 0 {
            self.initial_scale = self.scale;
        }
        if self.interval == 0 {
            return Err("cannot have log interval of 0".into());
        }
        if self.use_case == UseCase::DevopsGeneric {
            if self.max_metric_count < 1 {
                return Err("max metric count per host has to be greater than 0".into());
            }
            if self.initial_scale == self.scale {
                self.initial_scale = (self.scale / 2).max(1);
            }
        }
        if self.groups == 0 {
            return Err("incorrect interleaved groups configuration: total groups = 0".into());
        }
        if self.group_id >= self.groups {
            return Err(format!(
                "incorrect interleaved groups configuration: id {} >= total groups {}",
                self.group_id, self.groups
            ));
        }
        Ok(self)
    }

    /// Reporting periods: `uint64((end - start) / interval)`, as Go
    /// computes it (`Time.Sub` saturates to a `Duration`).
    pub fn epochs(&self) -> u64 {
        let d = (self.end - self.start).clamp(i64::MIN as i128, i64::MAX as i128) as i64;
        d.wrapping_div(self.interval) as u64
    }
}

/// Parse an RFC 3339 timestamp to Unix nanoseconds, accepting what Go's
/// `time.Parse(time.RFC3339, s)` accepts: a one- or two-digit hour, a
/// fraction after `.` or `,` with any number of digits (truncated to
/// nanoseconds), and `Z` or a `±hh:mm` offset (up to 24:60).
pub fn parse_rfc3339(s: &str) -> Result<i128, String> {
    let err = || format!("cannot parse time from string '{s}'");
    let b = s.as_bytes();
    let digits = |r: Range<usize>| -> Result<i64, String> {
        match b.get(r) {
            Some(t) if t.iter().all(u8::is_ascii_digit) => {
                Ok(t.iter().fold(0, |x, &c| x * 10 + i64::from(c - b'0')))
            }
            _ => Err(err()),
        }
    };
    let lit = |i: usize, c: u8| {
        if b.get(i) == Some(&c) {
            Ok(())
        } else {
            Err(err())
        }
    };
    let y = digits(0..4)?;
    lit(4, b'-')?;
    let mo = digits(5..7)?;
    lit(7, b'-')?;
    let d = digits(8..10)?;
    lit(10, b'T')?;
    let hl = if b.get(12).is_some_and(u8::is_ascii_digit) {
        2
    } else {
        1
    };
    let h = digits(11..11 + hl)?;
    let mut i = 11 + hl;
    lit(i, b':')?;
    let mi = digits(i + 1..i + 3)?;
    lit(i + 3, b':')?;
    let sec = digits(i + 4..i + 6)?;
    i += 6;
    if !(1..=12).contains(&mo) || h > 23 || mi > 59 || sec > 59 {
        return Err(err());
    }
    let mut nanos = 0i64;
    if matches!(b.get(i), Some(b'.' | b',')) && b.get(i + 1).is_some_and(u8::is_ascii_digit) {
        let st = i + 1;
        i = st;
        while b.get(i).is_some_and(u8::is_ascii_digit) {
            i += 1;
        }
        let n = (i - st).min(9);
        nanos = digits(st..st + n)? * 10i64.pow((9 - n) as u32);
    }
    let offset = match &b[i..] {
        [b'Z'] => 0,
        [sign @ (b'+' | b'-'), _, _, b':', _, _] => {
            let (oh, om) = (digits(i + 1..i + 3)?, digits(i + 4..i + 6)?);
            if oh > 24 || om > 60 {
                return Err(err());
            }
            let o = oh * 3600 + om * 60;
            if *sign == b'-' { -o } else { o }
        }
        _ => return Err(err()),
    };
    if d < 1 || d > days_in_month(y, mo) {
        return Err(err());
    }
    let days = days_from_civil(y, mo, d);
    let secs = days * 86_400 + h * 3600 + mi * 60 + sec - offset;
    Ok(i128::from(secs) * 1_000_000_000 + i128::from(nanos))
}

fn days_in_month(y: i64, m: i64) -> i64 {
    match m {
        2 if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

/// Days since 1970-01-01 of a proleptic Gregorian date.
pub fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// `(year, month, day)` of a day count since 1970-01-01.
pub fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (yoe + era * 400 + i64::from(m <= 2), m, d)
}

/// Parse a Go duration (`10s`, `1m30s`, `1.5h`, `250ms`) to nanoseconds,
/// as `time.ParseDuration` does, rounding of fractions included.
pub fn parse_duration(s: &str) -> Result<i64, String> {
    let err = || format!("time: invalid duration \"{s}\"");
    const MAX: u64 = 1 << 63;
    let mut b = s.as_bytes();
    let mut neg = false;
    if let Some(&c @ (b'-' | b'+')) = b.first() {
        neg = c == b'-';
        b = &b[1..];
    }
    if b == b"0" {
        return Ok(0);
    }
    if b.is_empty() {
        return Err(err());
    }
    let mut d: u64 = 0;
    while !b.is_empty() {
        if !(b[0] == b'.' || b[0].is_ascii_digit()) {
            return Err(err());
        }
        // leadingInt
        let mut v: u64 = 0;
        let mut n = 0;
        while n < b.len() && b[n].is_ascii_digit() {
            if v > MAX / 10 {
                return Err(err());
            }
            v = v * 10 + u64::from(b[n] - b'0');
            if v > MAX {
                return Err(err());
            }
            n += 1;
        }
        let pre = n > 0;
        b = &b[n..];
        // leadingFraction
        let (mut f, mut scale, mut post) = (0u64, 1f64, false);
        if b.first() == Some(&b'.') {
            b = &b[1..];
            let mut overflow = false;
            let mut n = 0;
            while n < b.len() && b[n].is_ascii_digit() {
                if !overflow {
                    if f > (MAX - 1) / 10 {
                        overflow = true;
                    } else {
                        let y = f * 10 + u64::from(b[n] - b'0');
                        if y > MAX {
                            overflow = true;
                        } else {
                            f = y;
                            scale *= 10.0;
                        }
                    }
                }
                n += 1;
            }
            post = n > 0;
            b = &b[n..];
        }
        if !pre && !post {
            return Err(err());
        }
        let n = b
            .iter()
            .position(|&c| c == b'.' || c.is_ascii_digit())
            .unwrap_or(b.len());
        if n == 0 {
            return Err(format!("time: missing unit in duration \"{s}\""));
        }
        let (u, rest) = b.split_at(n);
        b = rest;
        let unit: u64 = match u {
            b"ns" => 1,
            b"us" => 1_000,
            _ if u == "µs".as_bytes() || u == "μs".as_bytes() => 1_000,
            b"ms" => 1_000_000,
            b"s" => 1_000_000_000,
            b"m" => 60_000_000_000,
            b"h" => 3_600_000_000_000,
            _ => {
                let u = String::from_utf8_lossy(u);
                return Err(format!("time: unknown unit \"{u}\" in duration \"{s}\""));
            }
        };
        if v > MAX / unit {
            return Err(err());
        }
        v *= unit;
        if f > 0 {
            v += (f as f64 * (unit as f64 / scale)) as u64;
            if v > MAX {
                return Err(err());
            }
        }
        d += v;
        if d > MAX {
            return Err(err());
        }
    }
    if neg {
        return Ok((d as i64).wrapping_neg());
    }
    if d > MAX - 1 {
        return Err(err());
    }
    Ok(d as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn times() {
        assert_eq!(parse_rfc3339("1970-01-01T00:00:00Z"), Ok(0));
        assert_eq!(
            parse_rfc3339("2016-01-01T00:00:00Z"),
            Ok(1_451_606_400_000_000_000)
        );
        assert_eq!(
            parse_rfc3339("2016-01-01T01:00:00+01:00"),
            Ok(1_451_606_400_000_000_000)
        );
        assert_eq!(
            parse_rfc3339("2016-01-01T00:00:00.5Z"),
            Ok(1_451_606_400_500_000_000)
        );
        assert!(parse_rfc3339("2016-13-01T00:00:00Z").is_err());
        assert!(parse_rfc3339("2016-01-01t00:00:00Z").is_err());
        assert!(parse_rfc3339("2016-01-01T00:00:00z").is_err());
        assert!(parse_rfc3339("2016-02-30T00:00:00Z").is_err());
        assert_eq!(
            parse_rfc3339("2016-01-01T0:00:00,1234567899Z"),
            Ok(1_451_606_400_123_456_789)
        );
        assert_eq!(
            parse_rfc3339("1600-01-01T00:00:00Z"),
            Ok(-11_676_096_000_000_000_000)
        );
        for z in [-1000, 0, 16_801, 20_000, 100_000] {
            let (y, m, d) = civil_from_days(z);
            assert_eq!(days_from_civil(y, m, d), z);
        }
    }

    #[test]
    fn durations() {
        assert_eq!(parse_duration("10s"), Ok(10_000_000_000));
        assert_eq!(parse_duration("1m30s"), Ok(90_000_000_000));
        assert_eq!(parse_duration("1.5h"), Ok(5_400_000_000_000));
        assert_eq!(parse_duration("250ms"), Ok(250_000_000));
        assert_eq!(parse_duration("0"), Ok(0));
        assert!(parse_duration("10").is_err());
        assert_eq!(parse_duration("1.57m"), Ok(94_200_000_000));
        assert_eq!(parse_duration("0.1s"), Ok(100_000_000));
        assert_eq!(parse_duration(".5µs"), Ok(500));
        assert_eq!(parse_duration("-1h"), Ok(-3_600_000_000_000));
        assert_eq!(parse_duration("9223372036854775807ns"), Ok(i64::MAX));
        assert_eq!(parse_duration("-9223372036854775808ns"), Ok(i64::MIN));
        assert!(parse_duration("9223372036854775808ns").is_err());
        assert!(parse_duration(".s").is_err());
        assert!(parse_duration("1d").is_err());
    }
}
