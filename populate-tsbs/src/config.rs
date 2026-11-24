//! `tsbs_generate_data`'s configuration: use case, scale, time range, log
//! interval, seed and the rest, with Go's defaults, validation and its
//! RFC 3339 and duration syntax.

use std::fmt;
use std::str::FromStr;

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
    /// `--timestamp-start`, Unix nanoseconds.
    pub start: i64,
    /// `--timestamp-end`, Unix nanoseconds.
    pub end: i64,
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
            self.seed = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(1, |d| d.subsec_nanos() as i64);
        }
        if self.initial_scale == 0 {
            self.initial_scale = self.scale;
        }
        if self.interval == 0 {
            return Err("cannot have log interval of 0".into());
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
        if self.use_case == UseCase::DevopsGeneric {
            if self.max_metric_count < 1 {
                return Err("max metric count per host has to be greater than 0".into());
            }
            if self.initial_scale == self.scale {
                self.initial_scale = (self.scale / 2).max(1);
            }
        }
        Ok(self)
    }

    /// Reporting periods: `(end - start) / interval`, as Go computes it.
    pub fn epochs(&self) -> u64 {
        (self.end.saturating_sub(self.start) / self.interval) as u64
    }
}

/// Parse an RFC 3339 timestamp (`2016-01-01T00:00:00Z`, optional fraction
/// and numeric offset) to Unix nanoseconds.
pub fn parse_rfc3339(s: &str) -> Result<i64, String> {
    let err = || format!("cannot parse time from string '{s}'");
    let b = s.as_bytes();
    if b.len() < 20 || b[4] != b'-' || b[7] != b'-' || !matches!(b[10], b'T' | b't') {
        return Err(err());
    }
    if b[13] != b':' || b[16] != b':' {
        return Err(err());
    }
    let num = |r: std::ops::Range<usize>| -> Result<i64, String> {
        let t = &s[r];
        if t.bytes().all(|c| c.is_ascii_digit()) {
            t.parse().map_err(|_| err())
        } else {
            Err(err())
        }
    };
    let (y, mo, d) = (num(0..4)?, num(5..7)?, num(8..10)?);
    let (h, mi, sec) = (num(11..13)?, num(14..16)?, num(17..19)?);
    if !(1..=12).contains(&mo) || d < 1 || d > days_in_month(y, mo) || h > 23 || mi > 59 || sec > 59
    {
        return Err(err());
    }
    let mut i = 19;
    let mut nanos = 0i64;
    if b[i] == b'.' || b[i] == b',' {
        i += 1;
        let st = i;
        while i < b.len() && b[i].is_ascii_digit() {
            i += 1;
        }
        if i == st || i - st > 9 {
            return Err(err());
        }
        nanos = num(st..i)? * 10i64.pow((9 - (i - st)) as u32);
    }
    let offset = match &b[i..] {
        [b'Z' | b'z'] => 0,
        [sign @ (b'+' | b'-'), _, _, b':', _, _] => {
            let o = num(i + 1..i + 3)? * 3600 + num(i + 4..i + 6)? * 60;
            if *sign == b'-' { -o } else { o }
        }
        _ => return Err(err()),
    };
    let days = days_from_civil(y, mo, d);
    Ok((days * 86_400 + h * 3600 + mi * 60 + sec - offset) * 1_000_000_000 + nanos)
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

/// Parse a Go duration (`10s`, `1m30s`, `1.5h`, `250ms`) to nanoseconds.
pub fn parse_duration(s: &str) -> Result<i64, String> {
    let err = || format!("invalid duration \"{s}\"");
    let (neg, mut rest) = match s.as_bytes().first() {
        Some(b'-') => (true, &s[1..]),
        Some(b'+') => (false, &s[1..]),
        _ => (false, s),
    };
    if rest == "0" {
        return Ok(0);
    }
    if rest.is_empty() {
        return Err(err());
    }
    let mut total: f64 = 0.0;
    let mut total_i: i128 = 0;
    while !rest.is_empty() {
        let n = rest
            .find(|c: char| !(c.is_ascii_digit() || c == '.'))
            .ok_or_else(err)?;
        let (num, tail) = rest.split_at(n);
        if num.is_empty() || num == "." {
            return Err(err());
        }
        let u = tail
            .find(|c: char| c.is_ascii_digit() || c == '.')
            .unwrap_or(tail.len());
        let (unit, tail) = tail.split_at(u);
        let scale: i128 = match unit {
            "ns" => 1,
            "us" | "µs" | "μs" => 1_000,
            "ms" => 1_000_000,
            "s" => 1_000_000_000,
            "m" => 60_000_000_000,
            "h" => 3_600_000_000_000,
            _ => return Err(err()),
        };
        if let Some((int, frac)) = num.split_once('.') {
            let i: i128 = if int.is_empty() {
                0
            } else {
                int.parse().map_err(|_| err())?
            };
            total_i += i * scale;
            if !frac.is_empty() {
                let f: f64 = format!("0.{frac}").parse().map_err(|_| err())?;
                total += f * scale as f64;
            }
        } else {
            total_i += num.parse::<i128>().map_err(|_| err())? * scale;
        }
        rest = tail;
    }
    let v = total_i + total as i128;
    let v = i64::try_from(v).map_err(|_| err())?;
    Ok(if neg { -v } else { v })
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
    }
}
