//! `tsbs_generate_data`-compatible CLI (influx format only), for diffing
//! against the reference generator:
//!
//! ```text
//! cargo run -p populate-tsbs --release --example tsbs_generate -- \
//!   --use-case=cpu-only --seed=123 --scale=2 \
//!   --timestamp-start=2016-01-01T00:00:00Z --timestamp-end=2016-01-01T00:00:30Z \
//!   --log-interval=10s
//! ```

use populate_tsbs::{TsbsConfig, UseCase, influx_lines};
use std::env::args;
use std::io::{Write, stdout};

/// Parse `YYYY-MM-DDTHH:MM:SSZ` to ns since the Unix epoch.
fn parse_time(s: &str) -> i64 {
    let b = s.trim_end_matches('Z');
    let (date, time) = b.split_once('T').expect("RFC3339 time");
    let d: Vec<i64> = date.split('-').map(|x| x.parse().unwrap()).collect();
    let t: Vec<i64> = time.split(':').map(|x| x.parse().unwrap()).collect();
    // days_from_civil (Howard Hinnant)
    let (y, m, day) = (if d[1] <= 2 { d[0] - 1 } else { d[0] }, d[1], d[2]);
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146097 + doe - 719468;
    ((days * 86400) + t[0] * 3600 + t[1] * 60 + t[2]) * 1_000_000_000
}

/// Parse a Go duration made of `h`, `m`, `s`, `ms` parts (e.g. `1m30s`).
fn parse_duration(s: &str) -> i64 {
    let mut total = 0i64;
    let mut num = String::new();
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c.is_ascii_digit() {
            num.push(c);
            continue;
        }
        let n: i64 = num.parse().expect("duration number");
        num.clear();
        let unit = match (c, chars.peek()) {
            ('m', Some('s')) => {
                chars.next();
                1_000_000
            }
            ('h', _) => 3_600_000_000_000,
            ('m', _) => 60_000_000_000,
            ('s', _) => 1_000_000_000,
            _ => panic!("unsupported duration {s}"),
        };
        total += n * unit;
    }
    total
}

fn main() {
    let mut config = TsbsConfig::new(UseCase::CpuOnly);
    for arg in args().skip(1) {
        let (k, v) = arg
            .trim_start_matches('-')
            .split_once('=')
            .expect("--key=value");
        match k {
            "use-case" => {
                config.use_case = match v {
                    "cpu-only" => UseCase::CpuOnly,
                    "devops" => UseCase::Devops,
                    "iot" => UseCase::Iot,
                    _ => panic!("unsupported use case {v}"),
                }
            }
            "seed" => config.seed = v.parse().unwrap(),
            "scale" => config.scale = v.parse().unwrap(),
            "initial-scale" => config.initial_scale = v.parse().unwrap(),
            "timestamp-start" => config.start_ns = parse_time(v),
            "timestamp-end" => config.end_ns = parse_time(v),
            "log-interval" => config.interval_ns = parse_duration(v),
            "max-data-points" => config.limit = v.parse().unwrap(),
            "format" => assert_eq!(v, "influx", "only influx is supported"),
            _ => panic!("unknown flag {k}"),
        }
    }
    let out = influx_lines(&config, &mut config.rng());
    // Ignore a closed pipe (e.g. `| head`).
    let _ = stdout().write_all(out.as_bytes());
}
