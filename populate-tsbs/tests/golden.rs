//! Output against `tsbs_generate_data` (Go, TSBS `master`): the length and
//! FNV-1a hash of Go's output for every use case and text format, and the
//! flags that change the run (initial scale, point limit, interleaved
//! groups, log interval, seeds past `int32`). Each is checked chunked on
//! several threads and as one sequential run.
//!
//! With `TSBS_GENERATE_DATA` set to a `tsbs_generate_data` binary,
//! `against_go_binary` also compares a wider set of runs byte for byte.

use populate_tsbs::config::{parse_duration, parse_rfc3339};
use populate_tsbs::{Config, Format, Tsbs};

// (use case, format, scale, seed, end, extra flags, bytes, FNV-1a 64)
/// `(use case, format, scale, seed, end, extra flags, bytes, FNV-1a 64)`.
type Case = (
    &'static str,
    &'static str,
    u64,
    i64,
    &'static str,
    &'static str,
    usize,
    u64,
);

const GOLDEN: &[Case] = &[
    (
        "devops",
        "influx",
        3,
        123,
        "2016-01-01T01:00:00Z",
        "",
        4125177,
        0xe9a35559737f4404,
    ),
    (
        "devops",
        "timescaledb",
        3,
        123,
        "2016-01-01T01:00:00Z",
        "",
        2640638,
        0x7aaf56ae85e10ead,
    ),
    (
        "devops",
        "cratedb",
        3,
        123,
        "2016-01-01T01:00:00Z",
        "",
        3034838,
        0x6363166c3166feef,
    ),
    (
        "devops",
        "cassandra",
        3,
        123,
        "2016-01-01T01:00:00Z",
        "",
        27004977,
        0x59c9c1c4cb048836,
    ),
    (
        "cpu-only",
        "influx",
        3,
        123,
        "2016-01-01T01:00:00Z",
        "",
        368444,
        0x6d3a3467d199c267,
    ),
    (
        "cpu-only",
        "timescaledb",
        3,
        123,
        "2016-01-01T01:00:00Z",
        "",
        229417,
        0xb38fedbf0a219bfd,
    ),
    (
        "cpu-only",
        "cratedb",
        3,
        123,
        "2016-01-01T01:00:00Z",
        "",
        269377,
        0x2c1e6beab1d1af4f,
    ),
    (
        "cpu-only",
        "cassandra",
        3,
        123,
        "2016-01-01T01:00:00Z",
        "",
        2357804,
        0xe61fc008e254472b,
    ),
    (
        "cpu-single",
        "influx",
        3,
        123,
        "2016-01-01T01:00:00Z",
        "",
        208942,
        0x38f1f4dfa347828a,
    ),
    (
        "cpu-single",
        "timescaledb",
        3,
        123,
        "2016-01-01T01:00:00Z",
        "",
        201562,
        0xf20dcbf11ab21040,
    ),
    (
        "cpu-single",
        "cratedb",
        3,
        123,
        "2016-01-01T01:00:00Z",
        "",
        241522,
        0xe31a981f9225d0d2,
    ),
    (
        "cpu-single",
        "cassandra",
        3,
        123,
        "2016-01-01T01:00:00Z",
        "",
        234862,
        0x6c5db9907158a6a6,
    ),
    (
        "devops-generic",
        "influx",
        3,
        123,
        "2016-01-01T01:00:00Z",
        "",
        275271,
        0xe41282402ec9bc36,
    ),
    (
        "devops-generic",
        "timescaledb",
        3,
        123,
        "2016-01-01T01:00:00Z",
        "",
        153497,
        0xe2c2a3e6e8d4b5f1,
    ),
    (
        "devops-generic",
        "cratedb",
        3,
        123,
        "2016-01-01T01:00:00Z",
        "",
        173514,
        0x62dad819f9d04612,
    ),
    (
        "devops-generic",
        "cassandra",
        3,
        123,
        "2016-01-01T01:00:00Z",
        "",
        2740260,
        0x10a3a3202be0c62b,
    ),
    (
        "iot",
        "influx",
        3,
        123,
        "2016-01-01T01:00:00Z",
        "",
        459934,
        0x560542a24e4f627e,
    ),
    (
        "iot",
        "timescaledb",
        3,
        123,
        "2016-01-01T01:00:00Z",
        "",
        374077,
        0xda6a6db88d35957e,
    ),
    (
        "iot",
        "cratedb",
        3,
        123,
        "2016-01-01T01:00:00Z",
        "",
        430308,
        0xc6779fed5da15a20,
    ),
    (
        "devops",
        "influx",
        4,
        42,
        "2016-01-01T03:00:00Z",
        "",
        16484123,
        0xc74afcde1d1aadea,
    ),
    (
        "iot",
        "influx",
        20,
        5,
        "2016-01-01T06:00:00Z",
        "--initial-scale=3",
        9867159,
        0x083692dc721ef455,
    ),
    (
        "iot",
        "timescaledb",
        7,
        5,
        "2016-01-01T06:00:00Z",
        "--interleaved-generation-groups=4 --interleaved-generation-group-id=1",
        1305855,
        0x58d20be5c88183eb,
    ),
    (
        "iot",
        "influx",
        7,
        5,
        "2016-01-01T06:00:00Z",
        "--max-data-points=1234",
        260678,
        0xc6a709d31845514c,
    ),
    (
        "devops",
        "influx",
        7,
        5,
        "2016-01-01T02:00:00Z",
        "--max-data-points=12345 --initial-scale=2",
        1657645,
        0x4383aeb4173bc242,
    ),
    (
        "devops-generic",
        "influx",
        20,
        5,
        "2016-01-01T06:00:00Z",
        "--initial-scale=7 --max-metric-count=30",
        6804323,
        0x50a6bce44267b699,
    ),
    (
        "devops",
        "influx",
        3,
        -77,
        "2016-01-01T06:00:00Z",
        "--log-interval=1m30s",
        2676221,
        0x085919aa627a5a7d,
    ),
    (
        "cpu-only",
        "influx",
        1,
        2147483648,
        "2016-01-01T00:00:10Z",
        "",
        349,
        0xeb21787504f5eb86,
    ),
    (
        "iot",
        "influx",
        1,
        5,
        "2016-01-01T00:00:20Z",
        "",
        677,
        0x56f591765545f412,
    ),
];

fn fnv(b: &[u8]) -> u64 {
    let mut h = 0xcbf2_9ce4_8422_2325u64;
    for &c in b {
        h ^= c as u64;
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    h
}

fn config(uc: &str, scale: u64, seed: i64, end: &str, extra: &str) -> Config {
    let mut c = Config::new(uc.parse().unwrap(), scale, seed);
    c.start = parse_rfc3339("2016-01-01T00:00:00Z").unwrap();
    c.end = parse_rfc3339(end).unwrap();
    for f in extra.split_whitespace() {
        let (k, v) = f.trim_start_matches("--").split_once('=').unwrap();
        match k {
            "initial-scale" => c.initial_scale = v.parse().unwrap(),
            "max-data-points" => c.limit = v.parse().unwrap(),
            "max-metric-count" => c.max_metric_count = v.parse().unwrap(),
            "interleaved-generation-groups" => c.groups = v.parse().unwrap(),
            "interleaved-generation-group-id" => c.group_id = v.parse().unwrap(),
            "log-interval" => c.interval = parse_duration(v).unwrap(),
            _ => panic!("unknown flag {k}"),
        }
    }
    c
}

#[test]
fn golden_chunked() {
    for &(uc, f, scale, seed, end, extra, len, hash) in GOLDEN {
        let t = Tsbs::new(config(uc, scale, seed, end, extra)).unwrap();
        let f: Format = f.parse().unwrap();
        let out = t.write_text(f, Vec::new(), 4, 1000).unwrap();
        assert_eq!(
            (out.len(), fnv(&out)),
            (len, hash),
            "{uc} {f} {scale} {seed} {end} {extra}"
        );
    }
}

#[test]
fn golden_sequential() {
    for &(uc, f, scale, seed, end, extra, len, hash) in GOLDEN {
        let t = Tsbs::new(config(uc, scale, seed, end, extra)).unwrap();
        let out = t
            .write_text_sequential(f.parse().unwrap(), Vec::new())
            .unwrap();
        assert_eq!(
            (out.len(), fnv(&out)),
            (len, hash),
            "{uc} {f} {scale} {seed} {end} {extra}"
        );
    }
}

/// The formats that share a serializer write the same bytes.
#[test]
fn aliases() {
    let t = Tsbs::new(config("iot", 3, 9, "2016-01-01T01:00:00Z", "")).unwrap();
    let text = |f| t.write_text(f, Vec::new(), 2, 500).unwrap();
    let influx = text(Format::Influx);
    assert_eq!(influx, text(Format::VictoriaMetrics));
    assert_eq!(influx, text(Format::QuestDb));
    let ts = text(Format::TimescaleDb);
    assert_eq!(ts, text(Format::ClickHouse));
    // timestream: no header (it ends at the first blank line)
    let body = ts.windows(2).position(|w| w == b"\n\n").unwrap() + 2;
    assert_eq!(&ts[body..], &text(Format::Timestream)[..]);
    // and cassandra refuses IoT's float32 tags, as Go panics
    assert!(t.write_text(Format::Cassandra, Vec::new(), 1, 0).is_err());
}

#[test]
fn against_go_binary() {
    let Ok(go) = std::env::var("TSBS_GENERATE_DATA") else {
        eprintln!("TSBS_GENERATE_DATA not set: skipping");
        return;
    };
    let mut n = 0;
    for uc in ["devops", "cpu-only", "cpu-single", "devops-generic", "iot"] {
        for f in Format::ALL {
            if uc == "iot" && f == Format::Cassandra {
                continue;
            }
            for (scale, seed, end, extra) in [
                (7, 1, "2016-01-01T04:00:00Z", ""),
                (13, 31337, "2016-01-01T02:00:00Z", "--initial-scale=4"),
                (5, -8, "2016-01-01T03:00:00Z", "--max-data-points=7777"),
                (
                    6,
                    77,
                    "2016-01-01T03:00:00Z",
                    "--interleaved-generation-groups=3 --interleaved-generation-group-id=2",
                ),
            ] {
                let mut cmd = std::process::Command::new(&go);
                cmd.args([
                    format!("--use-case={uc}"),
                    format!("--format={f}"),
                    format!("--scale={scale}"),
                    format!("--seed={seed}"),
                    "--timestamp-start=2016-01-01T00:00:00Z".into(),
                    format!("--timestamp-end={end}"),
                ]);
                cmd.args(extra.split_whitespace());
                let want = cmd.output().unwrap().stdout;
                let t = Tsbs::new(config(uc, scale, seed, end, extra)).unwrap();
                let got = t.write_text(f, Vec::new(), 3, 777).unwrap();
                assert!(got == want, "{uc} {f} {scale} {seed} {end} {extra}");
                n += 1;
            }
        }
    }
    eprintln!("{n} runs equal to tsbs_generate_data");
}
