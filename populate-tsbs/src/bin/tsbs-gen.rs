//! Generate TSBS data, byte-identical to `tsbs_generate_data`.
//!
//! ```text
//! tsbs-gen --use-case devops --scale 100 --seed 123 --format influx \
//!          [--timestamp-start 2016-01-01T00:00:00Z] [--timestamp-end ...] \
//!          [--log-interval 10s] [--initial-scale N] [--max-data-points N] \
//!          [--max-metric-count 100] [--interleaved-generation-group-id I] \
//!          [--interleaved-generation-groups N] [--file PATH] \
//!          [--threads N] [--chunk-points N] [--out DIR]
//! ```
//!
//! The flags are `tsbs_generate_data`'s (with `--flag value` or
//! `--flag=value`), and text goes to `--file` or stdout. `--format
//! parquet` writes `--out DIR/<measurement>.parquet` instead, and
//! `--format arrow` generates the Arrow tables and drops them (to time
//! generation). `--threads` (default: all cores) and `--chunk-points`
//! change only the speed.

use populate_table::MultiChunks;
use populate_table::sched::par_sources;
use populate_tsbs::config::{parse_duration, parse_rfc3339};
use populate_tsbs::{Config, Format, Tsbs, UseCase};
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::PathBuf;
use std::process::exit;
use std::time::Instant;

enum Out {
    Text(Format),
    Parquet,
    Arrow,
}

struct Args {
    cfg: Config,
    out: Out,
    file: Option<PathBuf>,
    dir: PathBuf,
    threads: usize,
    chunk_points: u64,
}

const USAGE: &str = "usage: tsbs-gen --use-case devops|cpu-only|cpu-single|devops-generic|iot \
--scale N --seed N --format influx|victoriametrics|questdb|timescaledb|clickhouse|timestream|\
cratedb|cassandra|parquet|arrow [--timestamp-start T] [--timestamp-end T] [--log-interval D] \
[--initial-scale N] [--max-data-points N] [--max-metric-count N] \
[--interleaved-generation-group-id N] [--interleaved-generation-groups N] [--file PATH] \
[--out DIR] [--threads N] [--chunk-points N]";

fn parse() -> Result<Args, String> {
    let mut a = Args {
        cfg: Config::default(),
        out: Out::Text(Format::Influx),
        file: None,
        dir: PathBuf::from("tsbs"),
        threads: 0,
        chunk_points: 0,
    };
    let mut format = None;
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        let (flag, inline) = match arg.split_once('=') {
            Some((f, v)) => (f.to_string(), Some(v.to_string())),
            None => (arg.clone(), None),
        };
        let mut val = || {
            inline
                .clone()
                .or_else(|| it.next())
                .ok_or_else(|| format!("{flag} needs a value"))
        };
        let num = |s: String| s.parse::<u64>().map_err(|e| format!("{flag}: {e}"));
        let flag = flag.trim_start_matches('-');
        match flag {
            "use-case" => a.cfg.use_case = val()?.parse()?,
            "scale" => a.cfg.scale = num(val()?)?,
            "initial-scale" => a.cfg.initial_scale = num(val()?)?,
            "seed" => a.cfg.seed = val()?.parse().map_err(|e| format!("--seed: {e}"))?,
            "timestamp-start" => a.cfg.start = parse_rfc3339(&val()?)?,
            "timestamp-end" => a.cfg.end = parse_rfc3339(&val()?)?,
            "log-interval" => a.cfg.interval = parse_duration(&val()?)?,
            "max-data-points" => a.cfg.limit = num(val()?)?,
            "max-metric-count" => a.cfg.max_metric_count = num(val()?)?,
            "interleaved-generation-group-id" => a.cfg.group_id = num(val()?)?,
            "interleaved-generation-groups" => a.cfg.groups = num(val()?)?,
            "format" => format = Some(val()?),
            "file" => a.file = Some(val()?.into()),
            "out" => a.dir = val()?.into(),
            "threads" => a.threads = num(val()?)? as usize,
            "chunk-points" => a.chunk_points = num(val()?)?,
            "debug" => {
                val()?;
            }
            "h" | "help" => {
                println!("{USAGE}");
                exit(0);
            }
            _ => return Err(format!("unknown flag --{flag}\n{USAGE}")),
        }
    }
    a.out = match format.as_deref() {
        Some("parquet") => Out::Parquet,
        Some("arrow") => Out::Arrow,
        Some(f) => Out::Text(f.parse()?),
        None => return Err(format!("--format is required\n{USAGE}")),
    };
    Ok(a)
}

fn main() {
    let a = parse().unwrap_or_else(|e| {
        eprintln!("{e}");
        exit(2);
    });
    if let Err(e) = run(a) {
        eprintln!("tsbs-gen: {e}");
        exit(1);
    }
}

fn run(a: Args) -> Result<(), String> {
    let t = Tsbs::new(a.cfg)?;
    let cp = if a.chunk_points == 0 {
        populate_tsbs::DEFAULT_CHUNK_POINTS
    } else {
        a.chunk_points
    };
    let t0 = Instant::now();
    match a.out {
        Out::Text(f) => {
            let e = |e: io::Error| e.to_string();
            match &a.file {
                Some(p) => {
                    let w = BufWriter::with_capacity(1 << 20, File::create(p).map_err(e)?);
                    t.write_text(f, w, a.threads, cp)
                        .map_err(e)?
                        .flush()
                        .map_err(e)?;
                }
                None => {
                    let w = BufWriter::with_capacity(1 << 20, io::stdout());
                    t.write_text(f, w, a.threads, cp)
                        .map_err(e)?
                        .flush()
                        .map_err(e)?;
                }
            }
        }
        Out::Parquet => {
            std::fs::create_dir_all(&a.dir).map_err(|e| e.to_string())?;
            let names = t.arrow(cp).table_names();
            let mut ws = Vec::new();
            for n in &names {
                let p = a.dir.join(format!("{n}.parquet"));
                ws.push(BufWriter::new(File::create(&p).map_err(|e| e.to_string())?));
            }
            t.write_parquet(ws, a.threads, cp, None)
                .map_err(|e| e.to_string())?;
            eprintln!(
                "wrote {} tables to {} in {:.2?}",
                names.len(),
                a.dir.display(),
                t0.elapsed()
            );
        }
        Out::Arrow => {
            let src = t.arrow(cp);
            let mut rows = 0usize;
            let r: Result<(), ()> = par_sources(
                &[src.num_chunks()],
                a.threads,
                |_, i| {
                    let mut v = Vec::new();
                    src.chunk(i, &mut |k, b| v.push((k, b)));
                    v
                },
                |_, v| {
                    rows += v.iter().map(|(_, b)| b.num_rows()).sum::<usize>();
                    Ok(())
                },
            );
            r.ok();
            eprintln!("{rows} rows in {:.2?}", t0.elapsed());
        }
    }
    let _ = UseCase::ALL;
    Ok(())
}
