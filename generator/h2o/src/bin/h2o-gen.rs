//! Generate the H2O db-benchmark data sets, byte-identical to the R
//! datagen scripts' CSV.
//!
//! ```text
//! h2o-gen groupby [--n 1e7] [--k 1e2] [--nas 0] [--sort 0] [--out DIR]
//!                 [--format csv|parquet|none] [--threads N] [--chunk ROWS]
//! h2o-gen join    [--n 1e7] [--nas 0] [--sort 0] [--out DIR] [...]
//! ```
//!
//! `--format csv` (the default) writes the files the scripts write
//! (`G1_1e7_1e2_0_0.csv`; `J1_1e7_NA_0_0.csv` and its three right-hand
//! tables), `parquet` the same tables as `.parquet`, and `none` generates
//! Arrow batches and drops them.

use populate_h2o::{GroupBy, Join};
use populate_table::chunks::for_each_batch;
use populate_table::cli::{create, fail, file_size};
use populate_table::text::{TextChunks, write_text_many};
use populate_table::{Chunks, DEFAULT_CHUNK_ROWS, write_parquet};
use std::env::args;
use std::fs::create_dir_all;
use std::path::PathBuf;
use std::process::exit;
use std::time::Instant;

#[derive(Clone, Copy, PartialEq)]
enum Format {
    Csv,
    Parquet,
    None,
}

struct Args {
    join: bool,
    n: u64,
    k: u64,
    nas: u32,
    sort: bool,
    out: PathBuf,
    format: Format,
    threads: usize,
    chunk: u64,
}

const USAGE: &str = "usage: h2o-gen groupby|join [--n 1e7] [--k 1e2] [--nas 0] [--sort 0] \
[--out DIR] [--format csv|parquet|none] [--threads N] [--chunk ROWS]";

fn num(s: &str) -> Result<u64, String> {
    let v: f64 = s.parse().map_err(|e| format!("`{s}`: {e}"))?;
    if v < 0.0 || v.fract() != 0.0 {
        return Err(format!("`{s}` is not a whole number"));
    }
    Ok(v as u64)
}

fn parse() -> Result<Args, String> {
    let mut it = args().skip(1);
    let join = match it.next().as_deref() {
        Some("groupby") => false,
        Some("join") => true,
        Some("-h" | "--help") => {
            println!("{USAGE}");
            exit(0);
        }
        _ => return Err("first argument must be `groupby` or `join`".into()),
    };
    let mut a = Args {
        join,
        n: 10_000_000,
        k: 100,
        nas: 0,
        sort: false,
        out: PathBuf::from("."),
        format: Format::Csv,
        threads: 0,
        chunk: DEFAULT_CHUNK_ROWS,
    };
    while let Some(flag) = it.next() {
        let mut val = || it.next().ok_or_else(|| format!("{flag} needs a value"));
        match flag.as_str() {
            "--n" => a.n = num(&val()?)?,
            "--k" => a.k = num(&val()?)?,
            "--nas" => {
                a.nas = match num(&val()?)? {
                    v @ 0..=100 => v as u32,
                    v => return Err(format!("--nas must be in 0..=100, not {v}")),
                }
            }
            "--sort" => {
                a.sort = match num(&val()?)? {
                    0 => false,
                    1 => true,
                    v => return Err(format!("--sort must be 0 or 1, not {v}")),
                }
            }
            "--out" => a.out = val()?.into(),
            "--format" => {
                a.format = match val()?.as_str() {
                    "csv" => Format::Csv,
                    "parquet" => Format::Parquet,
                    "none" => Format::None,
                    f => return Err(format!("unknown format `{f}`")),
                }
            }
            "--threads" => a.threads = num(&val()?)? as usize,
            "--chunk" => a.chunk = num(&val()?)?,
            f => return Err(format!("unknown flag `{f}`")),
        }
    }
    if a.nas > 100 || a.chunk == 0 {
        return Err("--nas must be in 0..=100 and --chunk > 0".into());
    }
    Ok(a)
}

/// Write one table as `a.format` asks.
fn emit<T: Chunks + TextChunks>(a: &Args, name: &str, t: &T) {
    let t1 = Instant::now();
    let path = match a.format {
        Format::Parquet => a.out.join(name.replace(".csv", ".parquet")),
        _ => a.out.join(name),
    };
    let create = || create(std::slice::from_ref(&path)).pop().expect("one file");
    match a.format {
        Format::Csv => {
            write_text_many(&[t as &dyn TextChunks], vec![create()], a.threads)
                .unwrap_or_else(|e| fail(&path.display(), &e));
        }
        Format::Parquet => {
            write_parquet(t, create(), a.threads, None)
                .unwrap_or_else(|e| fail(&path.display(), &e));
        }
        Format::None => {
            let mut rows = 0;
            for_each_batch(t, a.threads, |b| rows += b.num_rows());
        }
    }
    let size = file_size(&path);
    eprintln!(
        "{name}: {:.2}s, {:.1} MB",
        t1.elapsed().as_secs_f64(),
        size as f64 / 1e6
    );
}

fn main() {
    let a = parse().unwrap_or_else(|e| {
        eprintln!("{e}\n{USAGE}");
        exit(2);
    });
    if a.format != Format::None {
        create_dir_all(&a.out).unwrap_or_else(|e| fail(&a.out.display(), &e));
    }
    let t0 = Instant::now();
    if a.join {
        let j = Join::with_chunk_rows(a.n, a.nas, a.sort, a.chunk);
        eprintln!("keys in {:.2}s", t0.elapsed().as_secs_f64());
        for t in j.tables() {
            emit(&a, &t.file_name(), &t);
        }
    } else {
        let g = GroupBy::with_chunk_rows(a.n, a.k, a.nas, a.sort, a.chunk, a.threads);
        eprintln!("plan in {:.2}s", t0.elapsed().as_secs_f64());
        emit(&a, &g.file_name(), &g);
    }
    eprintln!("total {:.2}s", t0.elapsed().as_secs_f64());
}
