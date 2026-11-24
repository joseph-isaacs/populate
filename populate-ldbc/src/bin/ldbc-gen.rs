//! Generate LDBC SNB data, byte-identical to the LDBC Spark datagen's
//! `--mode raw` output.
//!
//! ```text
//! ldbc-gen [--sf 1 | --persons N] [--resources DIR] [--out DIR]
//!          [--format csv|parquet|none] [--threads N]
//! ```
//!
//! `--resources` is the datagen's `src/main/resources` directory (default:
//! `$LDBC_DATAGEN_RESOURCES`, see `scripts/fetch-ldbc-resources.sh`).
//! `--format csv` (the default) writes `DIR/{dynamic,static}/<Table>.csv`,
//! the rows of the datagen's `graphs/csv/raw/composite-merged-fk/...`
//! files with one header; `parquet` writes `DIR/.../<Table>.parquet`;
//! `none` generates and drops the rows.

use populate_ldbc::rows::{DYNAMIC, STATIC};
use populate_ldbc::{Ldbc, csv, default_resources, persons_for_scale_factor, to_batch};
use populate_table::text::write_text_many;
use populate_table::write_parquet_many;
use std::fs::{self, File};
use std::io::{self, BufWriter, Write};
use std::path::PathBuf;
use std::process::exit;
use std::time::Instant;

#[derive(Clone, Copy, PartialEq)]
enum Format {
    Csv,
    Parquet,
    None,
}

const USAGE: &str = "usage: ldbc-gen [--sf 1 | --persons N] [--resources DIR] [--out DIR] \
[--format csv|parquet|none] [--threads N]";

struct Args {
    persons: u64,
    resources: Option<PathBuf>,
    out: PathBuf,
    format: Format,
    threads: usize,
}

fn parse() -> Result<Args, String> {
    let mut a = Args {
        persons: persons_for_scale_factor("1").expect("sf 1"),
        resources: default_resources(),
        out: PathBuf::from("ldbc-out"),
        format: Format::Csv,
        threads: 0,
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let mut val = || it.next().ok_or_else(|| format!("{flag} needs a value"));
        match flag.as_str() {
            "--sf" => {
                let v = val()?;
                a.persons = persons_for_scale_factor(&v).ok_or_else(|| {
                    format!("unknown scale factor {v} (one of the datagen's: 0.003, 0.1, 0.3, 1, 3, 10, 30, 100, ...)")
                })?;
            }
            "--persons" => a.persons = val()?.parse().map_err(|e| format!("--persons: {e}"))?,
            "--resources" => a.resources = Some(PathBuf::from(val()?)),
            "--out" => a.out = PathBuf::from(val()?),
            "--threads" => a.threads = val()?.parse().map_err(|e| format!("--threads: {e}"))?,
            "--format" => {
                a.format = match val()?.as_str() {
                    "csv" => Format::Csv,
                    "parquet" => Format::Parquet,
                    "none" => Format::None,
                    f => return Err(format!("unknown format {f}")),
                }
            }
            "-h" | "--help" => {
                println!("{USAGE}");
                exit(0);
            }
            f => return Err(format!("unknown flag {f}")),
        }
    }
    if a.persons == 0 {
        return Err("--persons must be positive".into());
    }
    Ok(a)
}

fn main() {
    let a = match parse() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("{e}\n{USAGE}");
            exit(2);
        }
    };
    let Some(res) = a.resources.clone() else {
        eprintln!(
            "no datagen resources: pass --resources DIR or set LDBC_DATAGEN_RESOURCES \
             (scripts/fetch-ldbc-resources.sh downloads them)"
        );
        exit(2);
    };
    if let Err(e) = run(&a, res) {
        eprintln!("ldbc-gen: {e}");
        exit(1);
    }
}

fn run(a: &Args, res: PathBuf) -> io::Result<()> {
    let t0 = Instant::now();
    let g = Ldbc::new(&res, a.persons, a.threads)?;
    let tm = &g.timings;
    eprintln!(
        "{} persons, {} friendships, {} blocks: dictionaries {:.2?}, persons {:.2?}, knows {:.2?}, states {:.2?}",
        g.num_persons(),
        g.num_knows(),
        g.num_blocks(),
        tm.dictionaries,
        tm.persons,
        tm.knows,
        tm.states
    );
    let t1 = Instant::now();
    let ext = match a.format {
        Format::Csv => "csv",
        Format::Parquet => "parquet",
        Format::None => "",
    };
    let (dyn_dir, static_dir) = (a.out.join("dynamic"), a.out.join("static"));
    if a.format != Format::None {
        fs::create_dir_all(&dyn_dir)?;
        fs::create_dir_all(&static_dir)?;
    }
    let files = |dir: &PathBuf,
                 names: &mut dyn Iterator<Item = &str>|
     -> io::Result<Vec<BufWriter<File>>> {
        names
            .map(|n| {
                Ok(BufWriter::with_capacity(
                    1 << 20,
                    File::create(dir.join(format!("{n}.{ext}")))?,
                ))
            })
            .collect()
    };
    let mut rows_total = 0usize;
    match a.format {
        Format::Csv => {
            let ws = files(&dyn_dir, &mut DYNAMIC.iter().map(|d| d.name))?;
            for mut w in write_text_many(&[&g.csv()], ws, a.threads)? {
                w.flush()?;
            }
            let st = g.static_rows();
            for (def, r) in STATIC.iter().zip(&st) {
                let mut buf = Vec::new();
                csv::header(def, &mut buf);
                csv::write(def, r, &mut buf);
                fs::write(static_dir.join(format!("{}.csv", def.name)), buf)?;
            }
        }
        Format::Parquet => {
            let ws = files(&dyn_dir, &mut DYNAMIC.iter().map(|d| d.name))?;
            for mut w in write_parquet_many(&[&g], ws, a.threads, None).map_err(io::Error::other)? {
                w.flush()?;
            }
            let st = g.static_rows();
            for (def, r) in STATIC.iter().zip(&st) {
                let f = File::create(static_dir.join(format!("{}.parquet", def.name)))?;
                let batch = to_batch(def, r);
                let mut w = parquet::arrow::ArrowWriter::try_new(f, batch.schema(), None)
                    .map_err(io::Error::other)?;
                w.write(&batch).map_err(io::Error::other)?;
                w.close().map_err(io::Error::other)?;
            }
        }
        Format::None => {
            rows_total = g.count_rows(a.threads);
        }
    }
    let t2 = Instant::now();
    eprintln!(
        "tables {:.2?}{}; total {:.2?}",
        t2 - t1,
        if a.format == Format::None {
            format!(" ({rows_total} rows)")
        } else {
            String::new()
        },
        t2 - t0
    );
    Ok(())
}
