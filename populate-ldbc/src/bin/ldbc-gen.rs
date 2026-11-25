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
//!
//! `--mode bi` writes the datagen's BI layout (`initial_snapshot/`,
//! `inserts/` and `deletes/` batches); `--explode-edges`,
//! `--explode-attrs` and `--epoch-millis` its other layouts.
//! `--generate-factors` also writes the 34 factor tables, as
//! `DIR/factors/<csv|parquet>/raw/composite-merged-fk/<name>/part-0.*`
//! (`--factor-format`, default `parquet` as the datagen's).

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use populate_ldbc::bi::{Bi, BiRun, Period};
use populate_ldbc::layout::{Layout, LayoutOptions, LayoutRun};
use populate_ldbc::rows::{Rows, TableDef};
use populate_ldbc::{Ldbc, csv, default_resources, persons_for_scale_factor, to_batch};
use populate_table::text::write_text_many;
use populate_table::{Chunks, MultiChunks, par_sources, write_parquet_many};
use std::env::args;
use std::fs::{self, File};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process::exit;
use std::time::Instant;

#[derive(Clone, Copy, PartialEq)]
enum Format {
    Csv,
    Parquet,
    None,
}

impl Format {
    fn ext(self) -> &'static str {
        match self {
            Format::Csv => "csv",
            Format::Parquet => "parquet",
            Format::None => "",
        }
    }
}

const USAGE: &str = "usage: ldbc-gen [--sf 1 | --persons N] [--resources DIR] [--out DIR] \
[--format csv|parquet|none] [--threads N] [--mode raw|bi] [--bulkload-portion 0.97] \
[--batch-period year|month|day|hour|minute] [--explode-edges] [--explode-attrs] [--epoch-millis] \
[--generate-factors] [--factor-format csv|parquet]";

struct Args {
    persons: u64,
    resources: Option<PathBuf>,
    out: PathBuf,
    format: Format,
    threads: usize,
    /// `--mode bi`, with its settings.
    bi: Option<Bi>,
    layout: LayoutOptions,
    /// `--generate-factors`, in this format.
    factors: Option<Format>,
}

fn parse() -> Result<Args, String> {
    let mut a = Args {
        persons: persons_for_scale_factor("1").expect("sf 1"),
        resources: default_resources(),
        out: PathBuf::from("ldbc-out"),
        format: Format::Csv,
        threads: 0,
        bi: None,
        layout: LayoutOptions::default(),
        factors: None,
    };
    let mut factor_format = Format::Parquet;
    let mut factors = false;
    let mut bi = Bi::default();
    let mut it = args().skip(1);
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
            "--mode" => {
                a.bi = match val()?.as_str() {
                    "raw" => None,
                    "bi" => Some(bi),
                    m => return Err(format!("unknown mode {m} (raw or bi)")),
                }
            }
            "--bulkload-portion" => {
                let v: f64 = val()?
                    .parse()
                    .map_err(|e| format!("--bulkload-portion: {e}"))?;
                if !(0.0..=1.0).contains(&v) {
                    return Err(format!("--bulkload-portion must be in [0, 1], not {v}"));
                }
                bi.bulkload_portion = v
            }
            "--batch-period" => {
                let v = val()?;
                bi.period = Period::parse(&v).ok_or_else(|| {
                    format!("unknown batch period {v} (year, month, day, hour, minute)")
                })?
            }
            "--explode-edges" => a.layout.explode_edges = true,
            "--explode-attrs" => a.layout.explode_attrs = true,
            "--epoch-millis" => a.layout.epoch_millis = true,
            "--generate-factors" => factors = true,
            "--factor-format" => {
                factor_format = match val()?.as_str() {
                    "csv" => Format::Csv,
                    "parquet" => Format::Parquet,
                    f => return Err(format!("unknown factor format {f} (csv or parquet)")),
                }
            }
            "-h" | "--help" => {
                println!("{USAGE}");
                exit(0);
            }
            f => return Err(format!("unknown flag {f}")),
        }
    }
    a.factors = factors.then_some(factor_format);
    if let Some(b) = &mut a.bi {
        *b = bi;
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
    let mut g = Ldbc::new(&res, a.persons, a.threads)?;
    if a.factors.is_some() {
        g.collect_factors();
    }
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
    let layout = Layout::new(a.layout);
    let names: Vec<&str> = layout.dynamic.iter().map(|t| t.def.name).collect();
    let lr = LayoutRun {
        g: &g,
        layout: &layout,
    };
    let rows = match a.bi {
        None => {
            let ws = files(a, &a.out.join("dynamic"), &names)?;
            // the raw layout straight from the generator; others projected
            let raw = a.layout == LayoutOptions::default();
            let rows = match a.format {
                Format::Csv if raw => finish(write_text_many(&[&g.csv()], ws, a.threads)?)?,
                Format::Csv => finish(write_text_many(&[&lr], ws, a.threads)?)?,
                Format::Parquet => {
                    let src: &dyn MultiChunks = if raw { &g } else { &lr };
                    finish(
                        write_parquet_many(&[src], ws, a.threads, None)
                            .map_err(io::Error::other)?,
                    )?
                }
                Format::None => g.count_rows(a.threads),
            };
            write_static(a, &layout, &lr, &a.out.join("static"))?;
            rows
        }
        Some(bi) => {
            let run = BiRun::new(&g, &layout, bi);
            let snap = a.out.join("initial_snapshot");
            let ws = files(a, &snap.join("dynamic"), &names)?;
            let mut rows = match a.format {
                Format::Csv => finish(write_text_many(&[&run], ws, a.threads)?)?,
                Format::Parquet => finish(
                    write_parquet_many(&[&run], ws, a.threads, None).map_err(io::Error::other)?,
                )?,
                Format::None => count_snapshot(&run, a.threads),
            };
            write_static(a, &layout, &lr, &snap.join("static"))?;
            let (inserts, deletes) = run.finish(a.threads);
            for (kind, tables) in [("inserts", inserts), ("deletes", deletes)] {
                for t in tables {
                    for (id, r) in &t.batches {
                        rows += r.len();
                        let dir = a.out.join(kind).join("dynamic").join(t.def.name);
                        write_table(a, t.def, r, &dir.join(format!("batch_id={id}")), "part-0")?;
                    }
                }
            }
            rows
        }
    };
    if let Some(f) = a.factors {
        let t = Instant::now();
        let dir = a
            .out
            .join("factors")
            .join(f.ext())
            .join("raw")
            .join("composite-merged-fk");
        for x in g.factors(a.threads) {
            write_factor(f, x.def, &x.rows, &dir.join(x.def.name), a.threads)?;
        }
        eprintln!("factors {:.2?}", t.elapsed());
    }
    let t2 = Instant::now();
    // rows are counted only when nothing is written
    let counted = if a.format == Format::None {
        format!(" ({rows} rows)")
    } else {
        String::new()
    };
    eprintln!("tables {:.2?}{counted}; total {:.2?}", t2 - t1, t2 - t0);
    Ok(())
}

/// One buffered file per dynamic table in `dir` (none for `--format none`).
fn files(a: &Args, dir: &Path, names: &[&str]) -> io::Result<Vec<BufWriter<File>>> {
    if a.format == Format::None {
        return Ok(Vec::new());
    }
    fs::create_dir_all(dir)?;
    names
        .iter()
        .map(|n| {
            let f = File::create(dir.join(format!("{n}.{}", a.format.ext())))?;
            Ok(BufWriter::with_capacity(1 << 20, f))
        })
        .collect()
}

/// Flush the writers (rows are not counted: 0).
fn finish(ws: Vec<BufWriter<File>>) -> io::Result<usize> {
    for mut w in ws {
        w.flush()?;
    }
    Ok(0)
}

fn count_snapshot(run: &BiRun, threads: usize) -> usize {
    let n = MultiChunks::num_chunks(run);
    let counts: Vec<usize> = par_chunks(n, threads, |i| {
        run.chunk_snapshot(i).iter().map(Rows::len).sum()
    });
    counts.into_iter().sum()
}

/// `f(i)` for `i in 0..n` on `threads` threads (0 = all cores).
fn par_chunks<T: Send>(n: u64, threads: usize, f: impl Fn(u64) -> T + Sync) -> Vec<T> {
    let mut out = Vec::new();
    let r: Result<(), ()> = par_sources(
        &[n],
        threads,
        |_, i| f(i),
        |_, t| {
            out.push(t);
            Ok(())
        },
    );
    r.expect("no errors");
    out
}

fn write_static(a: &Args, layout: &Layout, lr: &LayoutRun, dir: &Path) -> io::Result<()> {
    if a.format == Format::None {
        return Ok(());
    }
    for (t, r) in layout.statics.iter().zip(&lr.static_rows()) {
        write_table(a, t.def, r, dir, t.def.name)?;
    }
    Ok(())
}

/// One table as `dir/name.csv` (with a header) or `dir/name.parquet`.
fn write_table(a: &Args, def: &TableDef, r: &Rows, dir: &Path, name: &str) -> io::Result<()> {
    if a.format == Format::None {
        return Ok(());
    }
    fs::create_dir_all(dir)?;
    let path = dir.join(format!("{name}.{}", a.format.ext()));
    match a.format {
        Format::Csv => {
            let mut buf = Vec::new();
            csv::header(def, &mut buf);
            csv::write(def, r, &mut buf);
            fs::write(path, buf)
        }
        Format::Parquet => write_parquet(def, r, &path, a.threads),
        Format::None => Ok(()),
    }
}

/// A factor table as `dir/part-0.csv` (Spark's default CSV options, no
/// header) or `dir/part-0.parquet`.
fn write_factor(f: Format, def: &TableDef, r: &Rows, dir: &Path, threads: usize) -> io::Result<()> {
    fs::create_dir_all(dir)?;
    let path = dir.join(format!("part-0.{}", f.ext()));
    match f {
        Format::Csv => {
            let mut buf = Vec::new();
            csv::write_factor(def, r, &mut buf);
            fs::write(path, buf)
        }
        Format::Parquet => write_parquet(def, r, &path, threads),
        Format::None => Ok(()),
    }
}

/// One built table as row groups of up to [`ROW_GROUP`] rows, each a
/// zero-copy slice of the batch.
struct Built(RecordBatch);

/// Rows per row group of a static or factor table (`ArrowWriter`'s default).
const ROW_GROUP: usize = 1 << 20;

impl Chunks for Built {
    fn schema(&self) -> SchemaRef {
        self.0.schema()
    }

    fn num_chunks(&self) -> u64 {
        self.0.num_rows().div_ceil(ROW_GROUP) as u64
    }

    fn chunk(&self, i: u64, emit: &mut dyn FnMut(RecordBatch)) {
        let start = i as usize * ROW_GROUP;
        emit(
            self.0
                .slice(start, ROW_GROUP.min(self.0.num_rows() - start)),
        );
    }
}

/// A built table through the same parallel encoder as the generated ones.
fn write_parquet(def: &TableDef, r: &Rows, path: &Path, threads: usize) -> io::Result<()> {
    let w = BufWriter::with_capacity(1 << 20, File::create(path)?);
    let w = populate_table::write_parquet(&Built(to_batch(def, r)), w, threads, None)
        .map_err(io::Error::other)?;
    w.into_inner().map(drop).map_err(io::Error::other)
}
