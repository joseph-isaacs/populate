//! Generate the YCSB load, or the operations of a YCSB run.
//!
//! ```text
//! ycsb-gen load [--records 1000] [--insert-start 0] [--insert-count N]
//!               [--field-count 10] [--field-length 100] [--field-prefix field]
//!               [--ordered] [--zero-padding 1] [--data-integrity]
//!               [--java-seed 0] [--java-thread 1] [common flags]
//! ycsb-gen run  [load flags] [--operations 1000] [--workload a-f]
//!               [--read P] [--update P] [--insert P] [--scan P] [--rmw P]
//!               [--distribution uniform|zipfian|latest|hotspot]
//!               [--hotspot-data 0.2] [--hotspot-ops 0.8] [--read-one-field]
//!               [--write-all-fields] [--scan-length MIN,MAX] [--scan-zipfian]
//!               [common flags]
//!
//! common flags: [--out DIR] [--format parquet|text|none] [--threads N]
//!               [--batch ROWS] [--row-group ROWS] [--compression snappy|none]
//!               [--no-dictionary]
//! ```
//!
//! `--format parquet` (the default) writes the load as
//! `DIR/usertable.parquet` and the run as `DIR/transactions.parquet` (one
//! row per binding call: `op`, key, scan length, fields read, values
//! written); `--format text` writes them as TSV (`key\tfield0\t...` for the
//! load); `--format none` only generates.
//! Every chunk of every table is a job on one pool of `--threads` threads
//! (default: all cores); the data is the same for any thread count and
//! `--row-group`.

use arrow_array::cast::AsArray;
use arrow_array::{Array, RecordBatch};
#[cfg(feature = "mimalloc")]
use mimalloc::MiMalloc;
use parquet::basic::Compression;
use parquet::file::properties::WriterPropertiesBuilder;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::schema::types::ColumnPath;
use populate_table::Chunks;
use populate_table::text::TextChunks;
use populate_ycsb::{
    JavaTlr, RequestDistribution, Ycsb, YcsbRun, for_each_batch_many, write_parquet_many,
    write_text_many,
};
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process::exit;
use std::str::FromStr;
use std::time::Instant;

/// mimalloc: buffers are reused instead of returned to the OS and faulted
/// in again
#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

const USAGE: &str = "usage: ycsb-gen load|run [flags]  (see --help)";

const HELP: &str =
    "ycsb-gen load [--records 1000] [--insert-start 0] [--insert-count N] [--field-count 10]
              [--field-length 100] [--field-prefix field] [--ordered] [--zero-padding 1]
              [--data-integrity] [--java-seed 0] [--java-thread 1]
ycsb-gen run  [load flags] [--operations 1000] [--workload a-f]
              [--read P] [--update P] [--insert P] [--scan P] [--rmw P]
              [--distribution uniform|zipfian|latest|hotspot] [--hotspot-data 0.2]
              [--hotspot-ops 0.8] [--read-one-field] [--write-all-fields]
              [--scan-length MIN,MAX] [--scan-zipfian]
common: [--out DIR] [--format parquet|text|none] [--threads N] [--batch ROWS]
        [--row-group ROWS] [--compression snappy|none] [--no-dictionary]";

#[derive(Clone, Copy, PartialEq)]
enum Format {
    Parquet,
    Text,
    None,
}

struct Common {
    out: Option<PathBuf>,
    format: Format,
    threads: usize,
    batch: Option<usize>,
    row_group: Option<u64>,
    compression: Compression,
    dictionary: bool,
}

fn num<T: FromStr>(flag: &str, v: String) -> Result<T, String>
where
    T::Err: std::fmt::Display,
{
    v.parse().map_err(|e| format!("{flag}: {e}"))
}

fn main() {
    let mut args = std::env::args().skip(1);
    let cmd = args.next().unwrap_or_default();
    let r = match cmd.as_str() {
        "load" => ycsb(args),
        "run" => ycsb_run(args),
        "-h" | "--help" => {
            println!("{HELP}");
            exit(0);
        }
        _ => Err(format!("unknown command `{cmd}`")),
    };
    if let Err(e) = r {
        eprintln!("{e}\n{USAGE}");
        exit(2);
    }
}

/// Parse the flags: `own` handles the command's own, returning false for
/// flags it doesn't know.
fn parse(
    mut it: impl Iterator<Item = String>,
    mut own: impl FnMut(&str, &mut dyn FnMut() -> Result<String, String>) -> Result<bool, String>,
) -> Result<Common, String> {
    let mut c = Common {
        out: None,
        format: Format::Parquet,
        threads: 0,
        batch: None,
        row_group: None,
        compression: Compression::SNAPPY,
        dictionary: true,
    };
    while let Some(flag) = it.next() {
        let mut val = || it.next().ok_or_else(|| format!("{flag} needs a value"));
        match flag.as_str() {
            "--out" => c.out = Some(val()?.into()),
            "--format" => {
                c.format = match val()?.as_str() {
                    "parquet" => Format::Parquet,
                    "text" => Format::Text,
                    "none" => Format::None,
                    f => return Err(format!("unknown format `{f}`")),
                }
            }
            "--threads" => c.threads = num(&flag, val()?)?,
            "--batch" => c.batch = Some(num(&flag, val()?)?),
            "--row-group" => c.row_group = Some(num(&flag, val()?)?),
            "--compression" => {
                c.compression = match val()?.as_str() {
                    "snappy" => Compression::SNAPPY,
                    "none" => Compression::UNCOMPRESSED,
                    x => return Err(format!("unknown compression `{x}`")),
                }
            }
            "--no-dictionary" => c.dictionary = false,
            "-h" | "--help" => {
                println!("{HELP}");
                exit(0);
            }
            f => {
                if !own(f, &mut val)? {
                    return Err(format!("unknown flag `{f}`"));
                }
            }
        }
    }
    if c.batch == Some(0) || c.row_group == Some(0) {
        return Err("--batch and --row-group must be > 0".into());
    }
    Ok(c)
}

fn ycsb(args: impl Iterator<Item = String>) -> Result<(), String> {
    ycsb_cmd(args, false)
}

fn ycsb_run(args: impl Iterator<Item = String>) -> Result<(), String> {
    ycsb_cmd(args, true)
}

type RunSet = Box<dyn FnOnce(YcsbRun) -> YcsbRun>;

/// `ycsb` (the load) and `ycsb-run` (the transactions on top of it).
fn ycsb_cmd(args: impl Iterator<Item = String>, transactions: bool) -> Result<(), String> {
    let mut tlr = JavaTlr {
        seed: 0,
        thread_id: 1,
    };
    let mut records = 1000;
    let mut operations: u64 = 1000;
    let mut workload: Option<char> = None;
    let mut sets: Vec<Box<dyn FnOnce(Ycsb) -> Ycsb>> = Vec::new();
    let mut run_sets: Vec<RunSet> = Vec::new();
    let mut proportions: [Option<f64>; 5] = [None; 5];
    let mut distribution: Option<String> = None;
    let mut hotspot = (0.2, 0.8);
    let mut hotspot_set = false;
    let c = parse(args, |flag, val| {
        let mut prop = |k: usize, v: f64| {
            proportions[k] = Some(v);
        };
        match flag {
            "--records" => records = num(flag, val()?)?,
            "--insert-start" => {
                let n: u64 = num(flag, val()?)?;
                sets.push(Box::new(move |y| y.insert_start(n)))
            }
            "--insert-count" => {
                let n: u64 = num(flag, val()?)?;
                sets.push(Box::new(move |y| y.insert_count(n)))
            }
            "--field-count" => {
                let n: usize = num(flag, val()?)?;
                sets.push(Box::new(move |y| y.field_count(n)))
            }
            "--field-length" => {
                let n: usize = num(flag, val()?)?;
                sets.push(Box::new(move |y| y.field_length(n)))
            }
            "--field-prefix" => {
                let p = val()?;
                if !p.bytes().all(|b| b.is_ascii_graphic() || b == b' ') {
                    return Err("--field-prefix must be printable ASCII".into());
                }
                sets.push(Box::new(move |y| y.field_prefix(&p)))
            }
            "--zero-padding" => {
                let n: usize = num(flag, val()?)?;
                sets.push(Box::new(move |y| y.zero_padding(n)))
            }
            "--ordered" => sets.push(Box::new(|y| y.ordered(true))),
            "--data-integrity" => sets.push(Box::new(|y| y.data_integrity(true))),
            "--java-seed" => tlr.seed = num::<i64>(flag, val()?)? as u64,
            "--java-thread" => tlr.thread_id = num(flag, val()?)?,
            "--operations" if transactions => operations = num(flag, val()?)?,
            "--workload" if transactions => {
                let w = val()?.to_ascii_lowercase();
                match w.as_str() {
                    "a" | "b" | "c" | "d" | "e" | "f" => workload = w.chars().next(),
                    _ => return Err(format!("--workload: `{w}` is not a to f")),
                }
            }
            "--read" if transactions => prop(0, num(flag, val()?)?),
            "--update" if transactions => prop(1, num(flag, val()?)?),
            "--insert" if transactions => prop(2, num(flag, val()?)?),
            "--scan" if transactions => prop(3, num(flag, val()?)?),
            "--rmw" if transactions => prop(4, num(flag, val()?)?),
            "--distribution" if transactions => distribution = Some(val()?),
            "--hotspot-data" if transactions => {
                hotspot.0 = num(flag, val()?)?;
                hotspot_set = true;
            }
            "--hotspot-ops" if transactions => {
                hotspot.1 = num(flag, val()?)?;
                hotspot_set = true;
            }
            "--read-one-field" if transactions => {
                run_sets.push(Box::new(|r| r.read_all_fields(false)))
            }
            "--write-all-fields" if transactions => {
                run_sets.push(Box::new(|r| r.write_all_fields(true)))
            }
            "--scan-length" if transactions => {
                let v = val()?;
                let (a, b) = v.split_once(',').ok_or("--scan-length takes MIN,MAX")?;
                let (a, b): (i64, i64) = (num(flag, a.into())?, num(flag, b.into())?);
                if !(0 <= a && a <= b) {
                    return Err("--scan-length: need 0 <= MIN <= MAX".into());
                }
                run_sets.push(Box::new(move |r| {
                    let z = r.scan_zipfian();
                    r.scan_length(a, b, z)
                }))
            }
            "--scan-zipfian" if transactions => run_sets.push(Box::new(|r| {
                let (a, b) = r.scan_lengths();
                r.scan_length(a, b, true)
            })),
            _ => return Ok(false),
        }
        Ok(true)
    })?;
    let mut y = Ycsb::new(records);
    for s in sets {
        y = s(y);
    }
    y = y.java_random(tlr);
    y.check()?;
    if !transactions {
        if let Some(b) = c.batch {
            y = y.batch_size(b);
        }
        if let Some(r) = c.row_group {
            y = y.chunk_rows(r);
        }
        return run(
            &c,
            &[y],
            &["usertable".to_string()],
            "tsv",
            "ycsb",
            Ycsb::parquet_props(),
        );
    }
    let mut r = match workload {
        Some(w) => YcsbRun::workload(w, y, operations),
        None => YcsbRun::new(y, operations),
    };
    if hotspot_set && distribution.as_deref() != Some("hotspot") {
        return Err("--hotspot-data and --hotspot-ops need --distribution hotspot".into());
    }
    if proportions.iter().any(Option::is_some) {
        // override only the proportions given, as YCSB's -p does
        let mut p = r.proportion_values();
        for (v, o) in p.iter_mut().zip(proportions) {
            if let Some(o) = o {
                *v = o;
            }
        }
        if p.iter().any(|x| !(x.is_finite() && *x >= 0.0)) || p.iter().all(|&x| x == 0.0) {
            return Err("proportions must be finite and >= 0, and one > 0".into());
        }
        r = r.proportions(p[0], p[1], p[2], p[3], p[4]);
    }
    if let Some(d) = distribution {
        let (h, o) = hotspot;
        if d == "hotspot" && (!(0.0..=1.0).contains(&h) || !(0.0..=1.0).contains(&o)) {
            return Err("--hotspot-data and --hotspot-ops must be in 0..=1".into());
        }
        r = r.distribution(match d.as_str() {
            "uniform" => RequestDistribution::Uniform,
            "zipfian" => RequestDistribution::Zipfian,
            "latest" => RequestDistribution::Latest,
            "hotspot" => RequestDistribution::Hotspot {
                data_fraction: h,
                op_fraction: o,
            },
            _ => return Err(format!("--distribution: unknown `{d}`")),
        });
    }
    for s in run_sets {
        r = s(r);
    }
    if let Some(b) = c.batch {
        r = r.batch_size(b);
    }
    if let Some(n) = c.row_group {
        r = r.chunk_ops(n);
    }
    r.check()?;
    run(
        &c,
        &[r],
        &["transactions".to_string()],
        "tsv",
        "ycsb-run",
        YcsbRun::parquet_props(),
    )
}

fn run<T: Chunks + TextChunks>(
    c: &Common,
    tables: &[T],
    names: &[String],
    text_ext: &str,
    default_dir: &str,
    props: WriterPropertiesBuilder,
) -> Result<(), String> {
    let out = c.out.clone().unwrap_or_else(|| default_dir.into());
    let fail = |what: &dyn std::fmt::Display, e: &dyn std::fmt::Display| -> ! {
        eprintln!("{what}: {e}");
        exit(1);
    };
    if c.format != Format::None {
        std::fs::create_dir_all(&out).unwrap_or_else(|e| fail(&out.display(), &e));
    }
    let ext = if c.format == Format::Parquet {
        "parquet"
    } else {
        text_ext
    };
    let paths: Vec<PathBuf> = names
        .iter()
        .map(|n| out.join(format!("{n}.{ext}")))
        .collect();
    let create = || -> Vec<BufWriter<File>> {
        paths
            .iter()
            .map(|p| {
                let f = File::create(p).unwrap_or_else(|e| fail(&p.display(), &e));
                BufWriter::with_capacity(1 << 20, f)
            })
            .collect()
    };
    let size = |p: &Path| std::fs::metadata(p).map_or(0, |m| m.len());
    let start = Instant::now();
    let (rows, bytes): (Vec<u64>, Vec<u64>) = match c.format {
        Format::Parquet => {
            let mut props = props.set_compression(c.compression);
            if !c.dictionary {
                for t in tables {
                    for f in t.schema().fields() {
                        props = props.set_column_dictionary_enabled(
                            ColumnPath::from(f.name().as_str()),
                            false,
                        );
                    }
                }
            }
            let props = props.build();
            write_parquet_many(tables, create(), c.threads, Some(props))
                .unwrap_or_else(|e| fail(&"parquet", &e));
            paths.iter().map(|p| (parquet_rows(p), size(p))).unzip()
        }
        Format::Text => {
            let w: Vec<_> = create().into_iter().map(|f| Lines(f, 0)).collect();
            let w = write_text_many(tables, w, c.threads).unwrap_or_else(|e| fail(&"text", &e));
            w.iter().zip(&paths).map(|(l, p)| (l.1, size(p))).unzip()
        }
        Format::None => {
            let (mut rows, mut bytes) = (vec![0; tables.len()], vec![0; tables.len()]);
            for_each_batch_many(tables, c.threads, |t, b| {
                rows[t] += b.num_rows() as u64;
                bytes[t] += batch_bytes(&b);
            });
            (rows, bytes)
        }
    };
    let secs = start.elapsed().as_secs_f64();
    let unit = if c.format == Format::None {
        "Arrow"
    } else {
        "file"
    };
    for ((n, r), b) in names.iter().zip(&rows).zip(&bytes) {
        println!(
            "{n:>12} {r:>12} rows {:>10.1} MiB ({unit})",
            *b as f64 / 1048576.0
        );
    }
    let total: u64 = bytes.iter().sum();
    println!(
        "{:>12} {:>12} rows {:>10.1} MiB in {secs:.2} s ({:.0} MiB/s)",
        "total",
        rows.iter().sum::<u64>(),
        total as f64 / 1048576.0,
        total as f64 / 1048576.0 / secs
    );
    Ok(())
}

/// Rows of a Parquet file, from its footer.
fn parquet_rows(p: &Path) -> u64 {
    File::open(p)
        .ok()
        .and_then(|f| SerializedFileReader::new(f).ok())
        .map_or(0, |r| r.metadata().file_metadata().num_rows() as u64)
}

/// Arrow bytes of a batch: value buffers, and string views plus their data.
fn batch_bytes(b: &RecordBatch) -> u64 {
    b.columns()
        .iter()
        .map(|a| match a.as_string_view_opt() {
            Some(s) => {
                (s.len() * 16 + s.data_buffers().iter().map(|d| d.len()).sum::<usize>()) as u64
            }
            None => a.get_array_memory_size() as u64,
        })
        .sum()
}

/// A writer that counts lines.
struct Lines<W>(W, u64);

impl<W: Write> Write for Lines<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.0.write(buf)?;
        self.1 += newlines(&buf[..n]);
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}

/// Newlines in `b`: counted in `u8` lanes per 255 bytes, which
/// vectorises (a `u64` count per byte ran at ~1.4 GB/s, serially, on the
/// file's writer).
fn newlines(b: &[u8]) -> u64 {
    b.chunks(255)
        .map(|c| c.iter().fold(0u8, |n, &x| n + (x == b'\n') as u8) as u64)
        .sum()
}
