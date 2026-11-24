//! Generate the TPC-C initial database or the YCSB load.
//!
//! ```text
//! oltp-gen tpcc [--warehouses 1] [--tables a,b,...] [--seed 0] [--load-time MICROS]
//!               [common flags]
//! oltp-gen ycsb [--records 1000] [--insert-start 0] [--insert-count N]
//!               [--field-count 10] [--field-length 100] [--field-prefix field]
//!               [--ordered] [--zero-padding 1] [--data-integrity]
//!               [--java-seed 0] [--java-thread 1] [common flags]
//!
//! common flags: [--out DIR] [--format parquet|text|none] [--threads N]
//!               [--batch ROWS] [--row-group ROWS] [--compression snappy|none]
//!               [--no-dictionary]
//! ```
//!
//! `--format parquet` (the default) writes `DIR/<table>.parquet`; `--format
//! text` writes TPC-C as `DIR/<table>.csv` (go-tpc's CSV layout) and YCSB as
//! `DIR/usertable.tsv` (`key\tfield0\t...`); `--format none` only generates.
//! Every chunk of every table is a job on one pool of `--threads` threads
//! (default: all cores); the data is the same for any thread count and
//! `--row-group`.

use arrow_array::cast::AsArray;
use arrow_array::{Array, RecordBatch};
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use populate_oltp::{
    JavaTlr, Tpcc, TpccTable, TpccTableName, Ycsb, for_each_batch_many, write_parquet_many,
    write_text_many,
};
use populate_table::Chunks;
use populate_table::text::TextChunks;
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process::exit;
use std::str::FromStr;
use std::time::Instant;

const USAGE: &str = "usage: oltp-gen tpcc|ycsb [flags]  (see --help)";

const HELP: &str =
    "oltp-gen tpcc [--warehouses 1] [--tables a,b,...] [--seed 0] [--load-time MICROS]
oltp-gen ycsb [--records 1000] [--insert-start 0] [--insert-count N] [--field-count 10]
              [--field-length 100] [--field-prefix field] [--ordered] [--zero-padding 1]
              [--data-integrity] [--java-seed 0] [--java-thread 1]
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
        "tpcc" => tpcc(args),
        "ycsb" => ycsb(args),
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

fn tpcc(args: impl Iterator<Item = String>) -> Result<(), String> {
    let mut warehouses = 1u32;
    let mut names = TpccTableName::ALL.to_vec();
    let mut seed = 0u64;
    let mut load_time = None;
    let c = parse(args, |flag, val| {
        match flag {
            "--warehouses" => warehouses = num(flag, val()?)?,
            "--seed" => seed = num(flag, val()?)?,
            "--load-time" => load_time = Some(num(flag, val()?)?),
            "--tables" => {
                names = val()?
                    .split(',')
                    .map(str::parse)
                    .collect::<Result<_, _>>()?
            }
            _ => return Ok(false),
        }
        Ok(true)
    })?;
    if warehouses == 0 {
        return Err("--warehouses must be > 0".into());
    }
    let mut db = Tpcc::new(warehouses).seed(seed);
    if let Some(t) = load_time {
        db = db.load_time(t);
    }
    // biggest first, so they start first on the shared pool
    names.sort_by_key(|&t| std::cmp::Reverse(db.table(t).approx_rows()));
    let tables: Vec<TpccTable> = names
        .iter()
        .map(|&t| {
            let mut t = db.table(t);
            if let Some(b) = c.batch {
                t = t.batch_size(b);
            }
            if let Some(r) = c.row_group {
                t = t.chunk_rows(r);
            }
            t
        })
        .collect();
    let files: Vec<String> = names.iter().map(|t| t.to_string()).collect();
    run(&c, &tables, &files, "csv", "tpcc")
}

fn ycsb(args: impl Iterator<Item = String>) -> Result<(), String> {
    let mut tlr = JavaTlr {
        seed: 0,
        thread_id: 1,
    };
    let mut records = 1000;
    let mut sets: Vec<Box<dyn FnOnce(Ycsb) -> Ycsb>> = Vec::new();
    let c = parse(args, |flag, val| {
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
            _ => return Ok(false),
        }
        Ok(true)
    })?;
    let mut y = Ycsb::new(records);
    for s in sets {
        y = s(y);
    }
    y = y.java_random(tlr);
    if let Some(b) = c.batch {
        y = y.batch_size(b);
    }
    if let Some(r) = c.row_group {
        y = y.chunk_rows(r);
    }
    run(&c, &[y], &["usertable".to_string()], "tsv", "ycsb")
}

fn run<T: Chunks + TextChunks>(
    c: &Common,
    tables: &[T],
    names: &[String],
    text_ext: &str,
    default_dir: &str,
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
            let props = WriterProperties::builder()
                .set_compression(c.compression)
                .set_dictionary_enabled(c.dictionary)
                .build();
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
        self.1 += buf[..n].iter().filter(|&&b| b == b'\n').count() as u64;
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}
