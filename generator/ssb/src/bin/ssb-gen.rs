//! Generate SSB tables, byte-identical to ssb-dbgen.
//!
//! ```text
//! ssb-gen [--sf 1] [--out DIR] [--format parquet|tbl|none] [--tables a,b,...]
//!          [--threads N] [--batch ROWS] [--row-group ROWS]
//!          [--compression snappy|none] [--no-dictionary]
//! ```
//!
//! `--format parquet` (the default) writes `DIR/<table>.parquet`, one file per
//! table, generating and encoding row groups on all cores. `--format tbl`
//! writes `DIR/<table>.tbl`, the same bytes ssb-dbgen's `dbgen -s <sf>`
//! writes. `--tables` takes `part,supplier,customer,date,lineorder`
//! (the default) and `lineorder_flat`. `--format none` generates the Arrow batches
//! and drops them, which measures generation alone. Rows and size per table and the overall time
//! and throughput are printed.
//!
//! Every chunk of every table is a job on one pool of `--threads` threads
//! (default: all cores). Chunks (= Parquet row groups) hold about
//! `--row-group` rows (default 256Ki); the data is the same for any value.

use arrow_array::cast::AsArray;
use arrow_array::{Array, RecordBatch};
#[cfg(feature = "mimalloc")]
use mimalloc::MiMalloc;
use parquet::basic::Compression;
use parquet::file::properties::WriterPropertiesBuilder;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::schema::types::ColumnPath;
use populate_ssb::{
    Ssb, SsbTable, TableName, for_each_batch_many, parquet_props, write_parquet_many,
    write_tbl_many,
};
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process::exit;
use std::time::Instant;

/// mimalloc: buffers are reused instead of returned to the OS and faulted
/// in again for every batch
#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[derive(Clone, Copy, PartialEq)]
enum Format {
    Parquet,
    Tbl,
    None,
}

struct Args {
    sf: f64,
    out: PathBuf,
    format: Format,
    tables: Vec<TableName>,
    threads: usize,
    batch: usize,
    row_group: Option<u64>,
    compression: Compression,
    dictionary: bool,
}

const USAGE: &str = "usage: ssb-gen [--sf 1] [--out DIR] [--format parquet|tbl|none] \
[--tables a,b,...] [--threads N] [--batch ROWS] [--row-group ROWS] [--compression snappy|none] [--no-dictionary]";

fn parse() -> Result<Args, String> {
    let mut a = Args {
        sf: 1.0,
        out: PathBuf::from("ssb"),
        format: Format::Parquet,
        tables: TableName::ALL.to_vec(),
        threads: 0,
        batch: populate_ssb::DEFAULT_BATCH_SIZE,
        row_group: None,
        compression: Compression::SNAPPY,
        dictionary: true,
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let mut val = || it.next().ok_or_else(|| format!("{flag} needs a value"));
        match flag.as_str() {
            "--sf" => a.sf = val()?.parse().map_err(|e| format!("--sf: {e}"))?,
            "--out" => a.out = val()?.into(),
            "--format" => {
                a.format = match val()?.as_str() {
                    "parquet" => Format::Parquet,
                    "tbl" => Format::Tbl,
                    "none" => Format::None,
                    f => return Err(format!("unknown format `{f}`")),
                }
            }
            "--tables" => {
                a.tables = val()?
                    .split(',')
                    .map(str::parse)
                    .collect::<Result<_, _>>()?;
            }
            "--threads" => a.threads = val()?.parse().map_err(|e| format!("--threads: {e}"))?,
            "--batch" => a.batch = val()?.parse().map_err(|e| format!("--batch: {e}"))?,
            "--row-group" => {
                a.row_group = Some(val()?.parse().map_err(|e| format!("--row-group: {e}"))?)
            }
            "--compression" => {
                a.compression = match val()?.as_str() {
                    "snappy" => Compression::SNAPPY,
                    "none" => Compression::UNCOMPRESSED,
                    c => return Err(format!("unknown compression `{c}`")),
                }
            }
            "--no-dictionary" => a.dictionary = false,
            "-h" | "--help" => {
                println!("{USAGE}");
                exit(0);
            }
            f => return Err(format!("unknown flag `{f}`")),
        }
    }
    if a.sf <= 0.0 || a.sf > populate_ssb::MAX_SCALE_FACTOR {
        return Err(format!(
            "--sf must be in (0, {}]",
            populate_ssb::MAX_SCALE_FACTOR
        ));
    }
    if a.batch == 0 || a.row_group == Some(0) {
        return Err("--batch and --row-group must be > 0".into());
    }
    Ok(a)
}

fn main() {
    let a = parse().unwrap_or_else(|e| {
        eprintln!("{e}\n{USAGE}");
        exit(2);
    });
    let ds = Ssb::new(a.sf);
    let fail = |what: &dyn std::fmt::Display, e: &dyn std::fmt::Display| -> ! {
        eprintln!("{what}: {e}");
        exit(1);
    };
    if a.format != Format::None {
        std::fs::create_dir_all(&a.out).unwrap_or_else(|e| fail(&a.out.display(), &e));
    }
    // Every chunk of every table goes to one pool, biggest tables first.
    let mut names = a.tables.clone();
    names.sort_by_key(|&t| std::cmp::Reverse(ds.table(t).approx_rows()));
    let tables: Vec<SsbTable> = names
        .iter()
        .map(|&t| {
            let t = ds.table(t).batch_size(a.batch);
            match a.row_group {
                Some(rows) => t.chunk_rows(rows),
                None => t,
            }
        })
        .collect();
    let paths: Vec<PathBuf> = names
        .iter()
        .map(|t| {
            let ext = if a.format == Format::Parquet {
                "parquet"
            } else {
                "tbl"
            };
            a.out.join(format!("{t}.{ext}"))
        })
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
    let (rows, bytes): (Vec<u64>, Vec<u64>) = match a.format {
        Format::Parquet => {
            let props = parquet_props()
                .set_compression(a.compression)
                .set_dictionary_enabled(a.dictionary);
            let props = if a.dictionary {
                props
            } else {
                no_dictionary(props)
            };
            let props = props.build();
            let w = write_parquet_many(&tables, create(), a.threads, Some(props))
                .unwrap_or_else(|e| fail(&"parquet", &e));
            drop(w);
            paths.iter().map(|p| (parquet_rows(p), size(p))).unzip()
        }
        Format::Tbl => {
            let w: Vec<_> = create().into_iter().map(|f| Lines(f, 0)).collect();
            let w = write_tbl_many(&tables, w, a.threads).unwrap_or_else(|e| fail(&"tbl", &e));
            w.iter().zip(&paths).map(|(l, p)| (l.1, size(p))).unzip()
        }
        Format::None => {
            let (mut rows, mut bytes) = (vec![0; tables.len()], vec![0; tables.len()]);
            for_each_batch_many(&tables, a.threads, |t, b| {
                rows[t] += b.num_rows() as u64;
                bytes[t] += batch_bytes(&b);
            });
            (rows, bytes)
        }
    };
    let secs = start.elapsed().as_secs_f64();

    println!("| table | rows | MiB |");
    println!("|---|---:|---:|");
    let mib = |b: u64| b as f64 / (1024.0 * 1024.0);
    let mut order: Vec<usize> = (0..names.len()).collect();
    order.sort_by_key(|&i| names[i]);
    for i in order {
        println!("| {} | {} | {:.0} |", names[i], rows[i], mib(bytes[i]));
    }
    let (r, b): (u64, u64) = (rows.iter().sum(), bytes.iter().sum());
    println!("| **total** | {r} | {:.0} |", mib(b));
    println!(
        "\n{secs:.2} s: {:.1} M rows/s, {:.0} MiB/s",
        r as f64 / secs / 1e6,
        mib(b) / secs
    );
}

/// Turn dictionary encoding off for every column, overriding the per-column
/// choices of [`parquet_props`].
fn no_dictionary(mut p: WriterPropertiesBuilder) -> WriterPropertiesBuilder {
    for t in TableName::ALL.into_iter().chain([TableName::LineOrderFlat]) {
        for f in t.schema().fields() {
            p = p.set_column_dictionary_enabled(ColumnPath::from(f.name().as_str()), false);
        }
    }
    p
}

/// Arrow bytes of `b`, without the buffers string columns share: views,
/// and a column's own bytes when it has them (its first buffer, next to
/// the dictionary). `lineorder_flat`'s gathered dimension columns count
/// their views only.
fn batch_bytes(b: &RecordBatch) -> u64 {
    b.columns()
        .iter()
        .map(|c| match c.as_string_view_opt() {
            Some(s) => {
                let own = match &s.data_buffers()[..] {
                    [data, _dict] => data.len(),
                    _ => 0,
                };
                s.len() * 16 + own
            }
            None => c.get_array_memory_size(),
        } as u64)
        .sum()
}

/// Rows in a Parquet file, from its footer.
fn parquet_rows(path: &Path) -> u64 {
    File::open(path)
        .ok()
        .and_then(|f| SerializedFileReader::new(f).ok())
        .map_or(0, |r| r.metadata().file_metadata().num_rows() as u64)
}

/// `\n` bytes in `buf`. Summed in `u8` lanes, 255 bytes at a time, so it
/// vectorizes (~10x `filter().count()`, which went a byte at a time).
fn newlines(buf: &[u8]) -> u64 {
    buf.chunks(255)
        .map(|c| c.iter().fold(0u8, |n, &b| n + u8::from(b == b'\n')) as u64)
        .sum()
}

/// A writer that counts the lines written through it.
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
