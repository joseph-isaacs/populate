//! Generate TPC-DS tables, byte-identical to dsdgen.
//!
//! ```text
//! tpcds-gen [--sf 1] [--out DIR] [--format parquet|dat|none] [--tables a,b,...]
//!           [--threads N] [--batch ROWS] [--row-group ROWS]
//!           [--compression snappy|none] [--no-dictionary] [--encoding latin1|utf8]
//! ```
//!
//! `--format parquet` (the default) writes `DIR/<table>.parquet`, one file per
//! table, generating and encoding row groups on all cores. `--format dat`
//! writes `DIR/<table>.dat`, the same bytes `dsdgen -SCALE <sf>` writes
//! (`--encoding utf8` for UTF-8 text, as `tpcgen-cli` writes).
//! `--format none` generates the Arrow batches and drops them, which
//! measures generation alone. Rows and size per table and the overall time
//! and throughput are printed.
//!
//! Every chunk of every table is a job on one pool of `--threads` threads
//! (default: all cores). Chunks (= Parquet row groups) hold about
//! `--row-group` rows (default 256Ki); the data is the same for any value.

#[cfg(feature = "mimalloc")]
use mimalloc::MiMalloc;
use parquet::basic::Compression;
use parquet::file::properties::WriterPropertiesBuilder;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::schema::types::ColumnPath;
use populate_gen_tpcds::{
    DsTable, Encoding, TableName, Tpcds, for_each_batch_many, parquet_props, write_dat_many,
    write_parquet_many,
};
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process::exit;
use std::time::Instant;

/// mimalloc: 10-18% faster runs than the system allocator (buffers are
/// reused instead of returned to the OS and faulted in again)
#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[derive(Clone, Copy, PartialEq)]
enum Format {
    Parquet,
    Dat,
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
    encoding: Encoding,
}

const USAGE: &str = "usage: tpcds-gen [--sf 1] [--out DIR] [--format parquet|dat|none] \
[--tables a,b,...] [--threads N] [--batch ROWS] [--row-group ROWS] [--compression snappy|none] [--no-dictionary] [--encoding latin1|utf8]";

fn parse() -> Result<Args, String> {
    let mut a = Args {
        sf: 1.0,
        out: PathBuf::from("tpcds"),
        format: Format::Parquet,
        tables: TableName::ALL.to_vec(),
        threads: 0,
        batch: populate_gen_tpcds::DEFAULT_BATCH_SIZE,
        row_group: None,
        compression: Compression::SNAPPY,
        dictionary: true,
        encoding: Encoding::Latin1,
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
                    "dat" => Format::Dat,
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
            "--encoding" => {
                a.encoding = match val()?.as_str() {
                    "latin1" => Encoding::Latin1,
                    "utf8" => Encoding::Utf8,
                    e => return Err(format!("unknown encoding `{e}`")),
                }
            }
            "-h" | "--help" => {
                println!("{USAGE}");
                exit(0);
            }
            f => return Err(format!("unknown flag `{f}`")),
        }
    }
    if a.sf <= 0.0 || a.batch == 0 || a.row_group == Some(0) {
        return Err("--sf, --batch and --row-group must be > 0".into());
    }
    Ok(a)
}

fn main() {
    let a = parse().unwrap_or_else(|e| {
        eprintln!("{e}\n{USAGE}");
        exit(2);
    });
    let ds = Tpcds::new(a.sf);
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
    let tables: Vec<DsTable> = names
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
                "dat"
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
        Format::Dat => {
            let w: Vec<_> = create().into_iter().map(|f| Lines(f, 0)).collect();
            let w = write_dat_many(&tables, w, a.threads, a.encoding)
                .unwrap_or_else(|e| fail(&"dat", &e));
            w.iter().zip(&paths).map(|(l, p)| (l.1, size(p))).unzip()
        }
        Format::None => {
            let (mut rows, mut bytes) = (vec![0; tables.len()], vec![0; tables.len()]);
            for_each_batch_many(&tables, a.threads, |t, b| {
                rows[t] += b.num_rows() as u64;
                bytes[t] += b.get_array_memory_size() as u64;
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
    for t in TableName::ALL {
        for f in Tpcds::new(1.0).table(t).schema().fields() {
            p = p.set_column_dictionary_enabled(ColumnPath::from(f.name().as_str()), false);
        }
    }
    p
}

/// Rows in a Parquet file, from its footer.
fn parquet_rows(path: &Path) -> u64 {
    File::open(path)
        .ok()
        .and_then(|f| SerializedFileReader::new(f).ok())
        .map_or(0, |r| r.metadata().file_metadata().num_rows() as u64)
}

/// `\n` bytes in `buf`. Summed in `u8` lanes, 255 bytes at a time, so it
/// vectorizes (~10x `filter().count()`, which went a byte at a time and was
/// 13% of a one-thread `.dat` run's instructions).
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
