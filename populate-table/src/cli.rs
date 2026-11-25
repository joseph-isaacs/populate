//! What the benchmark generators' command-line tools share (feature
//! `parquet`): the common flags, output files, row and byte counts, and
//! the closing report. Each tool keeps its table list, its text format's
//! name and its Parquet properties.
//!
//! ```no_run
//! use populate_table::cli::{Args, Format};
//!
//! let a: Args<String> = Args::parse("my-gen", "tbl", 1000.0, || vec!["t".into()], 8192)
//!     .unwrap_or_else(|e| Args::<String>::exit_usage(&e));
//! if a.format == Format::Text { /* ... */ }
//! ```

use arrow_array::cast::AsArray;
use arrow_array::{Array, RecordBatch};
use parquet::basic::Compression;
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::fmt::Display;
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process::exit;
use std::str::FromStr;

/// What to write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    Parquet,
    /// The reference tool's text format (`.tbl`, `.dat`).
    Text,
    /// Generate the Arrow batches only.
    None,
}

/// The flags every generator takes: `--sf`, `--out`, `--format
/// parquet|<text>|none`, `--tables`, `--threads`, `--batch`,
/// `--row-group`, `--compression snappy|none` and `--no-dictionary`.
#[derive(Debug, Clone)]
pub struct Args<T> {
    pub sf: f64,
    pub out: PathBuf,
    pub format: Format,
    pub tables: Vec<T>,
    pub threads: usize,
    pub batch: usize,
    pub row_group: Option<u64>,
    pub compression: Compression,
    pub dictionary: bool,
    /// The text format's name and file extension (`tbl`, `dat`).
    pub text: &'static str,
    usage: String,
}

impl<T: FromStr<Err: Display>> Args<T> {
    /// Parse the process's arguments for the tool `name`. `--out`
    /// defaults to the tool's name without `-gen`; `--tables` to
    /// `all_tables()`.
    pub fn parse(
        name: &str,
        text: &'static str,
        max_sf: f64,
        all_tables: impl FnOnce() -> Vec<T>,
        batch: usize,
    ) -> Result<Self, String> {
        Self::parse_from(
            name,
            text,
            max_sf,
            all_tables,
            batch,
            std::env::args().skip(1),
        )
    }

    /// [`parse`](Self::parse) of `args` (without the program name).
    pub fn parse_from(
        name: &str,
        text: &'static str,
        max_sf: f64,
        all_tables: impl FnOnce() -> Vec<T>,
        batch: usize,
        args: impl IntoIterator<Item = String>,
    ) -> Result<Self, String> {
        let mut a = Args {
            sf: 1.0,
            out: PathBuf::from(name.trim_end_matches("-gen")),
            format: Format::Parquet,
            tables: all_tables(),
            threads: 0,
            batch,
            row_group: None,
            compression: Compression::SNAPPY,
            dictionary: true,
            text,
            usage: format!(
                "usage: {name} [--sf 1] [--out DIR] [--format parquet|{text}|none] \
[--tables a,b,...] [--threads N] [--batch ROWS] [--row-group ROWS] \
[--compression snappy|none] [--no-dictionary]"
            ),
        };
        let r = a.read(max_sf, args);
        r.map_err(|e| format!("{e}\n{}", a.usage))?;
        Ok(a)
    }

    fn read(&mut self, max_sf: f64, args: impl IntoIterator<Item = String>) -> Result<(), String> {
        let a = self;
        let text = a.text;
        let mut it = args.into_iter();
        while let Some(flag) = it.next() {
            let mut val = || it.next().ok_or_else(|| format!("{flag} needs a value"));
            let num = |v: String| v.parse::<u64>().map_err(|e| format!("{flag}: {e}"));
            match flag.as_str() {
                "--sf" => a.sf = val()?.parse().map_err(|e| format!("--sf: {e}"))?,
                "--out" => a.out = val()?.into(),
                "--format" => {
                    a.format = match val()?.as_str() {
                        "parquet" => Format::Parquet,
                        "none" => Format::None,
                        f if f == text => Format::Text,
                        f => return Err(format!("unknown format `{f}`")),
                    }
                }
                "--tables" => {
                    a.tables = val()?
                        .split(',')
                        .map(|t| t.parse().map_err(|e| format!("--tables: {e}")))
                        .collect::<Result<_, _>>()?;
                }
                "--threads" => a.threads = num(val()?)? as usize,
                "--batch" => a.batch = num(val()?)? as usize,
                "--row-group" => a.row_group = Some(num(val()?)?),
                "--compression" => {
                    a.compression = match val()?.as_str() {
                        "snappy" => Compression::SNAPPY,
                        "none" => Compression::UNCOMPRESSED,
                        c => return Err(format!("unknown compression `{c}`")),
                    }
                }
                "--no-dictionary" => a.dictionary = false,
                "-h" | "--help" => {
                    println!("{}", a.usage);
                    exit(0);
                }
                f => return Err(format!("unknown flag `{f}`")),
            }
        }
        if !(a.sf > 0.0 && a.sf <= max_sf) {
            return Err(format!("--sf must be in (0, {max_sf}]"));
        }
        if a.batch == 0 || a.row_group == Some(0) {
            return Err("--batch and --row-group must be > 0".into());
        }
        Ok(())
    }
}

impl<T> Args<T> {
    /// Print `err` (from [`parse`](Self::parse), with the usage) and exit
    /// with status 2.
    pub fn exit_usage(err: &str) -> ! {
        eprintln!("{err}");
        exit(2)
    }

    /// The usage line.
    pub fn usage(&self) -> &str {
        &self.usage
    }

    /// The output file of each table, `<out>/<table>.<ext>`, and the
    /// output directory created (unless the format is `none`).
    pub fn paths(&self, names: &[impl Display]) -> Vec<PathBuf> {
        if self.format != Format::None {
            std::fs::create_dir_all(&self.out).unwrap_or_else(|e| fail(&self.out.display(), &e));
        }
        let ext = match self.format {
            Format::Parquet => "parquet",
            _ => self.text,
        };
        names
            .iter()
            .map(|t| self.out.join(format!("{t}.{ext}")))
            .collect()
    }
}

/// Print `what: err` and exit with status 1.
pub fn fail(what: &dyn Display, err: &dyn Display) -> ! {
    eprintln!("{what}: {err}");
    exit(1)
}

/// Create every file, buffered for large writes.
pub fn create(paths: &[PathBuf]) -> Vec<BufWriter<File>> {
    paths
        .iter()
        .map(|p| {
            let f = File::create(p).unwrap_or_else(|e| fail(&p.display(), &e));
            BufWriter::with_capacity(1 << 20, f)
        })
        .collect()
}

/// Size of the file at `p` (0 if it cannot be read).
pub fn file_size(p: &Path) -> u64 {
    std::fs::metadata(p).map_or(0, |m| m.len())
}

/// Rows in a Parquet file, from its footer.
pub fn parquet_rows(path: &Path) -> u64 {
    File::open(path)
        .ok()
        .and_then(|f| SerializedFileReader::new(f).ok())
        .map_or(0, |r| r.metadata().file_metadata().num_rows() as u64)
}

/// Arrow bytes of `b`, without the buffers string views share between
/// batches: for a `Utf8View` column, its views and its own (first) data
/// buffer.
pub fn batch_bytes(b: &RecordBatch) -> u64 {
    b.columns()
        .iter()
        .map(|c| match c.as_string_view_opt() {
            Some(s) => s.len() * 16 + s.data_buffers().first().map_or(0, |d| d.len()),
            None => c.get_array_memory_size(),
        } as u64)
        .sum()
}

/// `\n` bytes in `buf`. Summed in `u8` lanes, 255 bytes at a time, so it
/// vectorizes (about 10x `filter().count()`, which goes a byte at a time).
fn newlines(buf: &[u8]) -> u64 {
    buf.chunks(255)
        .map(|c| c.iter().fold(0u8, |n, &b| n + u8::from(b == b'\n')) as u64)
        .sum()
}

/// A writer that counts the lines written through it (the rows of a text
/// file).
#[derive(Debug)]
pub struct Lines<W>(W, u64);

impl<W> Lines<W> {
    pub fn new(w: W) -> Self {
        Lines(w, 0)
    }

    /// Lines written so far.
    pub fn lines(&self) -> u64 {
        self.1
    }
}

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

/// Print a Markdown table of each table's rows and MiB (sorted by name),
/// a total, and the run's time and rates; `note` follows the time (such
/// as `"0.10 s text pool"`).
pub fn report(names: &[impl Display + Ord], rows: &[u64], bytes: &[u64], secs: f64, note: &str) {
    println!("| table | rows | MiB |");
    println!("|---|---:|---:|");
    let mib = |b: u64| b as f64 / (1024.0 * 1024.0);
    let mut order: Vec<usize> = (0..names.len()).collect();
    order.sort_by(|&a, &b| names[a].cmp(&names[b]));
    for i in order {
        println!("| {} | {} | {:.0} |", names[i], rows[i], mib(bytes[i]));
    }
    let (r, b): (u64, u64) = (rows.iter().sum(), bytes.iter().sum());
    println!("| **total** | {r} | {:.0} |", mib(b));
    let note = if note.is_empty() {
        String::new()
    } else {
        format!(" ({note})")
    };
    println!(
        "\n{secs:.2} s{note}: {:.1} M rows/s, {:.0} MiB/s",
        r as f64 / secs / 1e6,
        mib(b) / secs
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(v: &[&str]) -> Result<Args<String>, String> {
        Args::parse_from(
            "x-gen",
            "dat",
            100.0,
            || vec!["a".into(), "b".into()],
            8192,
            v.iter().map(|s| s.to_string()),
        )
    }

    #[test]
    fn flags() {
        let a = args(&[]).unwrap();
        assert_eq!(
            (a.out.to_str(), a.format, a.tables.len()),
            (Some("x"), Format::Parquet, 2)
        );
        let a = args(&[
            "--sf",
            "10",
            "--format",
            "dat",
            "--tables",
            "b",
            "--row-group",
            "5",
        ])
        .unwrap();
        assert_eq!(
            (a.sf, a.format, a.tables, a.row_group),
            (10.0, Format::Text, vec!["b".into()], Some(5))
        );
        assert!(args(&["--format", "tbl"]).is_err());
        assert!(args(&["--sf", "1000"]).is_err());
        assert!(args(&["--batch", "0"]).is_err());
        assert!(args(&["--bogus"]).is_err());
    }

    #[test]
    fn counts_lines() {
        let mut l = Lines::new(Vec::new());
        l.write_all(&b"a|\nb|\n".repeat(300)).unwrap();
        assert_eq!(l.lines(), 600);
    }
}
