//! What the benchmark generators' command-line tools share (feature
//! `parquet`): the common flags, output files, row and byte counts, and
//! writing every table on one pool, and the closing report. Each tool keeps
//! its own flags, its table list, its text format and its Parquet
//! properties.
//!
//! ```no_run
//! use populate_table::cli::{Args, Format, Tool, no_flags};
//!
//! let a: Args<String> = Args::parse(Tool::new("my-gen", "tbl", 1000.0), || vec!["t".into()], no_flags)
//!     .unwrap_or_else(|e| Args::<String>::exit_usage(&e));
//! if a.format == Format::Text { /* ... */ }
//! ```

use crate::chunks::for_each_batch_many;
use crate::text::{TextChunks, write_text_many};
use crate::{Chunks, DEFAULT_BATCH_ROWS, MultiChunks, write_parquet, write_parquet_many};
use arrow_array::cast::AsArray;
use arrow_array::{Array, RecordBatch};
use arrow_schema::SchemaRef;
use parquet::basic::Compression;
use parquet::file::metadata::ParquetMetaDataReader;
use parquet::file::properties::{WriterProperties, WriterPropertiesBuilder};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::schema::types::ColumnPath;
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

/// A tool's command line beyond the flags every generator takes.
#[derive(Debug, Clone, Copy)]
pub struct Tool {
    /// The command as the usage line shows it (`tpch-gen`, `ycsb-gen
    /// load`).
    pub name: &'static str,
    /// The default `--out`.
    pub out: &'static str,
    /// `--format`'s value for the text format (`tbl`, `text`).
    pub text: &'static str,
    /// The text files' extension.
    pub ext: &'static str,
    /// The largest `--sf`, or `None` for a tool sized by flags of its own
    /// (it then takes no `--sf`).
    pub max_sf: Option<f64>,
    /// Whether the tool takes `--tables`.
    pub tables: bool,
    /// The default `--batch`.
    pub batch: usize,
    /// The tool's own flags, for the usage line.
    pub flags: &'static str,
    /// Whether the tool takes `--row-group-bytes` (it then sizes its chunks
    /// with [`source_rows_per_group`]).
    pub row_group_bytes: bool,
}

impl Tool {
    /// The tool `name` whose text format is `text`, written to `.{text}`
    /// files, with `--sf` up to `max_sf` and `--tables`. `--out` defaults
    /// to the name's first word without `-gen`.
    pub fn new(name: &'static str, text: &'static str, max_sf: f64) -> Self {
        let first = name.split(' ').next().unwrap_or(name);
        Tool {
            name,
            out: first.trim_end_matches("-gen"),
            text,
            ext: text,
            max_sf: Some(max_sf),
            tables: true,
            batch: DEFAULT_BATCH_ROWS,
            flags: "",
            row_group_bytes: false,
        }
    }
}

/// A tool's own flags: called with each flag the common ones don't
/// cover and a function that takes its value; returns whether it knew
/// the flag.
pub trait OwnFlags:
    FnMut(&str, &mut dyn FnMut() -> Result<String, String>) -> Result<bool, String>
{
}

impl<F> OwnFlags for F where
    F: FnMut(&str, &mut dyn FnMut() -> Result<String, String>) -> Result<bool, String>
{
}

/// For a tool with no flags of its own.
pub fn no_flags(_: &str, _: &mut dyn FnMut() -> Result<String, String>) -> Result<bool, String> {
    Ok(false)
}

/// The flags every generator takes: `--sf`, `--out`, `--format
/// parquet|<text>|none`, `--tables`, `--threads`, `--batch`,
/// `--row-group` (or `--row-group-bytes`, where the tool takes it),
/// `--compression snappy|none` and `--no-dictionary`.
#[derive(Debug, Clone)]
pub struct Args<T> {
    pub sf: f64,
    pub out: PathBuf,
    pub format: Format,
    /// The tables asked for, each once, in the order given.
    pub tables: Vec<T>,
    pub threads: usize,
    pub batch: usize,
    pub row_group: Option<u64>,
    /// About this many uncompressed Parquet bytes per row group, as
    /// `tpchgen-cli`'s `--row-group-bytes` sizes them.
    pub row_group_bytes: Option<u64>,
    pub compression: Compression,
    pub dictionary: bool,
    /// The text files' extension.
    pub ext: &'static str,
    usage: String,
}

/// `v` parsed, or an error naming `flag`.
pub fn num<N: FromStr<Err: Display>>(flag: &str, v: String) -> Result<N, String> {
    v.parse().map_err(|e| format!("{flag}: {e}"))
}

impl<T: FromStr<Err: Display> + PartialEq> Args<T> {
    /// Parse the process's arguments for `tool`; `own` takes the tool's
    /// own flags. `--tables` defaults to `all_tables()`.
    pub fn parse(
        tool: Tool,
        all_tables: impl FnOnce() -> Vec<T>,
        own: impl OwnFlags,
    ) -> Result<Self, String> {
        Self::parse_from(tool, all_tables, own, std::env::args().skip(1))
    }

    /// [`parse`](Self::parse) of `args` (without the program name).
    pub fn parse_from(
        tool: Tool,
        all_tables: impl FnOnce() -> Vec<T>,
        own: impl OwnFlags,
        args: impl IntoIterator<Item = String>,
    ) -> Result<Self, String> {
        let name = tool.name;
        let text = tool.text;
        let sf = if tool.max_sf.is_some() {
            " [--sf 1]"
        } else {
            ""
        };
        let flags = if tool.flags.is_empty() {
            String::new()
        } else {
            format!(" {}", tool.flags)
        };
        let row_group = if tool.row_group_bytes {
            "[--row-group ROWS | --row-group-bytes BYTES]"
        } else {
            "[--row-group ROWS]"
        };
        let tables = if tool.tables {
            " [--tables a,b,...]"
        } else {
            ""
        };
        let mut a = Args {
            sf: 1.0,
            out: PathBuf::from(tool.out),
            format: Format::Parquet,
            tables: all_tables(),
            threads: 0,
            batch: tool.batch,
            row_group: None,
            row_group_bytes: None,
            compression: Compression::SNAPPY,
            dictionary: true,
            ext: tool.ext,
            usage: format!(
                "usage: {name}{flags}{sf} [--out DIR] [--format parquet|{text}|none]{tables} \
[--threads N] [--batch ROWS] {row_group} [--compression snappy|none] [--no-dictionary]"
            ),
        };
        let r = a.read(&tool, own, args);
        r.map_err(|e| format!("{e}\n{}", a.usage))?;
        Ok(a)
    }

    fn read(
        &mut self,
        tool: &Tool,
        mut own: impl OwnFlags,
        args: impl IntoIterator<Item = String>,
    ) -> Result<(), String> {
        let a = self;
        let mut it = args.into_iter();
        while let Some(flag) = it.next() {
            let mut val = || it.next().ok_or_else(|| format!("{flag} needs a value"));
            match flag.as_str() {
                "--sf" if tool.max_sf.is_some() => a.sf = num(&flag, val()?)?,
                "--out" => a.out = val()?.into(),
                "--format" => {
                    a.format = match val()?.as_str() {
                        "parquet" => Format::Parquet,
                        "none" => Format::None,
                        f if f == tool.text => Format::Text,
                        f => return Err(format!("unknown format `{f}`")),
                    }
                }
                "--tables" if tool.tables => {
                    a.tables.clear();
                    for t in val()?.split(',') {
                        let t = t.parse().map_err(|e| format!("--tables: {e}"))?;
                        if !a.tables.contains(&t) {
                            a.tables.push(t);
                        }
                    }
                }
                "--threads" => a.threads = num(&flag, val()?)?,
                "--batch" => a.batch = num(&flag, val()?)?,
                "--row-group" => a.row_group = Some(num(&flag, val()?)?),
                "--row-group-bytes" if tool.row_group_bytes => {
                    a.row_group_bytes = Some(num(&flag, val()?)?)
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
                    println!("{}", a.usage);
                    exit(0);
                }
                f => {
                    if !own(f, &mut val)? {
                        return Err(format!("unknown flag `{f}`"));
                    }
                }
            }
        }
        if let Some(max) = tool.max_sf
            && !(a.sf > 0.0 && a.sf <= max)
        {
            return Err(format!("--sf must be in (0, {max}]"));
        }
        if a.batch == 0 || a.row_group == Some(0) || a.row_group_bytes == Some(0) {
            return Err("--batch, --row-group and --row-group-bytes must be > 0".into());
        }
        if a.row_group.is_some() && a.row_group_bytes.is_some() {
            return Err("--row-group and --row-group-bytes are exclusive".into());
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

    /// The tool's Parquet properties `props` with `--compression` and
    /// `--no-dictionary` on top; `schemas` are the tables', whose columns
    /// lose any dictionary `props` set for them under `--no-dictionary`.
    pub fn parquet_props(
        &self,
        props: WriterPropertiesBuilder,
        schemas: impl IntoIterator<Item = SchemaRef>,
    ) -> WriterProperties {
        let mut props = props.set_compression(self.compression);
        if !self.dictionary {
            props = props.set_dictionary_enabled(false);
            for s in schemas {
                for f in s.fields() {
                    props = props
                        .set_column_dictionary_enabled(ColumnPath::from(f.name().as_str()), false);
                }
            }
        }
        props.build()
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
            _ => self.ext,
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

/// Source rows per chunk for row groups of about `bytes` uncompressed
/// Parquet bytes (the size a row group's footer records), measured by
/// writing `sample`, the table's first `sample_source_rows` source rows as
/// one chunk, with `props`.
pub fn source_rows_per_group(
    sample: &impl Chunks,
    sample_source_rows: u64,
    props: WriterProperties,
    bytes: u64,
) -> u64 {
    let buf = write_parquet(sample, Vec::new(), 1, Some(props))
        .unwrap_or_else(|e| fail(&"parquet sample", &e));
    // footer: metadata, its 4-byte length, `PAR1`
    let end = buf.len() - 8;
    let len = u32::from_le_bytes(buf[end..end + 4].try_into().expect("4 bytes")) as usize;
    let meta = ParquetMetaDataReader::decode_metadata(&buf[end - len..end])
        .unwrap_or_else(|e| fail(&"parquet sample", &e));
    let sample_bytes: i64 = meta.row_groups().iter().map(|g| g.total_byte_size()).sum();
    let per_row = sample_bytes.max(1) as f64 / sample_source_rows.max(1) as f64;
    ((bytes as f64 / per_row).round() as u64).max(1)
}

/// Write `tables` as `a.format` says, `tables[i]` to `paths[i]` (from
/// [`Args::paths`]), every chunk of every table a job on one pool of
/// `a.threads` threads. `props` are the tool's Parquet properties;
/// `--compression` and `--no-dictionary` go on top. Returns each table's
/// rows and bytes: of its file, or of its Arrow batches for `none`.
pub fn write<T, S: Chunks + TextChunks>(
    a: &Args<T>,
    paths: &[PathBuf],
    tables: &[S],
    props: WriterPropertiesBuilder,
) -> (Vec<u64>, Vec<u64>) {
    assert_eq!(tables.len(), paths.len(), "one path per table");
    match a.format {
        Format::Parquet => {
            let props = a.parquet_props(props, tables.iter().map(Chunks::schema));
            let srcs: Vec<&dyn MultiChunks> =
                tables.iter().map(|t| t as &dyn MultiChunks).collect();
            write_parquet_many(&srcs, create(paths), a.threads, Some(props))
                .unwrap_or_else(|e| fail(&"parquet", &e));
            paths
                .iter()
                .map(|p| (parquet_rows(p), file_size(p)))
                .unzip()
        }
        Format::Text => {
            let w: Vec<_> = create(paths).into_iter().map(Lines::new).collect();
            let srcs: Vec<&dyn TextChunks> = tables.iter().map(|t| t as &dyn TextChunks).collect();
            let w = write_text_many(&srcs, w, a.threads).unwrap_or_else(|e| fail(&a.ext, &e));
            w.iter()
                .zip(paths)
                .map(|(l, p)| (l.lines(), file_size(p)))
                .unzip()
        }
        Format::None => {
            let (mut rows, mut bytes) = (vec![0; tables.len()], vec![0; tables.len()]);
            let srcs: Vec<&dyn MultiChunks> =
                tables.iter().map(|t| t as &dyn MultiChunks).collect();
            for_each_batch_many(&srcs, a.threads, |t, b| {
                rows[t] += b.num_rows() as u64;
                bytes[t] += batch_bytes(&b);
            });
            (rows, bytes)
        }
    }
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
            Tool::new("x-gen", "dat", 100.0),
            || vec!["a".into(), "b".into()],
            no_flags,
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
        assert_eq!(args(&["--tables", "b,a,b"]).unwrap().tables, ["b", "a"]);
        // only for a tool that sizes its chunks by bytes
        assert!(args(&["--row-group-bytes", "100"]).is_err());
    }

    #[test]
    fn row_group_bytes() {
        let tool = Tool {
            row_group_bytes: true,
            ..Tool::new("x-gen", "dat", 100.0)
        };
        let parse = |v: &[&str]| {
            Args::<String>::parse_from(tool, Vec::new, no_flags, v.iter().map(|s| s.to_string()))
        };
        assert_eq!(
            parse(&["--row-group-bytes", "7340032"])
                .unwrap()
                .row_group_bytes,
            Some(7340032)
        );
        assert!(parse(&["--row-group-bytes", "0"]).is_err());
        assert!(parse(&["--row-group", "5", "--row-group-bytes", "5"]).is_err());
    }

    #[test]
    fn own_flags() {
        let tool = Tool {
            max_sf: None,
            tables: false,
            ext: "csv",
            ..Tool::new("y-gen run", "text", 1.0)
        };
        let mut n = 0u32;
        let a = Args::<String>::parse_from(
            tool,
            Vec::new,
            |f: &str, v: &mut dyn FnMut() -> Result<String, String>| {
                Ok(f == "--n" && {
                    n = num(f, v()?)?;
                    true
                })
            },
            ["--n", "3", "--format", "none"].map(String::from),
        )
        .unwrap();
        assert_eq!((n, a.format, a.out.to_str()), (3, Format::None, Some("y")));
        assert_eq!(a.paths(&["t"])[0].extension().unwrap(), "csv");
        let none = |v: &[&str]| {
            Args::<String>::parse_from(tool, Vec::new, no_flags, v.iter().map(|s| s.to_string()))
        };
        assert!(none(&["--sf", "1"]).is_err());
        assert!(none(&["--tables", "a"]).is_err());
    }

    #[test]
    fn counts_lines() {
        let mut l = Lines::new(Vec::new());
        l.write_all(&b"a|\nb|\n".repeat(300)).unwrap();
        assert_eq!(l.lines(), 600);
    }
}
