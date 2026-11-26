//! What makes a run against a reference tool like for like: the reference's
//! Parquet settings read off our own output, and a check that both tools
//! wrote the same thing.

use arrow_array::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::Compression;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::collections::BTreeSet;
use std::fs::{File, read_dir};
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};

/// The Parquet settings a reference needs to write what we wrote.
pub struct Settings {
    /// The tables (file stems), so neither side writes one the other does not.
    pub tables: Vec<String>,
    /// Columns we write without a dictionary. The reference's writer tries
    /// one on every column and falls back on the ones with too many
    /// distinct values, which costs time and changes the file.
    pub plain: Vec<String>,
    /// Mean uncompressed bytes per row group of our largest table (in
    /// uncompressed bytes). A
    /// reference that sizes row groups by bytes gets about our row count
    /// on that table, and so our share of per-row-group work.
    pub row_group_bytes: u64,
    /// The codecs we used (one, for every generator here).
    pub codecs: BTreeSet<String>,
}

fn files(dir: &Path) -> Result<Vec<PathBuf>, String> {
    let mut v: Vec<PathBuf> = read_dir(dir)
        .map_err(|e| format!("{}: {e}", dir.display()))?
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.is_file())
        .collect();
    v.sort();
    Ok(v)
}

fn name(p: &Path) -> String {
    p.file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default()
}

fn is_parquet(p: &Path) -> bool {
    p.extension().is_some_and(|e| e == "parquet")
}

fn metadata(p: &Path) -> Result<ParquetMetaData, String> {
    let f = File::open(p).map_err(|e| format!("{}: {e}", p.display()))?;
    let r = SerializedFileReader::new(f).map_err(|e| format!("{}: {e}", p.display()))?;
    Ok(r.metadata().clone())
}

fn codec(c: Compression) -> String {
    // the reference's `--compression` spelling
    match c {
        Compression::UNCOMPRESSED => "UNCOMPRESSED".into(),
        Compression::SNAPPY => "SNAPPY".into(),
        Compression::LZ4 => "LZ4".into(),
        Compression::LZ4_RAW => "LZ4_RAW".into(),
        Compression::ZSTD(l) => format!("ZSTD({})", l.compression_level()),
        Compression::GZIP(l) => format!("GZIP({})", l.compression_level()),
        Compression::BROTLI(l) => format!("BROTLI({})", l.compression_level()),
        c => format!("{c:?}"),
    }
}

/// Columns without a dictionary page in any row group, and the codecs used.
fn columns(m: &ParquetMetaData) -> (Vec<String>, BTreeSet<String>) {
    let mut codecs = BTreeSet::new();
    let Some(first) = m.row_groups().first() else {
        return (Vec::new(), codecs);
    };
    let plain = (0..first.num_columns())
        .filter(|&i| {
            m.row_groups()
                .iter()
                .all(|rg| rg.column(i).dictionary_page_offset().is_none())
        })
        .map(|i| first.column(i).column_path().string())
        .collect();
    for rg in m.row_groups() {
        for c in rg.columns() {
            codecs.insert(codec(c.compression()));
        }
    }
    (plain, codecs)
}

/// The tables in our output in `dir`, and the settings its Parquet files
/// were written with.
pub fn settings(dir: &Path) -> Result<Settings, String> {
    let mut s = Settings {
        tables: Vec::new(),
        plain: Vec::new(),
        row_group_bytes: 0,
        codecs: BTreeSet::new(),
    };
    let mut largest = 0;
    for p in files(dir)? {
        if let Some(stem) = p.file_stem() {
            s.tables.push(stem.to_string_lossy().into_owned());
        }
        if !is_parquet(&p) {
            continue;
        }
        let m = metadata(&p)?;
        let (plain, codecs) = columns(&m);
        s.plain.extend(plain);
        s.codecs.extend(codecs);
        let bytes: i64 = m.row_groups().iter().map(|r| r.total_byte_size()).sum();
        let rgs = m.num_row_groups() as i64;
        if bytes > largest && rgs > 0 {
            largest = bytes;
            s.row_group_bytes = (bytes / rgs) as u64;
        }
    }
    if s.tables.is_empty() {
        return Err(format!("no output in {}", dir.display()));
    }
    Ok(s)
}

/// Every difference between the files in `ours` and `theirs`: text files
/// byte for byte, Parquet files by schema, values, codecs and dictionary
/// use. Row groups only differ by how many there are, which is reported
/// separately by [`row_groups`].
pub fn compare(ours: &Path, theirs: &Path) -> Result<Vec<String>, String> {
    let (a, b) = (files(ours)?, files(theirs)?);
    let names = |v: &[PathBuf]| v.iter().map(|p| name(p)).collect::<BTreeSet<_>>();
    let (na, nb) = (names(&a), names(&b));
    let mut diffs: Vec<String> = na
        .symmetric_difference(&nb)
        .map(|n| {
            let who = if na.contains(n) {
                "us"
            } else {
                "the reference"
            };
            format!("{n}: written only by {who}")
        })
        .collect();
    for p in a.iter().filter(|p| nb.contains(&name(p))) {
        let q = theirs.join(name(p));
        let d = if is_parquet(p) {
            parquet_diff(p, &q)?
        } else {
            text_diff(p, &q)?
        };
        diffs.extend(d.map(|d| format!("{}: {d}", name(p))));
    }
    Ok(diffs)
}

/// `table ours/theirs` row-group counts where they differ by more than 2x.
pub fn row_groups(ours: &Path, theirs: &Path) -> Result<Vec<String>, String> {
    let mut v = Vec::new();
    for p in files(ours)?.into_iter().filter(|p| is_parquet(p)) {
        let q = theirs.join(name(&p));
        if !q.is_file() {
            continue;
        }
        let (a, b) = (
            metadata(&p)?.num_row_groups(),
            metadata(&q)?.num_row_groups(),
        );
        if a.max(b) > 2 * a.min(b) {
            let stem = p.file_stem().unwrap_or_default().to_string_lossy();
            v.push(format!("{stem} {a}/{b}"));
        }
    }
    Ok(v)
}

fn text_diff(a: &Path, b: &Path) -> Result<Option<String>, String> {
    let open = |p: &Path| {
        File::open(p)
            .map(|f| BufReader::with_capacity(1 << 20, f))
            .map_err(|e| format!("{}: {e}", p.display()))
    };
    let (mut ra, mut rb) = (open(a)?, open(b)?);
    let (mut ba, mut bb) = (vec![0u8; 1 << 20], vec![0u8; 1 << 20]);
    let mut at = 0u64;
    loop {
        let n = fill(&mut ra, &mut ba)?;
        let m = fill(&mut rb, &mut bb)?;
        if let Some(i) = ba[..n.min(m)]
            .iter()
            .zip(&bb[..m.min(n)])
            .position(|(x, y)| x != y)
        {
            return Ok(Some(format!("bytes differ at offset {}", at + i as u64)));
        }
        if n != m {
            return Ok(Some(format!(
                "lengths differ after {} bytes",
                at + n.min(m) as u64
            )));
        }
        if n == 0 {
            return Ok(None);
        }
        at += n as u64;
    }
}

/// Read until `buf` is full or the file ends.
fn fill(r: &mut impl Read, buf: &mut [u8]) -> Result<usize, String> {
    let mut n = 0;
    while n < buf.len() {
        match r.read(&mut buf[n..]).map_err(|e| e.to_string())? {
            0 => break,
            k => n += k,
        }
    }
    Ok(n)
}

fn batches(p: &Path) -> Result<impl Iterator<Item = Result<RecordBatch, String>>, String> {
    let f = File::open(p).map_err(|e| format!("{}: {e}", p.display()))?;
    let r = ParquetRecordBatchReaderBuilder::try_new(f)
        .and_then(|b| b.with_batch_size(64 * 1024).build())
        .map_err(|e| format!("{}: {e}", p.display()))?;
    Ok(r.map(|b| b.map_err(|e| e.to_string())))
}

fn parquet_diff(a: &Path, b: &Path) -> Result<Option<String>, String> {
    let (ma, mb) = (metadata(a)?, metadata(b)?);
    let (ra, rb) = (ma.file_metadata().num_rows(), mb.file_metadata().num_rows());
    if ra != rb {
        return Ok(Some(format!("{ra} rows, the reference {rb}")));
    }
    let (pa, ca) = columns(&ma);
    let (pb, cb) = columns(&mb);
    if ca != cb {
        return Ok(Some(format!("codecs {ca:?}, the reference {cb:?}")));
    }
    if pa != pb {
        return Ok(Some(format!(
            "columns without a dictionary {pa:?}, the reference {pb:?}"
        )));
    }
    let (mut ia, mut ib) = (batches(a)?, batches(b)?);
    let mut row = 0;
    loop {
        match (ia.next().transpose()?, ib.next().transpose()?) {
            (None, None) => return Ok(None),
            (Some(x), Some(y)) => {
                for (i, f) in x.schema().fields().iter().enumerate() {
                    let g = y.schema().field(i).clone();
                    if f.name() != g.name() || f.data_type() != g.data_type() {
                        return Ok(Some(format!(
                            "column {i} is {} {}, the reference's {} {}",
                            f.name(),
                            f.data_type(),
                            g.name(),
                            g.data_type()
                        )));
                    }
                    if x.column(i).as_ref() != y.column(i).as_ref() {
                        return Ok(Some(format!(
                            "{} differs in rows {row}..{}",
                            f.name(),
                            row + x.num_rows()
                        )));
                    }
                }
                row += x.num_rows();
            }
            _ => return Ok(Some("batches end at different rows".into())),
        }
    }
}
