//! Generate DSB tables, byte-identical to DSB's dsdgen.
//!
//! ```text
//! dsb-gen [--sf 1] [--out DIR] [--format parquet|dat|none] [--tables a,b,...]
//!           [--threads N] [--batch ROWS] [--row-group ROWS | --row-group-bytes BYTES]
//!           [--compression snappy|none] [--no-dictionary] [--encoding latin1|utf8]
//! ```
//!
//! `--format parquet` (the default) writes `DIR/<table>.parquet`, one file per
//! table, generating and encoding row groups on all cores. `--format dat`
//! writes `DIR/<table>.dat`, the same bytes `dsdgen -SCALE <sf>` writes
//! (UTF-8 by default, as DSB's dsdgen writes; `--encoding latin1` maps text to ISO-8859-1).
//! `--format none` generates the Arrow batches and drops them, which
//! measures generation alone. Rows and size per table and the overall time
//! and throughput are printed.
//!
//! Every chunk of every table is a job on one pool of `--threads` threads
//! (default: all cores). Chunks (= Parquet row groups) hold about
//! `--row-group` rows (default 256Ki), or about `--row-group-bytes`
//! uncompressed bytes, as `tpcgen-cli --row-group-bytes` sizes them
//! (measured on a sample of each table; a returns table is chunked as its
//! sales table, so the two are generated together); the data is the same
//! for any value.

#[cfg(feature = "mimalloc")]
use mimalloc::MiMalloc;
use populate_dsb::{
    DsTable, Dsb, Encoding, TableName, for_each_batch_many, parquet_props, write_dat_many,
    write_parquet_many,
};
use populate_table::cli::{
    Args, Format, Lines, Tool, batch_bytes, create, fail, file_size, parquet_rows, report,
    source_rows_per_group,
};
use std::time::Instant;

/// mimalloc: the system allocator returns buffers to the OS, and they
/// are faulted in again
#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

/// Source rows (tickets and orders for the sales tables) written to size
/// `--row-group-bytes`.
const SAMPLE_SOURCE_ROWS: u64 = 4096;

/// dsdgen's largest scale factor.
const MAX_SCALE_FACTOR: f64 = 100_000.0;

fn main() {
    let tool = Tool {
        batch: populate_dsb::DEFAULT_BATCH_SIZE,
        flags: "[--encoding latin1|utf8]",
        row_group_bytes: true,
        ..Tool::new("dsb-gen", "dat", MAX_SCALE_FACTOR)
    };
    let mut encoding = Encoding::Utf8;
    let a = Args::parse(
        tool,
        || TableName::ALL.to_vec(),
        |f: &str, v: &mut dyn FnMut() -> Result<String, String>| {
            Ok(f == "--encoding" && {
                encoding = match v()?.as_str() {
                    "latin1" => Encoding::Latin1,
                    "utf8" => Encoding::Utf8,
                    e => return Err(format!("unknown encoding `{e}`")),
                };
                true
            })
        },
    )
    .unwrap_or_else(|e| Args::<TableName>::exit_usage(&e));
    // DSB's dsdgen reads a whole scale, and crashes below 1.
    if a.sf < 1.0 || a.sf.fract() != 0.0 {
        Args::<TableName>::exit_usage(&format!(
            "--sf must be a whole number, at least 1\n{}",
            a.usage()
        ));
    }
    let ds = Dsb::new(a.sf);
    // Every chunk of every table goes to one pool, biggest tables first.
    let mut names = a.tables.clone();
    names.sort_by_key(|&t| std::cmp::Reverse(ds.table(t).approx_rows()));
    let paths = a.paths(&names);

    let start = Instant::now();
    let group_rows = |t: TableName| {
        let bytes = a.row_group_bytes?;
        let t = ds.table(sales_of(t).unwrap_or(t)).batch_size(a.batch);
        let n = SAMPLE_SOURCE_ROWS.min(t.source_rows());
        let sample = t.clone().limit_source_rows(n).chunk_source_rows(n);
        let props = a.parquet_props(parquet_props(), [t.schema()]);
        Some(source_rows_per_group(&sample, n, props, bytes))
    };
    let tables: Vec<DsTable> = names
        .iter()
        .map(|&t| {
            let d = ds.table(t).batch_size(a.batch);
            match (a.row_group, group_rows(t)) {
                (Some(rows), _) => d.chunk_rows(rows),
                (_, Some(n)) => d.chunk_source_rows(n),
                (None, None) => d,
            }
        })
        .collect();
    // DSB's own writers, not `cli::write`: they generate each sales
    // table with its returns in one pass
    let (rows, bytes): (Vec<u64>, Vec<u64>) = match a.format {
        Format::Parquet => {
            let props = a.parquet_props(parquet_props(), tables.iter().map(DsTable::schema));
            write_parquet_many(&tables, create(&paths), a.threads, Some(props))
                .unwrap_or_else(|e| fail(&"parquet", &e));
            paths
                .iter()
                .map(|p| (parquet_rows(p), file_size(p)))
                .unzip()
        }
        Format::Text => {
            let w: Vec<_> = create(&paths).into_iter().map(Lines::new).collect();
            let w = write_dat_many(&tables, w, a.threads, encoding)
                .unwrap_or_else(|e| fail(&"dat", &e));
            w.iter()
                .zip(&paths)
                .map(|(l, p)| (l.lines(), file_size(p)))
                .unzip()
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
    report(&names, &rows, &bytes, start.elapsed().as_secs_f64(), "");
}

/// The sales table a returns table is generated with.
fn sales_of(t: TableName) -> Option<TableName> {
    match t {
        TableName::CatalogReturns => Some(TableName::CatalogSales),
        TableName::StoreReturns => Some(TableName::StoreSales),
        TableName::WebReturns => Some(TableName::WebSales),
        _ => None,
    }
}
