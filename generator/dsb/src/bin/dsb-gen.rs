//! Generate DSB tables, byte-identical to DSB's dsdgen.
//!
//! ```text
//! dsb-gen [--sf 1] [--out DIR] [--format parquet|dat|none] [--tables a,b,...]
//!           [--threads N] [--batch ROWS] [--row-group ROWS]
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
//! `--row-group` rows (default 256Ki); the data is the same for any value.

#[cfg(feature = "mimalloc")]
use mimalloc::MiMalloc;
use populate_gen_dsb::{
    DsTable, Dsb, Encoding, TableName, for_each_batch_many, parquet_props, write_dat_many,
    write_parquet_many,
};
use populate_table::cli::{
    self, Args, Counts, Format, Tool, create, fail, parquet_counts, report, text_counts,
    text_writers,
};
use std::time::Instant;

/// mimalloc: the system allocator returns buffers to the OS, and they
/// are faulted in again
#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn main() {
    let mut encoding = Encoding::Utf8;
    let tool = Tool {
        batch: populate_gen_dsb::DEFAULT_BATCH_SIZE,
        flags: "[--encoding latin1|utf8]",
        ..Tool::new("dsb-gen", "dat", f64::INFINITY)
    };
    let own = |f: &str, val: &mut dyn FnMut() -> Result<String, String>| {
        if f != "--encoding" {
            return Ok(false);
        }
        encoding = match val()?.as_str() {
            "latin1" => Encoding::Latin1,
            "utf8" => Encoding::Utf8,
            e => return Err(format!("unknown encoding `{e}`")),
        };
        Ok(true)
    };
    let a = Args::parse(tool, || TableName::ALL.to_vec(), own)
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
    let paths = a.paths(&names);

    let start = Instant::now();
    // a sales table and its returns are made together, so the tables are
    // written by this crate's drivers rather than `cli::write`
    let (rows, bytes) = match a.format {
        Format::Parquet => {
            let props = cli::parquet_props(&a, parquet_props(), tables.iter().map(DsTable::schema));
            write_parquet_many(&tables, create(&paths), a.threads, Some(props))
                .unwrap_or_else(|e| fail(&"parquet", &e));
            parquet_counts(&paths)
        }
        Format::Text => {
            let w = write_dat_many(&tables, text_writers(&paths), a.threads, encoding)
                .unwrap_or_else(|e| fail(&"dat", &e));
            text_counts(&w, &paths)
        }
        Format::None => {
            let mut c = Counts::new(tables.len());
            for_each_batch_many(&tables, a.threads, |t, b| c.add(t, &b));
            c.into_parts()
        }
    };
    report(&names, &rows, &bytes, start.elapsed().as_secs_f64(), "");
}
