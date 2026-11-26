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

#[cfg(feature = "mimalloc")]
use mimalloc::MiMalloc;
use populate_ssb::{Ssb, SsbTable, TableName, parquet_props};
use populate_table::cli::{Args, Tool, no_flags, report, write};
use std::time::Instant;

/// mimalloc: buffers are reused instead of returned to the OS and faulted
/// in again for every batch
#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn main() {
    let tool = Tool::new("ssb-gen", "tbl", populate_ssb::MAX_SCALE_FACTOR);
    let a = Args::parse(tool, || TableName::ALL.to_vec(), no_flags)
        .unwrap_or_else(|e| Args::<TableName>::exit_usage(&e));
    let ds = Ssb::new(a.sf);
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
    let paths = a.paths(&names);

    let start = Instant::now();
    for t in &tables {
        t.prepare(a.threads);
    }
    let (rows, bytes) = write(&a, &paths, &tables, parquet_props());
    report(&names, &rows, &bytes, start.elapsed().as_secs_f64(), "");
}
