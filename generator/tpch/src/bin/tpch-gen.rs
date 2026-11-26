//! Generate TPC-H tables, byte-identical to dbgen and tpchgen.
//!
//! ```text
//! tpch-gen [--sf 1] [--out DIR] [--format parquet|tbl|none] [--tables a,b,...]
//!          [--threads N] [--batch ROWS] [--row-group ROWS]
//!          [--compression snappy|none] [--no-dictionary]
//! ```
//!
//! `--format parquet` (the default) writes `DIR/<table>.parquet`, one file per
//! table, generating and encoding row groups on all cores. `--format tbl`
//! writes `DIR/<table>.tbl`, the same bytes `dbgen -s <sf>` and
//! `tpchgen-cli -s <sf>` write. `--format none` generates the Arrow batches
//! and drops them, which measures generation alone. Rows and size per table and the overall time
//! and throughput are printed.
//!
//! Every chunk of every table is a job on one pool of `--threads` threads
//! (default: all cores). Chunks (= Parquet row groups) hold about
//! `--row-group` rows (default 256Ki); the data is the same for any value.

#[cfg(feature = "mimalloc")]
use mimalloc::MiMalloc;
use populate_table::cli::{Args, Tool, no_flags, report, write};
use populate_tpch::{TableName, Tpch, TpchTable, parquet_props};
use std::time::Instant;

/// mimalloc: 10-18% faster runs than the system allocator (buffers are
/// reused instead of returned to the OS and faulted in again)
#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn main() {
    let tool = Tool {
        batch: populate_tpch::DEFAULT_BATCH_SIZE,
        ..Tool::new("tpch-gen", "tbl", populate_tpch::MAX_SCALE_FACTOR)
    };
    let a = Args::parse(tool, || TableName::ALL.to_vec(), no_flags)
        .unwrap_or_else(|e| Args::<TableName>::exit_usage(&e));
    let ds = Tpch::new(a.sf);
    // Every chunk of every table goes to one pool, biggest tables first.
    let mut names = a.tables.clone();
    names.sort_by_key(|&t| std::cmp::Reverse(ds.table(t).approx_rows()));
    let tables: Vec<TpchTable> = names
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
    Tpch::prepare(a.threads);
    let pool = start.elapsed().as_secs_f64();
    let (rows, bytes) = write(&a, &paths, &tables, parquet_props());
    let secs = start.elapsed().as_secs_f64();
    report(
        &names,
        &rows,
        &bytes,
        secs,
        &format!("{pool:.2} s text pool"),
    );
}
