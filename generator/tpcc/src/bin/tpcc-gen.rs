//! Generate the TPC-C initial database.
//!
//! ```text
//! tpcc-gen [--warehouses 1] [--tables a,b,...] [--seed 0] [--load-time MICROS]
//!          [--out DIR] [--format parquet|text|none] [--threads N]
//!          [--batch ROWS] [--row-group ROWS] [--compression snappy|none]
//!          [--no-dictionary]
//! ```
//!
//! `--format parquet` (the default) writes `DIR/<table>.parquet`; `--format
//! text` writes `DIR/<table>.csv` (go-tpc's CSV layout); `--format none`
//! only generates. Every chunk of every table is a job on one pool of
//! `--threads` threads (default: all cores); the data is the same for any
//! thread count and `--row-group`.

#[cfg(feature = "mimalloc")]
use mimalloc::MiMalloc;
use populate_gen_tpcc::{Tpcc, TpccTable, TpccTableName, tpcc};
use populate_table::cli::{Args, Tool, num, report, write};
use std::time::Instant;

/// mimalloc: buffers are reused instead of returned to the OS and faulted
/// in again
#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn main() {
    let tool = Tool {
        ext: "csv",
        max_sf: None,
        flags: "[--warehouses 1] [--seed 0] [--load-time MICROS]",
        ..Tool::new("tpcc-gen", "text", 0.0)
    };
    let mut warehouses = 1u32;
    let mut seed = 0u64;
    let mut load_time = None;
    let a = Args::parse(
        tool,
        || TpccTableName::ALL.to_vec(),
        |flag: &str, val: &mut dyn FnMut() -> Result<String, String>| {
            match flag {
                "--warehouses" => warehouses = num(flag, val()?)?,
                "--seed" => seed = num(flag, val()?)?,
                "--load-time" => load_time = Some(num(flag, val()?)?),
                _ => return Ok(false),
            }
            Ok(true)
        },
    )
    .and_then(|a| match warehouses {
        0 => Err(format!("--warehouses must be > 0\n{}", a.usage())),
        _ => Ok(a),
    })
    .unwrap_or_else(|e| Args::<TpccTableName>::exit_usage(&e));
    let mut db = Tpcc::new(warehouses).seed(seed);
    if let Some(t) = load_time {
        db = db.load_time(t);
    }
    // biggest first, so they start first on the shared pool
    let mut names = a.tables.clone();
    names.sort_by_key(|&t| std::cmp::Reverse(db.table(t).approx_rows()));
    let tables: Vec<TpccTable> = names
        .iter()
        .map(|&t| {
            let t = db.table(t).batch_size(a.batch);
            match a.row_group {
                Some(r) => t.chunk_rows(r),
                None => t,
            }
        })
        .collect();
    let paths = a.paths(&names);
    let start = Instant::now();
    let (rows, bytes) = write(&a, &paths, &tables, tpcc::parquet_props());
    report(&names, &rows, &bytes, start.elapsed().as_secs_f64(), "");
}
