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
use parquet::file::properties::WriterPropertiesBuilder;
use parquet::schema::types::ColumnPath;
use populate_table::cli::{
    Args, Format, Lines, batch_bytes, create, fail, file_size, parquet_rows, report,
};
use populate_tpch::{
    TableName, Tpch, TpchTable, for_each_batch_many, parquet_props, write_parquet_many,
    write_tbl_many,
};
use std::time::Instant;

/// mimalloc: 10-18% faster runs than the system allocator (buffers are
/// reused instead of returned to the OS and faulted in again)
#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn main() {
    let a = Args::parse(
        "tpch-gen",
        "tbl",
        populate_tpch::MAX_SCALE_FACTOR,
        || TableName::ALL.to_vec(),
        populate_tpch::DEFAULT_BATCH_SIZE,
    )
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
            let w = write_parquet_many(&tables, create(&paths), a.threads, Some(props.build()))
                .unwrap_or_else(|e| fail(&"parquet", &e));
            drop(w);
            paths
                .iter()
                .map(|p| (parquet_rows(p), file_size(p)))
                .unzip()
        }
        Format::Text => {
            let w: Vec<_> = create(&paths).into_iter().map(Lines::new).collect();
            let w = write_tbl_many(&tables, w, a.threads).unwrap_or_else(|e| fail(&"tbl", &e));
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
    let secs = start.elapsed().as_secs_f64();
    report(
        &names,
        &rows,
        &bytes,
        secs,
        &format!("{pool:.2} s text pool"),
    );
}

/// Turn dictionary encoding off for every column, overriding the per-column
/// choices of [`parquet_props`].
fn no_dictionary(mut p: WriterPropertiesBuilder) -> WriterPropertiesBuilder {
    for t in TableName::ALL {
        for f in t.schema().fields() {
            p = p.set_column_dictionary_enabled(ColumnPath::from(f.name().as_str()), false);
        }
    }
    p
}
