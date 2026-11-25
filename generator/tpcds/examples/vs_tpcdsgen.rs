//! populate-tpcds vs tpcdsgen-arrow (the TPC-DS generator from the
//! tpcgen-rs project), SF 1, Arrow output only (no files).
//!
//! `cargo run -p populate-tpcds --release --example vs_tpcdsgen [-- <threads>]`
//!
//! Both produce the same data (populate-tpcds runs tpcdsgen's generators).
//! Each table is generated in full, batches of 8192 rows dropped as they
//! come. tpcdsgen runs one session chunk per thread (how `tpcgen-cli`
//! splits a table: dsdgen's rule, so tables under 1M source rows are not
//! split); populate-tpcds splits every table into ~256Ki-row chunks on the
//! same thread count. Best of 3 runs.

use populate_tpcds::{TableName, Tpcds};
use std::time::{Duration, Instant};
use tpcdsgen::config::{Session, SessionBuilder};
use tpcdsgen::table::Table as DsTable;
use tpcdsgen_arrow::arrow::array::RecordBatch as RecordBatch60;
use tpcdsgen_arrow::*;

const BATCH: usize = 8192;

fn session(chunk: usize, chunks: usize) -> Session {
    SessionBuilder::new()
        .with_scale_factor(1.0)
        .with_chunk_number(chunk as i32 + 1)
        .with_total_chunks(chunks as i32)
        .build()
        .expect("session")
}

/// Rows and bytes of a stream of batches.
fn drain(it: impl Iterator<Item = Result<RecordBatch60, impl std::fmt::Debug>>) -> (u64, u64) {
    it.map(|b| b.expect("batch")).fold((0, 0), |(r, by), b| {
        (
            r + b.num_rows() as u64,
            by + b.get_array_memory_size() as u64,
        )
    })
}

macro_rules! ds {
    ($ty:ident, $t:expr, $chunk:expr, $chunks:expr) => {{
        let s = session($chunk, $chunks);
        let range = s.get_source_row_range($t);
        drain(
            $ty::new(s)
                .with_source_row_range(*range.start(), *range.end())
                .with_batch_size(BATCH),
        )
    }};
}

fn tpcdsgen_chunk(t: TableName, chunk: usize, chunks: usize) -> (u64, u64) {
    match t {
        TableName::StoreSales => ds!(StoreSalesArrow, DsTable::StoreSales, chunk, chunks),
        TableName::CatalogSales => ds!(CatalogSalesArrow, DsTable::CatalogSales, chunk, chunks),
        TableName::WebSales => ds!(WebSalesArrow, DsTable::WebSales, chunk, chunks),
        TableName::StoreReturns => ds!(StoreReturnsArrow, DsTable::StoreReturns, chunk, chunks),
        TableName::Inventory => ds!(InventoryArrow, DsTable::Inventory, chunk, chunks),
        TableName::Customer => ds!(CustomerArrow, DsTable::Customer, chunk, chunks),
        TableName::CustomerAddress => {
            ds!(
                CustomerAddressArrow,
                DsTable::CustomerAddress,
                chunk,
                chunks
            )
        }
        TableName::CustomerDemographics => ds!(
            CustomerDemographicsArrow,
            DsTable::CustomerDemographics,
            chunk,
            chunks
        ),
        TableName::Item => ds!(ItemArrow, DsTable::Item, chunk, chunks),
        TableName::DateDim => ds!(DateDimArrow, DsTable::DateDim, chunk, chunks),
        t => unimplemented!("{t}"),
    }
}

fn tpcdsgen(t: TableName, threads: usize) -> (u64, u64) {
    std::thread::scope(|s| {
        let hs: Vec<_> = (0..threads)
            .map(|c| s.spawn(move || tpcdsgen_chunk(t, c, threads)))
            .collect();
        hs.into_iter()
            .map(|h| h.join().expect("thread"))
            .fold((0, 0), |a, b| (a.0 + b.0, a.1 + b.1))
    })
}

fn populate(t: TableName, threads: usize) -> (u64, u64) {
    let table = Tpcds::new(1.0).table(t).batch_size(BATCH);
    let (mut rows, mut bytes) = (0u64, 0u64);
    table.for_each_batch(threads, |b| {
        rows += b.num_rows() as u64;
        bytes += b.get_array_memory_size() as u64;
    });
    (rows, bytes)
}

fn best(f: impl Fn() -> (u64, u64)) -> (Duration, u64) {
    let rows = f().0;
    let t = (0..3)
        .map(|_| {
            let t = Instant::now();
            std::hint::black_box(f());
            t.elapsed()
        })
        .min()
        .expect("runs");
    (t, rows)
}

fn main() {
    let threads: usize = std::env::args()
        .nth(1)
        .map_or(1, |a| a.parse().expect("threads"));
    use TableName::*;
    let tables = [
        StoreSales,
        CatalogSales,
        WebSales,
        StoreReturns,
        Inventory,
        Customer,
        CustomerAddress,
        CustomerDemographics,
        Item,
        DateDim,
    ];
    println!("SF 1, Arrow, {threads} thread(s), best of 3\n");
    println!("| table | rows | tpcdsgen-arrow | populate | speedup |");
    println!("|---|---:|---:|---:|---:|");
    let (mut ta, mut tb) = (Duration::ZERO, Duration::ZERO);
    for t in tables {
        let (a, rows) = best(|| tpcdsgen(t, threads));
        let (b, _) = best(|| populate(t, threads));
        ta += a;
        tb += b;
        println!(
            "| {t} | {rows} | {:.0} ms | {:.0} ms | {:.1}x |",
            a.as_secs_f64() * 1e3,
            b.as_secs_f64() * 1e3,
            a.as_secs_f64() / b.as_secs_f64()
        );
    }
    println!(
        "| **total** | | {:.0} ms | {:.0} ms | {:.1}x |",
        ta.as_secs_f64() * 1e3,
        tb.as_secs_f64() * 1e3,
        ta.as_secs_f64() / tb.as_secs_f64()
    );
}
