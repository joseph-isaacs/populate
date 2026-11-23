//! TPC-H generation throughput.
//!
//! `cargo bench -p populate-tpch`. Each sample generates a prefix of up to
//! 16Ki source rows of a table at SF 1 (64Ki rows of `lineitem`); the text
//! pool is built once, before the first sample. Throughput is in output
//! rows/s (Arrow sizes would count the shared text pool), and in output
//! bytes/s for Parquet and `.tbl`.
//!
//! - `arrow`: one table, one thread, batches dropped as they come.
//! - `threads`: `lineitem` on 1, 2 and 4 threads.
//! - `parquet` / `tbl`: `lineitem` to an in-memory Parquet file (snappy,
//!   `parquet_props`) or `.tbl` text, 1 and 4 threads.

use divan::counter::{BytesCount, ItemsCount};
use divan::{Bencher, black_box};
use populate_tpch::TableName::{self, *};
use populate_tpch::{Tpch, TpchTable};

fn main() {
    Tpch::prepare(0);
    divan::main();
}

fn table(t: TableName) -> TpchTable {
    Tpch::new(1.0)
        .table(t)
        .limit_source_rows(16 * 1024)
        .chunk_source_rows(1024)
}

fn run(b: Bencher, t: TableName, threads: usize) {
    let t = table(t);
    let mut rows = 0;
    t.for_each_batch(1, |b| rows += b.num_rows());
    b.counter(ItemsCount::new(rows))
        .bench(|| t.for_each_batch(threads, |b| drop(black_box(b))));
}

#[divan::bench(
    sample_count = 10,
    args = [LineItem, Orders, PartSupp, Part, Customer, Supplier]
)]
fn arrow(b: Bencher, t: TableName) {
    run(b, t, 1);
}

#[divan::bench(sample_count = 10, args = [1, 2, 4])]
fn threads(b: Bencher, threads: usize) {
    run(b, LineItem, threads);
}

#[divan::bench(sample_count = 5, args = [1, 4])]
fn parquet(b: Bencher, threads: usize) {
    let t = table(LineItem);
    let size = t.write_parquet(Vec::new(), 1, None).expect("parquet").len();
    b.counter(BytesCount::new(size))
        .bench(|| t.write_parquet(Vec::new(), threads, None).expect("parquet"));
}

#[divan::bench(sample_count = 5, args = [1, 4])]
fn tbl(b: Bencher, threads: usize) {
    let t = table(LineItem);
    let size = t.write_tbl(Vec::new(), 1).expect("tbl").len();
    b.counter(BytesCount::new(size))
        .bench(|| t.write_tbl(Vec::new(), threads).expect("tbl"));
}
