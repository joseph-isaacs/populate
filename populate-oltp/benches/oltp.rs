//! TPC-C and YCSB generation throughput.
//!
//! `cargo bench -p populate-oltp`. TPC-C samples generate a prefix of a
//! table at 1 warehouse (16Ki source rows, ~160Ki lines of `order_line`);
//! YCSB samples 16Ki records of the default `usertable`, and 256Ki
//! operations of workloads A and E on 100,000 records. Throughput is in
//! output rows/s, and bytes/s for Parquet and text.

use divan::counter::{BytesCount, ItemsCount};
use divan::{Bencher, black_box};
use populate_oltp::TpccTableName::{self, *};
use populate_oltp::{Tpcc, TpccTable, Ycsb, YcsbRun};

fn main() {
    divan::main();
}

fn tpcc(t: TpccTableName) -> TpccTable {
    Tpcc::new(1)
        .table(t)
        .limit_source_rows(16 * 1024)
        .chunk_source_rows(2048)
}

fn ycsb(data_integrity: bool) -> Ycsb {
    Ycsb::new(16 * 1024)
        .data_integrity(data_integrity)
        .chunk_rows(2048)
}

#[divan::bench(sample_count = 10, args = [OrderLine, Stock, Customer, Orders, History, Item])]
fn tpcc_arrow(b: Bencher, t: TpccTableName) {
    let t = tpcc(t);
    let mut rows = 0;
    t.for_each_batch(1, |b| rows += b.num_rows());
    b.counter(ItemsCount::new(rows))
        .bench(|| t.for_each_batch(1, |b| drop(black_box(b))));
}

#[divan::bench(sample_count = 5, args = [1, 4])]
fn tpcc_parquet(b: Bencher, threads: usize) {
    let t = tpcc(Stock);
    let size = t.write_parquet(Vec::new(), 1, None).expect("parquet").len();
    b.counter(BytesCount::new(size))
        .bench(|| t.write_parquet(Vec::new(), threads, None).expect("parquet"));
}

#[divan::bench(sample_count = 5, args = [1, 4])]
fn tpcc_csv(b: Bencher, threads: usize) {
    let t = tpcc(Stock);
    let size = t.write_csv(Vec::new(), 1).expect("csv").len();
    b.counter(BytesCount::new(size))
        .bench(|| t.write_csv(Vec::new(), threads).expect("csv"));
}

#[divan::bench(sample_count = 10, args = [false, true])]
fn ycsb_arrow(b: Bencher, data_integrity: bool) {
    let y = ycsb(data_integrity);
    b.counter(ItemsCount::new(y.rows()))
        .bench(|| y.for_each_batch(1, |b| drop(black_box(b))));
}

#[divan::bench(sample_count = 5, args = [1, 4])]
fn ycsb_parquet(b: Bencher, threads: usize) {
    let y = ycsb(false);
    let size = y.write_parquet(Vec::new(), 1, None).expect("parquet").len();
    b.counter(BytesCount::new(size))
        .bench(|| y.write_parquet(Vec::new(), threads, None).expect("parquet"));
}

#[divan::bench(sample_count = 5, args = [1, 4])]
fn ycsb_tsv(b: Bencher, threads: usize) {
    let y = ycsb(false);
    let size = y.write_tsv(Vec::new(), 1).expect("tsv").len();
    b.counter(BytesCount::new(size))
        .bench(|| y.write_tsv(Vec::new(), threads).expect("tsv"));
}

#[divan::bench(sample_count = 5, args = ['a', 'e'])]
fn ycsb_run_tsv(b: Bencher, workload: char) {
    let r = || YcsbRun::workload(workload, Ycsb::new(100_000), 256 * 1024);
    let size = r().write_tsv(Vec::new(), 1).expect("tsv").len();
    // a fresh run each sample: the chunk start states are found once per run
    b.counter(BytesCount::new(size))
        .with_inputs(r)
        .bench_values(|r| r.write_tsv(Vec::new(), 0).expect("tsv"));
}
