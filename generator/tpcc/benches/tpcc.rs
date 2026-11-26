//! TPC-C generation throughput.
//!
//! `cargo bench -p populate-gen-tpcc`. Samples generate a prefix of a table at
//! 1 warehouse (16Ki source rows, ~160Ki lines of `order_line`).
//! Throughput is in output rows/s, and bytes/s for Parquet and CSV.

use divan::counter::{BytesCount, ItemsCount};
use divan::{Bencher, black_box};
use populate_gen_tpcc::TpccTableName::{self, *};
use populate_gen_tpcc::{Tpcc, TpccTable};

fn main() {
    divan::main();
}

fn tpcc(t: TpccTableName) -> TpccTable {
    Tpcc::new(1)
        .table(t)
        .limit_source_rows(16 * 1024)
        .chunk_source_rows(2048)
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
