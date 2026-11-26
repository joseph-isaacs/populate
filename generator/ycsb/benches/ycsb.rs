//! YCSB generation throughput.
//!
//! `cargo bench -p populate-gen-ycsb`. Samples 16Ki records of the default
//! `usertable`, and 256Ki operations of workloads A and E on 100,000
//! records. Throughput is in output rows/s, and bytes/s for Parquet and TSV.

use divan::counter::{BytesCount, ItemsCount};
use divan::{Bencher, black_box};
use populate_gen_ycsb::{Ycsb, YcsbRun};

fn main() {
    divan::main();
}

fn ycsb(data_integrity: bool) -> Ycsb {
    Ycsb::new(16 * 1024)
        .data_integrity(data_integrity)
        .chunk_rows(2048)
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
