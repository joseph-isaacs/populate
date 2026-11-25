//! H2O data set throughput: `cargo bench -p populate-h2o`.
//!
//! - `groupby_plan`: the sequential pass over R's stream (1e6 rows, K = 100).
//! - `groupby_csv`: the CSV of 1e6 rows on 1 and 4 threads, plan prebuilt.
//! - `groupby_arrow`: the same rows as Arrow batches.

use divan::counter::{BytesCount, ItemsCount};
use divan::{Bencher, black_box};
use populate_h2o::GroupBy;
use populate_table::chunks::for_each_batch;
use populate_table::text::{TextChunks, write_text_many};

const N: u64 = 1_000_000;

fn main() {
    divan::main();
}

fn groupby() -> GroupBy {
    GroupBy::with_chunk_rows(N, 100, 0, false, 64 * 1024, 0)
}

#[divan::bench(sample_count = 10)]
fn groupby_plan(b: Bencher) {
    b.counter(ItemsCount::new(N)).bench(groupby);
}

#[divan::bench(sample_count = 10, args = [1, 4])]
fn groupby_csv(b: Bencher, threads: usize) {
    let g = groupby();
    let csv = |t| write_text_many(&[&g as &dyn TextChunks], vec![Vec::new()], t).unwrap();
    let bytes = csv(1)[0].len();
    b.counter(BytesCount::new(bytes))
        .bench(|| black_box(csv(threads)));
}

#[divan::bench(sample_count = 10, args = [1, 4])]
fn groupby_arrow(b: Bencher, threads: usize) {
    let g = groupby();
    b.counter(ItemsCount::new(N))
        .bench(|| for_each_batch(&g, threads, |b| drop(black_box(b))));
}
