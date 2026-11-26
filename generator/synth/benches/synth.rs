//! Synthetic shapes: generation and Parquet throughput.
//!
//! `cargo bench -p populate-gen-synth`. Each sample generates 256Ki rows (64Ki
//! of `embeddings`, at 384 dimensions) in chunks of 32Ki rows. Throughput
//! is in rows/s.
//!
//! - `arrow`: one shape, one thread, batches dropped as they come.
//! - `threads`: `taxi` on 1, 2 and 4 threads.
//! - `parquet`: one shape to an in-memory Parquet file (snappy), 1 thread.

use divan::counter::ItemsCount;
use divan::{Bencher, black_box};
use populate_core::{Columns, Distribution};
use populate_gen_synth::{Embeddings, Features, Hits, Shape, ShapeName, Synth, Taxi};
use populate_sink_arrow::IntoArrowColumns;

fn main() {
    divan::main();
}

const ROWS: u64 = 256 * 1024;

fn with<R>(name: ShapeName, f: impl FnOnce(&(dyn Run + Sync)) -> R) -> R {
    match name {
        ShapeName::Taxi => f(&Taxi::new(1).table(ROWS)),
        ShapeName::Hits => f(&Hits::new(1).table(ROWS)),
        ShapeName::Features => f(&Features::new(1).table(ROWS)),
        ShapeName::Embeddings => f(&Embeddings::new(1).table(ROWS / 4)),
    }
}

/// Object-safe view of a `Synth<S>` for the benches.
trait Run {
    fn rows(&self) -> u64;
    fn arrow(&self, threads: usize);
    fn parquet(&self, threads: usize) -> usize;
}

impl<D> Run for Synth<D>
where
    D: Distribution + Clone + Sync,
    Columns<D::Item>: IntoArrowColumns,
{
    fn rows(&self) -> u64 {
        Synth::rows(self)
    }

    fn arrow(&self, threads: usize) {
        self.clone()
            .chunk_rows(32 * 1024)
            .for_each_batch(threads, |b| drop(black_box(b)));
    }

    fn parquet(&self, threads: usize) -> usize {
        self.clone()
            .chunk_rows(32 * 1024)
            .write_parquet(Vec::new(), threads, None)
            .expect("parquet")
            .len()
    }
}

const SHAPES: [ShapeName; 4] = ShapeName::ALL;

#[divan::bench(sample_count = 10, args = SHAPES)]
fn arrow(b: Bencher, s: ShapeName) {
    with(s, |t| {
        b.counter(ItemsCount::new(t.rows())).bench(|| t.arrow(1));
    });
}

#[divan::bench(sample_count = 10, args = [1, 2, 4])]
fn threads(b: Bencher, threads: usize) {
    with(ShapeName::Taxi, |t| {
        b.counter(ItemsCount::new(t.rows()))
            .bench(|| t.arrow(threads));
    });
}

#[divan::bench(sample_count = 5, args = SHAPES)]
fn parquet(b: Bencher, s: ShapeName) {
    with(s, |t| {
        b.counter(ItemsCount::new(t.rows()))
            .bench(|| black_box(t.parquet(1)));
    });
}
