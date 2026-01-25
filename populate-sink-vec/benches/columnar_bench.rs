use arrow_array::RecordBatch;
use divan::{Bencher, black_box};
use populate_builtins::{constant, range};
use populate_core::CollectExt;
use populate_sink_arrow::RecordBatchW;
use populate_sink_vec::{Col, Cols, ColumnarCols};
use rand::rngs::{SmallRng, StdRng};
use rand::{Rng, SeedableRng};

fn main() {
    divan::main();
}

const ROWS: usize = 1_000_000;

// === CONSTANT benchmarks (no RNG overhead) ===

/// Constant 7-tuple row-by-row - pure iteration overhead
#[divan::bench]
fn constant_7tuple_row_by_row(bencher: Bencher) {
    bencher.bench(|| {
        let mut rng = StdRng::seed_from_u64(42);
        let mut dist = (
            constant(42i64),
            constant(123i32),
            constant(3.14f64),
            constant(99i64),
            constant(456i32),
            constant(2.71f64),
            constant(0i64),
        );
        let result: Vec<(i64, i32, f64, i64, i32, f64, i64)> =
            dist.collect::<Col>(&mut rng, black_box(ROWS));
        black_box(result)
    });
}

/// Constant 7-tuple columnar - pure iteration overhead
#[divan::bench]
fn constant_7tuple_columnar(bencher: Bencher) {
    bencher.bench(|| {
        let mut rng = StdRng::seed_from_u64(42);
        let mut dist = (
            constant(42i64),
            constant(123i32),
            constant(3.14f64),
            constant(99i64),
            constant(456i32),
            constant(2.71f64),
            constant(0i64),
        );
        let result: (
            Vec<i64>,
            Vec<i32>,
            Vec<f64>,
            Vec<i64>,
            Vec<i32>,
            Vec<f64>,
            Vec<i64>,
        ) = dist.collect::<ColumnarCols>(&mut rng, black_box(ROWS));
        black_box(result)
    });
}

// === RANGE benchmarks (with RNG) ===

/// Baseline: Collect tuple row-by-row using Col marker
#[divan::bench]
fn collect_row_by_row(bencher: Bencher) {
    bencher.bench(|| {
        let mut rng = StdRng::seed_from_u64(42);
        let mut dist = (
            range(0i64, 1_000_000),
            range(0i32, 100_000),
            range(0.0f64, 1.0),
        );
        let result: Vec<(i64, i32, f64)> = dist.collect::<Col>(&mut rng, black_box(ROWS));
        black_box(result)
    });
}

/// Cols marker: generates column-by-column from tuple iterator
#[divan::bench]
fn collect_cols(bencher: Bencher) {
    bencher.bench(|| {
        let mut rng = StdRng::seed_from_u64(42);
        let mut dist = (
            range(0i64, 1_000_000),
            range(0i32, 100_000),
            range(0.0f64, 1.0),
        );
        let result: (Vec<i64>, Vec<i32>, Vec<f64>) =
            dist.collect::<Cols>(&mut rng, black_box(ROWS));
        black_box(result)
    });
}

/// ColumnarCols marker: generates each column fully before next
#[divan::bench]
fn collect_columnar_cols(bencher: Bencher) {
    bencher.bench(|| {
        let mut rng = StdRng::seed_from_u64(42);
        let mut dist = (
            range(0i64, 1_000_000),
            range(0i32, 100_000),
            range(0.0f64, 1.0),
        );
        let result: (Vec<i64>, Vec<i32>, Vec<f64>) =
            dist.collect::<ColumnarCols>(&mut rng, black_box(ROWS));
        black_box(result)
    });
}

/// 2-tuple comparison: row-by-row
#[divan::bench]
fn collect_2tuple_row_by_row(bencher: Bencher) {
    bencher.bench(|| {
        let mut rng = StdRng::seed_from_u64(42);
        let mut dist = (range(0i64, 1_000_000), range(0i32, 100_000));
        let result: Vec<(i64, i32)> = dist.collect::<Col>(&mut rng, black_box(ROWS));
        black_box(result)
    });
}

/// 2-tuple comparison: columnar
#[divan::bench]
fn collect_2tuple_columnar(bencher: Bencher) {
    bencher.bench(|| {
        let mut rng = StdRng::seed_from_u64(42);
        let mut dist = (range(0i64, 1_000_000), range(0i32, 100_000));
        let result: (Vec<i64>, Vec<i32>) = dist.collect::<ColumnarCols>(&mut rng, black_box(ROWS));
        black_box(result)
    });
}

/// 7-tuple row-by-row
#[divan::bench]
fn collect_7tuple_row_by_row(bencher: Bencher) {
    bencher.bench(|| {
        let mut rng = StdRng::seed_from_u64(42);
        let mut dist = (
            range(0i64, 1_000_000),
            range(0i32, 100_000),
            range(0.0f64, 1.0),
            range(0i64, 1_000_000),
            range(0i32, 100_000),
            range(0.0f64, 1.0),
            range(0i64, 1_000_000),
        );
        let result: Vec<(i64, i32, f64, i64, i32, f64, i64)> =
            dist.collect::<Col>(&mut rng, black_box(ROWS));
        black_box(result)
    });
}

/// 7-tuple columnar
#[divan::bench]
fn collect_7tuple_columnar(bencher: Bencher) {
    bencher.bench(|| {
        let mut rng = StdRng::seed_from_u64(42);
        let mut dist = (
            range(0i64, 1_000_000),
            range(0i32, 100_000),
            range(0.0f64, 1.0),
            range(0i64, 1_000_000),
            range(0i32, 100_000),
            range(0.0f64, 1.0),
            range(0i64, 1_000_000),
        );
        let result: (
            Vec<i64>,
            Vec<i32>,
            Vec<f64>,
            Vec<i64>,
            Vec<i32>,
            Vec<f64>,
            Vec<i64>,
        ) = dist.collect::<ColumnarCols>(&mut rng, black_box(ROWS));
        black_box(result)
    });
}

/// 20-column simulation: row-by-row access pattern
/// Simulates interleaved writes across 20 columns
#[divan::bench]
fn wide_20col_row_by_row(bencher: Bencher) {
    bencher.bench(|| {
        let mut rng = StdRng::seed_from_u64(42);
        // Pre-allocate 20 columns
        let mut cols: [Vec<i64>; 20] = std::array::from_fn(|_| Vec::with_capacity(ROWS));

        // Row-by-row: write one value to each column, then next row
        for _ in 0..ROWS {
            for col in cols.iter_mut() {
                col.push(rng.random_range(0i64..1_000_000));
            }
        }

        black_box(cols)
    });
}

/// 20-column simulation: columnar access pattern
/// Generates each column fully before moving to next
#[divan::bench]
fn wide_20col_columnar(bencher: Bencher) {
    bencher.bench(|| {
        let mut rng = StdRng::seed_from_u64(42);
        // Generate 20 columns, one at a time
        let cols: [Vec<i64>; 20] = std::array::from_fn(|_| {
            (0..ROWS)
                .map(|_| rng.random_range(0i64..1_000_000))
                .collect()
        });

        black_box(cols)
    });
}

// === SmallRng benchmarks (xoshiro - much faster than ChaCha) ===

/// SmallRng 7-tuple row-by-row
#[divan::bench]
fn smallrng_7tuple_row_by_row(bencher: Bencher) {
    bencher.bench(|| {
        let mut rng = SmallRng::seed_from_u64(42);
        let mut dist = (
            range(0i64, 1_000_000),
            range(0i32, 100_000),
            range(0.0f64, 1.0),
            range(0i64, 1_000_000),
            range(0i32, 100_000),
            range(0.0f64, 1.0),
            range(0i64, 1_000_000),
        );
        let result: Vec<(i64, i32, f64, i64, i32, f64, i64)> =
            dist.collect::<Col>(&mut rng, black_box(ROWS));
        black_box(result)
    });
}

/// SmallRng 7-tuple columnar
#[divan::bench]
fn smallrng_7tuple_columnar(bencher: Bencher) {
    bencher.bench(|| {
        let mut rng = SmallRng::seed_from_u64(42);
        let mut dist = (
            range(0i64, 1_000_000),
            range(0i32, 100_000),
            range(0.0f64, 1.0),
            range(0i64, 1_000_000),
            range(0i32, 100_000),
            range(0.0f64, 1.0),
            range(0i64, 1_000_000),
        );
        let result: (
            Vec<i64>,
            Vec<i32>,
            Vec<f64>,
            Vec<i64>,
            Vec<i32>,
            Vec<f64>,
            Vec<i64>,
        ) = dist.collect::<ColumnarCols>(&mut rng, black_box(ROWS));
        black_box(result)
    });
}

// === Batch fill benchmarks ===

/// Batch fill using Rng::fill - fills entire column at once
#[divan::bench]
fn batch_fill_columnar(bencher: Bencher) {
    bencher.bench(|| {
        let mut rng = SmallRng::seed_from_u64(42);
        // Use fill() to batch-fill entire columns
        let mut col1 = vec![0i64; ROWS];
        let mut col2 = vec![0i64; ROWS];
        let mut col3 = vec![0i64; ROWS];
        let mut col4 = vec![0i64; ROWS];
        let mut col5 = vec![0i64; ROWS];
        let mut col6 = vec![0i64; ROWS];
        let mut col7 = vec![0i64; ROWS];

        rng.fill(col1.as_mut_slice());
        rng.fill(col2.as_mut_slice());
        rng.fill(col3.as_mut_slice());
        rng.fill(col4.as_mut_slice());
        rng.fill(col5.as_mut_slice());
        rng.fill(col6.as_mut_slice());
        rng.fill(col7.as_mut_slice());

        black_box((col1, col2, col3, col4, col5, col6, col7))
    });
}

/// Batch fill row-by-row for comparison
#[divan::bench]
fn batch_fill_row_by_row(bencher: Bencher) {
    bencher.bench(|| {
        let mut rng = SmallRng::seed_from_u64(42);
        let mut col1 = Vec::with_capacity(ROWS);
        let mut col2 = Vec::with_capacity(ROWS);
        let mut col3 = Vec::with_capacity(ROWS);
        let mut col4 = Vec::with_capacity(ROWS);
        let mut col5 = Vec::with_capacity(ROWS);
        let mut col6 = Vec::with_capacity(ROWS);
        let mut col7 = Vec::with_capacity(ROWS);

        for _ in 0..ROWS {
            col1.push(rng.random::<i64>());
            col2.push(rng.random::<i64>());
            col3.push(rng.random::<i64>());
            col4.push(rng.random::<i64>());
            col5.push(rng.random::<i64>());
            col6.push(rng.random::<i64>());
            col7.push(rng.random::<i64>());
        }

        black_box((col1, col2, col3, col4, col5, col6, col7))
    });
}

// === Arrow RecordBatch benchmarks ===

/// Arrow RecordBatch 3-tuple (already columnar internally)
#[divan::bench]
fn arrow_recordbatch_3tuple(bencher: Bencher) {
    bencher.bench(|| {
        let mut rng = StdRng::seed_from_u64(42);
        let mut dist = (
            range(0i64, 1_000_000),
            range(0i32, 100_000),
            range(0.0f64, 1.0),
        );
        let result: RecordBatch = dist.collect::<RecordBatchW>(&mut rng, black_box(ROWS));
        black_box(result)
    });
}

/// Arrow RecordBatch with SmallRng
#[divan::bench]
fn arrow_recordbatch_smallrng(bencher: Bencher) {
    bencher.bench(|| {
        let mut rng = SmallRng::seed_from_u64(42);
        let mut dist = (
            range(0i64, 1_000_000),
            range(0i32, 100_000),
            range(0.0f64, 1.0),
        );
        let result: RecordBatch = dist.collect::<RecordBatchW>(&mut rng, black_box(ROWS));
        black_box(result)
    });
}
