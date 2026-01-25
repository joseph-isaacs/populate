use divan::{Bencher, black_box};
use populate_builtins::range;
use populate_core::CollectExt;
use populate_sink_vec::{Col, Cols, ColumnarCols};
use rand::Rng;
use rand::SeedableRng;
use rand::rngs::StdRng;

fn main() {
    divan::main();
}

const ROWS: usize = 1_000_000;

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
