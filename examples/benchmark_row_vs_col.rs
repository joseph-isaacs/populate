//! Benchmark: Row-by-row vs Column-by-column execution for Arrow export
//!
//! This benchmark compares two approaches for generating a wide table (10 columns):
//! 1. Row-by-row: Generate one row at a time, interleaving column generation
//! 2. Column-by-column: Generate all values for column 1, then column 2, etc.
//!
//! Theory: Column-by-column should be faster due to better cache locality.
//! When filling column 1, the iterator state for column 1 stays hot in cache.
//! Row-by-row alternates between 10 different iterator states, causing cache misses.

use arrow_array::builder::PrimitiveBuilder;
use arrow_array::types::{Float32Type, Float64Type, Int32Type, Int64Type};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use populate::builtins::Range;
use populate::core::{Distribution, Rng, SampleIter};
use rand::SeedableRng;
use rand::rngs::StdRng;
use std::sync::Arc;
use std::time::Instant;

/// 10-column distribution using primitive types
type TenColDist = (
    Range<i32>,
    Range<i64>,
    Range<f32>,
    Range<f64>,
    Range<i32>,
    Range<i64>,
    Range<f32>,
    Range<f64>,
    Range<i32>,
    Range<i64>,
);

fn create_dist() -> TenColDist {
    (
        Range::new(0i32, 1000),
        Range::new(0i64, 1000),
        Range::new(0.0f32, 1000.0),
        Range::new(0.0f64, 1000.0),
        Range::new(0i32, 1000),
        Range::new(0i64, 1000),
        Range::new(0.0f32, 1000.0),
        Range::new(0.0f64, 1000.0),
        Range::new(0i32, 1000),
        Range::new(0i64, 1000),
    )
}

/// Row-by-row collection: interleaves column generation
fn collect_row_by_row(dist: &mut TenColDist, rng: &mut impl Rng, n: usize) -> RecordBatch {
    // Get iterators for each column
    let mut i0 = dist.0.sample();
    let mut i1 = dist.1.sample();
    let mut i2 = dist.2.sample();
    let mut i3 = dist.3.sample();
    let mut i4 = dist.4.sample();
    let mut i5 = dist.5.sample();
    let mut i6 = dist.6.sample();
    let mut i7 = dist.7.sample();
    let mut i8 = dist.8.sample();
    let mut i9 = dist.9.sample();

    // Create builders
    let mut b0 = PrimitiveBuilder::<Int32Type>::with_capacity(n);
    let mut b1 = PrimitiveBuilder::<Int64Type>::with_capacity(n);
    let mut b2 = PrimitiveBuilder::<Float32Type>::with_capacity(n);
    let mut b3 = PrimitiveBuilder::<Float64Type>::with_capacity(n);
    let mut b4 = PrimitiveBuilder::<Int32Type>::with_capacity(n);
    let mut b5 = PrimitiveBuilder::<Int64Type>::with_capacity(n);
    let mut b6 = PrimitiveBuilder::<Float32Type>::with_capacity(n);
    let mut b7 = PrimitiveBuilder::<Float64Type>::with_capacity(n);
    let mut b8 = PrimitiveBuilder::<Int32Type>::with_capacity(n);
    let mut b9 = PrimitiveBuilder::<Int64Type>::with_capacity(n);

    // Row-by-row: generate one value per column per iteration
    // This interleaves access to all 10 iterator states
    for _ in 0..n {
        b0.append_value(i0.next(rng));
        b1.append_value(i1.next(rng));
        b2.append_value(i2.next(rng));
        b3.append_value(i3.next(rng));
        b4.append_value(i4.next(rng));
        b5.append_value(i5.next(rng));
        b6.append_value(i6.next(rng));
        b7.append_value(i7.next(rng));
        b8.append_value(i8.next(rng));
        b9.append_value(i9.next(rng));
    }

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(b0.finish()),
        Arc::new(b1.finish()),
        Arc::new(b2.finish()),
        Arc::new(b3.finish()),
        Arc::new(b4.finish()),
        Arc::new(b5.finish()),
        Arc::new(b6.finish()),
        Arc::new(b7.finish()),
        Arc::new(b8.finish()),
        Arc::new(b9.finish()),
    ];

    let schema = Schema::new(vec![
        Field::new("c0", DataType::Int32, false),
        Field::new("c1", DataType::Int64, false),
        Field::new("c2", DataType::Float32, false),
        Field::new("c3", DataType::Float64, false),
        Field::new("c4", DataType::Int32, false),
        Field::new("c5", DataType::Int64, false),
        Field::new("c6", DataType::Float32, false),
        Field::new("c7", DataType::Float64, false),
        Field::new("c8", DataType::Int32, false),
        Field::new("c9", DataType::Int64, false),
    ]);

    RecordBatch::try_new(Arc::new(schema), arrays).unwrap()
}

/// Column-by-column collection: fills each column entirely before moving to next
fn collect_column_by_column(dist: &mut TenColDist, rng: &mut impl Rng, n: usize) -> RecordBatch {
    // Get iterators for each column
    let mut i0 = dist.0.sample();
    let mut i1 = dist.1.sample();
    let mut i2 = dist.2.sample();
    let mut i3 = dist.3.sample();
    let mut i4 = dist.4.sample();
    let mut i5 = dist.5.sample();
    let mut i6 = dist.6.sample();
    let mut i7 = dist.7.sample();
    let mut i8 = dist.8.sample();
    let mut i9 = dist.9.sample();

    // Create builders
    let mut b0 = PrimitiveBuilder::<Int32Type>::with_capacity(n);
    let mut b1 = PrimitiveBuilder::<Int64Type>::with_capacity(n);
    let mut b2 = PrimitiveBuilder::<Float32Type>::with_capacity(n);
    let mut b3 = PrimitiveBuilder::<Float64Type>::with_capacity(n);
    let mut b4 = PrimitiveBuilder::<Int32Type>::with_capacity(n);
    let mut b5 = PrimitiveBuilder::<Int64Type>::with_capacity(n);
    let mut b6 = PrimitiveBuilder::<Float32Type>::with_capacity(n);
    let mut b7 = PrimitiveBuilder::<Float64Type>::with_capacity(n);
    let mut b8 = PrimitiveBuilder::<Int32Type>::with_capacity(n);
    let mut b9 = PrimitiveBuilder::<Int64Type>::with_capacity(n);

    // Column-by-column: fill each column entirely before moving to next
    // Iterator state stays hot in cache
    for _ in 0..n {
        b0.append_value(i0.next(rng));
    }
    for _ in 0..n {
        b1.append_value(i1.next(rng));
    }
    for _ in 0..n {
        b2.append_value(i2.next(rng));
    }
    for _ in 0..n {
        b3.append_value(i3.next(rng));
    }
    for _ in 0..n {
        b4.append_value(i4.next(rng));
    }
    for _ in 0..n {
        b5.append_value(i5.next(rng));
    }
    for _ in 0..n {
        b6.append_value(i6.next(rng));
    }
    for _ in 0..n {
        b7.append_value(i7.next(rng));
    }
    for _ in 0..n {
        b8.append_value(i8.next(rng));
    }
    for _ in 0..n {
        b9.append_value(i9.next(rng));
    }

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(b0.finish()),
        Arc::new(b1.finish()),
        Arc::new(b2.finish()),
        Arc::new(b3.finish()),
        Arc::new(b4.finish()),
        Arc::new(b5.finish()),
        Arc::new(b6.finish()),
        Arc::new(b7.finish()),
        Arc::new(b8.finish()),
        Arc::new(b9.finish()),
    ];

    let schema = Schema::new(vec![
        Field::new("c0", DataType::Int32, false),
        Field::new("c1", DataType::Int64, false),
        Field::new("c2", DataType::Float32, false),
        Field::new("c3", DataType::Float64, false),
        Field::new("c4", DataType::Int32, false),
        Field::new("c5", DataType::Int64, false),
        Field::new("c6", DataType::Float32, false),
        Field::new("c7", DataType::Float64, false),
        Field::new("c8", DataType::Int32, false),
        Field::new("c9", DataType::Int64, false),
    ]);

    RecordBatch::try_new(Arc::new(schema), arrays).unwrap()
}

fn benchmark(name: &str, f: impl Fn() -> RecordBatch, iterations: usize) -> f64 {
    // Warmup
    for _ in 0..3 {
        let _ = f();
    }

    // Timed runs
    let start = Instant::now();
    for _ in 0..iterations {
        let _ = f();
    }
    let elapsed = start.elapsed();

    let ms_per_iter = elapsed.as_secs_f64() * 1000.0 / iterations as f64;
    println!(
        "{}: {:.3} ms/iter ({} iterations)",
        name, ms_per_iter, iterations
    );
    ms_per_iter
}

fn main() {
    println!("==============================================");
    println!("BENCHMARK: Row-by-row vs Column-by-column");
    println!("==============================================\n");

    let row_counts = [10_000, 100_000, 1_000_000, 10_000_000];

    for &n in &row_counts {
        println!("--- {} rows, 10 columns ---", n);

        let iterations = if n >= 1_000_000 { 5 } else { 20 };

        // Row-by-row
        let row_ms = benchmark(
            "Row-by-row    ",
            || {
                let mut dist = create_dist();
                let mut rng = StdRng::seed_from_u64(42);
                collect_row_by_row(&mut dist, &mut rng, n)
            },
            iterations,
        );

        // Column-by-column
        let col_ms = benchmark(
            "Column-by-col ",
            || {
                let mut dist = create_dist();
                let mut rng = StdRng::seed_from_u64(42);
                collect_column_by_column(&mut dist, &mut rng, n)
            },
            iterations,
        );

        let speedup = row_ms / col_ms;
        let diff_pct = (row_ms - col_ms) / row_ms * 100.0;

        println!("Speedup: {:.2}x ({:.1}% faster)\n", speedup, diff_pct);
    }

    println!("==============================================");
    println!("ANALYSIS");
    println!("==============================================\n");

    println!("Column-by-column should be faster because:");
    println!("1. Iterator state stays in L1/L2 cache during column fill");
    println!("2. Builder's internal buffer has sequential writes");
    println!("3. No cache thrashing between 10 different iterator states");
    println!();
    println!("Row-by-row is slower because:");
    println!("1. Alternates between 10 iterator states (cache misses)");
    println!("2. Builder writes are still sequential, but iterator access isn't");
    println!("3. RNG state also gets interleaved access patterns");
}
