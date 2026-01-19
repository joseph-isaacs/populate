//! Benchmark: Row-by-row vs Column-by-column execution for Arrow export
//!
//! This benchmark compares two approaches for generating a wide table (1000 columns):
//! 1. Row-by-row: Generate one row at a time, interleaving column generation
//! 2. Column-by-column: Generate all values for column 1, then column 2, etc.
//!
//! Theory: Column-by-column should be faster due to better cache locality.
//! When filling column 1, the iterator state for column 1 stays hot in cache.
//! Row-by-row alternates between 1000 different iterator states, causing cache misses.

use populate::builtins::Range;
use populate::core::{Distribution, Rng, SampleIter};
use rand::SeedableRng;
use rand::rngs::StdRng;
use std::time::Instant;

const NUM_COLS: usize = 1000;

/// Create 1000 Range distributions (cycling through i32, i64, f32, f64)
fn create_iters(rng_seed: u64) -> (StdRng, Vec<Box<dyn Iterator<Item = f64>>>) {
    let rng = StdRng::seed_from_u64(rng_seed);

    // We'll create iterators that capture their RNG state
    // Each iterator is independent
    let iters: Vec<Box<dyn Iterator<Item = f64>>> = (0..NUM_COLS)
        .map(|i| {
            let mut local_rng = StdRng::seed_from_u64(rng_seed + i as u64);
            match i % 4 {
                0 => {
                    let mut d = Range::new(0i32, 1000);
                    let mut iter = d.sample();
                    Box::new(std::iter::from_fn(move || {
                        Some(iter.next(&mut local_rng) as f64)
                    })) as Box<dyn Iterator<Item = f64>>
                }
                1 => {
                    let mut d = Range::new(0i64, 1000);
                    let mut iter = d.sample();
                    Box::new(std::iter::from_fn(move || {
                        Some(iter.next(&mut local_rng) as f64)
                    })) as Box<dyn Iterator<Item = f64>>
                }
                2 => {
                    let mut d = Range::new(0.0f32, 1000.0);
                    let mut iter = d.sample();
                    Box::new(std::iter::from_fn(move || {
                        Some(iter.next(&mut local_rng) as f64)
                    })) as Box<dyn Iterator<Item = f64>>
                }
                _ => {
                    let mut d = Range::new(0.0f64, 1000.0);
                    let mut iter = d.sample();
                    Box::new(std::iter::from_fn(move || Some(iter.next(&mut local_rng))))
                        as Box<dyn Iterator<Item = f64>>
                }
            }
        })
        .collect();

    (rng, iters)
}

/// Row-by-row collection: interleaves column generation
fn collect_row_by_row(n: usize) -> Vec<Vec<f64>> {
    let (_, mut iters) = create_iters(42);

    // Pre-allocate output
    let mut columns: Vec<Vec<f64>> = (0..NUM_COLS).map(|_| Vec::with_capacity(n)).collect();

    // Row-by-row: generate one value per column per iteration
    // This interleaves access to all 1000 iterator states
    for _ in 0..n {
        for (col_idx, iter) in iters.iter_mut().enumerate() {
            columns[col_idx].push(iter.next().unwrap());
        }
    }

    columns
}

/// Column-by-column collection: fills each column entirely before moving to next
fn collect_column_by_column(n: usize) -> Vec<Vec<f64>> {
    let (_, mut iters) = create_iters(42);

    // Pre-allocate output
    let mut columns: Vec<Vec<f64>> = (0..NUM_COLS).map(|_| Vec::with_capacity(n)).collect();

    // Column-by-column: fill each column entirely before moving to next
    // Iterator state stays hot in cache
    for (col_idx, iter) in iters.iter_mut().enumerate() {
        for _ in 0..n {
            columns[col_idx].push(iter.next().unwrap());
        }
    }

    columns
}

fn benchmark(name: &str, f: impl Fn() -> Vec<Vec<f64>>, iterations: usize) -> f64 {
    // Warmup
    for _ in 0..2 {
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
    println!("         {} columns (wide table)", NUM_COLS);
    println!("==============================================\n");

    let row_counts = [1_000, 10_000, 100_000];

    for &n in &row_counts {
        println!("--- {} rows, {} columns ---", n, NUM_COLS);

        let iterations = if n >= 100_000 { 3 } else { 10 };

        // Row-by-row
        let row_ms = benchmark("Row-by-row    ", || collect_row_by_row(n), iterations);

        // Column-by-column
        let col_ms = benchmark("Column-by-col ", || collect_column_by_column(n), iterations);

        let speedup = row_ms / col_ms;
        let diff_pct = (row_ms - col_ms) / row_ms * 100.0;

        println!("Speedup: {:.2}x ({:.1}% faster)\n", speedup, diff_pct);
    }

    println!("==============================================");
    println!("ANALYSIS");
    println!("==============================================\n");

    println!(
        "With {} columns, column-by-column should show larger gains:",
        NUM_COLS
    );
    println!("1. {} iterator states exceed L1/L2 cache", NUM_COLS);
    println!("2. Row-by-row causes severe cache thrashing");
    println!("3. Column-by-column keeps one iterator hot at a time");
    println!();
    println!(
        "Row-by-row must load/evict iterator state {} times per row",
        NUM_COLS
    );
    println!("Column-by-column loads each iterator state once, uses it n times");
}
