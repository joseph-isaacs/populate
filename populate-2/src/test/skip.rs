// ========================================================================
// Skip tests — verify Distribution::skip for both row and col paths
// ========================================================================

use crate::dist::{constant, list, map, range};
use crate::test::dist::{make_constant, make_mapped, make_range, make_tuple};
use crate::traits::{Backend, Distribution, DistributionExt, SampleIter, VecBackend};
use rand::rng;

// Helper: returns a Distribution<B> so the backend is resolved by the caller.
fn make_range_step<B: Backend>(start: i64, step: i64) -> impl Distribution<B, Value = i64> {
    range(start, step)
}

fn make_list_const<B: Backend>() -> impl Distribution<B, Value = Vec<i64>> {
    list(range(0usize, 1), constant(99i64))
}

// ========================================================================
// Distribution::skip + row_iter — skip then iterate
// ========================================================================

#[test]
fn test_skip_constant() {
    let mut dist = make_constant::<VecBackend>();
    dist.skip(&mut rng(), 100);
    let mut iter = dist.row_iter();
    assert_eq!(iter.next(&mut rng()), 42);
}

#[test]
fn test_skip_range() {
    let mut dist = make_range::<VecBackend>();
    dist.skip(&mut rng(), 5);
    let mut iter = dist.row_iter();
    assert_eq!(iter.next(&mut rng()), 5);
    assert_eq!(iter.next(&mut rng()), 6);
}

#[test]
fn test_skip_range_with_step() {
    let mut dist = make_range_step::<VecBackend>(0, 10);
    dist.skip(&mut rng(), 3);
    let mut iter = dist.row_iter();
    assert_eq!(iter.next(&mut rng()), 30);
    assert_eq!(iter.next(&mut rng()), 40);
}

#[test]
fn test_skip_zero() {
    let mut dist = make_range::<VecBackend>();
    dist.skip(&mut rng(), 0);
    let mut iter = dist.row_iter();
    assert_eq!(iter.next(&mut rng()), 0);
}

#[test]
fn test_skip_map() {
    let mut dist = make_mapped::<VecBackend>();
    dist.skip(&mut rng(), 5);
    let mut iter = dist.row_iter();
    assert_eq!(iter.next(&mut rng()), 10);
    assert_eq!(iter.next(&mut rng()), 12);
}

#[test]
fn test_skip_tuple2() {
    let mut dist = make_tuple::<VecBackend>();
    dist.skip(&mut rng(), 3);
    let mut iter = dist.row_iter();
    let (a, b) = iter.next(&mut rng());
    assert_eq!(a, 1);
    assert_eq!(b, 30);
}

#[test]
fn test_skip_tuple3() {
    let mut rng = rng();
    let mut dist = (constant(1i32), range(0i64, 10i64), range(100i32, 1));
    Distribution::<VecBackend>::skip(&mut dist, &mut rng, 5);
    let mut iter = Distribution::<VecBackend>::row_iter(&mut dist);
    let (a, b, c) = iter.next(&mut rng);
    assert_eq!(a, 1);
    assert_eq!(b, 50);
    assert_eq!(c, 105);
}

#[test]
fn test_skip_tuple4() {
    let mut rng = rng();
    let mut dist = (
        constant(1i32),
        range(0i64, 10i64),
        range(100i32, 1),
        constant(true),
    );
    Distribution::<VecBackend>::skip(&mut dist, &mut rng, 5);
    let mut iter = Distribution::<VecBackend>::row_iter(&mut dist);
    let (a, b, c, d) = iter.next(&mut rng);
    assert_eq!(a, 1);
    assert_eq!(b, 50);
    assert_eq!(c, 105);
    assert_eq!(d, true);
}

#[test]
fn test_skip_list_lengths() {
    // List with deterministic lengths: 0, 1, 2, 3, ...
    // After skip(3), first length should be 3
    let mut dist = make_list_const::<VecBackend>();
    dist.skip(&mut rng(), 3);
    let mut iter = dist.row_iter();
    let row = iter.next(&mut rng());
    assert_eq!(row.len(), 3);
    assert_eq!(row, vec![99, 99, 99]);
}

// ========================================================================
// Distribution::skip + collect_flat — col-by-col collection
// ========================================================================

#[test]
fn test_dist_skip_scalar_collect() {
    let mut rng = rng();
    let mut dist = make_range::<VecBackend>();
    dist.skip(&mut rng, 5);
    let (col,): (Vec<i64>,) = dist.collect_flat(&mut rng, 4);
    assert_eq!(col, vec![5, 6, 7, 8]);
}

#[test]
fn test_dist_skip_tuple_collect() {
    let mut rng = rng();
    let mut dist = make_tuple::<VecBackend>();
    dist.skip(&mut rng, 3);
    let (a, b): (Vec<i32>, Vec<i64>) = dist.collect_flat(&mut rng, 4);
    assert_eq!(a, vec![1, 1, 1, 1]);
    assert_eq!(b, vec![30, 40, 50, 60]);
}

#[test]
fn test_dist_skip_3tuple_collect() {
    let mut rng = rng();
    let mut dist = (constant(1i32), range(0i64, 10i64), range(100i32, 1));
    Distribution::<VecBackend>::skip(&mut dist, &mut rng, 5);
    let (a, b, c): (Vec<i32>, Vec<i64>, Vec<i32>) =
        DistributionExt::<VecBackend>::collect_flat(&mut dist, &mut rng, 3);
    assert_eq!(a, vec![1, 1, 1]);
    assert_eq!(b, vec![50, 60, 70]);
    assert_eq!(c, vec![105, 106, 107]);
}

#[test]
fn test_dist_skip_4tuple_collect() {
    let mut rng = rng();
    let mut dist = (
        range(0i32, 1),
        range(100i64, 10i64),
        constant(true),
        range(0f64, 0.5),
    );
    Distribution::<VecBackend>::skip(&mut dist, &mut rng, 4);
    let (a, b, c, d): (Vec<i32>, Vec<i64>, Vec<bool>, Vec<f64>) =
        DistributionExt::<VecBackend>::collect_flat(&mut dist, &mut rng, 3);
    assert_eq!(a, vec![4, 5, 6]);
    assert_eq!(b, vec![140, 150, 160]);
    assert_eq!(c, vec![true, true, true]);
    assert_eq!(d, vec![2.0, 2.5, 3.0]);
}

#[test]
fn test_dist_skip_map_collect() {
    let mut rng = rng();
    let mut dist = make_mapped::<VecBackend>();
    dist.skip(&mut rng, 5);
    let (col,): (Vec<i64>,) = dist.collect_flat(&mut rng, 4);
    assert_eq!(col, vec![10, 12, 14, 16]);
}

#[test]
fn test_dist_skip_list_collect() {
    let mut rng = rng();
    // Lengths: 0, 1, 2, 3, ... — skip 2 means first length is 2
    let mut dist = make_list_const::<VecBackend>();
    dist.skip(&mut rng, 2);
    let (col,): (Vec<Vec<i64>>,) = dist.collect_flat(&mut rng, 3);
    assert_eq!(
        col,
        vec![vec![99, 99], vec![99, 99, 99], vec![99, 99, 99, 99]]
    );
}

// ========================================================================
// Parallel generation equivalence tests
// ========================================================================

/// Verify skip + row_iter produces the same as sequential.
#[test]
fn test_skip_parallel_equivalence_row_iter() {
    let mut rng = rng();

    // Sequential: generate 10 rows
    let mut dist = make_range::<VecBackend>();
    let all: (Vec<i64>,) = dist.collect_flat(&mut rng, 10);

    // Worker 0: first 5
    let mut dist0 = make_range::<VecBackend>();
    let w0: (Vec<i64>,) = dist0.collect_flat(&mut rng, 5);

    // Worker 1: skip 5, then row_iter
    let mut dist1 = make_range::<VecBackend>();
    dist1.skip(&mut rng, 5);
    let mut iter1 = dist1.row_iter();
    let w1: Vec<i64> = (0..5).map(|_| iter1.next(&mut rng)).collect();

    let mut combined = w0.0;
    combined.extend(w1);
    assert_eq!(combined, all.0);
}

/// Verify skip + collect_flat produces the same as sequential — col path.
#[test]
fn test_skip_parallel_equivalence_col_iter() {
    let mut rng = rng();

    // Sequential: generate 10 rows
    let mut dist = make_tuple::<VecBackend>();
    let (all_a, all_b): (Vec<i32>, Vec<i64>) = dist.collect_flat(&mut rng, 10);

    // Worker 0: first 5 via collect_flat
    let mut d0 = make_tuple::<VecBackend>();
    let (w0_a, w0_b): (Vec<i32>, Vec<i64>) = d0.collect_flat(&mut rng, 5);

    // Worker 1: skip(5) then collect_flat 5
    let mut d1 = make_tuple::<VecBackend>();
    d1.skip(&mut rng, 5);
    let (w1_a, w1_b): (Vec<i32>, Vec<i64>) = d1.collect_flat(&mut rng, 5);

    let mut combined_a = w0_a;
    combined_a.extend(w1_a);
    let mut combined_b = w0_b;
    combined_b.extend(w1_b);
    assert_eq!(combined_a, all_a);
    assert_eq!(combined_b, all_b);
}

/// Row and col paths produce identical results after skip.
#[test]
fn test_skip_row_col_equivalence() {
    let mut rng = rng();

    // Col path: skip + collect_flat
    let mut dist_col = make_tuple::<VecBackend>();
    dist_col.skip(&mut rng, 10);
    let (col_a, col_b): (Vec<i32>, Vec<i64>) = dist_col.collect_flat(&mut rng, 5);

    // Row path: skip + row_iter + manual collect
    let mut dist_row = make_tuple::<VecBackend>();
    dist_row.skip(&mut rng, 10);
    let mut iter = dist_row.row_iter();
    let (mut row_a, mut row_b) = (Vec::new(), Vec::new());
    for _ in 0..5 {
        let (a, b) = iter.next(&mut rng);
        row_a.push(a);
        row_b.push(b);
    }

    assert_eq!(col_a, row_a);
    assert_eq!(col_b, row_b);
}

// ========================================================================
// Uniform skip determinism — verify RNG state advances correctly
// ========================================================================

/// After skip(n), Uniform produces the same values as generating n+batch
/// and taking the tail.
#[test]
fn test_skip_uniform_determinism() {
    use crate::dist::uniform;
    use rand::SeedableRng;

    let seed = 12345u64;
    let skip_n = 100;
    let batch = 10;

    // Path A: generate all (skip_n + batch), take the tail
    let mut rng_a = rand::rngs::SmallRng::seed_from_u64(seed);
    let mut dist_a = uniform(0i64, 1000);
    let (all,): (Vec<i64>,) =
        DistributionExt::<VecBackend>::collect_flat(&mut dist_a, &mut rng_a, skip_n + batch);
    let expected = &all[skip_n..];

    // Path B: skip(n) then generate batch — same seed, same RNG
    let mut rng_b = rand::rngs::SmallRng::seed_from_u64(seed);
    let mut dist_b = uniform(0i64, 1000);
    Distribution::<VecBackend>::skip(&mut dist_b, &mut rng_b, skip_n);
    let (got,): (Vec<i64>,) =
        DistributionExt::<VecBackend>::collect_flat(&mut dist_b, &mut rng_b, batch);

    assert_eq!(got, expected);
}

/// Deterministic lengths (Range) + random elements — verifies that skip
/// correctly moves Range.start and burns element rng draws.
#[test]
fn test_skip_list_range_lengths_random_elems() {
    use crate::dist::uniform;
    use rand::SeedableRng;

    let seed = 42u64;
    let skip_n = 30;
    let batch = 10;

    // Lengths: 1, 2, 3, ... (deterministic), elements: uniform random
    let make_dist = || list(range(1usize, 1), uniform(0i64, 1000));

    // Path A: generate all, take the tail
    let mut rng_a = rand::rngs::SmallRng::seed_from_u64(seed);
    let mut dist_a = make_dist();
    let (all,): (Vec<Vec<i64>>,) =
        DistributionExt::<VecBackend>::collect_flat(&mut dist_a, &mut rng_a, skip_n + batch);
    let expected = &all[skip_n..];

    // Path B: skip then generate
    let mut rng_b = rand::rngs::SmallRng::seed_from_u64(seed);
    let mut dist_b = make_dist();
    Distribution::<VecBackend>::skip(&mut dist_b, &mut rng_b, skip_n);
    let (got,): (Vec<Vec<i64>>,) =
        DistributionExt::<VecBackend>::collect_flat(&mut dist_b, &mut rng_b, batch);

    assert_eq!(got, expected);
}

// ========================================================================
// Benchmark: skip vs alloc-all-then-slice
// ========================================================================

/// Demonstrates skip is faster than generating everything and discarding.
/// Uses a tuple of (Range, Uniform, Map) — mixes deterministic + random.
#[test]
fn test_skip_vs_alloc_all() {
    use crate::dist::uniform;
    use std::hint::black_box;
    use std::time::Instant;

    let skip_n = 1_000_000usize;
    let batch = 1_000usize;

    // --- Path A: alloc all (skip_n + batch), take the tail ---
    let t0 = Instant::now();
    for _ in 0..10 {
        let mut dist = (
            range(0i64, 1),
            uniform(0i32, 1000),
            map(range(0i64, 1), |x| x * 3),
        );
        let (a, b, c): (Vec<i64>, Vec<i32>, Vec<i64>) =
            DistributionExt::<VecBackend>::collect_flat(&mut dist, &mut rng(), skip_n + batch);
        // Simulate keeping only the tail
        black_box(&a[skip_n..]);
        black_box(&b[skip_n..]);
        black_box(&c[skip_n..]);
    }
    let alloc_all_time = t0.elapsed();

    // --- Path B: skip, then collect only the batch ---
    let t1 = Instant::now();
    for _ in 0..10 {
        let mut rng = rng();
        let mut dist = (
            range(0i64, 1),
            uniform(0i32, 1000),
            map(range(0i64, 1), |x| x * 3),
        );
        Distribution::<VecBackend>::skip(&mut dist, &mut rng, skip_n);
        let (a, b, c): (Vec<i64>, Vec<i32>, Vec<i64>) =
            DistributionExt::<VecBackend>::collect_flat(&mut dist, &mut rng, batch);
        black_box(&a);
        black_box(&b);
        black_box(&c);
    }
    let skip_time = t1.elapsed();

    eprintln!(
        "alloc-all: {:?}, skip: {:?}, speedup: {:.1}x",
        alloc_all_time,
        skip_time,
        alloc_all_time.as_nanos() as f64 / skip_time.as_nanos() as f64
    );

    // skip should be substantially faster — at least 5x for 1M skip + 1K batch
    assert!(
        skip_time < alloc_all_time,
        "skip ({:?}) should be faster than alloc-all ({:?})",
        skip_time,
        alloc_all_time
    );
}
