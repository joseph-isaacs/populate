//! Processes, including TSBS's own deterministic unit tests
//! (`pkg/data/usecases/common/distribution_test.go`, `iot/diagnostics_test.go`).

use crate::process::{
    Constant, Process, RefillWalk, clamped_walk, lazy, monotonic_walk, normal, precision,
    random_walk, series, uniform,
};
use crate::sampler::{GoRand, GoRandExt, GoSampler};
use crate::traits::{Distribution, DistributionScalar, VecBackend};

// ============================================================================
// Ported TSBS unit tests (deterministic parts)
// ============================================================================

#[test]
fn float_precision_get() {
    // common/distribution_test.go TestFloatPrecisionGet
    let cases = [
        (-1, 1.0),
        (0, 1.0),
        (1, 1.2),
        (2, 1.23),
        (3, 1.234),
        (4, 1.2345),
        (5, 1.23456),
        (6, 1.23456),
        (9, 1.23456),
    ];
    for (digits, want) in cases {
        assert_eq!(precision(Constant(1.234567890), digits).get(), want);
    }
}

#[test]
fn lazy_distribution() {
    // common/distribution_test.go TestLazyDistribution
    let mut rng = GoRand::new(1);
    let cases = [
        (1.0, 0.0, 1.0, 2.0, 2.0),
        (1.0, 1.0, 1.0, 2.0, 3.0),
        (0.5, 1.0, 0.5, 0.5, 2.0),
    ];
    for (threshold, motive, step, state, want) in cases {
        let mut ld = lazy(
            Constant(motive),
            clamped_walk(Constant(step), 2.0, 4.0, state),
            threshold,
        );
        ld.advance(&mut rng);
        assert_eq!(ld.get(), want);
    }
}

#[test]
fn refill_walk_refills_at_min() {
    // iot/diagnostics_test.go TestCustomFuelDistribution
    let mut rng = GoRand::new(1);
    let mut clamped = clamped_walk(Constant(-1.0), 10.0, 100.0, 100.0);
    let mut fuel = RefillWalk(clamped.clone());
    while clamped.get() > 10.0 {
        clamped.advance(&mut rng);
        fuel.advance(&mut rng);
        if clamped.get() == 10.0 {
            assert_eq!(fuel.get(), 100.0);
        } else {
            assert_eq!(fuel.get(), clamped.get());
        }
    }
}

#[test]
fn walks() {
    let mut rng = GoRand::new(1);
    let mut w = random_walk(Constant(-2.0), 1.0);
    let mut m = monotonic_walk(Constant(-2.0), 1.0);
    w.advance(&mut rng);
    m.advance(&mut rng);
    assert_eq!((w.get(), m.get()), (-1.0, 3.0));
}

// ============================================================================
// Processes as columns
// ============================================================================

fn make_clamped_series() -> impl Distribution<VecBackend, Item = f64> {
    series(clamped_walk(Constant(30.0), 0.0, 100.0, 15.0))
}

#[test]
fn series_is_a_populate_column() {
    let mut d = make_clamped_series();
    let col: Vec<f64> = d.collect_col(Vec::new(), &mut GoRand::new(1), 5);
    assert_eq!(col, vec![15.0, 45.0, 75.0, 100.0, 100.0]);
}

#[test]
fn normal_and_uniform_use_the_sampler() {
    let mut rng = GoRand::new(7);
    let mut n = normal(GoSampler, 10.0, 2.0);
    let mut u = uniform(GoSampler, -1.0, 3.0);
    n.advance(&mut rng);
    u.advance(&mut rng);
    let mut want = GoRand::new(7);
    assert_eq!(n.get(), want.go_norm_float64() * 2.0 + 10.0);
    assert_eq!(u.get(), want.go_float64() * 4.0 - 1.0);
}
