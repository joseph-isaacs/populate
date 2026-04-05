// ========================================================================
// Scalar (single-column) collection tests — bare Vec<T>, no tuples!
// ========================================================================

use crate::test::dist::{
    make_bernoulli_always_false, make_bernoulli_always_true, make_bernoulli_biased, make_constant,
    make_mapped, make_range,
};
use crate::traits::{DistributionExt, DistributionScalar, VecBackend};
use rand::rng;

#[test]
fn test_scalar_constant() {
    let mut dist = make_constant::<VecBackend>();
    let col = dist.collect_col(Vec::<i32>::new(), &mut rng(), 4);
    assert_eq!(col, vec![42, 42, 42, 42]);
}

#[test]
fn test_single_col_constant() {
    let mut dist = make_constant::<VecBackend>();
    let (col,) = dist.collect_flat((Vec::<i32>::new(),), &mut rng(), 4);
    assert_eq!(col, vec![42, 42, 42, 42]);
}

#[test]
fn test_scalar_range() {
    let mut dist = make_range::<VecBackend>();
    let col = dist.collect_col(Vec::<i64>::new(), &mut rng(), 5);
    assert_eq!(col, vec![0, 1, 2, 3, 4]);
}

#[test]
fn test_scalar_mapped() {
    let mut dist = make_mapped();
    let col = dist.collect_col(Vec::<i64>::new(), &mut rng(), 4);
    assert_eq!(col, vec![0, 2, 4, 6]);
}

#[test]
fn test_bernoulli_always_true() {
    let mut dist = make_bernoulli_always_true::<VecBackend>();
    let col = dist.collect_col(Vec::<bool>::new(), &mut rng(), 5);
    assert_eq!(col, vec![true, true, true, true, true]);
}

#[test]
fn test_bernoulli_always_false() {
    let mut dist = make_bernoulli_always_false::<VecBackend>();
    let col = dist.collect_col(Vec::<bool>::new(), &mut rng(), 5);
    assert_eq!(col, vec![false, false, false, false, false]);
}

#[test]
fn test_bernoulli_biased() {
    use rand::SeedableRng;
    let mut rng = rand::rngs::StdRng::seed_from_u64(42);
    let mut dist = make_bernoulli_biased::<VecBackend>();
    let col = dist.collect_col(Vec::<bool>::new(), &mut rng, 1000);
    let true_count = col.iter().filter(|&&b| b).count();
    // With p=0.5 and n=1000, expect ~500 trues; allow wide margin
    assert!(
        true_count > 400 && true_count < 600,
        "true_count={true_count}"
    );
}
