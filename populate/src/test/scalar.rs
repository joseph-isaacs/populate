// ========================================================================
// Scalar (single-column) collection tests — bare Vec<T>, no tuples!
// ========================================================================

use crate::test::dist::{make_constant, make_mapped, make_range};
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
