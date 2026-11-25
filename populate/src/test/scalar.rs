// ========================================================================
// Scalar (single-column) collection tests: the columns are a bare Vec<T>
// ========================================================================

use crate::Distribution;
use crate::test::dist::{make_constant, make_mapped, make_range};
use rand::rng;

#[test]
fn test_scalar_constant() {
    let mut dist = make_constant();
    let col: Vec<i32> = dist.collect(&mut rng(), 4);
    assert_eq!(col, vec![42, 42, 42, 42]);
}

#[test]
fn test_scalar_range() {
    let mut dist = make_range();
    let col: Vec<i64> = dist.collect(&mut rng(), 5);
    assert_eq!(col, vec![0, 1, 2, 3, 4]);
}

#[test]
fn test_scalar_mapped() {
    let mut dist = make_mapped();
    let col: Vec<i64> = dist.collect(&mut rng(), 4);
    assert_eq!(col, vec![0, 2, 4, 6]);
}

#[test]
fn test_collect_into_appends() {
    let mut dist = make_range();
    let mut col = vec![-1];
    dist.collect_into(&mut col, &mut rng(), 2);
    dist.collect_into(&mut col, &mut rng(), 2);
    assert_eq!(col, vec![-1, 0, 1, 2, 3]);
}
