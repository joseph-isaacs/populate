// ========================================================================
// NamedVecBackend tests — columns carry NamedCol<Vec<T>>
// ========================================================================

use crate::test::Log;
use crate::test::dist::{
    make_named_3tuple, make_named_list, make_named_scalar, make_named_tracked_tuple,
    make_named_tuple,
};
use crate::traits::{DistributionExt, DistributionScalar, NamedCol, NamedVecBackend};
use rand::rng;

#[test]
fn test_named_scalar_collect_col() {
    let mut dist = make_named_scalar::<NamedVecBackend>();
    let col = dist.collect_col(NamedCol::default(), &mut rng(), 4);
    assert_eq!(col.inner, vec![0, 1, 2, 3]);
}

#[test]
fn test_named_scalar_collect_flat() {
    let mut dist = make_named_scalar::<NamedVecBackend>();
    let (col,) = dist.collect_flat(&mut rng(), 4);
    assert_eq!(col.name, "id");
    assert_eq!(col.inner, vec![0, 1, 2, 3]);
}

#[test]
fn test_named_tuple_collect_flat() {
    let mut dist = make_named_tuple::<NamedVecBackend>();
    let (c1, c2) = dist.collect_flat(&mut rng(), 3);
    assert_eq!(c1.name, "id");
    assert_eq!(c1.inner, vec![0, 1, 2]);
    assert_eq!(c2.name, "val");
    assert_eq!(c2.inner, vec![42, 42, 42]);
}

#[test]
fn test_named_tuple_collect_flat_into() {
    let mut dist = make_named_tuple::<NamedVecBackend>();
    let (c1, c2) = dist.collect_flat_into(
        (
            NamedCol {
                name: "id",
                inner: Vec::<i64>::new(),
            },
            NamedCol {
                name: "val",
                inner: Vec::<i32>::new(),
            },
        ),
        &mut rng(),
        3,
    );
    assert_eq!(c1.name, "id");
    assert_eq!(c1.inner, vec![0, 1, 2]);
    assert_eq!(c2.name, "val");
    assert_eq!(c2.inner, vec![42, 42, 42]);
}

#[test]
fn test_named_3tuple_collect_flat() {
    let mut dist = make_named_3tuple::<NamedVecBackend>();
    let (c1, c2, c3) = dist.collect_flat(&mut rng(), 2);
    assert_eq!(c1.name, "id");
    assert_eq!(c1.inner, vec![0, 1]);
    assert_eq!(c2.name, "val");
    assert_eq!(c2.inner, vec![42, 42]);
    assert_eq!(c3.name, "flag");
    assert_eq!(c3.inner, vec![true, true]);
}

#[test]
fn test_named_list_in_tuple() {
    let mut dist = make_named_list::<NamedVecBackend>();
    let (c1, c2) = dist.collect_flat(&mut rng(), 3);
    assert_eq!(c1.name, "id");
    assert_eq!(c1.inner, vec![1, 1, 1]);
    assert_eq!(c2.name, "items");
    assert_eq!(c2.inner, vec![vec![0, 1], vec![0, 1], vec![0, 1]]);
}

#[test]
fn test_named_col_by_col_order() {
    let log: Log = Log::default();
    let mut dist = make_named_tracked_tuple::<NamedVecBackend>(&log);
    let (c1, c2) = dist.collect_flat(&mut rng(), 3);

    assert_eq!(c1.name, "id");
    assert_eq!(c1.inner, vec![0, 1, 2]);
    assert_eq!(c2.name, "val");
    assert_eq!(c2.inner, vec![100, 101, 102]);
    // Col-by-col: COL1, COL1, COL1, COL2, COL2, COL2
    assert_eq!(
        *log.borrow(),
        vec!["COL1", "COL1", "COL1", "COL2", "COL2", "COL2"]
    );
}
