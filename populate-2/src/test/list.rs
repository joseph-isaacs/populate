// ========================================================================
// List tests
// ========================================================================

use crate::test::Log;
use crate::test::dist::{gen_list, make_list_in_tuple, make_tracked_list_in_tuple};
use crate::traits::{Distribution, DistributionExt, DistributionScalar, SampleIter, VecBackend};
use rand::rng;

#[test]
fn test_list_row_iter() {
    let mut dist = gen_list::<VecBackend>();
    let mut iter = dist.row_iter();
    let mut rng = rng();
    let result = (0..5).map(|_| iter.next(&mut rng)).collect::<Vec<_>>();
    assert_eq!(
        result,
        vec![vec![], vec![0], vec![0, 1], vec![0, 1, 2], vec![0, 1, 2, 3]]
    );
}

#[test]
fn test_list_collect_col() {
    let mut dist = gen_list::<VecBackend>();
    let result = dist.collect_col(Vec::<Vec<i64>>::new(), &mut rng(), 3);
    assert_eq!(result, vec![vec![], vec![0], vec![0, 1]]);
}

#[test]
fn test_list_in_tuple() {
    let mut dist = make_list_in_tuple::<VecBackend>();
    let (scalars, lists) =
        dist.collect_flat_into((Vec::<i32>::new(), Vec::<Vec<i64>>::new()), &mut rng(), 3);

    assert_eq!(scalars, vec![42, 42, 42]);
    assert_eq!(lists, vec![vec![0, 1], vec![0, 1], vec![0, 1]]);
}

// ========================================================================
// Tracked<List> — exercises Sink<Vec<T>> through generic wrapper
// ========================================================================

#[test]
fn test_tracked_list_in_tuple() {
    let log: Log = Log::default();
    let mut dist = make_tracked_list_in_tuple::<VecBackend>(&log);
    let (scalars, lists) =
        dist.collect_flat_into((Vec::<i32>::new(), Vec::<Vec<i64>>::new()), &mut rng(), 3);

    assert_eq!(scalars, vec![42, 42, 42]);
    assert_eq!(lists, vec![vec![0, 1], vec![0, 1], vec![0, 1]]);
    // Col-by-col: COL1 x3, then LIST x3
    assert_eq!(
        *log.borrow(),
        vec!["COL1", "COL1", "COL1", "LIST", "LIST", "LIST"]
    );
}
