// ========================================================================
// List tests
// ========================================================================

use crate::test::Log;
use crate::test::dist::{gen_list, make_list_in_tuple, make_tracked_list_in_tuple};
use crate::traits::{Distribution, DistributionExt};
use crate::traits::{SampleIter, VecListSink};
use rand::rng;
use std::cell::RefCell;
use std::rc::Rc;

#[test]
fn test_list_row_iter() {
    let mut dist = gen_list();
    let mut iter = dist.row_iter();
    let mut rng = rng();
    let result = (0..5).map(|_| iter.next(&mut rng)).collect::<Vec<_>>();
    assert_eq!(
        result,
        vec![vec![], vec![0], vec![0, 1], vec![0, 1, 2], vec![0, 1, 2, 3]]
    );
}

#[test]
fn test_list_collect_into() {
    let mut dist = gen_list();
    let (result,) = dist.collect_flat((VecListSink::new(),), &mut rng(), 3);
    let (values, offsets) = result.into_parts();
    assert_eq!(values, vec![0, 0, 1]);
    assert_eq!(offsets, vec![0, 0, 1, 3]);
}

#[test]
fn test_list_in_tuple() {
    let mut dist = make_list_in_tuple();
    let (scalars, list_sink) = dist.collect_flat(
        (Vec::<i32>::new(), VecListSink::<i64>::new()),
        &mut rng(),
        3,
    );
    let (values, offsets) = list_sink.into_parts();

    assert_eq!(scalars, vec![42, 42, 42]);
    assert_eq!(values, vec![0, 1, 0, 1, 0, 1]);
    assert_eq!(offsets, vec![0, 2, 4, 6]);
}

// ========================================================================
// Tracked<List> — exercises Sink<Vec<T>> through generic wrapper
// ========================================================================

#[test]
fn test_tracked_list_in_tuple() {
    let log: Log = Rc::new(RefCell::new(Vec::new()));
    let mut dist = make_tracked_list_in_tuple(&log);
    let (scalars, list_sink) = dist.collect_flat(
        (Vec::<i32>::new(), VecListSink::<i64>::new()),
        &mut rng(),
        3,
    );
    let (values, offsets) = list_sink.into_parts();

    assert_eq!(scalars, vec![42, 42, 42]);
    assert_eq!(values, vec![0, 1, 0, 1, 0, 1]);
    assert_eq!(offsets, vec![0, 2, 4, 6]);
    // Col-by-col: COL1 x3, then LIST x3
    assert_eq!(
        *log.borrow(),
        vec!["COL1", "COL1", "COL1", "LIST", "LIST", "LIST"]
    );
}
