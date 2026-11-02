// ========================================================================
// List tick tests — observe *when* each leaf is sampled via `Tracked`.
//
// Columnar contract for `list(lengths, elements)` collecting n rows:
//   1. lengths ticked n times (one column)
//   2. elements ticked Σlen times (one column, or col-by-col if a tuple)
// Nothing interleaves unless something forces row mode.
// ========================================================================

use crate::test::Log;
use crate::test::dist::{
    make_list_wrapped_in_tracked, make_tracked_empty_list, make_tracked_list,
    make_tracked_list_between_cols, make_tracked_list_from_1, make_tracked_list_in_tuple,
    make_tracked_list_len2, make_tracked_list_of_tuples, make_tracked_map_to_list,
    make_tracked_nested_list,
};
use crate::traits::{
    DequeBackend, Distribution, DistributionExt, DistributionScalar, HCons, HNil, ListCol,
    SampleIter,
};
use rand::rng;
use std::collections::VecDeque;

type Col<T> = HCons<Vec<T>, HNil>;
type VecList<V> = ListCol<Vec<usize>, V>;

fn ticks(log: &Log) -> Vec<&'static str> {
    log.borrow().clone()
}

/// Run-length encode a tick log: ["A","A","B"] -> [("A",2),("B",1)].
/// Makes long logs readable and lets assertions state the *shape*.
fn runs(log: &Log) -> Vec<(&'static str, usize)> {
    let mut out: Vec<(&'static str, usize)> = Vec::new();
    for &t in log.borrow().iter() {
        match out.last_mut() {
            Some((last, n)) if *last == t => *n += 1,
            _ => out.push((t, 1)),
        }
    }
    out
}

#[test]
fn lengths_column_then_elements_column() {
    let log = Log::default();
    let mut d = make_tracked_list(&log);
    let c = d.collect_col(VecList::<Col<i64>>::default(), &mut rng(), 4);
    assert_eq!(c.offsets, vec![0, 1, 3, 6]);
    // 4 lengths first, then all 6 elements in one go
    assert_eq!(runs(&log), vec![("LEN", 4), ("E", 6)]);
}

#[test]
fn empty_lists_never_tick_elements() {
    let log = Log::default();
    let mut d = make_tracked_empty_list(&log);
    let c = d.collect_col(VecList::<Col<i64>>::default(), &mut rng(), 3);
    assert_eq!(c.offsets, vec![0, 0, 0]);
    assert!(c.values.0.is_empty());
    assert_eq!(runs(&log), vec![("LEN", 3)]);
}

#[test]
fn list_between_scalar_columns() {
    let log = Log::default();
    let mut d = make_tracked_list_between_cols(&log);
    let (c1, l, c3) = d.collect_flat(
        (Vec::new(), VecList::<Col<i64>>::default(), Vec::new()),
        &mut rng(),
        3,
    );
    assert_eq!((c1, c3), (vec![0, 1, 2], vec![0, 1, 2]));
    assert_eq!(l.values.0, vec![0, 1, 2, 3, 4, 5]);
    assert_eq!(runs(&log), vec![("C1", 3), ("LEN", 3), ("E", 6), ("C3", 3)]);
}

#[test]
fn list_of_tuple_elements_are_col_by_col() {
    let log = Log::default();
    let mut d = make_tracked_list_of_tuples(&log);
    let c = d.collect_col(
        VecList::<HCons<Vec<i64>, HCons<Vec<i32>, Col<bool>>>>::default(),
        &mut rng(),
        2,
    );
    assert_eq!(c.offsets, vec![3, 6]);
    assert_eq!(c.values.0, vec![0, 1, 2, 3, 4, 5]);
    assert_eq!(c.values.1.0, vec![0, 1, 2, 3, 4, 5]);
    assert_eq!(c.values.1.1.0, vec![true; 6]);
    // A for every element of every row, then B, then C — never A,B,C,A,B,C
    assert_eq!(runs(&log), vec![("LEN", 2), ("A", 6), ("B", 6), ("C", 6)]);
}

#[test]
fn nested_list_is_level_by_level() {
    let log = Log::default();
    let mut d = make_tracked_nested_list(&log);
    let c = d.collect_col(
        VecList::<HCons<VecList<Col<i64>>, HNil>>::default(),
        &mut rng(),
        3,
    );
    // 3 outer rows x 2 = 6 inner lists of len 1..=6 => 21 elements
    assert_eq!(c.offsets, vec![2, 4, 6]);
    assert_eq!(c.values.0.offsets, vec![1, 3, 6, 10, 15, 21]);
    assert_eq!(runs(&log), vec![("OUTER", 3), ("INNER", 6), ("E", 21)]);
}

#[test]
fn row_iter_interleaves_lengths_and_elements() {
    let log = Log::default();
    let mut d = make_tracked_list_from_1(&log);
    let mut it = d.row_iter();
    let mut r = rng();
    let rows: Vec<_> = (0..3).map(|_| it.next(&mut r)).collect();
    assert_eq!(rows, vec![vec![0], vec![1, 2], vec![3, 4, 5]]);
    // Row mode: same values as columnar, but ticks interleave per row
    assert_eq!(
        ticks(&log),
        vec!["LEN", "E", "LEN", "E", "E", "LEN", "E", "E", "E"]
    );
}

/// `Tracked` (like `Map`) implements `collect_into` by pulling rows from
/// `row_iter`, so wrapping a list in it forces the whole subtree into row
/// mode. Same output, different tick order — a wrapper must forward
/// `collect_into` to stay columnar.
#[test]
fn row_wrapper_forces_subtree_into_row_mode() {
    let log = Log::default();
    let mut d = make_list_wrapped_in_tracked(&log);
    let c = d.collect_col(VecList::<Col<i64>>::default(), &mut rng(), 2);
    assert_eq!(c.offsets, vec![2, 4]);
    assert_eq!(c.values.0, vec![0, 1, 2, 3]);
    assert_eq!(
        ticks(&log),
        vec!["LIST", "LEN", "E", "E", "LIST", "LEN", "E", "E"]
    );
}

#[test]
fn map_returning_list_is_row_mode_into_columnar_sink() {
    let log = Log::default();
    let mut d = make_tracked_map_to_list(&log);
    let c = d.collect_col(VecList::<Col<i64>>::default(), &mut rng(), 3);
    assert_eq!(c.offsets, vec![1, 3, 6]);
    assert_eq!(c.values.0, vec![0, 0, 1, 0, 1, 2]);
    assert_eq!(runs(&log), vec![("SRC", 3)]);
}

#[test]
fn backend_does_not_change_tick_order() {
    let log = Log::default();
    let mut d = make_tracked_list_in_tuple::<DequeBackend>(&log);
    let (c1, l) = d.collect_flat(
        (
            VecDeque::new(),
            ListCol::<VecDeque<usize>, HCons<VecDeque<i64>, HNil>>::default(),
        ),
        &mut rng(),
        3,
    );
    assert_eq!(c1, VecDeque::from([7, 7, 7]));
    assert_eq!(l.offsets, VecDeque::from([2, 4, 6]));
    assert_eq!(l.values.0, VecDeque::from([0, 1, 2, 3, 4, 5]));
    assert_eq!(runs(&log), vec![("C1", 3), ("LEN", 3), ("E", 6)]);
}

/// Collecting twice into the same sink appends: offsets continue from the
/// sink's running `len`. Each `collect_into` starts a fresh `row_iter`, so
/// stateful leaves (Range) restart per call.
#[test]
fn second_collect_appends_to_existing_sink() {
    let log = Log::default();
    let mut d = make_tracked_list_len2(&log);
    let sink = VecList::<Col<i64>>::default();
    let sink = d.collect_col(sink, &mut rng(), 2);
    let sink = d.collect_col(sink, &mut rng(), 1);
    assert_eq!(sink.offsets, vec![2, 4, 6]);
    assert_eq!(sink.len, 6);
    assert_eq!(sink.values.0, vec![0, 1, 2, 3, 0, 1]);
    assert_eq!(runs(&log), vec![("LEN", 2), ("E", 4), ("LEN", 1), ("E", 2)]);
}
