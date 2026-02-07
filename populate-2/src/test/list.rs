// ========================================================================
// List tests
// ========================================================================

use crate::test::Log;
use crate::test::dist::{
    gen_list, gen_struct_list, make_list_in_tuple, make_scalar_list_scalar,
    make_struct_list_in_tuple, make_tracked_list_in_tuple, make_tracked_struct_list,
    make_two_tracked_lists,
};
use crate::traits::{
    Distribution, DistributionExt, DistributionScalar, ListSink, SampleIter, Sink, VecBackend,
    VecListSink,
};
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

// ========================================================================
// Columnar flat layout — VecListSink correctness
// ========================================================================

/// Verify that VecListSink stores variable-length lists as a flat values
/// buffer plus offsets, and that the original lists can be reconstructed
/// by slicing values[offsets[i]..offsets[i+1]].
///
/// This is the core invariant that ColumnarList must preserve.
#[test]
fn test_vec_list_sink_columnar_roundtrip() {
    let mut sink = VecListSink::<i64>::new();

    let lists: Vec<Vec<i64>> = vec![vec![], vec![10, 20], vec![30], vec![40, 50, 60]];

    for l in &lists {
        sink.push_list(l.len(), l.iter().copied());
    }

    let (values, offsets) = sink.into_parts();

    // All values in one contiguous flat buffer
    assert_eq!(values, vec![10, 20, 30, 40, 50, 60]);
    // Offsets: initial 0 + cumulative end position of each list
    assert_eq!(offsets, vec![0, 0, 2, 3, 6]);

    // Reconstruct: list[i] = values[offsets[i]..offsets[i+1]]
    let reconstructed: Vec<Vec<i64>> = offsets
        .windows(2)
        .map(|w| values[w[0]..w[1]].to_vec())
        .collect();
    assert_eq!(reconstructed, lists);
}

/// The Sink<Vec<T>>::push bridge and ListSink::push_list must produce
/// identical flat layouts — both are entry points used by different
/// collection paths (row-by-row vs col-by-col).
#[test]
fn test_vec_list_sink_push_vs_push_list() {
    let lists: Vec<Vec<i64>> = vec![vec![], vec![10, 20], vec![30]];

    // Path A: ListSink::push_list (used by List::collect_into directly)
    let mut via_push_list = VecListSink::<i64>::new();
    for l in &lists {
        via_push_list.push_list(l.len(), l.iter().copied());
    }

    // Path B: Sink<Vec<T>>::push (used by Tracked/Map row-by-row path)
    let mut via_push = VecListSink::<i64>::new();
    for l in &lists {
        Sink::push(&mut via_push, l.clone());
    }

    assert_eq!(via_push_list.into_parts(), via_push.into_parts());
}

// ========================================================================
// Col-by-col iteration order with multiple list columns
// ========================================================================

/// Two list columns in a tuple: verify col-by-col ordering and that
/// each list's element iterator resets independently per row.
#[test]
fn test_two_lists_col_by_col_order() {
    let log: Log = Log::default();
    let mut dist = make_two_tracked_lists::<VecBackend>(&log);

    let (col_a, col_b) = dist.collect_flat_into(
        (Vec::<Vec<i64>>::new(), Vec::<Vec<i64>>::new()),
        &mut rng(),
        3,
    );

    // LIST_A: 1 element per list, element iter clones from base (starts at 0)
    assert_eq!(col_a, vec![vec![0], vec![0], vec![0]]);
    // LIST_B: 2 elements per list, element iter clones from base (starts at 100)
    assert_eq!(col_b, vec![vec![100, 101], vec![100, 101], vec![100, 101]]);

    // Col-by-col: all LIST_A rows fill first, then all LIST_B rows
    assert_eq!(
        *log.borrow(),
        vec!["LIST_A", "LIST_A", "LIST_A", "LIST_B", "LIST_B", "LIST_B"]
    );
}

/// Scalar-list-scalar interleaving: verify that col-by-col fills each
/// column completely before moving to the next, even when list columns
/// appear between scalar columns.
#[test]
fn test_scalar_list_scalar_col_order() {
    let log: Log = Log::default();
    let mut dist = make_scalar_list_scalar::<VecBackend>(&log);

    let (scalars_a, lists, scalars_b) = dist.collect_flat_into(
        (Vec::<i32>::new(), Vec::<Vec<i64>>::new(), Vec::<i32>::new()),
        &mut rng(),
        3,
    );

    assert_eq!(scalars_a, vec![1, 1, 1]);
    assert_eq!(lists, vec![vec![0, 1], vec![0, 1], vec![0, 1]]);
    assert_eq!(scalars_b, vec![0, 1, 2]);

    // Col-by-col: SCALAR_A x3, then LIST x3, then SCALAR_B x3
    assert_eq!(
        *log.borrow(),
        vec![
            "SCALAR_A", "SCALAR_A", "SCALAR_A", "LIST", "LIST", "LIST", "SCALAR_B", "SCALAR_B",
            "SCALAR_B",
        ]
    );
}

// ========================================================================
// Struct-inside-list: list<(u32, u16)>
// ========================================================================

/// list<(u32, u16)> via row_iter: each list's elements are (u32, u16)
/// tuples with element iterators cloned fresh per list, so elements
/// always restart from the same base values.
#[test]
fn test_struct_list_row_iter() {
    let mut dist = gen_struct_list::<VecBackend>();
    let mut iter = dist.row_iter();
    let mut rng = rng();

    // lengths: 0, 1, 2, 3  (from range(0usize, 1))
    // elements: (range(0u32, 1), range(0u16, 1)) → (0,0), (1,1), (2,2), ...
    // element iter resets per list
    let r0 = iter.next(&mut rng);
    let r1 = iter.next(&mut rng);
    let r2 = iter.next(&mut rng);
    let r3 = iter.next(&mut rng);

    assert_eq!(r0, vec![]);
    assert_eq!(r1, vec![(0u32, 0u16)]);
    assert_eq!(r2, vec![(0, 0), (1, 1)]);
    assert_eq!(r3, vec![(0, 0), (1, 1), (2, 2)]);
}

/// list<(u32, u16)> via collect_col: same values, collected into
/// Vec<Vec<(u32, u16)>> sink.
#[test]
fn test_struct_list_collect_col() {
    let mut dist = gen_struct_list::<VecBackend>();
    let result = dist.collect_col(Vec::<Vec<(u32, u16)>>::new(), &mut rng(), 4);

    assert_eq!(
        result,
        vec![
            vec![],
            vec![(0, 0)],
            vec![(0, 0), (1, 1)],
            vec![(0, 0), (1, 1), (2, 2)],
        ]
    );
}

/// Scalar column + list<(u32, u16)> column: verify col-by-col fills
/// the scalar completely, then the struct-lists, and that tuple elements
/// within each list are correct.
#[test]
fn test_struct_list_in_tuple() {
    let mut dist = make_struct_list_in_tuple::<VecBackend>();
    let (scalars, lists) = dist.collect_flat_into(
        (Vec::<i32>::new(), Vec::<Vec<(u32, u16)>>::new()),
        &mut rng(),
        3,
    );

    assert_eq!(scalars, vec![42, 42, 42]);
    // Each list has 3 elements (constant(3usize))
    // u32: range(0, 10) → 0, 10, 20  (step 10, resets per list)
    // u16: range(0, 1)  → 0, 1, 2    (step 1, resets per list)
    assert_eq!(
        lists,
        vec![
            vec![(0, 0), (10, 1), (20, 2)],
            vec![(0, 0), (10, 1), (20, 2)],
            vec![(0, 0), (10, 1), (20, 2)],
        ]
    );
}

/// Verify element generation order INSIDE a list<(u32, u16)>.
///
/// List::collect_into uses row_iter() + clone for elements, so the tuple
/// distribution's col-by-col collect_into is never called. Elements are
/// generated interleaved: U32, U16, U32, U16, ... per list — NOT
/// all U32s first then all U16s.
#[test]
fn test_struct_list_element_order_is_row_by_row() {
    let log: Log = Log::default();
    let mut dist = make_tracked_struct_list::<VecBackend>(&log);
    let result = dist.collect_col(Vec::<Vec<(u32, u16)>>::new(), &mut rng(), 2);

    // constant(3usize) → each list has 3 elements
    assert_eq!(result[0], vec![(0, 0), (1, 1), (2, 2)]);
    assert_eq!(result[1], vec![(0, 0), (1, 1), (2, 2)]);

    // Key: within each list, elements are generated row-by-row (interleaved),
    // NOT col-by-col. Each (u32, u16) pair is generated together.
    assert_eq!(
        *log.borrow(),
        vec![
            // List 0: 3 elements, each generates U32 then U16
            "U32", "U16", "U32", "U16", "U32", "U16", // List 1: same pattern
            "U32", "U16", "U32", "U16", "U32", "U16",
        ]
    );
}
