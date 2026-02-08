// ========================================================================
// List tests
// ========================================================================

use crate::test::Log;
use crate::test::dist::{
    gen_list, make_list_in_tuple, make_list_of_struct, make_list_of_struct_in_tuple,
    make_tracked_list_in_tuple, make_tracked_list_of_struct, make_tracked_list_of_struct_in_tuple,
};
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
    let ((values,), offsets) = dist.collect_col(&mut rng(), 3).into_parts();
    // 3 lists: [], [0], [0, 1]
    assert_eq!(values, vec![0, 0, 1]);
    assert_eq!(offsets, vec![0, 0, 1, 3]);
}

#[test]
fn test_list_in_tuple() {
    let mut dist = make_list_in_tuple::<VecBackend>();
    let (scalars, lists) = dist.collect_flat(&mut rng(), 3);
    let ((values,), offsets) = lists.into_parts();

    assert_eq!(scalars, vec![42, 42, 42]);
    // 3 lists, each [0, 1]
    assert_eq!(values, vec![0, 1, 0, 1, 0, 1]);
    assert_eq!(offsets, vec![0, 2, 4, 6]);
}

// ========================================================================
// Tracked<List> — exercises Sink<Vec<T>> through generic wrapper
// ========================================================================

#[test]
fn test_tracked_list_in_tuple() {
    let log: Log = Log::default();
    let mut dist = make_tracked_list_in_tuple::<VecBackend>(&log);
    let (scalars, lists) = dist.collect_flat(&mut rng(), 3);
    let ((values,), offsets) = lists.into_parts();

    assert_eq!(scalars, vec![42, 42, 42]);
    assert_eq!(values, vec![0, 1, 0, 1, 0, 1]);
    assert_eq!(offsets, vec![0, 2, 4, 6]);
    // Row-by-row at tuple level: COL1, LIST alternating
    assert_eq!(
        *log.borrow(),
        vec!["COL1", "LIST", "COL1", "LIST", "COL1", "LIST"]
    );
}

// ========================================================================
// List of struct — decomposed into separate columnar buffers
// ========================================================================

#[test]
fn test_list_of_struct_col_by_col() {
    // list<(u32, u64)> with 3 elements per list, 2 lists
    let mut dist = make_list_of_struct::<VecBackend>();
    let ((col1, col2), offsets) = dist.collect_col(&mut rng(), 2).into_parts();

    // Each list has 3 elements, 2 lists total = 6 elements per column
    assert_eq!(col1, vec![0, 1, 2, 0, 1, 2]); // u32 column: flat
    assert_eq!(col2, vec![100, 110, 120, 100, 110, 120]); // u64 column: flat
    assert_eq!(offsets, vec![0, 3, 6]); // shared offsets
}

#[test]
fn test_list_of_struct_in_tuple() {
    // (i32, list<(u32, u64)>) — scalar column + struct-inside-list
    let mut dist = make_list_of_struct_in_tuple::<VecBackend>();
    let (scalars, lists) = dist.collect_flat(&mut rng(), 3);
    let ((col1, col2), offsets) = lists.into_parts();

    assert_eq!(scalars, vec![42, 42, 42]);
    assert_eq!(col1, vec![0, 1, 0, 1, 0, 1]); // u32 column
    assert_eq!(col2, vec![100, 110, 100, 110, 100, 110]); // u64 column
    assert_eq!(offsets, vec![0, 2, 4, 6]); // shared offsets
}

#[test]
fn test_list_of_struct_row_iter() {
    // Row iterator still produces Vec<(u32, u64)>
    let mut dist = make_list_of_struct::<VecBackend>();
    let mut iter = dist.row_iter();
    let mut rng = rng();

    let row0 = iter.next(&mut rng);
    assert_eq!(row0, vec![(0, 100), (1, 110), (2, 120)]);
    let row1 = iter.next(&mut rng);
    assert_eq!(row1, vec![(0, 100), (1, 110), (2, 120)]);
}

// ========================================================================
// Tracked list-of-struct — verify element push order
// ========================================================================

#[test]
fn test_tracked_list_of_struct_element_order() {
    // list<(u32, u64)> with 2 elements per list, 3 lists
    let log: Log = Log::default();
    let mut dist = make_tracked_list_of_struct::<VecBackend>(&log);
    let ((col1, col2), offsets) = dist.collect_col(&mut rng(), 3).into_parts();

    assert_eq!(col1, vec![0, 1, 0, 1, 0, 1]); // u32 column
    assert_eq!(col2, vec![100, 110, 100, 110, 100, 110]); // u64 column
    assert_eq!(offsets, vec![0, 2, 4, 6]);

    // Element generation order: within each list, elements are generated
    // row-by-row (u32, u64 per element), across 3 lists of 2 elements
    assert_eq!(
        *log.borrow(),
        vec![
            "ELEM_U32", "ELEM_U64", "ELEM_U32", "ELEM_U64", // list 0: row-by-row
            "ELEM_U32", "ELEM_U64", "ELEM_U32", "ELEM_U64", // list 1
            "ELEM_U32", "ELEM_U64", "ELEM_U32", "ELEM_U64", // list 2
        ]
    );
}

#[test]
fn test_tracked_list_of_struct_in_tuple_order() {
    // (i32, list<(u32, u64)>) — scalar is col-by-col, then list elements
    let log: Log = Log::default();
    let mut dist = make_tracked_list_of_struct_in_tuple::<VecBackend>(&log);
    let (scalars, lists) = dist.collect_flat(&mut rng(), 2);
    let ((col1, col2), offsets) = lists.into_parts();

    assert_eq!(scalars, vec![42, 42]);
    assert_eq!(col1, vec![0, 1, 0, 1]); // u32 column
    assert_eq!(col2, vec![100, 110, 100, 110]); // u64 column
    assert_eq!(offsets, vec![0, 2, 4]);

    // Row-by-row at top level: SCALAR and list interleaved, elements row-by-row
    assert_eq!(
        *log.borrow(),
        vec![
            "SCALAR", "ELEM_U32", "ELEM_U64", "ELEM_U32",
            "ELEM_U64", // row 0: scalar + list elements
            "SCALAR", "ELEM_U32", "ELEM_U64", "ELEM_U32", "ELEM_U64", // row 1
        ]
    );
}
