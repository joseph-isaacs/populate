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
    let result = dist.collect_col(&mut rng(), 3);
    // 3 lists: [], [0], [0, 1]
    assert_eq!(result.inner.0, vec![0, 0, 1]); // flat values
    assert_eq!(result.offsets, vec![0, 0, 1, 3]); // offsets
}

#[test]
fn test_list_in_tuple() {
    let mut dist = make_list_in_tuple::<VecBackend>();
    let (scalars, lists) = dist.collect_flat(&mut rng(), 3);

    assert_eq!(scalars, vec![42, 42, 42]);
    // 3 lists, each [0, 1]
    assert_eq!(lists.inner.0, vec![0, 1, 0, 1, 0, 1]); // flat values
    assert_eq!(lists.offsets, vec![0, 2, 4, 6]); // offsets
}

// ========================================================================
// Tracked<List> — exercises Sink<Vec<T>> through generic wrapper
// ========================================================================

#[test]
fn test_tracked_list_in_tuple() {
    let log: Log = Log::default();
    let mut dist = make_tracked_list_in_tuple::<VecBackend>(&log);
    let (scalars, lists) = dist.collect_flat(&mut rng(), 3);

    assert_eq!(scalars, vec![42, 42, 42]);
    assert_eq!(lists.inner.0, vec![0, 1, 0, 1, 0, 1]);
    assert_eq!(lists.offsets, vec![0, 2, 4, 6]);
    // Col-by-col: COL1 x3, then LIST x3
    assert_eq!(
        *log.borrow(),
        vec!["COL1", "COL1", "COL1", "LIST", "LIST", "LIST"]
    );
}

// ========================================================================
// List of struct — decomposed into separate columnar buffers
// ========================================================================

#[test]
fn test_list_of_struct_col_by_col() {
    // list<(u32, u64)> with 3 elements per list, 2 lists
    // Elements: (0, 100), (1, 110), (2, 120), (0, 100), (1, 110), (2, 120)
    let mut dist = make_list_of_struct::<VecBackend>();
    let result = dist.collect_col(&mut rng(), 2);

    // Inner has two flat buffers — one per struct field — col-by-col!
    let col1 = &result.inner.0;
    let col2 = &result.inner.1.0;

    // Each list has 3 elements, 2 lists total = 6 elements per column
    assert_eq!(col1, &vec![0, 1, 2, 0, 1, 2]); // u32 column: flat
    assert_eq!(col2, &vec![100, 110, 120, 100, 110, 120]); // u64 column: flat
    assert_eq!(result.offsets, vec![0, 3, 6]); // shared offsets
}

#[test]
fn test_list_of_struct_in_tuple() {
    // (i32, list<(u32, u64)>) — scalar column + struct-inside-list
    let mut dist = make_list_of_struct_in_tuple::<VecBackend>();
    let (scalars, lists) = dist.collect_flat(&mut rng(), 3);

    assert_eq!(scalars, vec![42, 42, 42]);

    // 3 lists, each with 2 elements: (0, 100), (1, 110)
    let col1 = &lists.inner.0;
    let col2 = &lists.inner.1.0;
    assert_eq!(col1, &vec![0, 1, 0, 1, 0, 1]); // u32 column
    assert_eq!(col2, &vec![100, 110, 100, 110, 100, 110]); // u64 column
    assert_eq!(lists.offsets, vec![0, 2, 4, 6]); // shared offsets
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
    // The List distribution generates elements via row_iter(), so within
    // each list the elements are produced row-by-row: (u32, u64) pairs.
    // The sink decomposes each pair into separate buffers.
    let log: Log = Log::default();
    let mut dist = make_tracked_list_of_struct::<VecBackend>(&log);
    let result = dist.collect_col(&mut rng(), 3);

    // Values are correct — decomposed into two flat columns
    assert_eq!(result.inner.0, vec![0, 1, 0, 1, 0, 1]); // u32 column
    assert_eq!(result.inner.1.0, vec![100, 110, 100, 110, 100, 110]); // u64 column
    assert_eq!(result.offsets, vec![0, 2, 4, 6]); // offsets

    // Element generation order: within each list, elements are generated
    // row-by-row (u32 then u64 for each element), across 3 lists of 2 elements
    assert_eq!(
        *log.borrow(),
        vec![
            // List 0: 2 elements, each (u32, u64) pair
            "ELEM_U32", "ELEM_U64", "ELEM_U32", "ELEM_U64", // List 1: 2 elements
            "ELEM_U32", "ELEM_U64", "ELEM_U32", "ELEM_U64", // List 2: 2 elements
            "ELEM_U32", "ELEM_U64", "ELEM_U32", "ELEM_U64",
        ]
    );
}

#[test]
fn test_tracked_list_of_struct_in_tuple_order() {
    // (i32, list<(u32, u64)>) — scalar is col-by-col, then list elements
    let log: Log = Log::default();
    let mut dist = make_tracked_list_of_struct_in_tuple::<VecBackend>(&log);
    let (scalars, lists) = dist.collect_flat(&mut rng(), 2);

    assert_eq!(scalars, vec![42, 42]);
    assert_eq!(lists.inner.0, vec![0, 1, 0, 1]); // u32 column
    assert_eq!(lists.inner.1.0, vec![100, 110, 100, 110]); // u64 column
    assert_eq!(lists.offsets, vec![0, 2, 4]); // offsets

    // Col-by-col at top level: SCALAR x2 first, then list elements
    assert_eq!(
        *log.borrow(),
        vec![
            // Scalar column: col-by-col
            "SCALAR", "SCALAR",
            // List column: element generation (row-by-row within each list)
            "ELEM_U32", "ELEM_U64", "ELEM_U32", "ELEM_U64", "ELEM_U32", "ELEM_U64", "ELEM_U32",
            "ELEM_U64",
        ]
    );
}
