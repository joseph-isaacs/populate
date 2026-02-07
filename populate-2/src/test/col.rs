// ========================================================================
// Col-by-col collection tests (via collect_flat — no HCons visible!)
// ========================================================================

use crate::test::Log;
use crate::test::dist::{
    collect, gen_col4, make_3tuple, make_4tuple, make_flatten_order, make_map_in_tuple,
    make_nested_tuple, make_row_order, make_tracked_2tuple, make_tracked_3tuple,
    make_tracked_4tuple, make_tuple,
};
use crate::traits::{DistributionExt, VecBackend};
use rand::rng;
use std::cell::RefCell;
use std::rc::Rc;

#[test]
fn test_col_by_col_basic() {
    let mut dist = make_tuple::<VecBackend>();
    let (col1, col2) = dist.collect_flat((Vec::<i32>::new(), Vec::<i64>::new()), &mut rng(), 4);
    assert_eq!(col1, vec![1, 1, 1, 1]);
    assert_eq!(col2, vec![0, 10, 20, 30]);
}

#[test]
fn test_col_by_col_order() {
    let log: Log = Log::default();
    let mut dist = make_tracked_2tuple(&log);
    let (col1, col2) = dist.collect_flat((Vec::<i64>::new(), Vec::<i64>::new()), &mut rng(), 3);

    assert_eq!(col1, vec![0, 1, 2]);
    assert_eq!(col2, vec![100, 101, 102]);
    // Col-by-col: COL1, COL1, COL1, COL2, COL2, COL2
    assert_eq!(
        *log.borrow(),
        vec!["COL1", "COL1", "COL1", "COL2", "COL2", "COL2"]
    );
}

// ========================================================================
// 3-tuple tests
// ========================================================================

#[test]
fn test_3tuple_col_by_col() {
    let mut dist = make_3tuple();
    let (col1, col2, col3) = dist.collect_flat(
        (Vec::<i32>::new(), Vec::<i64>::new(), Vec::<bool>::new()),
        &mut rng(),
        3,
    );
    assert_eq!(col1, vec![1, 1, 1]);
    assert_eq!(col2, vec![0, 10, 20]);
    assert_eq!(col3, vec![true, true, true]);
}

#[test]
fn test_3tuple_col_by_col_order() {
    let log: Log = Rc::new(RefCell::new(Vec::new()));
    let mut dist = make_tracked_3tuple(&log);
    let (col1, col2, col3) = dist.collect_flat(
        (Vec::<i64>::new(), Vec::<i64>::new(), Vec::<i32>::new()),
        &mut rng(),
        2,
    );
    assert_eq!(col1, vec![0, 1]);
    assert_eq!(col2, vec![100, 101]);
    assert_eq!(col3, vec![1000, 1001]);
    assert_eq!(
        *log.borrow(),
        vec!["COL1", "COL1", "COL2", "COL2", "COL3", "COL3"]
    );
}

// ========================================================================
// 4-tuple tests
// ========================================================================

#[test]
fn test_4tuple_col_by_col() {
    let mut dist = make_4tuple();
    let (c1, c2, c3, c4) = dist.collect_flat(
        (
            Vec::<i32>::new(),
            Vec::<i64>::new(),
            Vec::<bool>::new(),
            Vec::<f64>::new(),
        ),
        &mut rng(),
        3,
    );
    assert_eq!(c1, vec![1, 1, 1]);
    assert_eq!(c2, vec![0, 10, 20]);
    assert_eq!(c3, vec![true, true, true]);
    assert_eq!(c4, vec![2.5, 2.5, 2.5]);
}

#[test]
fn test_4tuple_col_by_col_order() {
    let log: Log = Rc::new(RefCell::new(Vec::new()));
    let mut dist = make_tracked_4tuple(&log);
    let (c1, c2, c3, c4) = dist.collect_flat(
        (
            Vec::<i64>::new(),
            Vec::<i64>::new(),
            Vec::<i64>::new(),
            Vec::<i64>::new(),
        ),
        &mut rng(),
        2,
    );
    assert_eq!(c1, vec![0, 1]);
    assert_eq!(c2, vec![100, 101]);
    assert_eq!(c3, vec![1000, 1001]);
    assert_eq!(c4, vec![10000, 10001]);
    assert_eq!(
        *log.borrow(),
        vec![
            "COL1", "COL1", "COL2", "COL2", "COL3", "COL3", "COL4", "COL4"
        ]
    );
}

// ========================================================================
// Map in tuple tests
// ========================================================================

#[test]
fn test_map_in_tuple_values() {
    let log: Log = Rc::new(RefCell::new(Vec::new()));
    let mut dist = make_map_in_tuple(&log);
    let (c1, c2, c3) = dist.collect_flat(
        (Vec::<i64>::new(), Vec::<i64>::new(), Vec::<i64>::new()),
        &mut rng(),
        3,
    );

    assert_eq!(c1, vec![0, 1, 2]);
    assert_eq!(c2, vec![200, 202, 204]);
    assert_eq!(c3, vec![1000, 1001, 1002]);
}

#[test]
fn test_map_in_tuple_order() {
    let log: Log = Rc::new(RefCell::new(Vec::new()));
    let mut dist = make_map_in_tuple(&log);
    let (c1, c2, c3) = dist.collect_flat(
        (Vec::<i64>::new(), Vec::<i64>::new(), Vec::<i64>::new()),
        &mut rng(),
        3,
    );

    assert_eq!(c1, vec![0, 1, 2]);
    assert_eq!(c2, vec![200, 202, 204]);
    assert_eq!(c3, vec![1000, 1001, 1002]);

    assert_eq!(
        *log.borrow(),
        vec![
            "COL1", "COL1", "COL1", // col-by-col
            "MAP_SRC", "MAP_SRC", "MAP_SRC", // map row-by-row
            "COL3", "COL3", "COL3" // col-by-col
        ]
    );
}

// ========================================================================
// AUTO-FLATTENING: Map producing tuple → flat columns, no Unnest needed!
// ========================================================================

#[test]
fn test_map_tuple_auto_flatten() {
    let log = Log::default();
    let mut dist = gen_col4::<VecBackend>(&log);

    let (c1, c2, c3, c4) = dist.collect_flat(
        (
            Vec::<i16>::new(),
            Vec::<i64>::new(),
            Vec::<i64>::new(),
            Vec::<i32>::new(),
        ),
        &mut rng(),
        3,
    );

    assert_eq!(c1, vec![0, 1, 2]); // COL1
    assert_eq!(c2, vec![1100, 1102, 1104]); // sum
    assert_eq!(c3, vec![100000, 101101, 102204]); // product
    assert_eq!(c4, vec![0, 5, 10]); // COL4
}

#[test]
fn test_map_tuple_auto_flatten_order() {
    let log: Log = Rc::new(RefCell::new(Vec::new()));
    let mut dist = make_flatten_order::<VecBackend>(&log);

    let (_, _, _, _) = dist.collect_flat(
        (
            Vec::<i32>::new(),
            Vec::<i32>::new(),
            Vec::<i32>::new(),
            Vec::<i32>::new(),
        ),
        &mut rng(),
        3,
    );

    // COL1 col-by-col, then Map row-by-row (MAP_A, MAP_B per row), then COL4 col-by-col
    assert_eq!(
        *log.borrow(),
        vec![
            "COL1", "COL1", "COL1", "MAP_A", "MAP_B", "MAP_A", "MAP_B", "MAP_A", "MAP_B", "COL4",
            "COL4", "COL4",
        ]
    );
}

// ========================================================================
// Independent tuple auto-flatten (previously needed Unnest)
// ========================================================================

#[test]
fn test_independent_tuple_auto_flatten() {
    let log: Log = Rc::new(RefCell::new(Vec::new()));
    let mut dist = make_nested_tuple(&log);

    let (c1, c2, c3, c4) = dist.collect_flat(
        (
            Vec::<i32>::new(),
            Vec::<i32>::new(),
            Vec::<i32>::new(),
            Vec::<i32>::new(),
        ),
        &mut rng(),
        3,
    );

    assert_eq!(c1, vec![0, 1, 2]);
    assert_eq!(c2, vec![100, 101, 102]);
    assert_eq!(c3, vec![1000, 1001, 1002]);
    assert_eq!(c4, vec![0, 5, 10]);

    // All col-by-col!
    assert_eq!(
        *log.borrow(),
        vec![
            "COL1", "COL1", "COL1", "COL2", "COL2", "COL2", "COL3", "COL3", "COL3", "COL4", "COL4",
            "COL4",
        ]
    );
}

// ========================================================================
// Row-by-row iteration order test
// ========================================================================

#[test]
fn test_row_order() {
    let log: Log = Rc::new(RefCell::new(Vec::new()));
    let mut dist = make_row_order(&log);
    let mut sink: (Vec<i64>, Vec<i64>) = (Vec::new(), Vec::new());

    collect(&mut dist, &mut sink, 3, &mut rng());

    assert_eq!(sink.0, vec![0, 1, 2]);
    assert_eq!(sink.1, vec![100, 101, 102]);
    // Row-by-row: COL1, COL2, COL1, COL2, COL1, COL2
    assert_eq!(
        *log.borrow(),
        vec!["COL1", "COL2", "COL1", "COL2", "COL1", "COL2"]
    );
}
