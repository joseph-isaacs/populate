// ========================================================================
// Struct <-> list nesting. Recipe for writing one of these tests:
//
//   1. Helper in test/dist.rs returning `impl Distribution<Item = T>`.
//   2. The collected columns mirror T:
//        scalar  T      -> Vec<T>
//        struct (A, B)  -> (columns(A), columns(B))
//        list   Vec<T>  -> ListCol<columns(T)>   (offsets + T's columns)
//   3. Assert offsets per list level + every leaf column.
// ========================================================================

use crate::Distribution;
use crate::test::Log;
use crate::test::dist::{
    gen_list_of_struct_of_list, gen_struct_of_list, gen_struct_of_list_of_struct,
    make_tracked_list_of_struct_of_list,
};
use rand::rng;

#[test]
fn struct_of_list() {
    let mut d = gen_struct_of_list();
    let (ids, tags) = d.collect(&mut rng(), 4);
    assert_eq!(ids, vec![0, 1, 2, 3]);
    assert_eq!(tags.offsets(), [0, 0, 1, 3, 6]);
    assert_eq!(tags.values, vec![0, 1, 2, 3, 4, 5]);
}

#[test]
fn struct_of_list_of_struct() {
    let mut d = gen_struct_of_list_of_struct();
    let (ids, items) = d.collect(&mut rng(), 3);
    assert_eq!(ids, vec![0, 1, 2]);
    assert_eq!(items.offsets(), [0, 2, 4, 6]);
    assert_eq!(items.values.0, vec![0, 1, 2, 3, 4, 5]); // qty column
    assert_eq!(items.values.1, vec![true; 6]); // ok column
}

#[test]
fn list_of_struct_of_list() {
    let mut d = gen_list_of_struct_of_list();
    let c = d.collect(&mut rng(), 2);

    // rows: [ [{0,[0]}, {1,[1,2]}], [{2,[3,4,5]}, {3,[6,7,8,9]}] ]
    assert_eq!(c.offsets(), [0, 2, 4]); // 2 structs per row
    let (ids, tags) = &c.values;
    assert_eq!(*ids, vec![0, 1, 2, 3]); // one id per struct
    assert_eq!(tags.offsets(), [0, 1, 3, 6, 10]); // tag lens 1,2,3,4
    assert_eq!(tags.values, (0..10).collect::<Vec<i64>>());
}

#[test]
fn list_of_struct_of_list_tick_order() {
    let log = Log::default();
    let mut d = make_tracked_list_of_struct_of_list(&log);
    let c = d.collect(&mut rng(), 2);

    assert_eq!(c.offsets(), [0, 2, 4]);
    assert_eq!(c.values.0, vec![0, 1, 2, 3]);
    assert_eq!(c.values.1.offsets(), [0, 1, 3, 6, 10]);
    assert_eq!(c.values.1.values, (0..10).collect::<Vec<i64>>());

    // outer lens (2 rows) -> id column (4 structs) -> tag lens (4) -> tags (10)
    let runs: Vec<_> = log
        .borrow()
        .chunk_by(|a, b| a == b)
        .map(|r| (r[0], r.len()))
        .collect();
    assert_eq!(
        runs,
        vec![("OUTER", 2), ("ID", 4), ("TLEN", 4), ("TAG", 10)]
    );
}
