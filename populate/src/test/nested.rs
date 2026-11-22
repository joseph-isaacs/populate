// ========================================================================
// Struct <-> list nesting. Recipe for writing one of these tests:
//
//   1. Helper in test/dist.rs returning `impl Distribution<B, Item = T>`.
//   2. Build the sink type by mirroring T:
//        scalar  T      -> Vec<T>                    (VecBackend)
//        struct (A, B)  -> HCons<sink(A), HCons<sink(B), HNil>>, flattened:
//                          each field's columns are appended in order
//        list   Vec<T>  -> ListCol<Vec<usize>, sink(T)>   (offsets + T's columns)
//      At the top level, `collect_flat` takes a plain tuple instead of HCons.
//      Single list column: `collect_col(ListCol::default(), ..)`.
//   3. Assert offsets per list level + every leaf column.
// ========================================================================

use crate::test::Log;
use crate::test::dist::{
    gen_list_of_struct_of_list, gen_struct_of_list, gen_struct_of_list_of_struct,
    make_tracked_list_of_struct_of_list,
};
use crate::traits::{
    DequeBackend, DistributionExt, DistributionScalar, HCons, HNil, ListCol, VecBackend,
};
use rand::rng;
use std::collections::VecDeque;

type Col<T> = HCons<Vec<T>, HNil>;
type VecList<V> = ListCol<Vec<usize>, V>;

/// sink for list<{ id: i32, tags: list<i64> }>
///   offsets
///   └ values: [ id column, tags ListCol(offsets, tag column) ]
type ListOfStructOfList = VecList<HCons<Vec<i32>, HCons<VecList<Col<i64>>, HNil>>>;

#[test]
fn struct_of_list() {
    // (i32, Vec<i64>) at top level -> plain tuple of sinks
    let mut d = gen_struct_of_list::<VecBackend>();
    let (ids, tags) = d.collect_flat((Vec::new(), VecList::<Col<i64>>::default()), &mut rng(), 4);
    assert_eq!(ids, vec![0, 1, 2, 3]);
    assert_eq!(tags.offsets, vec![0, 1, 3, 6]);
    assert_eq!(tags.values.0, vec![0, 1, 2, 3, 4, 5]);
}

#[test]
fn struct_of_list_of_struct() {
    let mut d = gen_struct_of_list_of_struct::<VecBackend>();
    let (ids, items) = d.collect_flat(
        (Vec::new(), VecList::<HCons<Vec<i64>, Col<bool>>>::default()),
        &mut rng(),
        3,
    );
    assert_eq!(ids, vec![0, 1, 2]);
    assert_eq!(items.offsets, vec![2, 4, 6]);
    assert_eq!(items.values.0, vec![0, 1, 2, 3, 4, 5]); // qty column
    assert_eq!(items.values.1.0, vec![true; 6]); // ok column
}

#[test]
fn list_of_struct_of_list() {
    let mut d = gen_list_of_struct_of_list::<VecBackend>();
    let c = d.collect_col(ListOfStructOfList::default(), &mut rng(), 2);

    // rows: [ [{0,[0]}, {1,[1,2]}], [{2,[3,4,5]}, {3,[6,7,8,9]}] ]
    assert_eq!(c.offsets, vec![2, 4]); // 2 structs per row
    let HCons(ids, HCons(tags, HNil)) = &c.values;
    assert_eq!(*ids, vec![0, 1, 2, 3]); // one id per struct
    assert_eq!(tags.offsets, vec![1, 3, 6, 10]); // tag lens 1,2,3,4
    assert_eq!(tags.values.0, (0..10).collect::<Vec<i64>>());
}

#[test]
fn list_of_struct_of_list_deque_backend() {
    // same helper, other backend: swap Vec -> VecDeque in the sink type
    type DqList<V> = ListCol<VecDeque<usize>, V>;
    type Sink = DqList<HCons<VecDeque<i32>, HCons<DqList<HCons<VecDeque<i64>, HNil>>, HNil>>>;

    let mut d = gen_list_of_struct_of_list::<DequeBackend>();
    let c = d.collect_col(Sink::default(), &mut rng(), 2);
    assert_eq!(c.offsets, VecDeque::from([2, 4]));
    assert_eq!(c.values.0, VecDeque::from([0, 1, 2, 3]));
    assert_eq!(c.values.1.0.offsets, VecDeque::from([1, 3, 6, 10]));
    assert_eq!(c.values.1.0.values.0, (0..10).collect::<VecDeque<i64>>());
}

#[test]
fn list_of_struct_of_list_tick_order() {
    let log = Log::default();
    let mut d = make_tracked_list_of_struct_of_list(&log);
    let c = d.collect_col(ListOfStructOfList::default(), &mut rng(), 2);

    assert_eq!(c.offsets, vec![2, 4]);
    assert_eq!(c.values.0, vec![0, 1, 2, 3]);
    assert_eq!(c.values.1.0.offsets, vec![1, 3, 6, 10]);
    assert_eq!(c.values.1.0.values.0, (0..10).collect::<Vec<i64>>());

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
