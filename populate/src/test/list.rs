// ========================================================================
// List value tests — columnar layout (offsets + child column sinks)
// ========================================================================

use crate::test::dist::{
    gen_list, gen_list_in_flatten, gen_list_in_tuple, gen_list_of_tuples,
    gen_map_to_list_of_tuples, gen_nested_list,
};
use crate::traits::{
    DequeBackend, Distribution, DistributionExt, DistributionScalar, HCons, HNil, ListCol,
    SampleIter, VecBackend,
};
use rand::rng;
use std::collections::VecDeque;

type Col<T> = HCons<Vec<T>, HNil>;
type VecList<V> = ListCol<Vec<usize>, V>;

#[test]
fn list_of_scalars() {
    let mut d = gen_list();
    let c = d.collect_col(VecList::<Col<i64>>::default(), &mut rng(), 4);
    assert_eq!(c.offsets, vec![0, 1, 3, 6]); // lens 0,1,2,3
    assert_eq!(c.values.0, vec![0, 1, 2, 3, 4, 5]); // one continuous column
    assert_eq!(c.len, 6);
}

#[test]
fn list_of_tuples_is_struct_of_arrays() {
    let mut d = gen_list_of_tuples();
    let c = d.collect_col(
        VecList::<HCons<Vec<i64>, Col<bool>>>::default(),
        &mut rng(),
        2,
    );
    assert_eq!(c.offsets, vec![2, 4]);
    assert_eq!(c.values.0, vec![0, 1, 2, 3]);
    assert_eq!(c.values.1.0, vec![true; 4]);
}

#[test]
fn nested_list() {
    let mut d = gen_nested_list();
    let c = d.collect_col(
        VecList::<HCons<VecList<Col<i64>>, HNil>>::default(),
        &mut rng(),
        2,
    );
    let inner = &c.values.0;
    assert_eq!(c.offsets, vec![2, 4]); // outer: 2 inner lists each
    assert_eq!(inner.offsets, vec![1, 3, 6, 10]); // inner lens 1,2,3,4
    assert_eq!(inner.values.0, (0..10).collect::<Vec<i64>>());
}

#[test]
fn list_in_tuple_vec_backend() {
    let mut d = gen_list_in_tuple::<VecBackend>();
    let (a, l) = d.collect_flat((Vec::new(), VecList::<Col<i64>>::default()), &mut rng(), 2);
    assert_eq!(a, vec![7, 7]);
    assert_eq!(l.offsets, vec![2, 4]);
    assert_eq!(l.values.0, vec![0, 1, 2, 3]);
}

#[test]
fn list_in_tuple_deque_backend() {
    let mut d = gen_list_in_tuple::<DequeBackend>();
    let (a, l) = d.collect_flat(
        (
            VecDeque::new(),
            ListCol::<VecDeque<usize>, HCons<VecDeque<i64>, HNil>>::default(),
        ),
        &mut rng(),
        2,
    );
    assert_eq!(a, VecDeque::from([7, 7]));
    assert_eq!(l.offsets, VecDeque::from([2, 4]));
    assert_eq!(l.values.0, VecDeque::from([0, 1, 2, 3]));
}

#[test]
fn list_in_flatten() {
    let mut d = gen_list_in_flatten();
    let (a, l, b) = d.collect_flat(
        (Vec::new(), VecList::<Col<i64>>::default(), Vec::new()),
        &mut rng(),
        2,
    );
    assert_eq!((a, b), (vec![1, 1], vec![2, 2]));
    assert_eq!((l.offsets, l.values.0), (vec![1, 2], vec![0, 1]));
}

#[test]
fn map_returning_list_of_tuples_row_fallback() {
    let mut d = gen_map_to_list_of_tuples();
    let c = d.collect_col(
        VecList::<HCons<Vec<i64>, Col<i32>>>::default(),
        &mut rng(),
        2,
    );
    assert_eq!(c.offsets, vec![2, 4]);
    assert_eq!(c.values.0, vec![0, 0, 1, 1]);
    assert_eq!(c.values.1.0, vec![0, 0, 1, 1]);
}

#[test]
fn row_iter_matches_columnar() {
    let mut d = gen_list();
    let mut it = d.row_iter();
    let mut r = rng();
    let rows: Vec<_> = (0..4).map(|_| it.next(&mut r)).collect();
    assert_eq!(rows, vec![vec![], vec![0], vec![1, 2], vec![3, 4, 5]]);

    let c = gen_list().collect_col(VecList::<Col<i64>>::default(), &mut rng(), 4);
    let from_cols: Vec<Vec<i64>> = c
        .offsets
        .iter()
        .scan(0, |start, &end| {
            let row = c.values.0[*start..end].to_vec();
            *start = end;
            Some(row)
        })
        .collect();
    assert_eq!(rows, from_cols);
}
