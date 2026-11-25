// ========================================================================
// List value tests — columnar layout (offsets + element columns)
// ========================================================================

use crate::Distribution;
use crate::test::dist::{
    gen_list, gen_list_in_nested_tuple, gen_list_in_tuple, gen_list_of_tuples,
    gen_map_to_list_of_tuples, gen_nested_list,
};
use rand::rng;

#[test]
fn list_of_scalars() {
    let mut d = gen_list();
    let c = d.collect(&mut rng(), 4);
    assert_eq!(c.offsets(), [0, 0, 1, 3, 6]); // lens 0,1,2,3
    assert_eq!(c.values, vec![0, 1, 2, 3, 4, 5]); // one continuous column
    assert_eq!(c.len(), 4);
}

#[test]
fn list_of_tuples_is_struct_of_arrays() {
    let mut d = gen_list_of_tuples();
    let c = d.collect(&mut rng(), 2);
    assert_eq!(c.offsets(), [0, 2, 4]);
    assert_eq!(c.values.0, vec![0, 1, 2, 3]);
    assert_eq!(c.values.1, vec![true; 4]);
}

#[test]
fn nested_list() {
    let mut d = gen_nested_list();
    let c = d.collect(&mut rng(), 2);
    let inner = &c.values;
    assert_eq!(c.offsets(), [0, 2, 4]); // outer: 2 inner lists each
    assert_eq!(inner.offsets(), [0, 1, 3, 6, 10]); // inner lens 1,2,3,4
    assert_eq!(inner.values, (0..10).collect::<Vec<i64>>());
}

#[test]
fn list_in_tuple() {
    let mut d = gen_list_in_tuple();
    let (a, l) = d.collect(&mut rng(), 2);
    assert_eq!(a, vec![7, 7]);
    assert_eq!(l.offsets(), [0, 2, 4]);
    assert_eq!(l.values, vec![0, 1, 2, 3]);
}

#[test]
fn list_in_nested_tuple() {
    let mut d = gen_list_in_nested_tuple();
    let (a, (l, b)) = d.collect(&mut rng(), 2);
    assert_eq!((a, b), (vec![1, 1], vec![2, 2]));
    assert_eq!(l.offsets(), [0, 1, 2]);
    assert_eq!(l.values, vec![0, 1]);
}

#[test]
fn map_returning_list_of_tuples_row_fallback() {
    let mut d = gen_map_to_list_of_tuples();
    let c = d.collect(&mut rng(), 2);
    assert_eq!(c.offsets(), [0, 2, 4]);
    assert_eq!(c.values.0, vec![0, 0, 1, 1]);
    assert_eq!(c.values.1, vec![0, 0, 1, 1]);
}

#[test]
fn sampled_rows_match_columnar() {
    let mut d = gen_list();
    let mut r = rng();
    let rows: Vec<_> = (0..4).map(|_| d.sample(&mut r)).collect();
    assert_eq!(rows, vec![vec![], vec![0], vec![1, 2], vec![3, 4, 5]]);

    let c = gen_list().collect(&mut rng(), 4);
    let from_cols: Vec<Vec<i64>> = c
        .offsets()
        .windows(2)
        .map(|w| c.values[w[0] as usize..w[1] as usize].to_vec())
        .collect();
    assert_eq!(rows, from_cols);
}
