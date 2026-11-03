use crate::sampler::{FastSampler, RRng, RSampler};
use crate::table::{
    SortKey, covering_sample, fraction_of, null_rows_with_sampled_values, null_sampled_rows,
    split_keys, stable_order_by, take, unique_in_order,
};
use rand::SeedableRng;
use rand::rngs::StdRng;
use std::collections::HashSet;

/// set.seed(108); k = split_xlr(10); sample_all(c(k$x, k$l), 15)   (R 4.3.3)
#[test]
fn split_and_covering_sample_match_r() {
    let rng = &mut RRng::new(108);
    let k = split_keys(RSampler, rng, 10.0, 0.1);
    assert_eq!(k.matched, vec![10, 7, 6, 5, 11, 8, 9, 3, 4]);
    assert_eq!(k.left_only, vec![2]);
    assert_eq!(k.right_only, vec![1]);
    let col = covering_sample(RSampler, rng, &k.left(), 15);
    assert_eq!(col, vec![5, 8, 3, 11, 2, 8, 6, 9, 4, 9, 9, 8, 10, 7, 4]);
}

#[test]
fn covering_sample_contains_every_key() {
    let rng = &mut StdRng::seed_from_u64(3);
    let keys: Vec<i32> = (0..40).collect();
    let col = covering_sample(FastSampler, rng, &keys, 100);
    assert_eq!(col.len(), 100);
    assert_eq!(col.iter().collect::<HashSet<_>>().len(), 40);
}

#[test]
fn null_ops() {
    let rng = &mut StdRng::seed_from_u64(4);
    // 10 distinct values, 5 rows each: nulling 20% of values nulls 2 whole groups.
    let mut col: Vec<Option<i32>> = (0..50).map(|i| Some(i % 10)).collect();
    assert_eq!(
        null_rows_with_sampled_values(FastSampler, rng, &mut col, 0.2),
        2
    );
    assert_eq!(col.iter().filter(|v| v.is_none()).count(), 10);
    assert_eq!(unique_in_order(&col).len(), 9); // 8 values + None

    let mut col: Vec<Option<f64>> = vec![Some(1.0); 50];
    null_sampled_rows(FastSampler, rng, &mut col, fraction_of(50, 0.1));
    assert_eq!(col.iter().filter(|v| v.is_none()).count(), 5);
}

#[test]
fn stable_sort_nulls_first() {
    let a = vec![Some(2), None, Some(1), Some(2), Some(1)];
    let b = vec![Some("x"), Some("y"), Some("z"), Some("a"), Some("z")];
    let order = stable_order_by(a.len(), &[&a as &dyn SortKey, &b]);
    assert_eq!(order, vec![1, 2, 4, 3, 0]);
    assert_eq!(
        take(&a, &order),
        vec![None, Some(1), Some(1), Some(2), Some(2)]
    );
}
