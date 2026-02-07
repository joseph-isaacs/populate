// ========================================================================
// Tests using collect (row-by-row into Sink)
// ========================================================================

use crate::test::dist::{collect, make_3tuple, make_constant, make_mapped, make_range, make_tuple};
use crate::traits::Distribution;
use crate::traits::{SampleIter, VecBackend};
use rand::rng;

#[test]
fn test_constant_collect() {
    let mut dist = make_constant::<VecBackend>();
    let mut sink = Vec::new();
    collect(&mut dist, &mut sink, 5, &mut rng());
    assert_eq!(sink, vec![42, 42, 42, 42, 42]);
}

#[test]
fn test_range_collect() {
    let mut dist = make_range::<VecBackend>();
    let mut sink = Vec::new();
    collect(&mut dist, &mut sink, 5, &mut rng());
    assert_eq!(sink, vec![0, 1, 2, 3, 4]);
}

#[test]
fn test_tuple_collect() {
    let mut dist = make_tuple::<VecBackend>();
    let mut sink = (Vec::new(), Vec::new());
    collect(&mut dist, &mut sink, 4, &mut rng());
    assert_eq!(sink.0, vec![1, 1, 1, 1]);
    assert_eq!(sink.1, vec![0, 10, 20, 30]);
}

#[test]
fn test_map_collect() {
    let mut dist = make_mapped();
    let mut sink = Vec::new();
    collect(&mut dist, &mut sink, 5, &mut rng());
    assert_eq!(sink, vec![0, 2, 4, 6, 8]);
}

#[test]
fn test_constant_row_iter() {
    let mut dist = make_constant::<VecBackend>();
    let mut iter = dist.row_iter();
    let mut rng = rng();
    let results: Vec<_> = (0..5).map(|_| iter.next(&mut rng)).collect();
    assert_eq!(results, vec![42, 42, 42, 42, 42]);
}

#[test]
fn test_3tuple_row_iter() {
    let mut dist = make_3tuple();
    let mut iter = dist.row_iter();
    let mut rng = rng();
    let results: Vec<_> = (0..3).map(|_| iter.next(&mut rng)).collect();
    assert_eq!(results, vec![(1, 0, true), (1, 10, true), (1, 20, true)]);
}

#[test]
fn test_range_row_iter() {
    let mut dist = make_range::<VecBackend>();
    let mut iter = dist.row_iter();
    let mut rng = rng();
    let results: Vec<_> = (0..5).map(|_| iter.next(&mut rng)).collect();
    assert_eq!(results, vec![0, 1, 2, 3, 4]);
}

#[test]
fn test_tuple_row_iter() {
    let mut dist = make_tuple::<VecBackend>();
    let mut iter = dist.row_iter();
    let mut rng = rng();
    let results: Vec<_> = (0..4).map(|_| iter.next(&mut rng)).collect();
    assert_eq!(results, vec![(1, 0), (1, 10), (1, 20), (1, 30)]);
}

#[test]
fn test_map_row_iter() {
    let mut dist = make_mapped();
    let mut iter = dist.row_iter();
    let mut rng = rng();
    let results: Vec<_> = (0..5).map(|_| iter.next(&mut rng)).collect();
    assert_eq!(results, vec![0, 2, 4, 6, 8]);
}
