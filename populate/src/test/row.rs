// ========================================================================
// Row-by-row: `sample`, and sampled rows pushed into a Sink
// ========================================================================

use crate::Distribution;
use crate::test::dist::{collect, make_3tuple, make_constant, make_mapped, make_range, make_tuple};
use rand::rng;

fn sample_n<D: Distribution>(mut dist: D, n: usize) -> Vec<D::Item> {
    let mut rng = rng();
    (0..n).map(|_| dist.sample(&mut rng)).collect()
}

#[test]
fn test_constant_collect() {
    let mut dist = make_constant();
    let mut sink = Vec::new();
    collect(&mut dist, &mut sink, 5, &mut rng());
    assert_eq!(sink, vec![42, 42, 42, 42, 42]);
}

#[test]
fn test_range_collect() {
    let mut dist = make_range();
    let mut sink = Vec::new();
    collect(&mut dist, &mut sink, 5, &mut rng());
    assert_eq!(sink, vec![0, 1, 2, 3, 4]);
}

#[test]
fn test_tuple_collect() {
    let mut dist = make_tuple();
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
fn test_constant_sample() {
    assert_eq!(sample_n(make_constant(), 5), vec![42, 42, 42, 42, 42]);
}

#[test]
fn test_3tuple_sample() {
    assert_eq!(
        sample_n(make_3tuple(), 3),
        vec![(1, 0, true), (1, 10, true), (1, 20, true)]
    );
}

#[test]
fn test_range_sample() {
    assert_eq!(sample_n(make_range(), 5), vec![0, 1, 2, 3, 4]);
}

#[test]
fn test_tuple_sample() {
    assert_eq!(
        sample_n(make_tuple(), 4),
        vec![(1, 0), (1, 10), (1, 20), (1, 30)]
    );
}

#[test]
fn test_map_sample() {
    assert_eq!(sample_n(make_mapped(), 5), vec![0, 2, 4, 6, 8]);
}

/// A distribution is a stream: sampling and collecting both move it on, and
/// a clone starts again from where the original was.
#[test]
fn test_stream_continues_and_clone_restarts() {
    let mut dist = crate::dist::range(0i64, 1);
    let fresh = dist.clone();
    assert_eq!(dist.collect(&mut rng(), 3), vec![0, 1, 2]);
    assert_eq!(dist.sample(&mut rng()), 3);
    assert_eq!(dist.collect(&mut rng(), 2), vec![4, 5]);
    assert_eq!(sample_n(fresh, 2), vec![0, 1]);
}
