// ========================================================================
// with_column — dependent columns, collected column by column
// ========================================================================

use crate::dist::{constant, list, range, with_column};
use crate::test::{Log, tracked};
use crate::traits::{
    DequeBackend, Distribution, DistributionExt, DistributionScalar, HCons, HNil, ListCol,
    SampleIter, VecBackend,
};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng, rng};
use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;

fn make_with_column() -> impl Distribution<VecBackend, Item = (i64, i64)> {
    with_column(range(0i64, 1), |x: i64, _| x * 10)
}

fn make_chained() -> impl Distribution<VecBackend, Item = (i64, i64, bool)> {
    with_column(
        with_column(range(0i64, 1), |x: i64, _| x + 100),
        |(a, b): (i64, i64), _| a + b > 102,
    )
}

fn make_tracked_derived(log: &Log) -> impl Distribution<VecBackend, Item = (i64, i64)> {
    let l = Rc::clone(log);
    with_column(tracked("PARENT", range(0i64, 1), log), move |x: i64, _| {
        l.borrow_mut().push("DERIVED");
        x * 10
    })
}

fn make_noisy() -> impl Distribution<VecBackend, Item = (i64, i64)> {
    with_column(range(0i64, 1), |x: i64, rng| x + rng.random_range(1..=3))
}

fn make_deque_derived() -> impl Distribution<DequeBackend, Item = (i32, i32)> {
    with_column(range(5i32, 1), |x: i32, _| -x)
}

fn make_list_of_derived() -> impl Distribution<VecBackend, Item = Vec<(i64, i64)>> {
    list(
        constant(2usize),
        with_column(range(0i64, 1), |x: i64, _| x * x),
    )
}

#[test]
fn with_column_adds_a_column() {
    let mut d = make_with_column();
    let (a, b) = d.collect_flat((Vec::new(), Vec::new()), &mut rng(), 4);
    assert_eq!(a, vec![0, 1, 2, 3]);
    assert_eq!(b, vec![0, 10, 20, 30]);
}

#[test]
fn with_column_chains_flat() {
    let mut d = make_chained();
    let (a, b, c) = d.collect_flat((Vec::new(), Vec::new(), Vec::new()), &mut rng(), 4);
    assert_eq!(a, vec![0, 1, 2, 3]);
    assert_eq!(b, vec![100, 101, 102, 103]);
    assert_eq!(c, vec![false, false, true, true]);
}

#[test]
fn with_column_fills_parent_column_first() {
    let log: Log = Rc::new(RefCell::new(Vec::new()));
    let mut d = make_tracked_derived(&log);
    let (a, b) = d.collect_flat((Vec::new(), Vec::new()), &mut rng(), 3);
    assert_eq!(a, vec![0, 1, 2]);
    assert_eq!(b, vec![0, 10, 20]);
    assert_eq!(
        *log.borrow(),
        vec![
            "PARENT", "PARENT", "PARENT", "DERIVED", "DERIVED", "DERIVED"
        ]
    );
}

#[test]
fn with_column_row_iter_matches_columns() {
    let mut d = make_chained();
    let mut it = d.row_iter();
    let mut r = rng();
    let rows: Vec<_> = (0..4).map(|_| it.next(&mut r)).collect();
    assert_eq!(
        rows,
        vec![
            (0, 100, false),
            (1, 101, false),
            (2, 102, true),
            (3, 103, true)
        ]
    );
}

#[test]
fn with_column_uses_rng() {
    let mut d = make_noisy();
    let (a, b) = d.collect_flat(
        (Vec::new(), Vec::new()),
        &mut SmallRng::seed_from_u64(1),
        100,
    );
    assert_eq!(a, (0..100).collect::<Vec<i64>>());
    assert!(a.iter().zip(&b).all(|(x, y)| (x + 1..=x + 3).contains(y)));
    assert!(a.iter().zip(&b).any(|(x, y)| y - x != 1));
}

#[test]
fn with_column_appends_to_existing_sink() {
    let mut d = make_with_column();
    let sink = d.collect_flat((Vec::new(), Vec::new()), &mut rng(), 2);
    let (a, b) = d.collect_flat(sink, &mut rng(), 2);
    assert_eq!(a, vec![0, 1, 0, 1]);
    assert_eq!(b, vec![0, 10, 0, 10]);
}

#[test]
fn with_column_deque_backend() {
    let mut d = make_deque_derived();
    let (a, b) = d.collect_flat((VecDeque::new(), VecDeque::new()), &mut rng(), 3);
    assert_eq!(a, VecDeque::from([5, 6, 7]));
    assert_eq!(b, VecDeque::from([-5, -6, -7]));
}

#[test]
fn with_column_as_list_elements() {
    let mut d = make_list_of_derived();
    let c = d.collect_col(
        ListCol::<Vec<usize>, HCons<Vec<i64>, HCons<Vec<i64>, HNil>>>::default(),
        &mut rng(),
        2,
    );
    assert_eq!(c.offsets, vec![2, 4]);
    assert_eq!(c.values.0, vec![0, 1, 2, 3]);
    assert_eq!(c.values.1.0, vec![0, 1, 4, 9]);
}

#[test]
fn with_column_rng_is_deterministic_for_a_seed() {
    let collect = |seed| {
        make_noisy().collect_flat(
            (Vec::new(), Vec::new()),
            &mut SmallRng::seed_from_u64(seed),
            50,
        )
    };
    let (a1, b1) = collect(7);
    let (a2, b2) = collect(7);
    let (_, b3) = collect(8);
    assert_eq!(a1, a2);
    assert_eq!(b1, b2);
    assert_ne!(b1, b3);
}
