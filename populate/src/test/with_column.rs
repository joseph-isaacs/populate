// ========================================================================
// with_column — dependent columns, collected column by column
// ========================================================================

use crate::Distribution;
use crate::dist::{constant, list, range, with_column};
use crate::test::{Log, tracked};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng, rng};
use std::cell::RefCell;
use std::rc::Rc;

fn make_with_column() -> impl Distribution<Item = (i64, i64)> {
    with_column(range(0i64, 1), |x: i64, _| x * 10)
}

fn make_chained() -> impl Distribution<Item = (i64, i64, bool)> {
    with_column(
        with_column(range(0i64, 1), |x: i64, _| x + 100),
        |(a, b): (i64, i64), _| a + b > 102,
    )
}

fn make_tracked_derived(log: &Log) -> impl Distribution<Item = (i64, i64)> {
    let l = Rc::clone(log);
    with_column(tracked("PARENT", range(0i64, 1), log), move |x: i64, _| {
        l.borrow_mut().push("DERIVED");
        x * 10
    })
}

fn make_noisy() -> impl Distribution<Item = (i64, i64)> {
    with_column(range(0i64, 1), |x: i64, rng| x + rng.random_range(1..=3))
}

fn make_list_of_derived() -> impl Distribution<Item = Vec<(i64, i64)>> {
    list(
        constant(2usize),
        with_column(range(0i64, 1), |x: i64, _| x * x),
    )
}

#[test]
fn with_column_adds_a_column() {
    let mut d = make_with_column();
    let (a, b) = d.collect(&mut rng(), 4);
    assert_eq!(a, vec![0, 1, 2, 3]);
    assert_eq!(b, vec![0, 10, 20, 30]);
}

#[test]
fn with_column_chains_flat() {
    let mut d = make_chained();
    let (a, b, c) = d.collect(&mut rng(), 4);
    assert_eq!(a, vec![0, 1, 2, 3]);
    assert_eq!(b, vec![100, 101, 102, 103]);
    assert_eq!(c, vec![false, false, true, true]);
}

#[test]
fn with_column_fills_parent_column_first() {
    let log: Log = Rc::new(RefCell::new(Vec::new()));
    let mut d = make_tracked_derived(&log);
    let (a, b) = d.collect(&mut rng(), 3);
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
fn with_column_sampled_rows_match_columns() {
    let mut d = make_chained();
    let mut r = rng();
    let rows: Vec<_> = (0..4).map(|_| d.sample(&mut r)).collect();
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
    let (a, b) = d.collect(&mut SmallRng::seed_from_u64(1), 100);
    assert_eq!(a, (0..100).collect::<Vec<i64>>());
    assert!(a.iter().zip(&b).all(|(x, y)| (x + 1..=x + 3).contains(y)));
    assert!(a.iter().zip(&b).any(|(x, y)| y - x != 1));
}

#[test]
fn with_column_appends_to_existing_columns() {
    let mut d = make_with_column();
    let mut cols = d.collect(&mut rng(), 2);
    d.collect_into(&mut cols, &mut rng(), 2);
    let (a, b) = cols;
    assert_eq!(a, vec![0, 1, 2, 3]);
    assert_eq!(b, vec![0, 10, 20, 30]);
}

#[test]
fn with_column_as_list_elements() {
    let mut d = make_list_of_derived();
    let c = d.collect(&mut rng(), 2);
    assert_eq!(c.offsets(), [0, 2, 4]);
    assert_eq!(c.values.0, vec![0, 1, 2, 3]);
    assert_eq!(c.values.1, vec![0, 1, 4, 9]);
}

#[test]
fn with_column_rng_is_deterministic_for_a_seed() {
    let collect = |seed| make_noisy().collect(&mut SmallRng::seed_from_u64(seed), 50);
    let (a1, b1) = collect(7);
    let (a2, b2) = collect(7);
    let (_, b3) = collect(8);
    assert_eq!(a1, a2);
    assert_eq!(b1, b2);
    assert_ne!(b1, b3);
}

type Wide = (i64, i64, i64, i64, i64, i64);

fn make_wide_chain() -> impl Distribution<Item = Wide> {
    let b = with_column(range(1i64, 1), |a: i64, _| a * 2);
    let c = with_column(b, |(a, b): (i64, i64), _| a + b);
    let d = with_column(c, |(_, _, c): (i64, i64, i64), _| c * 10);
    let e = with_column(d, |(a, _, _, d): (i64, i64, i64, i64), _| d - a);
    with_column(e, |(a, b, c, d, e): (i64, i64, i64, i64, i64), _| {
        a + b + c + d + e
    })
}

fn make_two_string_columns() -> impl Distribution<Item = (i64, (&'static str, &'static str))> {
    with_column(range(0i64, 1), |x: i64, _| {
        (
            if x % 2 == 0 { "even" } else { "odd" },
            if x < 2 { "lo" } else { "hi" },
        )
    })
}

#[test]
fn with_column_wide_parent() {
    let mut d = make_wide_chain();
    let (a, b, c, dd, e, f) = d.collect(&mut rng(), 3);
    assert_eq!(a, vec![1, 2, 3]);
    assert_eq!(b, vec![2, 4, 6]);
    assert_eq!(c, vec![3, 6, 9]);
    assert_eq!(dd, vec![30, 60, 90]);
    assert_eq!(e, vec![29, 58, 87]);
    assert_eq!(f, vec![65, 130, 195]);
}

#[test]
fn with_column_returns_several_string_columns() {
    let mut d = make_two_string_columns();
    let (x, (parity, size)) = d.collect(&mut rng(), 3);
    assert_eq!(x, vec![0, 1, 2]);
    assert_eq!(
        (0..3)
            .map(|i| parity.get(i).to_string())
            .collect::<Vec<_>>(),
        ["even", "odd", "even"]
    );
    assert_eq!(
        (0..3).map(|i| size.get(i).to_string()).collect::<Vec<_>>(),
        ["lo", "lo", "hi"]
    );
}
