//! Fast mode (`FastSampler` + any `Rng`) keeps the reference's structure.

use crate::csv::Column;
use crate::ops::unique_in_order;
use crate::{FastSampler, GroupByConfig, GroupByTable, JoinConfig, JoinTables};
use rand::SeedableRng;
use rand::rngs::StdRng;
use std::collections::HashSet;

fn distinct<T: std::hash::Hash + Eq + Clone>(col: &[Option<T>]) -> usize {
    unique_in_order(col).iter().filter(|v| v.is_some()).count()
}

#[test]
fn groupby_fast_cardinalities_nas_and_sort() {
    let cfg = GroupByConfig::new(20_000, 10, 10, true);
    let t = GroupByTable::generate(cfg, FastSampler, &mut StdRng::seed_from_u64(7));
    // 10% of the 10 groups (= 1) is null; all others appear.
    assert_eq!(distinct(&t.id1), 9);
    assert_eq!(distinct(&t.id4), 9);
    // N/K = 2000 groups, 10% (200) nulled.
    assert_eq!(distinct(&t.id3), 1800);
    assert_eq!(t.v3.iter().filter(|v| v.is_none()).count(), 2000);
    assert!(t.v1.iter().flatten().all(|v| (1..=5).contains(v)));
    assert!(t.v2.iter().flatten().all(|v| (1..=15).contains(v)));
    assert!(t.v3.iter().flatten().all(|v| (0.0..100.0).contains(v)));
    // sorted by id1 with NAs first
    assert!(t.id1.windows(2).all(|w| w[0] <= w[1]));
    assert_eq!(t.id1[0], None);
}

#[test]
fn groupby_same_seed_same_table() {
    let cfg = GroupByConfig::new(500, 10, 0, false);
    let a = GroupByTable::generate(cfg, FastSampler, &mut StdRng::seed_from_u64(3));
    let b = GroupByTable::generate(cfg, FastSampler, &mut StdRng::seed_from_u64(3));
    assert_eq!(a, b);
}

#[test]
fn join_fast_key_overlap() {
    let cfg = JoinConfig::scaled(100_000, 10, 10_000, 0, false);
    let t = JoinTables::generate(cfg, FastSampler, &mut StdRng::seed_from_u64(5));
    let keys = |c: &[Option<i32>]| c.iter().flatten().copied().collect::<HashSet<_>>();
    // id2 of x vs id2 of medium: 10_000 keys each side, 9_000 shared.
    let lhs = keys(&t.x.ids[1]);
    let rhs = keys(&t.medium.ids[1]);
    assert_eq!(lhs.len(), 10_000);
    assert_eq!(rhs.len(), 10_000);
    assert_eq!(lhs.intersection(&rhs).count(), 9_000);
    // small: one row per right key (9 matched + 1 right-only), each once.
    assert_eq!(keys(&t.small.ids[0]).len(), 10);
    assert_eq!(t.small.ids[0].len(), 10);
    let [x, small, medium, big] = t.into_tables();
    assert_eq!(
        x.names(),
        vec!["id1", "id2", "id3", "id4", "id5", "id6", "v1"]
    );
    assert_eq!(small.names(), vec!["id1", "id4", "v2"]);
    assert_eq!(medium.names(), vec!["id1", "id2", "id4", "id5", "v2"]);
    assert_eq!(big.num_rows(), 100_000);
    let Some(Column::Str(id4)) = x.column("id4") else {
        panic!("id4 is a string column")
    };
    let Some(Column::Int(id1)) = x.column("id1") else {
        panic!("id1 is an int column")
    };
    assert_eq!(id4[0], id1[0].map(|i| format!("id{i}")));
}
