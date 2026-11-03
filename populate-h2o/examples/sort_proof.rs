//! Proof at scale that the linear-time radix sort gives exactly the row order
//! of the comparison sort on real db-benchmark groupby tables (R-exact, before
//! sorting), including the order of rows that tie on all six keys.
//!
//! ```text
//! cargo run --release -p populate-h2o --example sort_proof
//! ```
use populate_2::column::{DictCol, NullableCol};
use populate_2::table::{SortKey, radix_order_by, stable_order_by};
use populate_h2o::{GroupByConfig, GroupByTable, RRng, RSampler};
use std::time::Instant;

fn labels(col: &NullableCol<DictCol<String>>) -> Vec<Option<String>> {
    (0..col.len())
        .map(|i| col.is_valid(i).then(|| col.values.get(i).clone()))
        .collect()
}

fn ints(col: &NullableCol<Vec<i32>>) -> Vec<Option<i32>> {
    (0..col.len())
        .map(|i| col.is_valid(i).then_some(col.values[i]))
        .collect()
}

/// The columnar table's radix order vs the comparison sort over the same
/// data as plain `Option` columns (the pre-columnar representation).
fn main() {
    for (n, k, nas) in [
        (10_000_000, 10, 5),
        (10_000_000, 100, 0),
        (1_000_000, 2, 50),
    ] {
        let cfg = GroupByConfig::new(n, k, nas, false);
        let t = GroupByTable::generate(cfg, RSampler, &mut RRng::new(108));
        let s = Instant::now();
        let radix = radix_order_by(n, &t.id_keys());
        let radix_t = s.elapsed();

        let (o1, o2, o3) = (labels(&t.id1), labels(&t.id2), labels(&t.id3));
        let (o4, o5, o6) = (ints(&t.id4), ints(&t.id5), ints(&t.id6));
        let cmp_keys: [&dyn SortKey; 6] = [&o1, &o2, &o3, &o4, &o5, &o6];
        let s = Instant::now();
        let cmp = stable_order_by(n, &cmp_keys);
        let cmp_t = s.elapsed();
        let moved = radix.iter().enumerate().filter(|&(i, &r)| i != r).count();
        let ties = radix
            .windows(2)
            .filter(|w| cmp_keys.iter().all(|c| c.compare_rows(w[0], w[1]).is_eq()))
            .count();
        println!(
            "N={n:>8} K={k:>3} nas={nas:>2}: identical={} | rows moved {moved} | adjacent full-key ties {ties} | radix {radix_t:.2?} vs comparison {cmp_t:.2?}",
            radix == cmp
        );
    }
}
