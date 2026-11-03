//! Proof at scale that the linear-time radix sort gives exactly the row order
//! of the comparison sort on real db-benchmark groupby tables (R-exact, before
//! sorting), including the order of rows that tie on all six keys.
//!
//! ```text
//! cargo run --release -p populate-h2o --example sort_proof
//! ```
use populate_2::table::{RankKey, SortKey, radix_order_by, stable_order_by};
use populate_h2o::{GroupByConfig, GroupByTable};
use std::time::Instant;

fn main() {
    for (n, k, nas) in [
        (10_000_000, 10, 5),
        (10_000_000, 100, 0),
        (1_000_000, 2, 50),
    ] {
        let t = GroupByTable::reference(GroupByConfig::new(n, k, nas, false));
        let radix_keys: [&dyn RankKey; 6] = [&t.id1, &t.id2, &t.id3, &t.id4, &t.id5, &t.id6];
        let cmp_keys: [&dyn SortKey; 6] = [&t.id1, &t.id2, &t.id3, &t.id4, &t.id5, &t.id6];
        let s = Instant::now();
        let radix = radix_order_by(n, &radix_keys);
        let radix_t = s.elapsed();
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
