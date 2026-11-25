//! Dependent columns with `with_column`, straight into Arrow.
//!
//! ```sh
//! cargo run -p populate --example realistic
//! ```

use arrow_cast::pretty::pretty_format_batches;
use populate::Distribution;
use populate::arrow::DistributionArrow;
use populate::dist::{range, uniform, weighted, with_column};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

const COUNTRIES: [&[&str]; 3] = [&["UK", "FR", "DE"], &["US", "CA"], &["JP", "KR", "SG"]];

/// order_id, ship_day, receipt_day (ship + 1..=30 days), late, region,
/// country (drawn given region).
type Order = (i64, (i32, i32, bool), (i32, &'static str));

fn orders() -> impl Distribution<Item = Order> {
    let receipt = with_column(uniform(8035i32, 10_591), |ship, rng| {
        ship + rng.random_range(1..=30)
    });
    let region = weighted(&[(0i32, 5), (1, 3), (2, 2)]);
    (
        range(1i64, 1),
        with_column(receipt, |(ship, receipt), _| receipt - ship > 14),
        with_column(region, |r, rng| {
            let names = COUNTRIES[r as usize];
            names[rng.random_range(0..names.len())]
        }),
    )
}

fn main() {
    let batch = orders()
        .collect_record_batch(&mut SmallRng::seed_from_u64(42), 8)
        .unwrap();
    println!("{}", pretty_format_batches(&[batch]).unwrap());
}
