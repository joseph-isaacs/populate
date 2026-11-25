//! Dependent columns with `derive`, straight into Arrow.
//!
//! ```sh
//! cargo run -p populate --example realistic
//! ```

use arrow_cast::pretty::pretty_format_batches;
use populate::Distribution;
use populate::arrow::DistributionArrow;
use populate::dist::{derive, range, uniform, weighted};
use rand::SeedableRng;
use rand::rngs::SmallRng;

const COUNTRIES: [&[&str]; 3] = [&["UK", "FR", "DE"], &["US", "CA"], &["JP", "KR", "SG"]];

/// order_id, ship_day, receipt_day (ship + 1..=30 days), late, region,
/// country (drawn given region).
type Order = (i64, (i32, i32, bool), (i32, &'static str));

fn orders() -> impl Distribution<Item = Order> {
    // receipt: 1..=30 days after shipping; late: more than 14
    let dates = derive(
        (uniform(8035i32, 10_591), uniform(1i32, 30)),
        |(&ship, &days): (&i32, &i32)| (ship, ship + days, days > 14),
    );
    // a country drawn given the region: an index scaled to its list
    let place = derive(
        (weighted(&[(0i32, 5), (1, 3), (2, 2)]), uniform(0.0f64, 1.0)),
        |(&r, &u): (&i32, &f64)| {
            let names = COUNTRIES[r as usize];
            (r, names[(u * names.len() as f64) as usize % names.len()])
        },
    );
    (range(1i64, 1), dates, place)
}

fn main() {
    let batch = orders()
        .collect_record_batch(&mut SmallRng::seed_from_u64(42), 8)
        .unwrap();
    println!("{}", pretty_format_batches(&[batch]).unwrap());
}
