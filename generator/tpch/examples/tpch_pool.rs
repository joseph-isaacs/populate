//! Time building the text pool.
//!
//! `cargo run -p populate-tpch --release --example tpch_pool -- [threads]`

use std::time::Instant;

use populate_tpch::Tpch;

fn main() {
    let threads = std::env::args()
        .nth(1)
        .map_or(1, |t| t.parse().expect("threads"));
    let t = Instant::now();
    Tpch::prepare(threads);
    println!(
        "text pool ({threads} threads): {:.3} s",
        t.elapsed().as_secs_f64()
    );
}
