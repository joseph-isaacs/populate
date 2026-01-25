#![allow(clippy::type_complexity)]

use populate::{Col, CollectExt, ColumnarCols, range};
use rand::SeedableRng;
use rand::rngs::StdRng;

const ROWS: usize = 1_000_000;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mode = args.get(1).map(|s| s.as_str()).unwrap_or("columnar");

    let mut rng = StdRng::seed_from_u64(42);

    match mode {
        "row" => {
            // Row-by-row: 7-tuple
            let mut dist = (
                range(0i64, 1_000_000),
                range(0i32, 100_000),
                range(0.0f64, 1.0),
                range(0i64, 1_000_000),
                range(0i32, 100_000),
                range(0.0f64, 1.0),
                range(0i64, 1_000_000),
            );
            let result: Vec<(i64, i32, f64, i64, i32, f64, i64)> =
                dist.collect::<Col>(&mut rng, ROWS);
            std::hint::black_box(result);
            println!("Row-by-row mode completed");
        }
        "columnar" => {
            // Columnar: 7-tuple
            let mut dist = (
                range(0i64, 1_000_000),
                range(0i32, 100_000),
                range(0.0f64, 1.0),
                range(0i64, 1_000_000),
                range(0i32, 100_000),
                range(0.0f64, 1.0),
                range(0i64, 1_000_000),
            );
            let result: (
                Vec<i64>,
                Vec<i32>,
                Vec<f64>,
                Vec<i64>,
                Vec<i32>,
                Vec<f64>,
                Vec<i64>,
            ) = dist.collect::<ColumnarCols>(&mut rng, ROWS);
            std::hint::black_box(result);
            println!("Columnar mode completed");
        }
        _ => eprintln!("Usage: profile_columnar [row|columnar]"),
    }
}
