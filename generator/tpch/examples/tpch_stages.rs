//! Time per table and stage, on one thread: generation, `.tbl`
//! formatting (from the Arrow batches) and Parquet encoding.
//!
//! `cargo run -p populate-gen-tpch --release --example tpch_stages -- [sf]`
use populate_gen_tpch::{TableName, Tpch};
use std::hint::black_box;
use std::io::sink;
use std::time::Instant;

fn main() {
    let sf: f64 = std::env::args().nth(1).map_or(1.0, |s| s.parse().unwrap());
    let h = Tpch::new(sf);
    let start = Instant::now();
    Tpch::prepare(1);
    println!(
        "text pool (1 thread): {:.3} s",
        start.elapsed().as_secs_f64()
    );
    println!("| table | generate | + .tbl | + Parquet |");
    println!("|---|---:|---:|---:|");
    let mut total = [0.0; 3];
    for t in TableName::ALL {
        let t = h.table(t);
        let time = |f: &dyn Fn()| {
            let s = Instant::now();
            f();
            s.elapsed().as_secs_f64()
        };
        let g = time(&|| t.for_each_batch(1, |b| drop(black_box(b))));
        let tbl = time(&|| {
            black_box(t.write_tbl(sink(), 1).unwrap());
        });
        let pq = time(&|| {
            black_box(t.write_parquet(sink(), 1, None).unwrap());
        });
        for (a, b) in total.iter_mut().zip([g, tbl, pq]) {
            *a += b;
        }
        println!("| {} | {g:.3} s | {tbl:.3} s | {pq:.3} s |", t.name());
    }
    let [g, tbl, pq] = total;
    println!("| **total** | {g:.3} s | {tbl:.3} s | {pq:.3} s |");
}
