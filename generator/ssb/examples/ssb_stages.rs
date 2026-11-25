//! Time generation and `.tbl` formatting of each SSB table on one thread:
//! `cargo run -p populate-ssb --release --example ssb_stages -- [SF]`.
use populate_ssb::{Ssb, TableName};
use populate_table::text::{Encoding, write_batch};
use std::time::Instant;

fn main() {
    let sf = std::env::args()
        .nth(1)
        .map_or(1.0, |s| s.parse().expect("SF"));
    let s = Ssb::new(sf);
    println!("| table | generate | format .tbl | MB |");
    println!("|---|---:|---:|---:|");
    for t in TableName::ALL.into_iter().chain([TableName::LineOrderFlat]) {
        let table = s.table(t);
        // build shared state (dictionary, flat's dimensions) outside the timing
        table.clone().limit_source_rows(1).collect(1);
        let start = Instant::now();
        let batches = table.collect(1);
        let generate = start.elapsed().as_secs_f64();
        let start = Instant::now();
        let mut out = Vec::new();
        let mut bytes = 0;
        for b in &batches {
            write_batch(b, Encoding::Utf8, &mut out);
            bytes += out.len();
            out.clear();
        }
        let format = start.elapsed().as_secs_f64();
        println!(
            "| {t} | {generate:.3} s | {format:.3} s | {:.0} |",
            bytes as f64 / 1e6
        );
    }
}
