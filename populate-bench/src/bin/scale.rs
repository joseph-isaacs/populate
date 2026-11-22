//! Full-scale streaming run: generate a whole TPC-H `lineitem` table at a given
//! scale factor as Arrow batches, dropping each batch as soon as it's made.
//! Memory stays at a few batches per thread, whatever the scale factor.
//!
//! ```bash
//! cargo run -p populate-bench --release --bin scale -- <workload> <sf> [threads] [batch]
//! ```
//!
//! Workloads:
//! - `tpch-full`: tpchgen-arrow `LineItemArrow`, all 16 columns.
//! - `tpch-numeric`: tpchgen rows -> the 11 numeric columns.
//! - `populate`: populate's 11 numeric columns (same row count as tpchgen).
//!
//! With `threads > 1` each thread generates one partition. tpchgen uses its
//! own `part`/`part_count` support, which skips its RNGs ahead to the
//! partition's first row. populate gives each thread a contiguous row range
//! (the orderkey `range` starts at the partition's first row) and its own RNG
//! seed. Peak RSS (`VmHWM`) is per process, so run one workload per process.

use populate::arrow::DistributionArrow;
use populate_bench::{
    BATCH, batch_bytes, batch_bytes_60, lineitem_numeric_from, numeric_schema_60,
    tpch_numeric_batch, tpchgen_init,
};
use rand::SeedableRng;
use rand::rngs::SmallRng;
use std::fs::read_to_string;
use std::hint::black_box;
use std::thread::scope;
use std::time::Instant;
use tpchgen::generators::LineItemGenerator;
use tpchgen_arrow::LineItemArrow;

#[derive(Default, Clone, Copy)]
struct Totals {
    rows: usize,
    bytes: usize,
}

impl Totals {
    fn add(&mut self, o: Totals) {
        self.rows += o.rows;
        self.bytes += o.bytes;
    }
}

/// Exact `lineitem` row counts for the standard scale factors (from tpchgen);
/// anything else is approximated as 6M rows per SF.
fn lineitem_rows(sf: f64) -> usize {
    match sf {
        1.0 => 6_001_215,
        10.0 => 59_986_052,
        100.0 => 600_037_902,
        _ => (sf * 6_000_000.0) as usize,
    }
}

fn tpch_full(sf: f64, part: i32, parts: i32, batch: usize) -> Totals {
    let mut t = Totals::default();
    let g = LineItemGenerator::new(sf, part, parts);
    for b in LineItemArrow::new(g).with_batch_size(batch) {
        let b = b.expect("batch");
        t.add(Totals {
            rows: b.num_rows(),
            bytes: batch_bytes_60(&b),
        });
        drop(black_box(b));
    }
    t
}

fn tpch_numeric(sf: f64, part: i32, parts: i32, batch: usize) -> Totals {
    let mut t = Totals::default();
    let schema = numeric_schema_60();
    let mut it = LineItemGenerator::new(sf, part, parts).iter();
    let mut rows = Vec::with_capacity(batch);
    loop {
        rows.clear();
        rows.extend(it.by_ref().take(batch));
        if rows.is_empty() {
            break;
        }
        let b = tpch_numeric_batch(&rows, &schema);
        t.add(Totals {
            rows: b.num_rows(),
            bytes: batch_bytes_60(&b),
        });
        drop(black_box(b));
    }
    t
}

fn populate_numeric(start: usize, end: usize, seed: u64, batch: usize) -> Totals {
    let mut t = Totals::default();
    let mut d = lineitem_numeric_from(start as i64 + 1);
    let mut rng = SmallRng::seed_from_u64(seed);
    let mut row = start;
    while row < end {
        let len = batch.min(end - row);
        let b = d.collect_record_batch(&mut rng, len).expect("batch");
        t.add(Totals {
            rows: b.num_rows(),
            bytes: batch_bytes(&b),
        });
        drop(black_box(b));
        row += len;
    }
    t
}

fn peak_rss_mib() -> f64 {
    read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("VmHWM:"))
                .and_then(|l| l.split_whitespace().nth(1)?.parse::<f64>().ok())
        })
        .map_or(f64::NAN, |kib| kib / 1024.0)
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let usage = "usage: scale <tpch-full|tpch-numeric|populate> <sf> [threads] [batch]";
    let workload = args.first().expect(usage).clone();
    let sf: f64 = args.get(1).expect(usage).parse().expect("sf");
    let threads: usize = args.get(2).map_or(1, |a| a.parse().expect("threads"));
    let batch: usize = args.get(3).map_or(BATCH, |a| a.parse().expect("batch"));

    let init = Instant::now();
    if workload.starts_with("tpch") {
        tpchgen_init();
    }
    let init = init.elapsed();

    let n = lineitem_rows(sf);
    let t0 = Instant::now();
    let total = scope(|s| {
        let handles: Vec<_> = (0..threads)
            .map(|i| {
                let workload = workload.as_str();
                s.spawn(move || {
                    let (part, parts) = (i as i32 + 1, threads as i32);
                    match workload {
                        "tpch-full" => tpch_full(sf, part, parts, batch),
                        "tpch-numeric" => tpch_numeric(sf, part, parts, batch),
                        "populate" => populate_numeric(
                            i * n / threads,
                            (i + 1) * n / threads,
                            i as u64,
                            batch,
                        ),
                        other => panic!("unknown workload {other}; {usage}"),
                    }
                })
            })
            .collect();
        let mut total = Totals::default();
        for h in handles {
            total.add(h.join().expect("worker"));
        }
        total
    });
    let secs = t0.elapsed().as_secs_f64();
    println!(
        "| {workload} | SF{sf} | {threads} | {} | {:.2} s | {:.1} M | {:.2} | {:.0} | {:.0} ms | {:.0} |",
        total.rows,
        secs,
        total.rows as f64 / secs / 1e6,
        total.bytes as f64 / (1u64 << 30) as f64,
        total.bytes as f64 / (1u64 << 20) as f64 / secs,
        init.as_secs_f64() * 1e3,
        peak_rss_mib(),
    );
}
