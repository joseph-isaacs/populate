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
//! - `tpch-direct`: tpchgen's RNGs and row logic for just those columns,
//!   straight into column `Vec`s (same values; one thread only).
//! - `tpch-lanes`: the same columns, byte-identical to tpchgen, from the
//!   lane-parallel TPC-H RNG ([`TpchLanes`]); SF 10 only.
//! - `populate`: populate's 11 numeric columns (same row count as tpchgen).
//!
//! With `threads > 1` each thread generates one partition. tpchgen uses its
//! own `part`/`part_count` support, which skips its RNGs ahead to the
//! partition's first row. populate cuts the rows into fixed chunks of
//! 2^18, each seeded by its index (the orderkey `range` starts at the
//! chunk's first row), and deals them out to the threads, so its output does
//! not depend on the thread count: `SCALE_CHECK=1` prints a checksum to
//! show it. `tpch-lanes` deals out fixed chunks of orders the same way,
//! each started with [`TpchLanes::at_order`]. For `tpch-numeric` and
//! `tpch-lanes` the checksum is a sum of per-row hashes ([`row_hash`]), so
//! the two can be compared at any thread count. Peak RSS (`VmHWM`) is per process, so run one workload per process.

use arrow_array::RecordBatch;
use arrow_array::cast::AsArray;
use arrow_array::types::{Int32Type, Int64Type};
use populate_bench::fair::{Cols, SF, TpchDirect, cols_to_batch};
use populate_bench::lanes::{LANES, ORDERS, ORDERS_PER_BLOCK, TpchLanes};
use populate_bench::{
    BATCH, batch_bytes, batch_bytes_60, numeric_schema_60, p2_numeric_batches, tpch_numeric_batch,
    tpchgen_init,
};
use rand::SeedableRng;
use rand::rngs::SmallRng;
use std::env::var_os;
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
    /// populate only, with `SCALE_CHECK=1`: see [`chunk_hash`].
    hash: u64,
}

impl Totals {
    fn add(&mut self, o: Totals) {
        self.rows += o.rows;
        self.bytes += o.bytes;
        self.hash = self.hash.wrapping_add(o.hash);
    }
}

/// Exact `lineitem` row counts for the standard scale factors (from tpchgen);
/// anything else is approximated as 6M rows per SF.
fn lineitem_rows(sf: f64) -> usize {
    match sf {
        1.0 => 6_001_215,
        10.0 => 59_986_052,
        100.0 => 600_037_902,
        1000.0 => 5_999_989_709,
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
            hash: 0,
        });
        drop(black_box(b));
    }
    t
}

/// Order-independent checksum of a batch: each row's values are folded in
/// column order, and the row hashes are summed. Rows can then come in any
/// order or batching (tpchgen's partitions vs `tpch-lanes`' chunks) and the
/// totals still match exactly when the rows do.
fn row_hash(b: &RecordBatch) -> u64 {
    let mut rows = vec![0xcbf2_9ce4_8422_2325u64; b.num_rows()];
    for c in b.columns() {
        let mut fold = |vals: &mut dyn Iterator<Item = i64>| {
            for (h, v) in rows.iter_mut().zip(vals) {
                *h = (*h ^ v as u64).wrapping_mul(0x100_0000_01b3);
            }
        };
        if let Some(a) = c.as_primitive_opt::<Int64Type>() {
            fold(&mut a.values().iter().copied());
        } else {
            fold(
                &mut c
                    .as_primitive::<Int32Type>()
                    .values()
                    .iter()
                    .map(|&v| v as i64),
            );
        }
    }
    rows.iter()
        .fold(0u64, |acc, &h| acc.wrapping_add(h ^ (h >> 29)))
}

fn tpch_numeric(sf: f64, part: i32, parts: i32, batch: usize, check: bool) -> Totals {
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
            hash: if check { row_hash(&b) } else { 0 },
        });
        drop(black_box(b));
    }
    t
}

/// tpchgen's RNGs and row logic for the numeric columns only, straight into
/// columns (same values as `tpch-numeric`). No partition support.
fn tpch_direct(n: usize, batch: usize) -> Totals {
    let mut t = Totals::default();
    let mut g = TpchDirect::new(false);
    let mut left = n;
    while left > 0 {
        let len = batch.min(left);
        let mut c = Cols::default();
        g.fill(len, &mut c);
        let b = cols_to_batch(c);
        t.add(Totals {
            rows: b.num_rows(),
            bytes: batch_bytes_60(&b),
            hash: 0,
        });
        drop(black_box(b));
        left -= len;
    }
    t
}

/// Orders per `tpch-lanes` chunk: ~2^18 lineitems, a multiple of the
/// block size.
const LANE_CHUNK: usize = 32 * ORDERS_PER_BLOCK;

/// The whole SF 10 `lineitem` table, byte-identical to tpchgen, from the
/// lane-parallel RNG. Chunks of [`LANE_CHUNK`] orders are dealt out to the
/// threads like populate's, each starting with a skip-ahead, so the rows
/// don't depend on the thread count. Batches are ~`BATCH` rows (whole
/// orders), not exactly `batch`.
fn tpch_lanes(thread: usize, threads: usize, check: bool) -> Totals {
    let mut t = Totals::default();
    for c in (thread..ORDERS.div_ceil(LANE_CHUNK)).step_by(threads) {
        let (start, end) = (c * LANE_CHUNK, ((c + 1) * LANE_CHUNK).min(ORDERS));
        let mut g = TpchLanes::at_order(start);
        let mut o = start;
        while o < end {
            let len = ORDERS_PER_BLOCK.min(end - o);
            debug_assert_eq!(len % LANES, 0);
            let mut cols = Cols::default();
            g.fill_orders(len, &mut cols);
            let b = cols_to_batch(cols);
            t.add(Totals {
                rows: b.num_rows(),
                bytes: batch_bytes_60(&b),
                hash: if check { row_hash(&b) } else { 0 },
            });
            drop(black_box(b));
            o += len;
        }
    }
    t
}

/// populate's rows are cut into fixed [`CHUNK`]s, each with its own RNG
/// seeded by the chunk's index, and thread `i` of `threads` takes chunks
/// `i, i + threads, ...`. So the output (each chunk's batches) is the same
/// whatever the thread count; only which thread makes a chunk changes.
const CHUNK: usize = 1 << 18;

/// An order-independent checksum of populate's output: each chunk's values
/// are folded in order (FNV-style), then the chunks are summed, so threads
/// can finish in any order. Set `SCALE_CHECK=1` to compute it (one extra
/// pass over the data, so off by default).
fn chunk_hash(b: &RecordBatch, mut h: u64) -> u64 {
    for c in b.columns() {
        let mut fold = |v: i64| h = (h ^ v as u64).wrapping_mul(0x100_0000_01b3);
        if let Some(a) = c.as_primitive_opt::<Int64Type>() {
            a.values().iter().for_each(|&v| fold(v));
        } else {
            c.as_primitive::<Int32Type>()
                .values()
                .iter()
                .for_each(|&v| fold(v as i64));
        }
    }
    h
}

fn populate_numeric(n: usize, thread: usize, threads: usize, batch: usize, check: bool) -> Totals {
    let mut t = Totals::default();
    for c in (thread..n.div_ceil(CHUNK)).step_by(threads) {
        let (start, end) = (c * CHUNK, ((c + 1) * CHUNK).min(n));
        let mut rng = SmallRng::seed_from_u64(c as u64);
        let mut h = 0xcbf2_9ce4_8422_2325;
        p2_numeric_batches(start as i64 + 1, end - start, batch, &mut rng, |b| {
            t.add(Totals {
                rows: b.num_rows(),
                bytes: batch_bytes(&b),
                hash: 0,
            });
            if check {
                h = chunk_hash(&b, h);
            }
            drop(black_box(b));
        });
        t.hash = t.hash.wrapping_add(h);
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
    let usage = "usage: scale <tpch-full|tpch-numeric|tpch-direct|tpch-lanes|populate> <sf> [threads] [batch]";
    let workload = args.first().expect(usage).clone();
    let sf: f64 = args.get(1).expect(usage).parse().expect("sf");
    let threads: usize = args.get(2).map_or(1, |a| a.parse().expect("threads"));
    let batch: usize = args.get(3).map_or(BATCH, |a| a.parse().expect("batch"));

    let init = Instant::now();
    if workload.starts_with("tpch") && workload != "tpch-lanes" {
        tpchgen_init();
    }
    let init = init.elapsed();

    let n = lineitem_rows(sf);
    let check = var_os("SCALE_CHECK").is_some_and(|v| v == "1");
    let t0 = Instant::now();
    let total = scope(|s| {
        let handles: Vec<_> = (0..threads)
            .map(|i| {
                let workload = workload.as_str();
                s.spawn(move || {
                    let (part, parts) = (i as i32 + 1, threads as i32);
                    match workload {
                        "tpch-full" => tpch_full(sf, part, parts, batch),
                        "tpch-numeric" => tpch_numeric(sf, part, parts, batch, check),
                        "tpch-lanes" => {
                            assert_eq!(sf, SF, "tpch-lanes is fixed at SF{SF}");
                            tpch_lanes(i, threads, check)
                        }
                        "tpch-direct" => {
                            assert_eq!(threads, 1, "tpch-direct is single-threaded");
                            assert_eq!(sf, SF, "tpch-direct is fixed at SF{SF}");
                            tpch_direct(n, batch)
                        }
                        "populate" => populate_numeric(n, i, threads, batch, check),
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
    if check {
        println!("checksum {:016x}", total.hash);
    }
}
