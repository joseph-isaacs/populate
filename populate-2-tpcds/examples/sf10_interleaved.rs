//! SF 10 head-to-head, ours vs tpchgen-rs, in randomly interleaved order.
//!
//! Every (table, generator, mode) runs REPS times; all runs are shuffled
//! (seed printed, pass it as the first argument to reproduce) and executed
//! one after another, single-threaded, 8000-row batches.
//!
//! Modes: `arrow` = generate batches; `snappy` / `zstd3` = generate and
//! encode to Parquet (ArrowWriter, default properties + codec).
//!
//! Memory/disk: batches are streamed and dropped one at a time, and Parquet
//! goes to a byte-counting writer that discards its input, so nothing is held
//! for a whole table and nothing touches disk.
//!
//!   cargo run --release -p populate-2-tpcds --example sf10_interleaved [seed]

mod common;

use common::{BATCH, Counter, TABLES, median, peak_rss_mb, seed_from_args, shuffle, source};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use populate_2_tpch::values::POOL_BUFFER;
use std::collections::BTreeMap;
use std::time::Instant;

const SF: f64 = 10.0;
const REPS: usize = 3;

/// Returns (rows, parquet bytes or 0).
fn run(table: &str, ours: bool, mode: &str) -> (u64, u64) {
    let mut batches = source(table, ours, SF).peekable();
    let mut rows = 0;
    if mode == "arrow" {
        for b in batches {
            rows += b.num_rows() as u64;
        }
        return (rows, 0);
    }
    let codec = match mode {
        "snappy" => Compression::SNAPPY,
        _ => Compression::ZSTD(ZstdLevel::try_new(3).unwrap()),
    };
    let props = WriterProperties::builder().set_compression(codec).build();
    let schema = batches.peek().expect("non-empty").schema();
    let mut w = ArrowWriter::try_new(Counter(0), schema, Some(props)).unwrap();
    for b in batches {
        rows += b.num_rows() as u64;
        w.write(&b).unwrap();
    }
    let out = w.into_inner().unwrap();
    (rows, out.0)
}

fn main() {
    let seed = seed_from_args();
    println!("SF {SF}, batch {BATCH}, {REPS} reps, shuffle seed {seed}");

    // Shared one-time setup (text pool) outside timing, for both sides.
    tpchgen::text::TextPool::get_or_init_default();
    std::sync::LazyLock::force(&POOL_BUFFER);

    let tables = TABLES;
    let modes = ["arrow", "snappy", "zstd3"];
    let mut trials = Vec::new();
    for t in tables {
        for m in modes {
            for ours in [true, false] {
                for _ in 0..REPS {
                    trials.push((t, m, ours));
                }
            }
        }
    }
    shuffle(&mut trials, seed);

    let mut times: BTreeMap<(&str, &str, bool), Vec<f64>> = BTreeMap::new();
    let mut sizes: BTreeMap<(&str, &str, bool), u64> = BTreeMap::new();
    let mut rows_of: BTreeMap<&str, u64> = BTreeMap::new();
    let n = trials.len();
    for (i, &(t, m, ours)) in trials.iter().enumerate() {
        let start = Instant::now();
        let (rows, bytes) = run(t, ours, m);
        let ms = start.elapsed().as_secs_f64() * 1e3;
        let who = if ours { "ours" } else { "tpcgen" };
        println!(
            "[{:>3}/{n}] {t:<26} {m:<6} {who:<6} {ms:>9.1} ms  {rows} rows",
            i + 1
        );
        times.entry((t, m, ours)).or_default().push(ms);
        if bytes > 0 {
            sizes.insert((t, m, ours), bytes);
        }
        let prev = rows_of.insert(t, rows);
        assert!(
            prev.is_none() || prev == Some(rows),
            "row count differs for {t}"
        );
    }

    println!();
    println!(
        "{:<26} {:>11} {:<6} | {:>27} | {:>27} | {:>14} | parquet MB ours/tpcgen",
        "table",
        "rows",
        "mode",
        "ours median (min) ms",
        "tpcgen median (min) ms",
        "speedup med/min",
    );
    for t in tables {
        for m in modes {
            let o = &times[&(t, m, true)];
            let r = &times[&(t, m, false)];
            let (om, omin) = (median(o), o.iter().copied().fold(f64::MAX, f64::min));
            let (rm, rmin) = (median(r), r.iter().copied().fold(f64::MAX, f64::min));
            let size = match (sizes.get(&(t, m, true)), sizes.get(&(t, m, false))) {
                (Some(a), Some(b)) => format!("{:.1} / {:.1}", *a as f64 / 1e6, *b as f64 / 1e6),
                _ => String::new(),
            };
            println!(
                "{t:<26} {:>11} {m:<6} | {om:>12.1} ({omin:>10.1}) | {rm:>12.1} ({rmin:>10.1}) | {:>5.2}x / {:>5.2}x | {size}",
                rows_of[t],
                rm / om,
                rmin / omin
            );
        }
    }
    println!("\npeak RSS {} MB", peak_rss_mb());
}
