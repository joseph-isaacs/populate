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

use arrow_array::RecordBatch;
use arrow_schema::ArrowError;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use populate_2_tpch::values::POOL_BUFFER;
use std::collections::BTreeMap;
use std::io::Write;
use std::time::Instant;
use tpcdsgen::config::SessionBuilder;

const SF: f64 = 10.0;
const BATCH: usize = 8000;
const REPS: usize = 3;

type Batches = Box<dyn Iterator<Item = RecordBatch>>;

/// Discards bytes, counts them.
struct Counter(u64);

impl Write for Counter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0 += buf.len() as u64;
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn ok(r: Result<RecordBatch, ArrowError>) -> RecordBatch {
    r.expect("reference batch")
}

fn source(table: &str, ours: bool) -> Batches {
    use tpchgen::generators::*;
    use tpchgen_arrow::*;
    let ds = || SessionBuilder::new().with_scale_factor(SF).build().unwrap();
    match (table, ours) {
        ("H part", true) => Box::new(populate_2_tpch::batches::<populate_2_tpch::Part>(SF, BATCH)),
        ("H part", false) => Box::new(PartArrow::new(PartGenerator::new(SF, 1, 1)).map(ok)),
        ("H partsupp", true) => Box::new(populate_2_tpch::batches::<populate_2_tpch::PartSupp>(
            SF, BATCH,
        )),
        ("H partsupp", false) => {
            Box::new(PartSuppArrow::new(PartSuppGenerator::new(SF, 1, 1)).map(ok))
        }
        ("H customer", true) => Box::new(populate_2_tpch::batches::<populate_2_tpch::Customer>(
            SF, BATCH,
        )),
        ("H customer", false) => {
            Box::new(CustomerArrow::new(CustomerGenerator::new(SF, 1, 1)).map(ok))
        }
        ("H supplier", true) => Box::new(populate_2_tpch::batches::<populate_2_tpch::Supplier>(
            SF, BATCH,
        )),
        ("H supplier", false) => {
            Box::new(SupplierArrow::new(SupplierGenerator::new(SF, 1, 1)).map(ok))
        }
        ("DS inventory", true) => Box::new(
            populate_2_tpcds::batches::<populate_2_tpcds::Inventory>(SF, BATCH),
        ),
        ("DS inventory", false) => Box::new(tpcdsgen_arrow::InventoryArrow::new(ds()).map(ok)),
        ("DS customer_demographics", true) => Box::new(populate_2_tpcds::batches::<
            populate_2_tpcds::CustomerDemographics,
        >(SF, BATCH)),
        ("DS customer_demographics", false) => {
            Box::new(tpcdsgen_arrow::CustomerDemographicsArrow::new(ds()).map(ok))
        }
        _ => unreachable!("{table}"),
    }
}

/// Returns (rows, parquet bytes or 0).
fn run(table: &str, ours: bool, mode: &str) -> (u64, u64) {
    let mut batches = source(table, ours).peekable();
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

fn peak_rss_mb() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines().find(|l| l.starts_with("VmHWM")).and_then(|l| {
                l.split_whitespace()
                    .nth(1)
                    .map(|kb| kb.parse::<u64>().unwrap() / 1024)
            })
        })
        .unwrap_or(0)
}

fn main() {
    let seed: u64 = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64
        });
    println!("SF {SF}, batch {BATCH}, {REPS} reps, shuffle seed {seed}");

    // Shared one-time setup (text pool) outside timing, for both sides.
    tpchgen::text::TextPool::get_or_init_default();
    std::sync::LazyLock::force(&POOL_BUFFER);

    let tables = [
        "H part",
        "H partsupp",
        "H customer",
        "H supplier",
        "DS inventory",
        "DS customer_demographics",
    ];
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
    // Fisher-Yates with xorshift64*
    let mut x = seed | 1;
    for i in (1..trials.len()).rev() {
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        let j = (x.wrapping_mul(0x2545F4914F6CDD1D) % (i as u64 + 1)) as usize;
        trials.swap(i, j);
    }

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

    let median = |v: &[f64]| {
        let mut v = v.to_vec();
        v.sort_by(f64::total_cmp);
        v[v.len() / 2]
    };
    println!();
    println!(
        "{:<26} {:>11} {:<6} | {:>27} | {:>27} | {:>14} | {}",
        "table",
        "rows",
        "mode",
        "ours median (min) ms",
        "tpcgen median (min) ms",
        "speedup med/min",
        "parquet MB ours/tpcgen"
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
