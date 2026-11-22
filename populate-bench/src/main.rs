//! Prints a Markdown table comparing populate and tpchgen:
//! rows/s, output MB/s, allocations and peak heap per workload.
//!
//! `cargo run -p populate-bench --release [-- <rows> [<repeats>]]`
//!
//! Timing is the best of `repeats` runs (default 5) after one warm-up run.
//! Allocations and peak heap are measured on a separate run, with peak taken
//! relative to the heap in use when the run starts.

use populate_bench::{Group, WORKLOADS, tpchgen_init};
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::time::{Duration, Instant};

struct Tracking;

static CURRENT: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);
static ALLOCS: AtomicUsize = AtomicUsize::new(0);

fn grew(by: usize) {
    let now = CURRENT.fetch_add(by, Relaxed) + by;
    PEAK.fetch_max(now, Relaxed);
}

unsafe impl GlobalAlloc for Tracking {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Relaxed);
        grew(layout.size());
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        CURRENT.fetch_sub(layout.size(), Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOCS.fetch_add(1, Relaxed);
        if new_size > layout.size() {
            grew(new_size - layout.size());
        } else {
            CURRENT.fetch_sub(layout.size() - new_size, Relaxed);
        }
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: Tracking = Tracking;

struct Mem {
    allocs: usize,
    peak: usize,
}

fn measure_mem<R>(f: impl FnOnce() -> R) -> (R, Mem) {
    let base = CURRENT.load(Relaxed);
    PEAK.store(base, Relaxed);
    let allocs = ALLOCS.load(Relaxed);
    let r = f();
    let mem = Mem {
        allocs: ALLOCS.load(Relaxed) - allocs,
        peak: PEAK.load(Relaxed) - base,
    };
    (r, mem)
}

fn mib(bytes: usize) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

fn group_title(g: Group) -> &'static str {
    match g {
        Group::NumericArrow => "lineitem, 11 numeric columns -> Arrow",
        Group::NumericParquet => "lineitem, 11 numeric columns -> Parquet",
        Group::ListArrow => "orders with list<line price> -> Arrow",
        Group::ListParquet => "orders with list<line price> -> Parquet",
        Group::TpchFull => "tpchgen full lineitem (16 columns, strings included)",
    }
}

fn main() {
    let mut args = std::env::args().skip(1);
    let n: usize = args.next().map_or(1 << 20, |a| a.parse().expect("rows"));
    let repeats: usize = args.next().map_or(5, |a| a.parse().expect("repeats"));

    let t = Instant::now();
    let ((), init) = measure_mem(tpchgen_init);
    println!(
        "tpchgen one-time text pool init: {:.0} ms, {:.0} MiB heap (not counted below)\n",
        t.elapsed().as_secs_f64() * 1e3,
        mib(init.peak)
    );
    println!("n = {n} rows (list workloads: ~n list values), best of {repeats}, one thread\n");

    let mut last = None;
    for w in WORKLOADS {
        if last != Some(w.group) {
            println!("\n**{}**\n", group_title(w.group));
            println!(
                "| workload | rows | time | rows/s | output MiB | MiB/s | allocs | peak heap MiB |"
            );
            println!("|---|---:|---:|---:|---:|---:|---:|---:|");
            last = Some(w.group);
        }
        let out = (w.run)(n); // warm-up
        let best = (0..repeats)
            .map(|_| {
                let t = Instant::now();
                std::hint::black_box((w.run)(n));
                t.elapsed()
            })
            .min()
            .unwrap_or(Duration::ZERO);
        let (_, mem) = measure_mem(|| (w.run)(n));
        let s = best.as_secs_f64();
        println!(
            "| {} | {} | {:.1} ms | {:.1} M | {:.1} | {} | {} | {:.1} |",
            w.name,
            out.rows,
            s * 1e3,
            out.rows as f64 / s / 1e6,
            mib(out.bytes),
            if out.bytes == 0 {
                "-".to_string()
            } else {
                format!("{:.0}", mib(out.bytes) / s)
            },
            mem.allocs,
            mib(mem.peak),
        );
    }
}
