//! Generate a synthetic table as Parquet, or in memory only (to time
//! generation alone).
//!
//! ```sh
//! cargo run -p populate-synth --release --bin synth-gen -- taxi --rows 10000000 --out taxi.parquet
//! cargo run -p populate-synth --release --bin synth-gen -- embeddings --rows 1000000 --dim 768
//! ```

#[cfg(feature = "mimalloc")]
use mimalloc::MiMalloc;
use populate_synth::{
    DEFAULT_BATCH_SIZE, DEFAULT_CHUNK_ROWS, Embeddings, Features, Hits, Shape, ShapeName, Synth,
    Taxi,
};
use std::env::args;
use std::fs::{File, metadata};
use std::io::BufWriter;
use std::process::exit;
use std::time::Instant;

/// mimalloc: batch buffers are reused instead of returned to the OS and
/// faulted in again, as glibc does with a batch's worth of small frees
#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

const USAGE: &str = "usage: synth-gen <taxi|hits|features|embeddings> [--rows N] [--seed S] \
[--threads T] [--batch-size B] [--chunk-rows C] [--dim D] [--out FILE.parquet]

Without --out, batches are generated on all threads and dropped (generation time only).";

struct Args {
    shape: ShapeName,
    rows: u64,
    seed: u64,
    threads: usize,
    batch_size: usize,
    chunk_rows: u64,
    dim: usize,
    out: Option<String>,
}

fn fail(msg: &str) -> ! {
    eprintln!("{msg}\n\n{USAGE}");
    exit(2)
}

fn parse() -> Args {
    let mut it = args().skip(1);
    let shape = it
        .next()
        .and_then(|s| ShapeName::parse(&s))
        .unwrap_or_else(|| fail("first argument must be a shape"));
    let mut a = Args {
        shape,
        rows: 1_000_000,
        seed: 0,
        threads: 0,
        batch_size: DEFAULT_BATCH_SIZE,
        chunk_rows: DEFAULT_CHUNK_ROWS,
        dim: 384,
        out: None,
    };
    while let Some(flag) = it.next() {
        let mut val = || {
            it.next()
                .unwrap_or_else(|| fail(&format!("{flag} needs a value")))
        };
        let num = |v: String| -> u64 {
            v.replace('_', "")
                .parse()
                .unwrap_or_else(|_| fail(&format!("bad number {v}")))
        };
        match flag.as_str() {
            "--rows" => a.rows = num(val()),
            "--seed" => a.seed = num(val()),
            "--threads" => a.threads = num(val()) as usize,
            "--batch-size" => a.batch_size = num(val()) as usize,
            "--chunk-rows" => a.chunk_rows = num(val()),
            "--dim" => a.dim = num(val()) as usize,
            "--out" => a.out = Some(val()),
            "-h" | "--help" => {
                println!("{USAGE}");
                exit(0)
            }
            f => fail(&format!("unknown flag {f}")),
        }
    }
    if a.batch_size == 0 || a.chunk_rows == 0 || a.dim == 0 {
        fail("--batch-size, --chunk-rows and --dim must be > 0");
    }
    a
}

fn run<S: Shape>(shape: S, a: &Args) {
    let t = Synth::new(shape, a.rows)
        .batch_size(a.batch_size)
        .chunk_rows(a.chunk_rows);
    let start = Instant::now();
    let what = match &a.out {
        Some(path) => {
            let f = File::create(path).unwrap_or_else(|e| fail(&format!("{path}: {e}")));
            t.write_parquet(BufWriter::with_capacity(1 << 20, f), a.threads, None)
                .and_then(|w| w.into_inner().map_err(|e| e.into_error().into()))
                .unwrap_or_else(|e| fail(&format!("writing {path}: {e}")));
            let bytes = metadata(path).map(|m| m.len()).unwrap_or(0);
            format!("{path} ({:.1} MB)", bytes as f64 / 1e6)
        }
        None => {
            t.for_each_batch(a.threads, drop);
            "memory (dropped)".to_owned()
        }
    };
    let secs = start.elapsed().as_secs_f64();
    eprintln!(
        "{}: {} rows to {what} in {secs:.3}s ({:.1} M rows/s)",
        a.shape.as_str(),
        a.rows,
        a.rows as f64 / secs / 1e6
    );
}

fn main() {
    let a = parse();
    match a.shape {
        ShapeName::Taxi => run(Taxi::new(a.seed), &a),
        ShapeName::Hits => run(Hits::new(a.seed), &a),
        ShapeName::Features => run(Features::new(a.seed), &a),
        ShapeName::Embeddings => run(Embeddings::with_sizes(a.seed, a.dim, 256, 0.6), &a),
    }
}
