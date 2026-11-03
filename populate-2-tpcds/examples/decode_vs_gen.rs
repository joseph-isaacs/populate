//! Which is faster: generating a table to Arrow, or decoding it from Parquet?
//!
//! Per table (SF 10 by default), each table is encoded once to an in-memory
//! Parquet file (Snappy and zstd 3; untimed, with the Arrow schema embedded
//! so it decodes to the same types). Then, in randomly interleaved order,
//! REPS times each:
//!   gen ours / gen tpcgen : stream all batches from the generator
//!   decode snappy / zstd3 : stream all batches from the in-memory file
//! Single-threaded, 8000-row batches, batches dropped as they arrive.
//! Decoding reads from memory (no disk I/O): the best case for decode.
//!
//!   cargo run --release -p populate-2-tpcds --example decode_vs_gen [seed]

mod common;

use bytes::Bytes;
use common::{BATCH, TABLES, median, peak_rss_mb, seed_from_args, shuffle, source};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use populate_2_tpch::values::POOL_BUFFER;
use std::collections::BTreeMap;
use std::time::Instant;

const SF: f64 = 10.0;
const REPS: usize = 3;
const MODES: [&str; 4] = ["gen ours", "gen tpcgen", "decode snappy", "decode zstd3"];

fn encode(table: &str, codec: Compression) -> Bytes {
    let mut batches = source(table, true, SF).peekable();
    let schema = batches.peek().expect("non-empty").schema();
    let props = WriterProperties::builder().set_compression(codec).build();
    let mut w = ArrowWriter::try_new(Vec::new(), schema, Some(props)).unwrap();
    for b in batches {
        w.write(&b).unwrap();
    }
    Bytes::from(w.into_inner().unwrap())
}

fn decode(file: &Bytes) -> u64 {
    let reader = ParquetRecordBatchReaderBuilder::try_new(file.clone())
        .unwrap()
        .with_batch_size(BATCH)
        .build()
        .unwrap();
    reader.map(|b| b.unwrap().num_rows() as u64).sum()
}

fn generate(table: &str, ours: bool) -> u64 {
    source(table, ours, SF).map(|b| b.num_rows() as u64).sum()
}

fn main() {
    let seed = seed_from_args();
    println!("SF {SF}, batch {BATCH}, {REPS} reps, shuffle seed {seed}");
    tpchgen::text::TextPool::get_or_init_default();
    std::sync::LazyLock::force(&POOL_BUFFER);

    let mut files: BTreeMap<(&str, &str), Bytes> = BTreeMap::new();
    for t in TABLES {
        files.insert((t, "decode snappy"), encode(t, Compression::SNAPPY));
        files.insert(
            (t, "decode zstd3"),
            encode(t, Compression::ZSTD(ZstdLevel::try_new(3).unwrap())),
        );
        println!(
            "encoded {t}: snappy {:.1} MB, zstd3 {:.1} MB",
            files[&(t, "decode snappy")].len() as f64 / 1e6,
            files[&(t, "decode zstd3")].len() as f64 / 1e6
        );
    }

    let mut trials: Vec<(&str, &str)> = Vec::new();
    for t in TABLES {
        for m in MODES {
            for _ in 0..REPS {
                trials.push((t, m));
            }
        }
    }
    shuffle(&mut trials, seed);

    let mut times: BTreeMap<(&str, &str), Vec<f64>> = BTreeMap::new();
    let mut rows_of: BTreeMap<&str, u64> = BTreeMap::new();
    let n = trials.len();
    for (i, &(t, m)) in trials.iter().enumerate() {
        let start = Instant::now();
        let rows = match m {
            "gen ours" => generate(t, true),
            "gen tpcgen" => generate(t, false),
            _ => decode(&files[&(t, m)]),
        };
        let ms = start.elapsed().as_secs_f64() * 1e3;
        println!(
            "[{:>3}/{n}] {t:<26} {m:<13} {ms:>9.1} ms  {rows} rows",
            i + 1
        );
        times.entry((t, m)).or_default().push(ms);
        let prev = rows_of.insert(t, rows);
        assert!(
            prev.is_none() || prev == Some(rows),
            "row count differs for {t}"
        );
    }

    println!();
    println!(
        "{:<26} {:>11} | {:>10} {:>10} {:>13} {:>13} | ms (median of {REPS}); decode vs gen ours",
        "table", "rows", "gen ours", "gen tpcgen", "decode snappy", "decode zstd3"
    );
    for t in TABLES {
        let m: Vec<f64> = MODES.iter().map(|m| median(&times[&(t, *m)])).collect();
        println!(
            "{t:<26} {:>11} | {:>10.1} {:>10.1} {:>13.1} {:>13.1} | snappy {:.1}x, zstd3 {:.1}x slower than gen ours",
            rows_of[t],
            m[0],
            m[1],
            m[2],
            m[3],
            m[2] / m[0],
            m[3] / m[0]
        );
    }
    println!("\npeak RSS {} MB", peak_rss_mb());
}
