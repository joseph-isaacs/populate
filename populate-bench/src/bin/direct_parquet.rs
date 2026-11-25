//! Distribution → Parquet, three ways, at several row and thread counts:
//!
//! - `direct`: `write_dist_parquet`, from the collected `Vec`s, no Arrow.
//! - `table`: the same columns as a `Table` (`Table::write_parquet`:
//!   distribution → `RecordBatch` → populate-table's encoder, arrow-rs for
//!   what it does not cover).
//! - `arrow-rs`: the same `RecordBatch`es written by arrow-rs alone:
//!   `ArrowWriter` on one thread, or one row group per worker through
//!   `ArrowColumnWriter`s (arrow-rs's documented parallel pattern).
//!
//! Times include generation and are the best of `--reps` runs; output goes
//! to a byte counter. Usage:
//!
//! ```text
//! direct_parquet [--rows 1000000,4000000,16000000] [--threads 1,2,4]
//!                [--shapes numbers,strings,lists,mixed] [--reps 3] [--plain] [--snappy]
//! ```

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use mimalloc::MiMalloc;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_writer::compute_leaves;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use populate::Distribution;
use populate::arrow::{DistributionArrow, IntoArrowColumns};
use populate::dist::{chars, choose, list, range, text, uniform};
use populate::parquet::ParquetColumns;
use populate::table::direct::group_rng;
use populate::table::{ParquetOptions, Table, par_ordered, write_dist_parquet};
use populate_bench::CountingWriter;
use std::env::args;
use std::time::Instant;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

const ROWS_PER_GROUP: usize = 1 << 20;
const BATCH: usize = 8192;
const SEED: u64 = 42;

const WORDS: &[&str] = &[
    "furiously",
    "quickly",
    "carefully",
    "blithely",
    "slyly",
    "ironic",
    "final",
    "regular",
    "express",
    "pending",
    "deposits",
    "packages",
    "accounts",
    "requests",
    "theodolites",
];
const CATEGORIES: &[&str] = &[
    "AUTOMOBILE",
    "BUILDING",
    "FURNITURE",
    "HOUSEHOLD",
    "MACHINERY",
    "a category name over 12",
];
const ALNUM: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

fn numbers() -> impl Distribution<Item = (i64, i64, i32, f64)> + Clone + Send + Sync + 'static {
    (
        range(0i64, 1),
        uniform(i64::MIN, i64::MAX),
        uniform(0i32, 1000),
        uniform(0.0f64, 1.0),
    )
}

fn strings()
-> impl Distribution<Item = (&'static str, String, String)> + Clone + Send + Sync + 'static {
    (choose(CATEGORIES), text(WORDS, 2, 8), chars(ALNUM, 5, 20))
}

fn lists() -> impl Distribution<Item = (Vec<i64>, Vec<f32>)> + Clone + Send + Sync + 'static {
    (
        list(uniform(0usize, 9), uniform(0i64, 1_000_000)),
        list(uniform(0usize, 5), uniform(0.0f32, 1.0)),
    )
}

/// Time `f`, best of `reps`; returns seconds and bytes written.
fn time(reps: usize, mut f: impl FnMut() -> usize) -> (f64, usize) {
    let mut best = f64::MAX;
    let mut bytes = 0;
    for _ in 0..reps {
        let t = Instant::now();
        bytes = f();
        best = best.min(t.elapsed().as_secs_f64());
    }
    (best, bytes)
}

fn direct<D>(d: &D, rows: u64, threads: usize, props: &WriterProperties) -> usize
where
    D: Distribution + Clone + Sync,
    populate::Columns<D::Item>: ParquetColumns,
{
    let opts = ParquetOptions {
        rows_per_group: ROWS_PER_GROUP,
        threads,
        seed: SEED,
        props: props.clone(),
    };
    write_dist_parquet(d, rows, CountingWriter(0), &opts)
        .unwrap()
        .0
}

/// Row group `g` as one `RecordBatch`, columns `c0, c1, ...`.
fn group_batch<D>(d: &D, rows: u64, g: u64) -> RecordBatch
where
    D: Distribution + Clone,
    populate::Columns<D::Item>: IntoArrowColumns,
{
    let per = ROWS_PER_GROUP as u64;
    let mut d = d.clone();
    d.skip(g * per);
    let len = (rows - g * per).min(per) as usize;
    d.collect_record_batch(&mut group_rng(SEED, g), len)
        .unwrap()
}

fn arrow_rs<D>(d: &D, rows: u64, threads: usize, props: &WriterProperties) -> usize
where
    D: Distribution + Clone + Sync,
    populate::Columns<D::Item>: IntoArrowColumns,
{
    let groups = rows.div_ceil(ROWS_PER_GROUP as u64);
    let schema: SchemaRef = group_batch(d, 1, 0).schema();
    if threads == 1 {
        let mut w = ArrowWriter::try_new(CountingWriter(0), schema, Some(props.clone())).unwrap();
        for g in 0..groups {
            w.write(&group_batch(d, rows, g)).unwrap();
            w.flush().unwrap();
        }
        return w.into_inner().unwrap().0;
    }
    let (mut file, factory) =
        ArrowWriter::try_new(CountingWriter(0), schema.clone(), Some(props.clone()))
            .unwrap()
            .into_serialized_writer()
            .unwrap();
    par_ordered(
        groups,
        threads,
        |g| {
            let batch = group_batch(d, rows, g);
            let mut writers = factory.create_column_writers(g as usize).unwrap();
            let mut w = writers.iter_mut();
            for (field, array) in schema.fields().iter().zip(batch.columns()) {
                for leaf in compute_leaves(field, array).unwrap() {
                    w.next().unwrap().write(&leaf).unwrap();
                }
            }
            writers
                .into_iter()
                .map(|w| w.close().unwrap())
                .collect::<Vec<_>>()
        },
        |chunks| {
            let mut rg = file.next_row_group()?;
            for c in chunks {
                c.append_to_row_group(&mut rg)?;
            }
            rg.close().map(|_| ())
        },
    )
    .unwrap();
    file.into_inner().unwrap().0
}

fn table_path(t: &Table, threads: usize, props: &WriterProperties) -> usize {
    let props = props
        .clone()
        .into_builder()
        .set_max_row_group_row_count(Some(ROWS_PER_GROUP))
        .build();
    t.write_parquet(CountingWriter(0), BATCH, threads, Some(props))
        .unwrap()
        .0
}

fn table(shape: &str, rows: u64) -> Table {
    let t = Table::new("t", rows);
    let num = |t: Table| {
        t.col("n0", range(0i64, 1))
            .col("n1", uniform(i64::MIN, i64::MAX))
            .col("n2", uniform(0i32, 1000))
            .col("n3", uniform(0.0f64, 1.0))
    };
    let st = |t: Table| {
        t.col("s0", choose(CATEGORIES))
            .col("s1", text(WORDS, 2, 8))
            .col("s2", chars(ALNUM, 5, 20))
    };
    let li = |t: Table| {
        t.col("l0", list(uniform(0usize, 9), uniform(0i64, 1_000_000)))
            .col("l1", list(uniform(0usize, 5), uniform(0.0f32, 1.0)))
    };
    match shape {
        "numbers" => num(t),
        "strings" => st(t),
        "lists" => li(t),
        _ => li(st(num(t))),
    }
}

fn list_arg(name: &str, default: &str) -> Vec<String> {
    let a: Vec<String> = args().collect();
    let v = a
        .iter()
        .position(|x| x == name)
        .map_or(default.to_string(), |i| a[i + 1].clone());
    v.split(',').map(str::to_string).collect()
}

macro_rules! run_shape {
    ($d:expr, $shape:expr, $rows:expr, $threads:expr, $reps:expr, $props:expr) => {{
        let d = $d;
        let t = table($shape, $rows);
        for &th in $threads {
            let (td, bd) = time($reps, || direct(&d, $rows, th, $props));
            let (tt, bt) = time($reps, || table_path(&t, th, $props));
            let (ta, ba) = time($reps, || arrow_rs(&d, $rows, th, $props));
            println!(
                "| {} | {} | {} | {:.3} s | {:.3} s | {:.3} s | {:.2}x | {:.2}x | {:.1} / {:.1} / {:.1} MB |",
                $shape,
                $rows,
                th,
                td,
                tt,
                ta,
                tt / td,
                ta / td,
                bd as f64 / 1e6,
                bt as f64 / 1e6,
                ba as f64 / 1e6
            );
        }
    }};
}

fn main() {
    let rows: Vec<u64> = list_arg("--rows", "1000000,4000000,16000000")
        .iter()
        .map(|x| x.parse().unwrap())
        .collect();
    let threads: Vec<usize> = list_arg("--threads", "1,2,4")
        .iter()
        .map(|x| x.parse().unwrap())
        .collect();
    let shapes = list_arg("--shapes", "numbers,strings,lists,mixed");
    let reps: usize = list_arg("--reps", "3")[0].parse().unwrap();
    let plain = args().any(|a| a == "--plain");
    let compression = if args().any(|a| a == "--snappy") {
        Compression::SNAPPY
    } else {
        Compression::UNCOMPRESSED
    };
    let props = WriterProperties::builder()
        .set_dictionary_enabled(!plain)
        .set_compression(compression)
        .build();
    println!(
        "| shape | rows | threads | direct | table | arrow-rs | table / direct | arrow-rs / direct | MB direct / table / arrow-rs |"
    );
    println!("|---|---|---|---|---|---|---|---|---|");
    for shape in &shapes {
        for &r in &rows {
            match shape.as_str() {
                "numbers" => run_shape!(numbers(), "numbers", r, &threads, reps, &props),
                "strings" => run_shape!(strings(), "strings", r, &threads, reps, &props),
                "lists" => run_shape!(lists(), "lists", r, &threads, reps, &props),
                _ => run_shape!(
                    (numbers(), strings(), lists()),
                    "mixed",
                    r,
                    &threads,
                    reps,
                    &props
                ),
            }
        }
    }
}
