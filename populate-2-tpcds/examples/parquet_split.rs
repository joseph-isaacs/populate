//! Where does the time go when writing Parquet? For each table (SF 1,
//! single-threaded, 8000-row batches), ours vs tpchgen-rs:
//!   gen    = produce all RecordBatches in memory
//!   encode = write those batches to an in-memory Parquet file (ArrowWriter,
//!            default WriterProperties + codec, as tpcgen-cli does)
//! Best of 3 for each. Both sides produce identical data.
//!
//!   cargo run --release -p populate-2-tpcds --example parquet_split

use arrow_array::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use populate_2_tpch::values::POOL_BUFFER;
use std::time::Instant;
use tpcdsgen::config::SessionBuilder;

const SF: f64 = 1.0;
const BATCH: usize = 8000;

fn best_of_3<T>(mut f: impl FnMut() -> T) -> (f64, T) {
    let mut best = f64::MAX;
    let mut out = None;
    for _ in 0..3 {
        let t = Instant::now();
        let v = f();
        best = best.min(t.elapsed().as_secs_f64() * 1e3);
        out = Some(v);
    }
    (best, out.unwrap())
}

fn encode(batches: &[RecordBatch], compression: Compression) -> usize {
    let props = WriterProperties::builder()
        .set_compression(compression)
        .build();
    let mut buf = Vec::new();
    let mut w = ArrowWriter::try_new(&mut buf, batches[0].schema(), Some(props)).unwrap();
    for b in batches {
        w.write(b).unwrap();
    }
    w.close().unwrap();
    buf.len()
}

fn report(table: &str, who: &str, gen_ms: f64, batches: &[RecordBatch]) {
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    for (codec, c) in [
        ("snappy", Compression::SNAPPY),
        ("zstd3", Compression::ZSTD(ZstdLevel::try_new(3).unwrap())),
    ] {
        let (enc_ms, bytes) = best_of_3(|| encode(batches, c));
        let total = gen_ms + enc_ms;
        println!(
            "{table:<24} {who:<8} {rows:>9} rows  gen {gen_ms:>7.1} ms  {codec:<6} encode {enc_ms:>7.1} ms  total {total:>7.1} ms  parquet share {:>3.0}%  {:>6.1} MB",
            100.0 * enc_ms / total,
            bytes as f64 / 1e6
        );
    }
}

fn ours<T: populate_2_tpch::Table>(table: &str) {
    let (ms, b) = best_of_3(|| populate_2_tpch::batches::<T>(SF, BATCH).collect::<Vec<_>>());
    report(table, "ours", ms, &b);
}

fn ours_ds<T: populate_2_tpcds::Table>(table: &str) {
    let (ms, b) = best_of_3(|| populate_2_tpcds::batches::<T>(SF, BATCH).collect::<Vec<_>>());
    report(table, "ours", ms, &b);
}

fn reference(table: &str, f: impl FnMut() -> Vec<RecordBatch>) {
    let (ms, b) = best_of_3(f);
    report(table, "tpcgen", ms, &b);
}

fn main() {
    tpchgen::text::TextPool::get_or_init_default();
    std::sync::LazyLock::force(&POOL_BUFFER);
    use populate_2_tpch::{Customer, Part, PartSupp, Supplier};
    use tpchgen::generators::*;
    use tpchgen_arrow::*;
    let ok = |r: Result<RecordBatch, _>| r.unwrap();
    let ds = || SessionBuilder::new().with_scale_factor(SF).build().unwrap();

    ours::<Part>("TPC-H part");
    reference("TPC-H part", || {
        PartArrow::new(PartGenerator::new(SF, 1, 1))
            .map(ok)
            .collect()
    });
    ours::<PartSupp>("TPC-H partsupp");
    reference("TPC-H partsupp", || {
        PartSuppArrow::new(PartSuppGenerator::new(SF, 1, 1))
            .map(ok)
            .collect()
    });
    ours::<Customer>("TPC-H customer");
    reference("TPC-H customer", || {
        CustomerArrow::new(CustomerGenerator::new(SF, 1, 1))
            .map(ok)
            .collect()
    });
    ours::<Supplier>("TPC-H supplier");
    reference("TPC-H supplier", || {
        SupplierArrow::new(SupplierGenerator::new(SF, 1, 1))
            .map(ok)
            .collect()
    });
    ours_ds::<populate_2_tpcds::Inventory>("TPC-DS inventory");
    reference("TPC-DS inventory", || {
        tpcdsgen_arrow::InventoryArrow::new(ds()).map(ok).collect()
    });
    ours_ds::<populate_2_tpcds::CustomerDemographics>("TPC-DS customer_demographics");
    reference("TPC-DS customer_demographics", || {
        tpcdsgen_arrow::CustomerDemographicsArrow::new(ds())
            .map(ok)
            .collect()
    });
}
