//! Is encode slower on our batches because comment views point at random
//! places in the 300 MB text pool? Compare encode of: our batches as-is,
//! our batches after copy_text (strings copied into compact buffers), and the cost of gc itself. Snappy, SF 1, best of 5.

use arrow_array::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use populate_2_tpch::values::{POOL_BUFFER, copy_text};
use std::time::Instant;

fn best_of_3<T>(mut f: impl FnMut() -> T) -> (f64, T) {
    let mut best = f64::MAX;
    let mut out = None;
    for _ in 0..5 {
        let t = Instant::now();
        let v = f();
        best = best.min(t.elapsed().as_secs_f64() * 1e3);
        out = Some(v);
    }
    (best, out.unwrap())
}

fn encode(batches: &[RecordBatch]) -> usize {
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut buf = Vec::new();
    let mut w = ArrowWriter::try_new(&mut buf, batches[0].schema(), Some(props)).unwrap();
    for b in batches {
        w.write(b).unwrap();
    }
    w.close().unwrap();
    buf.len()
}

fn compact(batches: &[RecordBatch]) -> Vec<RecordBatch> {
    batches.iter().map(copy_text).collect()
}

fn run<T: populate_2_tpch::Table>(table: &str, reference: Vec<RecordBatch>) {
    let batches: Vec<_> = populate_2_tpch::batches::<T>(1.0, 8000).collect();
    let (enc_ref, _) = best_of_3(|| encode(&reference));
    let (enc_raw, _) = best_of_3(|| encode(&batches));
    let (gc_ms, compacted) = best_of_3(|| compact(&batches));
    let (enc_gc, _) = best_of_3(|| encode(&compacted));
    let mem = |bs: &[RecordBatch]| {
        bs.iter().map(|b| b.get_array_memory_size()).sum::<usize>() as f64 / 1e6
    };
    println!("{table:<10} encode tpcgen batches {enc_ref:>6.1} ms");
    println!(
        "{table:<10} encode as-is {enc_raw:>6.1} ms | gc {gc_ms:>5.1} ms + encode compacted {enc_gc:>6.1} ms = {:>6.1} ms | reported memory {:>7.1} MB -> {:>5.1} MB",
        gc_ms + enc_gc,
        mem(&batches),
        mem(&compacted)
    );
}

fn main() {
    std::sync::LazyLock::force(&POOL_BUFFER);
    use populate_2_tpch::{Customer, Part, PartSupp, Supplier};
    use tpchgen::generators::*;
    use tpchgen_arrow::*;
    let ok = |r: Result<RecordBatch, _>| r.unwrap();
    run::<Part>(
        "part",
        PartArrow::new(PartGenerator::new(1.0, 1, 1))
            .map(ok)
            .collect(),
    );
    run::<PartSupp>(
        "partsupp",
        PartSuppArrow::new(PartSuppGenerator::new(1.0, 1, 1))
            .map(ok)
            .collect(),
    );
    run::<Customer>(
        "customer",
        CustomerArrow::new(CustomerGenerator::new(1.0, 1, 1))
            .map(ok)
            .collect(),
    );
    run::<Supplier>(
        "supplier",
        SupplierArrow::new(SupplierGenerator::new(1.0, 1, 1))
            .map(ok)
            .collect(),
    );
}
