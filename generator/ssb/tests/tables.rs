//! populate-gen-ssb against ssb-dbgen, and chunked output against sequential.
//!
//! The reference hashes are FNV-1a 64 of `.tbl` files (or ranges of their
//! lines) written by eyalroz/ssb-dbgen's `dbgen -s <sf>` (default build: all
//! five tables, `|` separators with a trailing `|`). Set `SSB_DBGEN` to that
//! `dbgen` binary (its `dists.dss` next to it) and run the ignored test to
//! compare whole files at more scale factors.

use arrow_array::RecordBatch;
use arrow_array::cast::AsArray;
use arrow_array::types::Int32Type;
use arrow_select::concat::concat_batches;
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use populate_gen_ssb::TableName::{Customer, Date, LineOrder, LineOrderFlat, Part, Supplier};
use populate_gen_ssb::{Ssb, TableName, write_tbl_many};
use populate_table::fnv1a64;
use std::path::PathBuf;
use std::process::Command;

fn check(sf: f64, t: TableName, first: u64, end: u64, len: usize, hash: u64) {
    let got = Ssb::new(sf).table(t).tbl_range(first, end);
    assert_eq!(got.len(), len, "SF {sf} {t} {first}..{end}: length");
    assert_eq!(fnv1a64(&got), hash, "SF {sf} {t} {first}..{end}: bytes");
}

#[test]
fn sf_0_01_matches_ssb_dbgen() {
    for (t, len, hash) in [
        (Part, 167_463, 0x05936901d31af393),
        (Supplier, 1_594, 0x72e4b3d899e927d8),
        (Customer, 27_659, 0xc4aad751acd9b0de),
        (Date, 2_226, 0x5668ba983a776684),
        (LineOrder, 5_526_185, 0x547598974e6077ed),
    ] {
        let tbl = Ssb::new(0.01).table(t).write_tbl(Vec::new(), 0).unwrap();
        assert_eq!((tbl.len(), fnv1a64(&tbl)), (len, hash), "{t}");
    }
}

#[test]
fn dimensions_match_ssb_dbgen() {
    let all = u64::MAX;
    for (sf, t, first, end, len, hash) in [
        (1.0, Supplier, 0, all, 166_676, 0x739b3a6c5ddd7d94),
        (1.0, Customer, 0, all, 2_837_046, 0x7d6a35dc66922a6a),
        (1.0, Date, 0, all, 229_628, 0x3d4ae4d8c6274681),
        (1.0, Part, 0, all, 17_139_259, 0x0f07131814658c03),
        (8.0, Supplier, 0, all, 1_346_561, 0xb71e2a819261b4f4),
        (8.0, Customer, 0, all, 22_905_175, 0x203b6942388e4f96),
        (8.0, Part, 650_000, 651_000, 86_281, 0xe225060e453ac895),
        // customer keys past 2^21: the floating-point range mapping
        (
            100.0,
            Customer,
            2_000_000,
            2_001_000,
            96_648,
            0x07ec723892b8912f,
        ),
    ] {
        check(sf, t, first, end, len, hash);
    }
}

#[test]
fn lineorder_ranges_match_ssb_dbgen() {
    for (sf, first, end, len, hash) in [
        (1.0, 0, 1_000, 398_814, 0xa7c6f4ebf0e8d132),
        (1.0, 1_000_000, 1_001_000, 402_302, 0xb2bde89579e317c7),
        (1.0, 1_499_000, 1_500_000, 405_941, 0x22dd886daf2f7e3c),
        (8.0, 11_000_000, 11_000_500, 209_959, 0x0bcae4fec444ec6c),
        (100.0, 0, 1_000, 417_046, 0xb13a44469835b637),
        (1100.0, 0, 1_000, 427_359, 0x7366e035a9b44cf1),
    ] {
        check(sf, LineOrder, first, end, len, hash);
    }
}

#[test]
fn row_counts() {
    let s = Ssb::new(0.01);
    let rows = |t| -> usize {
        let mut n = 0;
        s.table(t).for_each_batch(0, |b| n += b.num_rows());
        n
    };
    assert_eq!(rows(Date), 25);
    assert_eq!(rows(LineOrder), 60_176);
    assert_eq!(rows(LineOrderFlat), 60_176);
    // part grows with log2 of the scale factor
    assert_eq!(Ssb::new(8.0).source_rows(Part), 800_000);
    assert_eq!(Ssb::new(10.0).source_rows(Part), 800_000);
    assert_eq!(Ssb::new(10.0).source_rows(Date), 2557);
}

#[test]
fn chunked_equals_sequential() {
    let s = Ssb::new(0.01);
    for t in TableName::ALL.into_iter().chain([LineOrderFlat]) {
        let whole = s.table(t);
        let want = whole.tbl_range(0, u64::MAX);
        for (chunk, batch, threads) in [(1, 1, 3), (7, 5, 2), (1_000, 64, 4), (100_000, 8192, 1)] {
            let tbl = s
                .table(t)
                .chunk_source_rows(chunk)
                .batch_size(batch)
                .write_tbl(Vec::new(), threads)
                .unwrap();
            assert!(tbl == want, "{t}: chunks of {chunk}, batches of {batch}");
        }
    }
}

#[test]
fn many_tables_on_one_pool() {
    let s = Ssb::new(0.01);
    let tables: Vec<_> = s.tables().map(|t| t.chunk_source_rows(97)).collect();
    let got = write_tbl_many(&tables, vec![Vec::new(); tables.len()], 3).unwrap();
    for (t, g) in tables.iter().zip(got) {
        assert!(g == t.tbl_range(0, u64::MAX), "{}", t.name());
    }
}

fn int(b: &RecordBatch, c: &str, r: usize) -> i32 {
    b.column_by_name(c)
        .unwrap()
        .as_primitive::<Int32Type>()
        .value(r)
}

fn text<'a>(b: &'a RecordBatch, c: &str, r: usize) -> &'a str {
    b.column_by_name(c).unwrap().as_string_view().value(r)
}

#[test]
fn flat_is_the_join() {
    flat_matches(0.01, 0);
    // dimensions built from many jobs (over 64Ki customers and parts)
    flat_matches(3.0, 20_000);
}

/// `lineorder_flat` of the first `orders` orders (all for 0) of SF `sf`
/// against `lineorder` joined with the dimension tables.
fn flat_matches(sf: f64, orders: u64) {
    let s = Ssb::new(sf);
    let limit = |t: TableName| {
        let t = s.table(t);
        if orders > 0 && matches!(t.name(), LineOrder | LineOrderFlat) {
            t.limit_source_rows(orders)
        } else {
            t
        }
    };
    let one = |t: TableName| {
        let v = limit(t).collect(0);
        concat_batches(&t.schema(), &v).unwrap()
    };
    let (lo, cust, supp, part) = (one(LineOrder), one(Customer), one(Supplier), one(Part));
    let flat = one(LineOrderFlat);
    assert_eq!(flat.num_rows(), lo.num_rows());
    assert_eq!(flat.num_columns(), 17 + 21);
    // the lineorder columns are lineorder's
    assert_eq!(flat.project(&(0..17).collect::<Vec<_>>()).unwrap(), lo);
    for r in (0..flat.num_rows()).step_by(37) {
        let c = int(&flat, "lo_custkey", r) as usize - 1;
        let sp = int(&flat, "lo_suppkey", r) as usize - 1;
        let p = int(&flat, "lo_partkey", r) as usize - 1;
        for col in [
            "c_name",
            "c_address",
            "c_city",
            "c_nation",
            "c_region",
            "c_phone",
        ] {
            assert_eq!(text(&flat, col, r), text(&cust, col, c), "{col}");
        }
        assert_eq!(
            text(&flat, "c_mktsegment", r),
            text(&cust, "c_mktsegment", c)
        );
        for col in [
            "s_name",
            "s_address",
            "s_city",
            "s_nation",
            "s_region",
            "s_phone",
        ] {
            assert_eq!(text(&flat, col, r), text(&supp, col, sp), "{col}");
        }
        for col in [
            "p_name",
            "p_mfgr",
            "p_category",
            "p_brand1",
            "p_color",
            "p_type",
        ] {
            assert_eq!(text(&flat, col, r), text(&part, col, p), "{col}");
        }
        assert_eq!(text(&flat, "p_container", r), text(&part, "p_container", p));
        assert_eq!(int(&flat, "p_size", r), int(&part, "p_size", p));
    }
}

#[test]
fn parquet_round_trips() {
    let s = Ssb::new(0.01);
    for t in [LineOrder, Customer, LineOrderFlat] {
        let table = s.table(t).chunk_rows(10_000);
        let want = table.collect(2);
        let file = table.write_parquet(Vec::new(), 2, None).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(file))
            .unwrap()
            .build()
            .unwrap();
        let got: Vec<RecordBatch> = reader.map(Result::unwrap).collect();
        let cat = |v: &[RecordBatch]| concat_batches(&t.schema(), v).unwrap();
        assert_eq!(cat(&got), cat(&want), "{t}");
    }
}

/// Every table at several scale factors against a local ssb-dbgen build:
/// `SSB_DBGEN=/path/to/dbgen cargo test --release -p populate-gen-ssb -- --ignored`.
#[test]
#[ignore = "needs ssb-dbgen: set SSB_DBGEN to its dbgen binary"]
fn whole_files_match_ssb_dbgen() {
    let dbgen = PathBuf::from(std::env::var("SSB_DBGEN").expect("SSB_DBGEN"));
    let dists = dbgen.with_file_name("dists.dss");
    for sf in ["0.05", "0.5", "1", "4"] {
        let dir = std::env::temp_dir().join(format!("populate-gen-ssb-dbgen-{sf}"));
        std::fs::create_dir_all(&dir).unwrap();
        let ok = Command::new(&dbgen)
            .args(["-q", "-f", "-s", sf, "-b"])
            .arg(&dists)
            .current_dir(&dir)
            .status()
            .unwrap()
            .success();
        assert!(ok, "dbgen -s {sf}");
        let s = Ssb::new(sf.parse().unwrap());
        for t in s.tables() {
            let want = std::fs::read(dir.join(format!("{}.tbl", t.name()))).unwrap();
            let got = t.write_tbl(Vec::new(), 0).unwrap();
            assert!(got == want, "SF {sf} {}", t.name());
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
