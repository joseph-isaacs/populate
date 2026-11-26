//! Every table against tpchgen: Arrow equal to tpchgen-arrow, `.tbl` equal
//! to tpchgen's rows' `Display`, the same output for any chunking and
//! thread count, and Parquet round-trips.

use arrow_array::RecordBatch;
use arrow_select::concat::concat_batches;
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use populate_tpch::{TableName, Tpch, TpchTable, write_tbl_many};
use tpchgen::generators::*;
use tpchgen_arrow::*;

fn cat(t: &TpchTable, v: &[RecordBatch]) -> RecordBatch {
    concat_batches(&t.schema(), v).expect("same schema")
}

/// tpchgen-arrow's batches for the first `source_rows` source rows.
fn reference(t: TableName, sf: f64, source_rows: u64) -> RecordBatch {
    let n = source_rows as usize;
    let b: Vec<RecordBatch> = match t {
        TableName::Nation => NationArrow::new(NationGenerator::new(sf, 1, 1))
            .map(Result::unwrap)
            .collect(),
        TableName::Region => RegionArrow::new(RegionGenerator::new(sf, 1, 1))
            .map(Result::unwrap)
            .collect(),
        TableName::Part => PartArrow::new(PartGenerator::new(sf, 1, 1))
            .map(Result::unwrap)
            .collect(),
        TableName::Supplier => SupplierArrow::new(SupplierGenerator::new(sf, 1, 1))
            .map(Result::unwrap)
            .collect(),
        TableName::PartSupp => PartSuppArrow::new(PartSuppGenerator::new(sf, 1, 1))
            .map(Result::unwrap)
            .collect(),
        TableName::Customer => CustomerArrow::new(CustomerGenerator::new(sf, 1, 1))
            .map(Result::unwrap)
            .collect(),
        TableName::Orders => OrderArrow::new(OrderGenerator::new(sf, 1, 1))
            .map(Result::unwrap)
            .collect(),
        TableName::LineItem => LineItemArrow::new(LineItemGenerator::new(sf, 1, 1))
            .map(Result::unwrap)
            .collect(),
    };
    let all = concat_batches(&b[0].schema(), &b).unwrap();
    let rows = match t {
        TableName::PartSupp => n * 4,
        TableName::LineItem => {
            let keys = all.column(0).as_any();
            let keys = keys
                .downcast_ref::<arrow_array::Int64Array>()
                .unwrap()
                .values();
            let last = OrderGenerator::make_order_key(n as i64);
            keys.partition_point(|&k| k <= last)
        }
        _ => n,
    };
    all.slice(0, rows.min(all.num_rows()))
}

#[test]
fn every_table_matches_tpchgen_arrow() {
    let h = Tpch::new(0.01);
    for t in h.tables() {
        let got = cat(&t, &t.collect(0));
        let want = reference(t.name(), 0.01, t.source_rows());
        assert_eq!(got.num_rows(), want.num_rows(), "{}", t.name());
        assert_eq!(got, want, "{}", t.name());
    }
}

#[test]
fn every_table_tbl_matches_tpchgen_display() {
    let h = Tpch::new(0.01);
    for t in h.tables() {
        let got = t.write_tbl(Vec::new(), 0).unwrap();
        let (_, want) = tpchgen_part(t.name(), 0.01, 1, 1, t.source_rows() as usize);
        assert!(got == want, "{}: .tbl differs", t.name());
    }
}

#[test]
fn any_chunking_and_thread_count() {
    let h = Tpch::new(0.01);
    for t in h.tables() {
        let want = cat(&t, &t.collect(1));
        for (chunk, batch, threads) in [(1, 1, 3), (7, 5, 2), (1000, 333, 4), (5000, 8192, 0)] {
            let c = t.clone().chunk_source_rows(chunk).batch_size(batch);
            assert_eq!(cat(&c, &c.collect(threads)), want, "{} {chunk}", t.name());
        }
    }
}

/// tpchgen's `.tbl` lines (its rows' `Display`) for `n` source rows of
/// part `part` of `parts` (tpchgen positions its streams at the part's
/// first row), and that row.
fn tpchgen_part(t: TableName, sf: f64, part: i32, parts: i32, n: usize) -> (u64, Vec<u8>) {
    use std::io::Write;
    let mut out = Vec::new();
    let mut put = |r: &dyn std::fmt::Display| writeln!(out, "{r}").unwrap();
    let start = |base: f64| ((base * sf) as i64 / parts as i64 * (part as i64 - 1)) as u64;
    let first = match t {
        TableName::Nation => {
            NationGenerator::default()
                .iter()
                .take(n)
                .for_each(|r| put(&r));
            0
        }
        TableName::Region => {
            RegionGenerator::default()
                .iter()
                .take(n)
                .for_each(|r| put(&r));
            0
        }
        TableName::Part => {
            PartGenerator::new(sf, part, parts)
                .iter()
                .take(n)
                .for_each(|r| put(&r));
            start(200_000.0)
        }
        TableName::Supplier => {
            SupplierGenerator::new(sf, part, parts)
                .iter()
                .take(n)
                .for_each(|r| put(&r));
            start(10_000.0)
        }
        TableName::Customer => {
            CustomerGenerator::new(sf, part, parts)
                .iter()
                .take(n)
                .for_each(|r| put(&r));
            start(150_000.0)
        }
        TableName::PartSupp => {
            PartSuppGenerator::new(sf, part, parts)
                .iter()
                .take(4 * n)
                .for_each(|r| put(&r));
            start(200_000.0)
        }
        TableName::Orders => {
            OrderGenerator::new(sf, part, parts)
                .iter()
                .take(n)
                .for_each(|r| put(&r));
            start(1_500_000.0)
        }
        TableName::LineItem => {
            let first = start(1_500_000.0);
            let last = OrderGenerator::make_order_key((first + n as u64) as i64);
            LineItemGenerator::new(sf, part, parts)
                .iter()
                .take_while(|r| r.l_orderkey <= last)
                .for_each(|r| put(&r));
            first
        }
    };
    (first, out)
}

/// Larger scale factors: a chunk deep into each table (part 78 of 100), and value ranges
/// past 2^21 (part keys at SF 1000), against tpchgen's partitioned
/// generators.
#[test]
fn deep_chunks_at_large_scale_factors() {
    for sf in [10.0, 1000.0] {
        let h = Tpch::new(sf);
        for name in [
            TableName::Part,
            TableName::Supplier,
            TableName::Customer,
            TableName::PartSupp,
            TableName::Orders,
            TableName::LineItem,
        ] {
            let (first, want) = tpchgen_part(name, sf, 78, 100, 300);
            let got = h.table(name).tbl_range(first, first + 300);
            let (g, w) = (
                String::from_utf8(got).unwrap(),
                String::from_utf8(want).unwrap(),
            );
            for (a, b) in g.lines().zip(w.lines()) {
                assert_eq!(a, b, "{name} at SF {sf}");
            }
            assert_eq!(g.len(), w.len(), "{name} at SF {sf}");
        }
    }
}

#[test]
fn parquet_round_trip() {
    let h = Tpch::new(0.01);
    for t in h.tables() {
        let t = t.chunk_rows(10_000);
        let want = cat(&t, &t.collect(0));
        let file = Bytes::from(t.write_parquet(Vec::new(), 0, None).unwrap());
        let r = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        assert_eq!(
            r.metadata().num_row_groups() as u64,
            t.num_chunks(),
            "{}",
            t.name()
        );
        let got: Vec<_> = r.build().unwrap().map(Result::unwrap).collect();
        assert_eq!(cat(&t, &got), want, "{}", t.name());
    }
}

#[test]
fn many_tables_on_one_pool() {
    let h = Tpch::new(0.01);
    let tables: Vec<_> = h.tables().map(|t| t.chunk_rows(5000)).collect();
    let writers = tables.iter().map(|_| Vec::new()).collect();
    let got = write_tbl_many(&tables, writers, 4).unwrap();
    for (t, g) in tables.iter().zip(got) {
        assert!(g == t.write_tbl(Vec::new(), 1).unwrap(), "{}", t.name());
    }
}

#[test]
fn arrow_many_preserves_each_output_with_repeated_and_empty_tables() {
    let h = Tpch::new(0.01);
    let mut tables: Vec<_> = h.tables().map(|t| t.chunk_rows(5000)).collect();
    tables.reverse();
    tables.push(h.table(TableName::Supplier).limit_source_rows(0));
    tables.push(h.table(TableName::LineItem).batch_size(137));
    for threads in [1, 4, 0] {
        let mut got = vec![Vec::new(); tables.len()];
        populate_tpch::for_each_batch_many(&tables, threads, |i, b| got[i].push(b));
        for (t, batches) in tables.iter().zip(&got) {
            assert_eq!(cat(t, batches), cat(t, &t.collect(1)), "{}", t.name());
        }
    }
    populate_tpch::for_each_batch_many(&[], 4, |_, _| panic!("empty input"));
}
