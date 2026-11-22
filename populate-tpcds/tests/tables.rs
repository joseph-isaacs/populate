//! The parallel, chunked tables must reproduce tpcdsgen's sequential
//! (dsdgen) output exactly.

use arrow_array::RecordBatch;
use arrow_schema::DataType;
use populate_tpcds::Encoding::{Latin1, Utf8};
use populate_tpcds::TableName::{self, *};
use populate_tpcds::Tpcds;
use std::fmt::Write as _;
use tpcdsgen::config::{SessionBuilder, Table as Ds};
use tpcdsgen::row::{CallCenterRowGenerator, StoreSalesRowGenerator, WebReturnsRowGenerator};

fn rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

fn concat(batches: &[RecordBatch]) -> RecordBatch {
    arrow_select::concat::concat_batches(&batches[0].schema(), batches).expect("concat")
}

/// Small enough to run unoptimised: whole dimensions, prefixes of facts.
fn small(ds: &Tpcds, t: TableName) -> populate_tpcds::DsTable {
    ds.table(t).limit_source_rows(3_000)
}

#[test]
fn chunks_and_threads_do_not_change_the_data() {
    let ds = Tpcds::new(1.0);
    for t in TableName::ALL {
        let one = small(&ds, t).chunk_source_rows(u64::MAX).collect(1);
        let many = small(&ds, t)
            .chunk_source_rows(97)
            .batch_size(1000)
            .collect(4);
        assert!(rows(&one) > 0, "{t}");
        assert_eq!(concat(&one), concat(&many), "{t}");
    }
}

#[test]
fn dat_is_dsdgen_text_for_any_chunking() {
    let ds = Tpcds::new(1.0);
    for t in [CallCenter, StoreSales, WebReturns, Item, DateDim] {
        let one = small(&ds, t)
            .chunk_source_rows(u64::MAX)
            .write_dat(Vec::new(), 1, Utf8);
        let many = small(&ds, t)
            .chunk_source_rows(13)
            .write_dat(Vec::new(), 4, Utf8);
        assert_eq!(one.unwrap(), many.unwrap(), "{t}");
    }
}

/// The fast Arrow-to-text path writes exactly what tpcdsgen's row
/// `Display` writes, for every table and both encodings.
#[test]
fn dat_matches_tpcdsgen_display_for_every_table() {
    let ds = Tpcds::new(1.0);
    for t in TableName::ALL {
        let table = ds.table(t).limit_source_rows(2_000).chunk_source_rows(333);
        for enc in [Utf8, Latin1] {
            let got = table.write_dat(Vec::new(), 4, enc).unwrap();
            let want = table.reference_dat(enc);
            assert!(!want.is_empty(), "{t}");
            if got != want {
                let (g, w) = (
                    String::from_utf8_lossy(&got),
                    String::from_utf8_lossy(&want),
                );
                let (gl, wl) = g
                    .lines()
                    .zip(w.lines())
                    .find(|(a, b)| a != b)
                    .unwrap_or_default();
                panic!("{t} {enc:?}:\n got  {gl}\n want {wl}");
            }
        }
    }
}

#[test]
fn latin1_is_one_byte_per_char() {
    let c = Tpcds::new(1.0).table(Customer);
    let dat = c.write_dat(Vec::new(), 0, Latin1).unwrap();
    assert!(dat.windows(13).any(|w| w == b"C\xd4TE D'IVOIRE"));
    let dat = c.write_dat(Vec::new(), 0, Utf8).unwrap();
    assert!(std::str::from_utf8(&dat).unwrap().contains("CÔTE D'IVOIRE"));
}

/// Our `.dat` equals tpcdsgen's own sequential generator, line for line.
#[test]
fn dat_matches_tpcdsgen_sequential() {
    let session = SessionBuilder::new()
        .with_scale_factor(1.0)
        .build()
        .unwrap();
    let mut want = String::new();
    let n = session.get_scaling().get_row_count(Ds::CallCenter);
    for r in CallCenterRowGenerator::new(session.clone(), n) {
        writeln!(want, "{r}").unwrap();
    }
    let got = Tpcds::new(1.0)
        .table(CallCenter)
        .chunk_source_rows(1)
        .write_dat(Vec::new(), 3, Utf8)
        .unwrap();
    assert_eq!(String::from_utf8(got).unwrap(), want);

    let mut want = String::new();
    let n = session.get_scaling().get_row_count(Ds::WebReturns);
    let mut g = WebReturnsRowGenerator::new(session.clone(), n);
    g.set_source_row_range(1, 500);
    for r in g {
        writeln!(want, "{r}").unwrap();
    }
    let got = Tpcds::new(1.0)
        .table(WebReturns)
        .limit_source_rows(500)
        .chunk_source_rows(7)
        .write_dat(Vec::new(), 4, Utf8)
        .unwrap();
    assert_eq!(String::from_utf8(got).unwrap(), want);
}

/// The first line of dsdgen's SF 1 `store_sales.dat`.
#[test]
fn store_sales_first_row_is_dsdgens() {
    let dat = Tpcds::new(1.0)
        .table(StoreSales)
        .limit_source_rows(1)
        .write_dat(Vec::new(), 1, Utf8)
        .unwrap();
    let first = String::from_utf8(dat).unwrap();
    assert_eq!(
        first.lines().next().unwrap(),
        "2451813|65495|3617|67006|591617|3428|24839|10|161|1|79|11.41|18.71|2.80|99.54|221.20\
         |901.39|1478.09|6.08|99.54|121.66|127.74|-779.73|"
    );
    // a ticket has 8..=16 line items
    let session = SessionBuilder::new().build().unwrap();
    let mut g = StoreSalesRowGenerator::new(session, 1);
    g.set_source_row_range(1, 1);
    assert_eq!(first.lines().count(), g.count());
}

#[test]
fn source_rows_at_sf1() {
    let ds = Tpcds::new(1.0);
    let want = [
        (CallCenter, 6),
        (CatalogPage, 11_718),
        (CatalogSales, 160_000),
        (Customer, 100_000),
        (CustomerAddress, 50_000),
        (CustomerDemographics, 1_920_800),
        (DateDim, 73_049),
        (HouseholdDemographics, 7_200),
        (IncomeBand, 20),
        (Inventory, 11_745_000),
        (Item, 18_000),
        (Promotion, 300),
        (Reason, 75), // the spec says 35; dsdgen writes all 75 reasons
        (ShipMode, 20),
        (Store, 12),
        (StoreSales, 240_000),
        (TimeDim, 86_400),
        (Warehouse, 5),
        (WebPage, 60),
        (WebSales, 60_000),
        (WebSite, 30),
    ];
    for (t, n) in want {
        assert_eq!(ds.source_rows(t), n, "{t}");
    }
}

#[test]
fn schema_types() {
    let ds = Tpcds::new(1.0);
    let s = ds.table(StoreSales).schema();
    assert_eq!(s.fields().len(), 23);
    assert_eq!(s.field(0).name(), "ss_sold_date_sk");
    assert_eq!(
        s.field_with_name("ss_net_profit").unwrap().data_type(),
        &DataType::Decimal128(7, 2)
    );
    let d = ds.table(DateDim).schema();
    assert_eq!(
        d.field_with_name("d_date").unwrap().data_type(),
        &DataType::Date32
    );
}

#[test]
fn parquet_round_trips_and_is_thread_independent() {
    let t = Tpcds::new(1.0).table(Customer).limit_source_rows(2_000);
    let a = t
        .clone()
        .chunk_source_rows(300)
        .write_parquet(Vec::new(), 1, None)
        .unwrap();
    let b = t
        .clone()
        .chunk_source_rows(300)
        .write_parquet(Vec::new(), 4, None)
        .unwrap();
    assert_eq!(a, b);
    let back: Vec<RecordBatch> = parquet::arrow::arrow_reader::ParquetRecordBatchReader::try_new(
        bytes::Bytes::from(a),
        4096,
    )
    .unwrap()
    .map(|b| b.unwrap())
    .collect();
    let want = concat(&t.collect(2));
    let got = concat(&back);
    assert_eq!(got.num_rows(), 2_000);
    for (g, w) in got.columns().iter().zip(want.columns()) {
        let w = arrow_cast::cast(w, g.data_type()).unwrap();
        assert_eq!(g.as_ref(), w.as_ref());
    }
}
