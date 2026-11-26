//! The parallel, chunked tables must reproduce tpcdsgen's sequential
//! (dsdgen) output exactly.

use arrow_array::RecordBatch;
use arrow_schema::DataType;
use populate_gen_tpcds::Encoding::{self, Latin1, Utf8};
use populate_gen_tpcds::TableName::{self, *};
use populate_gen_tpcds::{DsTable, Tpcds};
use std::fmt::Write as _;
use std::io::{self, Write};
use tpcdsgen::config::{SessionBuilder, Table as Ds};
use tpcdsgen::row::*;
use tpcdsgen_arrow::*;

macro_rules! references {
    ($($t:ident => $arrow:ident, $rows:ident;)*) => {
        /// The table as tpcdsgen-arrow makes it, sequentially, at SF 1.
        fn reference_arrow(t: &DsTable) -> Vec<RecordBatch> {
            reference_arrow_at(t, 1.0)
        }

        fn reference_arrow_at(t: &DsTable, sf: f64) -> Vec<RecordBatch> {
            let s = SessionBuilder::new().with_scale_factor(sf).build().unwrap();
            match t.name() {
                $($t => $arrow::new(s)
                    .with_source_row_range(1, t.source_rows())
                    .map(|b| b.expect("tpcdsgen-arrow batch"))
                    .collect(),)*
            }
        }

        /// The `.dat` file as tpcdsgen formats its rows (`Display`),
        /// sequentially, at SF 1.
        fn reference_dat(t: &DsTable, encoding: Encoding) -> Vec<u8> {
            let s = SessionBuilder::new().with_scale_factor(1.0).build().unwrap();
            let mut out = String::new();
            match t.name() {
                $($t => {
                    let total = s.get_scaling().get_row_count(Ds::$t);
                    let mut g = $rows::new(s, total);
                    g.set_source_row_range(1, t.source_rows());
                    for row in g {
                        writeln!(out, "{row}").unwrap();
                    }
                })*
            }
            match encoding {
                Utf8 => out.into_bytes(),
                Latin1 => out.chars().map(|c| u8::try_from(c).expect("Latin-1")).collect(),
            }
        }
    };
}

references! {
    CallCenter => CallCenterArrow, CallCenterRowGenerator;
    CatalogPage => CatalogPageArrow, CatalogPageRowGenerator;
    CatalogReturns => CatalogReturnsArrow, CatalogReturnsRowGenerator;
    CatalogSales => CatalogSalesArrow, CatalogSalesRowGenerator;
    Customer => CustomerArrow, CustomerRowGenerator;
    CustomerAddress => CustomerAddressArrow, CustomerAddressRowGenerator;
    CustomerDemographics => CustomerDemographicsArrow, CustomerDemographicsRowGenerator;
    DateDim => DateDimArrow, DateDimRowGenerator;
    HouseholdDemographics => HouseholdDemographicsArrow, HouseholdDemographicsRowGenerator;
    IncomeBand => IncomeBandArrow, IncomeBandRowGenerator;
    Inventory => InventoryArrow, InventoryRowGenerator;
    Item => ItemArrow, ItemRowGenerator;
    Promotion => PromotionArrow, PromotionRowGenerator;
    Reason => ReasonArrow, ReasonRowGenerator;
    ShipMode => ShipModeArrow, ShipModeRowGenerator;
    Store => StoreArrow, StoreRowGenerator;
    StoreReturns => StoreReturnsArrow, StoreReturnsRowGenerator;
    StoreSales => StoreSalesArrow, StoreSalesRowGenerator;
    TimeDim => TimeDimArrow, TimeDimRowGenerator;
    Warehouse => WarehouseArrow, WarehouseRowGenerator;
    WebPage => WebPageArrow, WebPageRowGenerator;
    WebReturns => WebReturnsArrow, WebReturnsRowGenerator;
    WebSales => WebSalesArrow, WebSalesRowGenerator;
    WebSite => WebSiteArrow, WebSiteRowGenerator;
}

fn rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

fn concat(batches: &[RecordBatch]) -> RecordBatch {
    arrow_select::concat::concat_batches(&batches[0].schema(), batches).expect("concat")
}

/// Small enough to run unoptimised: whole dimensions, prefixes of facts.
fn small(ds: &Tpcds, t: TableName) -> DsTable {
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
fn adaptive_arrow_work_preserves_all_tables_and_fused_outputs() {
    let ds = Tpcds::new(10.0);
    let mut tables: Vec<_> = TableName::ALL
        .iter()
        .rev()
        .map(|&t| ds.table(t).limit_source_rows(5_000))
        .collect();
    tables.push(ds.table(StoreSales).limit_source_rows(5_000));
    tables.push(ds.table(Inventory).limit_source_rows(0));
    let expected: Vec<_> = tables[..tables.len() - 1]
        .iter()
        .map(|t| concat(&reference_arrow_at(t, 10.0)))
        .collect();
    for threads in [1, 4, 0] {
        let mut got = vec![Vec::new(); tables.len()];
        populate_gen_tpcds::for_each_batch_many(&tables, threads, |i, b| got[i].push(b));
        assert!(got.last().unwrap().is_empty());
        for ((t, batches), want) in tables.iter().zip(&got).zip(&expected) {
            assert_eq!(&concat(batches), want, "{}", t.name());
        }
    }
    populate_gen_tpcds::for_each_batch_many(&[], 4, |_, _| panic!("empty input"));
}

#[test]
fn partitioned_channels_preserve_paired_outputs() {
    let ds = Tpcds::new(100.0);
    let tables: Vec<_> = [
        StoreSales,
        StoreReturns,
        CatalogSales,
        CatalogReturns,
        WebSales,
        WebReturns,
    ]
    .into_iter()
    .map(|t| ds.table(t).limit_source_rows(1_027))
    .collect();
    let expected: Vec<_> = tables
        .iter()
        .map(|t| concat(&reference_arrow_at(t, 100.0)))
        .collect();
    // Small batches cover subdivision of channels with many configured chunks.
    for (source_rows, batch_size) in [(7, 127), (97, 17)] {
        let configured: Vec<_> = tables
            .iter()
            .cloned()
            .map(|t| t.chunk_source_rows(source_rows).batch_size(batch_size))
            .collect();
        for threads in [1, 2, 4, 0] {
            let mut got = vec![Vec::new(); configured.len()];
            populate_gen_tpcds::for_each_batch_many(&configured, threads, |i, b| got[i].push(b));
            for ((table, batches), want) in configured.iter().zip(&got).zip(&expected) {
                assert_eq!(
                    &concat(batches),
                    want,
                    "{}: {source_rows} source rows, batch {batch_size}, {threads} threads",
                    table.name()
                );
            }
        }
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

/// The `.dat` text written from the columns is exactly what tpcdsgen's row
/// `Display` writes, for every table and both encodings.
#[test]
fn dat_matches_tpcdsgen_display_for_every_table() {
    let ds = Tpcds::new(1.0);
    for t in TableName::ALL {
        let table = ds.table(t).limit_source_rows(2_000).chunk_source_rows(333);
        for enc in [Utf8, Latin1] {
            let got = table.write_dat(Vec::new(), 4, enc).unwrap();
            let want = reference_dat(&table, enc);
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

/// A sales table and its returns requested together run as one pass
/// (in either order, with other tables between, a table listed twice); the
/// output must not change.
#[test]
fn channels_in_many() {
    let ds = Tpcds::new(1.0);
    let t = |n| ds.table(n).limit_source_rows(1_500).chunk_source_rows(211);
    let tables = [
        t(StoreReturns),
        t(Customer),
        t(StoreSales),
        t(CatalogSales),
        t(WebReturns),
        t(CatalogReturns),
        t(WebSales),
        t(WebSales),
    ];
    let n = tables.len();
    let mut got: Vec<Vec<RecordBatch>> = vec![Vec::new(); n];
    populate_gen_tpcds::for_each_batch_many(&tables, 4, |i, b| got[i].push(b));
    for (i, table) in tables.iter().enumerate() {
        let want = reference_arrow(table);
        assert_eq!(concat(&got[i]), concat(&want), "{}", table.name());
    }
    let dats = populate_gen_tpcds::write_dat_many(&tables, vec![Vec::new(); n], 3, Utf8).unwrap();
    for (dat, table) in dats.iter().zip(&tables) {
        assert_eq!(dat, &reference_dat(table, Utf8), "{}", table.name());
    }
    let pq = populate_gen_tpcds::write_parquet_many(&tables, vec![Vec::new(); n], 4, None).unwrap();
    for (file, table) in pq.into_iter().zip(&tables) {
        let back: Vec<RecordBatch> =
            parquet::arrow::arrow_reader::ParquetRecordBatchReader::try_new(
                bytes::Bytes::from(file),
                4096,
            )
            .unwrap()
            .map(|b| b.unwrap())
            .collect();
        assert_eq!(
            rows(&back),
            rows(&reference_arrow(table)),
            "{}",
            table.name()
        );
    }
}

/// A writer that fails once it is given more than `.0` bytes.
struct Failing(usize);

impl Write for Failing {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.len() > self.0 {
            return Err(io::Error::other("disk full"));
        }
        self.0 -= buf.len();
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// The workers write the `.dat` files: a write error stops them and is
/// returned, it does not hang the pool.
#[test]
fn dat_write_error_is_returned() {
    let ds = Tpcds::new(1.0);
    let t = |n| ds.table(n).limit_source_rows(3_000).chunk_source_rows(100);
    let tables = [t(StoreSales), t(Customer)];
    let writers = vec![Failing(100_000), Failing(usize::MAX)];
    let err = populate_gen_tpcds::write_dat_many(&tables, writers, 4, Utf8).err();
    assert_eq!(err.map(|e| e.to_string()), Some("disk full".into()));
}
