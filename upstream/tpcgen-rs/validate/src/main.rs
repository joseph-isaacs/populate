//! Differential validation: patched tpchgen/tpcdsgen vs released 4.0.0.
//!
//! Every row is rendered to its full text line (.tbl for TPC-H, CSV for
//! TPC-DS) by each crate's own formatter and compared, so every field of
//! every row is checked. Two kinds of runs per table:
//!   - every row at SF 1
//!   - >= 10 partitions far into a large scale factor, reached with
//!     part/part_count (TPC-H) or chunk_number/total_chunks +
//!     set_source_row_range (TPC-DS), i.e. the skip-ahead path

use std::fmt::Display;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

static ROWS: AtomicU64 = AtomicU64::new(0);
static FIELDS: AtomicU64 = AtomicU64::new(0);

/// Compare two row streams line by line; return rows compared.
fn compare<A: Display, B: Display>(
    label: &str,
    reference: impl Iterator<Item = A>,
    patched: impl Iterator<Item = B>,
    sep: char,
) -> u64 {
    let (mut a, mut b) = (reference, patched);
    let (mut sa, mut sb) = (String::new(), String::new());
    let mut rows = 0u64;
    let mut fields = 0u64;
    loop {
        match (a.next(), b.next()) {
            (None, None) => break,
            (Some(_), None) | (None, Some(_)) => panic!("{label}: row count differs after {rows} rows"),
            (Some(x), Some(y)) => {
                use std::fmt::Write;
                sa.clear();
                sb.clear();
                write!(sa, "{x}").unwrap();
                write!(sb, "{y}").unwrap();
                if sa != sb {
                    panic!("{label}: row {rows} differs\n  ref: {sa}\n  new: {sb}");
                }
                fields += sa.matches(sep).count() as u64 + 1;
                rows += 1;
            }
        }
    }
    ROWS.fetch_add(rows, Ordering::Relaxed);
    FIELDS.fetch_add(fields, Ordering::Relaxed);
    rows
}

/// >= 12 partitions spread over 1..=n: both ends plus evenly spaced, jittered.
fn picks(n: i32) -> Vec<i32> {
    if n <= 12 {
        return (1..=n).collect();
    }
    let mut v = vec![1, 2, n - 1, n];
    for k in 1..=10 {
        let base = 1 + (k as i64 * (n as i64 - 1) / 11) as i32;
        let jitter = ((k * 7919) % (n / 12).max(1)) as i32;
        v.push((base + jitter).clamp(1, n));
    }
    v.sort();
    v.dedup();
    v
}

// ============================================================================
// TPC-H
// ============================================================================

macro_rules! tpch {
    ($table:literal, $gen:ident, $sf:expr, $parts:expr) => {{
        let t = Instant::now();
        let full = compare(
            concat!($table, " sf1 full"),
            h_ref::generators::$gen::new(1.0, 1, 1).iter(),
            h_new::generators::$gen::new(1.0, 1, 1).iter(),
            '|',
        );
        let mut skipped = 0;
        let mut ranges = 0;
        if $parts > 1 {
            for part in picks($parts) {
                let label = format!("{} sf{} part {}/{}", $table, $sf, part, $parts);
                skipped += compare(
                    &label,
                    h_ref::generators::$gen::new($sf, part, $parts).iter(),
                    h_new::generators::$gen::new($sf, part, $parts).iter(),
                    '|',
                );
                ranges += 1;
            }
        }
        println!(
            "TPC-H  {:<22} sf1 full {:>9} rows | sf{} {:>2} partitions {:>9} rows | OK ({:.1}s)",
            $table, full, $sf, ranges, skipped, t.elapsed().as_secs_f64()
        );
    }};
}

fn tpch_all() {
    std::thread::scope(|s| {
        s.spawn(|| tpch!("region", RegionGenerator, 1.0, 1));
        s.spawn(|| tpch!("nation", NationGenerator, 1.0, 1));
        s.spawn(|| tpch!("part", PartGenerator, 100.0, 200));
        s.spawn(|| tpch!("supplier", SupplierGenerator, 1000.0, 100));
        s.spawn(|| tpch!("partsupp", PartSuppGenerator, 100.0, 800));
        s.spawn(|| tpch!("customer", CustomerGenerator, 100.0, 150));
        s.spawn(|| tpch!("orders", OrderGenerator, 100.0, 1500));
        s.spawn(|| tpch!("lineitem", LineItemGenerator, 100.0, 6000));
    });
}

// ============================================================================
// TPC-DS
// ============================================================================

macro_rules! tpcds_session {
    ($krate:ident, $sf:expr, $chunk:expr, $chunks:expr) => {
        $krate::config::SessionBuilder::new()
            .with_scale_factor($sf)
            .with_chunk_number($chunk)
            .with_total_chunks($chunks)
            .with_partitioned($chunks > 1)
            .build()
            .unwrap()
    };
}

/// One chunk, built the way tpcgen-cli does: new(session, source_rows) then
/// set_source_row_range(chunk range).
macro_rules! tpcds_chunk {
    ($krate:ident, $table:ident, $gen:ident, $csv:ident, $sf:expr, $chunk:expr, $chunks:expr) => {{
        use $krate::csv::CsvRow;
        let session = tpcds_session!($krate, $sf, $chunk, $chunks);
        let table = $krate::config::Table::$table;
        let source_rows = session.get_scaling().get_row_count(table);
        let range = session.get_source_row_range(table);
        let mut g = $krate::row::$gen::new(session, source_rows);
        g.set_source_row_range(*range.start(), *range.end());
        g.map(|r| $krate::csv::$csv::new(&r).to_string())
    }};
}

macro_rules! tpcds {
    ($name:literal, $table:ident, $gen:ident, $csv:ident, $sf:expr, $per_chunk:expr) => {{
        let t = Instant::now();
        let full = compare(
            concat!($name, " sf1 full"),
            tpcds_chunk!(ds_ref, $table, $gen, $csv, 1.0, 1, 1),
            tpcds_chunk!(ds_new, $table, $gen, $csv, 1.0, 1, 1),
            ',',
        );
        let rows = {
            let s = tpcds_session!(ds_ref, $sf, 1, 1);
            s.get_scaling().get_row_count(ds_ref::config::Table::$table)
        };
        // Small tables (<= 2M rows at this SF): split into 20 chunks (or one
        // per row) and compare all of them, i.e. the whole table via the
        // skip path. Large tables: >= 12 chunks spread across the table.
        let small = rows <= 2_000_000;
        let chunks = if small {
            rows.min(20) as i32
        } else {
            (rows / $per_chunk).clamp(1, i32::MAX as u64) as i32
        };
        let which: Vec<i32> = if small { (1..=chunks).collect() } else { picks(chunks) };
        let mut skipped = 0;
        let mut ranges = 0;
        for chunk in which {
            let label = format!("{} sf{} chunk {}/{}", $name, $sf, chunk, chunks);
            skipped += compare(
                &label,
                tpcds_chunk!(ds_ref, $table, $gen, $csv, $sf, chunk, chunks),
                tpcds_chunk!(ds_new, $table, $gen, $csv, $sf, chunk, chunks),
                ',',
            );
            ranges += 1;
        }
        println!(
            "TPC-DS {:<22} sf1 full {:>9} rows | sf{} {:>2} chunks of {} {:>9} rows | OK ({:.1}s)",
            $name, full, $sf, ranges, chunks, skipped, t.elapsed().as_secs_f64()
        );
    }};
}

fn tpcds_all() {
    // Sales/returns: a source row is a ticket/order with several line items,
    // so smaller chunks. Everything else: ~100k source rows per chunk.
    std::thread::scope(|s| {
        s.spawn(|| {
            tpcds!("call_center", CallCenter, CallCenterRowGenerator, CallCenterCsv, 100.0, 100_000);
            tpcds!("catalog_page", CatalogPage, CatalogPageRowGenerator, CatalogPageCsv, 100.0, 100_000);
            tpcds!("warehouse", Warehouse, WarehouseRowGenerator, WarehouseCsv, 100.0, 100_000);
            tpcds!("ship_mode", ShipMode, ShipModeRowGenerator, ShipModeCsv, 100.0, 100_000);
            tpcds!("reason", Reason, ReasonRowGenerator, ReasonCsv, 100.0, 100_000);
            tpcds!("income_band", IncomeBand, IncomeBandRowGenerator, IncomeBandCsv, 100.0, 100_000);
            tpcds!("household_demographics", HouseholdDemographics, HouseholdDemographicsRowGenerator, HouseholdDemographicsCsv, 100.0, 100_000);
            tpcds!("date_dim", DateDim, DateDimRowGenerator, DateDimCsv, 100.0, 100_000);
            tpcds!("time_dim", TimeDim, TimeDimRowGenerator, TimeDimCsv, 100.0, 100_000);
            tpcds!("store", Store, StoreRowGenerator, StoreCsv, 100.0, 100_000);
            tpcds!("web_page", WebPage, WebPageRowGenerator, WebPageCsv, 100.0, 100_000);
            tpcds!("web_site", WebSite, WebSiteRowGenerator, WebSiteCsv, 100.0, 100_000);
            tpcds!("promotion", Promotion, PromotionRowGenerator, PromotionCsv, 100.0, 100_000);
        });
        s.spawn(|| {
            tpcds!("customer_demographics", CustomerDemographics, CustomerDemographicsRowGenerator, CustomerDemographicsCsv, 100.0, 100_000);
            tpcds!("customer_address", CustomerAddress, CustomerAddressRowGenerator, CustomerAddressCsv, 100.0, 100_000);
            tpcds!("customer", Customer, CustomerRowGenerator, CustomerCsv, 100.0, 100_000);
            tpcds!("item", Item, ItemRowGenerator, ItemCsv, 100.0, 100_000);
            tpcds!("inventory", Inventory, InventoryRowGenerator, InventoryCsv, 100.0, 100_000);
        });
        s.spawn(|| {
            tpcds!("store_sales", StoreSales, StoreSalesRowGenerator, StoreSalesCsv, 100.0, 10_000);
            tpcds!("store_returns", StoreReturns, StoreReturnsRowGenerator, StoreReturnsCsv, 100.0, 100_000);
        });
        s.spawn(|| {
            tpcds!("catalog_sales", CatalogSales, CatalogSalesRowGenerator, CatalogSalesCsv, 100.0, 10_000);
            tpcds!("catalog_returns", CatalogReturns, CatalogReturnsRowGenerator, CatalogReturnsCsv, 100.0, 100_000);
            tpcds!("web_sales", WebSales, WebSalesRowGenerator, WebSalesCsv, 100.0, 10_000);
            tpcds!("web_returns", WebReturns, WebReturnsRowGenerator, WebReturnsCsv, 100.0, 100_000);
        });
    });
}

fn main() {
    let which = std::env::args().nth(1).unwrap_or_default();
    if which == "bench" {
        bench();
        return;
    }
    if which == "sweep" {
        sweep();
        return;
    }
    let t = Instant::now();
    if which != "tpcds" {
        tpch_all();
    }
    if which != "tpch" {
        tpcds_all();
    }
    println!(
        "ALL MATCH: {} rows, {} field values compared in {:.0}s",
        ROWS.load(Ordering::Relaxed),
        FIELDS.load(Ordering::Relaxed),
        t.elapsed().as_secs_f64()
    );
}

// ============================================================================
// Speed: best of 3, single-threaded, SF 1. "gen" = iterate rows only;
// "gen+fmt" = also render each row to text like the CLI's .tbl/CSV output.
// ============================================================================

fn best_of_3(mut f: impl FnMut() -> u64) -> (f64, u64) {
    let mut best = f64::MAX;
    let mut n = 0;
    for _ in 0..3 {
        let t = Instant::now();
        n = std::hint::black_box(f());
        best = best.min(t.elapsed().as_secs_f64());
    }
    (best, n)
}

fn count<T>(it: impl Iterator<Item = T>) -> u64 {
    it.map(std::hint::black_box).count() as u64
}

fn fmt_all<T: Display>(it: impl Iterator<Item = T>) -> u64 {
    use std::fmt::Write;
    let mut s = String::new();
    let mut n = 0;
    for r in it {
        s.clear();
        write!(s, "{r}").unwrap();
        n += std::hint::black_box(s.len()) as u64;
    }
    n
}

fn report(kind: &str, table: &str, (r, n): (f64, u64), (p, m): (f64, u64)) {
    assert_eq!(n, m);
    println!(
        "{kind:<7} {table:<24} released {:>8.1} ms  patched {:>8.1} ms  speedup {:>5.2}x",
        r * 1e3,
        p * 1e3,
        r / p
    );
}

macro_rules! bench_h {
    ($table:literal, $gen:ident) => {{
        let r = best_of_3(|| count(h_ref::generators::$gen::new(1.0, 1, 1).iter()));
        let p = best_of_3(|| count(h_new::generators::$gen::new(1.0, 1, 1).iter()));
        report("H gen", $table, r, p);
        let r = best_of_3(|| fmt_all(h_ref::generators::$gen::new(1.0, 1, 1).iter()));
        let p = best_of_3(|| fmt_all(h_new::generators::$gen::new(1.0, 1, 1).iter()));
        report("H +fmt", $table, r, p);
    }};
}

macro_rules! bench_ds {
    ($name:literal, $table:ident, $gen:ident, $csv:ident) => {{
        let gen_ref = || {
            let s = tpcds_session!(ds_ref, 1.0, 1, 1);
            let n = s.get_scaling().get_row_count(ds_ref::config::Table::$table);
            ds_ref::row::$gen::new(s, n)
        };
        let gen_new = || {
            let s = tpcds_session!(ds_new, 1.0, 1, 1);
            let n = s.get_scaling().get_row_count(ds_new::config::Table::$table);
            ds_new::row::$gen::new(s, n)
        };
        let r = best_of_3(|| count(gen_ref()));
        let p = best_of_3(|| count(gen_new()));
        report("DS gen", $name, r, p);
        let r = best_of_3(|| { use ds_ref::csv::CsvRow; fmt_all(gen_ref().map(|x| ds_ref::csv::$csv::new(&x).to_string())) });
        let p = best_of_3(|| { use ds_new::csv::CsvRow; fmt_all(gen_new().map(|x| ds_new::csv::$csv::new(&x).to_string())) });
        report("DS +fmt", $name, r, p);
    }};
}

pub fn bench() {
    bench_h!("part", PartGenerator);
    bench_h!("supplier", SupplierGenerator);
    bench_h!("partsupp", PartSuppGenerator);
    bench_h!("customer", CustomerGenerator);
    bench_h!("orders", OrderGenerator);
    bench_h!("lineitem", LineItemGenerator);
    bench_ds!("store_sales", StoreSales, StoreSalesRowGenerator, StoreSalesCsv);
    bench_ds!("catalog_sales", CatalogSales, CatalogSalesRowGenerator, CatalogSalesCsv);
    bench_ds!("web_sales", WebSales, WebSalesRowGenerator, WebSalesCsv);
    bench_ds!("inventory", Inventory, InventoryRowGenerator, InventoryCsv);
    bench_ds!("customer", Customer, CustomerRowGenerator, CustomerCsv);
    bench_ds!("customer_address", CustomerAddress, CustomerAddressRowGenerator, CustomerAddressCsv);
    bench_ds!("customer_demographics", CustomerDemographics, CustomerDemographicsRowGenerator, CustomerDemographicsCsv);
    bench_ds!("item", Item, ItemRowGenerator, ItemCsv);
    bench_ds!("date_dim", DateDim, DateDimRowGenerator, DateDimCsv);
}

// ============================================================================
// Scale-factor sweep: at each SF, >= 12 partitions (~20k rows each) spread
// across every table (all partitions for small tables), every value compared.
// ============================================================================

const H_SFS: [f64; 9] = [0.01, 0.1, 1.0, 10.0, 100.0, 1000.0, 10000.0, 30000.0, 100000.0];
const DS_SFS: [f64; 9] = [0.01, 0.1, 1.0, 10.0, 100.0, 1000.0, 10000.0, 100000.0, 300000.0];

/// Partitions to compare: all if there are <= 20, else picks().
fn sweep_parts(units: i64, per_part: i64) -> (i32, Vec<i32>) {
    let n = (units / per_part).clamp(1, i32::MAX as i64) as i32;
    let n = if units <= 20 * per_part { (units.min(20)).max(1) as i32 } else { n };
    let which = if n <= 20 { (1..=n).collect() } else { picks(n) };
    (n, which)
}

macro_rules! sweep_h {
    ($table:literal, $gen:ident, $count_gen:ident, $per_part:expr) => {{
        for sf in H_SFS {
            let t = Instant::now();
            let units = h_ref::generators::$count_gen::calculate_row_count(sf, 1, 1);
            let (parts, which) = sweep_parts(units, $per_part);
            let mut rows = 0;
            for &part in &which {
                let label = format!("{} sf{} part {}/{}", $table, sf, part, parts);
                rows += compare(
                    &label,
                    h_ref::generators::$gen::new(sf, part, parts).iter(),
                    h_new::generators::$gen::new(sf, part, parts).iter(),
                    '|',
                );
            }
            println!(
                "TPC-H  {:<22} sf{:<8} {:>2} of {:>7} parts {:>8} rows OK ({:.1}s)",
                $table, sf, which.len(), parts, rows, t.elapsed().as_secs_f64()
            );
        }
    }};
}

macro_rules! sweep_ds {
    ($name:literal, $table:ident, $gen:ident, $csv:ident, $per_chunk:expr) => {{
        for sf in DS_SFS {
            let t = Instant::now();
            let units = {
                let s = tpcds_session!(ds_ref, sf, 1, 1);
                s.get_scaling().get_row_count(ds_ref::config::Table::$table) as i64
            };
            if units == 0 {
                continue;
            }
            let (chunks, which) = sweep_parts(units, $per_chunk);
            let mut rows = 0;
            let mut both_panic = 0;
            for &chunk in &which {
                let label = format!("{} sf{} chunk {}/{}", $name, sf, chunk, chunks);
                // The released crate panics on some inputs (upstream bugs).
                // Then the patched crate must panic on the same chunk too.
                let ref_ok = std::panic::catch_unwind(|| {
                    count(tpcds_chunk!(ds_ref, $table, $gen, $csv, sf, chunk, chunks))
                })
                .is_ok();
                if !ref_ok {
                    let new_ok = std::panic::catch_unwind(|| {
                        count(tpcds_chunk!(ds_new, $table, $gen, $csv, sf, chunk, chunks))
                    })
                    .is_ok();
                    assert!(!new_ok, "{label}: released panics but patched does not");
                    both_panic += 1;
                    continue;
                }
                rows += compare(
                    &label,
                    tpcds_chunk!(ds_ref, $table, $gen, $csv, sf, chunk, chunks),
                    tpcds_chunk!(ds_new, $table, $gen, $csv, sf, chunk, chunks),
                    ',',
                );
            }
            println!(
                "TPC-DS {:<22} sf{:<8} {:>2} of {:>7} chunks {:>8} rows OK{} ({:.1}s)",
                $name, sf, which.len(), chunks, rows,
                if both_panic > 0 { format!(", {both_panic} chunks panic in BOTH (upstream bug)") } else { String::new() },
                t.elapsed().as_secs_f64()
            );
        }
    }};
}

pub fn sweep() {
    let t = Instant::now();
    std::thread::scope(|s| {
        s.spawn(|| {
            sweep_h!("part", PartGenerator, PartGenerator, 20_000);
            sweep_h!("supplier", SupplierGenerator, SupplierGenerator, 20_000);
            sweep_h!("partsupp", PartSuppGenerator, PartSuppGenerator, 5_000);
            sweep_h!("customer", CustomerGenerator, CustomerGenerator, 20_000);
            sweep_h!("orders", OrderGenerator, OrderGenerator, 20_000);
            sweep_h!("lineitem", LineItemGenerator, OrderGenerator, 5_000);
        });
        s.spawn(|| {
            sweep_ds!("store_sales", StoreSales, StoreSalesRowGenerator, StoreSalesCsv, 2_000);
            sweep_ds!("store_returns", StoreReturns, StoreReturnsRowGenerator, StoreReturnsCsv, 20_000);
            sweep_ds!("inventory", Inventory, InventoryRowGenerator, InventoryCsv, 20_000);
            sweep_ds!("customer_demographics", CustomerDemographics, CustomerDemographicsRowGenerator, CustomerDemographicsCsv, 20_000);
            sweep_ds!("household_demographics", HouseholdDemographics, HouseholdDemographicsRowGenerator, HouseholdDemographicsCsv, 20_000);
            sweep_ds!("income_band", IncomeBand, IncomeBandRowGenerator, IncomeBandCsv, 20_000);
        });
        s.spawn(|| {
            sweep_ds!("catalog_sales", CatalogSales, CatalogSalesRowGenerator, CatalogSalesCsv, 2_000);
            sweep_ds!("catalog_returns", CatalogReturns, CatalogReturnsRowGenerator, CatalogReturnsCsv, 20_000);
            sweep_ds!("web_sales", WebSales, WebSalesRowGenerator, WebSalesCsv, 2_000);
            sweep_ds!("web_returns", WebReturns, WebReturnsRowGenerator, WebReturnsCsv, 20_000);
            sweep_ds!("catalog_page", CatalogPage, CatalogPageRowGenerator, CatalogPageCsv, 20_000);
            sweep_ds!("date_dim", DateDim, DateDimRowGenerator, DateDimCsv, 20_000);
            sweep_ds!("time_dim", TimeDim, TimeDimRowGenerator, TimeDimCsv, 20_000);
        });
        s.spawn(|| {
            sweep_ds!("customer", Customer, CustomerRowGenerator, CustomerCsv, 20_000);
            sweep_ds!("customer_address", CustomerAddress, CustomerAddressRowGenerator, CustomerAddressCsv, 20_000);
            sweep_ds!("item", Item, ItemRowGenerator, ItemCsv, 20_000);
            sweep_ds!("store", Store, StoreRowGenerator, StoreCsv, 20_000);
            sweep_ds!("promotion", Promotion, PromotionRowGenerator, PromotionCsv, 20_000);
            sweep_ds!("call_center", CallCenter, CallCenterRowGenerator, CallCenterCsv, 20_000);
            sweep_ds!("warehouse", Warehouse, WarehouseRowGenerator, WarehouseCsv, 20_000);
            sweep_ds!("web_page", WebPage, WebPageRowGenerator, WebPageCsv, 20_000);
            sweep_ds!("web_site", WebSite, WebSiteRowGenerator, WebSiteCsv, 20_000);
            sweep_ds!("ship_mode", ShipMode, ShipModeRowGenerator, ShipModeCsv, 20_000);
            sweep_ds!("reason", Reason, ReasonRowGenerator, ReasonCsv, 20_000);
        });
    });
    println!(
        "SWEEP ALL MATCH: {} rows, {} field values compared in {:.0}s",
        ROWS.load(Ordering::Relaxed),
        FIELDS.load(Ordering::Relaxed),
        t.elapsed().as_secs_f64()
    );
}
