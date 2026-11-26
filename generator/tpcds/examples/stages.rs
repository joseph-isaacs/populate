//! Per-table, per-stage single-thread cost at SF 1: tpcdsgen row
//! generation alone, then our Arrow, `.dat` and Parquet output (each
//! includes the stages before it), in ms, best of 3.
//!
//! `cargo run -p populate-gen-tpcds --release --example stages`
use populate_gen_tpcds::{Encoding, TableName, Tpcds};
use populate_table::Chunks;
use std::hint::black_box;
use std::io::sink;
use std::time::Instant;
use tpcdsgen::config::{SessionBuilder, Table as T};
use tpcdsgen::row::*;

fn best(f: impl Fn()) -> f64 {
    (0..3)
        .map(|_| {
            let t = Instant::now();
            f();
            t.elapsed().as_secs_f64()
        })
        .fold(f64::MAX, f64::min)
}

macro_rules! rows {
    ($r:ident, $t:expr) => {{
        let s = SessionBuilder::new()
            .with_scale_factor(1.0)
            .build()
            .unwrap();
        let n = s.get_scaling().get_row_count($t);
        best(|| {
            for r in $r::new(s.clone(), n) {
                black_box(r);
            }
        })
    }};
}

fn main() {
    use TableName::*;
    let ds = Tpcds::new(1.0);
    println!("| table | rows | tpcdsgen rows | +Arrow | +.dat | +Parquet |");
    println!("|---|---:|---:|---:|---:|---:|");
    let mut tot = [0f64; 4];
    for (t, r) in [
        (StoreSales, rows!(StoreSalesRowGenerator, T::StoreSales)),
        (
            CatalogSales,
            rows!(CatalogSalesRowGenerator, T::CatalogSales),
        ),
        (WebSales, rows!(WebSalesRowGenerator, T::WebSales)),
        (
            StoreReturns,
            rows!(StoreReturnsRowGenerator, T::StoreReturns),
        ),
        (
            CatalogReturns,
            rows!(CatalogReturnsRowGenerator, T::CatalogReturns),
        ),
        (WebReturns, rows!(WebReturnsRowGenerator, T::WebReturns)),
        (Inventory, rows!(InventoryRowGenerator, T::Inventory)),
        (
            CustomerDemographics,
            rows!(CustomerDemographicsRowGenerator, T::CustomerDemographics),
        ),
        (Customer, rows!(CustomerRowGenerator, T::Customer)),
        (
            CustomerAddress,
            rows!(CustomerAddressRowGenerator, T::CustomerAddress),
        ),
        (Item, rows!(ItemRowGenerator, T::Item)),
        (DateDim, rows!(DateDimRowGenerator, T::DateDim)),
        (TimeDim, rows!(TimeDimRowGenerator, T::TimeDim)),
        (CatalogPage, rows!(CatalogPageRowGenerator, T::CatalogPage)),
    ] {
        let tb = ds.table(t);
        let mut n = 0;
        tb.for_each_batch(1, |b| n += b.num_rows());
        // Batches dropped as made: `for_each_batch` holds a whole chunk to
        // hand it over in order, which costs page faults, not generation.
        let a = best(|| {
            for i in 0..tb.num_chunks() {
                tb.chunk(i, &mut |b| drop(black_box(b)));
            }
        });
        let d = best(|| {
            black_box(tb.write_dat(sink(), 1, Encoding::Utf8).unwrap());
        });
        let p = best(|| {
            black_box(tb.write_parquet(sink(), 1, None).unwrap());
        });
        for (x, v) in tot.iter_mut().zip([r, a, d, p]) {
            *x += v;
        }
        println!(
            "| {t} | {n} | {:.0} | {:.0} | {:.0} | {:.0} |",
            r * 1e3,
            a * 1e3,
            d * 1e3,
            p * 1e3
        );
    }
    println!(
        "| **sum** | | {:.0} | {:.0} | {:.0} | {:.0} |",
        tot[0] * 1e3,
        tot[1] * 1e3,
        tot[2] * 1e3,
        tot[3] * 1e3
    );
}
