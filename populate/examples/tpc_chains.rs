//! TPC-H `lineitem` and TPC-DS `store_sales` dependent-column chains, built
//! with `with_column` and generated column by column into Arrow.
//!
//! ```sh
//! cargo run --release -p populate --example tpc_chains
//! ```
//!
//! The rules follow the specs (TPC-H 4.2.3, TPC-DS store_sales pricing). The
//! values aren't dbgen/dsdgen's: see `populate-tpcds` for byte-identical
//! output. This shows how the dependencies read as combinators.

use arrow_array::RecordBatch;
use arrow_cast::pretty::pretty_format_batches;
use arrow_schema::{Field, Schema};
use populate::arrow::{ArrowBackend, DistributionArrow};
use populate::dist::{range, uniform, with_column};
use populate::traits::Distribution;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use std::sync::Arc;
use std::time::Instant;

// ============================================================================
// TPC-H lineitem
// ============================================================================

/// Days since 1970-01-01: 1992-01-01, 1998-12-31, and CURRENTDATE 1995-06-17.
const START: i32 = 8035;
const END: i32 = 10_591;
const CURRENT: i32 = 9298;

/// P_RETAILPRICE in cents, a function of the part key (TPC-H 4.2.3).
fn retail_cents(part: i64) -> i64 {
    90_000 + (part / 10) % 20_001 + 100 * (part % 1_000)
}

const LINEITEM: [&str; 12] = [
    "l_orderkey",
    "l_partkey",
    "l_quantity",
    "l_extendedprice",
    "l_discount",
    "l_tax",
    "o_orderdate",
    "l_shipdate",
    "l_commitdate",
    "l_receiptdate",
    "l_returnflag",
    "l_linestatus",
];

type Price = (i64, i64, i64);
type Dates = (i32, i32, i32, i32, (&'static str, &'static str));
type LineItem = (i64, Price, (i64, i64), Dates);

fn lineitem() -> impl Distribution<ArrowBackend, Item = LineItem> {
    // extendedprice = quantity * retailprice(partkey)
    let price = with_column(
        (uniform(1i64, 200_000), uniform(1i64, 50)),
        |(part, qty), _| qty * retail_cents(part),
    );

    // orderdate -> shipdate -> commitdate -> receiptdate -> flags
    let order = uniform(START, END - 151);
    let ship = with_column(order, |o, rng| o + rng.random_range(1..=121));
    let commit = with_column(ship, |(o, _), rng| o + rng.random_range(30..=90));
    let receipt = with_column(commit, |(_, s, _), rng| s + rng.random_range(1..=30));
    // Two string columns from one step (strings can't be read back as a
    // parent, so both flags come out of the same closure).
    let dates = with_column(receipt, |(_, s, _, r), rng| {
        let returnflag = match (r <= CURRENT, rng.random_bool(0.5)) {
            (true, true) => "R",
            (true, false) => "A",
            (false, _) => "N",
        };
        let linestatus = if s > CURRENT { "O" } else { "F" };
        (returnflag, linestatus)
    });

    (
        range(1i64, 1),
        price,
        (uniform(0i64, 10), uniform(0i64, 8)), // discount, tax (percent)
        dates,
    )
}

// ============================================================================
// TPC-DS store_sales pricing
// ============================================================================

const STORE_SALES: [&str; 8] = [
    "ss_quantity",
    "ss_wholesale_cost",
    "ss_list_price",
    "ss_sales_price",
    "ss_coupon_amt",
    "ss_net_paid",
    "ss_ext_tax",
    "ss_net_profit",
];

type StoreSales = (i64, i64, i64, i64, i64, i64, i64, i64);

/// All money in cents. Each step reads every column before it.
fn store_sales() -> impl Distribution<ArrowBackend, Item = StoreSales> {
    let base = (uniform(1i64, 100), uniform(100i64, 10_000)); // quantity, wholesale
    // list = wholesale marked up 0..=200%
    let list = with_column(base, |(_, w), rng| w + w * rng.random_range(0..=200) / 100);
    // sales = list discounted 0..=100%
    let sales = with_column(list, |(_, _, l), rng| {
        l - l * rng.random_range(0..=100) / 100
    });
    // 20% of lines get a coupon worth up to the whole line
    let coupon = with_column(sales, |(q, _, _, s), rng| {
        if rng.random_bool(0.2) {
            rng.random_range(0..=q * s)
        } else {
            0
        }
    });
    let net_paid = with_column(coupon, |(q, _, _, s, c), _| q * s - c);
    let tax = with_column(net_paid, |(_, _, _, _, _, np), rng| {
        np * rng.random_range(0..=9) / 100
    });
    with_column(tax, |(q, w, _, _, _, np, _), _| np - q * w) // net_profit
}

// ============================================================================

fn named(batch: RecordBatch, names: &[&str]) -> RecordBatch {
    let fields: Vec<Field> = (batch.schema().fields().iter().zip(names))
        .map(|(f, n)| f.as_ref().clone().with_name(*n))
        .collect();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), batch.columns().to_vec()).unwrap()
}

fn run<D: DistributionArrow<ArrowBackend>>(name: &str, mut d: D, names: &[&str])
where
    <D::Item as populate::traits::DefaultSink<ArrowBackend>>::Sink:
        Default + populate::arrow::IntoArrowColumns,
{
    let mut rng = SmallRng::seed_from_u64(42);
    let sample = named(d.collect_record_batch(&mut rng, 6).unwrap(), names);
    println!("== {name}\n{}", pretty_format_batches(&[sample]).unwrap());

    let n = 1 << 20;
    let t = Instant::now();
    let batch = d.collect_record_batch(&mut rng, n).unwrap();
    let secs = t.elapsed().as_secs_f64();
    println!(
        "{n} rows x {} columns in {:.1} ms ({:.0} M rows/s)\n",
        batch.num_columns(),
        secs * 1e3,
        n as f64 / secs / 1e6
    );
}

fn main() {
    run("TPC-H lineitem", lineitem(), &LINEITEM);
    run("TPC-DS store_sales", store_sales(), &STORE_SALES);
}
