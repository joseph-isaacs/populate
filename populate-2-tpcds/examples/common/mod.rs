//! Shared by the sf benchmark examples: one streaming batch source per
//! (table, generator), and a byte-counting writer.

#![allow(dead_code)]

use arrow_array::RecordBatch;
use arrow_schema::ArrowError;
use std::io::Write;
use tpcdsgen::config::SessionBuilder;

pub const BATCH: usize = 8000;

pub const TABLES: [&str; 6] = [
    "H part",
    "H partsupp",
    "H customer",
    "H supplier",
    "DS inventory",
    "DS customer_demographics",
];

pub type Batches = Box<dyn Iterator<Item = RecordBatch>>;

/// Discards bytes, counts them.
pub struct Counter(pub u64);

impl Write for Counter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0 += buf.len() as u64;
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn ok(r: Result<RecordBatch, ArrowError>) -> RecordBatch {
    r.expect("reference batch")
}

pub fn source(table: &str, ours: bool, sf: f64) -> Batches {
    use tpchgen::generators::*;
    use tpchgen_arrow::*;
    let ds = || SessionBuilder::new().with_scale_factor(sf).build().unwrap();
    match (table, ours) {
        ("H part", true) => Box::new(populate_2_tpch::batches::<populate_2_tpch::Part>(sf, BATCH)),
        ("H part", false) => Box::new(PartArrow::new(PartGenerator::new(sf, 1, 1)).map(ok)),
        ("H partsupp", true) => Box::new(populate_2_tpch::batches::<populate_2_tpch::PartSupp>(
            sf, BATCH,
        )),
        ("H partsupp", false) => {
            Box::new(PartSuppArrow::new(PartSuppGenerator::new(sf, 1, 1)).map(ok))
        }
        ("H customer", true) => Box::new(populate_2_tpch::batches::<populate_2_tpch::Customer>(
            sf, BATCH,
        )),
        ("H customer", false) => {
            Box::new(CustomerArrow::new(CustomerGenerator::new(sf, 1, 1)).map(ok))
        }
        ("H supplier", true) => Box::new(populate_2_tpch::batches::<populate_2_tpch::Supplier>(
            sf, BATCH,
        )),
        ("H supplier", false) => {
            Box::new(SupplierArrow::new(SupplierGenerator::new(sf, 1, 1)).map(ok))
        }
        ("DS inventory", true) => Box::new(
            populate_2_tpcds::batches::<populate_2_tpcds::Inventory>(sf, BATCH),
        ),
        ("DS inventory", false) => Box::new(tpcdsgen_arrow::InventoryArrow::new(ds()).map(ok)),
        ("DS customer_demographics", true) => Box::new(populate_2_tpcds::batches::<
            populate_2_tpcds::CustomerDemographics,
        >(sf, BATCH)),
        ("DS customer_demographics", false) => {
            Box::new(tpcdsgen_arrow::CustomerDemographicsArrow::new(ds()).map(ok))
        }
        _ => unreachable!("{table}"),
    }
}

/// Fisher-Yates with xorshift64*.
pub fn shuffle<T>(v: &mut [T], seed: u64) {
    let mut x = seed | 1;
    for i in (1..v.len()).rev() {
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        let j = (x.wrapping_mul(0x2545F4914F6CDD1D) % (i as u64 + 1)) as usize;
        v.swap(i, j);
    }
}

pub fn seed_from_args() -> u64 {
    std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64
        })
}

pub fn peak_rss_mb() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines().find(|l| l.starts_with("VmHWM")).and_then(|l| {
                l.split_whitespace()
                    .nth(1)
                    .map(|kb| kb.parse::<u64>().unwrap() / 1024)
            })
        })
        .unwrap_or(0)
}

pub fn median(v: &[f64]) -> f64 {
    let mut v = v.to_vec();
    v.sort_by(f64::total_cmp);
    v[v.len() / 2]
}
