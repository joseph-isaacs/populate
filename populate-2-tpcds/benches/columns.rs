//! Per-column cost of inventory (1M rows each), vs just writing 1M i32s.

use divan::counter::ItemsCount;
use divan::{Bencher, black_box};
use populate_2::arrow::IntoArrowArray;
use populate_2::traits::{DistributionScalar, VecBackend};
use populate_2_tpcds::columns::NullI32Col;
use populate_2_tpcds::tables::{InvKey, InventoryKey, InventoryQuantity, Scale};
use populate_2_tpch::column::col;
use populate_2_tpch::tables::NoRng;
use tpcdsgen::config::Table as DsTable;

const N: usize = 1_000_000;

fn main() {
    divan::main();
}

fn key(k: InvKey) -> InventoryKey {
    let s = Scale::new(1.0);
    InventoryKey::new(
        k,
        s.ids(DsTable::Item),
        s.rows(DsTable::Warehouse),
        s.rows(DsTable::Item),
    )
}

#[divan::bench(sample_count = 30)]
fn baseline_write_i32(b: Bencher) {
    b.counter(ItemsCount::new(N)).bench(|| {
        let mut v = Vec::with_capacity(N);
        for i in 0..black_box(N) as i32 {
            v.push(i);
        }
        v
    });
}

#[divan::bench(sample_count = 30, args = ["date", "item", "warehouse"])]
fn inv_key(b: Bencher, which: &str) {
    let k = match which {
        "date" => InvKey::Date,
        "item" => InvKey::Item,
        _ => InvKey::Warehouse,
    };
    let c = key(k);
    b.counter(ItemsCount::new(N)).bench(|| {
        let mut d = col(c, 0);
        let s = <_ as DistributionScalar<VecBackend>>::collect_col(
            &mut d,
            Vec::<i32>::new(),
            &mut NoRng,
            black_box(N),
        );
        IntoArrowArray::into_array(s)
    });
}

#[divan::bench(sample_count = 30)]
fn inv_quantity(b: Bencher) {
    b.counter(ItemsCount::new(N)).bench(|| {
        let mut d = col(InventoryQuantity, 0);
        let s = <_ as DistributionScalar<VecBackend>>::collect_col(
            &mut d,
            NullI32Col::default(),
            &mut NoRng,
            black_box(N),
        );
        IntoArrowArray::into_array(s)
    });
}
