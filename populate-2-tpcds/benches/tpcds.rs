//! populate-2-tpcds vs tpcdsgen-arrow: full tables at SF 1, 8000-row batches,
//! single-threaded. Batches are returned so they're dropped after timing.

use arrow_array::RecordBatch;
use divan::counter::ItemsCount;
use divan::{Bencher, black_box};
use populate_2_tpcds::tables::Scale;
use populate_2_tpcds::{CustomerDemographics, Inventory, Table, batches};
use tpcdsgen::config::SessionBuilder;
use tpcdsgen_arrow::{CustomerDemographicsArrow, InventoryArrow};

const SF: f64 = 1.0;
const BATCH: usize = 8000;

fn main() {
    divan::main();
}

fn rows<T: Table>() -> usize {
    T::row_count(&Scale::new(SF)) as usize
}

#[divan::bench_group(sample_count = 10)]
mod inventory {
    use super::*;
    #[divan::bench]
    fn populate2(b: Bencher) {
        b.counter(ItemsCount::new(rows::<Inventory>()))
            .bench(|| batches::<Inventory>(black_box(SF), BATCH).collect::<Vec<RecordBatch>>());
    }
    #[divan::bench]
    fn tpcdsgen(b: Bencher) {
        b.counter(ItemsCount::new(rows::<Inventory>())).bench(|| {
            let s = SessionBuilder::new()
                .with_scale_factor(black_box(SF))
                .build()
                .unwrap();
            InventoryArrow::new(s).collect::<Vec<_>>()
        });
    }
}

#[divan::bench_group(sample_count = 10)]
mod customer_demographics {
    use super::*;
    #[divan::bench]
    fn populate2(b: Bencher) {
        b.counter(ItemsCount::new(rows::<CustomerDemographics>()))
            .bench(|| {
                batches::<CustomerDemographics>(black_box(SF), BATCH).collect::<Vec<RecordBatch>>()
            });
    }
    #[divan::bench]
    fn tpcdsgen(b: Bencher) {
        b.counter(ItemsCount::new(rows::<CustomerDemographics>()))
            .bench(|| {
                let s = SessionBuilder::new()
                    .with_scale_factor(black_box(SF))
                    .build()
                    .unwrap();
                CustomerDemographicsArrow::new(s).collect::<Vec<_>>()
            });
    }
}
