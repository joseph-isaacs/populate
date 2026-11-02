//! populate-2-tpch vs tpchgen-arrow: full tables at SF 1 in 8000-row batches,
//! single-threaded. The 300 MB text pool is built before timing for both.
//! Batches are returned so they're dropped after timing.

use arrow_array::RecordBatch;
use divan::counter::ItemsCount;
use divan::{Bencher, black_box};
use populate_2_tpch::values::POOL_BUFFER;
use populate_2_tpch::{Customer, Part, PartSupp, Supplier, Table, batches};
use tpchgen::generators::{CustomerGenerator, PartGenerator, PartSuppGenerator, SupplierGenerator};
use tpchgen::text::TextPool;
use tpchgen_arrow::{CustomerArrow, PartArrow, PartSuppArrow, SupplierArrow};

const SF: f64 = 1.0;
const BATCH: usize = 8000;

fn main() {
    TextPool::get_or_init_default();
    std::sync::LazyLock::force(&POOL_BUFFER);
    divan::main();
}

fn ours<T: Table>(b: Bencher) {
    b.counter(ItemsCount::new(T::row_count(SF) as usize))
        .bench(|| batches::<T>(black_box(SF), BATCH).collect::<Vec<RecordBatch>>());
}

#[divan::bench_group(sample_count = 15)]
mod part {
    use super::*;
    #[divan::bench]
    fn populate2(b: Bencher) {
        ours::<Part>(b)
    }
    #[divan::bench]
    fn tpchgen(b: Bencher) {
        b.counter(ItemsCount::new(Part::row_count(SF) as usize))
            .bench(|| {
                PartArrow::new(PartGenerator::new(black_box(SF), 1, 1))
                    .with_batch_size(BATCH)
                    .collect::<Vec<_>>()
            });
    }
}

#[divan::bench_group(sample_count = 15)]
mod partsupp {
    use super::*;
    #[divan::bench]
    fn populate2(b: Bencher) {
        ours::<PartSupp>(b)
    }
    #[divan::bench]
    fn tpchgen(b: Bencher) {
        b.counter(ItemsCount::new(PartSupp::row_count(SF) as usize))
            .bench(|| {
                PartSuppArrow::new(PartSuppGenerator::new(black_box(SF), 1, 1))
                    .with_batch_size(BATCH)
                    .collect::<Vec<_>>()
            });
    }
}

#[divan::bench_group(sample_count = 15)]
mod customer {
    use super::*;
    #[divan::bench]
    fn populate2(b: Bencher) {
        ours::<Customer>(b)
    }
    #[divan::bench]
    fn tpchgen(b: Bencher) {
        b.counter(ItemsCount::new(Customer::row_count(SF) as usize))
            .bench(|| {
                CustomerArrow::new(CustomerGenerator::new(black_box(SF), 1, 1))
                    .with_batch_size(BATCH)
                    .collect::<Vec<_>>()
            });
    }
}

#[divan::bench_group(sample_count = 15)]
mod supplier {
    use super::*;
    #[divan::bench]
    fn populate2(b: Bencher) {
        ours::<Supplier>(b)
    }
    #[divan::bench]
    fn tpchgen(b: Bencher) {
        b.counter(ItemsCount::new(Supplier::row_count(SF) as usize))
            .bench(|| {
                SupplierArrow::new(SupplierGenerator::new(black_box(SF), 1, 1))
                    .with_batch_size(BATCH)
                    .collect::<Vec<_>>()
            });
    }
}
