//! TPC-DS generation throughput.
//!
//! `cargo bench -p populate-gen-dsb`. Each sample generates a prefix of up to
//! 64Ki source rows of a table at SF 1 from scratch; throughput is in output
//! rows/s and Arrow bytes/s.
//!
//! - `arrow`: one table, one thread, batches dropped as they come.
//! - `threads`: `store_sales` on 1, 2 and 4 threads.
//! - `parquet` / `dat`: `store_sales` to an in-memory Parquet file (snappy,
//!   `parquet_props`) or `.dat` text, 1 and 4 threads.

use divan::counter::{BytesCount, ItemsCount};
use divan::{Bencher, black_box};
use populate_gen_dsb::TableName::{self, *};
use populate_gen_dsb::{DsTable, Dsb, Encoding, parquet_props};

fn main() {
    divan::main();
}

fn table(t: TableName) -> DsTable {
    Dsb::new(1.0)
        .table(t)
        .limit_source_rows(64 * 1024)
        .chunk_rows(64 * 1024)
}

fn run(b: Bencher, t: TableName, threads: usize) {
    let t = table(t);
    let (mut rows, mut bytes) = (0, 0);
    t.for_each_batch(1, |b| {
        rows += b.num_rows();
        bytes += b.get_array_memory_size();
    });
    b.counter(ItemsCount::new(rows))
        .counter(BytesCount::new(bytes))
        .bench(|| t.for_each_batch(threads, |b| drop(black_box(b))));
}

mod arrow {
    use super::*;

    #[divan::bench(
        sample_count = 10,
        args = [StoreSales, CatalogSales, WebSales, StoreReturns, Inventory,
                Customer, CustomerAddress, CustomerDemographics, Item, DateDim]
    )]
    fn table(b: Bencher, t: TableName) {
        run(b, t, 1);
    }
}

#[divan::bench(sample_count = 10, args = [1, 2, 4])]
fn threads(b: Bencher, threads: usize) {
    run(b, StoreSales, threads);
}

#[divan::bench(sample_count = 5, args = [1, 4])]
fn parquet(b: Bencher, threads: usize) {
    let t = table(StoreSales);
    let props = || parquet_props().build();
    let size = t
        .write_parquet(Vec::new(), 1, Some(props()))
        .expect("parquet")
        .len();
    b.counter(BytesCount::new(size)).bench(|| {
        t.write_parquet(Vec::new(), threads, Some(props()))
            .expect("parquet")
    });
}

#[divan::bench(sample_count = 5, args = [1, 4])]
fn dat(b: Bencher, threads: usize) {
    let t = table(StoreSales);
    let size = t
        .write_dat(Vec::new(), 1, Encoding::Latin1)
        .expect("dat")
        .len();
    b.counter(BytesCount::new(size)).bench(|| {
        t.write_dat(Vec::new(), threads, Encoding::Latin1)
            .expect("dat")
    });
}
