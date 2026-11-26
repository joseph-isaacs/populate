//! Times Arrow → `.dat` formatting alone, over batches held in memory.
use populate_gen_tpcds::dat::write_batch;
use populate_gen_tpcds::{Encoding, TableName, Tpcds};
use std::time::Instant;

fn main() {
    let ds = Tpcds::new(1.0);
    for t in [
        TableName::StoreSales,
        TableName::CatalogSales,
        TableName::Inventory,
        TableName::Customer,
    ] {
        let b = ds
            .table(t)
            .limit_source_rows(match t {
                TableName::StoreSales => 50_000,
                TableName::CatalogSales => 25_000,
                TableName::Inventory => 1_000_000,
                _ => 100_000,
            })
            .collect(4);
        let mut out = Vec::with_capacity(1 << 28);
        let mut best = f64::MAX;
        for _ in 0..15 {
            out.clear();
            let s = Instant::now();
            for b in &b {
                write_batch(b, Encoding::Utf8, &mut out);
            }
            best = best.min(s.elapsed().as_secs_f64());
        }
        let rows: usize = b.iter().map(|b| b.num_rows()).sum();
        println!(
            "{t}: {:.2} ns/row, {:.0} MB/s",
            best * 1e9 / rows as f64,
            out.len() as f64 / best / 1e6
        );
    }
}
