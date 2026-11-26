//! Parquet encoding time and size per column, one thread, with each
//! column's dictionary on and off.
//!
//! `cargo run -p populate-gen-tpcds --release --example ds_columns -- [sf] [table]`
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Schema, SchemaRef};
use parquet::schema::types::ColumnPath;
use populate_gen_tpcds::{TableName, Tpcds, parquet_props};
use populate_table::{Chunks, write_parquet};
use std::sync::Arc;
use std::time::Instant;

/// One column of the generated batches, one row group per 32 batches.
struct Column(SchemaRef, Vec<RecordBatch>);

impl Chunks for Column {
    fn schema(&self) -> SchemaRef {
        self.0.clone()
    }
    fn num_chunks(&self) -> u64 {
        self.1.len().div_ceil(32) as u64
    }
    fn chunk(&self, i: u64, emit: &mut dyn FnMut(RecordBatch)) {
        let i = i as usize * 32;
        self.1[i..(i + 32).min(self.1.len())]
            .iter()
            .cloned()
            .for_each(emit);
    }
}

fn main() {
    let mut args = std::env::args().skip(1);
    let sf: f64 = args.next().map_or(1.0, |s| s.parse().unwrap());
    let only = args.next();
    let all_types = std::env::var("ALL").is_ok();
    for t in TableName::ALL {
        if only.as_deref().is_some_and(|o| o != t.as_str()) {
            continue;
        }
        let table = Tpcds::new(sf).table(t);
        let batches = table.collect(1);
        let schema = table.schema();
        for (i, f) in schema.fields().iter().enumerate() {
            if !all_types && !matches!(f.data_type(), DataType::Decimal128(..) | DataType::Int64) {
                continue;
            }
            let s = Arc::new(Schema::new(vec![f.as_ref().clone()]));
            let col = Column(
                s.clone(),
                batches
                    .iter()
                    .map(|b| RecordBatch::try_new(s.clone(), vec![b.column(i).clone()]).unwrap())
                    .collect(),
            );
            let mut res = Vec::new();
            for dict in [false, true] {
                let props = parquet_props()
                    .set_column_dictionary_enabled(ColumnPath::from(f.name().as_str()), dict);
                let start = Instant::now();
                let mut size = 0;
                for _ in 0..3 {
                    size = write_parquet(&col, Vec::new(), 1, Some(props.clone().build()))
                        .unwrap()
                        .len();
                }
                res.push((start.elapsed().as_secs_f64() / 3.0, size));
            }
            println!(
                "{:<28} plain {:.4} s {:>10} | dict {:.4} s {:>10} {}",
                f.name(),
                res[0].0,
                res[0].1,
                res[1].0,
                res[1].1,
                if res[1].0 < res[0].0 && res[1].1 < res[0].1 {
                    "WIN"
                } else {
                    ""
                }
            );
        }
    }
}
