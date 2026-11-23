//! Parquet encoding time per column of one table, on one thread, through
//! populate-table's writer (direct encoding where it applies).
//!
//! `cargo run -p populate-tpch --release --example columns -- [table] [sf]`
use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use populate_table::{Chunks, write_parquet};
use populate_tpch::{TableName, Tpch, parquet_props};
use std::io::sink;
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
    let t: TableName = args
        .next()
        .as_deref()
        .unwrap_or("lineitem")
        .parse()
        .unwrap();
    let sf: f64 = args.next().map_or(1.0, |s| s.parse().unwrap());
    let table = Tpch::new(sf).table(t);
    Tpch::prepare(0);
    let start = Instant::now();
    let batches = table.collect(1);
    println!("generate: {:.3} s", start.elapsed().as_secs_f64());
    let schema = table.schema();
    let mut total = 0.0;
    for (i, f) in schema.fields().iter().enumerate() {
        let s = Arc::new(Schema::new(vec![f.as_ref().clone()]));
        let col = Column(
            s.clone(),
            batches
                .iter()
                .map(|b| RecordBatch::try_new(s.clone(), vec![b.column(i).clone()]).unwrap())
                .collect(),
        );
        let start = Instant::now();
        write_parquet(&col, sink(), 1, Some(parquet_props().build())).unwrap();
        let secs = start.elapsed().as_secs_f64();
        total += secs;
        println!("{:<18} {secs:.3} s", f.name());
    }
    println!("all columns: {total:.3} s");
}
