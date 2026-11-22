//! Parquet encoding time per column of one table, on one thread.
//!
//! `cargo run -p populate-tpch --release --example columns -- [table] [sf]`
use arrow_array::RecordBatch;
use arrow_schema::Schema;
use parquet::arrow::ArrowWriter;
use populate_tpch::{TableName, Tpch, parquet_props};
use std::io::sink;
use std::sync::Arc;
use std::time::Instant;

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
    let start = Instant::now();
    let batches = table.collect(1);
    println!("generate: {:.3} s", start.elapsed().as_secs_f64());
    let schema = table.schema();
    for (i, f) in schema.fields().iter().enumerate() {
        let s = Arc::new(Schema::new(vec![f.as_ref().clone()]));
        for dict in [true, false] {
            let props = parquet_props().set_dictionary_enabled(dict).build();
            let start = Instant::now();
            let mut w = ArrowWriter::try_new(sink(), s.clone(), Some(props)).unwrap();
            for b in &batches {
                let c = RecordBatch::try_new(s.clone(), vec![b.column(i).clone()]).unwrap();
                w.write(&c).unwrap();
            }
            w.close().unwrap();
            print!(
                "{:<18} dict={dict}: {:.3} s   ",
                f.name(),
                start.elapsed().as_secs_f64()
            );
        }
        println!();
    }
}
