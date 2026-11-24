//! Parquet time per row of one Utf8View column of URL-like strings (a long
//! shared prefix), PLAIN and uncompressed, on one thread. The first run
//! also pays for faulting in fresh memory; with mimalloc (as the CLIs use)
//! the later runs reuse it.
//!
//! `cargo run -p populate-table --features parquet --release --example plain_strings`
use arrow_array::{ArrayRef, RecordBatch, StringViewArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use populate_table::{Chunks, write_parquet};
use std::sync::Arc;
use std::time::Instant;

struct Urls(SchemaRef, Vec<RecordBatch>);

impl Chunks for Urls {
    fn schema(&self) -> SchemaRef {
        self.0.clone()
    }
    fn num_chunks(&self) -> u64 {
        self.1.len() as u64
    }
    fn chunk(&self, i: u64, emit: &mut dyn FnMut(RecordBatch)) {
        emit(self.1[i as usize].clone())
    }
}

fn main() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "url",
        DataType::Utf8View,
        false,
    )]));
    let (rows, chunk) = (1 << 20, 1 << 17);
    let batches = (0..rows / chunk)
        .map(|c| {
            let a: StringViewArray = (0..chunk)
                .map(|i| {
                    let k = (c * chunk + i) as u64 * 0x9E37_79B9_7F4A_7C15;
                    Some(format!(
                        "http://www.example.com/catalog/item/{}?ref={}",
                        k >> 40,
                        k % 997
                    ))
                })
                .collect();
            RecordBatch::try_new(schema.clone(), vec![Arc::new(a) as ArrayRef]).unwrap()
        })
        .collect();
    let urls = Urls(schema, batches);
    let props = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .set_compression(Compression::UNCOMPRESSED)
        .build();
    for _ in 0..3 {
        let t = Instant::now();
        let out = write_parquet(&urls, Vec::new(), 1, Some(props.clone())).unwrap();
        let ns = t.elapsed().as_secs_f64() * 1e9 / rows as f64;
        println!("{ns:.1} ns/row, {} bytes", out.len());
    }
}
