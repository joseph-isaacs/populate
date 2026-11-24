//! Chunked output equals one sequential run for any chunk size and thread
//! count, the Arrow tables hold exactly the text's values, and Parquet
//! round-trips them.

use arrow_array::cast::AsArray;
use arrow_array::types::{Float32Type, Float64Type, Int64Type, TimestampNanosecondType};
use arrow_array::{Array, RecordBatch};
use arrow_schema::DataType;
use arrow_select::concat::concat_batches;
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use populate_table::MultiChunks;
use populate_tsbs::text::{push_f32, push_f64, push_i64};
use populate_tsbs::{Config, Format, Tsbs, UseCase};

fn cfg(uc: UseCase, scale: u64, seed: i64, secs: i64) -> Config {
    let mut c = Config::new(uc, scale, seed);
    c.end = c.start + secs * 1_000_000_000;
    c
}

fn runs() -> Vec<Config> {
    let mut v = Vec::new();
    for uc in UseCase::ALL {
        v.push(cfg(uc, 5, 11, 1800));
        let mut c = cfg(uc, 9, -3, 1200);
        c.initial_scale = 2;
        c.groups = 3;
        c.group_id = 1;
        v.push(c);
        let mut c = cfg(uc, 4, 5, 3600);
        c.limit = 1001;
        v.push(c);
    }
    v
}

#[test]
fn chunked_equals_sequential() {
    for c in runs() {
        let t = Tsbs::new(c.clone()).unwrap();
        for f in [Format::Influx, Format::TimescaleDb] {
            let want = t.write_text_sequential(f, Vec::new()).unwrap();
            for (threads, cp) in [(1, 1), (4, 1), (3, 7), (2, 100), (4, 4096), (1, 1 << 30)] {
                let got = t.write_text(f, Vec::new(), threads, cp).unwrap();
                assert!(got == want, "{c:?} {f} threads {threads} chunk {cp}");
            }
        }
    }
}

/// An Arrow row as the influx serializer writes it.
fn influx_line(b: &RecordBatch, name: &str, row: usize, out: &mut Vec<u8>) {
    let schema = b.schema();
    out.extend_from_slice(name.as_bytes());
    let mut fake = Vec::new();
    let mut fields = Vec::new();
    for (i, f) in schema.fields().iter().enumerate().skip(1) {
        let col = b.column(i);
        let tag_or_field = f.name();
        match f.data_type() {
            DataType::Utf8 if col.is_valid(row) => {
                out.push(b',');
                out.extend_from_slice(tag_or_field.as_bytes());
                out.push(b'=');
                out.extend_from_slice(col.as_string::<i32>().value(row).as_bytes());
            }
            DataType::Float32 if col.is_valid(row) => {
                let mut v = Vec::new();
                push_f32(&mut v, col.as_primitive::<Float32Type>().value(row));
                fake.push((tag_or_field.clone(), v));
            }
            DataType::Int64 if col.is_valid(row) => {
                let mut v = Vec::new();
                push_i64(&mut v, col.as_primitive::<Int64Type>().value(row));
                v.push(b'i');
                fields.push((tag_or_field.clone(), v));
            }
            DataType::Float64 if col.is_valid(row) => {
                let mut v = Vec::new();
                push_f64(&mut v, col.as_primitive::<Float64Type>().value(row));
                fields.push((tag_or_field.clone(), v));
            }
            _ => {}
        }
    }
    let all: Vec<_> = fake.into_iter().chain(fields).collect();
    if all.is_empty() {
        out.truncate(out.len() - name.len());
        return;
    }
    out.push(b' ');
    for (i, (k, v)) in all.iter().enumerate() {
        if i > 0 {
            out.push(b',');
        }
        out.extend_from_slice(k.as_bytes());
        out.push(b'=');
        out.extend_from_slice(v);
    }
    out.push(b' ');
    push_i64(
        out,
        b.column(0)
            .as_primitive::<TimestampNanosecondType>()
            .value(row),
    );
    out.push(b'\n');
}

#[test]
fn arrow_matches_text_and_parquet_round_trips() {
    for c in runs() {
        let t = Tsbs::new(c.clone()).unwrap();
        let text = t.write_text(Format::Influx, Vec::new(), 2, 0).unwrap();
        let a = t.arrow(300);
        let names = a.table_names();
        let mut tables: Vec<Vec<RecordBatch>> = vec![Vec::new(); names.len()];
        for i in 0..a.num_chunks() {
            a.chunk(i, &mut |k, b| tables[k].push(b));
        }
        // every measurement's rows, as influx lines, are the text's lines
        // of that measurement, in order
        for (k, name) in names.iter().enumerate() {
            let mut got = Vec::new();
            for b in &tables[k] {
                for r in 0..b.num_rows() {
                    influx_line(b, name, r, &mut got);
                }
            }
            let prefix = format!("{name},");
            let want: Vec<u8> = text
                .split_inclusive(|&b| b == b'\n')
                .filter(|l| l.starts_with(prefix.as_bytes()))
                .flatten()
                .copied()
                .collect();
            assert!(got == want, "{c:?} {name}");
        }
        // Parquet: one file per measurement, the same rows
        let files = t
            .write_parquet(vec![Vec::new(); names.len()], 3, 300, None)
            .unwrap();
        for (k, f) in files.into_iter().enumerate() {
            let r = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(f))
                .unwrap()
                .build()
                .unwrap();
            let read: Vec<RecordBatch> = r.map(Result::unwrap).collect();
            let schema = a.schemas()[k].clone();
            let cat = |v: &[RecordBatch]| concat_batches(&schema, v).unwrap();
            assert_eq!(cat(&read), cat(&tables[k]), "{c:?} {}", names[k]);
        }
    }
}
