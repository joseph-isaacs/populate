//! `write_parquet_many` encodes each field of a row group as a job of its
//! own: the files must not depend on the thread count, for several sources,
//! a source with several outputs, chunks with no rows for an output, nested
//! (arrow-rs) and flat (direct) columns, and it must stop at a failing
//! writer.

use arrow_array::builder::{Int64Builder, ListBuilder};
use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::properties::WriterProperties;
use populate_table::{MultiChunks, write_parquet_many};
use std::io::{self, Write};
use std::sync::Arc;

/// Orders and their lines: chunk `i` has `i % 4` lines per order, so every
/// fourth chunk has none.
struct Sales {
    chunks: u64,
}

fn orders_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("note", DataType::Utf8, true),
        Field::new(
            "tags",
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            true,
        ),
    ]))
}

fn lines_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("order", DataType::Int64, false),
        Field::new("qty", DataType::Int64, true),
    ]))
}

impl MultiChunks for Sales {
    fn schemas(&self) -> Vec<SchemaRef> {
        vec![orders_schema(), lines_schema()]
    }

    fn num_chunks(&self) -> u64 {
        self.chunks
    }

    fn chunk(&self, i: u64, emit: &mut dyn FnMut(usize, RecordBatch)) {
        for b in 0..3 {
            let ids: Vec<i64> = (0..500).map(|r| (i * 3 + b) as i64 * 500 + r).collect();
            let notes: Vec<Option<String>> = ids
                .iter()
                .map(|x| (x % 3 != 0).then(|| format!("note {}", x % 17)))
                .collect();
            let mut tags = ListBuilder::new(Int64Builder::new());
            for x in &ids {
                if x % 5 == 0 {
                    tags.append_null();
                } else {
                    tags.values().append_slice(&[*x, x % 7]);
                    tags.append(true);
                }
            }
            let cols: Vec<ArrayRef> = vec![
                Arc::new(Int64Array::from(ids.clone())),
                Arc::new(StringArray::from(notes)),
                Arc::new(tags.finish()),
            ];
            emit(0, RecordBatch::try_new(orders_schema(), cols).unwrap());
            let per = (i % 4) as usize;
            let order: Vec<i64> = ids
                .iter()
                .flat_map(|&x| std::iter::repeat_n(x, per))
                .collect();
            let qty: Vec<Option<i64>> = order
                .iter()
                .enumerate()
                .map(|(k, x)| (k % 9 != 0).then_some(x % 50))
                .collect();
            let cols: Vec<ArrayRef> = vec![
                Arc::new(Int64Array::from(order)),
                Arc::new(Int64Array::from(qty)),
            ];
            emit(1, RecordBatch::try_new(lines_schema(), cols).unwrap());
        }
    }
}

fn write(threads: usize) -> Vec<Vec<u8>> {
    let (a, b) = (Sales { chunks: 9 }, Sales { chunks: 2 });
    let srcs: [&dyn MultiChunks; 2] = [&a, &b];
    let props = WriterProperties::builder().build();
    write_parquet_many(&srcs, vec![Vec::new(); 4], threads, Some(props)).unwrap()
}

fn rows(file: &[u8]) -> (usize, usize) {
    let r = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(file.to_vec())).unwrap();
    let groups = r.metadata().num_row_groups();
    let rows = r.build().unwrap().map(|b| b.unwrap().num_rows()).sum();
    (groups, rows)
}

#[test]
fn files_do_not_depend_on_the_thread_count() {
    let one = write(1);
    for threads in [2, 3, 4, 8] {
        assert_eq!(write(threads), one, "{threads} threads");
    }
    // orders: a row group per chunk; lines: none for chunks with no lines
    assert_eq!(rows(&one[0]), (9, 9 * 1500));
    assert_eq!(rows(&one[1]), (6, 1500 * (1 + 2 + 3) * 2));
    assert_eq!(rows(&one[2]), (2, 2 * 1500));
    assert_eq!(rows(&one[3]), (1, 1500));
}

/// A writer that fails after `ok` bytes.
struct Fails {
    ok: usize,
}

impl Write for Fails {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.len() > self.ok {
            return Err(io::Error::other("full"));
        }
        self.ok -= buf.len();
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn stops_at_a_failing_writer() {
    let a = Sales { chunks: 40 };
    let srcs: [&dyn MultiChunks; 1] = [&a];
    for threads in [1, 4] {
        let writers = vec![Fails { ok: 20_000 }, Fails { ok: usize::MAX }];
        assert!(write_parquet_many(&srcs, writers, threads, None).is_err());
    }
}
