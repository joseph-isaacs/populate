//! Parquet output for [`Chunks`] and [`Table`] (feature `parquet`).
//!
//! Generation *and* encoding run on all cores: each worker takes one chunk,
//! generates its batches and encodes every column chunk of its row group,
//! and the calling thread appends finished row groups to the file in order.
//! The file's contents are the same for any thread count.

use crate::chunks::{Chunks, Jobs};
use crate::table::{Table, par_ordered};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_writer::{ArrowColumnChunk, ArrowColumnWriter, compute_leaves};
use parquet::errors::ParquetError;
use parquet::file::properties::WriterProperties;
use std::io::Write;

/// Write `src` as one Parquet file, one row group per chunk, generating and
/// encoding on `threads` threads (0 = all cores). Returns the writer.
pub fn write_parquet<W: Write + Send>(
    src: &impl Chunks,
    writer: W,
    threads: usize,
    props: Option<WriterProperties>,
) -> Result<W, ParquetError> {
    let mut w = write_parquet_many(&[src as &dyn Chunks], vec![writer], threads, props)?;
    Ok(w.pop().expect("one writer"))
}

/// Write several sources, `srcs[i]` to `writers[i]`, on one shared pool of
/// `threads` threads: every chunk of every source is a job, so small and
/// large tables keep all cores busy together. Jobs run in `srcs` order (put
/// the biggest first). Returns the writers in order.
pub fn write_parquet_many<W: Write + Send>(
    srcs: &[&dyn Chunks],
    writers: Vec<W>,
    threads: usize,
    props: Option<WriterProperties>,
) -> Result<Vec<W>, ParquetError> {
    assert_eq!(srcs.len(), writers.len(), "one writer per source");
    let props = props.unwrap_or_default();
    let schemas: Vec<SchemaRef> = srcs.iter().map(|s| s.schema()).collect();
    let mut files = Vec::with_capacity(srcs.len());
    let mut factories = Vec::with_capacity(srcs.len());
    for (w, schema) in writers.into_iter().zip(&schemas) {
        let (file, factory) = ArrowWriter::try_new(w, schema.clone(), Some(props.clone()))?
            .into_serialized_writer()?;
        files.push(file);
        factories.push(factory);
    }
    let jobs = Jobs::new(srcs.iter().map(|s| s.num_chunks()));

    let encode = |j: u64| -> (usize, Result<Vec<ArrowColumnChunk>, ParquetError>) {
        let (s, g) = jobs.locate(j);
        let run = || {
            let mut cols = factories[s].create_column_writers(g as usize)?;
            let mut err = Ok(());
            srcs[s].chunk(g, &mut |batch| {
                if err.is_ok() {
                    err = write_batch(&schemas[s], &batch, &mut cols);
                }
            });
            err?;
            cols.into_iter().map(|c| c.close()).collect()
        };
        (s, run())
    };
    par_ordered(jobs.total(), threads, encode, |(s, chunks)| {
        let mut rg = files[s].next_row_group()?;
        for c in chunks? {
            c.append_to_row_group(&mut rg)?;
        }
        rg.close().map(drop)
    })?;
    files.into_iter().map(|f| f.into_inner()).collect()
}

fn write_batch(
    schema: &SchemaRef,
    batch: &RecordBatch,
    cols: &mut [ArrowColumnWriter],
) -> Result<(), ParquetError> {
    let mut w = cols.iter_mut();
    for (field, array) in schema.fields().iter().zip(batch.columns()) {
        for leaf in compute_leaves(field, array)? {
            w.next().expect("one writer per leaf").write(&leaf)?;
        }
    }
    Ok(())
}

/// A [`Table`] as row groups of `per_group` batches.
struct TableGroups<'a> {
    table: &'a Table,
    batch_size: usize,
    per_group: u64,
}

impl Chunks for TableGroups<'_> {
    fn schema(&self) -> SchemaRef {
        self.table.schema().clone()
    }

    fn num_chunks(&self) -> u64 {
        self.table
            .num_batches(self.batch_size)
            .div_ceil(self.per_group)
    }

    fn chunk(&self, g: u64, emit: &mut dyn FnMut(RecordBatch)) {
        let batches = self.table.num_batches(self.batch_size);
        for i in g * self.per_group..((g + 1) * self.per_group).min(batches) {
            emit(self.table.batch(i, self.batch_size));
        }
    }
}

impl Table {
    /// Generate the table on `threads` threads (0 = all cores) and write it
    /// as one Parquet file. Row groups hold `props.max_row_group_row_count()`
    /// rows (default 1Mi) rounded up to whole batches. Returns the writer.
    pub fn write_parquet<W: Write + Send>(
        &self,
        writer: W,
        batch_size: usize,
        threads: usize,
        props: Option<WriterProperties>,
    ) -> Result<W, ParquetError> {
        let props = props.unwrap_or_default();
        let rows = props.max_row_group_row_count().unwrap_or(1024 * 1024);
        let groups = TableGroups {
            table: self,
            batch_size,
            per_group: (rows / batch_size).max(1) as u64,
        };
        write_parquet(&groups, writer, threads, Some(props))
    }
}
