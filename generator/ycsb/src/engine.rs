//! What YCSB tables share: splitting source rows into chunks and
//! batches, and the drivers that run several tables on one pool.

use arrow_array::RecordBatch;
use parquet::errors::ParquetError;
use parquet::file::properties::WriterProperties;
use populate_table::text::{self, TextChunks};
use populate_table::{Chunks, Jobs, MultiChunks, par_ordered};
use std::convert::Infallible;
use std::io::{self, Write};

/// Default output rows per chunk (one Parquet row group).
pub const DEFAULT_CHUNK_ROWS: u64 = 128 * 1024;
/// Default rows per Arrow batch.
pub const DEFAULT_BATCH_SIZE: usize = 8 * 1024;

/// How a table's source rows are cut into chunks and batches.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Plan {
    pub(crate) source_rows: u64,
    /// Average output rows per source row.
    pub(crate) fanout: f64,
    pub(crate) chunk_source_rows: u64,
    pub(crate) batch_size: usize,
}

impl Plan {
    pub(crate) fn new(source_rows: u64, fanout: f64) -> Self {
        Plan {
            source_rows,
            fanout,
            chunk_source_rows: 1,
            batch_size: DEFAULT_BATCH_SIZE,
        }
        .chunk_rows(DEFAULT_CHUNK_ROWS)
    }

    pub(crate) fn chunk_rows(mut self, rows: u64) -> Self {
        self.chunk_source_rows = ((rows as f64 / self.fanout).round() as u64).max(1);
        self
    }

    pub(crate) fn chunk_source_rows(mut self, n: u64) -> Self {
        assert!(n > 0, "chunk_source_rows must be > 0");
        self.chunk_source_rows = n;
        self
    }

    pub(crate) fn batch_size(mut self, n: usize) -> Self {
        assert!(n > 0, "batch_size must be > 0");
        self.batch_size = n;
        self
    }

    pub(crate) fn num_chunks(&self) -> u64 {
        self.source_rows.div_ceil(self.chunk_source_rows)
    }

    /// Source rows `first..end` of chunk `i`.
    pub(crate) fn range(&self, i: u64) -> (u64, u64) {
        let first = i * self.chunk_source_rows;
        (
            first,
            (first + self.chunk_source_rows).min(self.source_rows),
        )
    }

    /// Call `f(first, n)` for each batch of source rows in `first..end`.
    pub(crate) fn batches(&self, first: u64, end: u64, mut f: impl FnMut(u64, u64)) {
        let per = ((self.batch_size as f64 / self.fanout).ceil() as u64).max(1);
        let mut at = first;
        while at < end {
            let n = per.min(end - at);
            f(at, n);
            at += n;
        }
    }
}

/// Write several tables as Parquet, `tables[i]` to `writers[i]`, on one
/// shared pool of `threads` threads (0 = all cores): every chunk of every
/// table is one job and one row group. `props: None` means the
/// populate-table defaults.
pub fn write_parquet_many<T: Chunks, W: Write + Send>(
    tables: &[T],
    writers: Vec<W>,
    threads: usize,
    props: Option<WriterProperties>,
) -> Result<Vec<W>, ParquetError> {
    assert_eq!(tables.len(), writers.len(), "one writer per table");
    let srcs: Vec<&dyn MultiChunks> = tables.iter().map(|t| t as &dyn MultiChunks).collect();
    populate_table::write_parquet_many(&srcs, writers, threads, props)
}

/// Write several tables as text (TSV) on one shared pool,
/// like [`write_parquet_many`]. The workers write the files themselves.
pub fn write_text_many<T: TextChunks, W: Write + Send>(
    tables: &[T],
    writers: Vec<W>,
    threads: usize,
) -> io::Result<Vec<W>> {
    assert_eq!(tables.len(), writers.len(), "one writer per table");
    let srcs: Vec<&dyn TextChunks> = tables.iter().map(|t| t as &dyn TextChunks).collect();
    text::write_text_many(&srcs, writers, threads)
}

/// Generate several tables on one shared pool and call `f(i, batch)` with
/// each batch of `tables[i]`, in order per table, on the calling thread.
pub fn for_each_batch_many<T: Chunks>(
    tables: &[T],
    threads: usize,
    mut f: impl FnMut(usize, RecordBatch),
) {
    let jobs = Jobs::new(tables.iter().map(|t| t.num_chunks()));
    let r: Result<(), Infallible> = par_ordered(
        jobs.total(),
        threads,
        |j| {
            let (t, i) = jobs.locate(j);
            let mut v = Vec::new();
            tables[t].chunk(i, &mut |b| v.push(b));
            (t, v)
        },
        |(t, v)| {
            v.into_iter().for_each(|b| f(t, b));
            Ok(())
        },
    );
    r.unwrap_or_else(|e| match e {});
}
