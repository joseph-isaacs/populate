//! Synthetic dataset shapes on `populate-table`'s [`Chunks`] engine.
//!
//! Four tables that look like data people benchmark on, with no reference
//! generator to match:
//!
//! - [`Taxi`]: NYC-taxi-like trips (the TLC yellow-cab schema): sorted
//!   pickup times with a daily rush-hour profile, log-normal distances,
//!   fares, tips and totals that add up, and the real data's correlated
//!   missing fields;
//! - [`Hits`]: ClickBench-like web-analytics hits: skewed counters and
//!   users, per-user devices and regions, generated URLs, referers and
//!   search phrases;
//! - [`Features`]: an ML feature table: `Float32` features (normal,
//!   skewed, correlated, with missing values), skewed categoricals, a train
//!   / valid / test split, and a label and a target that depend on them;
//! - [`Embeddings`]: clustered, L2-normalised vectors as
//!   `FixedSizeList<Float32>`.
//!
//! Every value is a pure function of `(seed, column, row)` (see
//! [`kernel`]), so a table can be cut into chunks of any size and
//! generated on any number of threads with the same bytes as one
//! sequential run, and the same seed always gives the same data.
//!
//! ```
//! use populate_synth::{Synth, Taxi};
//!
//! let trips = Synth::new(Taxi::new(42), 10_000);
//! let mut rows = 0;
//! trips.for_each_batch(0, |b| rows += b.num_rows());
//! assert_eq!(rows, 10_000);
//!
//! // any slice of rows on its own equals the same rows of the whole table
//! let all = trips.collect();
//! assert_eq!(trips.slice(1234, 10), all.slice(1234, 10));
//! ```

pub mod embeddings;
pub mod features;
pub mod hits;
pub mod kernel;
pub mod taxi;

pub use embeddings::Embeddings;
pub use features::Features;
pub use hits::Hits;
pub use taxi::Taxi;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use parquet::basic::Compression;
use parquet::errors::ParquetError;
use parquet::file::properties::WriterProperties;
use populate_table::Chunks;
use populate_table::chunks::{for_each_batch, try_for_each_batch};
use std::fmt::{self, Display, Formatter};
use std::io::Write;

/// Default rows per batch.
pub const DEFAULT_BATCH_SIZE: usize = 8192;
/// Default rows per chunk (one Parquet row group, one job on the pool).
pub const DEFAULT_CHUNK_ROWS: u64 = 128 * 1024;

/// A table whose rows are each a pure function of the row number.
pub trait Shape: Sync {
    /// Table name, e.g. `taxi`.
    fn name(&self) -> &'static str;

    /// Schema of every batch.
    fn schema(&self) -> SchemaRef;

    /// Rows `start..start + len`, which must equal those rows of any other
    /// batch that covers them.
    fn batch(&self, start: u64, len: usize) -> RecordBatch;
}

/// A [`Shape`] of `rows` rows, cut into chunks of batches: a [`Chunks`]
/// source for `populate-table`'s parallel drivers and Parquet writer.
#[derive(Debug, Clone)]
pub struct Synth<S> {
    shape: S,
    rows: u64,
    batch_size: usize,
    chunk_rows: u64,
}

impl<S: Shape> Synth<S> {
    pub fn new(shape: S, rows: u64) -> Self {
        Synth {
            shape,
            rows,
            batch_size: DEFAULT_BATCH_SIZE,
            chunk_rows: DEFAULT_CHUNK_ROWS,
        }
    }

    /// Rows per batch (the data does not depend on it).
    pub fn batch_size(mut self, n: usize) -> Self {
        assert!(n > 0, "batch_size must be > 0");
        self.batch_size = n;
        self
    }

    /// Rows per chunk, rounded up to whole batches (the data does not
    /// depend on it).
    pub fn chunk_rows(mut self, n: u64) -> Self {
        assert!(n > 0, "chunk_rows must be > 0");
        self.chunk_rows = n;
        self
    }

    pub fn shape(&self) -> &S {
        &self.shape
    }

    pub fn rows(&self) -> u64 {
        self.rows
    }

    fn chunk_len(&self) -> u64 {
        self.chunk_rows.div_ceil(self.batch_size as u64) * self.batch_size as u64
    }

    /// Rows `start..start + len` as one batch.
    pub fn slice(&self, start: u64, len: usize) -> RecordBatch {
        assert!(
            start
                .checked_add(len as u64)
                .is_some_and(|end| end <= self.rows),
            "slice past the end"
        );
        self.shape.batch(start, len)
    }

    /// The whole table as one batch (`Hits` holds at most 4 GiB of URL
    /// bytes in one batch, about 90 million rows).
    pub fn collect(&self) -> RecordBatch {
        self.slice(0, usize::try_from(self.rows).expect("table fits in memory"))
    }

    /// All batches, in order, on this thread.
    pub fn batches(&self) -> impl Iterator<Item = RecordBatch> + '_ {
        let bs = self.batch_size as u64;
        (0..self.rows.div_ceil(bs)).map(move |i| {
            let start = i * bs;
            self.shape
                .batch(start, (self.rows - start).min(bs) as usize)
        })
    }

    /// Every batch, generated on `threads` threads (0 = all cores), passed
    /// to `f` in order on the calling thread.
    pub fn for_each_batch(&self, threads: usize, f: impl FnMut(RecordBatch)) {
        for_each_batch(self, threads, f)
    }

    /// [`for_each_batch`](Self::for_each_batch) that stops at the first error.
    pub fn try_for_each_batch<E>(
        &self,
        threads: usize,
        f: impl FnMut(RecordBatch) -> Result<(), E>,
    ) -> Result<(), E> {
        try_for_each_batch(self, threads, f)
    }

    /// Generate and encode on `threads` threads into one Parquet file, one
    /// row group per chunk ([`parquet_props`] if `props` is `None`).
    pub fn write_parquet<W: Write + Send>(
        &self,
        writer: W,
        threads: usize,
        props: Option<WriterProperties>,
    ) -> Result<W, ParquetError> {
        populate_table::write_parquet(
            self,
            writer,
            threads,
            Some(props.unwrap_or_else(parquet_props)),
        )
    }
}

/// Snappy, everything else arrow-rs's defaults.
pub fn parquet_props() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build()
}

impl<S: Shape> Chunks for Synth<S> {
    fn schema(&self) -> SchemaRef {
        self.shape.schema()
    }

    fn num_chunks(&self) -> u64 {
        self.rows.div_ceil(self.chunk_len())
    }

    fn chunk(&self, i: u64, emit: &mut dyn FnMut(RecordBatch)) {
        let lo = i * self.chunk_len();
        let hi = (lo + self.chunk_len()).min(self.rows);
        let bs = self.batch_size as u64;
        let mut s = lo;
        while s < hi {
            let n = (hi - s).min(bs);
            emit(self.shape.batch(s, n as usize));
            s += n;
        }
    }
}

/// The shapes by name, for the CLI and benches.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ShapeName {
    Taxi,
    Hits,
    Features,
    Embeddings,
}

impl ShapeName {
    pub const ALL: [ShapeName; 4] = [
        ShapeName::Taxi,
        ShapeName::Hits,
        ShapeName::Features,
        ShapeName::Embeddings,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            ShapeName::Taxi => "taxi",
            ShapeName::Hits => "hits",
            ShapeName::Features => "features",
            ShapeName::Embeddings => "embeddings",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|n| n.as_str() == s)
    }
}

impl Display for ShapeName {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
