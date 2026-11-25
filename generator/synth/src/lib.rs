//! Synthetic dataset shapes, each one populate [`Distribution`] run by
//! `populate-table`'s [`DistTable`].
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
//! use populate_synth::{Shape, Taxi};
//!
//! let trips = Taxi::new(42).table(10_000);
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
use parquet::arrow::ArrowSchemaConverter;
use parquet::basic::Compression;
use parquet::errors::ParquetError;
use parquet::file::properties::WriterProperties;
use populate_core::{Columnar, Columns, Distribution};
use populate_sink_arrow::IntoArrowColumns;
use populate_table::chunks::{for_each_batch, try_for_each_batch};
use populate_table::{Chunks, DistTable};
use std::fmt::{self, Display, Formatter};
use std::io::Write;

/// Default rows per batch.
pub const DEFAULT_BATCH_SIZE: usize = 8192;
/// Default rows per chunk (one Parquet row group, one job on the pool).
pub const DEFAULT_CHUNK_ROWS: u64 = 128 * 1024;

/// A table whose rows are each a pure function of the row number: one
/// [`Distribution`] of named columns, its schema read off it.
pub trait Shape: Sync {
    /// Table name, e.g. `taxi`.
    fn name(&self) -> &'static str;

    /// The table's rows. Row `r` depends only on `r`, so any range of rows
    /// is the same however the table is cut.
    fn dist(
        &self,
    ) -> impl Distribution<Item: Columnar<Columns: IntoArrowColumns>> + Clone + Send + Sync + use<Self>;

    /// Whether a column is written PLAIN by default: nearly every value distinct within
    /// a chunk (ids, times, continuous floats), where a dictionary costs
    /// time and makes the file bigger.
    fn is_plain(&self, column: &str) -> bool {
        let _ = column;
        false
    }

    /// Whether a column is written uncompressed by default: random bits
    /// (continuous floats, vectors) that Snappy cannot shrink but still
    /// spends time on.
    fn is_random(&self, column: &str) -> bool {
        let _ = column;
        false
    }

    /// `rows` rows of this shape.
    fn table(
        &self,
        rows: u64,
    ) -> Synth<
        impl Distribution<Item: Columnar<Columns: IntoArrowColumns>> + Clone + Send + Sync + use<Self>,
    >
    where
        Self: Sized,
    {
        Synth::from_dist(self, self.dist(), rows)
    }
}

/// `rows` rows of a [`Shape`], cut into chunks of batches: a [`Chunks`]
/// source for `populate-table`'s parallel drivers and Parquet writer.
#[derive(Debug, Clone)]
pub struct Synth<D> {
    table: DistTable<D>,
    plain: Vec<String>,
    random: Vec<String>,
    batch_size: usize,
    chunk_rows: u64,
}

impl<D> Synth<D>
where
    D: Distribution + Clone + Sync,
    Columns<D::Item>: IntoArrowColumns,
{
    fn from_dist(shape: &impl Shape, dist: D, rows: u64) -> Self {
        let table = DistTable::new(shape.name(), rows, dist);
        let cols = || table.schema().fields().iter().map(|f| f.name());
        let plain = cols().filter(|c| shape.is_plain(c)).cloned().collect();
        let random = cols().filter(|c| shape.is_random(c)).cloned().collect();
        Synth {
            table,
            plain,
            random,
            batch_size: DEFAULT_BATCH_SIZE,
            chunk_rows: DEFAULT_CHUNK_ROWS,
        }
        .cut()
    }

    /// The table's batch and chunk sizes from ours: chunks of whole batches.
    fn cut(mut self) -> Self {
        let bs = self.batch_size as u64;
        let chunk = self.chunk_rows.div_ceil(bs) * bs;
        self.table = self.table.batch_rows(bs).chunk_rows(chunk);
        self
    }

    /// Rows per batch (the data does not depend on it).
    pub fn batch_size(mut self, n: usize) -> Self {
        assert!(n > 0, "batch_size must be > 0");
        self.batch_size = n;
        self.cut()
    }

    /// Rows per chunk, rounded up to whole batches (the data does not
    /// depend on it).
    pub fn chunk_rows(mut self, n: u64) -> Self {
        assert!(n > 0, "chunk_rows must be > 0");
        self.chunk_rows = n;
        self.cut()
    }

    pub fn name(&self) -> &str {
        self.table.name()
    }

    pub fn rows(&self) -> u64 {
        self.table.rows()
    }

    /// Rows `start..start + len` as one batch.
    pub fn slice(&self, start: u64, len: usize) -> RecordBatch {
        assert!(
            start
                .checked_add(len as u64)
                .is_some_and(|end| end <= self.rows()),
            "slice past the end"
        );
        self.table.batch(start, len as u64)
    }

    /// The whole table as one batch (`Hits` holds at most 4 GiB of URL
    /// bytes in one batch, about 90 million rows).
    pub fn collect(&self) -> RecordBatch {
        self.slice(
            0,
            usize::try_from(self.rows()).expect("table fits in memory"),
        )
    }

    /// All batches, in order, on this thread.
    pub fn batches(&self) -> impl Iterator<Item = RecordBatch> + '_ {
        let bs = self.batch_size as u64;
        (0..self.rows().div_ceil(bs)).map(move |i| {
            let start = i * bs;
            self.table.batch(start, (self.rows() - start).min(bs))
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
    /// row group per chunk. `None` uses [`Synth::parquet_props`]; given
    /// `props` are used as they are, so start from `parquet_props` to keep
    /// the shape's plain columns.
    pub fn write_parquet<W: Write + Send>(
        &self,
        writer: W,
        threads: usize,
        props: Option<WriterProperties>,
    ) -> Result<W, ParquetError> {
        let props = props.unwrap_or_else(|| self.parquet_props());
        populate_table::write_parquet(self, writer, threads, Some(props))
    }

    /// [`parquet_props`], with dictionaries off for the shape's
    /// [`is_plain`](Shape::is_plain) columns and compression off for
    /// [`is_random`](Shape::is_random) ones (every leaf under them).
    pub fn parquet_props(&self) -> WriterProperties {
        let mut b = WriterProperties::builder().set_compression(Compression::SNAPPY);
        let descr = ArrowSchemaConverter::new()
            .convert(self.table.schema())
            .expect("shape schemas convert to Parquet");
        for c in descr.columns() {
            let root = &c.path().parts()[0];
            if self.plain.contains(root) {
                b = b.set_column_dictionary_enabled(c.path().clone(), false);
            }
            if self.random.contains(root) {
                b = b.set_column_compression(c.path().clone(), Compression::UNCOMPRESSED);
            }
        }
        b.build()
    }
}

/// Snappy, everything else arrow-rs's defaults (dictionaries on for every
/// column).
pub fn parquet_props() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build()
}

impl<D> Chunks for Synth<D>
where
    D: Distribution + Clone + Sync,
    Columns<D::Item>: IntoArrowColumns,
{
    fn schema(&self) -> SchemaRef {
        self.table.schema().clone()
    }

    fn num_chunks(&self) -> u64 {
        self.table.num_chunks()
    }

    fn chunk(&self, i: u64, emit: &mut dyn FnMut(RecordBatch)) {
        self.table.chunk(i, emit)
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
