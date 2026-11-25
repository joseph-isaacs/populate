//! [`DistTable`]: a table that is one distribution, its schema read off the
//! distribution.
//!
//! ```
//! use populate_builtins::{named, range, uniform};
//! use populate_table::DistTable;
//!
//! let t = DistTable::new("t", 10, (named("id", range(1i64, 1)), named("x", uniform(0i32, 9))));
//! assert_eq!(t.schema().field(1).name(), "x");
//! let b = t.batch(4, 3);
//! assert_eq!(b.num_rows(), 3);
//! ```

use crate::chunks::Chunks;
use crate::stream::mix64;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use populate_core::{Columns, Distribution};
use populate_sink_arrow::{IntoArrowColumns, schema_of};
use rand::SeedableRng;
use rand::rngs::SmallRng;
use std::sync::Arc;

/// `rows` source rows of `dist`, as Arrow batches.
///
/// Rows `first..first + n` are `dist`, cloned, skipped `first` rows and
/// collected `n`, so any range is the same rows whichever order and
/// thread they are made on. A distribution of
/// [`Children`](populate_core::Children) makes a child table: its rows are
/// the children of the source rows.
///
/// The schema is [`schema_of`] the distribution, worked out once, from
/// its names and its column types, without generating a row.
#[derive(Debug, Clone)]
pub struct DistTable<D> {
    name: String,
    rows: u64,
    dist: D,
    schema: SchemaRef,
    chunk_rows: u64,
    batch_rows: u64,
}

impl<D> DistTable<D>
where
    D: Distribution + Clone + Sync,
    Columns<D::Item>: IntoArrowColumns,
{
    /// `rows` source rows of `dist`, in chunks of 256Ki source rows and
    /// batches of 8Ki.
    pub fn new(name: impl Into<String>, rows: u64, dist: D) -> Self {
        let schema = Arc::new(schema_of(&dist));
        DistTable {
            name: name.into(),
            rows,
            dist,
            schema,
            chunk_rows: 256 * 1024,
            batch_rows: 8 * 1024,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Source rows.
    pub fn rows(&self) -> u64 {
        self.rows
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn dist(&self) -> &D {
        &self.dist
    }

    /// Source rows per chunk (a chunk is one Parquet row group).
    pub fn chunk_rows(mut self, n: u64) -> Self {
        assert!(n > 0, "chunk_rows must be > 0");
        self.chunk_rows = n;
        self
    }

    /// Source rows per batch.
    pub fn batch_rows(mut self, n: u64) -> Self {
        assert!(n > 0, "batch_rows must be > 0");
        self.batch_rows = n;
        self
    }

    /// Source rows `first..first + n` as one batch.
    pub fn batch(&self, first: u64, n: u64) -> RecordBatch {
        let mut d = self.dist.clone();
        d.skip(first);
        // for distributions that draw from the caller: seeded by position,
        // so a range is the same wherever it is made
        let mut rng = SmallRng::seed_from_u64(mix64(first));
        let cols = d.collect(&mut rng, usize::try_from(n).expect("batch fits in memory"));
        let mut arrays = Vec::with_capacity(<Columns<D::Item>>::LEN);
        cols.into_arrays(&mut arrays);
        RecordBatch::try_new(self.schema.clone(), arrays)
            .unwrap_or_else(|e| panic!("table `{}`: columns don't make a batch: {e}", self.name))
    }

    /// Source rows `first..end` as batches of [`batch_rows`](Self::batch_rows).
    pub fn range(&self, first: u64, end: u64, emit: &mut dyn FnMut(RecordBatch)) {
        let mut at = first;
        while at < end {
            let n = self.batch_rows.min(end - at);
            emit(self.batch(at, n));
            at += n;
        }
    }
}

impl<D> Chunks for DistTable<D>
where
    D: Distribution + Clone + Sync,
    Columns<D::Item>: IntoArrowColumns,
{
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn num_chunks(&self) -> u64 {
        self.rows.div_ceil(self.chunk_rows)
    }

    fn chunk(&self, i: u64, emit: &mut dyn FnMut(RecordBatch)) {
        let first = i * self.chunk_rows;
        self.range(first, (first + self.chunk_rows).min(self.rows), emit)
    }
}
