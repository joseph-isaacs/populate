//! [`DistTable`]: a table that is one distribution, its schema read off the
//! distribution; [`DistTables`]: several tables that one distribution makes
//! together; [`DynTable`]: a [`DistTable`] with the distribution's type
//! erased, so tables of different distributions have one type.
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

use crate::chunks::{Chunks, MultiChunks};
use crate::stream::mix64;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use populate_core::{Columnar, Columns, Distribution, Tables};
use populate_sink_arrow::{IntoArrowColumns, IntoArrowTables, schema_of, schemas_of};
use rand::SeedableRng;
use rand::rngs::SmallRng;
use std::fmt;
use std::sync::Arc;

/// Default rows per chunk (one Parquet row group).
pub const DEFAULT_CHUNK_ROWS: u64 = 256 * 1024;
/// Default rows per Arrow batch.
pub const DEFAULT_BATCH_ROWS: usize = 8 * 1024;

/// `rows` source rows of `dist`, as Arrow batches.
///
/// Rows `first..first + n` are `dist`, cloned, skipped `first` rows and
/// collected `n`. For a distribution whose `skip` lands exactly where
/// running would (every built-in does), any range is the same rows
/// whichever order and thread they are made on. A distribution of
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
            chunk_rows: DEFAULT_CHUNK_ROWS,
            batch_rows: DEFAULT_BATCH_ROWS as u64,
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
        self.next_batch(&mut d, first, n)
    }

    /// The next `n` rows of `d`, which is at row `first`.
    fn next_batch(&self, d: &mut D, first: u64, n: u64) -> RecordBatch {
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
    /// The distribution skips to `first` once and runs on from there: the
    /// same rows as a [`batch`](Self::batch) per range when its `skip`
    /// lands exactly where running would.
    pub fn range(&self, first: u64, end: u64, emit: &mut dyn FnMut(RecordBatch)) {
        let mut d = self.dist.clone();
        d.skip(first);
        let mut at = first;
        while at < end {
            let n = self.batch_rows.min(end - at);
            emit(self.next_batch(&mut d, at, n));
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

/// Several tables made together by one distribution of `rows` source rows:
/// its rows are [`Tables`] of a tuple of
/// [`Children`](populate_core::Children), one per table
/// (`populate_builtins::flat_map`), so one pass over a range of source rows
/// makes every table's rows for it.
///
/// Chunks, batches and their reproducibility are as for [`DistTable`]; a
/// chunk is the same source rows in every table, so every table gets one
/// batch (and one Parquet row group) per chunk-batch of source rows, empty
/// ones included. The schemas are [`schemas_of`] the distribution.
#[derive(Debug, Clone)]
pub struct DistTables<D> {
    names: Vec<String>,
    rows: u64,
    dist: D,
    schemas: Vec<SchemaRef>,
    chunk_rows: u64,
    batch_rows: u64,
}

impl<D, T> DistTables<D>
where
    D: Distribution<Item = Tables<T>> + Clone + Sync,
    T: Columnar<Columns: IntoArrowTables>,
{
    /// Tables `names` (one per table the distribution makes), from `rows`
    /// source rows of `dist`, in chunks of 256Ki source rows and batches of
    /// 8Ki.
    pub fn new(names: &[&str], rows: u64, dist: D) -> Self {
        let schemas: Vec<SchemaRef> = schemas_of(&dist).into_iter().map(Arc::new).collect();
        assert_eq!(
            names.len(),
            schemas.len(),
            "one name per table the distribution makes"
        );
        DistTables {
            names: names.iter().map(|&n| n.to_owned()).collect(),
            rows,
            dist,
            schemas,
            chunk_rows: DEFAULT_CHUNK_ROWS,
            batch_rows: DEFAULT_BATCH_ROWS as u64,
        }
    }

    pub fn names(&self) -> &[String] {
        &self.names
    }

    /// Source rows.
    pub fn rows(&self) -> u64 {
        self.rows
    }

    pub fn schemas(&self) -> &[SchemaRef] {
        &self.schemas
    }

    pub fn dist(&self) -> &D {
        &self.dist
    }

    /// Source rows per chunk (a chunk is one Parquet row group per table).
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

    /// Source rows `first..first + n`: one batch per table, in order.
    pub fn batches(&self, first: u64, n: u64) -> Vec<RecordBatch> {
        let mut d = self.dist.clone();
        d.skip(first);
        let mut rng = SmallRng::seed_from_u64(mix64(first));
        let cols = d.collect(&mut rng, usize::try_from(n).expect("batch fits in memory"));
        let mut tables = Vec::with_capacity(self.schemas.len());
        cols.tables.into_tables(&mut tables);
        tables
            .into_iter()
            .zip(&self.schemas)
            .zip(&self.names)
            .map(|((arrays, schema), name)| {
                RecordBatch::try_new(schema.clone(), arrays)
                    .unwrap_or_else(|e| panic!("table `{name}`: columns don't make a batch: {e}"))
            })
            .collect()
    }

    /// Source rows `first..end` as batches of [`batch_rows`](Self::batch_rows),
    /// passed to `emit(table, batch)`.
    pub fn range(&self, first: u64, end: u64, emit: &mut dyn FnMut(usize, RecordBatch)) {
        let mut at = first;
        while at < end {
            let n = self.batch_rows.min(end - at);
            for (k, b) in self.batches(at, n).into_iter().enumerate() {
                emit(k, b);
            }
            at += n;
        }
    }
}

impl<D, T> MultiChunks for DistTables<D>
where
    D: Distribution<Item = Tables<T>> + Clone + Sync,
    T: Columnar<Columns: IntoArrowTables>,
{
    fn schemas(&self) -> Vec<SchemaRef> {
        self.schemas.clone()
    }

    fn num_chunks(&self) -> u64 {
        self.rows.div_ceil(self.chunk_rows)
    }

    fn chunk(&self, i: u64, emit: &mut dyn FnMut(usize, RecordBatch)) {
        let first = i * self.chunk_rows;
        self.range(first, (first + self.chunk_rows).min(self.rows), emit)
    }
}

/// A [`DistTable`] behind one type, whatever its distribution: a data set's
/// tables in one `Vec`, or returned from one `match`.
///
/// ```
/// use populate_builtins::{named, range, uniform};
/// use populate_table::DynTable;
///
/// let tables = [
///     DynTable::new("a", 10, named("id", range(1i64, 1))),
///     DynTable::new("b", 10, (named("x", uniform(0i32, 9)), named("y", range(0u64, 2)))),
/// ];
/// assert_eq!(tables[1].schema().fields().len(), 2);
/// assert_eq!(tables[0].clone().limit_rows(4).batch(0, 4).num_rows(), 4);
/// ```
#[derive(Clone)]
pub struct DynTable {
    table: Arc<dyn Ranges>,
    rows: u64,
    chunk_rows: u64,
    batch_rows: u64,
}

/// What [`DynTable`] needs of a [`DistTable`], object-safe.
trait Ranges: Send + Sync {
    fn name(&self) -> &str;
    fn schema(&self) -> &SchemaRef;
    fn batch(&self, first: u64, n: u64) -> RecordBatch;
}

impl<D> Ranges for DistTable<D>
where
    D: Distribution + Clone + Send + Sync,
    Columns<D::Item>: IntoArrowColumns,
{
    fn name(&self) -> &str {
        DistTable::name(self)
    }

    fn schema(&self) -> &SchemaRef {
        DistTable::schema(self)
    }

    fn batch(&self, first: u64, n: u64) -> RecordBatch {
        DistTable::batch(self, first, n)
    }
}

impl DynTable {
    /// `rows` source rows of `dist`, in chunks of 256Ki source rows and
    /// batches of 8Ki (as [`DistTable::new`]).
    pub fn new<D>(name: impl Into<String>, rows: u64, dist: D) -> Self
    where
        D: Distribution + Clone + Send + Sync + 'static,
        Columns<D::Item>: IntoArrowColumns,
    {
        let t = DistTable::new(name, rows, dist);
        DynTable {
            chunk_rows: t.chunk_rows,
            batch_rows: t.batch_rows,
            rows,
            table: Arc::new(t),
        }
    }

    pub fn name(&self) -> &str {
        self.table.name()
    }

    /// Source rows.
    pub fn rows(&self) -> u64 {
        self.rows
    }

    pub fn schema(&self) -> &SchemaRef {
        self.table.schema()
    }

    /// Only the first `n` source rows: a prefix of the table.
    pub fn limit_rows(mut self, n: u64) -> Self {
        self.rows = self.rows.min(n);
        self
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
        self.table.batch(first, n)
    }

    /// Source rows `first..end` as batches of
    /// [`batch_rows`](Self::batch_rows), cut at the table's end.
    pub fn range(&self, first: u64, end: u64, emit: &mut dyn FnMut(RecordBatch)) {
        let mut at = first;
        let end = end.min(self.rows);
        while at < end {
            let n = self.batch_rows.min(end - at);
            emit(self.batch(at, n));
            at += n;
        }
    }
}

impl fmt::Debug for DynTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DynTable")
            .field("name", &self.name())
            .field("rows", &self.rows)
            .field("chunk_rows", &self.chunk_rows)
            .field("batch_rows", &self.batch_rows)
            .finish()
    }
}

impl Chunks for DynTable {
    fn schema(&self) -> SchemaRef {
        self.schema().clone()
    }

    fn num_chunks(&self) -> u64 {
        self.rows.div_ceil(self.chunk_rows)
    }

    fn chunk(&self, i: u64, emit: &mut dyn FnMut(RecordBatch)) {
        let first = i * self.chunk_rows;
        self.range(first, first + self.chunk_rows, emit)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int64Array;
    use arrow_array::cast::AsArray;
    use arrow_array::types::Int64Type;
    use populate_builtins::{flat_map, named_all, range};
    use populate_core::{Children, Sink};

    type Two = Tables<(Children<i64>, Children<(i64, i64)>)>;

    /// Per key `k`: `k % 3` lines of `k`, and a return `(k, 10k)` for odd `k`.
    fn tables() -> DistTables<impl Distribution<Item = Two> + Clone + Sync> {
        let d = flat_map::<Two, _, _>(range(0i64, 1), |k: &i64, out: &mut Columns<Two>| {
            let (lines, rets) = &mut out.tables;
            for _ in 0..k % 3 {
                lines.values.push(*k);
            }
            if k % 2 == 1 {
                rets.values.push((*k, k * 10));
            }
        });
        DistTables::new(
            &["lines", "returns"],
            10,
            named_all(&["l_key", "r_key", "r_amount"], d),
        )
    }

    #[test]
    fn schemas_split_the_names() {
        let t = tables();
        let s = t.schemas();
        assert_eq!(s.len(), 2);
        assert_eq!(s[0].fields().len(), 1);
        assert_eq!(s[0].field(0).name(), "l_key");
        assert_eq!(s[1].field(0).name(), "r_key");
        assert_eq!(s[1].field(1).name(), "r_amount");
    }

    #[test]
    fn any_chunking_is_the_same_rows() {
        let whole = |t: &DistTables<_>| {
            let mut out = [Vec::new(), Vec::new()];
            for i in 0..MultiChunks::num_chunks(t) {
                MultiChunks::chunk(t, i, &mut |k, b| {
                    out[k].extend(b.column(0).as_primitive::<Int64Type>().values().to_vec())
                });
            }
            out
        };
        let a = whole(&tables());
        assert_eq!(a[0], [1, 2, 2, 4, 5, 5, 7, 8, 8]);
        assert_eq!(a[1], [1, 3, 5, 7, 9]);
        assert_eq!(whole(&tables().chunk_rows(3).batch_rows(2)), a);
        let b = tables().batches(3, 2);
        // keys 3 and 4: a line of 4, a return of 3
        assert_eq!(
            b[0].column(0).as_primitive::<Int64Type>(),
            &Int64Array::from(vec![4])
        );
        assert_eq!(
            b[1].column(1).as_primitive::<Int64Type>(),
            &Int64Array::from(vec![30])
        );
    }
}
