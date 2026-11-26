//! [`SsbTable`]: one SSB table, generated in parallel chunks of source
//! rows, and the drivers that run several tables on one pool.

use crate::TableName;
use crate::flat::{self, Dims};
use crate::tables::{self, Scale, TableGen};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, SchemaRef};
use parquet::basic::Compression;
use parquet::errors::ParquetError;
use parquet::file::properties::{WriterProperties, WriterPropertiesBuilder};
use parquet::schema::types::ColumnPath;
use populate_table::text::{Encoding, TextChunks, write_batch, write_text_many};
use populate_table::{Chunks, MultiChunks, chunks};
use std::io::{self, Write};
use std::sync::{Arc, LazyLock, OnceLock};

/// Default output rows per chunk (one Parquet row group).
pub const DEFAULT_CHUNK_ROWS: u64 = 256 * 1024;
/// Default rows per Arrow batch (about: `lineorder` batches end on an
/// order).
pub const DEFAULT_BATCH_SIZE: usize = 8 * 1024;

/// One table of an [`Ssb`](crate::Ssb) data set.
///
/// The table is split into chunks of consecutive source rows (orders for
/// `lineorder` and `lineorder_flat`). Each chunk is generated from scratch
/// with every stream positioned at its first row, so chunks are
/// independent and their concatenation is exactly the sequential
/// (ssb-dbgen) output.
#[derive(Debug, Clone)]
pub struct SsbTable {
    name: TableName,
    scale: Scale,
    source_rows: u64,
    chunk_source_rows: u64,
    batch_size: usize,
    /// The table's distribution, run over any range of source rows: set
    /// when the table is made, except for `lineorder_flat`, whose
    /// distribution holds the dimension tables, so it is built on first
    /// use and shared by every `lineorder_flat` of the data set.
    generator: Arc<OnceLock<Arc<dyn TableGen>>>,
}

/// Every table's schema, read off its distribution (no rows generated).
static SCHEMAS: LazyLock<Vec<SchemaRef>> = LazyLock::new(|| {
    let s = Scale::new(1.0);
    TableName::ALL
        .iter()
        .map(|&t| tables::generator(t, &s).schema())
        .chain([flat::generator(&s, Arc::default()).schema()])
        .collect()
});

impl TableName {
    /// Average output rows per source row: 4 lines per order, 1 elsewhere.
    pub fn rows_per_source_row(self) -> f64 {
        match self {
            TableName::LineOrder | TableName::LineOrderFlat => 4.0,
            _ => 1.0,
        }
    }

    /// Arrow schema: `Int32` integers and `YYYYMMDD` dates (`Int64`
    /// `lo_orderkey`), `Utf8View` text, column names as in ssb-dbgen's
    /// `ssb.ddl`.
    pub fn schema(self) -> SchemaRef {
        let i = match self {
            TableName::LineOrderFlat => TableName::ALL.len(),
            t => TableName::ALL
                .iter()
                .position(|&a| a == t)
                .expect("a base table"),
        };
        SCHEMAS[i].clone()
    }
}

impl SsbTable {
    pub(crate) fn new(
        name: TableName,
        scale: Scale,
        source_rows: u64,
        flat: Arc<OnceLock<Arc<dyn TableGen>>>,
    ) -> Self {
        let generator = match name {
            TableName::LineOrderFlat => flat,
            t => Arc::new(OnceLock::from(tables::generator(t, &scale))),
        };
        SsbTable {
            name,
            scale,
            source_rows,
            chunk_source_rows: 1,
            batch_size: DEFAULT_BATCH_SIZE,
            generator,
        }
        .chunk_rows(DEFAULT_CHUNK_ROWS)
    }

    pub fn name(&self) -> TableName {
        self.name
    }

    pub fn schema(&self) -> SchemaRef {
        self.name.schema()
    }

    /// Source rows (orders for `lineorder`, the row count elsewhere).
    pub fn source_rows(&self) -> u64 {
        self.source_rows
    }

    /// Expected output rows (exact for all tables but the line orders).
    pub fn approx_rows(&self) -> u64 {
        (self.source_rows as f64 * self.name.rows_per_source_row()).round() as u64
    }

    /// Only the first `n` source rows: a prefix of the full table (the same
    /// rows, not a smaller scale factor).
    pub fn limit_source_rows(mut self, n: u64) -> Self {
        self.source_rows = self.source_rows.min(n);
        self
    }

    /// Aim for about `rows` output rows per chunk (= per Parquet row
    /// group). The output is the same for any chunk size.
    pub fn chunk_rows(mut self, rows: u64) -> Self {
        let per = (rows as f64 / self.name.rows_per_source_row()).round() as u64;
        self.chunk_source_rows = per.max(1);
        self
    }

    /// Chunks of exactly `n` source rows (the last may be shorter).
    pub fn chunk_source_rows(mut self, n: u64) -> Self {
        assert!(n > 0, "chunk_source_rows must be > 0");
        self.chunk_source_rows = n;
        self
    }

    /// Rows per Arrow batch (default 8Ki).
    pub fn batch_size(mut self, n: usize) -> Self {
        assert!(n > 0, "batch_size must be > 0");
        self.batch_size = n;
        self
    }

    /// Number of chunks.
    pub fn num_chunks(&self) -> u64 {
        self.source_rows.div_ceil(self.chunk_source_rows)
    }

    /// 0-based source-row range `first..end` of chunk `i`.
    fn range(&self, i: u64) -> (u64, u64) {
        let first = i * self.chunk_source_rows;
        (
            first,
            (first + self.chunk_source_rows).min(self.source_rows),
        )
    }

    /// Build what generation needs before the first chunk: for
    /// `lineorder_flat`, the dimension tables, on `threads` threads.
    fn prepare(&self, threads: usize) {
        self.generator(threads);
    }

    fn generator(&self, threads: usize) -> &dyn TableGen {
        let s = &self.scale;
        self.generator
            .get_or_init(|| {
                // only `lineorder_flat`'s is not set in `new`
                debug_assert_eq!(self.name, TableName::LineOrderFlat);
                flat::generator(s, Arc::new(Dims::build(s, threads)))
            })
            .as_ref()
    }

    /// Source rows `first..end` as batches of about `batch_size` rows.
    fn generate(&self, first: u64, end: u64, emit: &mut dyn FnMut(RecordBatch)) {
        let g = self.generator(0);
        let per = (self.batch_size as f64 / self.name.rows_per_source_row()).ceil() as u64;
        let mut at = first;
        while at < end {
            let n = per.max(1).min(end - at);
            emit(g.batch(at, n));
            at += n;
        }
    }

    /// Generate on `threads` threads (0 = all cores) and call `f` with each
    /// batch, in table order, on the calling thread.
    pub fn for_each_batch(&self, threads: usize, f: impl FnMut(RecordBatch)) {
        self.prepare(threads);
        chunks::for_each_batch(self, threads, f)
    }

    /// The whole table, in order.
    pub fn collect(&self, threads: usize) -> Vec<RecordBatch> {
        let mut v = Vec::new();
        self.for_each_batch(threads, |b| v.push(b));
        v
    }

    /// Write the table as one Parquet file, one row group per chunk,
    /// generating and encoding on `threads` threads (0 = all cores).
    /// `props: None` means [`parquet_props`].
    pub fn write_parquet<W: Write + Send>(
        &self,
        writer: W,
        threads: usize,
        props: Option<WriterProperties>,
    ) -> Result<W, ParquetError> {
        let mut w = write_parquet_many(std::slice::from_ref(self), vec![writer], threads, props)?;
        Ok(w.pop().expect("one writer"))
    }

    /// Write the table in ssb-dbgen's `.tbl` format (`|`-separated fields,
    /// a trailing `|`, one row per line), byte-identical to `dbgen -s <sf>`.
    /// Chunks are formatted on `threads` threads and written in order.
    pub fn write_tbl<W: Write + Send>(&self, writer: W, threads: usize) -> io::Result<W> {
        let mut w = write_tbl_many(std::slice::from_ref(self), vec![writer], threads)?;
        Ok(w.pop().expect("one writer"))
    }

    /// Source rows `first..end` (0-based) as `.tbl` text, generated on the
    /// calling thread: one chunk, wherever it starts.
    pub fn tbl_range(&self, first: u64, end: u64) -> Vec<u8> {
        self.prepare(0);
        let mut out = Vec::new();
        self.generate(first, end.min(self.source_rows), &mut |b| {
            write_batch(&b, Encoding::Utf8, &mut out)
        });
        out
    }
}

/// Driving an `SsbTable` through these traits directly (rather than its
/// own `for_each_batch` / `write_*` methods) skips its preparation: for
/// `lineorder_flat`, the first chunk then builds the dimension tables on
/// all cores while the other workers wait.
impl Chunks for SsbTable {
    fn schema(&self) -> SchemaRef {
        SsbTable::schema(self)
    }

    fn num_chunks(&self) -> u64 {
        SsbTable::num_chunks(self)
    }

    fn chunk(&self, i: u64, emit: &mut dyn FnMut(RecordBatch)) {
        let (first, end) = self.range(i);
        self.generate(first, end, emit)
    }
}

impl TextChunks for SsbTable {
    fn outputs(&self) -> usize {
        1
    }

    fn num_chunks(&self) -> u64 {
        SsbTable::num_chunks(self)
    }

    fn chunk_text(&self, i: u64, bufs: &mut [Vec<u8>]) {
        let (first, end) = self.range(i);
        let out = &mut bufs[0];
        self.generate(first, end, &mut |b| write_batch(&b, Encoding::Utf8, out));
    }
}

/// Parquet writer properties for SSB: dictionary encoding for every column
/// but the ones with too many distinct values for a dictionary to pay
/// (keys, prices, names, addresses and phone numbers). Snappy compression.
/// Used when `write_parquet` gets no properties; start from it to change
/// other settings.
pub fn parquet_props() -> WriterPropertiesBuilder {
    let mut p = WriterProperties::builder().set_compression(Compression::SNAPPY);
    let distinct = [
        "lo_orderkey",
        "lo_extendedprice",
        "lo_ordtotalprice",
        "lo_revenue",
        "lo_supplycost",
        "c_custkey",
        "s_suppkey",
        "p_partkey",
        "c_name",
        "s_name",
        "c_address",
        "s_address",
        "c_phone",
        "s_phone",
    ];
    for t in TableName::ALL.into_iter().chain([TableName::LineOrderFlat]) {
        for f in t.schema().fields() {
            let dict = !distinct.contains(&f.name().as_str()) && f.data_type() != &DataType::Int64;
            p = p.set_column_dictionary_enabled(ColumnPath::from(f.name().as_str()), dict);
        }
    }
    p
}

fn prepare_all(tables: &[SsbTable], threads: usize) {
    for t in tables {
        t.prepare(threads);
    }
}

/// [`SsbTable::write_parquet`] for several tables, `tables[i]` to
/// `writers[i]`, on one shared pool of `threads` threads (0 = all cores).
/// Put the biggest tables first.
pub fn write_parquet_many<W: Write + Send>(
    tables: &[SsbTable],
    writers: Vec<W>,
    threads: usize,
    props: Option<WriterProperties>,
) -> Result<Vec<W>, ParquetError> {
    assert_eq!(tables.len(), writers.len(), "one writer per table");
    let props = props.unwrap_or_else(|| parquet_props().build());
    prepare_all(tables, threads);
    let srcs: Vec<&dyn MultiChunks> = tables.iter().map(|t| t as &dyn MultiChunks).collect();
    populate_table::write_parquet_many(&srcs, writers, threads, Some(props))
}

/// [`SsbTable::write_tbl`] for several tables on one shared pool, like
/// [`write_parquet_many`]. The workers write the files themselves, so
/// different files are written in parallel.
pub fn write_tbl_many<W: Write + Send>(
    tables: &[SsbTable],
    writers: Vec<W>,
    threads: usize,
) -> io::Result<Vec<W>> {
    assert_eq!(tables.len(), writers.len(), "one writer per table");
    prepare_all(tables, threads);
    let srcs: Vec<&dyn TextChunks> = tables.iter().map(|t| t as &dyn TextChunks).collect();
    write_text_many(&srcs, writers, threads)
}

/// Generate several tables on one shared pool and call `f(i, batch)` with
/// each batch of `tables[i]`, in order per table, on the calling thread.
pub fn for_each_batch_many(tables: &[SsbTable], threads: usize, f: impl FnMut(usize, RecordBatch)) {
    prepare_all(tables, threads);
    let srcs: Vec<&dyn MultiChunks> = tables.iter().map(|t| t as &dyn MultiChunks).collect();
    chunks::for_each_batch_many(&srcs, threads, f)
}
