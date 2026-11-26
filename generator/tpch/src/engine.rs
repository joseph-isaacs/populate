//! [`TpchTable`]: one TPC-H table, generated in parallel chunks of source
//! rows, and the drivers that run several tables on one pool.

use crate::TableName;
use crate::tables::{self, SUPPLIERS_PER_PART, Scale};
use crate::text::Text;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, SchemaRef};
use parquet::basic::Compression;
use parquet::errors::ParquetError;
use parquet::file::properties::{WriterProperties, WriterPropertiesBuilder};
use parquet::schema::types::ColumnPath;
use populate_table::text::{Encoding, TextChunks, write_batch_with, write_text_many};
use populate_table::{Chunks, DynTable, MultiChunks, chunks};
use std::io::{self, Write};

/// Default output rows per chunk (one Parquet row group).
pub const DEFAULT_CHUNK_ROWS: u64 = 256 * 1024;
/// Default rows per Arrow batch (about: `lineitem` batches end on an
/// order).
pub const DEFAULT_BATCH_SIZE: usize = 8 * 1024;

/// One table of a [`Tpch`](crate::Tpch) data set.
///
/// The table is split into chunks of consecutive source rows. Each chunk is
/// generated from scratch with every stream positioned at its first row,
/// so chunks are independent and their concatenation is exactly the
/// sequential (dbgen) output.
#[derive(Debug, Clone)]
pub struct TpchTable {
    name: TableName,
    /// The table's distribution, over its source rows.
    table: DynTable,
}

impl TableName {
    /// Average output rows per source row: 4 line items per order, 4
    /// suppliers per part, 1 elsewhere.
    pub fn rows_per_source_row(self) -> f64 {
        match self {
            TableName::LineItem => 4.0,
            TableName::PartSupp => SUPPLIERS_PER_PART as f64,
            _ => 1.0,
        }
    }

    /// Arrow schema (tpchgen-arrow's: `Int64` keys, `Int32` counts,
    /// `Decimal128(15, 2)`, `Date32`, `Utf8View` text), read off the
    /// table's distribution: nothing is generated.
    pub fn schema(self) -> SchemaRef {
        let i = TableName::ALL
            .iter()
            .position(|&t| t == self)
            .expect("every table");
        tables::SCHEMAS[i].clone()
    }
}

impl TpchTable {
    pub(crate) fn new(name: TableName, scale: Scale, source_rows: u64) -> Self {
        TpchTable {
            name,
            table: tables::generator(name, &scale, source_rows),
        }
        .chunk_rows(DEFAULT_CHUNK_ROWS)
        .batch_size(DEFAULT_BATCH_SIZE)
    }

    pub fn name(&self) -> TableName {
        self.name
    }

    pub fn schema(&self) -> SchemaRef {
        self.name.schema()
    }

    /// Source rows (orders, parts, ...; the row count for most tables).
    pub fn source_rows(&self) -> u64 {
        self.table.rows()
    }

    /// Expected output rows (exact for all tables but `lineitem`).
    pub fn approx_rows(&self) -> u64 {
        (self.source_rows() as f64 * self.name.rows_per_source_row()).round() as u64
    }

    /// Only the first `n` source rows: a prefix of the full table (the same
    /// rows, not a smaller scale factor).
    pub fn limit_source_rows(mut self, n: u64) -> Self {
        self.table = self.table.limit_rows(n);
        self
    }

    /// Aim for about `rows` output rows per chunk (= per Parquet row
    /// group). Smaller chunks spread a table over more cores; the output
    /// values are the same for any chunk size.
    pub fn chunk_rows(self, rows: u64) -> Self {
        let per = (rows as f64 / self.name.rows_per_source_row()).round() as u64;
        self.chunk_source_rows(per.max(1))
    }

    /// Chunks of exactly `n` source rows (the last may be shorter).
    pub fn chunk_source_rows(mut self, n: u64) -> Self {
        assert!(n > 0, "chunk_source_rows must be > 0");
        self.table = self.table.chunk_rows(n);
        self
    }

    /// About `n` rows per Arrow batch (default 8Ki; `lineitem` batches end
    /// on an order).
    pub fn batch_size(mut self, n: usize) -> Self {
        assert!(n > 0, "batch_size must be > 0");
        let per = (n as f64 / self.name.rows_per_source_row()).ceil() as u64;
        self.table = self.table.batch_rows(per.max(1));
        self
    }

    /// Number of chunks.
    pub fn num_chunks(&self) -> u64 {
        Chunks::num_chunks(&self.table)
    }

    /// Generate on `threads` threads (0 = all cores) and call `f` with each
    /// batch, in table order, on the calling thread. Parallel generation
    /// may subdivide chunks to balance work and limit buffered Arrow data.
    pub fn for_each_batch(&self, threads: usize, f: impl FnMut(RecordBatch)) {
        Text::init(threads);
        chunks::for_each_batch(&self.table.clone().parallel_chunks(threads), threads, f)
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

    /// Write the table in dbgen's `.tbl` format (`|`-separated fields, a
    /// trailing `|`, one row per line), byte-identical to `dbgen` and
    /// `tpchgen-cli`. Chunks are formatted on `threads` threads and written
    /// in order.
    pub fn write_tbl<W: Write + Send>(&self, writer: W, threads: usize) -> io::Result<W> {
        let mut w = write_tbl_many(std::slice::from_ref(self), vec![writer], threads)?;
        Ok(w.pop().expect("one writer"))
    }

    /// Chunk `i` as `.tbl` text, appended to `out`.
    fn chunk_tbl(&self, i: u64, out: &mut Vec<u8>) {
        Chunks::chunk(&self.table, i, &mut |b| tbl_batch(&b, out));
    }

    /// Source rows `first..end` (0-based) as `.tbl` text, generated on the
    /// calling thread: one chunk, wherever it starts.
    #[doc(hidden)]
    pub fn tbl_range(&self, first: u64, end: u64) -> Vec<u8> {
        let mut out = Vec::new();
        self.table
            .range(first, end, &mut |b| tbl_batch(&b, &mut out));
        out
    }
}

/// Append `batch` as `.tbl` lines: dbgen prints quantities, kept as
/// `Decimal128(15, 2)`, as integers.
fn tbl_batch(batch: &RecordBatch, out: &mut Vec<u8>) {
    write_batch_with(
        batch,
        Encoding::Utf8,
        |f| (f.name() == "l_quantity").then_some(0),
        out,
    )
}

impl Chunks for TpchTable {
    fn schema(&self) -> SchemaRef {
        TpchTable::schema(self)
    }

    fn num_chunks(&self) -> u64 {
        TpchTable::num_chunks(self)
    }

    fn chunk(&self, i: u64, emit: &mut dyn FnMut(RecordBatch)) {
        Chunks::chunk(&self.table, i, emit)
    }
}

impl TextChunks for TpchTable {
    fn outputs(&self) -> usize {
        1
    }

    fn num_chunks(&self) -> u64 {
        TpchTable::num_chunks(self)
    }

    fn chunk_text(&self, i: u64, bufs: &mut [Vec<u8>]) {
        self.chunk_tbl(i, &mut bufs[0])
    }
}

/// Parquet writer properties for TPC-H: dictionary encoding for 32-bit
/// integers, dates, the text columns with few distinct values and the
/// decimals with few (`l_quantity`, `l_discount`, `l_tax`, `ps_supplycost`:
/// 25 MB smaller at SF 1 and faster to encode); plain encoding for the
/// other decimals, 64-bit keys, comments, names, addresses and phone
/// numbers, which have too many distinct values for a dictionary to pay
/// (the writer would build one, outgrow its size limit and fall back).
/// Snappy compression. Used when `write_parquet` gets no properties; start from it
/// to change other settings.
pub fn parquet_props() -> WriterPropertiesBuilder {
    let mut p = WriterProperties::builder().set_compression(Compression::SNAPPY);
    for t in TableName::ALL {
        for f in t.schema().fields() {
            let distinct = ["comment", "name", "address", "phone"]
                .iter()
                .any(|s| f.name().ends_with(s))
                && f.name() != "n_name"
                && f.name() != "r_name";
            // decimals with few distinct values (quantities, discounts,
            // taxes, supply costs) are smaller and faster with a dictionary
            let few = ["l_quantity", "l_discount", "l_tax", "ps_supplycost"];
            let dict = few.contains(&f.name().as_str())
                || !distinct
                    && !matches!(f.data_type(), DataType::Decimal128(..) | DataType::Int64);
            p = p.set_column_dictionary_enabled(ColumnPath::from(f.name().as_str()), dict);
        }
    }
    p
}

/// [`TpchTable::write_parquet`] for several tables, `tables[i]` to
/// `writers[i]`, on one shared pool of `threads` threads (0 = all cores):
/// every chunk of every table is one job, so small tables never leave cores
/// idle. Put the biggest tables first.
pub fn write_parquet_many<W: Write + Send>(
    tables: &[TpchTable],
    writers: Vec<W>,
    threads: usize,
    props: Option<WriterProperties>,
) -> Result<Vec<W>, ParquetError> {
    assert_eq!(tables.len(), writers.len(), "one writer per table");
    let props = props.unwrap_or_else(|| parquet_props().build());
    Text::init(threads);
    let srcs: Vec<&dyn MultiChunks> = tables.iter().map(|t| t as &dyn MultiChunks).collect();
    populate_table::write_parquet_many(&srcs, writers, threads, Some(props))
}

/// [`TpchTable::write_tbl`] for several tables on one shared pool, like
/// [`write_parquet_many`] (`threads: 0` = all cores). The workers write the
/// files themselves, so different files are written in parallel.
pub fn write_tbl_many<W: Write + Send>(
    tables: &[TpchTable],
    writers: Vec<W>,
    threads: usize,
) -> io::Result<Vec<W>> {
    assert_eq!(tables.len(), writers.len(), "one writer per table");
    Text::init(threads);
    let srcs: Vec<&dyn TextChunks> = tables.iter().map(|t| t as &dyn TextChunks).collect();
    write_text_many(&srcs, writers, threads)
}

/// Generate several tables on one shared pool and call `f(i, batch)` with
/// each batch of `tables[i]`, in order per table, on the calling thread.
/// Parallel generation may subdivide chunks, as in [`TpchTable::for_each_batch`].
pub fn for_each_batch_many(
    tables: &[TpchTable],
    threads: usize,
    f: impl FnMut(usize, RecordBatch),
) {
    Text::init(threads);
    let tables: Vec<_> = tables
        .iter()
        .map(|t| t.table.clone().parallel_chunks(threads))
        .collect();
    let srcs: Vec<&dyn MultiChunks> = tables.iter().map(|t| t as &dyn MultiChunks).collect();
    chunks::for_each_batch_many(&srcs, threads, f)
}
