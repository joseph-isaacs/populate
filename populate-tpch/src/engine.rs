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
use populate_table::{Chunks, Jobs, MultiChunks, chunks, par_ordered};
use std::convert::Infallible;
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
    scale: Scale,
    source_rows: u64,
    chunk_source_rows: u64,
    batch_size: usize,
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
    /// `Decimal128(15, 2)`, `Date32`, `Utf8View` text).
    pub fn schema(self) -> SchemaRef {
        use TableName::*;
        match self {
            Nation => tables::NATION.clone(),
            Region => tables::REGION.clone(),
            Part => tables::PART.clone(),
            Supplier => tables::SUPPLIER.clone(),
            PartSupp => tables::PARTSUPP.clone(),
            Customer => tables::CUSTOMER.clone(),
            Orders => tables::ORDERS.clone(),
            LineItem => tables::LINEITEM.clone(),
        }
    }
}

impl TpchTable {
    pub(crate) fn new(name: TableName, scale: Scale, source_rows: u64) -> Self {
        TpchTable {
            name,
            scale,
            source_rows,
            chunk_source_rows: 1,
            batch_size: DEFAULT_BATCH_SIZE,
        }
        .chunk_rows(DEFAULT_CHUNK_ROWS)
    }

    pub fn name(&self) -> TableName {
        self.name
    }

    pub fn schema(&self) -> SchemaRef {
        self.name.schema()
    }

    /// Source rows (orders, parts, ...; the row count for most tables).
    pub fn source_rows(&self) -> u64 {
        self.source_rows
    }

    /// Expected output rows (exact for all tables but `lineitem`).
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
    /// group). Smaller chunks spread a table over more cores; the output
    /// values are the same for any chunk size.
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

    /// Source rows `first..end` as batches of about `batch_size` rows.
    fn generate(&self, first: u64, end: u64, emit: &mut dyn FnMut(RecordBatch)) {
        let t = Text::get();
        let s = &self.scale;
        let per = (self.batch_size as f64 / self.name.rows_per_source_row()).ceil() as u64;
        let mut at = first;
        while at < end {
            let n = per.max(1).min(end - at);
            emit(match self.name {
                TableName::Nation => tables::nation(t, at, n),
                TableName::Region => tables::region(t, at, n),
                TableName::Part => tables::part(t, at, n),
                TableName::Supplier => tables::supplier(t, at, n),
                TableName::PartSupp => tables::partsupp(t, s, at, n),
                TableName::Customer => tables::customer(t, at, n),
                TableName::Orders => tables::orders(t, s, at, n),
                TableName::LineItem => tables::lineitem(t, s, at, n),
            });
            at += n;
        }
    }

    /// Generate on `threads` threads (0 = all cores) and call `f` with each
    /// batch, in table order, on the calling thread.
    pub fn for_each_batch(&self, threads: usize, f: impl FnMut(RecordBatch)) {
        Text::init(threads);
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
        let (first, end) = self.range(i);
        self.generate(first, end, &mut |b| tbl_batch(&b, out));
    }

    /// Source rows `first..end` (0-based) as `.tbl` text, generated on the
    /// calling thread: one chunk, wherever it starts.
    #[doc(hidden)]
    pub fn tbl_range(&self, first: u64, end: u64) -> Vec<u8> {
        let mut out = Vec::new();
        self.generate(first, end.min(self.source_rows), &mut |b| {
            tbl_batch(&b, &mut out)
        });
        out
    }

    /// The `.tbl` file as tpchgen formats it (its rows' `Display`),
    /// sequentially. Slow.
    #[doc(hidden)]
    pub fn reference_tbl(&self) -> Vec<u8> {
        reference::tbl(self.name, self.scale.sf, self.source_rows)
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
        let (first, end) = self.range(i);
        self.generate(first, end, emit)
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

/// Parquet writer properties for TPC-H: dictionary encoding for text,
/// 32-bit integers and dates; plain encoding for decimals and 64-bit keys,
/// which have too many distinct values for a dictionary to pay. Snappy
/// compression. Used when `write_parquet` gets no properties; start from it
/// to change other settings.
pub fn parquet_props() -> WriterPropertiesBuilder {
    let mut p = WriterProperties::builder().set_compression(Compression::SNAPPY);
    for t in TableName::ALL {
        for f in t.schema().fields() {
            let dict = !matches!(f.data_type(), DataType::Decimal128(..) | DataType::Int64);
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
pub fn for_each_batch_many(
    tables: &[TpchTable],
    threads: usize,
    mut f: impl FnMut(usize, RecordBatch),
) {
    Text::init(threads);
    let jobs = Jobs::new(tables.iter().map(TpchTable::num_chunks));
    let r: Result<(), Infallible> = par_ordered(
        jobs.total(),
        threads,
        |j| {
            let (t, i) = jobs.locate(j);
            let mut v = Vec::new();
            Chunks::chunk(&tables[t], i, &mut |b| v.push(b));
            (t, v)
        },
        |(t, v)| {
            v.into_iter().for_each(|b| f(t, b));
            Ok(())
        },
    );
    r.unwrap_or_else(|e| match e {});
}

/// tpchgen's own generators, for the tests.
mod reference {
    use super::TableName;
    use std::fmt::Display;
    use std::io::Write;
    use tpchgen::generators::*;

    pub(super) fn tbl(t: TableName, sf: f64, source_rows: u64) -> Vec<u8> {
        let mut out = Vec::new();
        rows(t, sf, source_rows, &mut |r| {
            writeln!(out, "{r}").expect("in memory");
        });
        out
    }

    /// The first `source_rows` source rows of `t`, as tpchgen's rows.
    fn rows(t: TableName, sf: f64, source_rows: u64, f: &mut dyn FnMut(&dyn Display)) {
        let n = source_rows as usize;
        match t {
            TableName::Nation => NationGenerator::default()
                .iter()
                .take(n)
                .for_each(|r| f(&r)),
            TableName::Region => RegionGenerator::default()
                .iter()
                .take(n)
                .for_each(|r| f(&r)),
            TableName::Part => PartGenerator::new(sf, 1, 1)
                .iter()
                .take(n)
                .for_each(|r| f(&r)),
            TableName::Supplier => SupplierGenerator::new(sf, 1, 1)
                .iter()
                .take(n)
                .for_each(|r| f(&r)),
            TableName::PartSupp => PartSuppGenerator::new(sf, 1, 1)
                .iter()
                .take(n * 4)
                .for_each(|r| f(&r)),
            TableName::Customer => CustomerGenerator::new(sf, 1, 1)
                .iter()
                .take(n)
                .for_each(|r| f(&r)),
            TableName::Orders => OrderGenerator::new(sf, 1, 1)
                .iter()
                .take(n)
                .for_each(|r| f(&r)),
            TableName::LineItem => {
                let last = crate::tables::order_key(n as i64);
                LineItemGenerator::new(sf, 1, 1)
                    .iter()
                    .take_while(|r| r.l_orderkey <= last)
                    .for_each(|r| f(&r))
            }
        }
    }
}
