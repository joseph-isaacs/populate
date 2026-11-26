//! [`DsTable`]: one TPC-DS table, generated in parallel chunks of source
//! rows by its distribution.

use crate::dat::Encoding;
use crate::{TableName, Tpcds, channels, dims, inventory};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, SchemaRef};
use parquet::basic::Compression;
use parquet::errors::ParquetError;
use parquet::file::properties::{WriterProperties, WriterPropertiesBuilder};
use parquet::schema::types::ColumnPath;
use populate_core::{Columnar, Columns, Distribution, Tables};
use populate_sink_arrow::{IntoArrowColumns, IntoArrowTables};
use populate_table::text::{TextChunks, TextColumns, TextTables, write_text_many};
use populate_table::{
    Chunks, DEFAULT_CHUNK_ROWS, DistTable, DistTables, MultiChunks, chunks, par_sources,
    resolve_threads, write_parquet_many_in_batches,
};
use std::convert::Infallible;
use std::fmt;
use std::io::{self, Write};
use std::ops::Range;
use std::sync::Arc;
use tpcdsgen::config::{Session, Table as Ds};

/// Default Arrow batch size, in output rows: small enough that a batch's
/// columns stay in cache while they are made and written out.
pub const DEFAULT_BATCH_SIZE: usize = 2048;

/// One table of a [`Tpcds`](crate::Tpcds) data set.
///
/// The table is split into chunks of consecutive source rows. Each chunk
/// is made by the table's distribution, cloned and skipped straight to its
/// first row, so chunks are independent and the concatenation of all
/// chunks is exactly the sequential (dsdgen) output.
#[derive(Clone)]
pub struct DsTable {
    name: TableName,
    /// The distribution that makes the table, and which of its outputs
    /// the table is (a sales table and its returns are made together).
    source: Arc<dyn Source>,
    output: usize,
    source_rows: u64,
    chunk_source_rows: u64,
    batch_size: usize,
}

impl fmt::Debug for DsTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DsTable")
            .field("name", &self.name)
            .field("source_rows", &self.source_rows)
            .field("chunk_source_rows", &self.chunk_source_rows)
            .field("batch_size", &self.batch_size)
            .finish()
    }
}

/// What makes one or more tables: a [`DistTable`], or the [`DistTables`]
/// of a sales channel (its sales, then its returns).
trait Source: Send + Sync {
    fn schemas(&self) -> &[SchemaRef];

    /// Source rows `first..first + n` (0-based): a batch per output, to
    /// `emit(output, batch)`.
    fn batches(&self, first: u64, n: u64, emit: &mut dyn FnMut(usize, RecordBatch));

    /// Source rows `first..first + n` as `.dat` lines, output `k`'s
    /// appended to `bufs[k]`.
    fn text(&self, first: u64, n: u64, encoding: Encoding, bufs: &mut [Vec<u8>]);
}

impl<D> Source for DistTable<D>
where
    D: Distribution + Clone + Send + Sync,
    Columns<D::Item>: IntoArrowColumns + TextColumns,
{
    fn schemas(&self) -> &[SchemaRef] {
        std::slice::from_ref(self.schema())
    }

    fn batches(&self, first: u64, n: u64, emit: &mut dyn FnMut(usize, RecordBatch)) {
        emit(0, self.batch(first, n));
    }

    fn text(&self, first: u64, n: u64, encoding: Encoding, bufs: &mut [Vec<u8>]) {
        DistTable::text(self, first, n, encoding, &mut bufs[0]);
    }
}

impl<D, T> Source for DistTables<D>
where
    D: Distribution<Item = Tables<T>> + Clone + Send + Sync,
    T: Columnar<Columns: IntoArrowTables + TextTables>,
{
    fn schemas(&self) -> &[SchemaRef] {
        DistTables::schemas(self)
    }

    fn batches(&self, first: u64, n: u64, emit: &mut dyn FnMut(usize, RecordBatch)) {
        for (k, b) in DistTables::batches(self, first, n).into_iter().enumerate() {
            emit(k, b);
        }
    }

    fn text(&self, first: u64, n: u64, encoding: Encoding, bufs: &mut [Vec<u8>]) {
        DistTables::text(self, first, n, encoding, bufs);
    }
}

/// The source of table `t`, and which of its outputs `t` is.
fn source(t: TableName, s: &Session) -> (Arc<dyn Source>, usize) {
    use TableName::*;
    let rows = |t: TableName| s.get_scaling().get_row_count(t.ds());
    fn one<D>(t: TableName, rows: u64, dist: D) -> (Arc<dyn Source>, usize)
    where
        D: Distribution + Clone + Send + Sync + 'static,
        Columns<D::Item>: IntoArrowColumns + TextColumns,
    {
        (Arc::new(DistTable::new(t.as_str(), rows, dist)), 0)
    }
    fn channel<D, T>(names: [TableName; 2], rows: u64, dist: D) -> Arc<dyn Source>
    where
        D: Distribution<Item = Tables<T>> + Clone + Send + Sync + 'static,
        T: Columnar<Columns: IntoArrowTables + TextTables>,
    {
        Arc::new(DistTables::new(&names.map(TableName::as_str), rows, dist))
    }
    let n = rows(t);
    match t {
        StoreSales | StoreReturns => (
            channel([StoreSales, StoreReturns], n, channels::store(s)),
            (t == StoreReturns) as usize,
        ),
        CatalogSales | CatalogReturns => (
            channel([CatalogSales, CatalogReturns], n, channels::catalog(s)),
            (t == CatalogReturns) as usize,
        ),
        WebSales | WebReturns => (
            channel([WebSales, WebReturns], n, channels::web(s)),
            (t == WebReturns) as usize,
        ),
        Inventory => one(t, n, inventory::inventory(s)),
        CustomerDemographics => one(t, n, inventory::customer_demographics()),
        CallCenter => one(t, n, dims::call_center(s)),
        CatalogPage => one(t, n, dims::catalog_page(s)),
        Customer => one(t, n, dims::customer(s)),
        CustomerAddress => one(t, n, dims::customer_address(s)),
        DateDim => one(t, n, dims::date_dim(s)),
        HouseholdDemographics => one(t, n, dims::household_demographics(s)),
        IncomeBand => one(t, n, dims::income_band(s)),
        Item => one(t, n, dims::item(s)),
        Promotion => one(t, n, dims::promotion(s)),
        Reason => one(t, n, dims::reason(s)),
        ShipMode => one(t, n, dims::ship_mode(s)),
        Store => one(t, n, dims::store(s)),
        TimeDim => one(t, n, dims::time_dim(s)),
        Warehouse => one(t, n, dims::warehouse(s)),
        WebPage => one(t, n, dims::web_page(s)),
        WebSite => one(t, n, dims::web_site(s)),
    }
}

macro_rules! tables {
    ($($t:ident, $per_source:expr;)*) => {
        impl TableName {
            /// The tpcdsgen table.
            pub(crate) fn ds(self) -> Ds {
                match self { $(TableName::$t => Ds::$t,)* }
            }

            /// Average output rows per source row (line items per ticket or
            /// order; returned share of them; 1 otherwise).
            pub fn rows_per_source_row(self) -> f64 {
                match self { $(TableName::$t => $per_source,)* }
            }
        }
    };
}

tables! {
    CallCenter, 1.0;
    CatalogPage, 1.0;
    CatalogReturns, 0.9;
    CatalogSales, 9.0;
    Customer, 1.0;
    CustomerAddress, 1.0;
    CustomerDemographics, 1.0;
    DateDim, 1.0;
    HouseholdDemographics, 1.0;
    IncomeBand, 1.0;
    Inventory, 1.0;
    Item, 1.0;
    Promotion, 1.0;
    Reason, 1.0;
    ShipMode, 1.0;
    Store, 1.0;
    StoreReturns, 1.2;
    StoreSales, 12.0;
    TimeDim, 1.0;
    Warehouse, 1.0;
    WebPage, 1.0;
    WebReturns, 1.2;
    WebSales, 12.0;
    WebSite, 1.0;
}

/// Source rows whose generation costs about as much as producing a sales
/// line item: returns replay every line item of their sale and keep ~10%.
fn cost_per_source_row(t: TableName) -> f64 {
    match t {
        TableName::StoreReturns => TableName::StoreSales.rows_per_source_row(),
        TableName::CatalogReturns => TableName::CatalogSales.rows_per_source_row(),
        TableName::WebReturns => TableName::WebSales.rows_per_source_row(),
        t => t.rows_per_source_row(),
    }
}

impl DsTable {
    pub(crate) fn new(name: TableName, session: &Session) -> Self {
        let source_rows = session.get_scaling().get_row_count(name.ds());
        let (source, output) = source(name, session);
        DsTable {
            name,
            source,
            output,
            source_rows,
            chunk_source_rows: 1,
            batch_size: DEFAULT_BATCH_SIZE,
        }
        .chunk_rows(DEFAULT_CHUNK_ROWS)
    }

    pub fn name(&self) -> TableName {
        self.name
    }

    /// Arrow schema (tpcdsgen-arrow's: `Int32` keys, `Int64` identifiers,
    /// `Decimal128(p, s)`, `Date32`, `Utf8View` text), read off the table's
    /// distribution.
    pub fn schema(&self) -> SchemaRef {
        self.source.schemas()[self.output].clone()
    }

    /// Source rows (tickets, orders, ...; the row count for most tables).
    pub fn source_rows(&self) -> u64 {
        self.source_rows
    }

    /// Expected output rows (exact for tables with one row per source row).
    pub fn approx_rows(&self) -> u64 {
        (self.source_rows as f64 * self.name.rows_per_source_row()).round() as u64
    }

    /// Only the first `n` source rows: a prefix of the full table (the same
    /// rows, not a smaller scale factor).
    pub fn limit_source_rows(mut self, n: u64) -> Self {
        self.source_rows = self.source_rows.min(n);
        self
    }

    /// Aim for about `rows` rows of work per chunk (= per Parquet row
    /// group). Smaller chunks spread a table over more cores; the output
    /// values are the same for any chunk size.
    pub fn chunk_rows(mut self, rows: u64) -> Self {
        let per = (rows as f64 / cost_per_source_row(self.name)).round() as u64;
        self.chunk_source_rows = per.max(1);
        self
    }

    /// Chunks of exactly `n` source rows (the last may be shorter).
    pub fn chunk_source_rows(mut self, n: u64) -> Self {
        assert!(n > 0, "chunk_source_rows must be > 0");
        self.chunk_source_rows = n;
        self
    }

    /// About `n` rows per Arrow batch (default [`DEFAULT_BATCH_SIZE`]): a
    /// batch is made from whole source rows, so a sales table's batches
    /// are whole tickets or orders.
    pub fn batch_size(mut self, n: usize) -> Self {
        assert!(n > 0, "batch_size must be > 0");
        self.batch_size = n;
        self
    }

    /// Number of chunks.
    pub fn num_chunks(&self) -> u64 {
        self.source_rows.div_ceil(self.chunk_source_rows)
    }

    /// 0-based source rows of chunk `i`.
    fn range(&self, i: u64) -> Range<u64> {
        let first = i * self.chunk_source_rows;
        first
            ..first
                .saturating_add(self.chunk_source_rows)
                .min(self.source_rows)
    }

    /// Source rows per batch: `batch_size` output rows of the channel's
    /// sales for a channel table.
    fn batch_source_rows(&self) -> u64 {
        let per = match self.name {
            TableName::StoreReturns => TableName::StoreSales,
            TableName::CatalogReturns => TableName::CatalogSales,
            TableName::WebReturns => TableName::WebSales,
            t => t,
        }
        .rows_per_source_row();
        ((self.batch_size as f64 / per).round() as u64).max(1)
    }

    /// Source rows `range` in batches: `f(first, n)` for each.
    fn for_each_range(&self, range: Range<u64>, mut f: impl FnMut(u64, u64)) {
        let step = self.batch_source_rows();
        let mut at = range.start;
        while at < range.end {
            let n = step.min(range.end - at);
            f(at, n);
            at += n;
        }
    }

    /// Balance Arrow work without retaining a whole Parquet row group.
    /// Sales and returns use the same work estimate so they stay paired.
    fn arrow_chunks(&self, threads: usize) -> Self {
        let mut table = self.clone();
        let threads = resolve_threads(threads);
        if threads > 1 {
            // Keep already well-partitioned channels together to amortize scheduling.
            if channel_of(self.name).is_some()
                && self.num_chunks() >= (threads as u64).saturating_mul(4)
            {
                return table;
            }
            let batch = self.batch_source_rows();
            // A few cheap batches do not amortize starting extra workers.
            if self.source_rows <= batch.saturating_mul(4) {
                return table;
            }
            let target = self
                .source_rows
                .div_ceil((threads as u64).saturating_mul(4));
            let size = target.div_ceil(batch).clamp(1, 32).saturating_mul(batch);
            table.chunk_source_rows = table.chunk_source_rows.min(size);
        }
        table
    }

    /// Generate on `threads` threads (0 = all cores) and call `f` with each
    /// batch, in table order, on the calling thread. Parallel generation
    /// may subdivide chunks to balance work and limit buffered Arrow data.
    pub fn for_each_batch(&self, threads: usize, f: impl FnMut(RecordBatch)) {
        chunks::for_each_batch(&self.arrow_chunks(threads), threads, f)
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

    /// Write the table in dsdgen's `.dat` format (`|`-separated fields, a
    /// trailing `|`, one row per line), byte-identical to `dsdgen -TABLE`.
    /// Chunks are formatted on `threads` threads and written in order.
    pub fn write_dat<W: Write + Send>(
        &self,
        writer: W,
        threads: usize,
        encoding: Encoding,
    ) -> io::Result<W> {
        let mut w = write_dat_many(std::slice::from_ref(self), vec![writer], threads, encoding)?;
        Ok(w.pop().expect("one writer"))
    }
}

/// Parquet writer properties tuned for TPC-DS: dictionary encoding for
/// text, keys, dates and quantities; plain encoding for decimals and 64-bit
/// identifiers. Those have too many distinct values for a dictionary, so
/// trying one costs ~40% of the encoding time and saves nothing (the files
/// come out slightly smaller). Snappy compression. Used when `write_parquet`
/// gets no properties; start from it to change other settings.
pub fn parquet_props() -> WriterPropertiesBuilder {
    let mut p = WriterProperties::builder().set_compression(Compression::SNAPPY);
    for t in Tpcds::new(1.0).tables() {
        for f in t.schema().fields() {
            let dict = !matches!(f.data_type(), DataType::Decimal128(..) | DataType::Int64);
            p = p.set_column_dictionary_enabled(ColumnPath::from(f.name().as_str()), dict);
        }
    }
    p
}

/// [`DsTable::write_parquet`] for several tables, `tables[i]` to
/// `writers[i]`, on one shared pool of `threads` threads (0 = all cores):
/// every chunk of every table is one job, so small tables never leave cores
/// idle. Put the biggest tables first. A sales table and its returns with
/// the same chunking are generated together, in one pass.
pub fn write_parquet_many<W: Write + Send>(
    tables: &[DsTable],
    writers: Vec<W>,
    threads: usize,
    props: Option<WriterProperties>,
) -> Result<Vec<W>, ParquetError> {
    assert_eq!(tables.len(), writers.len(), "one writer per table");
    let props = props.unwrap_or_else(|| parquet_props().build());
    let units = plan(tables);
    let srcs: Vec<&dyn MultiChunks> = units.iter().map(|u| u as &dyn MultiChunks).collect();
    // generated batches vary in length (a ticket has 8 to 16 lines), and
    // Parquet pages end at batch ends: written as batches of `batch_size`
    let batch: Vec<usize> = units
        .iter()
        .map(|u| u.tables[u.idx[0]].batch_size)
        .collect();
    // writers in output order (units' tables, in order), and back
    let mut by_table: Vec<Option<W>> = writers.into_iter().map(Some).collect();
    let order: Vec<usize> = units.iter().flat_map(|u| u.idx.iter().copied()).collect();
    let ordered = order
        .iter()
        .map(|&t| by_table[t].take().expect("one writer per table"));
    let done =
        write_parquet_many_in_batches(&srcs, ordered.collect(), threads, Some(props), &batch)?;
    for (t, w) in order.into_iter().zip(done) {
        by_table[t] = Some(w);
    }
    Ok(by_table.into_iter().map(|w| w.expect("written")).collect())
}

/// [`DsTable::write_dat`] for several tables on one shared pool, like
/// [`write_parquet_many`] (`threads: 0` = all cores). The workers write the
/// files themselves ([`populate_table::text::write_text_many`]), so
/// different files are written in parallel and no thread only writes.
pub fn write_dat_many<W: Write + Send>(
    tables: &[DsTable],
    writers: Vec<W>,
    threads: usize,
    encoding: Encoding,
) -> io::Result<Vec<W>> {
    assert_eq!(tables.len(), writers.len(), "one writer per table");
    let units = plan(tables);
    let srcs: Vec<DatUnit<'_>> = units.iter().map(|u| DatUnit(u, encoding)).collect();
    let dyns: Vec<&dyn TextChunks> = srcs.iter().map(|u| u as &dyn TextChunks).collect();
    // writers in output order (units' tables, in order), and back
    let mut by_table: Vec<Option<W>> = writers.into_iter().map(Some).collect();
    let order: Vec<usize> = units.iter().flat_map(|u| u.idx.iter().copied()).collect();
    let ordered = order
        .iter()
        .map(|&t| by_table[t].take().expect("one writer per table"));
    let done = write_text_many(&dyns, ordered.collect(), threads)?;
    for (t, w) in order.into_iter().zip(done) {
        by_table[t] = Some(w);
    }
    Ok(by_table.into_iter().map(|w| w.expect("written")).collect())
}

/// A [`Unit`] as `.dat` text.
struct DatUnit<'a>(&'a Unit<'a>, Encoding);

impl TextChunks for DatUnit<'_> {
    fn outputs(&self) -> usize {
        self.0.idx.len()
    }

    fn num_chunks(&self) -> u64 {
        MultiChunks::num_chunks(self.0)
    }

    /// The `.dat` lines of chunk `i`, written straight from the columns.
    fn chunk_text(&self, i: u64, bufs: &mut [Vec<u8>]) {
        let u = self.0;
        let t = &u.tables[u.idx[0]];
        let outputs = t.source.schemas().len();
        if bufs.len() == outputs {
            return t.for_each_range(t.range(i), |first, n| {
                t.source.text(first, n, self.1, bufs);
            });
        }
        // one table of a channel: its text is written in place, the
        // other's dropped
        let mut all = vec![Vec::new(); outputs];
        std::mem::swap(&mut all[t.output], &mut bufs[0]);
        t.for_each_range(t.range(i), |first, n| {
            t.source.text(first, n, self.1, &mut all);
            for (k, b) in all.iter_mut().enumerate() {
                if k != t.output {
                    b.clear();
                }
            }
        });
        std::mem::swap(&mut all[t.output], &mut bufs[0]);
    }
}

/// Generate several tables on one shared pool and call `f(i, batch)` with
/// each batch of `tables[i]`, in order per table, on the calling thread.
/// Parallel generation may subdivide chunks, as in [`DsTable::for_each_batch`].
pub fn for_each_batch_many(
    tables: &[DsTable],
    threads: usize,
    mut f: impl FnMut(usize, RecordBatch),
) {
    let tables: Vec<_> = tables.iter().map(|t| t.arrow_chunks(threads)).collect();
    let units = plan(&tables);
    let chunks: Vec<u64> = units.iter().map(MultiChunks::num_chunks).collect();
    let r: Result<(), Infallible> = par_sources(
        &chunks,
        threads,
        |u, i| {
            let mut v = Vec::new();
            units[u].chunk(i, &mut |k, b| v.push((units[u].idx[k], b)));
            v
        },
        |_, v| {
            v.into_iter().for_each(|(t, b)| f(t, b));
            Ok(())
        },
    );
    r.unwrap_or_else(|e| match e {});
}

/// One job source: a table, or a sales table and its returns generated
/// together.
struct Unit<'a> {
    tables: &'a [DsTable],
    /// Indexes into `tables` of this unit's outputs: one table, or a
    /// channel's sales and returns, in that order.
    idx: Vec<usize>,
}

/// Units for `tables`, in order; a sales table and its returns with the
/// same chunks and batch size become one unit (sales first) at the first
/// one's place.
fn plan(tables: &[DsTable]) -> Vec<Unit<'_>> {
    // partner[i]: the table generated together with tables[i]
    let mut partner = vec![None; tables.len()];
    for (i, a) in tables.iter().enumerate() {
        for (j, b) in tables.iter().enumerate() {
            let same_channel = channel_of(a.name).is_some_and(|c| Some(c) == channel_of(b.name));
            if same_channel
                && a.output != b.output
                && a.chunk_source_rows == b.chunk_source_rows
                && a.source_rows == b.source_rows
                && a.batch_size == b.batch_size
            {
                partner[i] = Some(j);
            }
        }
    }
    // pair only tables that chose each other (a table listed twice pairs once)
    let partner: Vec<Option<usize>> = (0..tables.len())
        .map(|i| partner[i].filter(|&j| partner[j] == Some(i)))
        .collect();
    let mut units = Vec::new();
    for (i, t) in tables.iter().enumerate() {
        let idx = match partner[i] {
            Some(j) if j < i => continue,
            Some(j) if t.output == 0 => vec![i, j],
            Some(j) => vec![j, i],
            None => vec![i],
        };
        units.push(Unit { tables, idx });
    }
    units
}

/// The sales channel a table belongs to, by its sales table.
fn channel_of(t: TableName) -> Option<TableName> {
    use TableName::*;
    match t {
        StoreSales | StoreReturns => Some(StoreSales),
        CatalogSales | CatalogReturns => Some(CatalogSales),
        WebSales | WebReturns => Some(WebSales),
        _ => None,
    }
}

impl MultiChunks for Unit<'_> {
    fn schemas(&self) -> Vec<SchemaRef> {
        self.idx.iter().map(|&t| self.tables[t].schema()).collect()
    }

    fn num_chunks(&self) -> u64 {
        self.tables[self.idx[0]].num_chunks()
    }

    /// Batches of about `batch_size` rows.
    fn chunk(&self, i: u64, emit: &mut dyn FnMut(usize, RecordBatch)) {
        let t = &self.tables[self.idx[0]];
        let paired = self.idx.len() == 2;
        let out = |k: usize| match (paired, k == t.output) {
            (true, _) => Some(k),
            (false, true) => Some(0),
            (false, false) => None,
        };
        t.for_each_range(t.range(i), |first, n| {
            t.source.batches(first, n, &mut |k, b| {
                if let Some(o) = out(k) {
                    emit(o, b)
                }
            });
        });
    }
}

impl Chunks for DsTable {
    fn schema(&self) -> SchemaRef {
        DsTable::schema(self)
    }

    fn num_chunks(&self) -> u64 {
        DsTable::num_chunks(self)
    }

    /// Batches of about `batch_size` rows.
    fn chunk(&self, i: u64, emit: &mut dyn FnMut(RecordBatch)) {
        self.for_each_range(self.range(i), |first, n| {
            self.source.batches(first, n, &mut |k, b| {
                if k == self.output {
                    emit(b)
                }
            });
        });
    }
}
