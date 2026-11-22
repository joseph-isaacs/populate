//! [`DsTable`]: one TPC-DS table, generated in parallel chunks of source
//! rows by tpcdsgen's row generators.

use crate::TableName;
use crate::dat::{self, Encoding};
use crate::fused::{self, Want};
use arrow_array::RecordBatch;
use arrow_schema::DataType;
use arrow_schema::SchemaRef;
use arrow_select::concat::concat_batches;
use parquet::basic::Compression;
use parquet::errors::ParquetError;
use parquet::file::properties::{WriterProperties, WriterPropertiesBuilder};
use parquet::schema::types::ColumnPath;
use populate_table::{Chunks, Jobs, MultiChunks, chunks, par_ordered};
use std::convert::Infallible;
use std::fmt::Write as _;
use std::io::{self, Write};
use std::sync::Mutex;
use std::thread;
use tpcdsgen::config::{Session, Table as Ds};
use tpcdsgen::row::*;
use tpcdsgen_arrow::*;

/// Default output rows per chunk (one Parquet row group).
pub const DEFAULT_CHUNK_ROWS: u64 = 256 * 1024;
/// Default Arrow batch size: tpcdsgen's own conversion size, so batches are
/// passed on as made. Larger sizes are built by concatenating them.
pub const DEFAULT_BATCH_SIZE: usize = GEN_BATCH;
/// Rows tpcdsgen-arrow converts at a time. Its row-to-column conversion
/// makes one pass over the buffered rows per column, so the rows must stay
/// in cache: at 8Ki rows `catalog_sales` (~400 B/row) converts 5x slower.
const GEN_BATCH: usize = 2048;

/// One table of a [`Tpcds`](crate::Tpcds) data set.
///
/// The table is split into chunks of consecutive source rows. Each chunk is
/// generated from scratch by a fresh tpcdsgen generator that skips straight
/// to its first row, so chunks are independent and the concatenation of all
/// chunks is exactly the sequential (dsdgen) output.
#[derive(Debug, Clone)]
pub struct DsTable {
    name: TableName,
    session: Session,
    source_rows: u64,
    chunk_source_rows: u64,
    batch_size: usize,
}

macro_rules! tables {
    ($($t:ident => $arrow:ident, $rows:ident, $per_source:expr;)*) => {
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

        impl DsTable {
            /// Arrow schema (tpcdsgen-arrow's: `Int32` keys, `Int64`
            /// identifiers, `Decimal128(p, s)`, `Date32`, `Utf8View` text).
            pub fn schema(&self) -> SchemaRef {
                schema_of(self.name)
            }
        }

        fn schema_of(t: TableName) -> SchemaRef {
            match t { $(TableName::$t => $arrow::schema_ref(),)* }
        }

        impl DsTable {

            /// Source rows `first..=last` (1-based) as Arrow batches of up
            /// to `batch` rows.
            fn arrow_range(
                &self,
                first: u64,
                last: u64,
                batch: usize,
                emit: &mut dyn FnMut(RecordBatch),
            ) {
                let want = match self.name {
                    TableName::StoreSales => Some(Want { sales: true, returns: false }),
                    TableName::StoreReturns => Some(Want { sales: false, returns: true }),
                    _ => None,
                };
                if let Some(want) = want {
                    return fused::store_channel(&self.session, first, last, batch, want, &mut |_, b| emit(b));
                }
                match self.name {
                    $(TableName::$t => {
                        $arrow::new(self.session.clone())
                            .with_source_row_range(first, last)
                            .with_batch_size(batch)
                            .for_each(|b| emit(b.expect("tpcdsgen-arrow batch")));
                    })*
                }
            }

            /// The table as tpcdsgen-arrow generates it, sequentially: the
            /// reference the parallel and fused paths are tested against.
            #[doc(hidden)]
            pub fn reference_arrow(&self) -> Vec<RecordBatch> {
                match self.name {
                    $(TableName::$t => $arrow::new(self.session.clone())
                        .with_source_row_range(1, self.source_rows)
                        .map(|b| b.expect("tpcdsgen-arrow batch"))
                        .collect(),)*
                }
            }

            /// The `.dat` file as tpcdsgen formats it (its rows' `Display`),
            /// sequentially: the reference [`write_dat`](Self::write_dat) is
            /// tested against. Slow.
            #[doc(hidden)]
            pub fn reference_dat(&self, encoding: Encoding) -> Vec<u8> {
                let mut out = String::new();
                match self.name {
                    $(TableName::$t => {
                        let mut g = $rows::new(self.session.clone(), self.source_rows);
                        g.set_source_row_range(1, self.source_rows);
                        for row in g {
                            writeln!(out, "{row}").expect("write to String");
                        }
                    })*
                }
                match encoding {
                    Encoding::Utf8 => out.into_bytes(),
                    Encoding::Latin1 => out
                        .chars()
                        .map(|c| u8::try_from(c).expect("Latin-1 text"))
                        .collect(),
                }
            }
        }
    };
}

tables! {
    CallCenter => CallCenterArrow, CallCenterRowGenerator, 1.0;
    CatalogPage => CatalogPageArrow, CatalogPageRowGenerator, 1.0;
    CatalogReturns => CatalogReturnsArrow, CatalogReturnsRowGenerator, 0.9;
    CatalogSales => CatalogSalesArrow, CatalogSalesRowGenerator, 9.0;
    Customer => CustomerArrow, CustomerRowGenerator, 1.0;
    CustomerAddress => CustomerAddressArrow, CustomerAddressRowGenerator, 1.0;
    CustomerDemographics => CustomerDemographicsArrow, CustomerDemographicsRowGenerator, 1.0;
    DateDim => DateDimArrow, DateDimRowGenerator, 1.0;
    HouseholdDemographics => HouseholdDemographicsArrow, HouseholdDemographicsRowGenerator, 1.0;
    IncomeBand => IncomeBandArrow, IncomeBandRowGenerator, 1.0;
    Inventory => InventoryArrow, InventoryRowGenerator, 1.0;
    Item => ItemArrow, ItemRowGenerator, 1.0;
    Promotion => PromotionArrow, PromotionRowGenerator, 1.0;
    Reason => ReasonArrow, ReasonRowGenerator, 1.0;
    ShipMode => ShipModeArrow, ShipModeRowGenerator, 1.0;
    Store => StoreArrow, StoreRowGenerator, 1.0;
    StoreReturns => StoreReturnsArrow, StoreReturnsRowGenerator, 1.2;
    StoreSales => StoreSalesArrow, StoreSalesRowGenerator, 12.0;
    TimeDim => TimeDimArrow, TimeDimRowGenerator, 1.0;
    Warehouse => WarehouseArrow, WarehouseRowGenerator, 1.0;
    WebPage => WebPageArrow, WebPageRowGenerator, 1.0;
    WebReturns => WebReturnsArrow, WebReturnsRowGenerator, 1.2;
    WebSales => WebSalesArrow, WebSalesRowGenerator, 12.0;
    WebSite => WebSiteArrow, WebSiteRowGenerator, 1.0;
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
    pub(crate) fn new(name: TableName, session: Session) -> Self {
        let source_rows = session.get_scaling().get_row_count(name.ds());
        DsTable {
            name,
            session,
            source_rows,
            chunk_source_rows: 1,
            batch_size: DEFAULT_BATCH_SIZE,
        }
        .chunk_rows(DEFAULT_CHUNK_ROWS)
    }

    pub fn name(&self) -> TableName {
        self.name
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

    /// 1-based inclusive source-row range of chunk `i`.
    fn range(&self, i: u64) -> (u64, u64) {
        let first = i * self.chunk_source_rows + 1;
        let last = first.saturating_add(self.chunk_source_rows - 1);
        (first, last.min(self.source_rows))
    }

    /// Generate on `threads` threads (0 = all cores) and call `f` with each
    /// batch, in table order, on the calling thread.
    pub fn for_each_batch(&self, threads: usize, f: impl FnMut(RecordBatch)) {
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

    /// Write the table in dsdgen's `.dat` format (`|`-separated fields, a
    /// trailing `|`, one row per line), byte-identical to `dsdgen -TABLE`.
    /// Chunks are formatted on `threads` threads and written in order.
    pub fn write_dat<W: Write>(
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
    for t in TableName::ALL {
        for f in schema_of(t).fields() {
            let dict = !matches!(f.data_type(), DataType::Decimal128(..) | DataType::Int64);
            p = p.set_column_dictionary_enabled(ColumnPath::from(f.name().as_str()), dict);
        }
    }
    p
}

/// [`DsTable::write_parquet`] for several tables, `tables[i]` to
/// `writers[i]`, on one shared pool of `threads` threads (0 = all cores):
/// every chunk of every table is one job, so small tables never leave cores
/// idle. Put the biggest tables first. `store_sales` and `store_returns`
/// with the same chunking are generated together, in one pass.
pub fn write_parquet_many<W: Write + Send>(
    tables: &[DsTable],
    writers: Vec<W>,
    threads: usize,
    props: Option<WriterProperties>,
) -> Result<Vec<W>, ParquetError> {
    assert_eq!(tables.len(), writers.len(), "one writer per table");
    let props = props.unwrap_or_else(|| parquet_props().build());
    let units = plan(tables, true);
    let srcs: Vec<&dyn MultiChunks> = units.iter().map(|u| u as &dyn MultiChunks).collect();
    // writers in output order (units' tables, in order), and back
    let mut by_table: Vec<Option<W>> = writers.into_iter().map(Some).collect();
    let order: Vec<usize> = units.iter().flat_map(|u| u.idx.iter().copied()).collect();
    let ordered = order
        .iter()
        .map(|&t| by_table[t].take().expect("one writer per table"));
    let done = populate_table::write_parquet_many(&srcs, ordered.collect(), threads, Some(props))?;
    for (t, w) in order.into_iter().zip(done) {
        by_table[t] = Some(w);
    }
    Ok(by_table.into_iter().map(|w| w.expect("written")).collect())
}

/// [`DsTable::write_dat`] for several tables on one shared pool, like
/// [`write_parquet_many`]. `threads: 0` means one worker per core but one:
/// the calling thread writes ~1 GB of text per scale factor, and with a
/// worker on every core it falls behind.
pub fn write_dat_many<W: Write>(
    tables: &[DsTable],
    mut writers: Vec<W>,
    threads: usize,
    encoding: Encoding,
) -> io::Result<Vec<W>> {
    assert_eq!(tables.len(), writers.len(), "one writer per table");
    let threads = match threads {
        0 => thread::available_parallelism().map_or(1, |t| t.get().saturating_sub(1).max(1)),
        t => t,
    };
    let units = plan(tables, true);
    let jobs = Jobs::new(units.iter().map(MultiChunks::num_chunks));
    // Text buffers go back to a pool once written: a fresh multi-MB buffer
    // per chunk costs more in page faults than formatting it.
    let pool = Mutex::new(Vec::<Vec<u8>>::new());
    par_ordered(
        jobs.total(),
        threads,
        |j| {
            let (u, i) = jobs.locate(j);
            let unit = &units[u];
            let mut outs: Vec<(usize, Vec<u8>)> = unit
                .idx
                .iter()
                .map(|&t| (t, pool.lock().expect("pool").pop().unwrap_or_default()))
                .collect();
            unit.chunk(i, &mut |k, b| {
                dat::write_batch(&b, encoding, &mut outs[k].1)
            });
            outs
        },
        |outs| {
            for (t, mut s) in outs {
                writers[t].write_all(&s)?;
                s.clear();
                pool.lock().expect("pool").push(s);
            }
            Ok::<(), io::Error>(())
        },
    )?;
    for w in &mut writers {
        w.flush()?;
    }
    Ok(writers)
}

/// Generate several tables on one shared pool and call `f(i, batch)` with
/// each batch of `tables[i]`, in order per table, on the calling thread.
pub fn for_each_batch_many(
    tables: &[DsTable],
    threads: usize,
    mut f: impl FnMut(usize, RecordBatch),
) {
    let units = plan(tables, false);
    let jobs = Jobs::new(units.iter().map(MultiChunks::num_chunks));
    let r: Result<(), Infallible> = par_ordered(
        jobs.total(),
        threads,
        |j| {
            let (u, i) = jobs.locate(j);
            let mut v = Vec::new();
            units[u].chunk(i, &mut |k, b| v.push((units[u].idx[k], b)));
            v
        },
        |v| {
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
    /// Indexes into `tables` of this unit's outputs.
    idx: Vec<usize>,
    /// Pass generation batches on as made (Parquet, `.dat`) instead of
    /// building `batch_size`-row batches.
    raw: bool,
}

/// Units for `tables`, in order; `store_sales` and `store_returns` with the
/// same chunks and batch size become one unit at the first one's place.
fn plan(tables: &[DsTable], raw: bool) -> Vec<Unit<'_>> {
    let find = |n| tables.iter().position(|t| t.name == n);
    let pair = match (find(TableName::StoreSales), find(TableName::StoreReturns)) {
        (Some(s), Some(r))
            if tables[s].chunk_source_rows == tables[r].chunk_source_rows
                && tables[s].source_rows == tables[r].source_rows
                && tables[s].batch_size == tables[r].batch_size =>
        {
            Some((s, r))
        }
        _ => None,
    };
    let mut units = Vec::new();
    for i in 0..tables.len() {
        let idx = match pair {
            Some((s, r)) if i == s.min(r) => vec![s, r],
            Some((s, r)) if i == s.max(r) => continue,
            _ => vec![i],
        };
        units.push(Unit { tables, idx, raw });
    }
    units
}

impl MultiChunks for Unit<'_> {
    fn schemas(&self) -> Vec<SchemaRef> {
        self.idx.iter().map(|&t| self.tables[t].schema()).collect()
    }

    fn num_chunks(&self) -> u64 {
        self.tables[self.idx[0]].num_chunks()
    }

    fn chunk(&self, i: u64, emit: &mut dyn FnMut(usize, RecordBatch)) {
        let t = &self.tables[self.idx[0]];
        let (first, last) = t.range(i);
        if self.idx.len() == 2 {
            // outputs in `idx` order; the channel emits sales as 0
            let sales_first = t.name == TableName::StoreSales;
            let want = Want {
                sales: true,
                returns: true,
            };
            return fused::store_channel(
                &t.session,
                first,
                last,
                t.batch_size,
                want,
                &mut |k, b| emit(if sales_first { k } else { 1 - k }, b),
            );
        }
        if self.raw {
            t.arrow_range(first, last, GEN_BATCH, &mut |b| emit(0, b));
        } else {
            Chunks::chunk(t, i, &mut |b| emit(0, b));
        }
    }
}

impl Chunks for DsTable {
    fn schema(&self) -> SchemaRef {
        DsTable::schema(self)
    }

    fn num_chunks(&self) -> u64 {
        DsTable::num_chunks(self)
    }

    /// Batches of `batch_size` rows (the last of each chunk may be shorter),
    /// built from cache-sized generation batches.
    fn chunk(&self, i: u64, emit: &mut dyn FnMut(RecordBatch)) {
        let (first, last) = self.range(i);
        let direct = matches!(self.name, TableName::StoreSales | TableName::StoreReturns);
        if direct || self.batch_size <= GEN_BATCH {
            return self.arrow_range(first, last, self.batch_size, emit);
        }
        let schema = self.schema();
        let (mut pending, mut rows) = (Vec::new(), 0);
        let mut flush = |pending: &mut Vec<RecordBatch>| {
            if pending.len() == 1 {
                emit(pending.pop().expect("one batch"));
            } else if !pending.is_empty() {
                emit(concat_batches(&schema, pending.iter()).expect("same schema"));
                pending.clear();
            }
        };
        self.arrow_range(first, last, GEN_BATCH, &mut |b| {
            rows += b.num_rows();
            pending.push(b);
            if rows >= self.batch_size {
                flush(&mut pending);
                rows = 0;
            }
        });
        flush(&mut pending);
    }
}
