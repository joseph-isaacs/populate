//! The TPC-C initial database population (TPC-C 5.11, clause 4.3.3.1): all
//! nine tables, for any number of warehouses.
//!
//! Every random value is a pure function of `(seed, column, row)`
//! ([`Draws`]), so any range of rows is generated on its own and chunked,
//! multi-threaded output equals one sequential run. The two values that
//! tables must agree on are shared the same way: `o_ol_cnt` (the number of
//! `order_line` rows of an order) is the same stream in `orders` and
//! `order_line`, and `o_c_id` is a per-district permutation.
//!
//! The population rules, by clause:
//!
//! - a-strings are letters and digits, of uniform length in `[x .. y]`;
//!   n-strings digits; states two upper-case letters; zips 4 random digits
//!   then `11111` (4.3.2.2, 4.3.2.7);
//! - `i_data` / `s_data`: a-string `[26 .. 50]`, and in 10% of rows chosen
//!   at random `ORIGINAL` at a random position (4.3.3.1);
//! - `c_last`: syllables of the customer number for the first 1,000
//!   customers of each district, of `NURand(255, 0, 999)` for the rest
//!   (4.3.2.3), with one `C` for the whole load ([`Tpcc::c_load`]);
//! - `o_c_id`: a random permutation of `1 ..= 3,000` per district; orders
//!   `2,101 ..= 3,000` are undelivered: no carrier, no delivery date,
//!   random `ol_amount`, and a `new_order` row;
//! - "current date/time" is one fixed load time ([`Tpcc::load_time`]), so
//!   the data is reproducible.

use crate::raw;
use crate::rng::Draws;
use crate::tables::Gen;
#[cfg(feature = "formats")]
use crate::tables::{SCHEMAS, generator};
#[cfg(feature = "formats")]
use crate::text::Delimited;
#[cfg(feature = "formats")]
use arrow_array::RecordBatch;
#[cfg(feature = "formats")]
use arrow_schema::{DataType, SchemaRef};
#[cfg(feature = "formats")]
use parquet::basic::Compression;
#[cfg(feature = "formats")]
use parquet::errors::ParquetError;
#[cfg(feature = "formats")]
use parquet::file::properties::{WriterProperties, WriterPropertiesBuilder};
#[cfg(feature = "formats")]
use parquet::schema::types::ColumnPath;
#[cfg(feature = "formats")]
use populate_core::chunks::DEFAULT_BATCH_ROWS;
use populate_core::{DistChunks, Distribution, Stream};
#[cfg(feature = "formats")]
use populate_table::text::{self, TextChunks};
#[cfg(feature = "formats")]
use populate_table::{Chunks, DynTable, MultiChunks, chunks};
use std::fmt;
#[cfg(feature = "formats")]
use std::io::{self, Write};
use std::str::FromStr;

/// Rows of `item` (fixed).
pub const ITEMS: u64 = 100_000;
/// `stock` rows per warehouse (one per item).
pub const STOCK_PER_WAREHOUSE: u64 = ITEMS;
/// Districts per warehouse.
pub const DISTRICTS: u64 = 10;
/// Customers (and `history` rows, and orders) per district.
pub const CUSTOMERS: u64 = 3_000;
/// Orders per district.
pub const ORDERS: u64 = 3_000;
/// The first undelivered order of each district; orders from here on have
/// a `new_order` row.
pub const FIRST_NEW_ORDER: u64 = 2_101;
/// `new_order` rows per district.
pub const NEW_ORDERS: u64 = ORDERS - FIRST_NEW_ORDER + 1;
/// Default load time: 2026-01-01 00:00:00 UTC, in microseconds.
pub const DEFAULT_LOAD_TIME: i64 = 1_767_225_600_000_000;

/// The syllables of `c_last` (4.3.2.3).
pub const SYLLABLES: [&str; 10] = [
    "BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING",
];

/// Every TPC-C table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum TpccTableName {
    Warehouse,
    District,
    Customer,
    History,
    NewOrder,
    Orders,
    OrderLine,
    Item,
    Stock,
}

impl TpccTableName {
    /// All tables, in the spec's order (clause 1.3).
    pub const ALL: [TpccTableName; 9] = {
        use TpccTableName::*;
        [
            Warehouse, District, Customer, History, NewOrder, Orders, OrderLine, Item, Stock,
        ]
    };

    /// SQL name, e.g. `order_line`.
    pub fn as_str(self) -> &'static str {
        use TpccTableName::*;
        match self {
            Warehouse => "warehouse",
            District => "district",
            Customer => "customer",
            History => "history",
            NewOrder => "new_order",
            Orders => "orders",
            OrderLine => "order_line",
            Item => "item",
            Stock => "stock",
        }
    }

    /// Arrow schema: `Int32` ids and counts, `Decimal128` money and rates
    /// with the spec's precision, `Timestamp(µs)` dates, `Utf8View` text.
    ///
    /// Read off the table's distribution, without generating a row.
    #[cfg(feature = "formats")]
    pub fn schema(self) -> SchemaRef {
        SCHEMAS[self as usize].clone()
    }

    /// Average output rows per source row: 10 lines per order for
    /// `order_line` (5 to 15), 1 elsewhere.
    pub fn rows_per_source_row(self) -> f64 {
        match self {
            TpccTableName::OrderLine => 10.0,
            _ => 1.0,
        }
    }
}

impl fmt::Display for TpccTableName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for TpccTableName {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        TpccTableName::ALL
            .into_iter()
            .find(|t| t.as_str() == s)
            .ok_or_else(|| format!("unknown TPC-C table `{s}`"))
    }
}

/// A TPC-C initial database for some number of warehouses.
///
/// ```
/// # #[cfg(feature = "formats")] {
/// use populate_gen_tpcc::{Tpcc, TpccTableName};
///
/// let c = Tpcc::new(1);
/// let customer = c.table(TpccTableName::Customer);
/// assert_eq!(customer.source_rows(), 30_000);
///
/// let mut lines = 0;
/// c.table(TpccTableName::OrderLine).for_each_batch(0, |b| lines += b.num_rows());
/// assert!((290_000..310_000).contains(&lines));
/// # }
/// ```
#[derive(Debug, Clone, Copy)]
pub struct Tpcc {
    warehouses: u32,
    seed: u64,
    load_time: i64,
}

impl Tpcc {
    /// `warehouses` warehouses (at least 1), seed 0, load time
    /// [`DEFAULT_LOAD_TIME`].
    pub fn new(warehouses: u32) -> Self {
        assert!(warehouses > 0, "at least one warehouse");
        Tpcc {
            warehouses,
            seed: 0,
            load_time: DEFAULT_LOAD_TIME,
        }
    }

    /// Seed for every random value. Same seed, same data, for any chunk
    /// size and thread count.
    pub fn seed(mut self, seed: u64) -> Self {
        self.seed = seed;
        self
    }

    /// The "current date/time" of the load (`c_since`, `h_date`,
    /// `o_entry_d`, `ol_delivery_d`), in microseconds since 1970 UTC.
    pub fn load_time(mut self, micros: i64) -> Self {
        self.load_time = micros;
        self
    }

    pub fn warehouses(&self) -> u32 {
        self.warehouses
    }

    pub(crate) fn seed_value(&self) -> u64 {
        self.seed
    }

    pub(crate) fn load_time_value(&self) -> i64 {
        self.load_time
    }

    /// `C` of `NURand(255, 0, 999)` for `c_last` in this load: in
    /// `0 ..= 255`, from the seed. A run must pick its own `C` with
    /// `|C_run - C_load|` in `65 ..= 119`, not 96 or 112 (2.1.6.1).
    pub fn c_load(&self) -> i64 {
        Draws::new(Stream::new(self.seed, "c_load"), 0).int(0, 255)
    }

    /// Source rows of `t`: orders for `order_line`, the row count elsewhere.
    pub fn source_rows(&self, t: TpccTableName) -> u64 {
        use TpccTableName::*;
        let w = self.warehouses as u64;
        match t {
            Warehouse => w,
            District => w * DISTRICTS,
            Customer | History | Orders | OrderLine => w * DISTRICTS * CUSTOMERS,
            NewOrder => w * DISTRICTS * NEW_ORDERS,
            Item => ITEMS,
            Stock => w * STOCK_PER_WAREHOUSE,
        }
    }

    /// Table `t`, ready to generate.
    #[cfg(feature = "formats")]
    pub fn table(&self, t: TpccTableName) -> TpccTable {
        TpccTable {
            name: t,
            table: generator(t, self),
        }
        .chunk_rows(DEFAULT_CHUNK_ROWS)
        .batch_size(DEFAULT_BATCH_ROWS)
    }

    /// Every table, in [`TpccTableName::ALL`] order.
    #[cfg(feature = "formats")]
    pub fn tables(&self) -> impl Iterator<Item = TpccTable> + '_ {
        TpccTableName::ALL.into_iter().map(|t| self.table(t))
    }
    /// Native `warehouse` columns in independently addressable source chunks.
    pub fn warehouse(
        &self,
    ) -> DistChunks<impl Distribution<Item = raw::Warehouse> + Clone + Send + Sync + use<>> {
        Gen::new(self)
            .warehouse()
            .chunks(self.source_rows(TpccTableName::Warehouse))
            .chunk_rows(DEFAULT_CHUNK_ROWS)
    }

    /// Native `district` columns in independently addressable source chunks.
    pub fn district(
        &self,
    ) -> DistChunks<impl Distribution<Item = raw::District> + Clone + Send + Sync + use<>> {
        Gen::new(self)
            .district()
            .chunks(self.source_rows(TpccTableName::District))
            .chunk_rows(DEFAULT_CHUNK_ROWS)
    }

    /// Native `customer` columns in independently addressable source chunks.
    pub fn customer(
        &self,
    ) -> DistChunks<impl Distribution<Item = raw::Customer> + Clone + Send + Sync + use<>> {
        Gen::new(self)
            .customer()
            .chunks(self.source_rows(TpccTableName::Customer))
            .chunk_rows(DEFAULT_CHUNK_ROWS)
    }

    /// Native `history` columns in independently addressable source chunks.
    pub fn history(
        &self,
    ) -> DistChunks<impl Distribution<Item = raw::History> + Clone + Send + Sync + use<>> {
        Gen::new(self)
            .history()
            .chunks(self.source_rows(TpccTableName::History))
            .chunk_rows(DEFAULT_CHUNK_ROWS)
    }

    /// Native `new_order` columns in independently addressable source chunks.
    pub fn new_order(
        &self,
    ) -> DistChunks<impl Distribution<Item = raw::NewOrder> + Clone + Send + Sync + use<>> {
        Gen::new(self)
            .new_order()
            .chunks(self.source_rows(TpccTableName::NewOrder))
            .chunk_rows(DEFAULT_CHUNK_ROWS)
    }

    /// Native `orders` columns in independently addressable source chunks.
    pub fn orders(
        &self,
    ) -> DistChunks<impl Distribution<Item = raw::Orders> + Clone + Send + Sync + use<>> {
        Gen::new(self)
            .orders()
            .chunks(self.source_rows(TpccTableName::Orders))
            .chunk_rows(DEFAULT_CHUNK_ROWS)
    }

    /// Native `order_line` columns in independently addressable source chunks.
    pub fn order_line(
        &self,
    ) -> DistChunks<impl Distribution<Item = raw::OrderLine> + Clone + Send + Sync + use<>> {
        Gen::new(self)
            .order_line()
            .chunks(self.source_rows(TpccTableName::OrderLine))
            .chunk_rows(DEFAULT_CHUNK_ROWS)
    }

    /// Native `item` columns in independently addressable source chunks.
    pub fn item(
        &self,
    ) -> DistChunks<impl Distribution<Item = raw::Item> + Clone + Send + Sync + use<>> {
        Gen::new(self)
            .item()
            .chunks(self.source_rows(TpccTableName::Item))
            .chunk_rows(DEFAULT_CHUNK_ROWS)
    }

    /// Native `stock` columns in independently addressable source chunks.
    pub fn stock(
        &self,
    ) -> DistChunks<impl Distribution<Item = raw::Stock> + Clone + Send + Sync + use<>> {
        Gen::new(self)
            .stock()
            .chunks(self.source_rows(TpccTableName::Stock))
            .chunk_rows(DEFAULT_CHUNK_ROWS)
    }
}

/// Default output rows per chunk (one Parquet row group).
pub const DEFAULT_CHUNK_ROWS: u64 = 128 * 1024;

/// One table of a [`Tpcc`] database, split into chunks of source rows.
#[derive(Debug, Clone)]
#[cfg(feature = "formats")]
pub struct TpccTable {
    name: TpccTableName,
    /// The table's distribution, over its source rows.
    table: DynTable,
}

#[cfg(feature = "formats")]
impl TpccTable {
    pub fn name(&self) -> TpccTableName {
        self.name
    }

    pub fn schema(&self) -> SchemaRef {
        self.name.schema()
    }

    /// Source rows (orders for `order_line`; the row count elsewhere).
    pub fn source_rows(&self) -> u64 {
        self.table.rows()
    }

    /// Expected output rows (exact for all tables but `order_line`).
    pub fn approx_rows(&self) -> u64 {
        (self.source_rows() as f64 * self.name.rows_per_source_row()).round() as u64
    }

    /// Only the first `n` source rows: a prefix of the full table.
    pub fn limit_source_rows(mut self, n: u64) -> Self {
        self.table = self.table.limit_rows(n);
        self
    }

    /// Aim for about `rows` output rows per chunk (= per Parquet row
    /// group). The values are the same for any chunk size.
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

    /// About `n` output rows per Arrow batch (default 8Ki).
    pub fn batch_size(mut self, n: usize) -> Self {
        assert!(n > 0, "batch_size must be > 0");
        let per = (n as f64 / self.name.rows_per_source_row()).ceil() as u64;
        self.table = self.table.batch_rows(per.max(1));
        self
    }

    pub fn num_chunks(&self) -> u64 {
        Chunks::num_chunks(&self.table)
    }

    /// Source rows `first..end` (0-based), generated on this thread.
    pub fn range(&self, first: u64, end: u64) -> Vec<RecordBatch> {
        let mut v = Vec::new();
        self.table.range(first, end, &mut |b| v.push(b));
        v
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

    /// Write the table as one Parquet file, one row group per chunk
    /// (`props: None` means [`parquet_props`]).
    pub fn write_parquet<W: Write + Send>(
        &self,
        writer: W,
        threads: usize,
        props: Option<WriterProperties>,
    ) -> Result<W, ParquetError> {
        let mut w = write_parquet_many(std::slice::from_ref(self), vec![writer], threads, props)?;
        Ok(w.pop().expect("one writer"))
    }

    /// Write the table as CSV: `,`-separated, `NULL` for NULL, decimals
    /// with their scale, times as `YYYY-MM-DD HH:MM:SS`, no header (the
    /// layout `go-tpc tpcc prepare --output-type csv` writes, ready for
    /// `LOAD DATA` / `COPY`).
    pub fn write_csv<W: Write + Send>(&self, writer: W, threads: usize) -> io::Result<W> {
        let mut w = write_text_many(std::slice::from_ref(self), vec![writer], threads)?;
        Ok(w.pop().expect("one writer"))
    }
}

/// Parquet writer properties for TPC-C: dictionary encoding for the ids,
/// counts, constants and the text columns with few distinct values (states,
/// `c_last`, `c_credit`, `c_middle`); plain encoding for random text, item
/// ids and random amounts, which have too many distinct values for a
/// dictionary to pay (the writer would build one and fall back). Snappy.
/// Used when `write_parquet` gets no properties.
#[cfg(feature = "formats")]
pub fn parquet_props() -> WriterPropertiesBuilder {
    const DICT_TEXT: [&str; 6] = [
        "c_middle", "c_credit", "c_last", "w_state", "d_state", "c_state",
    ];
    const PLAIN: [&str; 6] = [
        "i_id",
        "s_i_id",
        "ol_i_id",
        "i_im_id",
        "i_price",
        "ol_amount",
    ];
    let mut p = WriterProperties::builder().set_compression(Compression::SNAPPY);
    for t in TpccTableName::ALL {
        for f in t.schema().fields() {
            let name = f.name().as_str();
            let dict = match f.data_type() {
                DataType::Utf8View => DICT_TEXT.contains(&name),
                _ => !PLAIN.contains(&name),
            };
            p = p.set_column_dictionary_enabled(ColumnPath::from(name), dict);
        }
    }
    p
}

#[cfg(feature = "formats")]
const CSV: Delimited = Delimited {
    sep: b',',
    null: b"NULL",
};

#[cfg(feature = "formats")]
impl Chunks for TpccTable {
    fn schema(&self) -> SchemaRef {
        TpccTable::schema(self)
    }

    fn num_chunks(&self) -> u64 {
        TpccTable::num_chunks(self)
    }

    fn chunk(&self, i: u64, emit: &mut dyn FnMut(RecordBatch)) {
        Chunks::chunk(&self.table, i, emit)
    }
}

#[cfg(feature = "formats")]
impl TextChunks for TpccTable {
    fn outputs(&self) -> usize {
        1
    }

    fn num_chunks(&self) -> u64 {
        TpccTable::num_chunks(self)
    }

    fn chunk_text(&self, i: u64, bufs: &mut [Vec<u8>]) {
        Chunks::chunk(&self.table, i, &mut |b| CSV.write(&b, &mut bufs[0]));
    }
}

/// [`TpccTable::write_parquet`] for several tables, `tables[i]` to
/// `writers[i]`, on one shared pool of `threads` threads (0 = all cores):
/// every chunk of every table is one job and one row group. `props: None`
/// means [`parquet_props`].
#[cfg(feature = "formats")]
pub fn write_parquet_many<W: Write + Send>(
    tables: &[TpccTable],
    writers: Vec<W>,
    threads: usize,
    props: Option<WriterProperties>,
) -> Result<Vec<W>, ParquetError> {
    assert_eq!(tables.len(), writers.len(), "one writer per table");
    let props = props.unwrap_or_else(|| parquet_props().build());
    let srcs: Vec<&dyn MultiChunks> = tables.iter().map(|t| t as &dyn MultiChunks).collect();
    populate_table::write_parquet_many(&srcs, writers, threads, Some(props))
}

/// [`TpccTable::write_csv`] for several tables on one shared pool, like
/// [`write_parquet_many`]. The workers write the files themselves.
#[cfg(feature = "formats")]
pub fn write_text_many<W: Write + Send>(
    tables: &[TpccTable],
    writers: Vec<W>,
    threads: usize,
) -> io::Result<Vec<W>> {
    assert_eq!(tables.len(), writers.len(), "one writer per table");
    let srcs: Vec<&dyn TextChunks> = tables.iter().map(|t| t as &dyn TextChunks).collect();
    text::write_text_many(&srcs, writers, threads)
}

/// Generate several tables on one shared pool and call `f(i, batch)` with
/// each batch of `tables[i]`, in order per table, on the calling thread.
#[cfg(feature = "formats")]
pub fn for_each_batch_many(
    tables: &[TpccTable],
    threads: usize,
    f: impl FnMut(usize, RecordBatch),
) {
    let srcs: Vec<&dyn MultiChunks> = tables.iter().map(|t| t as &dyn MultiChunks).collect();
    chunks::for_each_batch_many(&srcs, threads, f)
}
