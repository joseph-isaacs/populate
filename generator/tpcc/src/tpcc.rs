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

use crate::engine::Plan;
use crate::rng::Draws;
use crate::tables::{SCHEMAS, TableGen, generator};
use crate::text::Delimited;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, SchemaRef};
use parquet::basic::Compression;
use parquet::file::properties::{WriterProperties, WriterPropertiesBuilder};
use parquet::schema::types::ColumnPath;
use populate_table::text::TextChunks;
use populate_table::{Chunks, Stream, chunks};
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

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
/// use populate_tpcc::{Tpcc, TpccTableName};
///
/// let c = Tpcc::new(1);
/// let customer = c.table(TpccTableName::Customer);
/// assert_eq!(customer.source_rows(), 30_000);
///
/// let mut lines = 0;
/// c.table(TpccTableName::OrderLine).for_each_batch(0, |b| lines += b.num_rows());
/// assert!((290_000..310_000).contains(&lines));
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
    pub fn table(&self, t: TpccTableName) -> TpccTable {
        TpccTable {
            name: t,
            plan: Plan::new(self.source_rows(t), t.rows_per_source_row()),
            generator: generator(t, self),
        }
    }

    /// Every table, in [`TpccTableName::ALL`] order.
    pub fn tables(&self) -> impl Iterator<Item = TpccTable> + '_ {
        TpccTableName::ALL.into_iter().map(|t| self.table(t))
    }
}

/// One table of a [`Tpcc`] database, split into chunks of source rows.
#[derive(Clone)]
pub struct TpccTable {
    name: TpccTableName,
    plan: Plan,
    /// The table's distribution, run over any range of source rows.
    generator: Arc<dyn TableGen>,
}

impl TpccTable {
    pub fn name(&self) -> TpccTableName {
        self.name
    }

    pub fn schema(&self) -> SchemaRef {
        self.name.schema()
    }

    /// Source rows (orders for `order_line`; the row count elsewhere).
    pub fn source_rows(&self) -> u64 {
        self.plan.source_rows
    }

    /// Expected output rows (exact for all tables but `order_line`).
    pub fn approx_rows(&self) -> u64 {
        (self.plan.source_rows as f64 * self.name.rows_per_source_row()).round() as u64
    }

    /// Only the first `n` source rows: a prefix of the full table.
    pub fn limit_source_rows(mut self, n: u64) -> Self {
        self.plan.source_rows = self.plan.source_rows.min(n);
        self
    }

    /// Aim for about `rows` output rows per chunk (= per Parquet row
    /// group). The values are the same for any chunk size.
    pub fn chunk_rows(mut self, rows: u64) -> Self {
        self.plan = self.plan.chunk_rows(rows);
        self
    }

    /// Chunks of exactly `n` source rows (the last may be shorter).
    pub fn chunk_source_rows(mut self, n: u64) -> Self {
        self.plan = self.plan.chunk_source_rows(n);
        self
    }

    /// Rows per Arrow batch (default 8Ki).
    pub fn batch_size(mut self, n: usize) -> Self {
        self.plan = self.plan.batch_size(n);
        self
    }

    pub fn num_chunks(&self) -> u64 {
        self.plan.num_chunks()
    }

    /// Source rows `first..end` as batches.
    fn generate(&self, first: u64, end: u64, emit: &mut dyn FnMut(RecordBatch)) {
        self.plan
            .batches(first, end, |at, n| emit(self.generator.batch(at, n)));
    }

    /// Source rows `first..end` (0-based), generated on this thread.
    pub fn range(&self, first: u64, end: u64) -> Vec<RecordBatch> {
        let mut v = Vec::new();
        self.generate(first, end.min(self.plan.source_rows), &mut |b| v.push(b));
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
    pub fn write_parquet<W: std::io::Write + Send>(
        &self,
        writer: W,
        threads: usize,
        props: Option<parquet::file::properties::WriterProperties>,
    ) -> Result<W, parquet::errors::ParquetError> {
        let props = props.unwrap_or_else(|| parquet_props().build());
        let mut w = crate::write_parquet_many(
            std::slice::from_ref(self),
            vec![writer],
            threads,
            Some(props),
        )?;
        Ok(w.pop().expect("one writer"))
    }

    /// Write the table as CSV: `,`-separated, `NULL` for NULL, decimals
    /// with their scale, times as `YYYY-MM-DD HH:MM:SS`, no header (the
    /// layout `go-tpc tpcc prepare --output-type csv` writes, ready for
    /// `LOAD DATA` / `COPY`).
    pub fn write_csv<W: std::io::Write + Send>(
        &self,
        writer: W,
        threads: usize,
    ) -> std::io::Result<W> {
        let mut w = crate::write_text_many(std::slice::from_ref(self), vec![writer], threads)?;
        Ok(w.pop().expect("one writer"))
    }
}

/// Parquet writer properties for TPC-C: dictionary encoding for the ids,
/// counts, constants and the text columns with few distinct values (states,
/// `c_last`, `c_credit`, `c_middle`); plain encoding for random text, item
/// ids and random amounts, which have too many distinct values for a
/// dictionary to pay (the writer would build one and fall back). Snappy.
/// Used when `write_parquet` gets no properties.
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

const CSV: Delimited = Delimited {
    sep: b',',
    null: b"NULL",
};

impl Chunks for TpccTable {
    fn schema(&self) -> SchemaRef {
        TpccTable::schema(self)
    }

    fn num_chunks(&self) -> u64 {
        self.plan.num_chunks()
    }

    fn chunk(&self, i: u64, emit: &mut dyn FnMut(RecordBatch)) {
        let (first, end) = self.plan.range(i);
        self.generate(first, end, emit)
    }
}

impl TextChunks for TpccTable {
    fn outputs(&self) -> usize {
        1
    }

    fn num_chunks(&self) -> u64 {
        self.plan.num_chunks()
    }

    fn chunk_text(&self, i: u64, bufs: &mut [Vec<u8>]) {
        let (first, end) = self.plan.range(i);
        self.generate(first, end, &mut |b| CSV.write(&b, &mut bufs[0]));
    }
}
