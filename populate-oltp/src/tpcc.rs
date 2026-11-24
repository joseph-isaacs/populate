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
use crate::rng::{ALNUM, Charset, DIGITS, Draws, LETTERS, below, nurand};
use crate::strings::{Views, constant};
use crate::text::Delimited;
use arrow_array::types::Decimal128Type;
use arrow_array::{ArrayRef, Int32Array, PrimitiveArray, RecordBatch, TimestampMicrosecondArray};
use arrow_buffer::{BooleanBuffer, NullBuffer};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use populate_table::text::TextChunks;
use populate_table::{Chunks, Stream, chunks};
use std::fmt;
use std::str::FromStr;
use std::sync::{Arc, LazyLock};

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
    pub fn schema(self) -> SchemaRef {
        use TpccTableName::*;
        match self {
            Warehouse => WAREHOUSE.clone(),
            District => DISTRICT.clone(),
            Customer => CUSTOMER.clone(),
            History => HISTORY.clone(),
            NewOrder => NEW_ORDER.clone(),
            Orders => ORDERS_SCHEMA.clone(),
            OrderLine => ORDER_LINE.clone(),
            Item => ITEM.clone(),
            Stock => STOCK.clone(),
        }
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
/// use populate_oltp::{Tpcc, TpccTableName};
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
            cfg: *self,
            plan: Plan::new(self.source_rows(t), t.rows_per_source_row()),
        }
    }

    /// Every table, in [`TpccTableName::ALL`] order.
    pub fn tables(&self) -> impl Iterator<Item = TpccTable> + '_ {
        TpccTableName::ALL.into_iter().map(|t| self.table(t))
    }
}

/// One table of a [`Tpcc`] database, split into chunks of source rows.
#[derive(Debug, Clone)]
pub struct TpccTable {
    name: TpccTableName,
    cfg: Tpcc,
    plan: Plan,
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
        let g = Gen::new(&self.cfg);
        self.plan.batches(first, end, |at, n| {
            use TpccTableName::*;
            emit(match self.name {
                Warehouse => g.warehouse(at, n),
                District => g.district(at, n),
                Customer => g.customer(at, n),
                History => g.history(at, n),
                NewOrder => g.new_order(at, n),
                Orders => g.orders(at, n),
                OrderLine => g.order_line(at, n),
                Item => g.item(at, n),
                Stock => g.stock(at, n),
            })
        });
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

    /// Write the table as one Parquet file, one row group per chunk.
    pub fn write_parquet<W: std::io::Write + Send>(
        &self,
        writer: W,
        threads: usize,
        props: Option<parquet::file::properties::WriterProperties>,
    ) -> Result<W, parquet::errors::ParquetError> {
        let mut w =
            crate::write_parquet_many(std::slice::from_ref(self), vec![writer], threads, props)?;
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

// ---------------------------------------------------------------------------
// Schemas

const TS: DataType = DataType::Timestamp(TimeUnit::Microsecond, None);

/// `numeric(p, s)`.
const fn dec(p: u8, s: i8) -> DataType {
    DataType::Decimal128(p, s)
}

macro_rules! schema {
    ($($name:literal : $t:expr $(, $null:ident)?);* $(;)?) => {
        Arc::new(Schema::new(vec![$(schema!(@f $name, $t $(, $null)?)),*]))
    };
    (@f $name:literal, $t:expr) => { Field::new($name, $t, false) };
    (@f $name:literal, $t:expr, null) => { Field::new($name, $t, true) };
}

static WAREHOUSE: LazyLock<SchemaRef> = LazyLock::new(|| {
    use DataType::*;
    schema!(
        "w_id": Int32; "w_name": Utf8View; "w_street_1": Utf8View; "w_street_2": Utf8View;
        "w_city": Utf8View; "w_state": Utf8View; "w_zip": Utf8View; "w_tax": dec(4, 4);
        "w_ytd": dec(12, 2);
    )
});

static DISTRICT: LazyLock<SchemaRef> = LazyLock::new(|| {
    use DataType::*;
    schema!(
        "d_id": Int32; "d_w_id": Int32; "d_name": Utf8View; "d_street_1": Utf8View;
        "d_street_2": Utf8View; "d_city": Utf8View; "d_state": Utf8View; "d_zip": Utf8View;
        "d_tax": dec(4, 4); "d_ytd": dec(12, 2); "d_next_o_id": Int32;
    )
});

static CUSTOMER: LazyLock<SchemaRef> = LazyLock::new(|| {
    use DataType::*;
    schema!(
        "c_id": Int32; "c_d_id": Int32; "c_w_id": Int32; "c_first": Utf8View;
        "c_middle": Utf8View; "c_last": Utf8View; "c_street_1": Utf8View;
        "c_street_2": Utf8View; "c_city": Utf8View; "c_state": Utf8View; "c_zip": Utf8View;
        "c_phone": Utf8View; "c_since": TS; "c_credit": Utf8View; "c_credit_lim": dec(12, 2);
        "c_discount": dec(4, 4); "c_balance": dec(12, 2); "c_ytd_payment": dec(12, 2);
        "c_payment_cnt": Int32; "c_delivery_cnt": Int32; "c_data": Utf8View;
    )
});

static HISTORY: LazyLock<SchemaRef> = LazyLock::new(|| {
    use DataType::*;
    schema!(
        "h_c_id": Int32; "h_c_d_id": Int32; "h_c_w_id": Int32; "h_d_id": Int32;
        "h_w_id": Int32; "h_date": TS; "h_amount": dec(6, 2); "h_data": Utf8View;
    )
});

static NEW_ORDER: LazyLock<SchemaRef> = LazyLock::new(|| {
    use DataType::*;
    schema!("no_o_id": Int32; "no_d_id": Int32; "no_w_id": Int32)
});

static ORDERS_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    use DataType::*;
    schema!(
        "o_id": Int32; "o_d_id": Int32; "o_w_id": Int32; "o_c_id": Int32; "o_entry_d": TS;
        "o_carrier_id": Int32, null; "o_ol_cnt": Int32; "o_all_local": Int32;
    )
});

static ORDER_LINE: LazyLock<SchemaRef> = LazyLock::new(|| {
    use DataType::*;
    schema!(
        "ol_o_id": Int32; "ol_d_id": Int32; "ol_w_id": Int32; "ol_number": Int32;
        "ol_i_id": Int32; "ol_supply_w_id": Int32; "ol_delivery_d": TS, null;
        "ol_quantity": Int32; "ol_amount": dec(6, 2); "ol_dist_info": Utf8View;
    )
});

static ITEM: LazyLock<SchemaRef> = LazyLock::new(|| {
    use DataType::*;
    schema!(
        "i_id": Int32; "i_im_id": Int32; "i_name": Utf8View; "i_price": dec(5, 2);
        "i_data": Utf8View;
    )
});

static STOCK: LazyLock<SchemaRef> = LazyLock::new(|| {
    use DataType::*;
    schema!(
        "s_i_id": Int32; "s_w_id": Int32; "s_quantity": Int32; "s_dist_01": Utf8View;
        "s_dist_02": Utf8View; "s_dist_03": Utf8View; "s_dist_04": Utf8View;
        "s_dist_05": Utf8View; "s_dist_06": Utf8View; "s_dist_07": Utf8View;
        "s_dist_08": Utf8View; "s_dist_09": Utf8View; "s_dist_10": Utf8View; "s_ytd": Int32;
        "s_order_cnt": Int32; "s_remote_cnt": Int32; "s_data": Utf8View;
    )
});

// ---------------------------------------------------------------------------
// Column builders

type Rows = std::ops::Range<u64>;

fn int32(v: Vec<i32>) -> ArrayRef {
    Arc::new(Int32Array::from(v))
}

fn ints(rows: Rows, f: impl FnMut(u64) -> i32) -> ArrayRef {
    int32(rows.map(f).collect())
}

fn const_i32(v: i32, n: usize) -> ArrayRef {
    int32(vec![v; n])
}

/// Uniform in `lo..=hi` per row.
fn rand_ints(s: Stream, rows: Rows, lo: i64, hi: i64) -> ArrayRef {
    ints(rows, |r| Draws::new(s, r).int(lo, hi) as i32)
}

fn decimal(v: Vec<i128>, p: u8, s: i8) -> ArrayRef {
    Arc::new(
        PrimitiveArray::<Decimal128Type>::from(v)
            .with_precision_and_scale(p, s)
            .expect("valid decimal"),
    )
}

fn const_dec(units: i128, n: usize, p: u8, s: i8) -> ArrayRef {
    decimal(vec![units; n], p, s)
}

/// Uniform in `lo..=hi` units of `10^-s` per row.
fn rand_dec(s: Stream, rows: Rows, lo: i64, hi: i64, p: u8, scale: i8) -> ArrayRef {
    decimal(
        rows.map(|r| Draws::new(s, r).int(lo, hi) as i128).collect(),
        p,
        scale,
    )
}

fn timestamps(v: Vec<i64>, nulls: Option<NullBuffer>) -> ArrayRef {
    Arc::new(TimestampMicrosecondArray::new(v.into(), nulls))
}

/// Random strings of uniform length in `min..=max` from `cs`.
fn rand_str(s: Stream, rows: Rows, min: usize, max: usize, cs: Charset) -> ArrayRef {
    let n = (rows.end - rows.start) as usize;
    let avg = if max > 12 { (min + max).div_ceil(2) } else { 0 };
    let mut v = Views::with_capacity(n, n * avg);
    for r in rows {
        let mut d = Draws::new(s, r);
        let len = d.int(min as i64, max as i64) as usize;
        v.push_with(len, |b| cs.fill(&mut d, b));
    }
    v.finish()
}

/// `i_data` / `s_data`: a-string `[26 .. 50]`, 10% with `ORIGINAL` at a
/// random position.
fn data_str(s: Stream, rows: Rows) -> ArrayRef {
    let n = (rows.end - rows.start) as usize;
    let mut v = Views::with_capacity(n, n * 38);
    for r in rows {
        let mut d = Draws::new(s, r);
        let len = d.int(26, 50) as usize;
        let original = below(d.next(), 10) == 0;
        let at = d.int(0, len as i64 - 8) as usize;
        v.push_with(len, |b| {
            ALNUM.fill(&mut d, b);
            if original {
                b[at..at + 8].copy_from_slice(b"ORIGINAL");
            }
        });
    }
    v.finish()
}

/// Zip codes: 4 random digits and `11111` (4.3.2.7).
fn zips(s: Stream, rows: Rows) -> ArrayRef {
    let n = (rows.end - rows.start) as usize;
    let mut v = Views::with_capacity(n, 0);
    for r in rows {
        let mut d = Draws::new(s, r);
        v.push_with(9, |b| {
            DIGITS.fill(&mut d, &mut b[..4]);
            b[4..].copy_from_slice(b"11111");
        });
    }
    v.finish()
}

/// `c_last` of number `n` in `0..=999`.
fn last_name(n: usize, v: &mut Views) {
    let parts = [
        SYLLABLES[n / 100],
        SYLLABLES[n / 10 % 10],
        SYLLABLES[n % 10],
    ];
    let len = parts.iter().map(|p| p.len()).sum();
    v.push_with(len, |b| {
        let mut at = 0;
        for p in parts {
            b[at..at + p.len()].copy_from_slice(p.as_bytes());
            at += p.len();
        }
    });
}

/// Per-column streams of one database.
struct Gen {
    seed: u64,
    load_time: i64,
    c_load: i64,
}

impl Gen {
    fn new(cfg: &Tpcc) -> Self {
        Gen {
            seed: cfg.seed,
            load_time: cfg.load_time,
            c_load: cfg.c_load(),
        }
    }

    fn s(&self, column: &str) -> Stream {
        Stream::new(self.seed, column)
    }

    /// name, street 1 and 2, city, state, zip, as in warehouse and district.
    fn address(&self, p: &str, rows: Rows, out: &mut Vec<ArrayRef>) {
        let s = |c: &str| self.s(&format!("{p}_{c}"));
        out.push(rand_str(s("street_1"), rows.clone(), 10, 20, ALNUM));
        out.push(rand_str(s("street_2"), rows.clone(), 10, 20, ALNUM));
        out.push(rand_str(s("city"), rows.clone(), 10, 20, ALNUM));
        out.push(rand_str(s("state"), rows.clone(), 2, 2, LETTERS));
        out.push(zips(s("zip"), rows));
    }

    fn batch(schema: &SchemaRef, cols: Vec<ArrayRef>) -> RecordBatch {
        RecordBatch::try_new(schema.clone(), cols).expect("columns match schema")
    }

    fn warehouse(&self, at: u64, n: u64) -> RecordBatch {
        let rows = at..at + n;
        let len = n as usize;
        let mut c = vec![
            ints(rows.clone(), |r| r as i32 + 1),
            rand_str(self.s("w_name"), rows.clone(), 6, 10, ALNUM),
        ];
        self.address("w", rows.clone(), &mut c);
        c.push(rand_dec(self.s("w_tax"), rows, 0, 2000, 4, 4));
        c.push(const_dec(30_000_000, len, 12, 2));
        Self::batch(&WAREHOUSE, c)
    }

    fn district(&self, at: u64, n: u64) -> RecordBatch {
        let rows = at..at + n;
        let len = n as usize;
        let mut c = vec![
            ints(rows.clone(), |r| (r % DISTRICTS) as i32 + 1),
            ints(rows.clone(), |r| (r / DISTRICTS) as i32 + 1),
            rand_str(self.s("d_name"), rows.clone(), 6, 10, ALNUM),
        ];
        self.address("d", rows.clone(), &mut c);
        c.push(rand_dec(self.s("d_tax"), rows, 0, 2000, 4, 4));
        c.push(const_dec(3_000_000, len, 12, 2));
        c.push(const_i32(ORDERS as i32 + 1, len));
        Self::batch(&DISTRICT, c)
    }

    /// The `c_id`, `c_d_id`, `c_w_id` of customer rows (also history and
    /// orders, which have one row per customer slot).
    fn customer_ids(rows: Rows, out: &mut Vec<ArrayRef>) {
        out.push(ints(rows.clone(), |r| (r % CUSTOMERS) as i32 + 1));
        out.push(ints(rows.clone(), |r| {
            (r / CUSTOMERS % DISTRICTS) as i32 + 1
        }));
        out.push(ints(rows, |r| (r / (CUSTOMERS * DISTRICTS)) as i32 + 1));
    }

    fn customer(&self, at: u64, n: u64) -> RecordBatch {
        let rows = at..at + n;
        let len = n as usize;
        let mut c = Vec::with_capacity(21);
        Self::customer_ids(rows.clone(), &mut c);
        c.push(rand_str(self.s("c_first"), rows.clone(), 8, 16, ALNUM));
        c.push(constant("OE", len));
        let last = {
            let s = self.s("c_last");
            let mut v = Views::with_capacity(len, len * 12);
            for r in rows.clone() {
                let id = r % CUSTOMERS;
                let num = if id < 1000 {
                    id as i64
                } else {
                    nurand(&mut Draws::new(s, r), 255, 0, 999, self.c_load)
                };
                last_name(num as usize, &mut v);
            }
            v.finish()
        };
        c.push(last);
        let s = |col: &str| self.s(col);
        c.push(rand_str(s("c_street_1"), rows.clone(), 10, 20, ALNUM));
        c.push(rand_str(s("c_street_2"), rows.clone(), 10, 20, ALNUM));
        c.push(rand_str(s("c_city"), rows.clone(), 10, 20, ALNUM));
        c.push(rand_str(s("c_state"), rows.clone(), 2, 2, LETTERS));
        c.push(zips(s("c_zip"), rows.clone()));
        c.push(rand_str(s("c_phone"), rows.clone(), 16, 16, DIGITS));
        c.push(timestamps(vec![self.load_time; len], None));
        let credit = {
            let st = s("c_credit");
            let mut v = Views::with_capacity(len, 0);
            for r in rows.clone() {
                let bad = below(Draws::new(st, r).next(), 10) == 0;
                v.push(if bad { b"BC" } else { b"GC" });
            }
            v.finish()
        };
        c.push(credit);
        c.push(const_dec(5_000_000, len, 12, 2));
        c.push(rand_dec(s("c_discount"), rows.clone(), 0, 5000, 4, 4));
        c.push(const_dec(-10_00, len, 12, 2));
        c.push(const_dec(10_00, len, 12, 2));
        c.push(const_i32(1, len));
        c.push(const_i32(0, len));
        c.push(rand_str(s("c_data"), rows, 300, 500, ALNUM));
        Self::batch(&CUSTOMER, c)
    }

    fn history(&self, at: u64, n: u64) -> RecordBatch {
        let rows = at..at + n;
        let len = n as usize;
        let mut c = Vec::with_capacity(8);
        Self::customer_ids(rows.clone(), &mut c);
        c.push(c[1].clone()); // h_d_id = h_c_d_id
        c.push(c[2].clone()); // h_w_id = h_c_w_id
        c.push(timestamps(vec![self.load_time; len], None));
        c.push(const_dec(10_00, len, 6, 2));
        c.push(rand_str(self.s("h_data"), rows, 12, 24, ALNUM));
        Self::batch(&HISTORY, c)
    }

    fn new_order(&self, at: u64, n: u64) -> RecordBatch {
        let rows = at..at + n;
        let c = vec![
            ints(rows.clone(), |r| (FIRST_NEW_ORDER + r % NEW_ORDERS) as i32),
            ints(rows.clone(), |r| (r / NEW_ORDERS % DISTRICTS) as i32 + 1),
            ints(rows, |r| (r / (NEW_ORDERS * DISTRICTS)) as i32 + 1),
        ];
        Self::batch(&NEW_ORDER, c)
    }

    /// `o_ol_cnt` of order row `r`.
    #[inline(always)]
    fn ol_cnt(s: Stream, r: u64) -> u64 {
        Draws::new(s, r).int(5, 15) as u64
    }

    /// The `o_c_id` permutation of global district `d`: a Fisher-Yates
    /// shuffle of `1..=3000`.
    fn customer_perm(&self, d: u64) -> Vec<i32> {
        let mut p: Vec<i32> = (1..=CUSTOMERS as i32).collect();
        let mut dr = Draws::new(self.s("o_c_id"), d);
        for i in (1..p.len()).rev() {
            let j = below(dr.next(), i as u64 + 1) as usize;
            p.swap(i, j);
        }
        p
    }

    fn orders(&self, at: u64, n: u64) -> RecordBatch {
        let rows = at..at + n;
        let len = n as usize;
        let mut c = Vec::with_capacity(8);
        Self::customer_ids(rows.clone(), &mut c);
        let mut cids = Vec::with_capacity(len);
        let mut perm = (u64::MAX, Vec::new());
        for r in rows.clone() {
            let d = r / ORDERS;
            if perm.0 != d {
                perm = (d, self.customer_perm(d));
            }
            cids.push(perm.1[(r % ORDERS) as usize]);
        }
        c.push(int32(cids));
        c.push(timestamps(vec![self.load_time; len], None));
        let delivered = |r: u64| r % ORDERS + 1 < FIRST_NEW_ORDER;
        let carrier = self.s("o_carrier_id");
        let carriers: Vec<i32> = rows
            .clone()
            .map(|r| {
                if delivered(r) {
                    Draws::new(carrier, r).int(1, 10) as i32
                } else {
                    0
                }
            })
            .collect();
        let valid = BooleanBuffer::collect_bool(len, |i| delivered(at + i as u64));
        c.push(Arc::new(Int32Array::new(
            carriers.into(),
            Some(NullBuffer::new(valid)),
        )));
        let ol = self.s("o_ol_cnt");
        c.push(ints(rows, |r| Self::ol_cnt(ol, r) as i32));
        c.push(const_i32(1, len));
        Self::batch(&ORDERS_SCHEMA, c)
    }

    fn order_line(&self, at: u64, n: u64) -> RecordBatch {
        let ol = self.s("o_ol_cnt");
        let counts: Vec<u64> = (at..at + n).map(|r| Self::ol_cnt(ol, r)).collect();
        let len: usize = counts.iter().sum::<u64>() as usize;
        let (mut o_id, mut d_id, mut w_id, mut number) = (
            Vec::with_capacity(len),
            Vec::with_capacity(len),
            Vec::with_capacity(len),
            Vec::with_capacity(len),
        );
        // each line's row key: order row * 16 + line (lines < 16)
        let mut keys = Vec::with_capacity(len);
        let mut delivered = Vec::with_capacity(len);
        for (r, &k) in (at..).zip(&counts) {
            let o = r % ORDERS + 1;
            for l in 0..k {
                o_id.push(o as i32);
                d_id.push((r / ORDERS % DISTRICTS) as i32 + 1);
                w_id.push((r / (ORDERS * DISTRICTS)) as i32 + 1);
                number.push(l as i32 + 1);
                keys.push(r * 16 + l);
                delivered.push(o < FIRST_NEW_ORDER);
            }
        }
        let item = self.s("ol_i_id");
        let i_id: Vec<i32> = keys
            .iter()
            .map(|&k| Draws::new(item, k).int(1, ITEMS as i64) as i32)
            .collect();
        let valid = BooleanBuffer::from(delivered.clone());
        let amount = self.s("ol_amount");
        let amounts: Vec<i128> = keys
            .iter()
            .zip(&delivered)
            .map(|(&k, &del)| {
                if del {
                    0
                } else {
                    Draws::new(amount, k).int(1, 999_999) as i128
                }
            })
            .collect();
        let dist = self.s("ol_dist_info");
        let mut info = Views::with_capacity(len, len * 24);
        for &k in &keys {
            let mut d = Draws::new(dist, k);
            info.push_with(24, |b| ALNUM.fill(&mut d, b));
        }
        let w = w_id.clone();
        let c = vec![
            int32(o_id),
            int32(d_id),
            int32(w_id),
            int32(number),
            int32(i_id),
            int32(w),
            timestamps(vec![self.load_time; len], Some(NullBuffer::new(valid))),
            const_i32(5, len),
            decimal(amounts, 6, 2),
            info.finish(),
        ];
        Self::batch(&ORDER_LINE, c)
    }

    fn item(&self, at: u64, n: u64) -> RecordBatch {
        let rows = at..at + n;
        let c = vec![
            ints(rows.clone(), |r| r as i32 + 1),
            rand_ints(self.s("i_im_id"), rows.clone(), 1, 10_000),
            rand_str(self.s("i_name"), rows.clone(), 14, 24, ALNUM),
            rand_dec(self.s("i_price"), rows.clone(), 100, 10_000, 5, 2),
            data_str(self.s("i_data"), rows),
        ];
        Self::batch(&ITEM, c)
    }

    fn stock(&self, at: u64, n: u64) -> RecordBatch {
        let rows = at..at + n;
        let len = n as usize;
        let mut c = Vec::with_capacity(17);
        c.push(ints(rows.clone(), |r| (r % STOCK_PER_WAREHOUSE) as i32 + 1));
        c.push(ints(rows.clone(), |r| (r / STOCK_PER_WAREHOUSE) as i32 + 1));
        c.push(rand_ints(self.s("s_quantity"), rows.clone(), 10, 100));
        for k in 1..=10 {
            let s = self.s(&format!("s_dist_{k:02}"));
            c.push(rand_str(s, rows.clone(), 24, 24, ALNUM));
        }
        c.push(const_i32(0, len));
        c.push(const_i32(0, len));
        c.push(const_i32(0, len));
        c.push(data_str(self.s("s_data"), rows));
        Self::batch(&STOCK, c)
    }
}
