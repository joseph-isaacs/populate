//! The five SSB tables, generated straight into Arrow columns.
//!
//! ssb-dbgen is TPC-H's dbgen with the tables reshaped, and keeps dbgen's
//! random streams: one Park–Miller stream per column, each reserving a
//! fixed number of draws per row. So each generator takes a range of
//! source rows (orders for `lineorder`), positions every stream at the
//! range's first row, and draws the whole range at once
//! ([`draws`]) before mapping the draws to values.
//!
//! The sequential run (`dbgen -s N`, all tables, in the order part,
//! supplier, customer, date, lineorder) shares some streams between
//! tables, which moves where later tables start in them:
//!
//! - supplier and customer both draw their city from one stream and their
//!   phone number from another, so customer row `r` uses those streams at
//!   row `suppliers + r`;
//! - each `date` row skips one row of every order-level stream (the `date`
//!   table has the index the order table has in TPC-H, and dbgen skips a
//!   finished row's unused draws for the table's streams), so `lineorder`
//!   starts those streams at row `dates`.

use crate::dict::{COLORS, DAYS, Dict, Views, days_in_month, holiday, season};
use arrow_array::{ArrayRef, Int32Array, Int64Array, RecordBatch};
use arrow_schema::DataType::{Int32, Int64, Utf8View};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use populate_tpch::rng::{M, bound_all, draws, scale};
use std::sync::{Arc, LazyLock};

/// Most lines an order has, and the draws per order of the line streams.
pub(crate) const LINES: usize = 7;

/// One stream: its seed and draws reserved per row.
#[derive(Clone, Copy)]
struct Stream {
    seed: i64,
    per_row: u64,
}

impl Stream {
    const fn new(seed: i64, per_row: u64) -> Self {
        Stream { seed, per_row }
    }

    /// The draws of rows `row..row + n`, every reserved one.
    fn draws(self, row: u64, n: usize) -> Vec<u32> {
        let mut v = vec![0; n * self.per_row as usize];
        draws(self.seed, row * self.per_row, &mut v);
        v
    }

    /// [`Stream::draws`], mapped to `lo..=hi`.
    fn ints(self, row: u64, n: usize, lo: i32, hi: i32) -> Vec<u32> {
        let mut v = self.draws(row, n);
        bound_all(&mut v, lo, hi);
        v
    }
}

// Seeds and per-row draws of the streams SSB uses (dbgen's `Seed` table).
const P_MFG: Stream = Stream::new(1, 1);
const P_BRND: Stream = Stream::new(46_831_694, 1);
const P_TYPE: Stream = Stream::new(1_841_581_359, 1);
const P_SIZE: Stream = Stream::new(1_193_163_244, 1);
const P_CNTR: Stream = Stream::new(727_633_698, 1);
const P_NAME: Stream = Stream::new(709_314_158, COLORS as u64);
const P_CAT: Stream = Stream::new(637_858_759, 1);
const O_ODATE: Stream = Stream::new(1_066_728_069, 1);
const O_PRIO: Stream = Stream::new(591_449_447, 1);
const O_CKEY: Stream = Stream::new(851_767_375, 1);
const O_LCNT: Stream = Stream::new(1_434_868_289, 1);
const L_QTY: Stream = Stream::new(209_208_115, LINES as u64);
const L_DCNT: Stream = Stream::new(554_590_007, LINES as u64);
const L_TAX: Stream = Stream::new(721_958_466, LINES as u64);
const L_SMODE: Stream = Stream::new(675_466_456, LINES as u64);
const L_PKEY: Stream = Stream::new(1_808_217_256, LINES as u64);
const L_SKEY: Stream = Stream::new(2_095_021_727, LINES as u64);
const L_CDTE: Stream = Stream::new(904_914_315, LINES as u64);
const C_ADDR: Stream = Stream::new(881_155_353, ADDR_DRAWS);
const C_NTRG: Stream = Stream::new(1_489_529_863, 1);
const C_PHNE: Stream = Stream::new(1_521_138_112, 3);
const C_MSEG: Stream = Stream::new(1_140_279_430, 1);
const S_ADDR: Stream = Stream::new(706_178_559, ADDR_DRAWS);
const S_NTRG: Stream = Stream::new(110_356_601, 1);
const CITY: Stream = Stream::new(1_495_190_827, 1);

/// Draws reserved per address: a length, then one draw per 5 characters.
const ADDR_DRAWS: u64 = 9;
/// Address lengths: 0.4x to 1.6x of 15.
const ADDR_MIN: i32 = 6;
const ADDR_MAX: i32 = 24;
const ALPHA_NUM: &[u8; 64] = b"0123456789abcdefghijklmnopqrstuvwxyz ABCDEFGHIJKLMNOPQRSTUVWXYZ,";

/// First and last order date, as days since 1992-01-01: the last leaves
/// room for 121 days to ship and 30 to receive, as in TPC-H.
const ODATE_MAX: i32 = (DAYS - 121 - 30 - 1) as i32;
const CDATE_MIN: i32 = 30;
const CDATE_MAX: i32 = 90;

/// Row counts and key ranges, as ssb-dbgen computes them.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Scale {
    pub(crate) sf: f64,
    pub(crate) parts: u64,
    pub(crate) suppliers: u64,
    pub(crate) customers: u64,
    pub(crate) dates: u64,
    pub(crate) orders: u64,
    /// Largest `lo_partkey`.
    pub(crate) part_keys: i64,
}

impl Scale {
    /// ssb-dbgen's `-s sf`: below 1, every table's base row count is
    /// multiplied by `sf` (at least one row, the date table too); from 1,
    /// the scale is `sf` truncated to an integer.
    pub(crate) fn new(sf: f64) -> Self {
        let base = |b: i64| -> i64 {
            if sf < 1.0 {
                ((b as f64 * sf) as i64).max(1)
            } else {
                b
            }
        };
        let s = if sf < 1.0 { 1 } else { sf as i64 };
        let part_base = base(200_000);
        // ssb-dbgen sizes `part` with log2 of the scale but draws
        // `lo_partkey` with the natural log
        let parts = (part_base as f64 * (1.0 + (s as f64).ln() / 2f64.ln()).floor()) as u64;
        let part_keys = (part_base as f64 * ((s as f64).ln().floor() + 1.0)) as i64;
        Scale {
            sf,
            parts,
            suppliers: (base(2_000) * s) as u64,
            customers: (base(30_000) * s) as u64,
            dates: base(DAYS as i64) as u64,
            orders: (base(1_500_000) * s) as u64,
            part_keys,
        }
    }
}

/// `lo_orderkey` of the `i`th order (1-based): keys are sparse, 8 used of
/// every 32.
#[inline(always)]
pub(crate) fn order_key(i: i64) -> i64 {
    ((i >> 3) << 5) + (i & 7)
}

/// The retail price of part `p`, in cents.
#[inline(always)]
fn part_price(p: i64) -> i64 {
    90_000 + (p / 10) % 20_001 + (p % 1000) * 100
}

// ---------------------------------------------------------------------------
// Schemas: ssb-dbgen's columns, as `doc/ssb.ddl` names them. Integers are
// `Int32` (`lo_orderkey`, which passes 2^31 from SF 358, is `Int64`), dates
// `Int32` `YYYYMMDD` as dbgen prints them, text `Utf8View`.

fn field(name: &str, t: DataType) -> Field {
    Field::new(name, t, false)
}

macro_rules! schema {
    ($($name:literal : $t:expr),* $(,)?) => {
        Arc::new(Schema::new(vec![$(field($name, $t)),*]))
    };
}

pub(crate) static PART: LazyLock<SchemaRef> = LazyLock::new(|| {
    schema!(
        "p_partkey": Int32, "p_name": Utf8View, "p_mfgr": Utf8View, "p_category": Utf8View,
        "p_brand1": Utf8View, "p_color": Utf8View, "p_type": Utf8View, "p_size": Int32,
        "p_container": Utf8View,
    )
});

pub(crate) static SUPPLIER: LazyLock<SchemaRef> = LazyLock::new(|| {
    schema!(
        "s_suppkey": Int32, "s_name": Utf8View, "s_address": Utf8View, "s_city": Utf8View,
        "s_nation": Utf8View, "s_region": Utf8View, "s_phone": Utf8View,
    )
});

pub(crate) static CUSTOMER: LazyLock<SchemaRef> = LazyLock::new(|| {
    schema!(
        "c_custkey": Int32, "c_name": Utf8View, "c_address": Utf8View, "c_city": Utf8View,
        "c_nation": Utf8View, "c_region": Utf8View, "c_phone": Utf8View,
        "c_mktsegment": Utf8View,
    )
});

pub(crate) static DATE: LazyLock<SchemaRef> = LazyLock::new(|| {
    schema!(
        "d_datekey": Int32, "d_date": Utf8View, "d_dayofweek": Utf8View, "d_month": Utf8View,
        "d_year": Int32, "d_yearmonthnum": Int32, "d_yearmonth": Utf8View,
        "d_daynuminweek": Int32, "d_daynuminmonth": Int32, "d_daynuminyear": Int32,
        "d_monthnuminyear": Int32, "d_weeknuminyear": Int32, "d_sellingseason": Utf8View,
        "d_lastdayinweekfl": Utf8View, "d_lastdayinmonthfl": Utf8View, "d_holidayfl": Utf8View,
        "d_weekdayfl": Utf8View,
    )
});

/// `lineorder`'s fields, shared with `lineorder_flat`.
pub(crate) fn lineorder_fields() -> Vec<Field> {
    [
        ("lo_orderkey", Int64),
        ("lo_linenumber", Int32),
        ("lo_custkey", Int32),
        ("lo_partkey", Int32),
        ("lo_suppkey", Int32),
        ("lo_orderdate", Int32),
        ("lo_orderpriority", Utf8View),
        ("lo_shippriority", Int32),
        ("lo_quantity", Int32),
        ("lo_extendedprice", Int32),
        ("lo_ordtotalprice", Int32),
        ("lo_discount", Int32),
        ("lo_revenue", Int32),
        ("lo_supplycost", Int32),
        ("lo_tax", Int32),
        ("lo_commitdate", Int32),
        ("lo_shipmode", Utf8View),
    ]
    .into_iter()
    .map(|(n, t)| field(n, t))
    .collect()
}

pub(crate) static LINEORDER: LazyLock<SchemaRef> =
    LazyLock::new(|| Arc::new(Schema::new(lineorder_fields())));

fn batch(schema: &SchemaRef, cols: Vec<ArrayRef>) -> RecordBatch {
    RecordBatch::try_new(schema.clone(), cols).expect("columns match the schema")
}

fn i32s(v: Vec<i32>) -> ArrayRef {
    Arc::new(Int32Array::from(v))
}

/// The string column of views `$view(r)` for each row `r` of `$rows` (a
/// macro, so the view expression is inlined into the loop).
macro_rules! gather {
    ($d:expr, $rows:expr, |$r:ident| $view:expr) => {
        strs(
            $d,
            $rows
                .iter()
                .map(|&$r| {
                    let $r = $r as usize;
                    $view
                })
                .collect(),
        )
    };
}

fn strs(d: &Dict, views: Vec<u128>) -> ArrayRef {
    Arc::new(d.array(views, Vec::new()))
}

fn keys(first: u64, n: usize) -> ArrayRef {
    i32s((first as i32 + 1..).take(n).collect())
}

// ---------------------------------------------------------------------------
// part

/// The drawn values of a run of parts, as small codes: what `part` and
/// `lineorder_flat` both make their columns from.
#[derive(Debug, Default)]
pub(crate) struct PartRows {
    /// `p_name`: the second and third colour, `a * COLORS + b`.
    name: Vec<u16>,
    /// `p_color`: the first colour.
    color: Vec<u8>,
    /// Manufacturer and category, `(m - 1) * 5 + c - 1`.
    category: Vec<u8>,
    /// `0..40`.
    brand: Vec<u8>,
    /// Indices into the type and container distributions.
    kind: Vec<u8>,
    container: Vec<u8>,
    size: Vec<u8>,
}

impl PartRows {
    /// Parts `first..first + n` (0-based).
    pub(crate) fn new(d: &Dict, first: u64, n: usize) -> Self {
        // p_name: dbgen shuffles all 92 colours (92 draws) and joins the
        // first three; ssb-dbgen then moves the first to p_color
        let shuffle = P_NAME.draws(first, n);
        let mut name = Vec::with_capacity(n);
        let mut color = Vec::with_capacity(n);
        for row in shuffle.chunks_exact(COLORS) {
            let [c, a, b] = first_three(row);
            color.push(c as u8);
            name.push((a * COLORS + b) as u16);
        }
        let mfgr = P_MFG.ints(first, n, 1, 5);
        let cat = P_CAT.ints(first, n, 1, 5);
        PartRows {
            name,
            color,
            category: mfgr
                .iter()
                .zip(&cat)
                .map(|(m, c)| ((m - 1) * 5 + c - 1) as u8)
                .collect(),
            brand: P_BRND
                .ints(first, n, 0, 39)
                .iter()
                .map(|&b| b as u8)
                .collect(),
            kind: P_TYPE
                .draws(first, n)
                .iter()
                .map(|&x| d.types.index(x) as u8)
                .collect(),
            container: P_CNTR
                .draws(first, n)
                .iter()
                .map(|&x| d.containers.index(x) as u8)
                .collect(),
            size: P_SIZE
                .ints(first, n, 1, 50)
                .iter()
                .map(|&s| s as u8)
                .collect(),
        }
    }

    pub(crate) fn append(&mut self, o: PartRows) {
        self.name.extend(o.name);
        self.color.extend(o.color);
        self.category.extend(o.category);
        self.brand.extend(o.brand);
        self.kind.extend(o.kind);
        self.container.extend(o.container);
        self.size.extend(o.size);
    }

    /// The columns after `p_partkey` of rows `rows` (indices into these).
    pub(crate) fn columns(&self, d: &Dict, rows: &[u32]) -> Vec<ArrayRef> {
        vec![
            gather!(d, rows, |r| d.names[self.name[r] as usize]),
            gather!(d, rows, |r| d.mfgrs[self.category[r] as usize / 5]),
            gather!(d, rows, |r| d.categories[self.category[r] as usize]),
            gather!(d, rows, |r| d.brands
                [self.category[r] as usize * 40 + self.brand[r] as usize]),
            gather!(d, rows, |r| d.colors[self.color[r] as usize]),
            gather!(d, rows, |r| d.types.views[self.kind[r] as usize]),
            i32s(rows.iter().map(|&r| self.size[r as usize] as i32).collect()),
            gather!(d, rows, |r| d.containers.views[self.container[r] as usize]),
        ]
    }
}

/// `0..n`, the rows of a whole run.
fn all(n: usize) -> Vec<u32> {
    (0..n as u32).collect()
}

/// Parts `first..first + n` (0-based).
pub(crate) fn part(d: &Dict, first: u64, n: usize) -> RecordBatch {
    let mut cols = vec![keys(first, n)];
    cols.extend(PartRows::new(d, first, n).columns(d, &all(n)));
    batch(&PART, cols)
}

/// The first three entries of dbgen's shuffle of `0..92` by these draws
/// (`permute`: for each `i`, swap `i` with a uniform position).
#[inline]
fn first_three(draws: &[u32]) -> [usize; 3] {
    let mut a = [0u8; COLORS];
    for (i, x) in a.iter_mut().enumerate() {
        *x = i as u8;
    }
    for (i, &x) in draws.iter().enumerate() {
        a.swap(scale(x, COLORS as u32) as usize, i);
    }
    [a[0] as usize, a[1] as usize, a[2] as usize]
}

// ---------------------------------------------------------------------------
// supplier and customer

/// dbgen's `UnifInt(0, 2^31 - 1)` with 64-bit `long`s: `x / m * 2^31`.
#[inline(always)]
fn full_range(x: u32) -> u64 {
    ((x as f64 / M as f64) * 2_147_483_648.0) as u64
}

/// Append the address the draws of one row make (`a_rnd`): a length, then
/// 5 characters of 6 bits from each further draw.
#[inline]
fn address(row: &[u32], out: &mut Vec<u8>) {
    let len = ADDR_MIN as usize + scale(row[0], (ADDR_MAX - ADDR_MIN + 1) as u32) as usize;
    let mut bits = 0;
    for i in 0..len {
        if i % 5 == 0 {
            bits = full_range(row[1 + i / 5]);
        }
        out.push(ALPHA_NUM[(bits & 63) as usize]);
        bits >>= 6;
    }
}

/// Append `prefix` and `k` in 9 zero-padded digits (dbgen's `%09ld`; keys
/// stay below 10^9 up to `MAX_SCALE_FACTOR`).
#[inline]
fn name(prefix: &[u8], k: u64, out: &mut Vec<u8>) {
    debug_assert!(k < 1_000_000_000);
    out.extend_from_slice(prefix);
    let mut digits = [b'0'; 9];
    let mut k = k;
    for d in digits.iter_mut().rev() {
        *d = b'0' + (k % 10) as u8;
        k /= 10;
    }
    out.extend_from_slice(&digits);
}

/// Bytes of a phone number.
const PHONE: usize = 15;

/// Append a phone number: the nation's country code, then three draws.
#[inline]
fn phone(nation: u32, draws: &[u32], out: &mut Vec<u8>) {
    let a = 100 + scale(draws[0], 900);
    let b = 100 + scale(draws[1], 900);
    let c = 1000 + scale(draws[2], 9000);
    let cc = 10 + nation;
    let digit = |v: u32| b'0' + v as u8;
    out.extend_from_slice(&[
        digit(cc / 10),
        digit(cc % 10),
        b'-',
        digit(a / 100),
        digit(a / 10 % 10),
        digit(a % 10),
        b'-',
        digit(b / 100),
        digit(b / 10 % 10),
        digit(b % 10),
        b'-',
        digit(c / 1000),
        digit(c / 100 % 10),
        digit(c / 10 % 10),
        digit(c % 10),
    ]);
}

/// Supplier or customer.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Party {
    Supplier,
    Customer,
}

impl Party {
    fn prefix(self) -> &'static [u8] {
        match self {
            Party::Supplier => b"Supplier#",
            Party::Customer => b"Customer#",
        }
    }
}

/// The drawn values of a run of suppliers or customers (rows `first..`):
/// what their tables and `lineorder_flat` make their columns from.
#[derive(Debug)]
pub(crate) struct PartyRows {
    party: Party,
    first: u64,
    nation: Vec<u8>,
    /// City digit.
    city: Vec<u8>,
    /// Address `i` is `addresses[ends[i - 1]..ends[i]]`.
    ends: Vec<u32>,
    addresses: Vec<u8>,
    /// `PHONE` bytes per row.
    phones: Vec<u8>,
    /// Market segment (customers only).
    segment: Vec<u8>,
}

impl PartyRows {
    /// Rows `first..first + n` of a data set of scale `s`: customers draw
    /// their city and phone where the suppliers left those streams.
    pub(crate) fn new(d: &Dict, s: &Scale, party: Party, first: u64, n: usize) -> Self {
        let (addr, nation, shared) = match party {
            Party::Supplier => (S_ADDR, S_NTRG, first),
            Party::Customer => (C_ADDR, C_NTRG, s.suppliers + first),
        };
        let mut addresses = Vec::with_capacity(n * ADDR_MAX as usize);
        let mut ends = Vec::with_capacity(n);
        for row in addr.draws(first, n).chunks_exact(ADDR_DRAWS as usize) {
            address(row, &mut addresses);
            ends.push(addresses.len() as u32);
        }
        let nat = nation.ints(first, n, 0, 24);
        let mut phones = Vec::with_capacity(n * PHONE);
        for (row, &k) in C_PHNE.draws(shared, n).chunks_exact(3).zip(&nat) {
            phone(k, row, &mut phones);
        }
        let segment = match party {
            Party::Supplier => Vec::new(),
            Party::Customer => C_MSEG
                .draws(first, n)
                .iter()
                .map(|&x| d.segments.index(x) as u8)
                .collect(),
        };
        PartyRows {
            party,
            first,
            nation: nat.iter().map(|&k| k as u8).collect(),
            city: CITY
                .ints(shared, n, 0, 9)
                .iter()
                .map(|&c| c as u8)
                .collect(),
            ends,
            addresses,
            phones,
            segment,
        }
    }

    pub(crate) fn append(&mut self, o: PartyRows) {
        let base = self.addresses.len() as u32;
        self.nation.extend(o.nation);
        self.city.extend(o.city);
        self.ends.extend(o.ends.iter().map(|e| e + base));
        self.addresses.extend(o.addresses);
        self.phones.extend(o.phones);
        self.segment.extend(o.segment);
    }

    fn address(&self, r: usize) -> &[u8] {
        let start = if r == 0 { 0 } else { self.ends[r - 1] as usize };
        &self.addresses[start..self.ends[r] as usize]
    }

    pub(crate) fn len(&self) -> usize {
        self.nation.len()
    }

    /// Name, address and phone of rows `rows` (indices into these),
    /// written into their own buffers.
    pub(crate) fn unique_columns(&self, d: &Dict, rows: &[u32]) -> [ArrayRef; 3] {
        let n = rows.len();
        let prefix = self.party.prefix();
        let mut names = Views::with_capacity(n, n * (prefix.len() + 9));
        let mut addresses = Views::with_capacity(n, n * ADDR_MAX as usize);
        let mut phones = Views::with_capacity(n, n * PHONE);
        for &r in rows {
            let r = r as usize;
            let s = names.data.len();
            name(prefix, self.first + r as u64 + 1, &mut names.data);
            names.end_string(s);
            let s = addresses.data.len();
            addresses.data.extend_from_slice(self.address(r));
            addresses.end_string(s);
            let s = phones.data.len();
            phones
                .data
                .extend_from_slice(&self.phones[r * PHONE..(r + 1) * PHONE]);
            phones.end_string(s);
        }
        [
            Arc::new(names.finish(d)),
            Arc::new(addresses.finish(d)),
            Arc::new(phones.finish(d)),
        ]
    }

    /// City, nation, region and, for customers, market segment of rows
    /// `rows`: views into the dictionary.
    pub(crate) fn coded_columns(&self, d: &Dict, rows: &[u32]) -> Vec<ArrayRef> {
        let nation = |r: usize| self.nation[r] as usize;
        let mut cols = vec![
            gather!(d, rows, |r| d.cities
                [nation(r) * 10 + self.city[r] as usize]),
            gather!(d, rows, |r| d.nations[nation(r)]),
            gather!(d, rows, |r| d.regions[nation(r)]),
        ];
        if let Party::Customer = self.party {
            cols.push(gather!(d, rows, |r| d.segments.views[self.segment[r] as usize]));
        }
        cols
    }

    /// The columns after the key of rows `rows` (indices into these):
    /// name, address, city, nation, region, phone and, for customers,
    /// market segment.
    pub(crate) fn columns(&self, d: &Dict, rows: &[u32]) -> Vec<ArrayRef> {
        let [names, addresses, phones] = self.unique_columns(d, rows);
        let mut coded = self.coded_columns(d, rows);
        let mut cols = vec![names, addresses];
        cols.extend(coded.drain(..3));
        cols.push(phones);
        cols.extend(coded);
        cols
    }
}

/// Suppliers `first..first + n`.
pub(crate) fn supplier(d: &Dict, s: &Scale, first: u64, n: usize) -> RecordBatch {
    let mut cols = vec![keys(first, n)];
    cols.extend(PartyRows::new(d, s, Party::Supplier, first, n).columns(d, &all(n)));
    batch(&SUPPLIER, cols)
}

/// Customers `first..first + n`.
pub(crate) fn customer(d: &Dict, s: &Scale, first: u64, n: usize) -> RecordBatch {
    let mut cols = vec![keys(first, n)];
    cols.extend(PartyRows::new(d, s, Party::Customer, first, n).columns(d, &all(n)));
    batch(&CUSTOMER, cols)
}

// ---------------------------------------------------------------------------
// date

/// Days `first..first + n` of the calendar.
pub(crate) fn date(d: &Dict, first: u64, n: usize) -> RecordBatch {
    let days = &d.days[first as usize..first as usize + n];
    // ssb-dbgen numbers weekdays from Sunday = 1, but computes the number
    // as `(tm_wday + 1) % 7 + 1` and names the day from it: Wednesday is 5,
    // "Thursday"
    let dnw: Vec<i32> = days.iter().map(|t| (t.wday + 1) % 7 + 1).collect();
    let col = |f: &dyn Fn(usize) -> i32| i32s((0..n).map(f).collect());
    let txt = |f: &dyn Fn(usize) -> u128| strs(d, (0..n).map(f).collect());
    let flag = |b: bool| d.flags[b as usize];
    batch(
        &DATE,
        vec![
            col(&|i| days[i].key()),
            txt(&|i| d.day_names[first as usize + i]),
            txt(&|i| d.weekdays[dnw[i] as usize - 1]),
            txt(&|i| d.months[days[i].month as usize - 1]),
            col(&|i| days[i].year),
            col(&|i| days[i].year * 100 + days[i].month),
            txt(&|i| d.year_months[((days[i].year - 1992) * 12 + days[i].month - 1) as usize]),
            col(&|i| dnw[i]),
            col(&|i| days[i].mday),
            col(&|i| days[i].yday),
            col(&|i| days[i].month),
            col(&|i| days[i].yday / 7 + 1),
            txt(&|i| d.seasons[season(days[i].month, days[i].mday)]),
            txt(&|i| flag(dnw[i] == 7)),
            txt(&|i| {
                let t = days[i];
                flag(t.mday == days_in_month(t.year, t.month))
            }),
            txt(&|i| flag(holiday(days[i].month, days[i].mday))),
            txt(&|i| flag(dnw[i] != 1 && dnw[i] != 7)),
        ],
    )
}

// ---------------------------------------------------------------------------
// lineorder

/// The line orders of orders `first..first + n` (0-based).
///
/// Column at a time: the draws of every stream for the whole block, then
/// the slots the orders use (`line_count` of each order's 7), then each
/// column gathered from its draws through that list.
pub(crate) fn lineorder(d: &Dict, s: &Scale, first: u64, n: usize) -> RecordBatch {
    let ord = s.dates + first;
    let lcnt = O_LCNT.ints(ord, n, 1, LINES as i32);

    // `slot[i]`: the draw (order * 7 + line) of line `i`; `order[i]`: its
    // order. Each order writes all 7 and keeps `line_count`, so there is
    // no branch per line.
    let lines: usize = lcnt.iter().map(|&c| c as usize).sum();
    let mut slot = vec![0u32; lines + LINES];
    let mut order = vec![0u32; lines + LINES];
    let mut at = 0;
    for (o, &c) in lcnt.iter().enumerate() {
        let (sl, or) = (&mut slot[at..at + LINES], &mut order[at..at + LINES]);
        for l in 0..LINES {
            sl[l] = (o * LINES + l) as u32;
            or[l] = o as u32;
        }
        at += c as usize;
    }
    slot.truncate(lines);
    order.truncate(lines);
    let gather = |v: &[u32]| -> Vec<i32> { slot.iter().map(|&j| v[j as usize] as i32).collect() };
    let per_order = |v: &[i32]| -> Vec<i32> { order.iter().map(|&o| v[o as usize]).collect() };

    // order-level columns
    let odate = O_ODATE.ints(ord, n, 0, ODATE_MAX);
    let cmax = s.customers as i32;
    let cust: Vec<i32> = O_CKEY
        .ints(ord, n, 1, cmax)
        .into_iter()
        .map(|c| {
            // a third of customers have no orders: step off multiples of
            // 3, alternating up and down
            let mut c = c as i32;
            let mut delta = 1;
            while c % 3 == 0 {
                c = (c + delta).min(cmax);
                delta = -delta;
            }
            c
        })
        .collect();
    let prio: Vec<u128> = O_PRIO
        .draws(ord, n)
        .iter()
        .map(|&x| d.priorities.view(x))
        .collect();
    let okey: Vec<i64> = order
        .iter()
        .map(|&o| order_key((first + o as u64 + 1) as i64))
        .collect();
    let line_no: Vec<i32> = slot
        .iter()
        .map(|&j| (j % LINES as u32) as i32 + 1)
        .collect();

    // line-level columns
    let pkey = gather(&L_PKEY.ints(first, n, 1, s.part_keys as i32));
    let qty = gather(&L_QTY.ints(first, n, 1, 50));
    let disc = gather(&L_DCNT.ints(first, n, 0, 10));
    let tax = gather(&L_TAX.ints(first, n, 0, 8));
    // every product fits in 31 bits: prices are under 210,000 cents, so
    // `price * 50 * 100` and `revenue * 108` are under 2^31
    let price: Vec<i32> = pkey.iter().map(|&p| part_price(p as i64) as i32).collect();
    let eprice: Vec<i32> = price.iter().zip(&qty).map(|(p, q)| p * q).collect();
    let rev: Vec<i32> = eprice
        .iter()
        .zip(&disc)
        .map(|(e, dc)| e * (100 - dc) / 100)
        .collect();
    let cost: Vec<i32> = price.iter().map(|p| 6 * p / 10).collect();
    let mut total = vec![0i32; n];
    for ((&o, &r), &t) in order.iter().zip(&rev).zip(&tax) {
        total[o as usize] += r * (100 + t) / 100;
    }
    let date = |o: u32, plus: i32| d.keys[(odate[o as usize] as i32 + plus) as usize];
    let cdate = L_CDTE.ints(first, n, CDATE_MIN, CDATE_MAX);
    let mode = L_SMODE.draws(first, n);
    batch(
        &LINEORDER,
        vec![
            Arc::new(Int64Array::from(okey)),
            i32s(line_no),
            i32s(per_order(&cust)),
            i32s(pkey),
            i32s(gather(&L_SKEY.ints(first, n, 1, s.suppliers as i32))),
            i32s(order.iter().map(|&o| date(o, 0)).collect()),
            strs(d, order.iter().map(|&o| prio[o as usize]).collect()),
            i32s(vec![0; lines]),
            i32s(qty),
            i32s(eprice),
            i32s(per_order(&total)),
            i32s(disc),
            i32s(rev),
            i32s(cost),
            i32s(tax),
            i32s(
                slot.iter()
                    .zip(&order)
                    .map(|(&j, &o)| date(o, cdate[j as usize] as i32))
                    .collect(),
            ),
            strs(
                d,
                slot.iter()
                    .map(|&j| d.modes.view(mode[j as usize]))
                    .collect(),
            ),
        ],
    )
}
