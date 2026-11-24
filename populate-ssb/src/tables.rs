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

use crate::dict::{COLORS, DAYS, Dict, Views};
use arrow_array::{ArrayRef, Int32Array, Int64Array, RecordBatch, StringViewArray};
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
    use DataType::*;
    schema!(
        "p_partkey": Int32, "p_name": Utf8View, "p_mfgr": Utf8View, "p_category": Utf8View,
        "p_brand1": Utf8View, "p_color": Utf8View, "p_type": Utf8View, "p_size": Int32,
        "p_container": Utf8View,
    )
});

pub(crate) static SUPPLIER: LazyLock<SchemaRef> = LazyLock::new(|| {
    use DataType::*;
    schema!(
        "s_suppkey": Int32, "s_name": Utf8View, "s_address": Utf8View, "s_city": Utf8View,
        "s_nation": Utf8View, "s_region": Utf8View, "s_phone": Utf8View,
    )
});

pub(crate) static CUSTOMER: LazyLock<SchemaRef> = LazyLock::new(|| {
    use DataType::*;
    schema!(
        "c_custkey": Int32, "c_name": Utf8View, "c_address": Utf8View, "c_city": Utf8View,
        "c_nation": Utf8View, "c_region": Utf8View, "c_phone": Utf8View,
        "c_mktsegment": Utf8View,
    )
});

pub(crate) static DATE: LazyLock<SchemaRef> = LazyLock::new(|| {
    use DataType::*;
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
    use DataType::*;
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

fn u32s(v: Vec<u32>) -> ArrayRef {
    i32s(v.into_iter().map(|x| x as i32).collect())
}

fn strs(d: &Dict, views: Vec<u128>) -> ArrayRef {
    Arc::new(d.array(views, Vec::new()))
}

fn keys(first: u64, n: usize) -> ArrayRef {
    i32s((first as i32 + 1..).take(n).collect())
}

// ---------------------------------------------------------------------------
// part

/// Parts `first..first + n` (0-based).
pub(crate) fn part(d: &Dict, first: u64, n: usize) -> RecordBatch {
    // p_name: dbgen shuffles all 92 colours (92 draws) and joins the first
    // three; ssb-dbgen then moves the first to p_color
    let shuffle = P_NAME.draws(first, n);
    let mut names = Vec::with_capacity(n);
    let mut colors = Vec::with_capacity(n);
    for row in shuffle.chunks_exact(COLORS) {
        let [c, a, b] = first_three(row);
        colors.push(d.colors[c]);
        names.push(d.names[a * COLORS + b]);
    }
    let mfgr = P_MFG.ints(first, n, 1, 5);
    let cat = P_CAT.ints(first, n, 1, 5);
    let brand = P_BRND.ints(first, n, 1, 40);
    let mut mfgrs = Vec::with_capacity(n);
    let mut cats = Vec::with_capacity(n);
    let mut brands = Vec::with_capacity(n);
    for i in 0..n {
        let mc = ((mfgr[i] - 1) * 5 + cat[i] - 1) as usize;
        mfgrs.push(d.mfgrs[mfgr[i] as usize - 1]);
        cats.push(d.categories[mc]);
        brands.push(d.brands[mc * 40 + brand[i] as usize - 1]);
    }
    let types = P_TYPE.draws(first, n);
    let containers = P_CNTR.draws(first, n);
    batch(
        &PART,
        vec![
            keys(first, n),
            strs(d, names),
            strs(d, mfgrs),
            strs(d, cats),
            strs(d, brands),
            strs(d, colors),
            strs(d, types.iter().map(|&x| d.types.view(x)).collect()),
            u32s(P_SIZE.ints(first, n, 1, 50)),
            strs(
                d,
                containers.iter().map(|&x| d.containers.view(x)).collect(),
            ),
        ],
    )
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

/// Append `prefix` and `k` in 9 zero-padded digits.
#[inline]
fn name(prefix: &[u8], k: u64, out: &mut Vec<u8>) {
    out.extend_from_slice(prefix);
    let mut digits = [b'0'; 9];
    let mut k = k;
    for d in digits.iter_mut().rev() {
        *d = b'0' + (k % 10) as u8;
        k /= 10;
    }
    out.extend_from_slice(&digits);
}

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

/// The columns supplier and customer share: name, address, city, nation,
/// region and phone of rows `first..first + n`, whose city and phone are
/// at row `shared` of those streams.
struct Party {
    names: StringViewArray,
    addresses: StringViewArray,
    cities: Vec<u128>,
    nations: Vec<u128>,
    regions: Vec<u128>,
    phones: StringViewArray,
}

#[allow(clippy::too_many_arguments)]
fn party(
    d: &Dict,
    prefix: &[u8],
    addr: Stream,
    nation: Stream,
    first: u64,
    shared: u64,
    n: usize,
) -> Party {
    let mut names = Views::with_capacity(n, n * (prefix.len() + 9));
    for i in 0..n as u64 {
        let s = names.data.len();
        name(prefix, first + i + 1, &mut names.data);
        names.end_string(s);
    }
    let mut addresses = Views::with_capacity(n, n * ADDR_MAX as usize);
    for row in addr.draws(first, n).chunks_exact(ADDR_DRAWS as usize) {
        let s = addresses.data.len();
        address(row, &mut addresses.data);
        addresses.end_string(s);
    }
    let nat = nation.ints(first, n, 0, 24);
    let city = CITY.ints(shared, n, 0, 9);
    let mut phones = Views::with_capacity(n, n * 15);
    for (row, &k) in C_PHNE.draws(shared, n).chunks_exact(3).zip(&nat) {
        let s = phones.data.len();
        phone(k, row, &mut phones.data);
        phones.end_string(s);
    }
    Party {
        names: names.finish(d),
        addresses: addresses.finish(d),
        cities: nat
            .iter()
            .zip(&city)
            .map(|(&k, &c)| d.cities[(k * 10 + c) as usize])
            .collect(),
        nations: nat.iter().map(|&k| d.nations[k as usize]).collect(),
        regions: nat.iter().map(|&k| d.regions[k as usize]).collect(),
        phones: phones.finish(d),
    }
}

/// Suppliers `first..first + n`.
pub(crate) fn supplier(d: &Dict, first: u64, n: usize) -> RecordBatch {
    let p = party(d, b"Supplier#", S_ADDR, S_NTRG, first, first, n);
    batch(
        &SUPPLIER,
        vec![
            keys(first, n),
            Arc::new(p.names),
            Arc::new(p.addresses),
            strs(d, p.cities),
            strs(d, p.nations),
            strs(d, p.regions),
            Arc::new(p.phones),
        ],
    )
}

/// Customers `first..first + n`, of a data set with `s.suppliers`
/// suppliers (generated first, on the same city and phone streams).
pub(crate) fn customer(d: &Dict, s: &Scale, first: u64, n: usize) -> RecordBatch {
    let p = party(
        d,
        b"Customer#",
        C_ADDR,
        C_NTRG,
        first,
        s.suppliers + first,
        n,
    );
    let segments = C_MSEG.draws(first, n);
    batch(
        &CUSTOMER,
        vec![
            keys(first, n),
            Arc::new(p.names),
            Arc::new(p.addresses),
            strs(d, p.cities),
            strs(d, p.nations),
            strs(d, p.regions),
            Arc::new(p.phones),
            strs(d, segments.iter().map(|&x| d.segments.view(x)).collect()),
        ],
    )
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
            txt(&|i| d.seasons[crate::dict::season(days[i].month, days[i].mday)]),
            txt(&|i| flag(dnw[i] == 7)),
            txt(&|i| {
                let t = days[i];
                flag(t.mday == crate::dict::days_in_month(t.year, t.month))
            }),
            txt(&|i| flag(crate::dict::holiday(days[i].month, days[i].mday))),
            txt(&|i| flag(dnw[i] != 1 && dnw[i] != 7)),
        ],
    )
}

// ---------------------------------------------------------------------------
// lineorder

/// The line orders of orders `first..first + n` (0-based).
pub(crate) fn lineorder(d: &Dict, s: &Scale, first: u64, n: usize) -> RecordBatch {
    let ord = s.dates + first;
    let odate = O_ODATE.ints(ord, n, 0, ODATE_MAX);
    let cmax = s.customers as i32;
    let ckey = O_CKEY.ints(ord, n, 1, cmax);
    let prio = O_PRIO.draws(ord, n);
    let lcnt = O_LCNT.ints(ord, n, 1, LINES as i32);
    let pkey = L_PKEY.ints(first, n, 1, s.part_keys as i32);
    let skey = L_SKEY.ints(first, n, 1, s.suppliers as i32);
    let qty = L_QTY.ints(first, n, 1, 50);
    let disc = L_DCNT.ints(first, n, 0, 10);
    let tax = L_TAX.ints(first, n, 0, 8);
    let cdate = L_CDTE.ints(first, n, CDATE_MIN, CDATE_MAX);
    let mode = L_SMODE.draws(first, n);

    let lines: usize = lcnt.iter().map(|&c| c as usize).sum();
    let mut c_okey = Vec::with_capacity(lines);
    let mut c_line = Vec::with_capacity(lines);
    let mut c_cust = Vec::with_capacity(lines);
    let mut c_part = Vec::with_capacity(lines);
    let mut c_supp = Vec::with_capacity(lines);
    let mut c_odate = Vec::with_capacity(lines);
    let mut c_prio = Vec::with_capacity(lines);
    let mut c_qty = Vec::with_capacity(lines);
    let mut c_eprice = Vec::with_capacity(lines);
    let mut c_total = Vec::with_capacity(lines);
    let mut c_disc = Vec::with_capacity(lines);
    let mut c_rev = Vec::with_capacity(lines);
    let mut c_cost = Vec::with_capacity(lines);
    let mut c_tax = Vec::with_capacity(lines);
    let mut c_cdate = Vec::with_capacity(lines);
    let mut c_mode = Vec::with_capacity(lines);
    for o in 0..n {
        let okey = order_key((first + o as u64 + 1) as i64);
        // a third of customers have no orders: step off multiples of 3,
        // alternating up and down
        let mut cust = ckey[o] as i32;
        let mut delta = 1;
        while cust % 3 == 0 {
            cust = (cust + delta).min(cmax);
            delta = -delta;
        }
        let od = odate[o] as usize;
        let pr = d.priorities.view(prio[o]);
        let mut total = 0i64;
        let start = c_okey.len();
        for l in 0..lcnt[o] as usize {
            let j = o * LINES + l;
            let p = pkey[j] as i64;
            let price = part_price(p);
            let q = qty[j] as i64;
            let dc = disc[j] as i64;
            let t = tax[j] as i64;
            let ep = price * q;
            let rev = ep * (100 - dc) / 100;
            total += rev * (100 + t) / 100;
            c_okey.push(okey);
            c_line.push(l as i32 + 1);
            c_cust.push(cust);
            c_part.push(p as i32);
            c_supp.push(skey[j] as i32);
            c_odate.push(d.keys[od]);
            c_prio.push(pr);
            c_qty.push(q as i32);
            c_eprice.push(ep as i32);
            c_disc.push(dc as i32);
            c_rev.push(rev as i32);
            c_cost.push((6 * price / 10) as i32);
            c_tax.push(t as i32);
            c_cdate.push(d.keys[od + cdate[j] as usize]);
            c_mode.push(d.modes.view(mode[j]));
        }
        c_total.extend(std::iter::repeat_n(total as i32, c_okey.len() - start));
    }
    batch(
        &LINEORDER,
        vec![
            Arc::new(Int64Array::from(c_okey)),
            i32s(c_line),
            i32s(c_cust),
            i32s(c_part),
            i32s(c_supp),
            i32s(c_odate),
            strs(d, c_prio),
            i32s(vec![0; lines]),
            i32s(c_qty),
            i32s(c_eprice),
            i32s(c_total),
            i32s(c_disc),
            i32s(c_rev),
            i32s(c_cost),
            i32s(c_tax),
            i32s(c_cdate),
            strs(d, c_mode),
        ],
    )
}
