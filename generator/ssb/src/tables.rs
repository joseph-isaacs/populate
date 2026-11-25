//! The five SSB tables, each one populate [`Distribution`].
//!
//! A table is a tuple of named columns, each a distribution over source
//! rows (orders for `lineorder`). Its schema is read off that expression:
//! the names from [`named`], the types from the columns the values are
//! collected into ([`schema_of`](populate_sink_arrow::schema_of)).
//!
//! ssb-dbgen is TPC-H's dbgen with the tables reshaped, and keeps dbgen's
//! random streams: one Park–Miller stream per column ([`Lcg`]), each
//! reserving a fixed number of draws per row. [`draws`] and [`Ints`] make
//! such a stream a leaf, collected by one vectorised fill and skipped by
//! an `O(log n)` jump, so any range of rows starts where the sequential
//! generator would be.
//!
//! The sequential run (`dbgen -s N`, all tables, in the order part,
//! supplier, customer, date, lineorder) shares some streams between
//! tables, which moves where later tables start in them ([`at`]):
//!
//! - supplier and customer both draw their city from one stream and their
//!   phone number from another, so customer row `r` uses those streams at
//!   row `suppliers + r`;
//! - each `date` row skips one row of every order-level stream (the `date`
//!   table has the index the order table has in TPC-H, and dbgen skips a
//!   finished row's unused draws for the table's streams), so `lineorder`
//!   starts those streams at row `dates`.
//!
//! `lineorder` is [`children_of`] the orders: each block of orders writes
//! its lines a column at a time ([`lines`]).
//!
//! The dimension columns are made in two steps, so `lineorder_flat` can
//! keep the dimensions as a few bytes per row: a distribution of small
//! codes ([`part_codes`], [`party_codes`]) and a [`derive()`] of those into
//! dictionary views ([`part_words`], [`party_words`]).

use crate::TableName;
use crate::cols::{Bytes, Word, children_of, format};
use crate::dict::{COLORS, DAYS, Dict, NATIONS, days_in_month, holiday, season};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use populate_builtins::{Draws, derive, draws, named, named_all, range};
use populate_combinators::lehmer::{Int, Ints, Lcg, M, scale};
use populate_core::{Columnar, Columns, Distribution, Sink};
use populate_sink_arrow::IntoArrowColumns;
use populate_table::DistTable;
use std::fmt;
use std::sync::Arc;

/// Most lines an order has, and the draws per order of the line streams.
pub(crate) const LINES: usize = 7;

// Seeds of the streams SSB uses (dbgen's `Seed` table).
const P_MFG: i64 = 1;
const P_BRND: i64 = 46_831_694;
const P_TYPE: i64 = 1_841_581_359;
const P_SIZE: i64 = 1_193_163_244;
const P_CNTR: i64 = 727_633_698;
const P_NAME: i64 = 709_314_158;
const P_CAT: i64 = 637_858_759;
const O_ODATE: i64 = 1_066_728_069;
const O_PRIO: i64 = 591_449_447;
const O_CKEY: i64 = 851_767_375;
const O_LCNT: i64 = 1_434_868_289;
const L_QTY: i64 = 209_208_115;
const L_DCNT: i64 = 554_590_007;
const L_TAX: i64 = 721_958_466;
const L_SMODE: i64 = 675_466_456;
const L_PKEY: i64 = 1_808_217_256;
const L_SKEY: i64 = 2_095_021_727;
const L_CDTE: i64 = 904_914_315;
const C_ADDR: i64 = 881_155_353;
const C_NTRG: i64 = 1_489_529_863;
const C_PHNE: i64 = 1_521_138_112;
const C_MSEG: i64 = 1_140_279_430;
const S_ADDR: i64 = 706_178_559;
const S_NTRG: i64 = 110_356_601;
const CITY: i64 = 1_495_190_827;

/// Draws reserved per address: a length, then one draw per 5 characters.
const ADDR_DRAWS: usize = 9;
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
// Leaves

/// Trait bounds every column distribution meets, so helpers can return
/// one as `impl Col<T>`.
pub(crate) trait Col<T>: Distribution<Item = T> + Clone + Send + Sync + 'static {}

impl<T, D: Distribution<Item = T> + Clone + Send + Sync + 'static> Col<T> for D {}

/// `d` from its row `row` on: for a stream another table used first.
pub(crate) fn at<D: Distribution>(mut d: D, row: u64) -> D {
    d.skip(row);
    d
}

/// The stream seeded `seed`, `K` draws per row.
fn stream<const K: usize>(seed: i64) -> Draws<Lcg, K> {
    draws(Lcg::new(seed))
}

/// `lo..=hi`, one draw per row.
fn ints(seed: i64, lo: i32, hi: i32) -> Int {
    Int(Ints::new(seed, lo, hi))
}

/// The 1-based row number: every dimension's key.
fn key() -> impl Col<i32> {
    range(1i32, 1)
}

/// The value `f` of the row's day, for the `date` table.
fn day<T: Columnar + Clone + 'static>(
    f: impl Fn(usize) -> T + Clone + Send + Sync + 'static,
) -> impl Col<T> {
    derive(range(0usize, 1), move |&i| f(i))
}

// ---------------------------------------------------------------------------
// Tables

/// A table, generated from any range of its source rows.
pub(crate) trait TableGen: Send + Sync {
    fn schema(&self) -> SchemaRef;

    /// Source rows `first..first + n`.
    fn batch(&self, first: u64, n: u64) -> RecordBatch;
}

impl fmt::Debug for dyn TableGen {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TableGen").finish_non_exhaustive()
    }
}

impl<D> TableGen for DistTable<D>
where
    D: Distribution + Clone + Send + Sync,
    Columns<D::Item>: IntoArrowColumns,
{
    fn schema(&self) -> SchemaRef {
        DistTable::schema(self).clone()
    }

    fn batch(&self, first: u64, n: u64) -> RecordBatch {
        DistTable::batch(self, first, n)
    }
}

pub(crate) fn table<D>(t: TableName, rows: u64, dist: D) -> Arc<dyn TableGen>
where
    D: Distribution + Clone + Send + Sync + 'static,
    Columns<D::Item>: IntoArrowColumns,
{
    Arc::new(DistTable::new(t.as_str(), rows, dist))
}

/// Base table `t` (not `lineorder_flat`) at scale `s`.
pub(crate) fn generator(t: TableName, s: &Scale) -> Arc<dyn TableGen> {
    let d = Dict::get();
    match t {
        TableName::Part => part(d, s),
        TableName::Supplier => party(d, s, Party::Supplier),
        TableName::Customer => party(d, s, Party::Customer),
        TableName::Date => date(d, s),
        TableName::LineOrder => table(
            t,
            s.orders,
            named_all(&LINEORDER, children_of::<_, Line, _>(orders(s), lines(d))),
        ),
        TableName::LineOrderFlat => unreachable!("lineorder_flat is built by `flat`"),
    }
}

// ---------------------------------------------------------------------------
// part

/// A part's drawn values, as small codes: the name (second and third
/// colour, `a * COLORS + b`) and first colour, manufacturer and category
/// (`(m - 1) * 5 + c - 1`), brand (`0..40`), type and container (indices
/// into their distributions) and size.
pub(crate) type PartCode = ((u16, u8), u8, u8, u8, u8, i32);

/// The codes of every part.
pub(crate) fn part_codes(d: &'static Dict) -> impl Col<PartCode> {
    (
        // p_name: dbgen shuffles all 92 colours (92 draws) and joins the
        // first three; ssb-dbgen then moves the first to p_color
        derive(stream::<COLORS>(P_NAME), |x| {
            let [c, a, b] = first_three(x);
            ((a * COLORS + b) as u16, c as u8)
        }),
        derive((ints(P_MFG, 1, 5), ints(P_CAT, 1, 5)), |(&m, &c)| {
            ((m - 1) * 5 + c - 1) as u8
        }),
        derive(ints(P_BRND, 0, 39), |&b| b as u8),
        derive(stream::<1>(P_TYPE), |&[x]| d.types.index(x) as u8),
        derive(stream::<1>(P_CNTR), |&[x]| d.containers.index(x) as u8),
        ints(P_SIZE, 1, 50),
    )
}

/// The columns after `p_partkey`.
pub(crate) const PART_COLUMNS: [&str; 8] = [
    "p_name",
    "p_mfgr",
    "p_category",
    "p_brand1",
    "p_color",
    "p_type",
    "p_size",
    "p_container",
];

/// The columns after `p_partkey` of a part's codes.
pub(crate) type PartWords = (Word, Word, Word, Word, Word, Word, i32, Word);

#[inline(always)]
pub(crate) fn part_words(d: &Dict, code: PartCode) -> PartWords {
    let ((name, color), category, brand, kind, container, size) = code;
    let category = category as usize;
    (
        Word(d.names[name as usize]),
        Word(d.mfgrs[category / 5]),
        Word(d.categories[category]),
        Word(d.brands[category * 40 + brand as usize]),
        Word(d.colors[color as usize]),
        Word(d.types.views[kind as usize]),
        size,
        Word(d.containers.views[container as usize]),
    )
}

fn part(d: &'static Dict, s: &Scale) -> Arc<dyn TableGen> {
    table(
        TableName::Part,
        s.parts,
        (
            named("p_partkey", key()),
            named_all(
                &PART_COLUMNS,
                derive(part_codes(d), move |((&n, &c), &cat, &b, &k, &ct, &sz)| {
                    part_words(d, ((n, c), cat, b, k, ct, sz))
                }),
            ),
        ),
    )
}

/// The first three entries of dbgen's shuffle of `0..92` by these draws
/// (`permute`: for each `i`, swap `i` with a uniform position).
#[inline]
fn first_three(draws: &[u32; COLORS]) -> [usize; 3] {
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
fn address(row: &[u32; ADDR_DRAWS], out: &mut Vec<u8>) {
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

/// Append a phone number: the nation's country code, then three draws.
#[inline]
fn phone(nation: i32, draws: [u32; 3], out: &mut Vec<u8>) {
    let a = 100 + scale(draws[0], 900);
    let b = 100 + scale(draws[1], 900);
    let c = 1000 + scale(draws[2], 9000);
    let cc = 10 + nation as u32;
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Party {
    Supplier,
    Customer,
}

impl Party {
    fn table(self) -> TableName {
        match self {
            Party::Supplier => TableName::Supplier,
            Party::Customer => TableName::Customer,
        }
    }

    fn rows(self, s: &Scale) -> u64 {
        match self {
            Party::Supplier => s.suppliers,
            Party::Customer => s.customers,
        }
    }

    fn prefix(self) -> &'static [u8] {
        match self {
            Party::Supplier => b"Supplier#",
            Party::Customer => b"Customer#",
        }
    }

    /// Address and nation streams.
    fn seeds(self) -> (i64, i64) {
        match self {
            Party::Supplier => (S_ADDR, S_NTRG),
            Party::Customer => (C_ADDR, C_NTRG),
        }
    }

    /// Where its rows start in the city and phone streams: customers draw
    /// where the suppliers left them.
    fn shared(self, s: &Scale) -> u64 {
        match self {
            Party::Supplier => 0,
            Party::Customer => s.suppliers,
        }
    }

    /// Column names, the key first.
    pub(crate) fn columns(self) -> [&'static str; 8] {
        match self {
            Party::Supplier => [
                "s_suppkey",
                "s_name",
                "s_address",
                "s_city",
                "s_nation",
                "s_region",
                "s_phone",
                "",
            ],
            Party::Customer => [
                "c_custkey",
                "c_name",
                "c_address",
                "c_city",
                "c_nation",
                "c_region",
                "c_phone",
                "c_mktsegment",
            ],
        }
    }
}

/// A supplier's or customer's nation (`0..25`) and city digit.
pub(crate) type PartyCode = (i32, i32);

/// The codes of every supplier or customer.
pub(crate) fn party_codes(p: Party, s: &Scale) -> impl Col<PartyCode> {
    (
        ints(p.seeds().1, 0, NATIONS as i32 - 1),
        at(ints(CITY, 0, 9), p.shared(s)),
    )
}

/// City, nation and region of a party's codes.
#[inline(always)]
pub(crate) fn party_words(d: &Dict, (nation, city): PartyCode) -> (Word, Word, Word) {
    let n = nation as usize;
    (
        Word(d.cities[n * 10 + city as usize]),
        Word(d.nations[n]),
        Word(d.regions[n]),
    )
}

/// A customer's market segment (an index into the distribution).
pub(crate) fn segment_codes(d: &'static Dict) -> impl Col<u8> {
    derive(stream::<1>(C_MSEG), |&[x]| d.segments.index(x) as u8)
}

fn party_name(p: Party) -> impl Col<Bytes> {
    let prefix = p.prefix();
    format(key(), move |&k, out| name(prefix, k as u64, out))
}

fn party_address(p: Party) -> impl Col<Bytes> {
    format(stream::<ADDR_DRAWS>(p.seeds().0), address)
}

fn party_phone(p: Party, s: &Scale) -> impl Col<Bytes> {
    format(
        (
            ints(p.seeds().1, 0, NATIONS as i32 - 1),
            at(stream::<3>(C_PHNE), p.shared(s)),
        ),
        |(&n, &x), out| phone(n, x, out),
    )
}

/// The name, address and phone number of every supplier or customer.
pub(crate) fn party_text(p: Party, s: &Scale) -> impl Col<(Bytes, Bytes, Bytes)> {
    (party_name(p), party_address(p), party_phone(p, s))
}

fn party(d: &'static Dict, s: &Scale, p: Party) -> Arc<dyn TableGen> {
    let c = p.columns();
    let words = derive(party_codes(p, s), move |(&n, &c)| party_words(d, (n, c)));
    let row = (
        named(c[0], key()),
        named(c[1], party_name(p)),
        named(c[2], party_address(p)),
        named_all(&c[3..6], words),
        named(c[6], party_phone(p, s)),
    );
    match p {
        Party::Supplier => table(p.table(), p.rows(s), row),
        Party::Customer => table(
            p.table(),
            p.rows(s),
            (
                row,
                named(
                    c[7],
                    derive(segment_codes(d), |&i| Word(d.segments.views[i as usize])),
                ),
            ),
        ),
    }
}

// ---------------------------------------------------------------------------
// date

fn date(d: &'static Dict, s: &Scale) -> Arc<dyn TableGen> {
    let t = |i: usize| d.days[i];
    // ssb-dbgen numbers weekdays from Sunday = 1, but computes the number
    // as `(tm_wday + 1) % 7 + 1` and names the day from it: Wednesday is 5,
    // "Thursday"
    let dnw = move |i: usize| (t(i).wday + 1) % 7 + 1;
    let flag = move |b: bool| Word(d.flags[b as usize]);
    table(
        TableName::Date,
        s.dates,
        (
            (
                named("d_datekey", day(move |i| t(i).key())),
                named("d_date", day(|i| Word(d.day_names[i]))),
                named(
                    "d_dayofweek",
                    day(move |i| Word(d.weekdays[dnw(i) as usize - 1])),
                ),
                named(
                    "d_month",
                    day(move |i| Word(d.months[t(i).month as usize - 1])),
                ),
                named("d_year", day(move |i| t(i).year)),
                named("d_yearmonthnum", day(move |i| t(i).year * 100 + t(i).month)),
                named(
                    "d_yearmonth",
                    day(move |i| {
                        let t = t(i);
                        Word(d.year_months[((t.year - 1992) * 12 + t.month - 1) as usize])
                    }),
                ),
                named("d_daynuminweek", day(dnw)),
                named("d_daynuminmonth", day(move |i| t(i).mday)),
            ),
            (
                named("d_daynuminyear", day(move |i| t(i).yday)),
                named("d_monthnuminyear", day(move |i| t(i).month)),
                named("d_weeknuminyear", day(move |i| t(i).yday / 7 + 1)),
                named(
                    "d_sellingseason",
                    day(move |i| Word(d.seasons[season(t(i).month, t(i).mday)])),
                ),
                named("d_lastdayinweekfl", day(move |i| flag(dnw(i) == 7))),
                named(
                    "d_lastdayinmonthfl",
                    day(move |i| flag(t(i).mday == days_in_month(t(i).year, t(i).month))),
                ),
                named(
                    "d_holidayfl",
                    day(move |i| flag(holiday(t(i).month, t(i).mday))),
                ),
                named(
                    "d_weekdayfl",
                    day(move |i| flag(dnw(i) != 1 && dnw(i) != 7)),
                ),
            ),
        ),
    )
}

// ---------------------------------------------------------------------------
// lineorder

/// `lineorder`'s columns.
pub(crate) const LINEORDER: [&str; 17] = [
    "lo_orderkey",
    "lo_linenumber",
    "lo_custkey",
    "lo_partkey",
    "lo_suppkey",
    "lo_orderdate",
    "lo_orderpriority",
    "lo_shippriority",
    "lo_quantity",
    "lo_extendedprice",
    "lo_ordtotalprice",
    "lo_discount",
    "lo_revenue",
    "lo_supplycost",
    "lo_tax",
    "lo_commitdate",
    "lo_shipmode",
];

/// One `lineorder` row. Integers are `Int32` (`lo_orderkey`, which passes
/// 2^31 from SF 358, is `Int64`), dates `Int32` `YYYYMMDD` as dbgen prints
/// them.
pub(crate) type Line = (
    (i64, i32, i32, i32, i32, i32, Word, i32),
    (i32, i32, i32, i32, i32, i32, i32, i32, Word),
);

/// An order's own values (its number, date offset, customer, priority
/// draw and line count), then the line streams, 7 slots each: part,
/// supplier, quantity, discount, tax, commit delay and ship mode draw.
pub(crate) type Order = (
    (i64, i32, i32, [u32; 1], i32),
    (
        [i32; LINES],
        [i32; LINES],
        [i32; LINES],
        [i32; LINES],
        [i32; LINES],
        [i32; LINES],
        [u32; LINES],
    ),
);

/// Every order's streams. The order streams start at row `dates`.
pub(crate) fn orders(s: &Scale) -> impl Col<Order> {
    let cmax = s.customers as i32;
    let custkey = derive(ints(O_CKEY, 1, cmax), move |&c| {
        let mut c = c;
        // a third of customers have no orders: step off multiples of 3,
        // alternating up and down
        let mut delta = 1;
        while c % 3 == 0 {
            c = (c + delta).min(cmax);
            delta = -delta;
        }
        c
    });
    let o = s.dates;
    (
        (
            range(1i64, 1),
            at(ints(O_ODATE, 0, ODATE_MAX), o),
            at(custkey, o),
            at(stream::<1>(O_PRIO), o),
            at(ints(O_LCNT, 1, LINES as i32), o),
        ),
        (
            Ints::new(L_PKEY, 1, s.part_keys as i32),
            Ints::new(L_SKEY, 1, s.suppliers as i32),
            Ints::new(L_QTY, 1, 50),
            Ints::new(L_DCNT, 0, 10),
            Ints::new(L_TAX, 0, 8),
            Ints::new(L_CDTE, CDATE_MIN, CDATE_MAX),
            stream::<LINES>(L_SMODE),
        ),
    )
}

/// `lineorder`'s lines of a block of orders, written into its columns.
///
/// A column at a time, as dbgen's line streams are laid out: every order
/// reserves 7 slots of each, and an order of `k` lines uses the first `k`.
/// So the slot (`order * 7 + line`) and order of every line are listed
/// once, and each column is then one pass gathering its values through
/// that list.
pub(crate) fn lines(d: &'static Dict) -> impl Fn(&Columns<Order>, &mut Columns<Line>) + Clone {
    move |src, out| {
        let ((index, odate, cust, prio, lcnt), (pk, sk, qty, disc, tax, cd, mode)) = src;
        let lines: usize = lcnt.iter().map(|&c| c as usize).sum();
        assert!(
            lcnt.len() * LINES <= u32::MAX as usize,
            "a block of {} orders is too many to index",
            lcnt.len()
        );
        // each order writes all 7 entries and keeps `line count`, so there
        // is no branch per line
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
        let line = |v: &Vec<[i32; LINES]>| -> Vec<i32> {
            let v = v.as_flattened();
            slot.iter().map(|&j| v[j as usize]).collect()
        };
        let per_order = |v: &[i32]| -> Vec<i32> { order.iter().map(|&o| v[o as usize]).collect() };
        let (pkey, qty, disc, tax) = (line(pk), line(qty), line(disc), line(tax));
        // every product fits in 31 bits: prices are under 210,000 cents, so
        // `price * 50 * 100` and `revenue * 108` are under 2^31
        let price: Vec<i32> = pkey.iter().map(|&p| part_price(p as i64) as i32).collect();
        let eprice: Vec<i32> = price.iter().zip(&qty).map(|(p, q)| p * q).collect();
        let revenue: Vec<i32> = eprice
            .iter()
            .zip(&disc)
            .map(|(e, dc)| e * (100 - dc) / 100)
            .collect();
        let mut total = vec![0i32; lcnt.len()];
        for ((&o, &r), &t) in order.iter().zip(&revenue).zip(&tax) {
            total[o as usize] += r * (100 + t) / 100;
        }
        let date = |o: u32| odate[o as usize] as usize;
        let (cd, mode) = (cd.as_flattened(), mode.as_flattened());
        let (
            (o_key, o_line, o_cust, o_part, o_supp, o_date, o_prio, o_ship),
            (o_qty, o_eprice, o_total, o_disc, o_revenue, o_cost, o_tax, o_commit, o_mode),
        ) = out;
        o_key.extend(order.iter().map(|&o| order_key(index[o as usize])));
        o_line.extend(slot.iter().map(|&j| (j % LINES as u32) as i32 + 1));
        o_cust.extend(per_order(cust));
        o_supp.extend(line(sk));
        o_date.extend(order.iter().map(|&o| d.keys[date(o)]));
        o_prio.push_n(
            order
                .iter()
                .map(|&o| Word(d.priorities.view(prio[o as usize][0]))),
        );
        o_ship.resize(o_ship.len() + lines, 0);
        o_cost.extend(price.iter().map(|p| 6 * p / 10));
        o_total.extend(per_order(&total));
        o_commit.extend(
            slot.iter()
                .zip(&order)
                .map(|(&j, &o)| d.keys[date(o) + cd[j as usize] as usize]),
        );
        o_mode.push_n(slot.iter().map(|&j| Word(d.modes.view(mode[j as usize]))));
        for (col, v) in [
            (o_part, pkey),
            (o_qty, qty),
            (o_eprice, eprice),
            (o_disc, disc),
            (o_revenue, revenue),
            (o_tax, tax),
        ] {
            append(col, v);
        }
    }
}

/// `v` appended to `col`, moved if `col` is empty.
fn append<T>(col: &mut Vec<T>, v: Vec<T>) {
    if col.is_empty() {
        *col = v;
    } else {
        col.extend(v);
    }
}
