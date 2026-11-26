//! The eight TPC-H tables, each one populate [`Distribution`].
//!
//! A table is a tuple of named columns, each a distribution over source
//! rows (orders for `lineitem`, parts for `partsupp`). Its schema is read
//! off that expression: the names from [`named`], the types from the
//! columns the values are collected into
//! ([`schema_of`](populate_sink_arrow::schema_of)).
//!
//! Every column draws from its own dbgen stream ([`Lcg`]), which reserves
//! a fixed number of draws per row: [`draws`] makes such a stream a leaf,
//! collected by one vectorised fill and skipped by an `O(log n)` jump, so
//! any range of rows starts where the sequential generator would be. The
//! row logic is dbgen's, as ported by tpchgen (`tpchgen::generators`); the
//! tests check every table against tpchgen-arrow's batches.
//!
//! - Columns computed from others are [`derive()`]s of the streams they read
//!   (`o_totalprice` from the order's line streams).
//! - `partsupp` is [`children`] of parts: 4 per part, each stream
//!   reserving 4 draws per part.
//! - `lineitem` is a [`flat_map`] over orders: every line stream reserves
//!   7 draws per order (the most lines an order has) and an order with `k`
//!   lines writes its first `k`, its line columns sharing the work that
//!   several of them read (a line's ship date decides its status, return
//!   flag and receipt date).

#[cfg(feature = "formats")]
use crate::TableName;
use crate::cols::{Bytes, Comment, CommentLen, Word};
use crate::text::{Dicts, Text};
#[cfg(feature = "formats")]
use arrow_schema::SchemaRef;
use populate_builtins::{
    Draws, children, constant, derive, draws, draws_reserving, flat_map, format, named, named_all,
    range,
};
use populate_combinators::lehmer::{Int, Ints, Lcg, bound, bounded};
use populate_combinators::views::Picks;
use populate_core::{Children, Date32, Decimal, Dist, Padded, Sink};
#[cfg(feature = "formats")]
use populate_core::{Columns, Distribution};
#[cfg(feature = "formats")]
use populate_sink_arrow::IntoArrowColumns;
#[cfg(feature = "formats")]
use populate_table::DynTable;
use std::iter::repeat_n;
use std::sync::LazyLock;
use tpchgen::dates::{MIN_GENERATE_DATE, TOTAL_DATE_RANGE, TPCHDate};

/// Draws per order reserved by the line streams: the most lines an order
/// has.
const LINES: usize = 7;
/// An order's line slots: one value per line, `LINES` reserved.
type Slots<T> = [T; LINES];
/// Unix day of dbgen's date `MIN_GENERATE_DATE` (1992-01-01).
const EPOCH: i32 = TPCHDate::UNIX_EPOCH_OFFSET;
/// dbgen's nations.
const NATIONS: i32 = 25;

/// TPC-H's decimals: `Decimal128(15, 2)`, in cents.
pub type Dec = Decimal<15, 2>;

/// `v` cents.
#[inline(always)]
fn dec(v: i64) -> Dec {
    Decimal(v as i128)
}

/// Last generated date (as dbgen numbers them) that is in the past
/// (`TPCHDate::is_in_past`: on or before 1995-06-17).
static PAST: LazyLock<i32> = LazyLock::new(|| {
    let dates = MIN_GENERATE_DATE..MIN_GENERATE_DATE + TOTAL_DATE_RANGE;
    let last = dates
        .clone()
        .filter(|&d| TPCHDate::is_in_past(d))
        .max()
        .expect("a date in the past");
    assert!(
        dates
            .into_iter()
            .all(|d| TPCHDate::is_in_past(d) == (d <= last))
    );
    last
});

/// Scale-factor-dependent bounds, as tpchgen computes them.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Scale {
    pub(crate) sf: f64,
    /// Parts (`p_partkey` runs `1..=parts`).
    pub(crate) parts: i64,
    pub(crate) suppliers: i64,
    pub(crate) customers: i64,
    pub(crate) orders: i64,
    clerks: i32,
}

impl Scale {
    pub(crate) fn new(sf: f64) -> Self {
        Scale {
            sf,
            parts: (200_000.0 * sf) as i64,
            suppliers: (10_000.0 * sf) as i64,
            customers: (150_000.0 * sf) as i64,
            orders: (1_500_000.0 * sf) as i64,
            clerks: (sf * 1000.0).max(1000.0) as i32,
        }
    }
}

// ---------------------------------------------------------------------------
// Leaves

/// The stream seeded `seed`, `K` draws per row.
fn stream<const K: usize>(seed: i64) -> Draws<Lcg, K> {
    draws(Lcg::new(seed))
}

/// `lo..=hi`, one draw per row.
fn ints(seed: i64, lo: i32, hi: i32) -> impl Dist<i32> {
    Int(Ints::new(seed, lo, hi))
}

/// A value of `dist`, one draw per row.
fn pick(dist: &'static Picks, seed: i64) -> impl Dist<Word> {
    derive(stream::<1>(seed), move |&[x]| Word::new(dist.view(x)))
}

/// A comment of average length `avg`, two draws per row.
fn comment(seed: i64, avg: f64) -> impl Dist<Comment> {
    let c = CommentLen::new(avg);
    derive(stream::<2>(seed), move |&[a, b]| c.make(a, b))
}

/// The 1-based row number: the key of most tables.
fn key() -> impl Dist<i64> {
    range(1i64, 1)
}

/// A nation, one draw per row (supplier's and customer's `nationkey`, and
/// the country code of their phone numbers).
fn nation(seed: i64) -> impl Dist<i64> {
    derive(ints(seed, 0, NATIONS - 1), |&n| n as i64)
}

/// A balance in `-999.99..=9999.99`.
fn balance(seed: i64) -> impl Dist<Dec> {
    derive(ints(seed, -99_999, 999_999), |&b| dec(b as i64))
}

/// `p_retailprice` of part `p`, in cents.
#[inline(always)]
fn part_price(p: i64) -> i64 {
    90_000 + (p / 10) % 20_001 + (p % 1000) * 100
}

/// Supplier `n` (0..4) of part `p`.
#[inline(always)]
fn part_supplier(p: i64, n: i64, suppliers: i64) -> i64 {
    (p + n * (suppliers / 4 + (p - 1) / suppliers)) % suppliers + 1
}

/// `o_orderkey` of the `i`th order (1-based): keys are sparse, 8 used of
/// every 32.
#[inline(always)]
pub(crate) fn order_key(i: i64) -> i64 {
    ((i >> 3) << 5) + (i & 7)
}

/// `prefix` then `v` zero-padded to 9 digits (`Supplier#000000001`).
fn numbered(prefix: &'static str, keys: impl Dist<i64>) -> impl Dist<Bytes> {
    format(keys, move |&k, out| {
        out.push_str(prefix);
        out.push_u64_padded(k as u64, 9);
    })
}

/// A `RandomAlphaNumeric` address (`avg` 25), 9 draws per row: a length
/// draw, then the characters, 5 per draw.
fn address(seed: i64) -> impl Dist<Bytes> {
    const CHARS: &[u8; 64] = b"0123456789abcdefghijklmnopqrstuvwxyz ABCDEFGHIJKLMNOPQRSTUVWXYZ,";
    format(stream::<9>(seed), |x, out| {
        let len = bound(x[0], 10, 40) as usize;
        let mut b = [b' '; 40];
        let mut idx = 0i64;
        for (i, c) in b[..len].iter_mut().enumerate() {
            if i % 5 == 0 {
                idx = bounded(x[1 + i / 5], 0, i32::MAX) as i64;
            }
            *c = CHARS[(idx & 0x3f) as usize];
            idx >>= 6;
        }
        out.push_padded(&Padded::ascii(b, len));
    })
}

/// A `RandomPhoneNumber`, `CC-LLL-LLL-LLLL`, 3 draws per row; the country
/// is the row's nation (the same stream as `nation`).
fn phone(nation_seed: i64, seed: i64) -> impl Dist<Bytes> {
    format(
        (nation(nation_seed), stream::<3>(seed)),
        |(&nation, &[a, b, c]), out| {
            out.push_u64_padded((10 + nation % 90) as u64, 2);
            out.push_ascii(b'-');
            out.push_u64_padded(bound(a, 100, 999) as u64, 3);
            out.push_ascii(b'-');
            out.push_u64_padded(bound(b, 100, 999) as u64, 3);
            out.push_ascii(b'-');
            out.push_u64_padded(bound(c, 1000, 9999) as u64, 4);
        },
    )
}

// ---------------------------------------------------------------------------
// Tables

#[cfg(feature = "formats")]
fn table<D>(t: TableName, rows: u64, dist: D) -> DynTable
where
    D: Distribution + Clone + Send + Sync + 'static,
    Columns<D::Item>: IntoArrowColumns,
{
    DynTable::new(t.as_str(), rows, dist)
}

/// Table `t` at scale `s`, `rows` source rows.
#[cfg(feature = "formats")]
pub(crate) fn generator(t: TableName, s: &Scale, rows: u64) -> DynTable {
    match t {
        TableName::Nation => nation_table(rows),
        TableName::Region => region_table(rows),
        TableName::Part => part(rows),
        TableName::Supplier => supplier(rows),
        TableName::PartSupp => partsupp(s, rows),
        TableName::Customer => customer(rows),
        TableName::Orders => orders(s, rows),
        TableName::LineItem => lineitem(s, rows),
    }
}

/// Every table's schema, read off its distribution (no rows generated).
#[cfg(feature = "formats")]
pub(crate) static SCHEMAS: LazyLock<Vec<SchemaRef>> = LazyLock::new(|| {
    let s = Scale::new(1.0);
    TableName::ALL
        .iter()
        .map(|&t| generator(t, &s, 0).schema().clone())
        .collect()
});

pub(crate) fn nation_table_dist() -> impl Dist<Nation> + use<> {
    let d = Dicts::get();
    (
        named("n_nationkey", range(0i64, 1)),
        named(
            "n_name",
            derive(range(0usize, 1), |&i| Word::new(d.nations.views[i])),
        ),
        named(
            "n_regionkey",
            derive(range(0usize, 1), |&i| d.nation_regions[i]),
        ),
        named("n_comment", comment(606179079, 72.0)),
    )
}

pub(crate) fn region_table_dist() -> impl Dist<Region> + use<> {
    let d = Dicts::get();
    (
        named("r_regionkey", range(0i64, 1)),
        named(
            "r_name",
            derive(range(0usize, 1), |&i| Word::new(d.regions.views[i])),
        ),
        named("r_comment", comment(1500869201, 72.0)),
    )
}

/// `p_name`: dbgen's `RandomStringSequence`, the first 5 of all colors
/// after a partial shuffle. The stream reserves one draw per color and
/// uses 5.
fn part_name() -> impl Dist<Bytes> {
    let colors = &Dicts::get().colors;
    format(
        draws_reserving::<_, 5>(Lcg::new(709314158), colors.values.len() as u64),
        move |x, out| {
            let mut order = [0u8; 256];
            let all = colors.values.len();
            for (k, v) in order[..all].iter_mut().enumerate() {
                *v = k as u8;
            }
            for (pos, &x) in x.iter().enumerate() {
                let swap = bound(x, pos as i32, all as i32 - 1) as usize;
                order.swap(pos, swap);
            }
            for (k, &c) in order[..5].iter().enumerate() {
                if k > 0 {
                    out.push_ascii(b' ');
                }
                out.push_str(colors.values[c as usize]);
            }
        },
    )
}

pub(crate) fn part_dist() -> impl Dist<Part> + use<> {
    let d = Dicts::get();
    let mfgr = || ints(1, 1, 5);
    (
        named("p_partkey", key()),
        named("p_name", part_name()),
        named(
            "p_mfgr",
            format::<_, Bytes, _>(mfgr(), |&m, out| {
                out.push_str("Manufacturer#");
                out.push_u64_padded(m as u64, 1);
            }),
        ),
        named(
            "p_brand",
            format::<_, Bytes, _>((mfgr(), ints(46831694, 1, 5)), |(&m, &b), out| {
                out.push_str("Brand#");
                out.push_u64_padded((m * 10 + b) as u64, 1);
            }),
        ),
        named("p_type", pick(&d.types, 1841581359)),
        named("p_size", ints(1193163244, 1, 50)),
        named("p_container", pick(&d.containers, 727633698)),
        named("p_retailprice", derive(key(), |&p| dec(part_price(p)))),
        named("p_comment", comment(804159733, 14.0)),
    )
}

pub(crate) fn supplier_dist() -> impl Dist<Supplier> + use<> {
    const BBB_BASE: &str = "Customer ";
    const COMPLAINTS: &str = "Complaints";
    const RECOMMENDS: &str = "Recommends";
    const BBB_LEN: usize = BBB_BASE.len() + COMPLAINTS.len();
    let len = CommentLen::new(63.0);
    let comments = format::<_, Bytes, _>(
        (
            stream::<2>(1341315363),
            stream::<1>(202794285),
            stream::<1>(263032577),
            stream::<1>(715851524),
            stream::<1>(753643799),
        ),
        move |(&[a, b], &[bbb], &[junk], &[offset], &[kind]), out| {
            let c = len.make(a, b);
            let pool = Text::get().pool;
            let c = &pool[c.off as usize..(c.off + c.len) as usize];
            if bound(bbb, 1, 10_000) > 10 {
                out.push_str(c);
                return;
            }
            // a Better Business Bureau complaint or recommendation, written
            // over the comment at a random place
            let noise = bound(junk, 0, (c.len() - BBB_LEN) as i32) as usize;
            let off = bound(offset, 0, (c.len() - (BBB_LEN + noise)) as i32) as usize;
            let kind = if bound(kind, 0, 100) < 50 {
                COMPLAINTS
            } else {
                RECOMMENDS
            };
            out.push_str(&c[..off]);
            out.push_str(BBB_BASE);
            out.push_str(&c[off + BBB_BASE.len()..off + BBB_BASE.len() + noise]);
            out.push_str(kind);
            out.push_str(&c[off + BBB_BASE.len() + noise + kind.len()..]);
        },
    );
    (
        named("s_suppkey", key()),
        named("s_name", numbered("Supplier#", key())),
        named("s_address", address(706178559)),
        named("s_nationkey", nation(110356601)),
        named("s_phone", phone(110356601, 884434366)),
        named("s_acctbal", balance(962338209)),
        named("s_comment", comments),
    )
}

pub(crate) fn customer_dist() -> impl Dist<Customer> + use<> {
    let d = Dicts::get();
    (
        named("c_custkey", key()),
        named("c_name", numbered("Customer#", key())),
        named("c_address", address(881155353)),
        named("c_nationkey", nation(1489529863)),
        named("c_phone", phone(1489529863, 1521138112)),
        named("c_acctbal", balance(298370230)),
        named("c_mktsegment", pick(&d.segments, 1140279430)),
        named("c_comment", comment(1335826707, 73.0)),
    )
}

/// Rows of `partsupp` per part.
pub(crate) const SUPPLIERS_PER_PART: u64 = 4;
const PER_PART: usize = SUPPLIERS_PER_PART as usize;

/// `partsupp`, 4 rows per part: every column is [`children`] of the parts,
/// its stream reserving 4 draws per part (8 for the comment).
pub(crate) fn partsupp_dist(s: &Scale) -> impl Dist<PartSupp> + use<> {
    let suppliers = s.suppliers;
    let per_part = || constant(PER_PART);
    let len = CommentLen::new(124.0);
    (
        named(
            "ps_partkey",
            children(per_part(), derive(key(), |&p| [p; PER_PART])),
        ),
        named(
            "ps_suppkey",
            children(
                per_part(),
                derive(key(), move |&p| {
                    std::array::from_fn::<_, PER_PART, _>(|n| part_supplier(p, n as i64, suppliers))
                }),
            ),
        ),
        named(
            "ps_availqty",
            children(per_part(), Ints::<PER_PART>::new(1671059989, 1, 9999)),
        ),
        named(
            "ps_supplycost",
            children(
                per_part(),
                derive(Ints::<PER_PART>::new(1051288424, 100, 100_000), |x| {
                    x.map(|x| dec(x as i64))
                }),
            ),
        ),
        named(
            "ps_comment",
            children(
                per_part(),
                derive(stream::<{ 2 * PER_PART }>(1961692154), move |x| {
                    std::array::from_fn::<_, PER_PART, _>(|i| len.make(x[2 * i], x[2 * i + 1]))
                }),
            ),
        ),
    )
}

// ---------------------------------------------------------------------------
// orders and lineitem

/// An order's date, as dbgen numbers dates.
fn order_date() -> impl Dist<i32> {
    ints(
        1066728069,
        MIN_GENERATE_DATE,
        MIN_GENERATE_DATE + TOTAL_DATE_RANGE - 151 - 1,
    )
}

/// An order's number of lines.
fn line_count() -> impl Dist<usize> {
    derive(ints(1434868289, 1, LINES as i32), |&n| n as usize)
}

/// A line stream's values in `lo..=hi`, one per line slot of an order.
fn slots(seed: i64, lo: i32, hi: i32) -> impl Dist<Slots<i32>> {
    Ints::new(seed, lo, hi)
}

/// Quantity, discount, tax, part and ship delay of each line slot.
type LineValues = (Slots<i32>, Slots<i32>, Slots<i32>, Slots<i32>, Slots<i32>);

/// The line streams `orders` and `lineitem` both read: quantity, discount,
/// tax, part and ship delay.
fn line_values(s: &Scale) -> impl Dist<LineValues> + use<> {
    (
        slots(209208115, 1, 50),
        slots(554590007, 0, 10),
        slots(721958466, 0, 8),
        slots(1808217256, 1, s.parts as i32),
        slots(1769349045, 1, 121),
    )
}

/// A dbgen date as a `Date32`.
#[inline(always)]
fn date(d: i32) -> Date32 {
    Date32(d - MIN_GENERATE_DATE + EPOCH)
}

pub(crate) fn orders_dist(s: &Scale) -> impl Dist<Orders> + use<> {
    let d = Dicts::get();
    let (customers, past) = (s.customers, *PAST);
    let custkey = derive(stream::<1>(851767375), move |&[x]| {
        // skipping every third customer (who has no orders)
        let mut c = bound(x, 1, customers as i32) as i64;
        let mut delta = 1;
        while c % 3 == 0 {
            c = (c + delta).min(customers);
            delta = -delta;
        }
        c
    });
    let status_and_total = derive(
        (order_date(), line_count(), line_values(s)),
        move |(&date, &lines, (qty, disc, tax, part, ship))| {
            let (mut total, mut shipped) = (0i64, 0);
            for j in 0..lines {
                let (q, d, t) = (qty[j] as i64, disc[j] as i64, tax[j] as i64);
                let price = part_price(part[j] as i64) * q;
                total += ((price * (100 - d) / 100) * (100 + t)) / 100;
                shipped += (ship[j] + date <= past) as usize;
            }
            let status = if shipped == lines {
                b"F"
            } else if shipped > 0 {
                b"P"
            } else {
                b"O"
            };
            (Word::lit(status), dec(total))
        },
    );
    (
        named("o_orderkey", derive(key(), |&i| order_key(i))),
        named("o_custkey", custkey),
        named_all(&["o_orderstatus", "o_totalprice"], status_and_total),
        named("o_orderdate", derive(order_date(), |&d| date(d))),
        named("o_orderpriority", pick(&d.priorities, 591449447)),
        named(
            "o_clerk",
            numbered(
                "Clerk#",
                derive(ints(1171034773, 1, s.clerks), |&c| c as i64),
            ),
        ),
        named("o_shippriority", constant(0i32)),
        named("o_comment", comment(276090261, 49.0)),
    )
}

/// One `lineitem` row.
pub type Line = (
    (i64, i64, i64, i32, Dec, Dec, Dec, Dec),
    (Word, Word, Date32, Date32, Date32, Word, Word, Comment),
);

/// `lineitem`: the lines of each order, from streams that reserve 7 draws
/// per order.
pub(crate) fn lineitem_dist(s: &Scale) -> impl Dist<LineItem> + use<> {
    let d = Dicts::get();
    let (suppliers, past) = (s.suppliers, *PAST);
    let len = CommentLen::new(27.0);
    let (n, f, o) = (Word::lit(b"N"), Word::lit(b"F"), Word::lit(b"O"));
    let per_order = (
        (derive(key(), |&i| order_key(i)), order_date(), line_count()),
        line_values(s),
        (
            slots(2095021727, 0, 3),
            slots(904914315, 30, 90),
            slots(373135028, 1, 30),
        ),
        (
            stream::<LINES>(717419739),
            stream::<LINES>(1371272478),
            stream::<LINES>(675466456),
            stream::<{ 2 * LINES }>(1095462486),
        ),
    );
    let lines = flat_map::<Children<Line>, _, _>(
        per_order,
        move |(
            (key, od, lines),
            (qty, disc, tax, part, ship),
            (supp, commit, receipt),
            (flag_x, instr_x, mode_x, cx),
        ),
              out| {
            let (key, od, lines) = (*key, *od, *lines);
            let shipped = ship.map(|delay| delay + od);
            let received: Slots<i32> = std::array::from_fn(|j| receipt[j] + shipped[j]);
            let (numbers, other) = &mut out.values;
            // Each append has an exact length, so capacity is checked once
            // per order and the column's line slots form one tight loop.
            numbers.0.extend(repeat_n(key, lines));
            numbers.1.extend(part[..lines].iter().map(|&p| p as i64));
            numbers.2.extend(
                (0..lines).map(|j| part_supplier(part[j] as i64, supp[j] as i64, suppliers)),
            );
            numbers.3.extend((1..=lines).map(|j| j as i32));
            numbers
                .4
                .extend(qty[..lines].iter().map(|&q| dec(q as i64 * 100)));
            numbers
                .5
                .extend((0..lines).map(|j| dec(part_price(part[j] as i64) * qty[j] as i64)));
            numbers
                .6
                .extend(disc[..lines].iter().map(|&v| dec(v as i64)));
            numbers
                .7
                .extend(tax[..lines].iter().map(|&v| dec(v as i64)));
            // Only received lines consume a return-flag draw.
            let mut returned = 0;
            other.0.push_n(received[..lines].iter().map(|&received| {
                if received <= past {
                    let x = flag_x[returned];
                    returned += 1;
                    Word::new(d.return_flags.view(x))
                } else {
                    n
                }
            }));
            other.1.push_n(
                shipped[..lines]
                    .iter()
                    .map(|&s| if s <= past { f } else { o }),
            );
            other.2.extend(shipped[..lines].iter().map(|&s| date(s)));
            other
                .3
                .extend(commit[..lines].iter().map(|&c| date(c + od)));
            other.4.extend(received[..lines].iter().map(|&r| date(r)));
            other.5.push_n(
                instr_x[..lines]
                    .iter()
                    .map(|&x| Word::new(d.instructions.view(x))),
            );
            other
                .6
                .push_n(mode_x[..lines].iter().map(|&x| Word::new(d.modes.view(x))));
            other
                .7
                .push_n((0..lines).map(|j| len.make(cx[2 * j], cx[2 * j + 1])));
        },
    )
    // 1 to 7 lines an order, 4 on average; a batch of 8Ki orders is within
    // 1% of the mean, so the line columns are allocated once
    .reserving(&[4.1]);
    named_all(
        &[
            "l_orderkey",
            "l_partkey",
            "l_suppkey",
            "l_linenumber",
            "l_quantity",
            "l_extendedprice",
            "l_discount",
            "l_tax",
            "l_returnflag",
            "l_linestatus",
            "l_shipdate",
            "l_commitdate",
            "l_receiptdate",
            "l_shipinstruct",
            "l_shipmode",
            "l_comment",
        ],
        lines,
    )
}

/// Native row shape for `Nation`.
pub type Nation = (i64, Word, i64, Comment);

/// Native row shape for `Region`.
pub type Region = (i64, Word, Comment);

/// Native row shape for `Part`.
pub type Part = (i64, Bytes, Bytes, Bytes, Word, i32, Word, Dec, Comment);

/// Native row shape for `Supplier`.
pub type Supplier = (i64, Bytes, Bytes, i64, Bytes, Dec, Bytes);

/// Native row shape for `Customer`.
pub type Customer = (i64, Bytes, Bytes, i64, Bytes, Dec, Word, Comment);

/// Native row shape for `PartSupp`.
pub type PartSupp = (
    Children<i64>,
    Children<i64>,
    Children<i32>,
    Children<Dec>,
    Children<Comment>,
);

/// Native row shape for `Orders`.
pub type Orders = (i64, i64, (Word, Dec), Date32, Word, Bytes, i32, Comment);

/// Native row shape for `LineItem`.
pub type LineItem = Children<Line>;

#[cfg(feature = "formats")]
fn nation_table(rows: u64) -> DynTable {
    table(TableName::Nation, rows, nation_table_dist())
}
#[cfg(feature = "formats")]
fn region_table(rows: u64) -> DynTable {
    table(TableName::Region, rows, region_table_dist())
}
#[cfg(feature = "formats")]
fn part(rows: u64) -> DynTable {
    table(TableName::Part, rows, part_dist())
}
#[cfg(feature = "formats")]
fn supplier(rows: u64) -> DynTable {
    table(TableName::Supplier, rows, supplier_dist())
}
#[cfg(feature = "formats")]
fn customer(rows: u64) -> DynTable {
    table(TableName::Customer, rows, customer_dist())
}
#[cfg(feature = "formats")]
fn partsupp(s: &Scale, rows: u64) -> DynTable {
    table(TableName::PartSupp, rows, partsupp_dist(s))
}
#[cfg(feature = "formats")]
fn orders(s: &Scale, rows: u64) -> DynTable {
    table(TableName::Orders, rows, orders_dist(s))
}
#[cfg(feature = "formats")]
fn lineitem(s: &Scale, rows: u64) -> DynTable {
    table(TableName::LineItem, rows, lineitem_dist(s))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn helpers_match_tpchgen() {
        use tpchgen::generators::{
            OrderGenerator, PartGeneratorIterator, PartSuppGeneratorIterator,
        };
        for p in (1..2_000_000).step_by(7919) {
            assert_eq!(
                part_price(p),
                PartGeneratorIterator::calculate_part_price(p)
            );
            for n in 0..4 {
                for sf in [0.01, 1.0, 10.0, 300.0] {
                    let sc = Scale::new(sf);
                    assert_eq!(
                        part_supplier(p, n, sc.suppliers),
                        PartSuppGeneratorIterator::select_part_supplier(p, n, sf)
                    );
                }
            }
            assert_eq!(order_key(p), OrderGenerator::make_order_key(p));
        }
    }
}
