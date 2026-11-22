//! The eight TPC-H tables, generated straight into Arrow columns.
//!
//! Each generator takes a range of source rows (orders for `lineitem`,
//! parts for `partsupp`) and positions every stream at the range's first
//! row, so any range is exactly the rows the sequential generator makes
//! there. The row logic is dbgen's, as ported by tpchgen
//! (`tpchgen::generators`); the tests check every table against
//! tpchgen-arrow's batches.
//!
//! The order-level streams (`orders` and `lineitem`) are generated a whole
//! block at a time with [`draws`]: each stream reserves 7 draws per order,
//! so the draws of a block are one dense run, made by independent LCG
//! lanes, and each order keeps the first `line_count` of its 7.

use crate::rng::{Rng, bound_all, bounded, draws};
use crate::text::{Text, Views};
use arrow_array::types::Decimal128Type;
use arrow_array::{
    ArrayRef, Date32Array, Int32Array, Int64Array, PrimitiveArray, RecordBatch, StringViewArray,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use std::sync::{Arc, LazyLock};
use tpchgen::dates::{MIN_GENERATE_DATE, TOTAL_DATE_RANGE, TPCHDate};

/// Draws per row reserved by the order-level streams: the most lines an
/// order has.
const LINES: usize = 7;
/// Unix day of dbgen's date `MIN_GENERATE_DATE` (1992-01-01).
const EPOCH: i32 = TPCHDate::UNIX_EPOCH_OFFSET;

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

/// dbgen's comment lengths for an average of `avg`: `0.4x..=1.6x`.
fn comment_bounds(avg: f64) -> (i32, i32) {
    ((avg * 0.4) as i32, (avg * 1.6) as i32)
}

/// A `RandomText` stream: two draws per comment, offset then length.
#[derive(Clone, Copy)]
struct Comment {
    seed: i64,
    min: i32,
    max: i32,
}

impl Comment {
    fn new(seed: i64, avg: f64) -> Self {
        let (min, max) = comment_bounds(avg);
        Comment { seed, min, max }
    }

    /// The view of the comment that draws `x0`, `x1` make.
    #[inline(always)]
    fn view(&self, t: &Text, x0: u32, x1: u32) -> u128 {
        let off = bounded(x0, 0, t.pool.len() as i32 - self.max);
        let len = bounded(x1, self.min, self.max);
        t.pool_view(off as u32, len as u32)
    }

    /// The comment the next two draws of `r` make, as bytes.
    #[inline(always)]
    fn next<'t>(&self, t: &'t Text, r: &mut Rng) -> &'t [u8] {
        let off = r.next_int(0, t.pool.len() as i32 - self.max) as usize;
        let len = r.next_int(self.min, self.max) as usize;
        &t.pool[off..off + len]
    }

    /// Serial stream for `per_row` comments per row, at row `row`.
    fn rng(&self, per_row: u32, row: u64) -> Rng {
        Rng::at(self.seed, 2 * per_row, row)
    }
}

// ---------------------------------------------------------------------------
// Schemas: tpchgen-arrow's.

fn field(name: &str, t: DataType) -> Field {
    Field::new(name, t, false)
}

const DEC: DataType = DataType::Decimal128(15, 2);

macro_rules! schema {
    ($($name:literal : $t:expr),* $(,)?) => {
        Arc::new(Schema::new(vec![$(field($name, $t)),*]))
    };
}

pub(crate) static NATION: LazyLock<SchemaRef> = LazyLock::new(|| {
    use DataType::*;
    schema!("n_nationkey": Int64, "n_name": Utf8View, "n_regionkey": Int64, "n_comment": Utf8View)
});

pub(crate) static REGION: LazyLock<SchemaRef> = LazyLock::new(|| {
    use DataType::*;
    schema!("r_regionkey": Int64, "r_name": Utf8View, "r_comment": Utf8View)
});

pub(crate) static PART: LazyLock<SchemaRef> = LazyLock::new(|| {
    use DataType::*;
    schema!(
        "p_partkey": Int64, "p_name": Utf8View, "p_mfgr": Utf8View, "p_brand": Utf8View,
        "p_type": Utf8View, "p_size": Int32, "p_container": Utf8View,
        "p_retailprice": DEC, "p_comment": Utf8View,
    )
});

pub(crate) static SUPPLIER: LazyLock<SchemaRef> = LazyLock::new(|| {
    use DataType::*;
    schema!(
        "s_suppkey": Int64, "s_name": Utf8View, "s_address": Utf8View, "s_nationkey": Int64,
        "s_phone": Utf8View, "s_acctbal": DEC, "s_comment": Utf8View,
    )
});

pub(crate) static PARTSUPP: LazyLock<SchemaRef> = LazyLock::new(|| {
    use DataType::*;
    schema!(
        "ps_partkey": Int64, "ps_suppkey": Int64, "ps_availqty": Int32,
        "ps_supplycost": DEC, "ps_comment": Utf8View,
    )
});

pub(crate) static CUSTOMER: LazyLock<SchemaRef> = LazyLock::new(|| {
    use DataType::*;
    schema!(
        "c_custkey": Int64, "c_name": Utf8View, "c_address": Utf8View, "c_nationkey": Int64,
        "c_phone": Utf8View, "c_acctbal": DEC, "c_mktsegment": Utf8View, "c_comment": Utf8View,
    )
});

pub(crate) static ORDERS: LazyLock<SchemaRef> = LazyLock::new(|| {
    use DataType::*;
    schema!(
        "o_orderkey": Int64, "o_custkey": Int64, "o_orderstatus": Utf8View,
        "o_totalprice": DEC, "o_orderdate": Date32, "o_orderpriority": Utf8View,
        "o_clerk": Utf8View, "o_shippriority": Int32, "o_comment": Utf8View,
    )
});

pub(crate) static LINEITEM: LazyLock<SchemaRef> = LazyLock::new(|| {
    use DataType::*;
    schema!(
        "l_orderkey": Int64, "l_partkey": Int64, "l_suppkey": Int64, "l_linenumber": Int32,
        "l_quantity": DEC, "l_extendedprice": DEC, "l_discount": DEC, "l_tax": DEC,
        "l_returnflag": Utf8View, "l_linestatus": Utf8View, "l_shipdate": Date32,
        "l_commitdate": Date32, "l_receiptdate": Date32, "l_shipinstruct": Utf8View,
        "l_shipmode": Utf8View, "l_comment": Utf8View,
    )
});

// ---------------------------------------------------------------------------
// Column helpers.

fn i64s(v: Vec<i64>) -> ArrayRef {
    Arc::new(Int64Array::from(v))
}

fn i32s(v: Vec<i32>) -> ArrayRef {
    Arc::new(Int32Array::from(v))
}

fn dates(v: Vec<i32>) -> ArrayRef {
    Arc::new(Date32Array::from(v))
}

fn decimals(v: Vec<i128>) -> ArrayRef {
    Arc::new(
        PrimitiveArray::<Decimal128Type>::from(v)
            .with_precision_and_scale(15, 2)
            .expect("decimal(15, 2)"),
    )
}

fn strs(a: StringViewArray) -> ArrayRef {
    Arc::new(a)
}

fn batch(schema: &SchemaRef, cols: Vec<ArrayRef>) -> RecordBatch {
    RecordBatch::try_new(schema.clone(), cols).expect("columns match the schema")
}

/// Append `v` in decimal, zero-padded to at least `width` digits.
#[inline]
fn padded(out: &mut Vec<u8>, v: u64, width: usize) {
    let mut buf = [b'0'; 20];
    let mut i = 20;
    let mut v = v;
    while v > 0 {
        i -= 1;
        buf[i] = b'0' + (v % 10) as u8;
        v /= 10;
    }
    let i = i.min(20 - width);
    out.extend_from_slice(&buf[i..]);
}

/// `prefix` then `v` zero-padded to 9 digits (`Supplier#000000001`), as a
/// string of `out`.
#[inline]
fn numbered(out: &mut Views, prefix: &[u8], v: u64) {
    let start = out.data.len();
    out.data.extend_from_slice(prefix);
    padded(&mut out.data, v, 9);
    out.end_string(start);
}

/// A `RandomAlphaNumeric` address (`avg` 25): a length draw, then the
/// characters from a copy of the stream, 5 per draw.
#[inline]
fn address(out: &mut Views, r: &mut Rng) {
    const CHARS: &[u8; 64] = b"0123456789abcdefghijklmnopqrstuvwxyz ABCDEFGHIJKLMNOPQRSTUVWXYZ,";
    let len = r.next_int(10, 40) as usize;
    let mut chars = *r;
    let start = out.data.len();
    let mut idx = 0i64;
    for i in 0..len {
        if i % 5 == 0 {
            idx = chars.next_int(0, i32::MAX) as i64;
        }
        out.data.push(CHARS[(idx & 0x3f) as usize]);
        idx >>= 6;
    }
    out.end_string(start);
}

/// A `RandomPhoneNumber`: `CC-LLL-LLL-LLLL`, country from the nation.
#[inline]
fn phone(out: &mut Views, r: &mut Rng, nation: i64) {
    let start = out.data.len();
    padded(&mut out.data, (10 + nation % 90) as u64, 2);
    out.data.push(b'-');
    padded(&mut out.data, r.next_int(100, 999) as u64, 3);
    out.data.push(b'-');
    padded(&mut out.data, r.next_int(100, 999) as u64, 3);
    out.data.push(b'-');
    padded(&mut out.data, r.next_int(1000, 9999) as u64, 4);
    out.end_string(start);
}

// ---------------------------------------------------------------------------
// The small tables.

/// `nation` or `region` (all rows; `first`/`n` select a range).
fn named(t: &Text, region: bool, first: u64, n: u64) -> RecordBatch {
    let (dist, seed) = if region {
        (&t.regions, 1500869201)
    } else {
        (&t.nations, 606179079)
    };
    let comment = Comment::new(seed, 72.0);
    let mut r = comment.rng(1, first);
    let (mut keys, mut names, mut regions, mut comments) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    for i in first..first + n {
        keys.push(i as i64);
        names.push(dist.views[i as usize]);
        regions.push(t.nation_regions[i as usize]);
        let c = comment.next(t, &mut r);
        comments.push(crate::text::view(c, crate::text::POOL, offset_in(t, c)));
        r.row_finished();
    }
    let names = t.array(names, Vec::new());
    let comments = t.array(comments, Vec::new());
    if region {
        batch(&REGION, vec![i64s(keys), strs(names), strs(comments)])
    } else {
        batch(
            &NATION,
            vec![i64s(keys), strs(names), i64s(regions), strs(comments)],
        )
    }
}

/// Offset of `s` (a slice of the pool) in the pool.
fn offset_in(t: &Text, s: &[u8]) -> u32 {
    (s.as_ptr() as usize - t.pool.as_ptr() as usize) as u32
}

pub(crate) fn nation(t: &Text, first: u64, n: u64) -> RecordBatch {
    named(t, false, first, n)
}

pub(crate) fn region(t: &Text, first: u64, n: u64) -> RecordBatch {
    named(t, true, first, n)
}

// ---------------------------------------------------------------------------
// part

pub(crate) fn part(t: &Text, first: u64, n: u64) -> RecordBatch {
    let rows = n as usize;
    let colors = &t.colors;
    let mut name_r = Rng::at(709314158, colors.values.len() as u32, first);
    let mut mfgr_r = Rng::at(1, 1, first);
    let mut brand_r = Rng::at(46831694, 1, first);
    let mut type_r = Rng::at(1841581359, 1, first);
    let mut size_r = Rng::at(1193163244, 1, first);
    let mut cont_r = Rng::at(727633698, 1, first);
    let comment = Comment::new(804159733, 14.0);
    let mut comment_r = comment.rng(1, first);

    let mut keys = Vec::with_capacity(rows);
    let mut names = Views::with_capacity(rows, rows * 40);
    let mut mfgrs = Views::with_capacity(rows, rows * 14);
    let mut brands = Views::with_capacity(rows, 0);
    let mut types = Vec::with_capacity(rows);
    let mut sizes = Vec::with_capacity(rows);
    let mut containers = Vec::with_capacity(rows);
    let mut prices = Vec::with_capacity(rows);
    let mut comment_x = Vec::with_capacity(rows);
    let mut order: Vec<u8> = (0..colors.values.len() as u8).collect();
    for i in first..first + n {
        let key = i as i64 + 1;
        keys.push(key);
        // RandomStringSequence: shuffle the first 5 of all colors
        for (k, v) in order.iter_mut().enumerate() {
            *v = k as u8;
        }
        let last = order.len() as i32 - 1;
        for pos in 0..5 {
            let swap = name_r.next_int(pos, last) as usize;
            order.swap(pos as usize, swap);
        }
        let start = names.data.len();
        for (k, &c) in order[..5].iter().enumerate() {
            if k > 0 {
                names.data.push(b' ');
            }
            names
                .data
                .extend_from_slice(colors.values[c as usize].as_bytes());
        }
        names.end_string(start);

        let m = mfgr_r.next_int(1, 5);
        let start = mfgrs.data.len();
        mfgrs.data.extend_from_slice(b"Manufacturer#");
        padded(&mut mfgrs.data, m as u64, 1);
        mfgrs.end_string(start);
        let b = m * 10 + brand_r.next_int(1, 5);
        let start = brands.data.len();
        brands.data.extend_from_slice(b"Brand#");
        padded(&mut brands.data, b as u64, 1);
        brands.end_string(start);

        types.push(t.types.views[t.types.pick(&mut type_r)]);
        sizes.push(size_r.next_int(1, 50));
        containers.push(t.containers.views[t.containers.pick(&mut cont_r)]);
        prices.push(part_price(key) as i128);
        comment_x.push((comment_r.next_rand(), comment_r.next_rand()));
        for r in [
            &mut name_r,
            &mut mfgr_r,
            &mut brand_r,
            &mut type_r,
            &mut size_r,
            &mut cont_r,
            &mut comment_r,
        ] {
            r.row_finished();
        }
    }
    batch(
        &PART,
        vec![
            i64s(keys),
            strs(names.finish(t)),
            strs(mfgrs.finish(t)),
            strs(brands.finish(t)),
            strs(t.array(types, Vec::new())),
            i32s(sizes),
            strs(t.array(containers, Vec::new())),
            decimals(prices),
            strs(comment_views(t, &comment, &comment_x)),
        ],
    )
}

/// Views of the comments the draw pairs `x` make, in a pass of their own
/// (see `lineitem`).
fn comment_views(t: &Text, c: &Comment, x: &[(u32, u32)]) -> StringViewArray {
    t.array(
        x.iter().map(|&(a, b)| c.view(t, a, b)).collect(),
        Vec::new(),
    )
}

// ---------------------------------------------------------------------------
// supplier

pub(crate) fn supplier(t: &Text, first: u64, n: u64) -> RecordBatch {
    const BBB_BASE: &[u8] = b"Customer ";
    const COMPLAINTS: &[u8] = b"Complaints";
    const RECOMMENDS: &[u8] = b"Recommends";
    const BBB_LEN: usize = BBB_BASE.len() + COMPLAINTS.len();

    let rows = n as usize;
    let mut addr_r = Rng::at(706178559, 9, first);
    let mut nation_r = Rng::at(110356601, 1, first);
    let mut phone_r = Rng::at(884434366, 3, first);
    let mut bal_r = Rng::at(962338209, 1, first);
    let comment = Comment::new(1341315363, 63.0);
    let mut comment_r = comment.rng(1, first);
    let mut bbb_r = Rng::at(202794285, 1, first);
    let mut junk_r = Rng::at(263032577, 1, first);
    let mut offset_r = Rng::at(715851524, 1, first);
    let mut kind_r = Rng::at(753643799, 1, first);
    let last_nation = t.nations.values.len() as i32 - 1;

    let mut keys = Vec::with_capacity(rows);
    let mut names = Views::with_capacity(rows, rows * 18);
    let mut addrs = Views::with_capacity(rows, rows * 26);
    let mut nations = Vec::with_capacity(rows);
    let mut phones = Views::with_capacity(rows, rows * 15);
    let mut bals = Vec::with_capacity(rows);
    let mut comments = Views::with_capacity(rows, 0);
    let mut buf = Vec::new();
    for i in first..first + n {
        let key = i as i64 + 1;
        keys.push(key);
        numbered(&mut names, b"Supplier#", key as u64);
        address(&mut addrs, &mut addr_r);
        let nation = nation_r.next_int(0, last_nation) as i64;
        nations.push(nation);
        phone(&mut phones, &mut phone_r, nation);
        bals.push(bal_r.next_int(-99_999, 999_999) as i128);

        let c = comment.next(t, &mut comment_r);
        if bbb_r.next_int(1, 10_000) <= 10 {
            // a Better Business Bureau complaint or recommendation,
            // written over the comment at a random place
            let noise = junk_r.next_int(0, (c.len() - BBB_LEN) as i32) as usize;
            let off = offset_r.next_int(0, (c.len() - (BBB_LEN + noise)) as i32) as usize;
            let kind = if kind_r.next_int(0, 100) < 50 {
                COMPLAINTS
            } else {
                RECOMMENDS
            };
            buf.clear();
            buf.extend_from_slice(&c[..off]);
            buf.extend_from_slice(BBB_BASE);
            buf.extend_from_slice(&c[off + BBB_BASE.len()..off + BBB_BASE.len() + noise]);
            buf.extend_from_slice(kind);
            buf.extend_from_slice(&c[off + BBB_BASE.len() + noise + kind.len()..]);
            let start = comments.data.len();
            comments.data.extend_from_slice(&buf);
            comments.end_string(start);
        } else {
            comments
                .views
                .push(crate::text::view(c, crate::text::POOL, offset_in(t, c)));
        }
        for r in [
            &mut addr_r,
            &mut nation_r,
            &mut phone_r,
            &mut bal_r,
            &mut comment_r,
            &mut bbb_r,
            &mut junk_r,
            &mut offset_r,
            &mut kind_r,
        ] {
            r.row_finished();
        }
    }
    batch(
        &SUPPLIER,
        vec![
            i64s(keys),
            strs(names.finish(t)),
            strs(addrs.finish(t)),
            i64s(nations),
            strs(phones.finish(t)),
            decimals(bals),
            strs(comments.finish(t)),
        ],
    )
}

// ---------------------------------------------------------------------------
// customer

pub(crate) fn customer(t: &Text, first: u64, n: u64) -> RecordBatch {
    let rows = n as usize;
    let mut addr_r = Rng::at(881155353, 9, first);
    let mut nation_r = Rng::at(1489529863, 1, first);
    let mut phone_r = Rng::at(1521138112, 3, first);
    let mut bal_r = Rng::at(298370230, 1, first);
    let mut seg_r = Rng::at(1140279430, 1, first);
    let comment = Comment::new(1335826707, 73.0);
    let mut comment_r = comment.rng(1, first);
    let last_nation = t.nations.values.len() as i32 - 1;

    let mut keys = Vec::with_capacity(rows);
    let mut names = Views::with_capacity(rows, rows * 18);
    let mut addrs = Views::with_capacity(rows, rows * 26);
    let mut nations = Vec::with_capacity(rows);
    let mut phones = Views::with_capacity(rows, rows * 15);
    let mut bals = Vec::with_capacity(rows);
    let mut segs = Vec::with_capacity(rows);
    let mut comment_x = Vec::with_capacity(rows);
    for i in first..first + n {
        let key = i as i64 + 1;
        keys.push(key);
        numbered(&mut names, b"Customer#", key as u64);
        address(&mut addrs, &mut addr_r);
        let nation = nation_r.next_int(0, last_nation) as i64;
        nations.push(nation);
        phone(&mut phones, &mut phone_r, nation);
        bals.push(bal_r.next_int(-99_999, 999_999) as i128);
        segs.push(t.segments.views[t.segments.pick(&mut seg_r)]);
        comment_x.push((comment_r.next_rand(), comment_r.next_rand()));
        for r in [
            &mut addr_r,
            &mut nation_r,
            &mut phone_r,
            &mut bal_r,
            &mut seg_r,
            &mut comment_r,
        ] {
            r.row_finished();
        }
    }
    batch(
        &CUSTOMER,
        vec![
            i64s(keys),
            strs(names.finish(t)),
            strs(addrs.finish(t)),
            i64s(nations),
            strs(phones.finish(t)),
            decimals(bals),
            strs(t.array(segs, Vec::new())),
            strs(comment_views(t, &comment, &comment_x)),
        ],
    )
}

// ---------------------------------------------------------------------------
// partsupp

/// Rows of `partsupp` per part.
pub(crate) const SUPPLIERS_PER_PART: u64 = 4;

/// The `partsupp` rows of parts `first..first + n`.
pub(crate) fn partsupp(t: &Text, s: &Scale, first: u64, n: u64) -> RecordBatch {
    let rows = (n * SUPPLIERS_PER_PART) as usize;
    let comment = Comment::new(1961692154, 124.0);
    // one stream per column, 4 draws (8 for the comment) per part
    let mut qty = vec![0u32; rows];
    draws(1671059989, first * SUPPLIERS_PER_PART, &mut qty);
    bound_all(&mut qty, 1, 9999);
    let mut cost = vec![0u32; rows];
    draws(1051288424, first * SUPPLIERS_PER_PART, &mut cost);
    bound_all(&mut cost, 100, 100_000);
    let mut cx = vec![0u32; 2 * rows];
    draws(comment.seed, 2 * first * SUPPLIERS_PER_PART, &mut cx);

    let mut keys = Vec::with_capacity(rows);
    let mut supps = Vec::with_capacity(rows);
    for p in first..first + n {
        let key = p as i64 + 1;
        for k in 0..SUPPLIERS_PER_PART as i64 {
            keys.push(key);
            supps.push(part_supplier(key, k, s.suppliers));
        }
    }
    let comments = cx
        .chunks_exact(2)
        .map(|x| comment.view(t, x[0], x[1]))
        .collect();
    batch(
        &PARTSUPP,
        vec![
            i64s(keys),
            i64s(supps),
            i32s(qty.into_iter().map(|v| v as i32).collect()),
            decimals(cost.into_iter().map(|v| v as i32 as i128).collect()),
            strs(t.array(comments, Vec::new())),
        ],
    )
}

// ---------------------------------------------------------------------------
// orders and lineitem

/// The order-level values of a block of orders, and the line values both
/// tables need, each line stream as its dense 7 draws per order.
struct Block {
    first: u64,
    /// dbgen order dates.
    date: Vec<i32>,
    lines: Vec<u8>,
    quantity: Vec<u32>,
    discount: Vec<u32>,
    tax: Vec<u32>,
    part: Vec<u32>,
    ship: Vec<u32>,
}

impl Block {
    fn new(s: &Scale, first: u64, n: u64) -> Self {
        let n = n as usize;
        let dense = |seed: i64, lo: i32, hi: i32| {
            let mut v = vec![0u32; n * LINES];
            draws(seed, first * LINES as u64, &mut v);
            bound_all(&mut v, lo, hi);
            v
        };
        let mut date = vec![0u32; n];
        draws(1066728069, first, &mut date);
        bound_all(
            &mut date,
            MIN_GENERATE_DATE,
            MIN_GENERATE_DATE + TOTAL_DATE_RANGE - 151 - 1,
        );
        let mut lines = vec![0u32; n];
        draws(1434868289, first, &mut lines);
        bound_all(&mut lines, 1, LINES as i32);
        Block {
            first,
            date: date.into_iter().map(|d| d as i32).collect(),
            lines: lines.into_iter().map(|l| l as u8).collect(),
            quantity: dense(209208115, 1, 50),
            discount: dense(554590007, 0, 10),
            tax: dense(721958466, 0, 8),
            part: dense(1808217256, 1, s.parts as i32),
            ship: dense(1769349045, 1, 121),
        }
    }

    fn orders(&self) -> usize {
        self.date.len()
    }

    fn total_lines(&self) -> usize {
        self.lines.iter().map(|&l| l as usize).sum()
    }

    /// Lines of the block's orders, as `(order, slot)` pairs in order.
    fn slots(&self) -> impl Iterator<Item = (usize, usize)> + '_ {
        self.lines
            .iter()
            .enumerate()
            .flat_map(|(o, &l)| (0..l as usize).map(move |j| (o, j)))
    }
}

/// `orders` rows `first..first + n`.
pub(crate) fn orders(t: &Text, s: &Scale, first: u64, n: u64) -> RecordBatch {
    let b = Block::new(s, first, n);
    let rows = b.orders();
    let mut cust = vec![0u32; rows];
    draws(851767375, first, &mut cust);
    let mut prio = vec![0u32; rows];
    draws(591449447, first, &mut prio);
    let mut clerk = vec![0u32; rows];
    draws(1171034773, first, &mut clerk);
    bound_all(&mut clerk, 1, s.clerks);
    let comment = Comment::new(276090261, 49.0);
    let mut cx = vec![0u32; 2 * rows];
    draws(comment.seed, 2 * first, &mut cx);

    let past = *PAST;
    let mut keys = Vec::with_capacity(rows);
    let mut custs = Vec::with_capacity(rows);
    let mut status = Vec::with_capacity(rows);
    let mut totals = Vec::with_capacity(rows);
    let mut clerks = Views::with_capacity(rows, rows * 15);
    for o in 0..rows {
        keys.push(order_key((b.first + o as u64) as i64 + 1));
        // customer key, skipping every third customer (who has no orders)
        let mut c = bounded(cust[o], 1, s.customers as i32) as i64;
        let mut delta = 1;
        while c % 3 == 0 {
            c = (c + delta).min(s.customers);
            delta = -delta;
        }
        custs.push(c);
        let (mut total, mut shipped) = (0i64, 0);
        let lines = b.lines[o] as usize;
        for j in o * LINES..o * LINES + lines {
            let q = b.quantity[j] as i64;
            let d = b.discount[j] as i64;
            let tx = b.tax[j] as i64;
            let price = part_price(b.part[j] as i64) * q;
            total += ((price * (100 - d) / 100) * (100 + tx)) / 100;
            shipped += (b.ship[j] as i32 + b.date[o] <= past) as usize;
        }
        totals.push(total as i128);
        status.push(crate::text::view(
            if shipped == lines {
                b"F"
            } else if shipped > 0 {
                b"P"
            } else {
                b"O"
            },
            0,
            0,
        ));
        numbered(&mut clerks, b"Clerk#", clerk[o] as u64);
    }
    let prios = prio
        .iter()
        .map(|&x| t.priorities.views[t.priorities.pick_draw(x)])
        .collect();
    let comments = cx
        .chunks_exact(2)
        .map(|x| comment.view(t, x[0], x[1]))
        .collect();
    batch(
        &ORDERS,
        vec![
            i64s(keys),
            i64s(custs),
            strs(t.array(status, Vec::new())),
            decimals(totals),
            dates(
                b.date
                    .iter()
                    .map(|&d| d - MIN_GENERATE_DATE + EPOCH)
                    .collect(),
            ),
            strs(t.array(prios, Vec::new())),
            strs(clerks.finish(t)),
            i32s(vec![0; rows]),
            strs(t.array(comments, Vec::new())),
        ],
    )
}

/// The `lineitem` rows of orders `first..first + n`.
pub(crate) fn lineitem(t: &Text, s: &Scale, first: u64, n: u64) -> RecordBatch {
    let b = Block::new(s, first, n);
    let orders = b.orders();
    let dense = |seed: i64, lo: i32, hi: i32| {
        let mut v = vec![0u32; orders * LINES];
        draws(seed, first * LINES as u64, &mut v);
        bound_all(&mut v, lo, hi);
        v
    };
    let supp = dense(2095021727, 0, 3);
    let commit = dense(904914315, 30, 90);
    let receipt = dense(373135028, 1, 30);
    let mut flag_x = vec![0u32; orders * LINES];
    draws(717419739, first * LINES as u64, &mut flag_x);
    let mut instr_x = vec![0u32; orders * LINES];
    draws(1371272478, first * LINES as u64, &mut instr_x);
    let mut mode_x = vec![0u32; orders * LINES];
    draws(675466456, first * LINES as u64, &mut mode_x);
    let comment = Comment::new(1095462486, 27.0);
    let mut cx = vec![0u32; 2 * orders * LINES];
    draws(comment.seed, 2 * first * LINES as u64, &mut cx);

    let rows = b.total_lines();
    let past = *PAST;
    let mut keys = Vec::with_capacity(rows);
    let mut parts = Vec::with_capacity(rows);
    let mut supps = Vec::with_capacity(rows);
    let mut numbers = Vec::with_capacity(rows);
    let mut qty = Vec::with_capacity(rows);
    let mut ext = Vec::with_capacity(rows);
    let mut disc = Vec::with_capacity(rows);
    let mut tax = Vec::with_capacity(rows);
    let mut flags = Vec::with_capacity(rows);
    let mut status = Vec::with_capacity(rows);
    let mut ships = Vec::with_capacity(rows);
    let mut commits = Vec::with_capacity(rows);
    let mut receipts = Vec::with_capacity(rows);
    let mut instrs = Vec::with_capacity(rows);
    let mut modes = Vec::with_capacity(rows);
    let mut slots = Vec::with_capacity(rows);
    let (n_view, f_view, o_view) = (
        crate::text::view(b"N", 0, 0),
        crate::text::view(b"F", 0, 0),
        crate::text::view(b"O", 0, 0),
    );
    let mut returned = 0;
    for (o, j) in b.slots() {
        if j == 0 {
            returned = 0;
        }
        let k = o * LINES + j;
        let key = order_key((b.first + o as u64) as i64 + 1);
        let part = b.part[k] as i64;
        let q = b.quantity[k] as i64;
        keys.push(key);
        parts.push(part);
        supps.push(part_supplier(part, supp[k] as i64, s.suppliers));
        numbers.push(j as i32 + 1);
        qty.push(q as i128 * 100);
        ext.push((part_price(part) * q) as i128);
        disc.push(b.discount[k] as i128);
        tax.push(b.tax[k] as i128);
        let od = b.date[o];
        let ship = b.ship[k] as i32 + od;
        let receipt = receipt[k] as i32 + ship;
        // the return flag stream is drawn only for lines already received
        flags.push(if receipt <= past {
            let x = flag_x[o * LINES + returned];
            returned += 1;
            t.return_flags.views[t.return_flags.pick_draw(x)]
        } else {
            n_view
        });
        status.push(if ship <= past { f_view } else { o_view });
        ships.push(ship - MIN_GENERATE_DATE + EPOCH);
        commits.push(commit[k] as i32 + od - MIN_GENERATE_DATE + EPOCH);
        receipts.push(receipt - MIN_GENERATE_DATE + EPOCH);
        instrs.push(t.instructions.views[t.instructions.pick_draw(instr_x[k])]);
        modes.push(t.modes.views[t.modes.pick_draw(mode_x[k])]);
        slots.push(k as u32);
    }
    // the comment views read the pool at random offsets, a cache miss
    // each: in their own pass the loads are independent and overlap
    let comments = slots
        .iter()
        .map(|&k| comment.view(t, cx[2 * k as usize], cx[2 * k as usize + 1]))
        .collect();
    batch(
        &LINEITEM,
        vec![
            i64s(keys),
            i64s(parts),
            i64s(supps),
            i32s(numbers),
            decimals(qty),
            decimals(ext),
            decimals(disc),
            decimals(tax),
            strs(t.array(flags, Vec::new())),
            strs(t.array(status, Vec::new())),
            dates(ships),
            dates(commits),
            dates(receipts),
            strs(t.array(instrs, Vec::new())),
            strs(t.array(modes, Vec::new())),
            strs(t.array(comments, Vec::new())),
        ],
    )
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

    #[test]
    fn padding() {
        let mut v = Vec::new();
        padded(&mut v, 7, 9);
        padded(&mut v, 0, 1);
        padded(&mut v, 1_234_567_890, 9);
        assert_eq!(v, b"00000000701234567890");
    }
}
