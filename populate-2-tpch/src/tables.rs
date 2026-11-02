//! TPC-H tables as populate-2 distributions.
//!
//! Each table is an ordinary populate-2 tuple of columns. Nesting tuples
//! (`((a, b, c, d), (e, f, g, h), i)`) gets past populate-2's 4-tuple limit:
//! the sink is still a flat list, one Arrow column per leaf, in order.
//! Key-derived columns use populate-2's own `range` and `map`.

use crate::column::Text;
use crate::column::{
    Column, MARKET_SEGMENTS, PART_CONTAINERS, PART_TYPES, alpha_num, bounded, col, dists,
    grouped_bounded, grouped_text, key_name, phone, pick, text,
};
use crate::rng::Stream;
use crate::values::{Dec, POOL_BUFFER, Str, StrCol, push_int, text_pool};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use populate_2::arrow::IntoArrowColumns;
use populate_2::dist::{map, range};
use populate_2::traits::{DefaultSink, Distribution, VecBackend};
use rand::RngCore;
use std::borrow::Cow;
use std::sync::{Arc, LazyLock};

// ============================================================================
// Table trait + batching
// ============================================================================

pub trait Table {
    fn schema() -> SchemaRef;
    /// Total rows at scale factor `sf`.
    fn row_count(sf: f64) -> i64;
    /// Rows `[start, start + n)` as one batch.
    fn batch(sf: f64, start: i64, n: usize) -> RecordBatch;
    /// Rows must start on a multiple of this (partsupp: 4 rows per part).
    const ROW_GROUP: i64 = 1;
}

/// All rows of `T` in batches of `batch_size`, like `tpchgen-arrow`.
pub fn batches<T: Table>(sf: f64, batch_size: usize) -> impl Iterator<Item = RecordBatch> {
    assert_eq!(batch_size as i64 % T::ROW_GROUP, 0);
    let rows = T::row_count(sf);
    (0..rows)
        .step_by(batch_size)
        .map(move |start| T::batch(sf, start, (rows - start).min(batch_size as i64) as usize))
}

/// TPC-H columns carry their own streams; populate-2's `rng` argument is unused.
struct NoRng;

impl RngCore for NoRng {
    fn next_u32(&mut self) -> u32 {
        0
    }
    fn next_u64(&mut self) -> u64 {
        0
    }
    fn fill_bytes(&mut self, dst: &mut [u8]) {
        dst.fill(0)
    }
}

/// Collect `n` rows column by column and wrap them in `schema`.
pub fn collect_batch<D>(mut dist: D, n: usize, schema: &SchemaRef) -> RecordBatch
where
    D: Distribution<VecBackend>,
    <D::Item as DefaultSink<VecBackend>>::Sink: Default + IntoArrowColumns,
{
    let sink = dist.collect_into(Default::default(), &mut NoRng, n);
    let mut columns = Vec::with_capacity(schema.fields().len());
    sink.into_arrays(&mut columns);
    RecordBatch::try_new(Arc::clone(schema), columns).expect("schema matches columns")
}

fn schema(fields: &[(&str, DataType)]) -> SchemaRef {
    Arc::new(Schema::new(
        fields
            .iter()
            .map(|(name, dt)| Field::new(*name, dt.clone(), false))
            .collect::<Vec<_>>(),
    ))
}

const UTF8: DataType = DataType::Utf8View;
const DEC: DataType = DataType::Decimal128(15, 2);

fn rows(base: i64, sf: f64) -> i64 {
    (base as f64 * sf) as i64
}

// ============================================================================
// REGION, NATION
// ============================================================================

pub struct Region;

static REGION_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    schema(&[
        ("r_regionkey", DataType::Int64),
        ("r_name", UTF8),
        ("r_comment", UTF8),
    ])
});

pub fn region(start: i64) -> impl Distribution<VecBackend, Item = (i64, Str, Str)> {
    let names = dists().regions().get_values();
    (
        range(start, 1),
        map(range(start as usize, 1), move |i: usize| {
            Str::borrowed(names[i])
        }),
        col(text(1500869201, 72), start),
    )
}

impl Table for Region {
    fn schema() -> SchemaRef {
        REGION_SCHEMA.clone()
    }
    fn row_count(_sf: f64) -> i64 {
        dists().regions().size() as i64
    }
    fn batch(_sf: f64, start: i64, n: usize) -> RecordBatch {
        collect_batch(region(start), n, &REGION_SCHEMA)
    }
}

pub struct Nation;

static NATION_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    schema(&[
        ("n_nationkey", DataType::Int64),
        ("n_name", UTF8),
        ("n_regionkey", DataType::Int64),
        ("n_comment", UTF8),
    ])
});

pub fn nation(start: i64) -> impl Distribution<VecBackend, Item = (i64, Str, i64, Str)> {
    let nations = dists().nations();
    (
        range(start, 1),
        map(range(start as usize, 1), move |i: usize| {
            Str::borrowed(nations.get_values()[i])
        }),
        map(range(start as usize, 1), move |i: usize| {
            nations.get_weight(i) as i64
        }),
        col(text(606179079, 72), start),
    )
}

impl Table for Nation {
    fn schema() -> SchemaRef {
        NATION_SCHEMA.clone()
    }
    fn row_count(_sf: f64) -> i64 {
        dists().nations().size() as i64
    }
    fn batch(_sf: f64, start: i64, n: usize) -> RecordBatch {
        collect_batch(nation(start), n, &NATION_SCHEMA)
    }
}

// ============================================================================
// PART
// ============================================================================

/// `p_name`: 5 distinct colors (partial Fisher-Yates over all 92), joined by
/// spaces. Reserves one draw per color per row.
#[derive(Clone, Copy)]
pub struct PartName;

const NAME_WORDS: usize = 5;

impl PartName {
    #[inline]
    fn fill(st: &mut Stream, colors: &[&'static str], out: &mut Vec<u8>) {
        // Only the first NAME_WORDS slots are read back, so track the swapped
        // positions sparsely instead of copying all 92 colors per row.
        // moved[k] = (position, color index) written by swap k
        let mut slots: [u16; NAME_WORDS] = [0; NAME_WORDS];
        let mut moved: [(u16, u16); NAME_WORDS] = [(0, 0); NAME_WORDS];
        let at = |moved: &[(u16, u16)], pos: u16| -> u16 {
            moved
                .iter()
                .rev()
                .find(|(p, _)| *p == pos)
                .map_or(pos, |(_, v)| *v)
        };
        let r = st.next_item();
        for (i, slot) in slots.iter_mut().enumerate() {
            let j = r.next_int(i as i32, colors.len() as i32 - 1) as u16;
            let vi = at(&moved[..i], i as u16);
            *slot = at(&moved[..i], j);
            // position j now holds vi (position i is never read again)
            moved[i] = (j, vi);
        }
        for (k, &idx) in slots.iter().enumerate() {
            if k > 0 {
                out.push(b' ');
            }
            out.extend_from_slice(colors[idx as usize].as_bytes());
        }
    }
}

impl Column for PartName {
    type Item = Str;
    type Sink = StrCol;
    /// The color list is looked up once, not per row.
    type State = (Stream, &'static [&'static str]);

    fn reserve(&self, sink: &mut StrCol, n: usize) {
        sink.reserve(n, 34);
    }

    fn init(&self, start: i64) -> Self::State {
        let colors = dists().part_colors().get_values();
        (
            Stream::ungrouped(709314158, colors.len() as i32, start),
            colors,
        )
    }

    fn value(&self, st: &mut Self::State, _row: i64) -> Str {
        let mut out = Vec::new();
        Self::fill(&mut st.0, st.1, &mut out);
        Str(Cow::Owned(String::from_utf8(out).expect("ascii")))
    }

    #[inline]
    fn write(&self, st: &mut Self::State, _row: i64, sink: &mut StrCol) {
        let colors = st.1;
        sink.push_with(|out| Self::fill(&mut st.0, colors, out));
    }
}

/// `p_mfgr` ("Manufacturer#m") and `p_brand` ("Brand#mb"): the brand re-runs
/// the manufacturer stream rather than reading the other column.
#[derive(Clone, Copy)]
pub struct Mfgr {
    pub brand: bool,
}

impl Column for Mfgr {
    type Item = Str;
    type Sink = StrCol;
    type State = (Stream, Stream);

    fn reserve(&self, sink: &mut StrCol, n: usize) {
        sink.reserve(n, 14);
    }

    fn init(&self, start: i64) -> (Stream, Stream) {
        (
            Stream::ungrouped(1, 1, start),
            Stream::ungrouped(46831694, 1, start),
        )
    }

    fn value(&self, st: &mut (Stream, Stream), row: i64) -> Str {
        let mut sink = StrCol::default();
        self.write(st, row, &mut sink);
        let arr = populate_2::arrow::IntoArrowArray::into_array(sink);
        let s = arrow_array::cast::AsArray::as_string_view(&arr)
            .value(0)
            .to_string();
        Str(Cow::Owned(s))
    }

    #[inline]
    fn write(&self, st: &mut (Stream, Stream), _row: i64, sink: &mut StrCol) {
        let m = st.0.next_item().next_int(1, 5);
        if self.brand {
            let b = st.1.next_item().next_int(1, 5);
            sink.push_with(|out| {
                out.extend_from_slice(b"Brand#");
                push_int(out, (m * 10 + b) as u64, 1);
            });
        } else {
            sink.push_with(|out| {
                out.extend_from_slice(b"Manufacturer#");
                push_int(out, m as u64, 1);
            });
        }
    }
}

pub fn retail_price(part_key: i64) -> i64 {
    90000 + (part_key / 10) % 20001 + (part_key % 1000) * 100
}

pub struct Part;

static PART_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    schema(&[
        ("p_partkey", DataType::Int64),
        ("p_name", UTF8),
        ("p_mfgr", UTF8),
        ("p_brand", UTF8),
        ("p_type", UTF8),
        ("p_size", DataType::Int32),
        ("p_container", UTF8),
        ("p_retailprice", DEC),
        ("p_comment", UTF8),
    ])
});

#[allow(clippy::type_complexity)]
pub fn part(
    start: i64,
) -> impl Distribution<VecBackend, Item = ((i64, Str, Str, Str), (Str, i32, Str, Dec), Str)> {
    (
        (
            range(start + 1, 1),
            col(PartName, start),
            col(Mfgr { brand: false }, start),
            col(Mfgr { brand: true }, start),
        ),
        (
            col(pick(1841581359, &PART_TYPES), start),
            col(bounded::<i32>(1193163244, 1, 50), start),
            col(pick(727633698, &PART_CONTAINERS), start),
            map(range(start + 1, 1), |key: i64| Dec(retail_price(key))),
        ),
        col(text(804159733, 14), start),
    )
}

impl Table for Part {
    fn schema() -> SchemaRef {
        PART_SCHEMA.clone()
    }
    fn row_count(sf: f64) -> i64 {
        rows(200_000, sf)
    }
    fn batch(_sf: f64, start: i64, n: usize) -> RecordBatch {
        collect_batch(part(start), n, &PART_SCHEMA)
    }
}

// ============================================================================
// SUPPLIER
// ============================================================================

const BBB_BASE: &str = "Customer ";
const BBB_COMPLAINT: &str = "Complaints";
const BBB_RECOMMEND: &str = "Recommends";
const BBB_LEN: usize = BBB_BASE.len() + BBB_COMPLAINT.len();

/// `s_comment`: a text-pool slice, except ~0.1% of suppliers get
/// "Customer ... Complaints/Recommends" spliced in. Five streams.
#[derive(Clone, Copy)]
pub struct SupplierComment;

impl Column for SupplierComment {
    type Item = Str;
    type Sink = StrCol;
    type State = ([Stream; 5], Text, &'static [u8]);

    fn reserve(&self, sink: &mut StrCol, n: usize) {
        sink.reserve(n, 0);
    }

    fn init(&self, start: i64) -> Self::State {
        let streams = [
            Stream::ungrouped(1341315363, 2, start), // comment text
            Stream::ungrouped(202794285, 1, start),  // BBB yes/no
            Stream::ungrouped(263032577, 1, start),  // junk length
            Stream::ungrouped(715851524, 1, start),  // offset
            Stream::ungrouped(753643799, 1, start),  // complaint vs recommend
        ];
        (streams, text(0, 63), POOL_BUFFER.as_slice())
    }

    fn value(&self, st: &mut Self::State, row: i64) -> Str {
        let mut sink = StrCol::default();
        self.write(st, row, &mut sink);
        let arr = populate_2::arrow::IntoArrowArray::into_array(sink);
        let s = arrow_array::cast::AsArray::as_string_view(&arr)
            .value(0)
            .to_string();
        Str(Cow::Owned(s))
    }

    #[inline]
    fn write(&self, state: &mut Self::State, _row: i64, sink: &mut StrCol) {
        let (st, comment, pool) = state;
        let (offset, len) = comment.draw(st[0].next_item());
        // The other streams must advance every row even when unused.
        let bbb = st[1].next_item().next_int(1, 10_000);
        let mut junk = *st[2].next_item();
        let mut place = *st[3].next_item();
        let mut kind = *st[4].next_item();
        if bbb > 10 {
            sink.push_pool(pool, offset as u32, len as u32);
            return;
        }
        let comment = text_pool().text(offset, offset + len);
        let noise = junk.next_int(0, (comment.len() - BBB_LEN) as i32) as usize;
        let at = place.next_int(0, (comment.len() - (BBB_LEN + noise)) as i32) as usize;
        let kind_text = if kind.next_int(0, 100) < 50 {
            BBB_COMPLAINT
        } else {
            BBB_RECOMMEND
        };
        sink.push_with(|out| {
            out.extend_from_slice(&comment.as_bytes()[..at]);
            out.extend_from_slice(BBB_BASE.as_bytes());
            out.extend_from_slice(
                &comment.as_bytes()[at + BBB_BASE.len()..at + BBB_BASE.len() + noise],
            );
            out.extend_from_slice(kind_text.as_bytes());
            out.extend_from_slice(
                &comment.as_bytes()[at + BBB_BASE.len() + noise + kind_text.len()..],
            );
        });
    }
}

pub struct Supplier;

static SUPPLIER_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    schema(&[
        ("s_suppkey", DataType::Int64),
        ("s_name", UTF8),
        ("s_address", UTF8),
        ("s_nationkey", DataType::Int64),
        ("s_phone", UTF8),
        ("s_acctbal", DEC),
        ("s_comment", UTF8),
    ])
});

#[allow(clippy::type_complexity)]
pub fn supplier(
    start: i64,
) -> impl Distribution<VecBackend, Item = ((i64, Str, Str, i64), (Str, Dec, Str))> {
    (
        (
            range(start + 1, 1),
            col(key_name("Supplier#", 9), start),
            col(alpha_num(706178559, 25), start),
            col(bounded::<i64>(110356601, 0, 24), start),
        ),
        (
            col(phone(110356601, 884434366), start),
            col(bounded::<Dec>(962338209, -99999, 999999), start),
            col(SupplierComment, start),
        ),
    )
}

impl Table for Supplier {
    fn schema() -> SchemaRef {
        SUPPLIER_SCHEMA.clone()
    }
    fn row_count(sf: f64) -> i64 {
        rows(10_000, sf)
    }
    fn batch(_sf: f64, start: i64, n: usize) -> RecordBatch {
        collect_batch(supplier(start), n, &SUPPLIER_SCHEMA)
    }
}

// ============================================================================
// CUSTOMER
// ============================================================================

pub struct Customer;

static CUSTOMER_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    schema(&[
        ("c_custkey", DataType::Int64),
        ("c_name", UTF8),
        ("c_address", UTF8),
        ("c_nationkey", DataType::Int64),
        ("c_phone", UTF8),
        ("c_acctbal", DEC),
        ("c_mktsegment", UTF8),
        ("c_comment", UTF8),
    ])
});

#[allow(clippy::type_complexity)]
pub fn customer(
    start: i64,
) -> impl Distribution<VecBackend, Item = ((i64, Str, Str, i64), (Str, Dec, Str, Str))> {
    (
        (
            range(start + 1, 1),
            col(key_name("Customer#", 9), start),
            col(alpha_num(881155353, 25), start),
            col(bounded::<i64>(1489529863, 0, 24), start),
        ),
        (
            col(phone(1489529863, 1521138112), start),
            col(bounded::<Dec>(298370230, -99999, 999999), start),
            col(pick(1140279430, &MARKET_SEGMENTS), start),
            col(text(1335826707, 73), start),
        ),
    )
}

impl Table for Customer {
    fn schema() -> SchemaRef {
        CUSTOMER_SCHEMA.clone()
    }
    fn row_count(sf: f64) -> i64 {
        rows(150_000, sf)
    }
    fn batch(_sf: f64, start: i64, n: usize) -> RecordBatch {
        collect_batch(customer(start), n, &CUSTOMER_SCHEMA)
    }
}

// ============================================================================
// PARTSUPP: 4 rows per part; its streams reserve draws per *part*
// ============================================================================

const SUPPLIERS_PER_PART: i64 = 4;

pub fn part_supplier(part_key: i64, supplier_number: i64, sf: f64) -> i64 {
    let supplier_count = rows(10_000, sf);
    ((part_key
        + (supplier_number
            * ((supplier_count / SUPPLIERS_PER_PART) + ((part_key - 1) / supplier_count))))
        % supplier_count)
        + 1
}

pub struct PartSupp;

static PARTSUPP_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    schema(&[
        ("ps_partkey", DataType::Int64),
        ("ps_suppkey", DataType::Int64),
        ("ps_availqty", DataType::Int32),
        ("ps_supplycost", DEC),
        ("ps_comment", UTF8),
    ])
});

/// `start` counts partsupp rows (must be a multiple of 4).
pub fn partsupp(
    sf: f64,
    start: i64,
) -> impl Distribution<VecBackend, Item = ((i64, i64, i32, Dec), Str)> {
    let g = SUPPLIERS_PER_PART as u32;
    (
        (
            map(range(start, 1), |i: i64| i / SUPPLIERS_PER_PART + 1),
            map(range(start, 1), move |i: i64| {
                part_supplier(i / SUPPLIERS_PER_PART + 1, i % SUPPLIERS_PER_PART, sf)
            }),
            col(grouped_bounded::<i32>(1671059989, 1, 9999, 4, g), start),
            col(grouped_bounded::<Dec>(1051288424, 100, 100000, 4, g), start),
        ),
        col(grouped_text(1961692154, 124, g), start),
    )
}

impl Table for PartSupp {
    const ROW_GROUP: i64 = SUPPLIERS_PER_PART;
    fn schema() -> SchemaRef {
        PARTSUPP_SCHEMA.clone()
    }
    fn row_count(sf: f64) -> i64 {
        rows(200_000, sf) * SUPPLIERS_PER_PART
    }
    fn batch(sf: f64, start: i64, n: usize) -> RecordBatch {
        collect_batch(partsupp(sf, start), n, &PARTSUPP_SCHEMA)
    }
}
