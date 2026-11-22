//! Workloads for comparing populate with tpchgen.
//!
//! Every workload generates `n` rows (or `n` list values) on one thread and
//! returns an [`Output`] with the row count and output bytes. Arrow bytes are
//! the batches' buffer memory (`get_array_memory_size`); Parquet bytes are the
//! file size written to a [`CountingWriter`] (no disk I/O).
//!
//! The shared schema is the **numeric half of TPC-H `lineitem`**: the 11
//! columns that aren't strings, with decimals as `i64` cents and dates as
//! `i32` days (Arrow `Date32` storage):
//!
//! | column        | type | populate distribution        |
//! |---------------|------|--------------------------------|
//! | orderkey      | i64  | `range(1, 1)`                  |
//! | partkey       | i64  | uniform 1..=200_000            |
//! | suppkey       | i64  | uniform 1..=10_000             |
//! | linenumber    | i32  | uniform 1..=7                  |
//! | quantity      | i64  | uniform 1..=50                 |
//! | extendedprice | i64  | uniform 90_100..=10_494_950    |
//! | discount      | i64  | uniform 0..=10                 |
//! | tax           | i64  | uniform 0..=8                  |
//! | shipdate      | i32  | uniform over 1992-01-02..1998  |
//! | commitdate    | i32  | uniform over 1992-01-31..1998  |
//! | receiptdate   | i32  | uniform over 1992-01-03..1998  |
//!
//! Same shape, types and value ranges as tpchgen, not the same values or the
//! cross-column correlations (tpchgen derives `extendedprice` from the part's
//! price and the dates from the order date). tpchgen always generates every
//! `lineitem` column, strings included, so its numeric-subset variant still
//! pays for them; that is the real cost of getting these columns from it.

pub mod fair;
pub mod lanes;

use arrow_array::builder::{Int32Builder, Int64Builder};
use arrow_array::{ArrayRef, Int32Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use populate::arrow::{ArrowBackend, DistributionArrow};
use populate::dist::{list, range};
use populate::parquet::{DistributionParquet, write_parquet};
use populate::traits::{DefaultSink, Distribution, SampleIter, Sink};
use rand::distr::uniform::SampleUniform;
use rand::distr::{Distribution as RandDistribution, Uniform as RandUniform};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use std::hint::black_box;
use std::io::{self, Write};
use std::sync::Arc;
use tpchgen::generators::{LineItem, LineItemGenerator};
use tpchgen::text::TextPool;
use tpchgen_arrow::LineItemArrow;
use tpchgen_arrow::arrow::array::{
    Array as Array60, ArrayRef as ArrayRef60, Int32Array as Int32Array60,
    Int64Array as Int64Array60, ListArray as ListArray60, RecordBatch as RecordBatch60,
};
use tpchgen_arrow::arrow::buffer::OffsetBuffer as OffsetBuffer60;
use tpchgen_arrow::arrow::datatypes::{
    DataType as DataType60, Field as Field60, Schema as Schema60, SchemaRef as SchemaRef60,
};

/// Batch size for streaming variants (tpchgen-arrow's default is 8000).
pub const BATCH: usize = 8192;

/// Mean lineitems per order in TPC-H (uniform 1..=7).
pub const LINES_PER_ORDER: usize = 4;

/// What a workload produced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Output {
    pub rows: usize,
    pub bytes: usize,
}

// ============================================================================
// Shared Parquet setup
// ============================================================================

/// Discards bytes, counts them. Keeps Parquet runs free of disk/page-cache
/// effects while still running the full encoder.
#[derive(Debug, Default)]
pub struct CountingWriter(pub usize);

impl Write for CountingWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0 += buf.len();
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Same settings on both sides: uncompressed, no dictionary (populate's
/// zero-copy default), default page/row-group sizes.
pub fn parquet_props() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .set_dictionary_enabled(false)
        .build()
}

fn parquet_props_60() -> parquet::file::properties::WriterProperties {
    parquet::file::properties::WriterProperties::builder()
        .set_compression(parquet::basic::Compression::UNCOMPRESSED)
        .set_dictionary_enabled(false)
        .build()
}

// ============================================================================
// populate: a uniform leaf distribution
// ============================================================================

/// Uniform over `lo..=hi`, drawn from the caller's RNG. populate doesn't
/// ship a random leaf yet; this is the obvious one a user would write.
pub struct Uniform<T: SampleUniform> {
    dist: RandUniform<T>,
}

pub fn uniform<T: SampleUniform>(lo: T, hi: T) -> Uniform<T> {
    Uniform {
        dist: RandUniform::new_inclusive(lo, hi).expect("lo <= hi"),
    }
}

pub struct UniformIter<T: SampleUniform> {
    dist: RandUniform<T>,
}

impl<T: SampleUniform> SampleIter for UniformIter<T>
where
    RandUniform<T>: Clone,
{
    type Item = T;

    fn next(&mut self, rng: &mut impl Rng) -> T {
        self.dist.sample(rng)
    }
}

impl<T> Distribution<ArrowBackend> for Uniform<T>
where
    T: SampleUniform + DefaultSink<ArrowBackend>,
    RandUniform<T>: Clone,
    <T as DefaultSink<ArrowBackend>>::Sink: Sink<T>,
{
    type Item = T;
    type RowIter = UniformIter<T>;

    fn row_iter(&mut self) -> UniformIter<T> {
        UniformIter {
            dist: self.dist.clone(),
        }
    }

    fn collect_into(
        &mut self,
        mut sink: <T as DefaultSink<ArrowBackend>>::Sink,
        rng: &mut impl Rng,
        n: usize,
    ) -> <T as DefaultSink<ArrowBackend>>::Sink {
        sink.reserve(n);
        // as populate's own `uniform`: one `extend` of a `TrustedLen`
        // iterator, no capacity check per value
        let dist = &self.dist;
        sink.push_n((0..n).map(|_| dist.sample(rng)));
        sink
    }
}

// ============================================================================
// populate distributions
// ============================================================================

/// TPC-H dates as days since 1970-01-01 (1992-01-01 is day 8035).
const D1992: i32 = 8035;
const D1998_END: i32 = D1992 + 2556;

pub type LineItemNumeric = ((i64, i64, i64, i32), (i64, i64, i64, i64), (i32, i32, i32));

/// The 11 numeric lineitem columns (see the module docs).
pub fn lineitem_numeric() -> impl Distribution<ArrowBackend, Item = LineItemNumeric> {
    lineitem_numeric_from(1)
}

/// [`lineitem_numeric`] with orderkeys starting at `first_key`. A partition
/// that starts at row `k` skips ahead in O(1): only the key range moves, and
/// the uniform columns just need their own RNG seed.
pub fn lineitem_numeric_from(
    first_key: i64,
) -> impl Distribution<ArrowBackend, Item = LineItemNumeric> {
    (
        (
            range(first_key, 1),
            uniform(1i64, 200_000),
            uniform(1i64, 10_000),
            uniform(1i32, 7),
        ),
        (
            uniform(1i64, 50),
            uniform(90_100i64, 10_494_950),
            uniform(0i64, 10),
            uniform(0i64, 8),
        ),
        (
            uniform(D1992 + 1, D1998_END - 151),
            uniform(D1992 + 30, D1998_END - 61),
            uniform(D1992 + 2, D1998_END - 121),
        ),
    )
}

/// Orders with their line prices nested: `(orderkey, list<extendedprice>)`,
/// 1..=7 lines per order (mean 4), as TPC-H.
pub fn orders_with_line_prices() -> impl Distribution<ArrowBackend, Item = (i64, Vec<i64>)> {
    (
        range(1i64, 1),
        list(uniform(1usize, 7), uniform(90_100i64, 10_494_950)),
    )
}

pub fn rng() -> SmallRng {
    SmallRng::seed_from_u64(42)
}

pub fn batch_bytes(b: &RecordBatch) -> usize {
    b.columns().iter().map(|c| c.get_array_memory_size()).sum()
}

pub fn batch_bytes_60(b: &RecordBatch60) -> usize {
    b.columns().iter().map(|c| c.get_array_memory_size()).sum()
}

/// Split `n` into `BATCH`-sized chunks.
fn chunks(n: usize) -> impl Iterator<Item = usize> {
    (0..n.div_ceil(BATCH)).map(move |i| BATCH.min(n - i * BATCH))
}

// ============================================================================
// populate workloads
// ============================================================================

/// 11 numeric columns, one `RecordBatch` of `n` rows.
pub fn p2_numeric_arrow(n: usize) -> Output {
    let mut d = lineitem_numeric();
    let batch = d.collect_record_batch(&mut rng(), n).expect("batch");
    black_box(Output {
        rows: batch.num_rows(),
        bytes: batch_bytes(&batch),
    })
}

/// Same, streamed as `BATCH`-row batches (bounded memory, like tpchgen-arrow).
pub fn p2_numeric_arrow_batched(n: usize) -> Output {
    let mut out = Output { rows: 0, bytes: 0 };
    p2_numeric_batches(1, n, BATCH, &mut rng(), |batch| {
        out.rows += batch.num_rows();
        out.bytes += batch_bytes(&batch);
        black_box(batch);
    });
    out
}

/// Stream `n` rows of [`lineitem_numeric`] as `batch`-row batches, orderkeys
/// `first_key..first_key + n`. `collect_into` restarts a `range` from its
/// start on every call, so the distribution is rebuilt per batch at the next
/// key (cheap: it's a few `Copy` structs). The uniform columns carry on from
/// the shared `rng`.
pub fn p2_numeric_batches(
    first_key: i64,
    n: usize,
    batch: usize,
    rng: &mut SmallRng,
    mut f: impl FnMut(RecordBatch),
) {
    let mut key = first_key;
    let mut left = n;
    // made by the first batch, then shared by the rest
    let mut schema = None;
    while left > 0 {
        let len = batch.min(left);
        let mut d = lineitem_numeric_from(key);
        let b = match &schema {
            None => d.collect_record_batch(rng, len),
            Some(s) => d.collect_record_batch_with(s, rng, len),
        };
        let b = b.expect("batch");
        schema.get_or_insert_with(|| b.schema());
        f(b);
        key += len as i64;
        left -= len;
    }
}

// ============================================================================
// Hand-written Arrow baselines (no populate): same columns, ranges and RNG
// ============================================================================

/// The 11 columns' `(lo, hi)` ranges, in schema order, as populate uses them.
struct NumericRanges {
    i64s: [(i64, i64); 6],
    i32s: [(i32, i32); 4],
}

const RANGES: NumericRanges = NumericRanges {
    // partkey, suppkey, quantity, extendedprice, discount, tax
    i64s: [
        (1, 200_000),
        (1, 10_000),
        (1, 50),
        (90_100, 10_494_950),
        (0, 10),
        (0, 8),
    ],
    // linenumber, shipdate, commitdate, receiptdate
    i32s: [
        (1, 7),
        (D1992 + 1, D1998_END - 151),
        (D1992 + 30, D1998_END - 61),
        (D1992 + 2, D1998_END - 121),
    ],
};

fn numeric_schema_57() -> Arc<Schema> {
    let i64f = |n: &str| Field::new(n, DataType::Int64, false);
    let i32f = |n: &str| Field::new(n, DataType::Int32, false);
    Arc::new(Schema::new(vec![
        i64f("orderkey"),
        i64f("partkey"),
        i64f("suppkey"),
        i32f("linenumber"),
        i64f("quantity"),
        i64f("extendedprice"),
        i64f("discount"),
        i64f("tax"),
        i32f("shipdate"),
        i32f("commitdate"),
        i32f("receiptdate"),
    ]))
}

/// Hand-written column-at-a-time: one exactly-sized `Vec` per column, filled
/// with a tight loop, moved into an Arrow array. The best you can do by hand;
/// populate's columnar path should match it.
pub fn raw_columnar_arrow_batched(n: usize) -> Output {
    fn col<T: SampleUniform + Copy>(r: &mut SmallRng, (lo, hi): (T, T), len: usize) -> Vec<T> {
        let u = RandUniform::new_inclusive(lo, hi).expect("range");
        let mut v = Vec::with_capacity(len);
        for _ in 0..len {
            v.push(u.sample(r));
        }
        v
    }
    let schema = numeric_schema_57();
    let mut r = rng();
    let mut key = 1i64;
    let mut out = Output { rows: 0, bytes: 0 };
    let [pk, sk, q, ep, d, t] = RANGES.i64s;
    let [ln, sd, cd, rd] = RANGES.i32s;
    for len in chunks(n) {
        let keys: Vec<i64> = (key..key + len as i64).collect();
        key += len as i64;
        let i64c = |v: Vec<i64>| Arc::new(Int64Array::from(v)) as ArrayRef;
        let i32c = |v: Vec<i32>| Arc::new(Int32Array::from(v)) as ArrayRef;
        let cols = vec![
            i64c(keys),
            i64c(col(&mut r, pk, len)),
            i64c(col(&mut r, sk, len)),
            i32c(col(&mut r, ln, len)),
            i64c(col(&mut r, q, len)),
            i64c(col(&mut r, ep, len)),
            i64c(col(&mut r, d, len)),
            i64c(col(&mut r, t, len)),
            i32c(col(&mut r, sd, len)),
            i32c(col(&mut r, cd, len)),
            i32c(col(&mut r, rd, len)),
        ];
        let batch = RecordBatch::try_new(schema.clone(), cols).expect("batch");
        out.rows += batch.num_rows();
        out.bytes += batch_bytes(&batch);
        black_box(batch);
    }
    out
}

/// Hand-written row-at-a-time: draw all 11 values for a row, append each to
/// a typed Arrow `PrimitiveBuilder` (the usual way to fill Arrow by hand).
pub fn raw_row_builders_arrow_batched(n: usize) -> Output {
    let schema = numeric_schema_57();
    let u64s = RANGES
        .i64s
        .map(|(lo, hi)| RandUniform::new_inclusive(lo, hi).expect("range"));
    let u32s = RANGES
        .i32s
        .map(|(lo, hi)| RandUniform::new_inclusive(lo, hi).expect("range"));
    let mut r = rng();
    let mut key = 1i64;
    let mut out = Output { rows: 0, bytes: 0 };
    for len in chunks(n) {
        let mut b64: [Int64Builder; 7] = std::array::from_fn(|_| Int64Builder::with_capacity(len));
        let mut b32: [Int32Builder; 4] = std::array::from_fn(|_| Int32Builder::with_capacity(len));
        for _ in 0..len {
            b64[0].append_value(key);
            key += 1;
            b64[1].append_value(u64s[0].sample(&mut r));
            b64[2].append_value(u64s[1].sample(&mut r));
            b32[0].append_value(u32s[0].sample(&mut r));
            b64[3].append_value(u64s[2].sample(&mut r));
            b64[4].append_value(u64s[3].sample(&mut r));
            b64[5].append_value(u64s[4].sample(&mut r));
            b64[6].append_value(u64s[5].sample(&mut r));
            b32[1].append_value(u32s[1].sample(&mut r));
            b32[2].append_value(u32s[2].sample(&mut r));
            b32[3].append_value(u32s[3].sample(&mut r));
        }
        let [k, pk, sk, q, ep, d, t] = &mut b64;
        let [ln, sd, cd, rd] = &mut b32;
        let cols: Vec<ArrayRef> = vec![
            Arc::new(k.finish()),
            Arc::new(pk.finish()),
            Arc::new(sk.finish()),
            Arc::new(ln.finish()),
            Arc::new(q.finish()),
            Arc::new(ep.finish()),
            Arc::new(d.finish()),
            Arc::new(t.finish()),
            Arc::new(sd.finish()),
            Arc::new(cd.finish()),
            Arc::new(rd.finish()),
        ];
        let batch = RecordBatch::try_new(schema.clone(), cols).expect("batch");
        out.rows += batch.num_rows();
        out.bytes += batch_bytes(&batch);
        black_box(batch);
    }
    out
}

/// 11 numeric columns straight to Parquet with the zero-copy column writer.
pub fn p2_numeric_parquet(n: usize) -> Output {
    let mut d = lineitem_numeric();
    let w = d
        .collect_parquet(
            &mut rng(),
            n,
            CountingWriter::default(),
            Some(parquet_props()),
        )
        .expect("parquet");
    Output {
        rows: n,
        bytes: w.0,
    }
}

/// 11 numeric columns: `RecordBatch`, then `ArrowWriter` (the generic path).
pub fn p2_numeric_parquet_arrow_writer(n: usize) -> Output {
    let mut d = lineitem_numeric();
    let batch = d.collect_record_batch(&mut rng(), n).expect("batch");
    let w = write_parquet(&batch, CountingWriter::default(), Some(parquet_props())).expect("pq");
    Output {
        rows: n,
        bytes: w.0,
    }
}

/// `n / 4` orders with `list<i64>` line prices (about `n` list values).
pub fn p2_list_arrow(n: usize) -> Output {
    let mut d = orders_with_line_prices();
    let batch = d
        .collect_record_batch(&mut rng(), n / LINES_PER_ORDER)
        .expect("batch");
    black_box(Output {
        rows: batch.num_rows(),
        bytes: batch_bytes(&batch),
    })
}

/// Same list data written with the zero-copy Parquet writer.
pub fn p2_list_parquet(n: usize) -> Output {
    let rows = n / LINES_PER_ORDER;
    let mut d = orders_with_line_prices();
    let w = d
        .collect_parquet(
            &mut rng(),
            rows,
            CountingWriter::default(),
            Some(parquet_props()),
        )
        .expect("parquet");
    Output { rows, bytes: w.0 }
}

// ============================================================================
// tpchgen workloads
// ============================================================================

/// Build tpchgen's 300 MiB text pool (a process-wide one-time cost; every
/// tpchgen workload assumes it is already built).
pub fn tpchgen_init() {
    black_box(TextPool::get_or_init_default());
}

/// Scale factor large enough that `n` lineitems never run out
/// (SF 1 ≈ 6M lineitems).
fn tpch_lineitems() -> LineItemGenerator<'static> {
    LineItemGenerator::new(10.0, 1, 1)
}

/// Floor: tpchgen's row generator alone, every row black-boxed, no output.
pub fn tpch_rows(n: usize) -> Output {
    let mut it = tpch_lineitems().iter();
    for _ in 0..n {
        black_box(it.next().expect("row"));
    }
    Output { rows: n, bytes: 0 }
}

/// tpchgen-arrow `LineItemArrow`: all 16 columns (Decimal128, Utf8View, ...).
pub fn tpch_full_arrow(n: usize) -> Output {
    let mut out = Output { rows: 0, bytes: 0 };
    let mut gen_ = LineItemArrow::new(tpch_lineitems()).with_batch_size(BATCH);
    while out.rows < n {
        let batch = gen_.next().expect("batch").expect("ok");
        out.rows += batch.num_rows();
        out.bytes += batch_bytes_60(&batch);
        black_box(batch);
    }
    out
}

pub fn numeric_schema_60() -> SchemaRef60 {
    let i64f = |n: &str| Field60::new(n, DataType60::Int64, false);
    let i32f = |n: &str| Field60::new(n, DataType60::Int32, false);
    Arc::new(Schema60::new(vec![
        i64f("orderkey"),
        i64f("partkey"),
        i64f("suppkey"),
        i32f("linenumber"),
        i64f("quantity"),
        i64f("extendedprice"),
        i64f("discount"),
        i64f("tax"),
        i32f("shipdate"),
        i32f("commitdate"),
        i32f("receiptdate"),
    ]))
}

/// tpchgen rows -> the same 11 numeric columns populate makes, written into
/// exactly-sized `Vec`s and moved into Arrow (the cheapest row->column path).
pub fn tpch_numeric_batch(rows: &[LineItem<'_>], schema: &SchemaRef60) -> RecordBatch60 {
    fn col64(rows: &[LineItem<'_>], f: impl Fn(&LineItem<'_>) -> i64) -> ArrayRef60 {
        Arc::new(Int64Array60::from(rows.iter().map(f).collect::<Vec<_>>()))
    }
    fn col32(rows: &[LineItem<'_>], f: impl Fn(&LineItem<'_>) -> i32) -> ArrayRef60 {
        Arc::new(Int32Array60::from(rows.iter().map(f).collect::<Vec<_>>()))
    }
    RecordBatch60::try_new(
        schema.clone(),
        vec![
            col64(rows, |r| r.l_orderkey),
            col64(rows, |r| r.l_partkey),
            col64(rows, |r| r.l_suppkey),
            col32(rows, |r| r.l_linenumber),
            col64(rows, |r| r.l_quantity),
            col64(rows, |r| r.l_extendedprice.into_inner()),
            col64(rows, |r| r.l_discount.into_inner()),
            col64(rows, |r| r.l_tax.into_inner()),
            col32(rows, |r| r.l_shipdate.to_unix_epoch()),
            col32(rows, |r| r.l_commitdate.to_unix_epoch()),
            col32(rows, |r| r.l_receiptdate.to_unix_epoch()),
        ],
    )
    .expect("batch")
}

/// Stream `BATCH`-row numeric batches from tpchgen rows into `f`.
fn tpch_numeric_batches(n: usize, mut f: impl FnMut(RecordBatch60)) {
    let schema = numeric_schema_60();
    let mut it = tpch_lineitems().iter();
    let mut rows = Vec::with_capacity(BATCH);
    for len in chunks(n) {
        rows.clear();
        rows.extend(it.by_ref().take(len));
        f(tpch_numeric_batch(&rows, &schema));
    }
}

/// tpchgen, numeric subset only, as `BATCH`-row Arrow batches.
pub fn tpch_numeric_arrow(n: usize) -> Output {
    let mut out = Output { rows: 0, bytes: 0 };
    tpch_numeric_batches(n, |b| {
        out.rows += b.num_rows();
        out.bytes += batch_bytes_60(&b);
        black_box(b);
    });
    out
}

/// tpchgen-arrow full lineitem -> `ArrowWriter` (what `tpchgen-cli` does).
pub fn tpch_full_parquet(n: usize) -> Output {
    let mut w = parquet::arrow::ArrowWriter::try_new(
        CountingWriter::default(),
        LineItemArrow::schema_ref(),
        Some(parquet_props_60()),
    )
    .expect("writer");
    let mut rows = 0;
    let mut gen_ = LineItemArrow::new(tpch_lineitems()).with_batch_size(BATCH);
    while rows < n {
        let batch = gen_.next().expect("batch").expect("ok");
        rows += batch.num_rows();
        w.write(&batch).expect("write");
    }
    let w = w.into_inner().expect("close");
    Output { rows, bytes: w.0 }
}

/// tpchgen numeric subset -> `ArrowWriter`.
pub fn tpch_numeric_parquet(n: usize) -> Output {
    let mut w = parquet::arrow::ArrowWriter::try_new(
        CountingWriter::default(),
        numeric_schema_60(),
        Some(parquet_props_60()),
    )
    .expect("writer");
    tpch_numeric_batches(n, |b| w.write(&b).expect("write"));
    let w = w.into_inner().expect("close");
    Output {
        rows: n,
        bytes: w.0,
    }
}

/// tpchgen lineitems grouped by orderkey into `(orderkey, list<extendedprice>)`:
/// how you get the nested shape from a row generator. Stops after the order
/// that crosses `n` values (about `n / 4` orders), one Arrow batch.
pub fn tpch_list_batch(n: usize) -> RecordBatch60 {
    let mut keys = Vec::with_capacity(n / LINES_PER_ORDER + 8);
    let mut offsets = Vec::with_capacity(n / LINES_PER_ORDER + 9);
    let mut prices = Vec::with_capacity(n + 8);
    offsets.push(0i32);
    for r in tpch_lineitems().iter() {
        if keys.last() != Some(&r.l_orderkey) {
            if prices.len() >= n {
                break;
            }
            if !keys.is_empty() {
                offsets.push(prices.len() as i32);
            }
            keys.push(r.l_orderkey);
        }
        prices.push(r.l_extendedprice.into_inner());
    }
    offsets.push(prices.len() as i32);
    let item = Arc::new(Field60::new_list_field(DataType60::Int64, false));
    let list = ListArray60::new(
        item.clone(),
        OffsetBuffer60::new(offsets.into()),
        Arc::new(Int64Array60::from(prices)),
        None,
    );
    let schema = Arc::new(Schema60::new(vec![
        Field60::new("orderkey", DataType60::Int64, false),
        Field60::new("line_prices", DataType60::List(item), false),
    ]));
    RecordBatch60::try_new(
        schema,
        vec![Arc::new(Int64Array60::from(keys)), Arc::new(list)],
    )
    .expect("batch")
}

pub fn tpch_list_arrow(n: usize) -> Output {
    let b = tpch_list_batch(n);
    black_box(Output {
        rows: b.num_rows(),
        bytes: batch_bytes_60(&b),
    })
}

pub fn tpch_list_parquet(n: usize) -> Output {
    let b = tpch_list_batch(n);
    let mut w = parquet::arrow::ArrowWriter::try_new(
        CountingWriter::default(),
        b.schema(),
        Some(parquet_props_60()),
    )
    .expect("writer");
    w.write(&b).expect("write");
    let w = w.into_inner().expect("close");
    Output {
        rows: b.num_rows(),
        bytes: w.0,
    }
}

// ============================================================================
// Registry (shared by the divan bench and the report binary)
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Group {
    /// 11 numeric lineitem columns to Arrow
    NumericArrow,
    /// 11 numeric lineitem columns to Parquet
    NumericParquet,
    /// orders with list<line price> to Arrow
    ListArrow,
    /// orders with list<line price> to Parquet
    ListParquet,
    /// tpchgen full lineitem, strings included (no populate equivalent)
    TpchFull,
}

pub struct Workload {
    pub name: &'static str,
    pub group: Group,
    pub run: fn(usize) -> Output,
}

pub const WORKLOADS: &[Workload] = &[
    Workload {
        name: "populate (one batch)",
        group: Group::NumericArrow,
        run: p2_numeric_arrow,
    },
    Workload {
        name: "populate (8192-row batches)",
        group: Group::NumericArrow,
        run: p2_numeric_arrow_batched,
    },
    Workload {
        name: "hand-written columnar Vec -> Arrow, no populate (8192-row batches)",
        group: Group::NumericArrow,
        run: raw_columnar_arrow_batched,
    },
    Workload {
        name: "hand-written row loop -> Arrow builders, no populate (8192-row batches)",
        group: Group::NumericArrow,
        run: raw_row_builders_arrow_batched,
    },
    Workload {
        name: "tpchgen rows -> Vec -> Arrow (8192-row batches)",
        group: Group::NumericArrow,
        run: tpch_numeric_arrow,
    },
    Workload {
        name: "tpchgen rows only, no output",
        group: Group::NumericArrow,
        run: tpch_rows,
    },
    Workload {
        name: "tpchgen RNGs + row logic -> columns, + string RNG draws",
        group: Group::NumericArrow,
        run: fair::tpch_direct_with_string_draws,
    },
    Workload {
        name: "tpchgen RNGs + row logic -> columns, numeric only (same output)",
        group: Group::NumericArrow,
        run: fair::tpch_direct_numeric,
    },
    Workload {
        name: "populate, every draw from tpchgen's RNG",
        group: Group::NumericArrow,
        run: fair::p2_uniform_tpch_rng,
    },
    Workload {
        name: "populate, tpchgen's RNG as lane-parallel leaves (same values)",
        group: Group::NumericArrow,
        run: fair::p2_uniform_tpch_rng_lanes,
    },
    Workload {
        name: "populate zero-copy writer",
        group: Group::NumericParquet,
        run: p2_numeric_parquet,
    },
    Workload {
        name: "populate RecordBatch -> ArrowWriter",
        group: Group::NumericParquet,
        run: p2_numeric_parquet_arrow_writer,
    },
    Workload {
        name: "tpchgen -> ArrowWriter",
        group: Group::NumericParquet,
        run: tpch_numeric_parquet,
    },
    Workload {
        name: "populate",
        group: Group::ListArrow,
        run: p2_list_arrow,
    },
    Workload {
        name: "tpchgen rows grouped by orderkey",
        group: Group::ListArrow,
        run: tpch_list_arrow,
    },
    Workload {
        name: "populate, price(partkey) * quantity",
        group: Group::ListArrow,
        run: fair::p2_list_fair,
    },
    Workload {
        name: "tpchgen, only the 3 RNG streams needed (same output)",
        group: Group::ListArrow,
        run: fair::tpch_list_direct,
    },
    Workload {
        name: "populate zero-copy writer",
        group: Group::ListParquet,
        run: p2_list_parquet,
    },
    Workload {
        name: "tpchgen grouped -> ArrowWriter",
        group: Group::ListParquet,
        run: tpch_list_parquet,
    },
    Workload {
        name: "tpchgen-arrow LineItemArrow -> Arrow",
        group: Group::TpchFull,
        run: tpch_full_arrow,
    },
    Workload {
        name: "tpchgen-arrow LineItemArrow -> ArrowWriter",
        group: Group::TpchFull,
        run: tpch_full_parquet,
    },
];

#[cfg(test)]
mod tests {
    use super::*;

    const N: usize = 20_000;

    #[test]
    fn numeric_outputs_match_in_shape() {
        tpchgen_init();
        let p2 = p2_numeric_arrow(N);
        let p2b = p2_numeric_arrow_batched(N);
        let tp = tpch_numeric_arrow(N);
        assert_eq!((p2.rows, p2b.rows, tp.rows), (N, N, N));
        // same 11 columns, same widths: 7 x i64 + 4 x i32 per row
        let data = N * (7 * 8 + 4 * 4);
        for o in [p2, p2b, tp] {
            assert!(o.bytes >= data, "{o:?}");
        }
    }

    #[test]
    fn batched_orderkeys_continue_across_batches() {
        let mut keys = Vec::new();
        p2_numeric_batches(101, 3 * BATCH + 5, BATCH, &mut rng(), |b| {
            let c = b
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("i64");
            keys.extend_from_slice(c.values());
        });
        let want: Vec<i64> = (101..101 + 3 * BATCH as i64 + 5).collect();
        assert_eq!(keys, want);
    }

    #[test]
    fn parquet_files_are_comparable() {
        tpchgen_init();
        let zc = p2_numeric_parquet(N);
        let aw = p2_numeric_parquet_arrow_writer(N);
        let tp = tpch_numeric_parquet(N);
        // plain encoding, no compression: files are about the raw data size
        let data = N * (7 * 8 + 4 * 4);
        for o in [zc, aw, tp] {
            assert_eq!(o.rows, N);
            assert!(o.bytes > data && o.bytes < data * 11 / 10, "{o:?}");
        }
    }

    #[test]
    fn list_workloads_have_tpch_shape() {
        tpchgen_init();
        let p2 = p2_list_arrow(4 * N);
        let tp = tpch_list_arrow(4 * N);
        // about N orders each (1..=7 lines per order, mean 4)
        for o in [p2, tp] {
            assert!(o.rows > N * 9 / 10 && o.rows < N * 11 / 10, "{o:?}");
        }
        let pq = p2_list_parquet(4 * N);
        let tq = tpch_list_parquet(4 * N);
        assert_eq!(pq.rows, N);
        assert!(pq.bytes > 0 && tq.bytes > 0);
    }

    #[test]
    fn uniform_stays_in_range() {
        let mut d = uniform(3i32, 9);
        let b = d.collect_record_batch(&mut rng(), 1000).expect("batch");
        let c = b
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("i32");
        assert!(c.values().iter().all(|v| (3..=9).contains(v)));
        assert_eq!(c.len(), 1000);
    }
}
