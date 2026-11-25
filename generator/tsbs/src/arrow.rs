//! Arrow output: one table per measurement (`cpu`, `mem`, ...,
//! `readings`, `diagnostics`), rows in the order the text formats write
//! them, with a `time` column, one column per tag and one per field.
//!
//! The simulation makes the points of every table in one interleaved
//! stream, so a chunk is run once and its points are logged per
//! measurement ([`Log`]): times and field values straight into the
//! columns they end up in, and each point's host or truck. Each table is
//! then one [`Distribution`] over its log: named leaf columns (time, each
//! tag, each extra tag, each field) in a tuple of run-time-width groups,
//! collected column by column. Its schema is [`schema_of`] that
//! distribution: the names it gives its columns, their types, and
//! nullability from the row types (`Option` where the run can leave a
//! value empty: IoT clears tags and fields, devops-generic hosts have
//! fewer fields than the table).

use crate::chunked::Chunked;
use crate::model::{Measurement, NO_TAG, Point, Statics, TagVal, Type, Val};
use crate::sim::AnySim;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use populate_builtins::{Named, named};
use populate_core::{
    Columnar, Columns, Distribution, Nullable, Sink, StrCol, Timestamp, time_unit,
};
use populate_sink_arrow::{IntoArrowColumns, into_record_batch_with, schema_of};
use populate_table::MultiChunks;
use rand::Rng;
use rand::SeedableRng;
use rand::rngs::SmallRng;
use std::cell::RefCell;
use std::marker::PhantomData;
use std::ops::Range;
use std::sync::Arc;

/// A point's time: Unix nanoseconds, UTC.
pub type Time = Timestamp<{ time_unit::NANOSECOND }, true>;

const ALWAYS: &str = "only nullable columns are left empty";

// ============================================================================
// Logged columns
// ============================================================================

/// A row type whose columns can be read back: the time and field columns
/// are logged in their final form, and a leaf hands them on.
trait Logged: Columnar<Columns: Clone> {
    /// Row `i` of `cols`.
    fn row(cols: &Self::Columns, i: usize) -> Self;

    /// Append `src`'s rows `r` to `dst`.
    fn extend(dst: &mut Self::Columns, src: &Self::Columns, r: Range<usize>);

    fn len(cols: &Self::Columns) -> usize;

    /// The row of a logged field value, missing where the point has none.
    fn of_val(v: Option<Val>) -> Self;
}

macro_rules! logged {
    ($($t:ty => $of:expr),* $(,)?) => {$(
        impl Logged for $t {
            fn row(cols: &Vec<$t>, i: usize) -> $t {
                cols[i]
            }

            fn extend(dst: &mut Vec<$t>, src: &Vec<$t>, r: Range<usize>) {
                dst.extend_from_slice(&src[r]);
            }

            fn len(cols: &Vec<$t>) -> usize {
                cols.len()
            }

            #[inline]
            fn of_val(v: Option<Val>) -> $t {
                let f: fn(Val) -> Option<$t> = $of;
                v.and_then(f).expect(ALWAYS)
            }
        }

        impl Logged for Option<$t> {
            fn row(cols: &Nullable<Vec<$t>>, i: usize) -> Option<$t> {
                cols.valid.get(i).then(|| cols.values[i])
            }

            fn extend(dst: &mut Nullable<Vec<$t>>, src: &Nullable<Vec<$t>>, r: Range<usize>) {
                dst.valid.reserve(r.len());
                for i in r.clone() {
                    dst.valid.push(src.valid.get(i));
                }
                dst.values.extend_from_slice(&src.values[r]);
            }

            fn len(cols: &Nullable<Vec<$t>>) -> usize {
                cols.len()
            }

            #[inline]
            fn of_val(v: Option<Val>) -> Option<$t> {
                let f: fn(Val) -> Option<$t> = $of;
                v.and_then(f)
            }
        }
    )*};
}

logged!(
    i64 => |v| match v {
        Val::I(i) => Some(i),
        _ => None,
    },
    f64 => Val::as_f64,
);

impl Logged for Time {
    fn row(cols: &Vec<Time>, i: usize) -> Time {
        cols[i]
    }

    fn extend(dst: &mut Vec<Time>, src: &Vec<Time>, r: Range<usize>) {
        dst.extend_from_slice(&src[r]);
    }

    fn len(cols: &Vec<Time>) -> usize {
        cols.len()
    }

    fn of_val(_: Option<Val>) -> Time {
        unreachable!("times are logged from the point")
    }
}

/// A column already made: its rows in order. Collecting all of them at
/// once into empty columns hands the buffers over as they are.
struct Column<S: Logged> {
    cols: Columns<S>,
    at: usize,
}

impl<S: Logged> Clone for Column<S> {
    fn clone(&self) -> Self {
        Column {
            cols: self.cols.clone(),
            at: self.at,
        }
    }
}

impl<S: Logged> Column<S> {
    fn new(cols: Columns<S>) -> Self {
        Column { cols, at: 0 }
    }

    fn take(&mut self, n: usize) -> Range<usize> {
        let r = self.at..self.at + n;
        let len = S::len(&self.cols);
        assert!(r.end <= len, "the column has {len} rows");
        self.at = r.end;
        r
    }
}

impl<S: Logged> Distribution for Column<S> {
    type Item = S;

    fn sample(&mut self, _: &mut impl Rng) -> S {
        let i = self.take(1).start;
        S::row(&self.cols, i)
    }

    fn collect_into(&mut self, cols: &mut Columns<S>, _: &mut impl Rng, n: usize) {
        let r = self.take(n);
        if r.start == 0 && r.end == S::len(&self.cols) && S::len(cols) == 0 {
            // every row, into nothing yet: the buffers are the result
            std::mem::swap(cols, &mut self.cols);
        } else {
            S::extend(cols, &self.cols, r);
        }
    }

    fn skip(&mut self, n: u64) {
        self.at += usize::try_from(n).expect("a chunk fits in memory");
    }
}

// ============================================================================
// Tags
// ============================================================================

/// Each logged point's host or truck, and the tag its simulator cleared.
#[derive(Debug, Clone, Default)]
struct Sources {
    source: Vec<u32>,
    zero_tag: Vec<u8>,
}

/// A tag leaf's place in the [`Sources`] it reads.
#[derive(Debug, Clone)]
struct Rows {
    sources: Arc<Sources>,
    at: usize,
}

impl Rows {
    fn take(&mut self, n: usize) -> Range<usize> {
        let r = self.at..self.at + n;
        let len = self.sources.source.len();
        assert!(r.end <= len, "the table has {len} rows");
        self.at = r.end;
        r
    }

    fn skip(&mut self, n: u64) {
        self.at += usize::try_from(n).expect("a chunk fits in memory");
    }
}

/// How a tag column holds a value of type `V`: as `V` itself where the
/// run always has one, or `Option<V>` where it can be cleared.
pub trait Slot<V>: Columnar {
    fn of(v: Option<V>) -> Self;

    /// Append `n` values.
    fn put_n(cols: &mut Self::Columns, n: usize, vs: impl Iterator<Item = Option<V>>);
}

impl<'a> Slot<&'a str> for String {
    fn of(v: Option<&str>) -> Self {
        v.expect(ALWAYS).to_owned()
    }

    #[inline]
    fn put_n(cols: &mut StrCol, n: usize, vs: impl Iterator<Item = Option<&'a str>>) {
        let mut vs = vs.map(|v| v.expect(ALWAYS));
        cols.push_n_with(n, |b| b.push_str(vs.next().expect("n values")));
    }
}

impl<'a> Slot<&'a str> for Option<String> {
    fn of(v: Option<&str>) -> Self {
        v.map(str::to_owned)
    }

    #[inline]
    fn put_n(cols: &mut Nullable<StrCol>, n: usize, mut vs: impl Iterator<Item = Option<&'a str>>) {
        let valid = &mut cols.valid;
        valid.reserve(n);
        cols.values.push_n_with(n, |b| {
            let v = vs.next().expect("n values");
            valid.push(v.is_some());
            b.push_str(v.unwrap_or_default());
        });
    }
}

impl Slot<f32> for f32 {
    fn of(v: Option<f32>) -> Self {
        v.expect(ALWAYS)
    }

    #[inline]
    fn put_n(cols: &mut Vec<f32>, n: usize, vs: impl Iterator<Item = Option<f32>>) {
        cols.reserve(n);
        cols.extend(vs.map(|v| v.expect(ALWAYS)));
    }
}

impl Slot<f32> for Option<f32> {
    fn of(v: Option<f32>) -> Self {
        v
    }

    #[inline]
    fn put_n(cols: &mut Nullable<Vec<f32>>, n: usize, vs: impl Iterator<Item = Option<f32>>) {
        cols.reserve(n);
        for v in vs {
            cols.push(v);
        }
    }
}

/// Each tag's value per host or truck, laid out by tag so a column reads
/// one array.
#[derive(Debug, Default)]
struct TagValues {
    strs: Vec<Arc<StrTable>>,
    f32s: Vec<Arc<Vec<f32>>>,
}

impl TagValues {
    fn new(st: &Statics) -> Self {
        let mut t = TagValues::default();
        for (k, ty) in st.tag_types.iter().enumerate() {
            let vals = st.gen_tags.iter().map(|g| &g[k]);
            if *ty == Type::F32 {
                let v = vals.map(|v| match *v {
                    TagVal::F32(f) => f,
                    TagVal::Str(_) => unreachable!("a float32 tag"),
                });
                t.f32s.push(Arc::new(v.collect()));
            } else {
                let mut tab = StrTable {
                    offsets: vec![0],
                    bytes: Vec::new(),
                };
                for v in vals {
                    let TagVal::Str(s) = v else {
                        unreachable!("a string tag")
                    };
                    tab.bytes.extend_from_slice(s.as_bytes());
                    tab.offsets.push(tab.bytes.len());
                }
                t.strs.push(Arc::new(tab));
            }
        }
        t
    }
}

/// Strings by index, in one buffer.
#[derive(Debug)]
struct StrTable {
    offsets: Vec<usize>,
    bytes: Vec<u8>,
}

/// A kind of tag: where its values are and how one is read.
trait TagKind {
    type Values: std::fmt::Debug;
    type Out<'a>;
    /// The `i`th tag of this kind.
    fn values(t: &TagValues, i: usize) -> Arc<Self::Values>;
    fn get(v: &Self::Values, source: usize) -> Self::Out<'_>;
}

/// String tags.
#[derive(Debug)]
struct StrTag;

/// Float32 tags (IoT's capacities).
#[derive(Debug)]
struct F32Tag;

impl TagKind for StrTag {
    type Values = StrTable;
    type Out<'a> = &'a str;

    fn values(t: &TagValues, i: usize) -> Arc<StrTable> {
        t.strs[i].clone()
    }

    #[inline(always)]
    fn get(v: &StrTable, source: usize) -> &str {
        let b = &v.bytes[v.offsets[source]..v.offsets[source + 1]];
        // SAFETY: the table holds whole `str`s, split at their ends
        unsafe { std::str::from_utf8_unchecked(b) }
    }
}

impl TagKind for F32Tag {
    type Values = Vec<f32>;
    type Out<'a> = f32;

    fn values(t: &TagValues, i: usize) -> Arc<Vec<f32>> {
        t.f32s[i].clone()
    }

    #[inline(always)]
    fn get(v: &Vec<f32>, source: usize) -> f32 {
        v[source]
    }
}

/// Tag `k` of each point's host or truck, missing where the simulator
/// cleared it: a string or a float32 (`K`).
#[derive(Debug)]
struct TagCol<S, K: TagKind> {
    rows: Rows,
    /// Tag `k`'s value per host or truck.
    values: Arc<K::Values>,
    k: usize,
    _slot: PhantomData<fn() -> S>,
}

impl<S, K: TagKind> Clone for TagCol<S, K> {
    fn clone(&self) -> Self {
        TagCol {
            rows: self.rows.clone(),
            values: self.values.clone(),
            k: self.k,
            _slot: PhantomData,
        }
    }
}

impl<S, K: TagKind> TagCol<S, K> {
    #[inline(always)]
    fn value<'a>(values: &'a K::Values, src: &Sources, k: usize, i: usize) -> Option<K::Out<'a>> {
        let z = src.zero_tag[i];
        (z == NO_TAG || usize::from(z) != k).then(|| K::get(values, src.source[i] as usize))
    }
}

impl<S, K> Distribution for TagCol<S, K>
where
    K: TagKind,
    S: for<'a> Slot<K::Out<'a>>,
{
    type Item = S;

    fn sample(&mut self, _: &mut impl Rng) -> S {
        let i = self.rows.take(1).start;
        S::of(Self::value(&self.values, &self.rows.sources, self.k, i))
    }

    fn collect_into(&mut self, cols: &mut Columns<S>, _: &mut impl Rng, n: usize) {
        let r = self.rows.take(n);
        let (values, src, k) = (&*self.values, &*self.rows.sources, self.k);
        S::put_n(cols, n, r.map(|i| Self::value(values, src, k, i)));
    }

    fn skip(&mut self, n: u64) {
        self.rows.skip(n);
    }
}

/// Extra tag `k` of measurement `m` (devops `disk`'s `path`, ...): never
/// missing.
#[derive(Debug, Clone)]
struct ExtraCol {
    rows: Rows,
    st: Arc<Statics>,
    m: usize,
    k: usize,
}

impl ExtraCol {
    fn value(&self, i: usize) -> &str {
        &self.st.extra_tags[self.rows.sources.source[i] as usize][self.m][self.k]
    }
}

impl Distribution for ExtraCol {
    type Item = String;

    fn sample(&mut self, _: &mut impl Rng) -> String {
        let i = self.rows.take(1).start;
        self.value(i).to_owned()
    }

    fn collect_into(&mut self, cols: &mut StrCol, _: &mut impl Rng, n: usize) {
        let mut r = self.rows.take(n);
        cols.push_n_with(n, |b| b.push_str(self.value(r.next().expect("n rows"))));
    }

    fn skip(&mut self, n: u64) {
        self.rows.skip(n);
    }
}

// ============================================================================
// Tables
// ============================================================================

/// One measurement's points from one chunk, in order: `IS`/`DS` are its
/// int64 and float64 field rows.
struct Log<IS: Logged, DS: Logged> {
    time: Vec<Time>,
    sources: Sources,
    /// Per field, in order: whether it is a float64, and its index in
    /// `floats` or `ints`.
    slots: Vec<(bool, usize)>,
    ints: Vec<Columns<IS>>,
    floats: Vec<Columns<DS>>,
}

impl<IS: Logged, DS: Logged> Log<IS, DS> {
    /// Empty, for measurement `m`, with room for `rows` points, reusing
    /// the `sources` buffers of an earlier chunk.
    fn new(m: &Measurement, mut sources: Sources, rows: usize) -> Self {
        sources.source.clear();
        sources.zero_tag.clear();
        sources.source.reserve(rows);
        sources.zero_tag.reserve(rows);
        let mut log = Log {
            time: Vec::with_capacity(rows),
            sources,
            slots: Vec::with_capacity(m.types.len()),
            ints: Vec::new(),
            floats: Vec::new(),
        };
        for &t in &m.types {
            if t == Type::F64 {
                let mut c = Columns::<DS>::default();
                c.reserve(rows);
                log.slots.push((true, log.floats.len()));
                log.floats.push(c);
            } else {
                let mut c = Columns::<IS>::default();
                c.reserve(rows);
                log.slots.push((false, log.ints.len()));
                log.ints.push(c);
            }
        }
        log
    }

    #[inline]
    fn push(&mut self, p: &Point) {
        self.time.push(Timestamp(p.ts));
        self.sources.source.push(p.source);
        self.sources.zero_tag.push(p.zero_tag);
        let mut vals = p.fields.iter().copied();
        for &(float, i) in &self.slots {
            let v = vals.next();
            if float {
                self.floats[i].push(DS::of_val(v));
            } else {
                self.ints[i].push(IS::of_val(v));
            }
        }
    }
}

/// A thread's [`Sources`] buffers, reused from chunk to chunk, and each
/// measurement's points in its last chunk, to size the next one's
/// columns.
#[derive(Default)]
struct Pool {
    sources: Vec<Sources>,
    rows: Vec<usize>,
}

thread_local! {
    static POOL: RefCell<Pool> = RefCell::default();
}

/// One measurement table: `time`, the string tags, the float32 tags, the
/// extra tags, and the fields as runs of one type (float64s, int64s,
/// float64s: every TSBS measurement's fields fit that).
type MeasDist<TS, FS, IS, DS> = (
    Named<Column<Time>>,
    Vec<Named<TagCol<TS, StrTag>>>,
    Vec<Named<TagCol<FS, F32Tag>>>,
    Vec<Named<ExtraCol>>,
    Vec<Named<Column<DS>>>,
    Vec<Named<Column<IS>>>,
    Vec<Named<Column<DS>>>,
);

/// The row types of a run's tables: `TS`/`FS` string and float32 tags,
/// `IS`/`DS` int64 and float64 fields.
trait Slots: 'static {
    type TS: for<'a> Slot<&'a str>;
    type FS: Slot<f32>;
    type IS: Logged;
    type DS: Logged;
}

/// Devops: nothing is ever missing.
struct Devops;

/// Devops-generic: hosts have fewer fields than the table.
struct Generic;

/// IoT: tags and fields can be cleared.
struct Iot;

impl Slots for Devops {
    type TS = String;
    type FS = f32;
    type IS = i64;
    type DS = f64;
}

impl Slots for Generic {
    type TS = String;
    type FS = f32;
    type IS = Option<i64>;
    type DS = Option<f64>;
}

impl Slots for Iot {
    type TS = Option<String>;
    type FS = Option<f32>;
    type IS = Option<i64>;
    type DS = Option<f64>;
}

type Dist<R> = MeasDist<<R as Slots>::TS, <R as Slots>::FS, <R as Slots>::IS, <R as Slots>::DS>;

/// Every measurement's table of a run whose rows are `R`'s.
struct Typed<R> {
    chunks: Chunked<AnySim>,
    st: Arc<Statics>,
    tags: TagValues,
    schemas: Vec<SchemaRef>,
    _slots: PhantomData<fn() -> R>,
}

impl<R: Slots> Typed<R>
where
    Columns<<Dist<R> as Distribution>::Item>: IntoArrowColumns,
{
    fn new(chunks: Chunked<AnySim>) -> Self {
        let st = chunks.statics();
        let tags = TagValues::new(&st);
        let schemas = st
            .measurements
            .iter()
            .enumerate()
            .map(|(m, meas)| {
                let log = Log::new(meas, Sources::default(), 0);
                let d = Self::dist(&st, &tags, m, log, &Arc::default());
                Arc::new(schema_of(&d))
            })
            .collect();
        Typed {
            chunks,
            st,
            tags,
            schemas,
            _slots: PhantomData,
        }
    }

    /// Measurement `m`'s table over `log`, whose points' hosts or trucks
    /// are `sources`.
    fn dist(
        st: &Arc<Statics>,
        tags: &TagValues,
        m: usize,
        log: Log<R::IS, R::DS>,
        sources: &Arc<Sources>,
    ) -> Dist<R> {
        fn tag_cols<S, K>(
            st: &Statics,
            tags: &TagValues,
            rows: &Rows,
            ks: Range<usize>,
        ) -> Vec<Named<TagCol<S, K>>>
        where
            K: TagKind,
            S: for<'a> Slot<K::Out<'a>>,
        {
            let first = ks.start;
            ks.map(|k| {
                let c = TagCol {
                    rows: rows.clone(),
                    values: K::values(tags, k - first),
                    k,
                    _slot: PhantomData,
                };
                named(&st.tag_keys[k], c)
            })
            .collect()
        }

        fn field_cols<S: Logged>(
            keys: &[Box<str>],
            cols: impl Iterator<Item = Columns<S>>,
        ) -> Vec<Named<Column<S>>> {
            keys.iter()
                .zip(cols)
                .map(|(key, c)| named(key, Column::new(c)))
                .collect()
        }

        let meas = &st.measurements[m];
        let rows = Rows {
            sources: sources.clone(),
            at: 0,
        };
        let n_str = st.tag_types.iter().take_while(|&&t| t != Type::F32).count();
        assert!(
            st.tag_types[n_str..].iter().all(|&t| t == Type::F32),
            "string tags precede float32 tags"
        );
        let extra = meas
            .extra_tags
            .iter()
            .enumerate()
            .map(|(k, key)| {
                let c = ExtraCol {
                    rows: rows.clone(),
                    st: st.clone(),
                    m,
                    k,
                };
                named(key, c)
            })
            .collect();
        let lead = meas.types.iter().take_while(|&&t| t == Type::F64).count();
        let ints = lead
            + meas.types[lead..]
                .iter()
                .take_while(|&&t| t != Type::F64)
                .count();
        assert!(
            meas.types[ints..].iter().all(|&t| t == Type::F64),
            "{}: fields are float64s, int64s, float64s",
            meas.name
        );
        let keys = &meas.fields;
        let mut floats = log.floats.into_iter();
        (
            named("time", Column::new(log.time)),
            tag_cols(st, tags, &rows, 0..n_str),
            tag_cols(st, tags, &rows, n_str..st.tag_keys.len()),
            extra,
            field_cols(&keys[..lead], floats.by_ref().take(lead)),
            field_cols(&keys[lead..ints], log.ints.into_iter()),
            field_cols(&keys[ints..], floats),
        )
    }
}

/// A run's tables, whichever row types they have.
trait Tables: Send + Sync {
    fn chunks(&self) -> &Chunked<AnySim>;
    fn schemas(&self) -> &[SchemaRef];
    fn chunk(&self, i: u64, emit: &mut dyn FnMut(usize, RecordBatch));
}

impl<R: Slots> Tables for Typed<R>
where
    Columns<<Dist<R> as Distribution>::Item>: IntoArrowColumns,
{
    fn chunks(&self) -> &Chunked<AnySim> {
        &self.chunks
    }

    fn schemas(&self) -> &[SchemaRef] {
        &self.schemas
    }

    fn chunk(&self, i: u64, emit: &mut dyn FnMut(usize, RecordBatch)) {
        let st = &*self.st;
        let n = st.measurements.len();
        let mut pool = POOL.with_borrow_mut(std::mem::take);
        pool.sources.resize_with(n, Sources::default);
        pool.rows.resize(n, 0);
        let mut logs: Vec<Log<R::IS, R::DS>> = st
            .measurements
            .iter()
            .zip(pool.sources.drain(..))
            .zip(&pool.rows)
            .map(|((m, s), &rows)| Log::new(m, s, rows))
            .collect();
        self.chunks
            .run(i, |_: &AnySim, p| logs[p.meas as usize].push(p));
        for (m, mut log) in logs.into_iter().enumerate() {
            let rows = log.time.len();
            pool.rows[m] = rows;
            let sources = Arc::new(std::mem::take(&mut log.sources));
            if rows > 0 {
                let mut d = Self::dist(&self.st, &self.tags, m, log, &sources);
                // the leaves read the log and draw nothing
                let mut rng = SmallRng::seed_from_u64(0);
                let cols = d.collect(&mut rng, rows);
                drop(d);
                let b = into_record_batch_with(&self.schemas[m], cols)
                    .unwrap_or_else(|e| panic!("{}: {e}", st.measurements[m].name));
                emit(m, b);
            }
            pool.sources
                .push(Arc::try_unwrap(sources).unwrap_or_default());
        }
        POOL.set(pool);
    }
}

/// A TSBS run as Arrow tables, one per measurement, on the
/// [`populate_table`] chunk engine ([`MultiChunks`]: Parquet via
/// `write_parquet_many`, batches via `par_sources`).
pub struct TsbsArrow {
    statics: Arc<Statics>,
    tables: Box<dyn Tables>,
}

impl TsbsArrow {
    /// Arrow tables of `chunks`. IoT tags and fields can be missing
    /// (nullable columns), and devops-generic hosts' fields.
    pub fn new(chunks: Chunked<AnySim>, iot: bool, generic: bool) -> Self {
        let statics = chunks.statics();
        let tables: Box<dyn Tables> = if iot {
            Box::new(Typed::<Iot>::new(chunks))
        } else if generic {
            Box::new(Typed::<Generic>::new(chunks))
        } else {
            Box::new(Typed::<Devops>::new(chunks))
        };
        TsbsArrow { statics, tables }
    }

    /// Measurement (table) names, in output order.
    pub fn table_names(&self) -> Vec<String> {
        self.statics
            .measurements
            .iter()
            .map(|m| m.name.to_string())
            .collect()
    }
}

impl MultiChunks for TsbsArrow {
    fn schemas(&self) -> Vec<SchemaRef> {
        self.tables.schemas().to_vec()
    }

    fn num_chunks(&self) -> u64 {
        self.tables.chunks().num_chunks()
    }

    fn chunk(&self, i: u64, emit: &mut dyn FnMut(usize, RecordBatch)) {
        self.tables.chunk(i, emit);
    }
}
