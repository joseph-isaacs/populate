//! The YCSB run phase (`ycsb run`, `CoreWorkload` transactions): the
//! operations a database binding receives, byte-identical to YCSB 0.17.0
//! given the client thread's random seed.
//!
//! Every YCSB choice draws a fixed number of values from the client
//! thread's `ThreadLocalRandom` (a counter, [`JavaTlr`]; a `nextDouble` is
//! two draws): the operation (one
//! `nextDouble`), the key (`uniform`: one `nextLong`; `zipfian`: one
//! `nextDouble`, YCSB's scrambled zipfian over 10^10 items; `latest`: one
//! `nextDouble`, a zipfian back from the newest key; `hotspot`: two), the
//! field and scan length, and value bytes as in the load. Only two things
//! make an operation depend on the ones before it: a key past the last
//! inserted one is drawn again, and inserts move the newest key. So a quick
//! sequential pass computes the generator state at the start of each chunk
//! (draw position, next insert key, `latest`'s zipfian sums), and chunks are
//! then generated independently, in parallel.
//!
//! Each row is one call into the binding: a read-modify-write is a `READ`
//! row then an `UPDATE` row, as the binding sees it.

use crate::engine::DEFAULT_BATCH_SIZE;
use crate::rng::JavaTlr;
use crate::strings::Views;
use crate::ycsb::{Ycsb, deterministic_value, fnv_hash64, java_hash, java_hashmap_order, six};
use arrow_array::{ArrayRef, Int32Array, RecordBatch};
use arrow_buffer::{BooleanBuffer, NullBuffer};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use parquet::file::properties::WriterPropertiesBuilder;
use parquet::schema::types::ColumnPath;
use populate_table::text::TextChunks;
use populate_table::{Chunks, chunks};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};

/// Default operations per chunk.
pub const YCSB_RUN_CHUNK_OPS: u64 = 64 * 1024;

/// YCSB's zipfian constant.
const THETA: f64 = 0.99;
/// `ScrambledZipfianGenerator`'s item count and its precomputed zeta.
const SCRAMBLED_ITEMS: i64 = 10_000_000_000;
const SCRAMBLED_ZETAN: f64 = 26.46902820178302;

/// How keys are chosen (`requestdistribution`).
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RequestDistribution {
    Uniform,
    /// YCSB's scrambled zipfian (constant 0.99): popular keys spread over
    /// the key space.
    Zipfian,
    /// Zipfian from the most recently inserted key back.
    Latest,
    /// `data_fraction` of the keys get `op_fraction` of the operations
    /// (`hotspotdatafraction`, `hotspotopnfraction`; YCSB's defaults 0.2
    /// and 0.8).
    Hotspot {
        data_fraction: f64,
        op_fraction: f64,
    },
}

/// The operations of `ycsb run` with `CoreWorkload`, on top of a load.
///
/// ```
/// use populate_oltp::{Ycsb, YcsbRun};
///
/// // workload A: 50% reads, 50% updates, zipfian keys
/// let run = YcsbRun::workload('a', Ycsb::new(1000), 10_000);
/// let tsv = run.write_tsv(Vec::new(), 0).unwrap();
/// assert_eq!(tsv.split(|&b| b == b'\n').count() - 1, 10_000);
/// ```
#[derive(Debug, Clone)]
pub struct YcsbRun {
    load: Ycsb,
    operation_count: u64,
    /// read, update, insert, scan, read-modify-write
    proportions: [f64; 5],
    distribution: RequestDistribution,
    read_all_fields: bool,
    write_all_fields: bool,
    scan: (i64, i64),
    scan_zipfian: bool,
    chunk_ops: u64,
    batch: usize,
    /// Generator states at the chunk starts, extended on demand.
    checkpoints: Arc<Mutex<Vec<State>>>,
    /// The zipfian scan lengths' zeta, summed once per run.
    scan_zeta: Arc<OnceLock<f64>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Op {
    Read,
    Update,
    Insert,
    Scan,
    ReadModifyWrite,
}

const OPS: [Op; 5] = [
    Op::Read,
    Op::Update,
    Op::Insert,
    Op::Scan,
    Op::ReadModifyWrite,
];

impl YcsbRun {
    /// `operationcount` operations on `load`'s records, with YCSB's
    /// defaults: 95% reads, 5% updates, uniform keys, all fields read, one
    /// field written, scans of 1 to 1,000 records.
    pub fn new(load: Ycsb, operation_count: u64) -> Self {
        YcsbRun {
            load,
            operation_count,
            proportions: [0.95, 0.05, 0.0, 0.0, 0.0],
            distribution: RequestDistribution::Uniform,
            read_all_fields: true,
            write_all_fields: false,
            scan: (1, 1000),
            scan_zipfian: false,
            chunk_ops: YCSB_RUN_CHUNK_OPS,
            batch: DEFAULT_BATCH_SIZE,
            checkpoints: Arc::default(),
            scan_zeta: Arc::default(),
        }
    }

    /// YCSB's core workloads `a` to `f` (`workloads/workloada` ...).
    pub fn workload(name: char, load: Ycsb, operation_count: u64) -> Self {
        use RequestDistribution::*;
        let w = YcsbRun::new(load, operation_count);
        match name.to_ascii_lowercase() {
            'a' => w.proportions(0.5, 0.5, 0.0, 0.0, 0.0).distribution(Zipfian),
            'b' => w
                .proportions(0.95, 0.05, 0.0, 0.0, 0.0)
                .distribution(Zipfian),
            'c' => w.proportions(1.0, 0.0, 0.0, 0.0, 0.0).distribution(Zipfian),
            'd' => w
                .proportions(0.95, 0.0, 0.05, 0.0, 0.0)
                .distribution(Latest),
            'e' => w
                .proportions(0.0, 0.0, 0.05, 0.95, 0.0)
                .distribution(Zipfian)
                .scan_length(1, 100, false),
            'f' => w.proportions(0.5, 0.0, 0.0, 0.0, 0.5).distribution(Zipfian),
            c => panic!("unknown YCSB workload `{c}` (a to f)"),
        }
    }

    fn reset(mut self) -> Self {
        self.checkpoints = Arc::default();
        self.scan_zeta = Arc::default();
        self
    }

    /// `readproportion`, `updateproportion`, `insertproportion`,
    /// `scanproportion`, `readmodifywriteproportion`.
    pub fn proportions(mut self, read: f64, update: f64, insert: f64, scan: f64, rmw: f64) -> Self {
        let p = [read, update, insert, scan, rmw];
        assert!(
            p.iter().all(|&x| x >= 0.0 && x.is_finite()) && p.iter().any(|&x| x > 0.0),
            "proportions must be finite and >= 0, and one > 0"
        );
        self.proportions = p;
        self.reset()
    }

    /// `requestdistribution`.
    pub fn distribution(mut self, d: RequestDistribution) -> Self {
        if let RequestDistribution::Hotspot {
            data_fraction,
            op_fraction,
        } = d
        {
            assert!(
                (0.0..=1.0).contains(&data_fraction) && (0.0..=1.0).contains(&op_fraction),
                "hotspot fractions must be in 0..=1"
            );
        }
        self.distribution = d;
        self.reset()
    }

    /// `readallfields` (default true).
    pub fn read_all_fields(mut self, on: bool) -> Self {
        self.read_all_fields = on;
        self.reset()
    }

    /// `writeallfields` (default false).
    pub fn write_all_fields(mut self, on: bool) -> Self {
        self.write_all_fields = on;
        self.reset()
    }

    /// `minscanlength`, `maxscanlength`, and `scanlengthdistribution`
    /// (`zipfian` if true, else `uniform`).
    pub fn scan_length(mut self, min: i64, max: i64, zipfian: bool) -> Self {
        assert!(0 <= min && min <= max, "scan lengths: 0 <= min <= max");
        self.scan = (min, max);
        self.scan_zipfian = zipfian;
        self.reset()
    }

    /// Operations per chunk (default 64Ki).
    pub fn chunk_ops(mut self, n: u64) -> Self {
        assert!(n > 0, "chunk_ops must be > 0");
        self.chunk_ops = n;
        self.reset()
    }

    /// Rows per Arrow batch (default 8Ki).
    pub fn batch_size(mut self, n: usize) -> Self {
        assert!(n > 0, "batch_size must be > 0");
        self.batch = n;
        self
    }

    /// `readproportion`, `updateproportion`, `insertproportion`,
    /// `scanproportion`, `readmodifywriteproportion`.
    pub fn proportion_values(&self) -> [f64; 5] {
        self.proportions
    }

    /// `(minscanlength, maxscanlength)`.
    pub fn scan_lengths(&self) -> (i64, i64) {
        self.scan
    }

    /// Whether scan lengths are zipfian.
    pub fn scan_zipfian(&self) -> bool {
        self.scan_zipfian
    }

    /// Parquet properties for [`YcsbRun::schema`]: dictionaries for the
    /// operation and fields-read columns only.
    pub fn parquet_props() -> WriterPropertiesBuilder {
        Ycsb::parquet_props()
            .set_column_dictionary_enabled(ColumnPath::from("op"), true)
            .set_column_dictionary_enabled(ColumnPath::from("read_fields"), true)
            .set_column_dictionary_enabled(ColumnPath::from("scan_length"), true)
    }

    pub fn operation_count(&self) -> u64 {
        self.operation_count
    }

    pub fn num_chunks(&self) -> u64 {
        self.operation_count.div_ceil(self.chunk_ops)
    }

    /// The properties YCSB refuses, as its `CoreWorkload` does, and those
    /// this generator does not support.
    pub fn check(&self) -> Result<(), String> {
        self.load.check()?;
        if self.load.record_count == 0 {
            return Err("recordcount must be > 0".into());
        }
        if self.operation_count > i32::MAX as u64 {
            return Err("operationcount is an int in YCSB".into());
        }
        if self.distribution == RequestDistribution::Latest && self.load.record_count < 2 {
            return Err("latest needs at least 2 records".into());
        }
        let default_count =
            self.load.record_count - self.load.insert_start.min(self.load.record_count);
        if self.load.insert_count.unwrap_or(default_count) > i32::MAX as u64 {
            return Err("insertcount is an int in YCSB".into());
        }
        let count = self.insert_count();
        let uniform_or_hot = matches!(
            self.distribution,
            RequestDistribution::Uniform | RequestDistribution::Hotspot { .. }
        );
        if count <= 0 && uniform_or_hot {
            return Err("no records to choose keys from (insertcount is 0)".into());
        }
        let p = &self.proportions;
        let reads_one = !self.read_all_fields && (p[0] > 0.0 || p[3] > 0.0 || p[4] > 0.0);
        let writes_one = !self.write_all_fields && (p[1] > 0.0 || p[4] > 0.0);
        if self.load.field_count == 0 && (reads_one || writes_one) {
            return Err("fieldcount 0: YCSB divides by it to choose a field".into());
        }
        if let RequestDistribution::Hotspot {
            data_fraction,
            op_fraction,
        } = self.distribution
        {
            // YCSB divides by the hot and cold set sizes
            let hot = (count as f64 * data_fraction) as i32 as i64;
            if hot <= 0 && op_fraction > 0.0 {
                return Err("hotspot: the hot set is empty".into());
            }
            if count - hot <= 0 && op_fraction < 1.0 {
                return Err("hotspot: the cold set is empty".into());
            }
        }
        Ok(())
    }

    /// `op`, `ycsb_key`, `scan_length` (scans), `read_fields` (the fields
    /// a read or scan asks for, comma-separated; NULL for all), then the
    /// values written (NULL where not written).
    pub fn schema(&self) -> SchemaRef {
        let mut f = vec![
            Field::new("op", DataType::Utf8View, false),
            Field::new("ycsb_key", DataType::Utf8View, false),
            Field::new("scan_length", DataType::Int32, true),
            Field::new("read_fields", DataType::Utf8View, true),
        ];
        f.extend(
            self.load
                .field_names()
                .into_iter()
                .map(|n| Field::new(n, DataType::Utf8View, true)),
        );
        Arc::new(Schema::new(f))
    }

    fn insert_count(&self) -> i64 {
        // CoreWorkload's own default, unlike the load client's
        self.load
            .insert_count
            .unwrap_or(self.load.record_count - self.load.insert_start.min(self.load.record_count))
            as i64
    }

    fn chooser(&self) -> Chooser {
        let start = self.load.insert_start as i64;
        let count = self.insert_count();
        match self.distribution {
            RequestDistribution::Uniform => Chooser::Uniform {
                lb: start,
                interval: Rem::new(count),
            },
            RequestDistribution::Zipfian => {
                let new_keys =
                    (self.operation_count as i32 as f64 * self.proportions[2] * 2.0) as i32 as i64;
                Chooser::Scrambled {
                    min: start,
                    items: Rem::new(count + new_keys + 1),
                }
            }
            RequestDistribution::Latest => Chooser::Latest,
            RequestDistribution::Hotspot {
                data_fraction,
                op_fraction,
            } => {
                let hot = (count as f64 * data_fraction) as i32 as i64;
                Chooser::Hotspot {
                    lb: start,
                    hot: Rem::new(hot),
                    cold: Rem::new(count - hot),
                    op_fraction,
                }
            }
        }
    }

    fn initial(&self) -> State {
        let latest = (self.distribution == RequestDistribution::Latest)
            .then(|| Zipf::new(0, self.load.record_count as i64 - 2, None));
        State {
            pos: 0,
            next_insert: self.load.record_count as i64,
            latest,
        }
    }

    /// The generator state at the start of every chunk: one pass over the
    /// operations without building keys or values.
    /// The generator state at the start of chunk `i`. The states are found
    /// by a cheap pass that only advances the generators, one chunk at a
    /// time under a lock, by whichever thread first needs one: so the pass
    /// runs alongside the other threads' chunks instead of before them.
    fn checkpoint(&self, i: u64) -> State {
        let mut v = self
            .checkpoints
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if v.is_empty() {
            v.push(self.initial());
        }
        if v.len() as u64 <= i {
            let g = Gen::new(self);
            while v.len() as u64 <= i {
                let k = v.len() as u64 - 1;
                let mut st = v[k as usize].clone();
                let first = k * self.chunk_ops;
                for _ in first..(first + self.chunk_ops).min(self.operation_count) {
                    g.op(&mut st, None);
                }
                v.push(st);
            }
        }
        v[i as usize].clone()
    }

    /// Operations `first..end` of chunk `i`, as rows passed to `row`.
    fn run_chunk(&self, i: u64, row: &mut dyn FnMut(&Row)) {
        let g = Gen::new(self);
        let mut st = self.checkpoint(i);
        let first = i * self.chunk_ops;
        let end = (first + self.chunk_ops).min(self.operation_count);
        let mut rows = [g.new_row(), g.new_row()];
        for _ in first..end {
            g.op(&mut st, Some((&mut rows, &mut *row)));
        }
    }

    fn generate(&self, i: u64, emit: &mut dyn FnMut(RecordBatch)) {
        let schema = self.schema();
        let new = || Builder::new(self.load.field_names(), self.batch, self.load.field_length);
        let mut b = new();
        self.run_chunk(i, &mut |r| {
            b.push(r);
            if b.rows == self.batch {
                emit(std::mem::replace(&mut b, new()).finish(&schema));
            }
        });
        if b.rows > 0 {
            emit(b.finish(&schema));
        }
    }

    /// Generate on `threads` threads (0 = all cores) and call `f` with each
    /// batch, in order, on the calling thread.
    pub fn for_each_batch(&self, threads: usize, f: impl FnMut(RecordBatch)) {
        chunks::for_each_batch(self, threads, f)
    }

    /// Write the operations as one Parquet file, one row group per chunk
    /// (`props: None` means [`YcsbRun::parquet_props`]).
    pub fn write_parquet<W: std::io::Write + Send>(
        &self,
        writer: W,
        threads: usize,
        props: Option<parquet::file::properties::WriterProperties>,
    ) -> Result<W, parquet::errors::ParquetError> {
        let props = props.unwrap_or_else(|| Self::parquet_props().build());
        let mut w = crate::write_parquet_many(
            std::slice::from_ref(self),
            vec![writer],
            threads,
            Some(props),
        )?;
        Ok(w.pop().expect("one writer"))
    }

    /// Write the operations as TSV, one line per binding call:
    /// `OP\tkey\tscanlength\treadfields\tfield0...`, empty where NULL (the
    /// format of `ycsb-ref`'s `RefDB` with `refdb.run=true`).
    pub fn write_tsv<W: std::io::Write + Send>(
        &self,
        writer: W,
        threads: usize,
    ) -> std::io::Result<W> {
        let mut w = crate::write_text_many(std::slice::from_ref(self), vec![writer], threads)?;
        Ok(w.pop().expect("one writer"))
    }
}

impl Chunks for YcsbRun {
    fn schema(&self) -> SchemaRef {
        YcsbRun::schema(self)
    }

    fn num_chunks(&self) -> u64 {
        YcsbRun::num_chunks(self)
    }

    fn chunk(&self, i: u64, emit: &mut dyn FnMut(RecordBatch)) {
        self.generate(i, emit)
    }
}

impl TextChunks for YcsbRun {
    fn outputs(&self) -> usize {
        1
    }

    fn num_chunks(&self) -> u64 {
        YcsbRun::num_chunks(self)
    }

    fn chunk_text(&self, i: u64, bufs: &mut [Vec<u8>]) {
        let out = &mut bufs[0];
        let names = self.load.field_names();
        let mut b = itoa::Buffer::new();
        self.run_chunk(i, &mut |r| {
            out.extend_from_slice(r.op.as_bytes());
            out.push(b'\t');
            out.extend_from_slice(r.key.as_bytes());
            out.push(b'\t');
            if let Some(n) = r.scan {
                out.extend_from_slice(b.format(n).as_bytes());
            }
            out.push(b'\t');
            r.read_fields(&names, out);
            for f in 0..names.len() {
                out.push(b'\t');
                if r.written[f] {
                    out.extend_from_slice(&r.values[f]);
                }
            }
            out.push(b'\n');
        });
    }
}

/// One binding call.
struct Row {
    op: &'static str,
    key: String,
    scan: Option<i32>,
    /// Fields asked for: `None` = all, `Some(None)` = every field listed,
    /// `Some(Some(f))` = field `f`.
    read: Option<Option<usize>>,
    written: Vec<bool>,
    values: Vec<Vec<u8>>,
}

impl Row {
    fn read_fields(&self, names: &[String], out: &mut Vec<u8>) {
        match self.read {
            None => {}
            Some(Some(f)) => out.extend_from_slice(names[f].as_bytes()),
            Some(None) => {
                for (k, n) in names.iter().enumerate() {
                    if k > 0 {
                        out.push(b',');
                    }
                    out.extend_from_slice(n.as_bytes());
                }
            }
        }
    }
}

/// Arrow columns being built.
struct Builder {
    rows: usize,
    op: Views,
    key: Views,
    scan: Vec<i32>,
    scan_valid: Vec<bool>,
    read: Views,
    read_valid: Vec<bool>,
    fields: Vec<(Views, Vec<bool>)>,
    names: Vec<String>,
}

impl Builder {
    fn new(names: Vec<String>, n: usize, len: usize) -> Self {
        let fc = names.len();
        Builder {
            rows: 0,
            op: Views::with_capacity(n, 0),
            key: Views::with_capacity(n, n * 24),
            scan: Vec::with_capacity(n),
            scan_valid: Vec::with_capacity(n),
            read: Views::with_capacity(n, 0),
            read_valid: Vec::with_capacity(n),
            fields: (0..fc)
                .map(|_| {
                    (
                        Views::with_capacity(n, len.min(4096) * 8),
                        Vec::with_capacity(n),
                    )
                })
                .collect(),
            names,
        }
    }

    fn push(&mut self, r: &Row) {
        self.rows += 1;
        self.op.push(r.op.as_bytes());
        self.key.push(r.key.as_bytes());
        self.scan.push(r.scan.unwrap_or(0));
        self.scan_valid.push(r.scan.is_some());
        let mut s = Vec::new();
        r.read_fields(&self.names, &mut s);
        self.read.push(&s);
        self.read_valid.push(r.read.is_some());
        for (f, (v, valid)) in self.fields.iter_mut().enumerate() {
            v.push(if r.written[f] { &r.values[f] } else { b"" });
            valid.push(r.written[f]);
        }
    }

    fn finish(self, schema: &SchemaRef) -> RecordBatch {
        let nulls = |v: Vec<bool>| Some(NullBuffer::new(BooleanBuffer::from(v)));
        let mut cols: Vec<ArrayRef> = vec![
            self.op.finish(),
            self.key.finish(),
            Arc::new(Int32Array::new(self.scan.into(), nulls(self.scan_valid))),
            self.read.finish_nulls(nulls(self.read_valid)),
        ];
        for (v, valid) in self.fields {
            cols.push(v.finish_nulls(nulls(valid)));
        }
        RecordBatch::try_new(schema.clone(), cols).expect("columns match schema")
    }
}

/// YCSB's `ZipfianGenerator`, with its incremental zeta.
#[derive(Debug, Clone)]
struct Zipf {
    items: i64,
    base: i64,
    zeta2theta: f64,
    alpha: f64,
    zetan: f64,
    eta: f64,
    count_for_zeta: i64,
    /// `1 + Math.pow(0.5, theta)`, which YCSB computes on every call.
    one_plus_half_pow_theta: f64,
}

/// `zetastatic(st, n, theta, initialsum)`, summed in YCSB's order.
fn zeta(st: i64, n: i64, initial: f64) -> f64 {
    let mut sum = initial;
    for i in st..n {
        sum += 1.0 / ((i + 1) as f64).powf(THETA);
    }
    sum
}

impl Zipf {
    fn new(min: i64, max: i64, zetan: Option<f64>) -> Self {
        let items = max - min + 1;
        let zeta2theta = zeta(0, 2, 0.0);
        let zetan = zetan.unwrap_or_else(|| zeta(0, items, 0.0));
        Zipf {
            items,
            base: min,
            zeta2theta,
            alpha: 1.0 / (1.0 - THETA),
            zetan,
            eta: Self::eta(items, zeta2theta, zetan),
            count_for_zeta: items,
            one_plus_half_pow_theta: 1.0 + 0.5f64.powf(THETA),
        }
    }

    fn eta(items: i64, zeta2theta: f64, zetan: f64) -> f64 {
        (1.0 - (2.0 / items as f64).powf(1.0 - THETA)) / (1.0 - zeta2theta / zetan)
    }

    /// `nextLong(itemcount)` with `u` its `nextDouble()`.
    fn next(&mut self, itemcount: i64, u: f64) -> i64 {
        if itemcount > self.count_for_zeta {
            self.zetan = zeta(self.count_for_zeta, itemcount, self.zetan);
            self.count_for_zeta = itemcount;
            self.eta = Self::eta(self.items, self.zeta2theta, self.zetan);
        }
        let uz = u * self.zetan;
        if uz < 1.0 {
            return self.base;
        }
        if uz < self.one_plus_half_pow_theta {
            return self.base + 1;
        }
        self.base + scaled_pow(itemcount as f64, self.eta * u - self.eta + 1.0, self.alpha)
    }
}

/// `(long) (n * Math.pow(x, alpha))` for `alpha` = `1 / (1 - 0.99)` and
/// `0 <= x <= 1`, without a `pow` call where it can't change the result.
///
/// `x^100` by squaring is within ~100 ulp of the true `x^alpha` (alpha is
/// 100 less 9e-14, worth at most 5e-14 relative where the product is >=
/// 0.5, since then `x >= n^(-1/100)`), so its product with `n` truncates to
/// the same integer as `pow`'s unless it is within 1e-12 (relative) of one;
/// those few call `pow`.
#[inline]
fn scaled_pow(n: f64, x: f64, alpha: f64) -> i64 {
    let exact = || (n * x.powf(alpha)) as i64;
    if !(0.0..=1.0).contains(&x) || n > 1e19 {
        return exact();
    }
    let x2 = x * x;
    let x4 = x2 * x2;
    let x8 = x4 * x4;
    let x16 = x8 * x8;
    let x32 = x16 * x16;
    let x64 = x32 * x32;
    let y = n * (x64 * x32 * x4);
    if y < 0.5 {
        return 0;
    }
    let r = y.floor();
    let margin = y * 1e-12;
    if y - r > margin && r + 1.0 - y > margin {
        r as i64
    } else {
        exact()
    }
}

#[derive(Debug, Clone)]
struct State {
    /// Next `ThreadLocalRandom` draw.
    pos: u64,
    /// `transactioninsertkeysequence`'s next key (its last is one less).
    next_insert: i64,
    /// `latest`'s zipfian, whose zeta grows with the inserts.
    latest: Option<Zipf>,
}

#[derive(Debug, Clone)]
enum Chooser {
    Uniform {
        lb: i64,
        interval: Rem,
    },
    Scrambled {
        min: i64,
        items: Rem,
    },
    Latest,
    Hotspot {
        lb: i64,
        hot: Rem,
        cold: Rem,
        op_fraction: f64,
    },
}

/// Two scratch rows, and the callback that receives each binding call.
type Out<'r, 'f> = (&'r mut [Row; 2], &'f mut dyn FnMut(&Row));

/// One run's fixed parameters.
struct Gen<'a> {
    run: &'a YcsbRun,
    tlr: JavaTlr,
    chooser: Chooser,
    scrambled: Zipf,
    scan_zipf: Option<Zipf>,
    /// Draws per field value read.
    per_field: u64,
    /// Rank of each field in the `HashMap` of all fields.
    rank: Vec<u64>,
    names: Vec<String>,
    field_rem: Rem,
    scan_rem: Rem,
    /// `DiscreteGenerator`'s operations and their `weight / sum`.
    ops: Vec<(Op, f64)>,
}

/// Java's `n % d` for a fixed `d > 0`, without a division when `n >= 0`
/// (`m = floor((2^64 - 1) / d)` puts the quotient estimate at most one
/// below the true one).
#[derive(Debug, Clone, Copy)]
struct Rem {
    d: u64,
    m: u64,
}

impl Rem {
    /// `d` of 0 is allowed until it is used, as in Java.
    fn new(d: i64) -> Self {
        assert!(d >= 0, "modulus must be >= 0");
        Rem {
            d: d as u64,
            m: u64::MAX.checked_div(d as u64).unwrap_or(0),
        }
    }

    #[inline(always)]
    fn of(self, n: i64) -> i64 {
        if n < 0 || self.d == 0 {
            return n % self.d as i64;
        }
        let n = n as u64;
        let q = ((n as u128 * self.m as u128) >> 64) as u64;
        let mut r = n - q * self.d;
        if r >= self.d {
            r -= self.d;
        }
        r as i64
    }
}

impl<'a> Gen<'a> {
    fn new(run: &'a YcsbRun) -> Self {
        let names = run.load.field_names();
        let mut rank = vec![0; names.len()];
        for (r, &f) in java_hashmap_order(&names).iter().enumerate() {
            rank[f] = r as u64;
        }
        Gen {
            run,
            tlr: run.load.tlr,
            chooser: run.chooser(),
            scrambled: Zipf::new(0, SCRAMBLED_ITEMS, Some(SCRAMBLED_ZETAN)),
            scan_zipf: run.scan_zipfian.then(|| {
                let (min, max) = run.scan;
                let zetan = *run.scan_zeta.get_or_init(|| zeta(0, max - min + 1, 0.0));
                Zipf::new(min, max, Some(zetan))
            }),
            per_field: (run.load.field_length as u64).div_ceil(6),
            rank,
            ops: {
                let p = &run.proportions;
                let sum: f64 = p.iter().filter(|&&w| w > 0.0).sum();
                (0..5)
                    .filter(|&k| p[k] > 0.0)
                    .map(|k| (OPS[k], p[k] / sum))
                    .collect()
            },
            field_rem: Rem::new(names.len() as i64),
            scan_rem: Rem::new(run.scan.1 - run.scan.0 + 1),
            names,
        }
    }

    #[inline]
    fn long(&self, st: &mut State) -> i64 {
        st.pos += 1;
        JavaTlr::long_of(self.tlr.state_at(st.pos - 1))
    }

    #[inline]
    fn double(&self, st: &mut State) -> f64 {
        st.pos += 2;
        self.tlr.double_at(st.pos - 2)
    }

    /// `UniformLongGenerator(lb, lb + interval - 1)`.
    #[inline]
    fn uniform(&self, st: &mut State, lb: i64, interval: Rem) -> i64 {
        interval.of(self.long(st).wrapping_abs()) + lb
    }

    /// `keychooser.nextValue()`.
    fn choose(&self, st: &mut State) -> i64 {
        match self.chooser {
            Chooser::Uniform { lb, interval } => self.uniform(st, lb, interval),
            Chooser::Scrambled { min, items } => {
                let u = self.double(st);
                let ret = self.scrambled.clone().next(self.scrambled.items, u);
                min + items.of(fnv_hash64(ret))
            }
            Chooser::Latest => {
                let u = self.double(st);
                let max = st.next_insert - 1;
                let z = st.latest.as_mut().expect("latest state");
                max - z.next(max, u)
            }
            Chooser::Hotspot {
                lb,
                hot,
                cold,
                op_fraction,
            } => {
                if self.double(st) < op_fraction {
                    lb + hot.of(self.long(st).wrapping_abs())
                } else {
                    lb + hot.d as i64 + cold.of(self.long(st).wrapping_abs())
                }
            }
        }
    }

    /// `nextKeynum()`: keys past the last inserted one are drawn again.
    fn key(&self, st: &mut State) -> i64 {
        loop {
            let k = self.choose(st) as i32 as i64;
            // YCSB: `keynum > transactioninsertkeysequence.lastValue()`
            if k < st.next_insert {
                return k;
            }
        }
    }

    fn field(&self, st: &mut State) -> usize {
        self.uniform(st, 0, self.field_rem) as usize
    }

    fn op_kind(&self, st: &mut State) -> Op {
        let mut val = self.double(st);
        for &(op, pw) in &self.ops {
            if val < pw {
                return op;
            }
            val -= pw;
        }
        self.ops.last().expect("one proportion > 0").0
    }

    /// Bytes of a random value whose `toArray` draws start at `at`.
    fn random_value(&self, at: u64, out: &mut Vec<u8>) {
        let len = self.run.load.field_length;
        out.clear();
        out.resize(len, 0);
        let gamma = self.tlr.gamma();
        let mut s = self.tlr.state_at(at);
        for c in out.chunks_mut(6) {
            let n = c.len();
            c.copy_from_slice(&six(JavaTlr::int_of(s)).to_le_bytes()[..n]);
            s = s.wrapping_add(gamma);
        }
    }

    /// The values an update / insert writes: one field
    /// (`buildSingleValue`) or all (`buildValues`), drawn as YCSB draws
    /// them, and read by the binding in `HashMap` order.
    fn values(&self, st: &mut State, all: bool, row: Option<&mut Row>) {
        let di = self.run.load.data_integrity;
        let len = self.run.load.field_length;
        if all {
            let fc = self.names.len() as u64;
            let ctor = st.pos;
            if !di {
                st.pos += fc + fc * self.per_field;
            }
            if let Some(Row {
                key,
                written,
                values,
                ..
            }) = row
            {
                let h = java_hash(0, key.as_bytes());
                for f in 0..self.names.len() {
                    written[f] = true;
                    let v = &mut values[f];
                    if di {
                        v.clear();
                        deterministic_value(key.as_bytes(), h, self.names[f].as_bytes(), len, v);
                    } else {
                        self.random_value(ctor + fc + self.rank[f] * self.per_field, v);
                    }
                }
            }
        } else {
            let f = self.field(st);
            let at = st.pos + 1;
            if !di {
                st.pos += 1 + self.per_field;
            }
            if let Some(Row {
                key,
                written,
                values,
                ..
            }) = row
            {
                written[f] = true;
                let v = &mut values[f];
                if di {
                    v.clear();
                    let h = java_hash(0, key.as_bytes());
                    deterministic_value(key.as_bytes(), h, self.names[f].as_bytes(), len, v);
                } else {
                    self.random_value(at, v);
                }
            }
        }
    }

    fn new_row(&self) -> Row {
        let fc = self.names.len();
        Row {
            op: "",
            key: String::new(),
            scan: None,
            read: None,
            written: vec![false; fc],
            values: vec![Vec::new(); fc],
        }
    }

    /// Reset `r` for an `op` call on key number `k`.
    fn start_row(&self, r: &mut Row, op: &'static str, k: i64) {
        r.op = op;
        r.key.clear();
        self.run.load.push_key_long(k, &mut r.key);
        r.scan = None;
        r.read = None;
        r.written.fill(false);
    }

    /// One `doTransaction`: advance `st`, and if `out` is given, pass its
    /// binding calls to the callback (built in the two scratch rows).
    fn op(&self, st: &mut State, out: Option<Out<'_, '_>>) {
        let run = self.run;
        let one_field = |g: &Self, st: &mut State| (!run.read_all_fields).then(|| g.field(st));
        match self.op_kind(st) {
            Op::Read => {
                let k = self.key(st);
                let read = match one_field(self, st) {
                    Some(f) => Some(Some(f)),
                    None if run.load.data_integrity => Some(None),
                    None => None,
                };
                if let Some((rows, emit)) = out {
                    let r = &mut rows[0];
                    self.start_row(r, "READ", k);
                    r.read = read;
                    emit(r);
                }
            }
            Op::Update => {
                let k = self.key(st);
                match out {
                    Some((rows, emit)) => {
                        let w = &mut rows[0];
                        self.start_row(w, "UPDATE", k);
                        self.values(st, run.write_all_fields, Some(w));
                        emit(w);
                    }
                    None => self.values(st, run.write_all_fields, None),
                }
            }
            Op::Insert => {
                let k = st.next_insert;
                match out {
                    Some((rows, emit)) => {
                        let w = &mut rows[0];
                        self.start_row(w, "INSERT", k);
                        self.values(st, true, Some(w));
                        emit(w);
                    }
                    None => self.values(st, true, None),
                }
                st.next_insert += 1;
            }
            Op::Scan => {
                let k = self.key(st);
                let len = match &self.scan_zipf {
                    None => self.uniform(st, run.scan.0, self.scan_rem),
                    Some(z) => {
                        let u = self.double(st);
                        z.clone().next(z.items, u)
                    }
                } as i32;
                let read = one_field(self, st).map(Some);
                if let Some((rows, emit)) = out {
                    let r = &mut rows[0];
                    self.start_row(r, "SCAN", k);
                    r.scan = Some(len);
                    r.read = read;
                    emit(r);
                }
            }
            Op::ReadModifyWrite => {
                let k = self.key(st);
                let read = one_field(self, st).map(Some);
                match out {
                    Some((rows, emit)) => {
                        let [r, w] = rows;
                        self.start_row(r, "READ", k);
                        r.read = read;
                        self.start_row(w, "UPDATE", k);
                        self.values(st, run.write_all_fields, Some(w));
                        emit(r);
                        emit(w);
                    }
                    None => self.values(st, run.write_all_fields, None),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scaled_pow_matches_pow() {
        let alpha = 1.0 / (1.0 - THETA);
        let mut x = 0x2545_f491_4f6c_dd1du64;
        let mut fast = 0u64;
        for n in [2.0, 3.0, 1000.0, 1e6, 123_456_789.0, 1e10 + 1.0, 4e12] {
            for k in 0..400_000u32 {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                let u = (x >> 11) as f64 / (1u64 << 53) as f64;
                // skew towards 1, where the products are large
                let v = if k % 2 == 0 { u } else { 1.0 - u * 1e-3 };
                let want = (n * v.powf(alpha)) as i64;
                assert_eq!(scaled_pow(n, v, alpha), want, "n {n} x {v}");
                fast += 1;
            }
        }
        assert_eq!(scaled_pow(10.0, 1.0, alpha), 10);
        assert_eq!(scaled_pow(10.0, 0.0, alpha), 0);
        assert!(fast > 0);
    }

    #[test]
    fn rem_matches_java() {
        for d in [
            1,
            2,
            3,
            7,
            10,
            1000,
            1 << 40,
            i64::MAX,
            i64::MAX - 1,
            999_999_937,
        ] {
            let r = Rem::new(d);
            for n in [
                0,
                1,
                d - 1,
                d,
                d.wrapping_add(1),
                i64::MAX,
                i64::MAX - 1,
                i64::MIN,
                -1,
                -d,
            ] {
                assert_eq!(r.of(n), n % d, "{n} % {d}");
            }
            let mut x = 0x9e37_79b9_7f4a_7c15u64;
            for _ in 0..10_000 {
                x = x
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                let n = (x >> 1) as i64;
                assert_eq!(r.of(n), n % d, "{n} % {d}");
            }
        }
    }
}
