//! The YCSB load phase (`ycsb load`, `CoreWorkload`): the `usertable`
//! records, byte-identical to YCSB 0.17.0.
//!
//! - Keys are exactly YCSB's: `user` + the FNV-64 hash of the insert
//!   sequence number (or the number itself, `insertorder=ordered`),
//!   zero-padded to `zeropadding` digits.
//! - With `dataintegrity=true` the values are YCSB's deterministic ones
//!   (`key:field:hash:hash...`), byte for byte.
//! - Otherwise YCSB fills values from the client thread's
//!   `ThreadLocalRandom`, which it never seeds. Its draws are a counter
//!   ([`JavaTlr`]), and each record uses a fixed number of them, so given
//!   the thread's seed every record's values are computed directly: they
//!   are what YCSB writes when its loading thread starts from that seed
//!   (checked against YCSB 0.17.0 on OpenJDK 21, with the seed set by
//!   reflection).
//!
//! Only `fieldlengthdistribution=constant` is supported: the other length
//! distributions draw a variable number of random values per record, so
//! records could not be generated independently.

use crate::engine::{DEFAULT_BATCH_SIZE, Plan};
use crate::rng::JavaTlr;
use crate::strings::Views;
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use parquet::basic::Compression;
use parquet::file::properties::{WriterProperties, WriterPropertiesBuilder};
use populate_table::text::TextChunks;
use populate_table::{Chunks, chunks};
use std::sync::Arc;

/// Default records per chunk (one Parquet row group): about 32 MiB of
/// values with the default 10 fields of 100 bytes.
pub const YCSB_CHUNK_ROWS: u64 = 32 * 1024;

/// The YCSB `usertable` load. The defaults are YCSB's (`recordcount`
/// aside): 10 fields of 100 bytes, `field0`..`field9`, hashed insert order,
/// `zeropadding=1`, random values.
///
/// ```
/// use populate_oltp::Ycsb;
///
/// let y = Ycsb::new(1000).data_integrity(true);
/// let tsv = y.write_tsv(Vec::new(), 0).unwrap();
/// assert!(tsv.starts_with(b"user6284781860667377211\tuser6284781860667377211:field0:-56807877:"));
/// ```
#[derive(Debug, Clone)]
pub struct Ycsb {
    pub(crate) record_count: u64,
    pub(crate) insert_start: u64,
    pub(crate) insert_count: Option<u64>,
    pub(crate) field_count: usize,
    pub(crate) field_length: usize,
    pub(crate) field_prefix: String,
    pub(crate) ordered: bool,
    pub(crate) zero_padding: usize,
    pub(crate) data_integrity: bool,
    pub(crate) tlr: JavaTlr,
    pub(crate) chunk_rows: u64,
    pub(crate) batch: usize,
    pub(crate) plan: Plan,
    pub(crate) schema: SchemaRef,
}

impl Ycsb {
    /// `recordcount` records.
    pub fn new(record_count: u64) -> Self {
        let mut y = Ycsb {
            record_count,
            insert_start: 0,
            insert_count: None,
            field_count: 10,
            field_length: 100,
            field_prefix: "field".into(),
            ordered: false,
            zero_padding: 1,
            data_integrity: false,
            tlr: JavaTlr {
                seed: 0,
                thread_id: 1,
            },
            chunk_rows: YCSB_CHUNK_ROWS,
            batch: DEFAULT_BATCH_SIZE,
            plan: Plan::new(0, 1.0),
            schema: Arc::new(Schema::empty()),
        };
        y.update();
        y
    }

    fn update(&mut self) {
        // as YCSB's Client: `insertcount` records if given, else
        // `recordcount`, starting at `insertstart`
        let count = self.insert_count.unwrap_or(self.record_count);
        self.plan = Plan::new(count, 1.0)
            .chunk_source_rows(self.chunk_rows)
            .batch_size(self.batch);
        let mut fields = vec![Field::new("ycsb_key", DataType::Utf8View, false)];
        fields.extend(
            self.field_names()
                .into_iter()
                .map(|f| Field::new(f, DataType::Utf8View, false)),
        );
        self.schema = Arc::new(Schema::new(fields));
    }

    fn set(mut self, f: impl FnOnce(&mut Self)) -> Self {
        f(&mut self);
        self.update();
        self
    }

    /// `insertstart`: the first insert sequence number (for loading in
    /// parallel from several clients).
    pub fn insert_start(self, n: u64) -> Self {
        self.set(|y| y.insert_start = n)
    }

    /// `insertcount` (default: `recordcount`, as YCSB's client loads).
    pub fn insert_count(self, n: u64) -> Self {
        self.set(|y| y.insert_count = Some(n))
    }

    /// `fieldcount` (default 10).
    pub fn field_count(self, n: usize) -> Self {
        self.set(|y| y.field_count = n)
    }

    /// `fieldlength` (default 100).
    pub fn field_length(self, n: usize) -> Self {
        self.set(|y| y.field_length = n)
    }

    /// `fieldnameprefix` (default `field`).
    ///
    /// Panics unless `p` is printable ASCII without tabs: the values are
    /// built byte-wise, and the TSV output separates fields with tabs.
    pub fn field_prefix(self, p: &str) -> Self {
        assert!(
            p.bytes().all(|b| b.is_ascii_graphic() || b == b' '),
            "fieldnameprefix must be printable ASCII"
        );
        self.set(|y| y.field_prefix = p.into())
    }

    /// YCSB's own check of the properties: `CoreWorkload` refuses
    /// `insertstart + insertcount > recordcount`.
    pub fn check(&self) -> Result<(), String> {
        match self.insert_count {
            Some(c) if self.insert_start + c > self.record_count => Err(format!(
                "insertstart ({}) + insertcount ({c}) is more than recordcount ({})",
                self.insert_start, self.record_count
            )),
            _ => Ok(()),
        }
    }

    /// `insertorder=ordered` (default `hashed`).
    pub fn ordered(self, ordered: bool) -> Self {
        self.set(|y| y.ordered = ordered)
    }

    /// `zeropadding` (default 1).
    pub fn zero_padding(self, n: usize) -> Self {
        self.set(|y| y.zero_padding = n)
    }

    /// `dataintegrity` (default false): deterministic values.
    pub fn data_integrity(self, on: bool) -> Self {
        self.set(|y| y.data_integrity = on)
    }

    /// The loading thread's `ThreadLocalRandom` state, for random values
    /// (default seed 0, thread 1).
    pub fn java_random(self, tlr: JavaTlr) -> Self {
        self.set(|y| y.tlr = tlr)
    }

    /// Records per chunk (= per Parquet row group).
    pub fn chunk_rows(self, n: u64) -> Self {
        assert!(n > 0, "chunk_rows must be > 0");
        self.set(|y| y.chunk_rows = n)
    }

    /// Rows per Arrow batch (default 8Ki).
    pub fn batch_size(self, n: usize) -> Self {
        assert!(n > 0, "batch_size must be > 0");
        self.set(|y| y.batch = n)
    }

    /// Records this load inserts.
    pub fn rows(&self) -> u64 {
        self.plan.source_rows
    }

    pub fn num_chunks(&self) -> u64 {
        self.plan.num_chunks()
    }

    /// `ycsb_key` then the fields.
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    pub fn field_names(&self) -> Vec<String> {
        (0..self.field_count)
            .map(|i| format!("{}{i}", self.field_prefix))
            .collect()
    }

    /// The key of insert sequence number `keynum` (`buildKeyName`).
    pub fn key(&self, keynum: u64) -> String {
        let mut s = String::new();
        self.push_key(keynum, &mut s);
        s
    }

    fn push_key(&self, keynum: u64, out: &mut String) {
        // the load takes the sequence number as an `int`
        self.push_key_long(keynum as i32 as i64, out)
    }

    /// `buildKeyName(keynum)`.
    pub(crate) fn push_key_long(&self, k: i64, out: &mut String) {
        let k = if self.ordered { k } else { fnv_hash64(k) };
        let mut b = itoa::Buffer::new();
        let digits = b.format(k);
        out.push_str("user");
        for _ in digits.len()..self.zero_padding {
            out.push('0');
        }
        out.push_str(digits);
    }

    /// Records `first..first + n` of this load as one batch.
    fn batch(&self, first: u64, n: u64) -> RecordBatch {
        let len = n as usize;
        let mut keys = Views::with_capacity(len, len * 24);
        let mut key_strs = Vec::with_capacity(len);
        let mut s = String::new();
        for j in first..first + n {
            s.clear();
            self.push_key(self.insert_start + j, &mut s);
            keys.push(s.as_bytes());
            key_strs.push(s.clone());
        }
        let mut cols: Vec<ArrayRef> = Vec::with_capacity(self.field_count + 1);
        cols.push(keys.finish());
        if self.data_integrity {
            let names = self.field_names();
            let hashes: Vec<i32> = key_strs
                .iter()
                .map(|k| java_hash(0, k.as_bytes()))
                .collect();
            let mut buf = Vec::new();
            for name in &names {
                let mut v = Views::with_capacity(len, len * self.field_length);
                for (key, &h) in key_strs.iter().zip(&hashes) {
                    buf.clear();
                    deterministic_value(
                        key.as_bytes(),
                        h,
                        name.as_bytes(),
                        self.field_length,
                        &mut buf,
                    );
                    v.push(&buf);
                }
                cols.push(v.finish());
            }
        } else {
            let layout = RandomLayout::new(self);
            for f in 0..self.field_count {
                let mut v = Views::with_capacity(len, len * self.field_length);
                for j in first..first + n {
                    v.push_with(self.field_length, |b| layout.fill(self.tlr, j, f, b));
                }
                cols.push(v.finish());
            }
        }
        RecordBatch::try_new(self.schema.clone(), cols).expect("columns match schema")
    }

    /// Records `first..end` as TSV lines, straight from the generators.
    fn text(&self, first: u64, end: u64, out: &mut Vec<u8>) {
        let names = self.field_names();
        let layout = (!self.data_integrity).then(|| RandomLayout::new(self));
        let line = 24 + self.field_count * (self.field_length + 1);
        out.reserve((end - first) as usize * line);
        let mut key = String::new();
        for j in first..end {
            key.clear();
            self.push_key(self.insert_start + j, &mut key);
            out.extend_from_slice(key.as_bytes());
            let h = java_hash(0, key.as_bytes());
            for (f, name) in names.iter().enumerate() {
                out.push(b'\t');
                match &layout {
                    None => deterministic_value(
                        key.as_bytes(),
                        h,
                        name.as_bytes(),
                        self.field_length,
                        out,
                    ),
                    Some(l) => {
                        let at = out.len();
                        out.resize(at + self.field_length, 0);
                        l.fill(self.tlr, j, f, &mut out[at..]);
                    }
                }
            }
            out.push(b'\n');
        }
    }

    fn generate(&self, first: u64, end: u64, emit: &mut dyn FnMut(RecordBatch)) {
        self.plan
            .batches(first, end, |at, n| emit(self.batch(at, n)));
    }

    /// Records `first..end` (0-based within this load), on this thread.
    pub fn range(&self, first: u64, end: u64) -> Vec<RecordBatch> {
        let mut v = Vec::new();
        self.generate(first, end.min(self.plan.source_rows), &mut |b| v.push(b));
        v
    }

    /// Generate on `threads` threads (0 = all cores) and call `f` with each
    /// batch, in order, on the calling thread.
    pub fn for_each_batch(&self, threads: usize, f: impl FnMut(RecordBatch)) {
        chunks::for_each_batch(self, threads, f)
    }

    /// Parquet writer properties for YCSB: every value is distinct, so no
    /// dictionaries (the writer would build them and fall back). Snappy.
    /// Used when `write_parquet` gets no properties.
    pub fn parquet_props() -> WriterPropertiesBuilder {
        WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_dictionary_enabled(false)
    }

    /// Write the records as one Parquet file, one row group per chunk
    /// (`props: None` means [`Ycsb::parquet_props`]).
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

    /// Write the records as TSV: `key\tfield0\t...\n`, fields in name
    /// order. YCSB's values never contain a tab or newline.
    pub fn write_tsv<W: std::io::Write + Send>(
        &self,
        writer: W,
        threads: usize,
    ) -> std::io::Result<W> {
        let mut w = crate::write_text_many(std::slice::from_ref(self), vec![writer], threads)?;
        Ok(w.pop().expect("one writer"))
    }
}

impl Chunks for Ycsb {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn num_chunks(&self) -> u64 {
        self.plan.num_chunks()
    }

    fn chunk(&self, i: u64, emit: &mut dyn FnMut(RecordBatch)) {
        let (first, end) = self.plan.range(i);
        self.generate(first, end, emit)
    }
}

impl TextChunks for Ycsb {
    fn outputs(&self) -> usize {
        1
    }

    fn num_chunks(&self) -> u64 {
        self.plan.num_chunks()
    }

    fn chunk_text(&self, i: u64, bufs: &mut [Vec<u8>]) {
        let (first, end) = self.plan.range(i);
        self.text(first, end, &mut bufs[0]);
    }
}

/// `Utils.fnvhash64`: FNV-1a over the 8 bytes of `val` (arithmetic shift),
/// then `Math.abs`.
pub fn fnv_hash64(mut val: i64) -> i64 {
    let mut h = 0xcbf2_9ce4_8422_2325u64 as i64;
    for _ in 0..8 {
        let octet = val & 0xff;
        val >>= 8;
        h ^= octet;
        h = h.wrapping_mul(1_099_511_628_211);
    }
    h.wrapping_abs()
}

/// `String.hashCode` of ASCII `s`, continued from `h`.
#[inline]
pub(crate) fn java_hash(h: i32, s: &[u8]) -> i32 {
    s.iter()
        .fold(h, |h, &b| h.wrapping_mul(31).wrapping_add(b as i32))
}

/// `CoreWorkload.buildDeterministicValue(key, field)` for a constant field
/// length `size`, into `out`.
/// `h` is `String.hashCode` of `key`; the value is appended to `out`.
pub(crate) fn deterministic_value(
    key: &[u8],
    h: i32,
    field: &[u8],
    size: usize,
    out: &mut Vec<u8>,
) {
    let start = out.len();
    out.extend_from_slice(key);
    out.push(b':');
    out.extend_from_slice(field);
    let mut h = java_hash(java_hash(h, b":"), field);
    let mut b = itoa::Buffer::new();
    while out.len() - start < size {
        out.push(b':');
        h = java_hash(h, b":");
        let t = b.format(h).as_bytes();
        out.extend_from_slice(t);
        h = java_hash(h, t);
    }
    out.truncate(start + size);
}

/// Where each field's random bytes come from in the thread's int stream.
///
/// For each record, `buildValues` constructs one `RandomByteIterator` per
/// field in name order, each drawing one int it never uses (its `toArray`
/// fills from fresh draws); then the database binding reads the values in
/// the `HashMap`'s iteration order, each drawing one int per 6 bytes.
struct RandomLayout {
    /// Ints per record.
    per_record: u64,
    /// Start of each field's ints within the record.
    at: Vec<u64>,
}

impl RandomLayout {
    fn new(y: &Ycsb) -> Self {
        let fc = y.field_count as u64;
        let per_field = (y.field_length as u64).div_ceil(6);
        let order = java_hashmap_order(&y.field_names());
        let mut at = vec![0; y.field_count];
        for (rank, &f) in order.iter().enumerate() {
            at[f] = fc + rank as u64 * per_field;
        }
        RandomLayout {
            per_record: fc * (1 + per_field),
            at,
        }
    }

    /// Field `f` of record `j` of the load, into `out`.
    #[inline]
    fn fill(&self, tlr: JavaTlr, j: u64, f: usize, out: &mut [u8]) {
        let at = j * self.per_record + self.at[f];
        let gamma = tlr.gamma();
        let mut state = tlr.state_at(at);
        let mut chunks = out.chunks_exact_mut(6);
        for c in &mut chunks {
            c.copy_from_slice(&six(JavaTlr::int_of(state)).to_le_bytes()[..6]);
            state = state.wrapping_add(gamma);
        }
        let rest = chunks.into_remainder();
        if !rest.is_empty() {
            let n = rest.len();
            rest.copy_from_slice(&six(JavaTlr::int_of(state)).to_le_bytes()[..n]);
        }
    }
}

/// `RandomByteIterator.fillBytesImpl`: 6 printable bytes from one int.
///
/// Byte `k` is `((bytes >> 5k) & mask_k) + ' '` (masks 31, 63, 95, 31,
/// 63, 95), little-endian in the low 6 bytes of the result.
#[inline(always)]
pub(crate) fn six(bytes: i32) -> u64 {
    let x = bytes as u32 as u64;
    (x & 31
        | (x << 3) & (63 << 8)
        | (x << 6) & (95 << 16)
        | (x << 9) & (31 << 24)
        | (x << 12) & (63 << 32)
        | (x << 15) & (95 << 40))
        + 0x2020_2020_2020
}

/// The iteration order of a `java.util.HashMap<String, _>` (default
/// capacity) after inserting `keys` in order, as indexes into `keys`.
///
/// Buckets in index order, each in insertion order (resizes keep the
/// relative order). Panics if a bucket gets a 9th entry, where HashMap
/// would turn it into a tree or resize.
pub(crate) fn java_hashmap_order(keys: &[String]) -> Vec<usize> {
    let mut cap = 16usize;
    while keys.len() > cap / 4 * 3 {
        cap *= 2;
    }
    let mut buckets: Vec<Vec<usize>> = vec![Vec::new(); cap];
    for (i, k) in keys.iter().enumerate() {
        let h = java_hash(0, k.as_bytes()) as u32;
        let spread = h ^ (h >> 16);
        buckets[spread as usize & (cap - 1)].push(i);
    }
    assert!(
        buckets.iter().all(|b| b.len() <= 8),
        "field names collide in Java's HashMap; random values are not supported for them"
    );
    buckets.into_iter().flatten().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys() {
        // `ycsb load` with the defaults, then zeropadding=12 / ordered
        let y = Ycsb::new(10);
        assert_eq!(y.key(0), "user6284781860667377211");
        assert_eq!(y.key(1), "user8517097267634966620");
        assert_eq!(y.clone().ordered(true).key(5), "user5");
        assert_eq!(y.ordered(true).zero_padding(8).key(5), "user00000005");
    }

    #[test]
    fn hashmap_order() {
        let y = Ycsb::new(1);
        // field0..field9 hash to consecutive buckets
        let o = java_hashmap_order(&y.field_names());
        assert_eq!(o.len(), 10);
        let mut s = o.clone();
        s.sort();
        assert_eq!(s, (0..10).collect::<Vec<_>>());
    }
}
