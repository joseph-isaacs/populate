//! Parquet column chunks for flat integer columns, encoded here instead of
//! by arrow-rs's column writer.
//!
//! arrow-rs is general: it builds definition levels and a list of non-null
//! indexes per batch, gathers values through it, and dictionary-encodes
//! through a generic hash interner. For a flat `Int32`, `Date32`, `Int64` or
//! small `Decimal128` column it is much less: copy the valid values 64 rows
//! at a time, map each to its dictionary index (a direct table when the
//! values span a small range, else an open-addressing hash), RLE/bit-pack
//! the indexes, and take the definition levels straight from the validity
//! bitmap, which is already their bit-packed form.
//!
//! The chunk is what arrow-rs writes: a PLAIN dictionary page, then v1 data
//! pages with RLE levels and RLE_DICTIONARY values (PLAIN after the
//! dictionary page size limit, or with dictionaries off), Snappy or
//! uncompressed; the same chunk statistics and level histograms; column and
//! offset indexes. Pages end where arrow-rs ends them when it writes
//! mini-batches of `write_batch_size` rows (arrow-rs uses longer
//! mini-batches for arrays without nulls, so its pages there can differ).
//! It is appended to the row group as a finished chunk.

use arrow_array::cast::AsArray;
use arrow_array::types::{Date32Type, Decimal128Type, Int32Type, Int64Type};
use arrow_array::{Array, ArrayRef};
use arrow_buffer::{BooleanBuffer, BooleanBufferBuilder};
use arrow_schema::DataType;
use bytes::Bytes;
use parquet::basic::{BoundaryOrder, Compression, Encoding, Type as PhysicalType};
use parquet::column::page::{CompressedPage, Page, PageWriter};
use parquet::column::writer::ColumnCloseResult;
use parquet::errors::{ParquetError, Result};
use parquet::file::metadata::{
    ColumnChunkMetaData, ColumnIndexBuilder, LevelHistogram, OffsetIndexBuilder,
};
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use parquet::file::statistics::Statistics;
use parquet::file::writer::{SerializedPageWriter, TrackedWrite};
use parquet::schema::types::{ColumnDescPtr, ColumnDescriptor, ColumnPath};
use std::ops::Range;

/// Whether a flat column of type `dt`, stored as `descr`, can be encoded
/// here under `props` (anything else goes to arrow-rs).
pub(crate) fn eligible(dt: &DataType, descr: &ColumnDescriptor, props: &WriterProperties) -> bool {
    let path = descr.path();
    let type_ok = match (dt, descr.physical_type()) {
        (DataType::Int32 | DataType::Date32, PhysicalType::INT32) => true,
        (DataType::Int64, PhysicalType::INT64) => true,
        (DataType::Decimal128(p, _), PhysicalType::INT32) => *p <= 9,
        (DataType::Decimal128(p, _), PhysicalType::INT64) => *p <= 18,
        _ => false,
    };
    type_ok
        && descr.max_rep_level() == 0
        && descr.max_def_level() <= 1
        && props.writer_version() == WriterVersion::PARQUET_1_0
        && matches!(
            props.compression(path),
            Compression::SNAPPY | Compression::UNCOMPRESSED
        )
        && props.bloom_filter_properties(path).is_none()
        && props.encoding(path).is_none()
}

/// One column chunk's arrays, encoded on [`DirectColumn::close`].
pub(crate) struct DirectColumn {
    descr: ColumnDescPtr,
    arrays: Vec<ArrayRef>,
}

impl DirectColumn {
    pub(crate) fn new(descr: ColumnDescPtr) -> Self {
        Self {
            descr,
            arrays: Vec::new(),
        }
    }

    pub(crate) fn write(&mut self, array: &ArrayRef) {
        self.arrays.push(array.clone());
    }

    /// The encoded chunk (offsets relative to its start) and its metadata.
    pub(crate) fn close(self, props: &WriterProperties) -> Result<(Bytes, ColumnCloseResult)> {
        match self.descr.physical_type() {
            PhysicalType::INT32 => {
                let v = Values::<i32>::gather(&self.arrays, |a, out| match a.data_type() {
                    DataType::Int32 => extend(out, a.as_primitive::<Int32Type>().values(), a),
                    DataType::Date32 => extend(out, a.as_primitive::<Date32Type>().values(), a),
                    _ => extend_map(out, a.as_primitive::<Decimal128Type>().values(), a, |d| {
                        d as i32
                    }),
                });
                encode(&self.descr, props, &v)
            }
            _ => {
                let v = Values::<i64>::gather(&self.arrays, |a, out| match a.data_type() {
                    DataType::Int64 => extend(out, a.as_primitive::<Int64Type>().values(), a),
                    _ => extend_map(out, a.as_primitive::<Decimal128Type>().values(), a, |d| {
                        d as i64
                    }),
                });
                encode(&self.descr, props, &v)
            }
        }
    }
}

/// The physical value types handled here.
trait Native: Copy + Ord + Default + Send + 'static {
    const SIZE: usize;
    const MIN: Self;
    const MAX: Self;
    /// Order-preserving: `a <= b` iff `a.key() <= b.key()`.
    fn key(self) -> u64;
    fn put(self, out: &mut Vec<u8>);
    fn stats(min: Option<Self>, max: Option<Self>, nulls: u64) -> Statistics;
}

impl Native for i32 {
    const SIZE: usize = 4;
    const MIN: Self = i32::MIN;
    const MAX: Self = i32::MAX;
    fn key(self) -> u64 {
        (self as i64 - i32::MIN as i64) as u64
    }
    fn put(self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.to_le_bytes());
    }
    fn stats(min: Option<Self>, max: Option<Self>, nulls: u64) -> Statistics {
        Statistics::int32(min, max, None, Some(nulls), false)
    }
}

impl Native for i64 {
    const SIZE: usize = 8;
    const MIN: Self = i64::MIN;
    const MAX: Self = i64::MAX;
    fn key(self) -> u64 {
        self as u64 ^ (1 << 63)
    }
    fn put(self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.to_le_bytes());
    }
    fn stats(min: Option<Self>, max: Option<Self>, nulls: u64) -> Statistics {
        Statistics::int64(min, max, None, Some(nulls), false)
    }
}

/// A chunk's non-null values, its validity bitmap (the definition levels,
/// bit-packed at width 1) if any row is null, and its arrays' lengths.
struct Values<T> {
    vals: Vec<T>,
    valid: Option<BooleanBuffer>,
    rows: usize,
    lens: Vec<usize>,
}

impl<T: Native> Values<T> {
    fn gather(arrays: &[ArrayRef], mut push: impl FnMut(&ArrayRef, &mut Vec<T>)) -> Self {
        let rows: usize = arrays.iter().map(|a| a.len()).sum();
        let mut vals = Vec::with_capacity(rows);
        let mut valid: Option<BooleanBufferBuilder> = None;
        let mut done = 0;
        for a in arrays {
            match a.logical_nulls().filter(|n| n.null_count() > 0) {
                Some(n) => {
                    let valid = valid.get_or_insert_with(|| {
                        let mut b = BooleanBufferBuilder::new(rows);
                        b.append_n(done, true);
                        b
                    });
                    valid.append_buffer(n.inner());
                }
                None => {
                    if let Some(b) = &mut valid {
                        b.append_n(a.len(), true);
                    }
                }
            }
            push(a, &mut vals);
            done += a.len();
        }
        Self {
            vals,
            valid: valid.map(|mut b| b.finish()),
            rows,
            lens: arrays.iter().map(|a| a.len()).collect(),
        }
    }
}

/// Append the valid entries of `values` (the values of `a`).
fn extend<T: Copy>(out: &mut Vec<T>, values: &[T], a: &ArrayRef) {
    extend_map(out, values, a, |v| v)
}

fn extend_map<S: Copy, T: Copy>(out: &mut Vec<T>, values: &[S], a: &ArrayRef, f: impl Fn(S) -> T) {
    let Some(n) = a.logical_nulls().filter(|n| n.null_count() > 0) else {
        out.extend(values.iter().map(|&v| f(v)));
        return;
    };
    // 64 rows at a time: copy all-valid words, else walk the set bits
    let chunks = n.inner().bit_chunks();
    let mut put = |base: usize, mut w: u64, len: usize| {
        if w == u64::MAX {
            out.extend(values[base..base + 64].iter().map(|&v| f(v)));
        } else {
            w &= u64::MAX >> (64 - len);
            while w != 0 {
                out.push(f(values[base + w.trailing_zeros() as usize]));
                w &= w - 1;
            }
        }
    };
    let mut base = 0;
    for w in chunks.iter() {
        put(base, w, 64);
        base += 64;
    }
    if chunks.remainder_len() > 0 {
        put(base, chunks.remainder_bits(), chunks.remainder_len());
    }
}

/// Smallest and largest of `v`.
fn min_max<T: Native>(v: &[T]) -> Option<(T, T)> {
    if v.is_empty() {
        return None;
    }
    // two plain folds: each vectorizes, a fold over a pair does not
    let lo = v.iter().copied().fold(T::MAX, Ord::min);
    let hi = v.iter().copied().fold(T::MIN, Ord::max);
    Some((lo, hi))
}

/// Writes one column chunk's pages and gathers its metadata: the parts
/// that don't depend on the value type.
struct ChunkWriter<'a> {
    descr: &'a ColumnDescPtr,
    props: &'a WriterProperties,
    compression: Compression,
    snappy: snap::raw::Encoder,
    pages: SerializedPageWriter<'a, Vec<u8>>,
    has_levels: bool,
    histograms: bool,
    encodings: Vec<Encoding>,
    compressed: i64,
    uncompressed: i64,
    written: u64,
    dictionary_offset: Option<i64>,
    data_offset: Option<i64>,
    column_index: ColumnIndexBuilder,
    offset_index: OffsetIndexBuilder,
}

impl<'a> ChunkWriter<'a> {
    fn new(
        descr: &'a ColumnDescPtr,
        props: &'a WriterProperties,
        sink: &'a mut TrackedWrite<Vec<u8>>,
    ) -> Self {
        let has_levels = descr.max_def_level() > 0;
        let stats = props.statistics_enabled(descr.path());
        ChunkWriter {
            descr,
            props,
            compression: props.compression(descr.path()),
            snappy: snap::raw::Encoder::new(),
            pages: SerializedPageWriter::new(sink),
            has_levels,
            // as arrow-rs: level histograms whenever statistics are on
            histograms: has_levels && stats != EnabledStatistics::None,
            encodings: if has_levels {
                vec![Encoding::RLE]
            } else {
                Vec::new()
            },
            compressed: 0,
            uncompressed: 0,
            written: 0,
            dictionary_offset: None,
            data_offset: None,
            column_index: ColumnIndexBuilder::new(descr.physical_type()),
            offset_index: OffsetIndexBuilder::new(),
        }
    }

    fn compress(&mut self, body: Vec<u8>) -> Result<(Bytes, usize)> {
        let n = body.len();
        Ok(match self.compression {
            Compression::SNAPPY => (
                Bytes::from(
                    self.snappy
                        .compress_vec(&body)
                        .map_err(|e| ParquetError::External(Box::new(e)))?,
                ),
                n,
            ),
            _ => (Bytes::from(body), n),
        })
    }

    /// The PLAIN dictionary page of `entries` entries.
    fn dictionary(&mut self, body: Vec<u8>, entries: usize) -> Result<()> {
        let (buf, n) = self.compress(body)?;
        let page = Page::DictionaryPage {
            buf,
            num_values: entries as u32,
            encoding: Encoding::PLAIN,
            is_sorted: false,
        };
        let spec = self.pages.write_page(CompressedPage::new(page, n))?;
        self.dictionary_offset = Some(spec.offset as i64);
        self.written += spec.bytes_written;
        self.compressed += spec.compressed_size as i64;
        self.uncompressed += spec.uncompressed_size as i64;
        self.encodings
            .extend([Encoding::PLAIN, Encoding::RLE_DICTIONARY]);
        Ok(())
    }

    /// A v1 data page of rows `rows` (`vals` of them valid): definition
    /// levels from `valid`, then the values `put` writes in `encoding`.
    /// `min_max` is the page's (column index bytes).
    fn data_page(
        &mut self,
        rows: Range<usize>,
        vals: usize,
        valid: Option<&BooleanBuffer>,
        encoding: Encoding,
        put: impl FnOnce(&mut Vec<u8>),
        min_max: Option<(Vec<u8>, Vec<u8>)>,
    ) -> Result<()> {
        let (r0, r1) = (rows.start, rows.end);
        let mut body = Vec::new();
        if self.has_levels {
            body.extend_from_slice(&[0; 4]);
            match valid {
                // `sliced` copies only if the page starts mid-byte
                Some(b) => bit_levels(b.slice(r0, r1 - r0).sliced().as_slice(), r1 - r0, &mut body),
                None => rle(1, r1 - r0, 1, &mut body),
            }
            let len = (body.len() - 4) as u32;
            body[..4].copy_from_slice(&len.to_le_bytes());
        }
        put(&mut body);
        if encoding == Encoding::PLAIN && !self.encodings.contains(&Encoding::PLAIN) {
            self.encodings.push(Encoding::PLAIN);
        }
        let (buf, n) = self.compress(body)?;
        let page = Page::DataPage {
            buf,
            num_values: (r1 - r0) as u32,
            encoding,
            def_level_encoding: Encoding::RLE,
            rep_level_encoding: Encoding::RLE,
            statistics: None,
        };
        let spec = self.pages.write_page(CompressedPage::new(page, n))?;
        self.data_offset.get_or_insert(spec.offset as i64);
        self.written += spec.bytes_written;
        self.compressed += spec.compressed_size as i64;
        self.uncompressed += spec.uncompressed_size as i64;
        self.offset_index.append_row_count((r1 - r0) as i64);
        self.offset_index
            .append_offset_and_size(spec.offset as i64, spec.compressed_size as i32);
        let page_nulls = ((r1 - r0) - vals) as i64;
        let (lo, hi) = min_max.unwrap_or_default();
        self.column_index
            .append(vals == 0, lo, hi, page_nulls, None);
        if self.histograms {
            let h = LevelHistogram::from(vec![page_nulls, vals as i64]);
            self.column_index.append_histograms(&None, &Some(h));
        }
        Ok(())
    }

    /// Finish the chunk: `rows` rows, `vals` valid, chunk statistics
    /// `stats` and the pages' `order`.
    fn finish(
        mut self,
        rows: usize,
        vals: usize,
        stats: Statistics,
        order: BoundaryOrder,
    ) -> Result<ColumnCloseResult> {
        self.column_index.set_boundary_order(order);
        self.pages.close()?;
        let stats_level = self.props.statistics_enabled(self.descr.path());
        let nulls = (rows - vals) as i64;
        let mut meta = ColumnChunkMetaData::builder(self.descr.clone())
            .set_compression(self.compression)
            .set_encodings(self.encodings)
            .set_num_values(rows as i64)
            .set_total_compressed_size(self.compressed)
            .set_total_uncompressed_size(self.uncompressed)
            .set_data_page_offset(self.data_offset.unwrap_or(0))
            .set_dictionary_page_offset(self.dictionary_offset);
        if stats_level != EnabledStatistics::None {
            meta = meta.set_statistics(stats);
            if self.histograms {
                let h = vec![nulls, vals as i64];
                meta = meta.set_definition_level_histogram(Some(LevelHistogram::from(h)));
            }
        }
        Ok(ColumnCloseResult {
            bytes_written: self.written,
            rows_written: rows as u64,
            metadata: meta.build()?,
            bloom_filter: None,
            column_index: match stats_level {
                EnabledStatistics::Page => Some(self.column_index.build()?),
                _ => None,
            },
            offset_index: (!self.props.offset_index_disabled()).then(|| self.offset_index.build()),
        })
    }
}

/// Pages' boundary order, from each page's `(min, max)` in order (`None`
/// for all-null pages), as arrow-rs computes it.
fn boundary_order<K: PartialOrd>(pages: impl Iterator<Item = Option<(K, K)>>) -> BoundaryOrder {
    let (mut ascending, mut descending, mut last) = (true, true, None::<(K, K)>);
    for m in pages {
        if let (Some((lo, hi)), Some((last_lo, last_hi))) = (&m, &last) {
            ascending &= last_lo <= lo && last_hi <= hi;
            descending &= lo <= last_lo && hi <= last_hi;
        }
        if m.is_some() {
            last = m;
        }
    }
    match (ascending, descending) {
        (true, _) => BoundaryOrder::ASCENDING,
        (false, true) => BoundaryOrder::DESCENDING,
        (false, false) => BoundaryOrder::UNORDERED,
    }
}

fn encode<T: Native>(
    descr: &ColumnDescPtr,
    props: &WriterProperties,
    v: &Values<T>,
) -> Result<(Bytes, ColumnCloseResult)> {
    let path = descr.path();
    let (lo, hi) = match min_max(&v.vals) {
        Some((lo, hi)) => (Some(lo), Some(hi)),
        None => (None, None),
    };
    let plan = plan(v, props, path, lo.zip(hi));
    let mut sink = TrackedWrite::new(Vec::new());
    let mut w = ChunkWriter::new(descr, props, &mut sink);
    if let Some(entries) = &plan.entries {
        let mut body = Vec::with_capacity(entries.len() * T::SIZE);
        entries.iter().for_each(|e| e.put(&mut body));
        w.dictionary(body, entries.len())?;
    }
    let bytes = |x: T| {
        let mut b = Vec::new();
        x.put(&mut b);
        b
    };
    let mut page_ranges = Vec::with_capacity(plan.pages.len());
    for p in &plan.pages {
        let (v0, v1) = (p.vals.start, p.vals.end);
        let vals = &v.vals[v0..v1];
        let mm = min_max(vals);
        page_ranges.push(mm);
        let (encoding, idx) = match p.entries {
            Some(n) => (Encoding::RLE_DICTIONARY, Some((n, &plan.idx[v0..v1]))),
            None => (Encoding::PLAIN, None),
        };
        w.data_page(
            p.rows.clone(),
            vals.len(),
            v.valid.as_ref(),
            encoding,
            |body| match idx {
                Some((n, idx)) => {
                    // as arrow-rs: the width of the dictionary when the
                    // page was flushed
                    let bits = 64 - (n.saturating_sub(1) as u64).leading_zeros();
                    body.push(bits as u8);
                    hybrid(idx, bits, body);
                }
                None => {
                    body.reserve(vals.len() * T::SIZE);
                    vals.iter().for_each(|x| x.put(body));
                }
            },
            mm.map(|(lo, hi)| (bytes(lo), bytes(hi))),
        )?;
    }
    let order = boundary_order(page_ranges.into_iter());
    let nulls = (v.rows - v.vals.len()) as u64;
    let close = w.finish(v.rows, v.vals.len(), T::stats(lo, hi, nulls), order)?;
    Ok((Bytes::from(sink.into_inner()?), close))
}

/// A data page: its rows and values, and the number of dictionary entries
/// when it was flushed (`None`: PLAIN).
struct PageSpan {
    rows: Range<usize>,
    vals: Range<usize>,
    entries: Option<usize>,
}

/// A chunk's pages, its dictionary (if any page uses one) and each
/// dictionary-encoded value's index.
struct Plan<T> {
    pages: Vec<PageSpan>,
    entries: Option<Vec<T>>,
    idx: Vec<u32>,
}

/// The pages arrow-rs would write: it writes each array in mini-batches of
/// `write_batch_size` rows, ends the page after the mini-batch that reaches
/// the row limit, and once the dictionary reaches its page size limit
/// (checked after each mini-batch) ends the page and writes the rest PLAIN.
fn plan<T: Native>(
    v: &Values<T>,
    props: &WriterProperties,
    path: &ColumnPath,
    range: Option<(T, T)>,
) -> Plan<T> {
    let limit = props
        .data_page_row_count_limit()
        .min(props.data_page_size_limit() / T::SIZE)
        .max(1);
    let batch = props.write_batch_size().max(1);
    let dict_limit = props.dictionary_page_size_limit();
    let mut dict = range
        .filter(|_| props.dictionary_enabled(path))
        .map(|(lo, hi)| Dict::new(lo, hi, v.vals.len()));
    let mut idx = Vec::with_capacity(if dict.is_some() { v.vals.len() } else { 0 });
    let mut pages = Vec::new();
    let (mut r0, mut v0, mut row, mut val) = (0, 0, 0, 0);
    let mut fell_back = dict.is_none();
    for &len in &v.lens {
        let mut left = len;
        while left > 0 {
            let step = left.min(batch);
            let n = v
                .valid
                .as_ref()
                .map_or(step, |b| b.slice(row, step).count_set_bits());
            if let Some(d) = dict.as_mut().filter(|_| !fell_back) {
                d.encode(&v.vals[val..val + n], &mut idx);
            }
            (row, val, left) = (row + step, val + n, left - step);
            let entries = dict
                .as_ref()
                .filter(|_| !fell_back)
                .map(|d| d.entries.len());
            let fall_back = entries.is_some_and(|e| e * T::SIZE >= dict_limit);
            if row - r0 >= limit || fall_back {
                pages.push(PageSpan {
                    rows: r0..row,
                    vals: v0..val,
                    entries,
                });
                (r0, v0) = (row, val);
            }
            fell_back |= fall_back;
        }
    }
    if row > r0 {
        let entries = dict
            .as_ref()
            .filter(|_| !fell_back)
            .map(|d| d.entries.len());
        pages.push(PageSpan {
            rows: r0..row,
            vals: v0..val,
            entries,
        });
    }
    Plan {
        pages,
        entries: dict.map(|d| d.entries),
        idx,
    }
}

/// Dictionary entries in first-seen order. Values spanning a small range
/// index a table by `value - min`; others go through an open-addressing
/// hash table.
struct Dict<T> {
    entries: Vec<T>,
    lo: T,
    table: Vec<u32>,
    keys: Vec<T>,
    bits: u32,
}

impl<T: Native> Dict<T> {
    fn new(lo: T, hi: T, n: usize) -> Self {
        let span = hi.key() - lo.key();
        let direct = span < (n as u64).max(1024);
        let bits = if direct { 0 } else { 10 };
        Self {
            entries: Vec::new(),
            lo,
            table: vec![u32::MAX; if direct { span as usize + 1 } else { 1 << bits }],
            keys: if direct {
                Vec::new()
            } else {
                vec![T::default(); 1 << bits]
            },
            bits,
        }
    }

    fn encode(&mut self, xs: &[T], idx: &mut Vec<u32>) {
        if self.bits == 0 {
            let lo = self.lo.key();
            for &x in xs {
                let slot = &mut self.table[(x.key() - lo) as usize];
                if *slot == u32::MAX {
                    *slot = self.entries.len() as u32;
                    self.entries.push(x);
                }
                idx.push(*slot);
            }
            return;
        }
        for &x in xs {
            let mask = (1usize << self.bits) - 1;
            let mut h = hash(x.key(), self.bits);
            let i = loop {
                match self.table[h] {
                    u32::MAX => {
                        let i = self.entries.len() as u32;
                        self.table[h] = i;
                        self.keys[h] = x;
                        self.entries.push(x);
                        break i;
                    }
                    i if self.keys[h] == x => break i,
                    _ => h = (h + 1) & mask,
                }
            };
            idx.push(i);
            if self.entries.len() * 2 > 1 << self.bits {
                self.grow();
            }
        }
    }

    fn grow(&mut self) {
        self.bits += 1;
        let mask = (1usize << self.bits) - 1;
        self.table = vec![u32::MAX; 1 << self.bits];
        self.keys = vec![T::default(); 1 << self.bits];
        for (i, &e) in self.entries.iter().enumerate() {
            let mut h = hash(e.key(), self.bits);
            while self.table[h] != u32::MAX {
                h = (h + 1) & mask;
            }
            self.table[h] = i as u32;
            self.keys[h] = e;
        }
    }
}

fn hash(k: u64, bits: u32) -> usize {
    (k.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> (64 - bits)) as usize
}

/// RLE / bit-packed hybrid encoding of `v` at `w` bits per value: runs of
/// 8 or more equal values (after padding the literals before them to a
/// multiple of 8) are RLE runs, the rest bit-packed.
fn hybrid<V: Copy + Eq + Into<u64>>(v: &[V], w: u32, out: &mut Vec<u8>) {
    let n = v.len();
    let (mut lit, mut i) = (0, 0);
    while i < n {
        let x = v[i];
        let mut j = i + 1;
        while j < n && v[j] == x {
            j += 1;
        }
        let pad = (8 - (i - lit) % 8) % 8;
        if j - i >= pad + 8 {
            let s = i + pad;
            if s > lit {
                bitpack(&v[lit..s], w, out);
            }
            rle(x.into(), j - s, w, out);
            lit = j;
        }
        i = j;
    }
    if lit < n {
        bitpack(&v[lit..], w, out);
    }
}

/// Definition levels (width 1) straight from `n` bits of a validity
/// bitmap, which is already their bit-packed form: runs of 2 or more
/// all-valid (or all-null) bytes become RLE runs, other bytes are copied
/// as bit-packed groups of 8.
fn bit_levels(b: &[u8], n: usize, out: &mut Vec<u8>) {
    let full = n / 8;
    let literal = |b: &[u8], out: &mut Vec<u8>| {
        if !b.is_empty() {
            varint(((b.len() as u64) << 1) | 1, out);
            out.extend_from_slice(b);
        }
    };
    let (mut lit, mut i) = (0, 0);
    while i < full {
        let x = b[i];
        let mut j = i + 1;
        if x == 0 || x == 0xFF {
            while j < full && b[j] == x {
                j += 1;
            }
            if j - i >= 2 {
                literal(&b[lit..i], out);
                rle(u64::from(x & 1), (j - i) * 8, 1, out);
                lit = j;
            }
        }
        i = j;
    }
    if n.is_multiple_of(8) {
        literal(&b[lit..full], out);
    } else {
        // the last group, zero-padded
        let mut tail = b[lit..=full].to_vec();
        *tail.last_mut().expect("tail") &= (1u8 << (n % 8)) - 1;
        literal(&tail, out);
    }
}

fn rle(x: u64, count: usize, w: u32, out: &mut Vec<u8>) {
    varint((count as u64) << 1, out);
    out.extend_from_slice(&x.to_le_bytes()[..w.div_ceil(8) as usize]);
}

/// One bit-packed run of `v`, zero-padded to a multiple of 8 values.
fn bitpack<V: Copy + Into<u64>>(v: &[V], w: u32, out: &mut Vec<u8>) {
    let groups = v.len().div_ceil(8);
    varint(((groups as u64) << 1) | 1, out);
    out.reserve(groups * w as usize);
    let (mut acc, mut bits) = (0u64, 0u32);
    let pad = groups * 8 - v.len();
    for x in v
        .iter()
        .map(|&x| x.into())
        .chain(std::iter::repeat_n(0, pad))
    {
        acc |= x << bits;
        bits += w;
        while bits >= 8 {
            out.push(acc as u8);
            acc >>= 8;
            bits -= 8;
        }
    }
}

fn varint(mut x: u64, out: &mut Vec<u8>) {
    while x >= 0x80 {
        out.push(x as u8 | 0x80);
        x >>= 7;
    }
    out.push(x as u8);
}
