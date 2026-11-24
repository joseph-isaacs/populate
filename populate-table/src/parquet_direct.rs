//! Parquet column chunks for flat integer, timestamp, float and string
//! columns, encoded here instead of by arrow-rs's column writer.
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
use arrow_array::types::{
    Date32Type, Decimal128Type, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type,
    Int64Type, TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType,
};
use arrow_array::{Array, ArrayRef};
use arrow_buffer::{BooleanBuffer, BooleanBufferBuilder};
use arrow_schema::{DataType, TimeUnit};
use bytes::Bytes;
use parquet::basic::{BoundaryOrder, Compression, Encoding, Type as PhysicalType};
use parquet::column::page::{CompressedPage, Page, PageWriter};
use parquet::column::writer::ColumnCloseResult;
use parquet::errors::{ParquetError, Result};
use parquet::file::metadata::{
    ColumnChunkMetaData, ColumnIndexBuilder, LevelHistogram, OffsetIndexBuilder,
};
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use parquet::file::statistics::{Statistics, ValueStatistics};
use parquet::file::writer::{SerializedPageWriter, TrackedWrite};
use parquet::schema::types::{ColumnDescPtr, ColumnDescriptor, ColumnPath};
use std::ops::Range;

/// Whether a flat column of type `dt`, stored as `descr`, can be encoded
/// here under `props` (anything else goes to arrow-rs).
pub(crate) fn eligible(dt: &DataType, descr: &ColumnDescriptor, props: &WriterProperties) -> bool {
    let path = descr.path();
    let type_ok = match (dt, descr.physical_type()) {
        (
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Date32,
            PhysicalType::INT32,
        ) => true,
        (DataType::Int64 | DataType::Timestamp(..), PhysicalType::INT64) => true,
        (DataType::Float32, PhysicalType::FLOAT) => true,
        (DataType::Float64, PhysicalType::DOUBLE) => true,
        (DataType::Decimal128(p, _), PhysicalType::INT32) => *p <= 9,
        (DataType::Decimal128(p, _), PhysicalType::INT64) => *p <= 18,
        (DataType::Utf8View, PhysicalType::BYTE_ARRAY) => true,
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
                    DataType::Int16 => {
                        extend_map(out, a.as_primitive::<Int16Type>().values(), a, i32::from)
                    }
                    DataType::Int8 => {
                        extend_map(out, a.as_primitive::<Int8Type>().values(), a, i32::from)
                    }
                    _ => extend_map(out, a.as_primitive::<Decimal128Type>().values(), a, |d| {
                        d as i32
                    }),
                });
                encode(&self.descr, props, &v)
            }
            PhysicalType::BYTE_ARRAY => {
                let v = Values::<&[u8]>::gather(&self.arrays, |a, out| {
                    let sv = a.as_string_view();
                    let get = |i: usize| unsafe { sv.value_unchecked(i) }.as_bytes();
                    match a.logical_nulls().filter(|n| n.null_count() > 0) {
                        // SAFETY: `i` is a row of `sv`
                        None => out.extend((0..sv.len()).map(get)),
                        Some(n) => out.extend(n.valid_indices().map(get)),
                    }
                });
                encode_bytes(&self.descr, props, &v)
            }
            PhysicalType::FLOAT => {
                let v = Values::<F32>::gather(&self.arrays, |a, out| {
                    extend_map(out, a.as_primitive::<Float32Type>().values(), a, |x| {
                        F32(x.to_bits())
                    })
                });
                encode(&self.descr, props, &v)
            }
            PhysicalType::DOUBLE => {
                let v = Values::<F64>::gather(&self.arrays, |a, out| {
                    extend_map(out, a.as_primitive::<Float64Type>().values(), a, |x| {
                        F64(x.to_bits())
                    })
                });
                encode(&self.descr, props, &v)
            }
            _ => {
                let v = Values::<i64>::gather(&self.arrays, |a, out| match a.data_type() {
                    DataType::Int64 => extend(out, a.as_primitive::<Int64Type>().values(), a),
                    DataType::Timestamp(unit, _) => {
                        let values = match unit {
                            TimeUnit::Second => a.as_primitive::<TimestampSecondType>().values(),
                            TimeUnit::Millisecond => {
                                a.as_primitive::<TimestampMillisecondType>().values()
                            }
                            TimeUnit::Microsecond => {
                                a.as_primitive::<TimestampMicrosecondType>().values()
                            }
                            TimeUnit::Nanosecond => {
                                a.as_primitive::<TimestampNanosecondType>().values()
                            }
                        };
                        extend(out, values, a)
                    }
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
    /// Chunk statistics; `nans` is the NaN count (floats only).
    fn stats(min: Option<Self>, max: Option<Self>, nulls: u64, nans: Option<u64>) -> Statistics;
    /// Floating point: NaNs are left out of min / max unless every value
    /// is NaN, and pages and chunks count them, as arrow-rs does.
    const FLOAT: bool = false;
    fn is_nan(self) -> bool {
        false
    }
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
    fn stats(min: Option<Self>, max: Option<Self>, nulls: u64, _: Option<u64>) -> Statistics {
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
    fn stats(min: Option<Self>, max: Option<Self>, nulls: u64, _: Option<u64>) -> Statistics {
        Statistics::int64(min, max, None, Some(nulls), false)
    }
}

/// An `f32` by its bits: equal when the bits are (as arrow-rs's
/// dictionary compares them), ordered by IEEE 754 total order (as its
/// statistics are).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct F32(u32);

/// An `f64` by its bits, as [`F32`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct F64(u64);

macro_rules! float_native {
    ($t:ident, $bits:ty, $f:ty, $size:expr, $variant:ident) => {
        impl $t {
            /// Total-order key: flip all bits of negatives, the sign of
            /// positives.
            #[inline(always)]
            fn total(self) -> $bits {
                const SIGN: $bits = 1 << (<$bits>::BITS - 1);
                if self.0 & SIGN != 0 {
                    !self.0
                } else {
                    self.0 | SIGN
                }
            }
        }

        impl Ord for $t {
            fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                self.total().cmp(&other.total())
            }
        }

        impl PartialOrd for $t {
            fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                Some(self.cmp(other))
            }
        }

        impl Native for $t {
            const SIZE: usize = $size;
            // the extremes of the total order: -NaN and +NaN with every
            // payload bit set
            const MIN: Self = $t(<$bits>::MAX);
            const MAX: Self = $t(<$bits>::MAX >> 1);
            const FLOAT: bool = true;
            fn key(self) -> u64 {
                self.total() as u64
            }
            fn put(self, out: &mut Vec<u8>) {
                out.extend_from_slice(&self.0.to_le_bytes());
            }
            fn stats(
                min: Option<Self>,
                max: Option<Self>,
                nulls: u64,
                nans: Option<u64>,
            ) -> Statistics {
                let f = |x: Option<Self>| x.map(|x| <$f>::from_bits(x.0));
                Statistics::$variant(
                    ValueStatistics::new(f(min), f(max), None, Some(nulls), false)
                        .with_nan_count(nans),
                )
            }
            fn is_nan(self) -> bool {
                <$f>::from_bits(self.0).is_nan()
            }
        }
    };
}

float_native!(F32, u32, f32, 4, Float);
float_native!(F64, u64, f64, 8, Double);

/// A chunk's non-null values, its validity bitmap (the definition levels,
/// bit-packed at width 1) if any row is null, and its arrays' lengths.
struct Values<T> {
    vals: Vec<T>,
    valid: Option<BooleanBuffer>,
    rows: usize,
    lens: Vec<usize>,
}

impl<T> Values<T> {
    fn gather<'a>(arrays: &'a [ArrayRef], mut push: impl FnMut(&'a ArrayRef, &mut Vec<T>)) -> Self {
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

/// Smallest and largest of `v` (for floats, of its non-NaN values if it
/// has any).
fn min_max<T: Native>(v: &[T]) -> Option<(T, T)> {
    if v.is_empty() {
        return None;
    }
    if T::FLOAT {
        let numbers = v.iter().copied().filter(|x| !x.is_nan());
        let lo = numbers.clone().fold(T::MAX, Ord::min);
        let hi = numbers.fold(T::MIN, Ord::max);
        if lo <= hi {
            return Some((lo, hi));
        }
        // every value is NaN: their total-order extremes
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
    /// Total string bytes (byte array columns only).
    unencoded: Option<i64>,
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
            unencoded: None,
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
    /// `put` returns the page's min and max (column index bytes).
    fn data_page(
        &mut self,
        rows: Range<usize>,
        vals: usize,
        valid: Option<&BooleanBuffer>,
        encoding: Encoding,
        nans: Option<i64>,
        put: impl FnOnce(&mut Vec<u8>) -> Option<(Vec<u8>, Vec<u8>)>,
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
        let min_max = put(&mut body);
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
            .append(vals == 0, lo, hi, page_nulls, nans);
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
            .set_dictionary_page_offset(self.dictionary_offset)
            .set_unencoded_byte_array_data_bytes(self.unencoded);
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

impl ChunkWriter<'_> {
    /// Record a byte array page's string bytes, as arrow-rs does.
    fn unencoded_bytes(&mut self, n: i64) {
        *self.unencoded.get_or_insert(0) += n;
        self.offset_index
            .append_unencoded_byte_array_data_bytes(Some(n));
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
        let nans = T::FLOAT.then(|| vals.iter().filter(|x| x.is_nan()).count() as i64);
        w.data_page(
            p.rows.clone(),
            vals.len(),
            v.valid.as_ref(),
            encoding,
            nans,
            |body| {
                match idx {
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
                }
                mm.map(|(lo, hi)| (bytes(lo), bytes(hi)))
            },
        )?;
    }
    let order = boundary_order(page_ranges.into_iter());
    let nulls = (v.rows - v.vals.len()) as u64;
    let nans = (T::FLOAT && !v.vals.is_empty())
        .then(|| v.vals.iter().filter(|x| x.is_nan()).count() as u64);
    let close = w.finish(v.rows, v.vals.len(), T::stats(lo, hi, nulls, nans), order)?;
    Ok((Bytes::from(sink.into_inner()?), close))
}

// ---------------------------------------------------------------------------
// Strings (`Utf8View` as BYTE_ARRAY)

/// `data` truncated to at most `len` bytes on a character boundary (an
/// exact copy if shorter), as arrow-rs truncates a minimum. Whether it was
/// truncated.
fn truncate_min(data: &[u8], len: Option<usize>) -> (Vec<u8>, bool) {
    match (len, std::str::from_utf8(data)) {
        (Some(l), Ok(s)) if data.len() > l => match (1..=l).rfind(|&x| s.is_char_boundary(x)) {
            Some(split) => (data[..split].to_vec(), true),
            None => (data.to_vec(), false),
        },
        (Some(l), Err(_)) if data.len() > l => (data[..l].to_vec(), true),
        _ => (data.to_vec(), false),
    }
}

/// `data` truncated to at most `len` bytes and incremented, an upper bound
/// (an exact copy if shorter or if no such bound exists), as arrow-rs
/// truncates a maximum.
fn truncate_max(data: &[u8], len: Option<usize>) -> (Vec<u8>, bool) {
    let Some(l) = len.filter(|&l| data.len() > l) else {
        return (data.to_vec(), false);
    };
    let bound = match std::str::from_utf8(data) {
        Ok(s) => (l.saturating_sub(3)..=l)
            .rfind(|&x| s.is_char_boundary(x))
            .and_then(|split| {
                let s = &s[..split];
                s.char_indices().rev().find_map(|(i, c)| {
                    let next = char::from_u32(c as u32 + 1)?;
                    (next.len_utf8() == c.len_utf8()).then(|| {
                        let mut r = s.as_bytes()[..i + c.len_utf8()].to_vec();
                        next.encode_utf8(&mut r[i..]);
                        r
                    })
                })
            }),
        Err(_) => {
            let mut r = data[..l].to_vec();
            r.iter_mut()
                .rev()
                .find_map(|b| {
                    let (v, overflow) = b.overflowing_add(1);
                    *b = v;
                    (!overflow).then_some(())
                })
                .map(|()| r)
        }
    };
    match bound {
        Some(b) => (b, true),
        None => (data.to_vec(), false),
    }
}

/// Start loading `x` into cache: strings of a view array can be anywhere
/// (TPC-H's comments are random slices of a 300 MiB pool), and each is a
/// cache miss when its turn comes.
#[inline(always)]
fn prefetch(x: &[u8]) {
    #[cfg(target_arch = "x86_64")]
    // SAFETY: a prefetch is only a hint; it never faults.
    unsafe {
        use std::arch::x86_64::{_MM_HINT_T0, _mm_prefetch};
        _mm_prefetch::<_MM_HINT_T0>(x.as_ptr().cast());
    }
    #[cfg(not(target_arch = "x86_64"))]
    let _ = x;
}

/// `a < b` by bytes, deciding on the first 8 bytes when they differ.
#[inline(always)]
fn less(a: &[u8], b: &[u8]) -> bool {
    if a.len() >= 8 && b.len() >= 8 {
        let x = u64::from_be_bytes(a[..8].try_into().expect("8 bytes"));
        let y = u64::from_be_bytes(b[..8].try_into().expect("8 bytes"));
        if x != y {
            return x < y;
        }
    }
    a < b
}

/// Smallest and largest of `v`, by bytes.
fn bytes_min_max<'a>(v: &[&'a [u8]]) -> Option<(&'a [u8], &'a [u8])> {
    let (&first, rest) = v.split_first()?;
    Some(rest.iter().fold((first, first), |(lo, hi), &x| {
        (
            if less(x, lo) { x } else { lo },
            if less(hi, x) { x } else { hi },
        )
    }))
}

/// String dictionary entries in first-seen order, in an open-addressing
/// hash table keyed by the bytes.
struct BytesDict<'a> {
    entries: Vec<&'a [u8]>,
    /// PLAIN size of the entries.
    bytes: usize,
    table: Vec<u32>,
    bits: u32,
}

/// A cheap hash of a string: its length and its first and last 8 bytes.
#[inline(always)]
fn bytes_hash(x: &[u8], bits: u32) -> usize {
    let n = x.len();
    let u32_at = |i: usize| u32::from_le_bytes(x[i..i + 4].try_into().expect("4 bytes")) as u64;
    let (a, b) = if n >= 8 {
        let w = |i: usize| u64::from_le_bytes(x[i..i + 8].try_into().expect("8 bytes"));
        (w(0), w(n - 8))
    } else if n >= 4 {
        (u32_at(0), u32_at(n - 4))
    } else {
        // 0..=3 bytes: first, middle and last
        let b = |i: usize| x.get(i).copied().unwrap_or(0) as u64;
        (b(0) | b(n / 2) << 8 | b(n.saturating_sub(1)) << 16, 0)
    };
    let k = a.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ b.rotate_left(29) ^ n as u64;
    hash(k ^ (k >> 31), bits)
}

impl<'a> BytesDict<'a> {
    fn new() -> Self {
        BytesDict {
            entries: Vec::new(),
            bytes: 0,
            table: vec![u32::MAX; 1 << 10],
            bits: 10,
        }
    }

    fn encode(&mut self, xs: &[&'a [u8]], idx: &mut Vec<u32>) {
        for &x in xs {
            let mask = (1usize << self.bits) - 1;
            let mut h = bytes_hash(x, self.bits);
            let i = loop {
                match self.table[h] {
                    u32::MAX => {
                        let i = self.entries.len() as u32;
                        self.table[h] = i;
                        self.entries.push(x);
                        self.bytes += 4 + x.len();
                        break i;
                    }
                    i if self.entries[i as usize] == x => break i,
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
        for (i, &e) in self.entries.iter().enumerate() {
            let mut h = bytes_hash(e, self.bits);
            while self.table[h] != u32::MAX {
                h = (h + 1) & mask;
            }
            self.table[h] = i as u32;
        }
    }
}

/// [`plan`] for strings: pages end after the mini-batch that reaches the
/// row limit or the page size limit (PLAIN bytes, or the bit-packed
/// indexes), and the dictionary falls back to PLAIN at its page size limit.
fn plan_bytes<'a>(
    v: &Values<&'a [u8]>,
    props: &WriterProperties,
    path: &ColumnPath,
) -> (Vec<PageSpan>, Option<BytesDict<'a>>, Vec<u32>) {
    let row_limit = props.data_page_row_count_limit().max(1);
    let size_limit = props.data_page_size_limit();
    let batch = props.write_batch_size().max(1);
    let dict_limit = props.dictionary_page_size_limit();
    let mut dict = (props.dictionary_enabled(path) && !v.vals.is_empty()).then(BytesDict::new);
    let mut idx = Vec::with_capacity(if dict.is_some() { v.vals.len() } else { 0 });
    let mut pages = Vec::new();
    let (mut r0, mut v0, mut row, mut val) = (0, 0, 0, 0);
    let mut plain_bytes = 0;
    let mut fell_back = dict.is_none();
    for &len in &v.lens {
        let mut left = len;
        while left > 0 {
            let step = left.min(batch);
            let n = v
                .valid
                .as_ref()
                .map_or(step, |b| b.slice(row, step).count_set_bits());
            let xs = &v.vals[val..val + n];
            match dict.as_mut().filter(|_| !fell_back) {
                Some(d) => d.encode(xs, &mut idx),
                None => plain_bytes += xs.iter().map(|x| 4 + x.len()).sum::<usize>(),
            }
            (row, val, left) = (row + step, val + n, left - step);
            let entries = dict
                .as_ref()
                .filter(|_| !fell_back)
                .map(|d| d.entries.len());
            let size = match entries {
                Some(e) => {
                    1 + ((val - v0) * (64 - (e.max(1) as u64 - 1).leading_zeros() as usize)) / 8
                }
                None => plain_bytes,
            };
            let fall_back = dict
                .as_ref()
                .is_some_and(|d| !fell_back && d.bytes >= dict_limit);
            if row - r0 >= row_limit || size >= size_limit || fall_back {
                pages.push(PageSpan {
                    rows: r0..row,
                    vals: v0..val,
                    entries,
                });
                (r0, v0, plain_bytes) = (row, val, 0);
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
    (pages, dict, idx)
}

fn put_bytes(x: &[u8], out: &mut Vec<u8>) {
    out.extend_from_slice(&(x.len() as u32).to_le_bytes());
    out.extend_from_slice(x);
}

fn encode_bytes(
    descr: &ColumnDescPtr,
    props: &WriterProperties,
    v: &Values<&[u8]>,
) -> Result<(Bytes, ColumnCloseResult)> {
    let path = descr.path();
    let (pages, dict, idx) = plan_bytes(v, props, path);
    let mut sink = TrackedWrite::new(Vec::new());
    let mut w = ChunkWriter::new(descr, props, &mut sink);
    if let Some(d) = &dict {
        let mut body = Vec::with_capacity(d.bytes);
        d.entries.iter().for_each(|e| put_bytes(e, &mut body));
        w.dictionary(body, d.entries.len())?;
    }
    let index_len = props.column_index_truncate_length();
    let entries: &[&[u8]] = dict.as_ref().map_or(&[], |d| &d.entries);
    let mut seen = vec![false; entries.len()];
    let mut page_ranges: Vec<Option<(&[u8], &[u8])>> = Vec::with_capacity(pages.len());
    for p in &pages {
        let (v0, v1) = (p.vals.start, p.vals.end);
        let vals = &v.vals[v0..v1];
        let (encoding, ix) = match p.entries {
            Some(n) => (Encoding::RLE_DICTIONARY, Some((n, &idx[v0..v1]))),
            None => (Encoding::PLAIN, None),
        };
        let mut strings = 0i64;
        let mut mm = None;
        w.data_page(
            p.rows.clone(),
            vals.len(),
            v.valid.as_ref(),
            encoding,
            None,
            |body| {
                match ix {
                    Some((n, ix)) => {
                        let bits = 64 - (n.saturating_sub(1) as u64).leading_zeros();
                        body.push(bits as u8);
                        hybrid(ix, bits, body);
                        // min and max over the entries the page uses
                        for &i in ix {
                            seen[i as usize] = true;
                        }
                        let used = seen.iter().enumerate().filter(|&(_, &s)| s);
                        let used: Vec<&[u8]> = used.map(|(i, _)| entries[i]).collect();
                        mm = bytes_min_max(&used);
                        seen.iter_mut().for_each(|s| *s = false);
                        strings = vals.iter().map(|x| x.len() as i64).sum();
                    }
                    None => {
                        // one pass: the strings may be scattered in memory
                        let need: usize =
                            vals.len() * 4 + vals.iter().map(|x| x.len()).sum::<usize>();
                        body.reserve(need);
                        if let Some((&first, _)) = vals.split_first() {
                            let (mut lo, mut hi) = (first, first);
                            for (i, &x) in vals.iter().enumerate() {
                                if let Some(ahead) = vals.get(i + 16) {
                                    prefetch(ahead);
                                }
                                put_bytes(x, body);
                                if less(x, lo) {
                                    lo = x;
                                } else if less(hi, x) {
                                    hi = x;
                                }
                                strings += x.len() as i64;
                            }
                            mm = Some((lo, hi));
                        }
                    }
                }
                mm.map(|(lo, hi)| (truncate_min(lo, index_len).0, truncate_max(hi, index_len).0))
            },
        )?;
        page_ranges.push(mm);
        w.unencoded_bytes(strings);
    }
    let order = boundary_order(page_ranges.iter().copied());
    let nulls = (v.rows - v.vals.len()) as u64;
    let chunk =
        page_ranges
            .iter()
            .flatten()
            .fold(None, |acc: Option<(&[u8], &[u8])>, &(lo, hi)| {
                Some(match acc {
                    None => (lo, hi),
                    Some((a, b)) => (
                        if less(lo, a) { lo } else { a },
                        if less(b, hi) { hi } else { b },
                    ),
                })
            });
    let stats = match chunk {
        Some((lo, hi)) => {
            let len = props.statistics_truncate_length();
            let (lo, tlo) = truncate_min(lo, len);
            let (hi, thi) = truncate_max(hi, len);
            Statistics::ByteArray(
                ValueStatistics::new(Some(lo.into()), Some(hi.into()), None, Some(nulls), false)
                    .with_min_is_exact(!tlo)
                    .with_max_is_exact(!thi),
            )
        }
        None => Statistics::ByteArray(ValueStatistics::new(None, None, None, Some(nulls), false)),
    };
    let close = w.finish(v.rows, v.vals.len(), stats, order)?;
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
        .map(|(lo, hi)| Dict::new(lo, hi, v.vals.len(), dict_limit / T::SIZE + batch));
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
/// hash table of `(value, index)` slots (one cache line per probe), sized
/// up front for the most entries the chunk can reach before the dictionary
/// page size limit, so it rarely has to grow.
struct Dict<T> {
    entries: Vec<T>,
    lo: T,
    /// Direct mode: entry index by `value - lo`.
    table: Vec<u32>,
    /// Hash mode: `(value, entry index)`, `u32::MAX` for empty.
    slots: Vec<(T, u32)>,
    bits: u32,
}

impl<T: Native> Dict<T> {
    /// For `n` values, of which at most about `cap` become entries.
    fn new(lo: T, hi: T, n: usize, cap: usize) -> Self {
        let span = hi.key() - lo.key();
        let direct = span < (n as u64).max(1024);
        let bits = if direct {
            0
        } else {
            // load factor at most 1/2 for the entries the chunk can reach
            let most = n.min(cap).max(1);
            (2 * most).next_power_of_two().trailing_zeros().max(10)
        };
        Self {
            entries: Vec::new(),
            lo,
            table: if direct {
                vec![u32::MAX; span as usize + 1]
            } else {
                Vec::new()
            },
            slots: if direct {
                Vec::new()
            } else {
                vec![(T::default(), u32::MAX); 1 << bits]
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
        // how far ahead to prefetch slots: tables past the caches miss on
        // nearly every new value
        const AHEAD: usize = 16;
        for (k, &x) in xs.iter().enumerate() {
            if let Some(&next) = xs.get(k + AHEAD) {
                prefetch_slot(&self.slots[hash(next.key(), self.bits)]);
            }
            let mask = (1usize << self.bits) - 1;
            let mut h = hash(x.key(), self.bits);
            let i = loop {
                let (key, i) = self.slots[h];
                if i == u32::MAX {
                    let i = self.entries.len() as u32;
                    self.slots[h] = (x, i);
                    self.entries.push(x);
                    break i;
                }
                if key == x {
                    break i;
                }
                h = (h + 1) & mask;
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
        self.slots = vec![(T::default(), u32::MAX); 1 << self.bits];
        for (i, &e) in self.entries.iter().enumerate() {
            let mut h = hash(e.key(), self.bits);
            while self.slots[h].1 != u32::MAX {
                h = (h + 1) & mask;
            }
            self.slots[h] = (e, i as u32);
        }
    }
}

/// Start loading a hash slot into cache.
#[inline(always)]
fn prefetch_slot<S>(s: &S) {
    #[cfg(target_arch = "x86_64")]
    // SAFETY: a prefetch is only a hint; it never faults.
    unsafe {
        use std::arch::x86_64::{_MM_HINT_T0, _mm_prefetch};
        _mm_prefetch::<_MM_HINT_T0>((s as *const S).cast());
    }
    #[cfg(not(target_arch = "x86_64"))]
    let _ = s;
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
