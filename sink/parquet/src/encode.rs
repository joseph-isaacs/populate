//! Parquet column chunks for flat integer, timestamp, float, boolean and
//! string (`Utf8` and `Utf8View`) columns, required `List`s of numbers,
//! and required `FixedSizeList`s of numbers (PLAIN, in pages of whole
//! records), encoded here instead of by arrow-rs's column writer.
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
use arrow_array::{Array, ArrayRef, ArrowPrimitiveType};
use arrow_buffer::{BooleanBuffer, BooleanBufferBuilder};
use arrow_schema::{DataType, TimeUnit};
use bytes::{Buf, Bytes};
use parquet::basic::{BoundaryOrder, Compression, Encoding, Type as PhysicalType};
use parquet::column::page::{CompressedPage, Page, PageWriteSpec, PageWriter};
use parquet::column::writer::ColumnCloseResult;
use parquet::errors::{ParquetError, Result};
use parquet::file::metadata::{
    ColumnChunkMetaData, ColumnIndexBuilder, LevelHistogram, OffsetIndexBuilder,
};
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use parquet::file::reader::{ChunkReader, Length};
use parquet::file::statistics::{Statistics, ValueStatistics};
use parquet::file::writer::{SerializedPageWriter, TrackedWrite};
use parquet::schema::types::{ColumnDescPtr, ColumnDescriptor, ColumnPath};
use std::borrow::Cow;
use std::collections::VecDeque;
use std::io::{self, Read, Write};
use std::mem::take;
use std::ops::Range;
use std::ptr::{copy_nonoverlapping, null};
use std::slice::from_raw_parts;
use std::sync::{Arc, Mutex};

/// Whether a flat column, a required `List` of required numbers, or a
/// required `FixedSizeList` of required numbers with its dictionary off, of
/// type `dt`, stored as `descr`, can be encoded here under `props` (anything else goes to arrow-rs).
pub fn eligible(dt: &DataType, descr: &ColumnDescriptor, props: &WriterProperties) -> bool {
    let path = descr.path();
    if let DataType::List(item) = dt {
        // a required list of required numbers
        let item_ok = matches!(
            (item.data_type(), descr.physical_type()),
            (DataType::Float32, PhysicalType::FLOAT)
                | (DataType::Float64, PhysicalType::DOUBLE)
                | (DataType::Int32, PhysicalType::INT32)
                | (DataType::Int64, PhysicalType::INT64)
        );
        return item_ok
            && descr.max_rep_level() == 1
            && descr.max_def_level() == 1
            && common_ok(path, props);
    }
    if let DataType::FixedSizeList(item, n) = dt {
        // a required list of required values, PLAIN
        let item_ok = matches!(
            (item.data_type(), descr.physical_type()),
            (DataType::Float32, PhysicalType::FLOAT)
                | (DataType::Float64, PhysicalType::DOUBLE)
                | (DataType::Int32, PhysicalType::INT32)
                | (DataType::Int64, PhysicalType::INT64)
        );
        return item_ok
            && *n > 0
            && descr.max_rep_level() == 1
            && descr.max_def_level() == 1
            && !props.dictionary_enabled(path)
            && common_ok(path, props);
    }
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
        (DataType::Utf8 | DataType::Utf8View, PhysicalType::BYTE_ARRAY) => true,
        (DataType::Boolean, PhysicalType::BOOLEAN) => true,
        _ => false,
    };
    type_ok && descr.max_rep_level() == 0 && descr.max_def_level() <= 1 && common_ok(path, props)
}

/// Writer settings every direct column needs.
fn common_ok(path: &ColumnPath, props: &WriterProperties) -> bool {
    props.writer_version() == WriterVersion::PARQUET_1_0
        && matches!(
            props.compression(path),
            Compression::SNAPPY | Compression::UNCOMPRESSED
        )
        && props.bloom_filter_properties(path).is_none()
        && props.encoding(path).is_none()
}

/// One column chunk's arrays, encoded on [`DirectColumn::close`].
pub struct DirectColumn {
    descr: ColumnDescPtr,
    arrays: Vec<ArrayRef>,
    batch_rows: Option<usize>,
}

impl DirectColumn {
    pub fn new(descr: ColumnDescPtr) -> Self {
        Self {
            descr,
            arrays: Vec::new(),
            batch_rows: None,
        }
    }

    /// Encode a flat column as if its rows had been written in arrays of
    /// `rows` each (the last shorter), whatever arrays they come in: pages
    /// end where they would then, and no array is re-cut or copied. A
    /// `List` or `FixedSizeList` column is encoded by its arrays as given.
    pub fn with_batch_rows(mut self, rows: usize) -> Self {
        self.batch_rows = Some(rows.max(1));
        self
    }

    /// The chunk's values ([`Values::gather`]), in arrays of
    /// [`with_batch_rows`](Self::with_batch_rows) rows if set.
    fn gather<'a, T: Clone>(
        &'a self,
        push: impl FnMut(&'a ArrayRef, &mut Vec<T>),
    ) -> Values<'a, T> {
        let mut v = Values::gather(&self.arrays, push);
        if let Some(n) = self.batch_rows {
            v.lens = (0..v.rows).step_by(n).map(|r| n.min(v.rows - r)).collect();
        }
        v
    }

    pub fn write(&mut self, array: &ArrayRef) {
        self.arrays.push(array.clone());
    }

    /// The encoded chunk (offsets relative to its start) and its metadata.
    pub fn close(self, props: &WriterProperties) -> Result<(Rope, ColumnCloseResult)> {
        if let Some(DataType::List(_)) = self.arrays.first().map(|a| a.data_type()) {
            let d = &self.descr;
            return match d.physical_type() {
                PhysicalType::FLOAT => list_parts::<Float32Type>(&self.arrays, d, props),
                PhysicalType::DOUBLE => list_parts::<Float64Type>(&self.arrays, d, props),
                PhysicalType::INT32 => list_parts::<Int32Type>(&self.arrays, d, props),
                _ => list_parts::<Int64Type>(&self.arrays, d, props),
            };
        }
        if let Some(DataType::FixedSizeList(_, n)) = self.arrays.first().map(|a| a.data_type()) {
            let n = *n as usize;
            return match self.descr.physical_type() {
                PhysicalType::FLOAT => {
                    let v = list_slices(&self.arrays, |a| {
                        f32_bits(a.as_primitive::<Float32Type>().values())
                    })?;
                    encode_list(&self.descr, props, n, &v)
                }
                PhysicalType::DOUBLE => {
                    let v = list_slices(&self.arrays, |a| {
                        f64_bits(a.as_primitive::<Float64Type>().values())
                    })?;
                    encode_list(&self.descr, props, n, &v)
                }
                PhysicalType::INT32 => {
                    let v = list_slices(&self.arrays, |a| a.as_primitive::<Int32Type>().values())?;
                    encode_list(&self.descr, props, n, &v)
                }
                _ => {
                    let v = list_slices(&self.arrays, |a| a.as_primitive::<Int64Type>().values())?;
                    encode_list(&self.descr, props, n, &v)
                }
            };
        }
        match self.descr.physical_type() {
            PhysicalType::BOOLEAN => {
                let v = self.gather::<bool>(|a, out| {
                    let b = a.as_boolean();
                    match a.logical_nulls().filter(|n| n.null_count() > 0) {
                        None => out.extend(b.values().iter()),
                        Some(n) => out.extend(n.valid_indices().map(|i| b.value(i))),
                    }
                });
                encode_bools(&self.descr, props, &v)
            }
            PhysicalType::INT32 => {
                let v = self.gather::<i32>(|a, out| match a.data_type() {
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
                // the views of the valid values too, if a dictionary may be
                // built: equal views are equal strings, so they look up
                // entries by one 16-byte key instead of by the bytes
                let keyed = props.dictionary_enabled(self.descr.path());
                let mut keys = Vec::new();
                let v = self.gather::<&[u8]>(|a, out| {
                    if let Some(sa) = a.as_string_opt::<i32>() {
                        // `Utf8`: offsets into one buffer (no views, so
                        // the dictionary looks up the bytes)
                        let get = |i: usize| sa.value(i).as_bytes();
                        match a.logical_nulls().filter(|n| n.null_count() > 0) {
                            None => out.extend((0..sa.len()).map(get)),
                            Some(n) => out.extend(n.valid_indices().map(get)),
                        }
                        return;
                    }
                    let sv = a.as_string_view();
                    let views = sv.views();
                    // the bytes of value `i` without a branch on its length
                    // (lengths are often mixed, so it would mispredict)
                    let mut bufs: Vec<*const u8> =
                        sv.data_buffers().iter().map(|b| b.as_ptr()).collect();
                    bufs.push(null());
                    let get = |i: usize| {
                        let v = views[i];
                        let len = v as u32 as usize;
                        let long = len > 12;
                        let b = if long { (v >> 64) as u32 as usize } else { 0 };
                        let off = if long { (v >> 96) as u32 as usize } else { 0 };
                        let inline = views[i..].as_ptr().cast::<u8>().wrapping_add(4);
                        // SAFETY: a valid view array: a long view's buffer
                        // index and range are in bounds, and a short view's
                        // bytes are in the view itself.
                        unsafe {
                            let long_ptr = (*bufs.get_unchecked(b)).wrapping_add(off);
                            from_raw_parts(if long { long_ptr } else { inline }, len)
                        }
                    };
                    match a.logical_nulls().filter(|n| n.null_count() > 0) {
                        // SAFETY: `i` is a row of `sv`
                        None => {
                            let start = out.len();
                            out.extend((0..sv.len()).map(get));
                            if keyed {
                                keys.extend(views.iter().zip(&out[start..]).map(view_key));
                            }
                        }
                        Some(n) => {
                            let start = out.len();
                            out.extend(n.valid_indices().map(get));
                            if keyed {
                                let views = n.valid_indices().map(|i| &views[i]);
                                keys.extend(views.zip(&out[start..]).map(view_key));
                            }
                        }
                    }
                });
                encode_bytes(&self.descr, props, &v, &keys)
            }
            PhysicalType::FLOAT => {
                let v = self.gather::<F32>(|a, out| {
                    extend_map(out, a.as_primitive::<Float32Type>().values(), a, |x| {
                        F32(x.to_bits())
                    })
                });
                encode(&self.descr, props, &v)
            }
            PhysicalType::DOUBLE => {
                let v = self.gather::<F64>(|a, out| {
                    extend_map(out, a.as_primitive::<Float64Type>().values(), a, |x| {
                        F64(x.to_bits())
                    })
                });
                encode(&self.descr, props, &v)
            }
            _ => {
                let v = self.gather::<i64>(|a, out| match a.data_type() {
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
pub(crate) trait Native: Copy + Ord + Default + Send + 'static {
    const SIZE: usize;
    const MIN: Self;
    const MAX: Self;
    /// Order-preserving: `a <= b` iff `a.key() <= b.key()`.
    fn key(self) -> u64;
    fn put(self, out: &mut Vec<u8>);
    /// PLAIN encoding of `v`.
    fn put_all(v: &[Self], out: &mut Vec<u8>) {
        out.reserve(v.len() * Self::SIZE);
        v.iter().for_each(|x| x.put(out));
    }
    /// Chunk statistics; `nans` is the NaN count (floats only).
    fn stats(min: Option<Self>, max: Option<Self>, nulls: u64, nans: Option<u64>) -> Statistics;
    /// Floating point: NaNs are left out of min / max unless every value
    /// is NaN, and pages and chunks count them, as arrow-rs does.
    const FLOAT: bool = false;
    fn is_nan(self) -> bool {
        false
    }
    /// Floats: smallest and largest non-NaN values, `None` if there are
    /// none.
    fn min_max_numbers(v: &[Self]) -> Option<(Self, Self)> {
        let _ = v;
        None
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
#[repr(transparent)]
pub(crate) struct F32(u32);

/// An `f64` by its bits, as [`F32`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[repr(transparent)]
pub(crate) struct F64(u64);

macro_rules! float_native {
    ($t:ident, $bits:ty, $f:ty, $size:expr, $variant:ident) => {
        impl $t {
            const SIGN: $bits = 1 << (<$bits>::BITS - 1);

            /// Total-order key: flip all bits of negatives, the sign of
            /// positives.
            #[inline(always)]
            fn total(self) -> $bits {
                if self.0 & Self::SIGN != 0 {
                    !self.0
                } else {
                    self.0 | Self::SIGN
                }
            }

            /// Inverse of [`total`](Self::total).
            #[inline(always)]
            fn from_total(k: $bits) -> Self {
                $t(if k & Self::SIGN != 0 {
                    k ^ Self::SIGN
                } else {
                    !k
                })
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
            fn put_all(v: &[Self], out: &mut Vec<u8>) {
                #[cfg(target_endian = "little")]
                {
                    // SAFETY: `Self` is a transparent integer: its bytes, in
                    // order, are the values' little-endian PLAIN encoding
                    let b = unsafe { from_raw_parts(v.as_ptr().cast::<u8>(), size_of_val(v)) };
                    out.extend_from_slice(b);
                }
                #[cfg(not(target_endian = "little"))]
                v.iter().for_each(|x| x.put(out));
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
            #[inline(always)]
            fn is_nan(self) -> bool {
                // exponent all ones, mantissa not zero
                (self.0 & !Self::SIGN) > (<$f>::INFINITY).to_bits()
            }
            fn min_max_numbers(v: &[Self]) -> Option<(Self, Self)> {
                // branch-free folds over total-order keys, NaN as the
                // identity of each, so they vectorise
                let lo = v.iter().fold(<$bits>::MAX, |m, &x| {
                    m.min(if x.is_nan() { <$bits>::MAX } else { x.total() })
                });
                let hi = v
                    .iter()
                    .fold(0, |m, &x| m.max(if x.is_nan() { 0 } else { x.total() }));
                (lo <= hi).then(|| (Self::from_total(lo), Self::from_total(hi)))
            }
        }
    };
}

float_native!(F32, u32, f32, 4, Float);
float_native!(F64, u64, f64, 8, Double);

/// A chunk's non-null values, its validity bitmap (the definition levels,
/// bit-packed at width 1) if any row is null, and its arrays' lengths.
pub(crate) struct Values<'v, T: Clone> {
    vals: Cow<'v, [T]>,
    valid: Option<BooleanBuffer>,
    rows: usize,
    lens: Vec<usize>,
}

impl<'v, T: Clone> Values<'v, T> {
    /// A chunk of `vals.len()` rows, none null, encoded where they are.
    pub(crate) fn borrowed(vals: &'v [T]) -> Self {
        Self {
            rows: vals.len(),
            lens: vec![vals.len()],
            vals: Cow::Borrowed(vals),
            valid: None,
        }
    }

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
            vals: Cow::Owned(vals),
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
    wide(|| min_max_in(v))
}

#[inline(always)]
fn min_max_in<T: Native>(v: &[T]) -> Option<(T, T)> {
    if v.is_empty() {
        return None;
    }
    if T::FLOAT
        && let Some(r) = T::min_max_numbers(v)
    {
        return Some(r);
    }
    // floats with every value NaN: their total-order extremes
    // two plain folds: each vectorizes, a fold over a pair does not
    let lo = v.iter().copied().fold(T::MAX, Ord::min);
    let hi = v.iter().copied().fold(T::MIN, Ord::max);
    Some((lo, hi))
}

/// NaNs in `v` (floats only, else `None`).
fn nan_count<T: Native>(v: &[T]) -> Option<usize> {
    T::FLOAT.then(|| wide(|| v.iter().filter(|x| x.is_nan()).count()))
}

/// `f()` compiled for AVX2 where the CPU has it (min / max of 32-bit
/// lanes and the like need SSE4.1 or later, past the x86-64 baseline), so
/// the folds over every value vectorise wide.
#[inline(always)]
fn wide<R>(f: impl FnOnce() -> R) -> R {
    #[cfg(target_arch = "x86_64")]
    if is_x86_feature_detected!("avx2") {
        // SAFETY: the CPU has AVX2
        return unsafe { avx2(f) };
    }
    f()
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn avx2<R>(f: impl FnOnce() -> R) -> R {
    f()
}

/// An encoded column chunk, its pages kept in the buffers they were
/// encoded into: a page body is not copied into one chunk-sized buffer,
/// only page headers and small pages are.
///
/// `ChunkWriter` names each page body in `next` before writing it; when
/// the page writer then hands over exactly those bytes, they are kept as
/// they are.
#[derive(Default)]
pub struct Rope {
    parts: Vec<Bytes>,
    tail: Vec<u8>,
    len: usize,
    next: Arc<Mutex<Option<Bytes>>>,
}

impl Rope {
    fn seal(&mut self) {
        if !self.tail.is_empty() {
            self.parts.push(Bytes::from(take(&mut self.tail)));
        }
    }

    /// The chunk, sealed.
    pub(crate) fn finish(mut self) -> Self {
        self.seal();
        self
    }
}

impl Write for Rope {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut next = self.next.lock().expect("unpoisoned");
        if let Some(b) = next.as_ref()
            && b.as_ptr() == buf.as_ptr()
            && b.len() == buf.len()
        {
            let b = next.take().expect("named");
            drop(next);
            self.seal();
            self.parts.push(b);
        } else {
            drop(next);
            self.tail.extend_from_slice(buf);
        }
        self.len += buf.len();
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Length for Rope {
    fn len(&self) -> u64 {
        self.len as u64
    }
}

/// Reads a sealed [`Rope`] from an offset.
pub struct RopeRead {
    parts: VecDeque<Bytes>,
}

impl Read for RopeRead {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        while let Some(p) = self.parts.front_mut() {
            if p.is_empty() {
                self.parts.pop_front();
                continue;
            }
            let n = p.len().min(out.len());
            out[..n].copy_from_slice(&p[..n]);
            p.advance(n);
            return Ok(n);
        }
        Ok(0)
    }
}

impl ChunkReader for Rope {
    type T = RopeRead;

    fn get_read(&self, start: u64) -> Result<RopeRead> {
        let mut skip = start as usize;
        let mut parts = VecDeque::with_capacity(self.parts.len());
        for p in &self.parts {
            if skip >= p.len() {
                skip -= p.len();
            } else {
                parts.push_back(p.slice(skip..));
                skip = 0;
            }
        }
        Ok(RopeRead { parts })
    }

    fn get_bytes(&self, start: u64, length: usize) -> Result<Bytes> {
        let mut out = Vec::with_capacity(length);
        self.get_read(start)?
            .take(length as u64)
            .read_to_end(&mut out)?;
        Ok(Bytes::from(out))
    }
}

/// A chunk's page sink.
pub(crate) fn rope_sink() -> TrackedWrite<Rope> {
    TrackedWrite::new(Rope::default())
}

/// Writes one column chunk's pages and gathers its metadata: the parts
/// that don't depend on the value type.
struct ChunkWriter<'a> {
    descr: &'a ColumnDescPtr,
    props: &'a WriterProperties,
    compression: Compression,
    snappy: snap::raw::Encoder,
    pages: SerializedPageWriter<'a, Rope>,
    /// The sink's slot for the next page body.
    next: Arc<Mutex<Option<Bytes>>>,
    has_levels: bool,
    histograms: bool,
    encodings: Vec<Encoding>,
    compressed: i64,
    uncompressed: i64,
    written: u64,
    /// Total string bytes (byte array columns only).
    unencoded: Option<i64>,
    /// Values per record of a fixed-size list column (levels are values).
    list: Option<usize>,
    /// Records of a variable-length list column (levels are not rows).
    records: Option<usize>,
    dictionary_offset: Option<i64>,
    data_offset: Option<i64>,
    column_index: ColumnIndexBuilder,
    offset_index: OffsetIndexBuilder,
}

impl<'a> ChunkWriter<'a> {
    fn new(
        descr: &'a ColumnDescPtr,
        props: &'a WriterProperties,
        sink: &'a mut TrackedWrite<Rope>,
    ) -> Self {
        let next = sink.inner().next.clone();
        let has_levels = descr.max_def_level() > 0;
        let stats = props.statistics_enabled(descr.path());
        ChunkWriter {
            descr,
            props,
            compression: props.compression(descr.path()),
            snappy: snap::raw::Encoder::new(),
            pages: SerializedPageWriter::new(sink),
            next,
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
            list: None,
            records: None,
            data_offset: None,
            column_index: ColumnIndexBuilder::new(descr.physical_type()),
            offset_index: OffsetIndexBuilder::new(),
        }
    }

    /// Write `page` (`n` bytes before compression); its body is kept as is.
    fn write_page(&mut self, page: Page, n: usize) -> Result<PageWriteSpec> {
        *self.next.lock().expect("unpoisoned") = Some(page.buffer().clone());
        let spec = self.pages.write_page(CompressedPage::new(page, n));
        self.next.lock().expect("unpoisoned").take();
        spec
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
        let spec = self.write_page(page, n)?;
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
        let spec = self.write_page(page, n)?;
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
                let records = match (self.list, self.records) {
                    (Some(n), _) => Some(rows / n),
                    (None, r) => r,
                };
                if let Some(records) = records {
                    let records = records as i64;
                    let h = vec![records, rows as i64 - records];
                    meta = meta.set_repetition_level_histogram(Some(LevelHistogram::from(h)));
                }
            }
        }
        Ok(ColumnCloseResult {
            bytes_written: self.written,
            rows_written: self.records.unwrap_or(rows / self.list.unwrap_or(1)) as u64,
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
    /// A v1 data page of `records` lists of `n` values each (none null):
    /// repetition levels (0 then `n - 1` ones per list), definition levels
    /// (all 1), then the PLAIN values `put` writes. `put` returns the
    /// page's min and max (column index bytes).
    fn list_page(
        &mut self,
        records: usize,
        n: usize,
        nans: Option<i64>,
        put: impl FnOnce(&mut Vec<u8>) -> Option<(Vec<u8>, Vec<u8>)>,
    ) -> Result<()> {
        let levels = records * n;
        let mut body = Vec::with_capacity(16 + levels * 8);
        body.extend_from_slice(&[0; 4]);
        if n == 1 {
            rle(0, records, 1, &mut body);
        } else if n < 16 {
            // short lists: bit-packed (two RLE runs per record would cost
            // more than the bits)
            let rep: Vec<u8> = (0..levels).map(|i| (i % n != 0) as u8).collect();
            bitpack(&rep, 1, &mut body);
        } else {
            // a run of one 0, then a run of `n - 1` ones, per record
            let mut one = Vec::new();
            rle(0, 1, 1, &mut one);
            rle(1, n - 1, 1, &mut one);
            for _ in 0..records {
                body.extend_from_slice(&one);
            }
        }
        let len = (body.len() - 4) as u32;
        body[..4].copy_from_slice(&len.to_le_bytes());
        let at = body.len();
        body.extend_from_slice(&[0; 4]);
        rle(1, levels, 1, &mut body);
        let len = (body.len() - at - 4) as u32;
        body[at..at + 4].copy_from_slice(&len.to_le_bytes());
        let min_max = put(&mut body);
        if !self.encodings.contains(&Encoding::PLAIN) {
            self.encodings.push(Encoding::PLAIN);
        }
        let (buf, size) = self.compress(body)?;
        let page = Page::DataPage {
            buf,
            num_values: levels as u32,
            encoding: Encoding::PLAIN,
            def_level_encoding: Encoding::RLE,
            rep_level_encoding: Encoding::RLE,
            statistics: None,
        };
        let spec = self.write_page(page, size)?;
        self.data_offset.get_or_insert(spec.offset as i64);
        self.written += spec.bytes_written;
        self.compressed += spec.compressed_size as i64;
        self.uncompressed += spec.uncompressed_size as i64;
        self.offset_index.append_row_count(records as i64);
        self.offset_index
            .append_offset_and_size(spec.offset as i64, spec.compressed_size as i32);
        let (lo, hi) = min_max.unwrap_or_default();
        self.column_index.append(false, lo, hi, 0, nans);
        if self.histograms {
            let rep = LevelHistogram::from(vec![records as i64, (levels - records) as i64]);
            let def = LevelHistogram::from(vec![0, levels as i64]);
            self.column_index.append_histograms(&Some(rep), &Some(def));
        }
        Ok(())
    }

    /// Record a byte array page's string bytes, as arrow-rs does.
    fn unencoded_bytes(&mut self, n: i64) {
        *self.unencoded.get_or_insert(0) += n;
        self.offset_index
            .append_unencoded_byte_array_data_bytes(Some(n));
    }
}

/// A boolean column: PLAIN (bit-packed, as arrow-rs writes v1 pages; there
/// is no boolean dictionary), pages ending after the mini-batch that
/// reaches the row or page size limit.
fn encode_bools(
    descr: &ColumnDescPtr,
    props: &WriterProperties,
    v: &Values<bool>,
) -> Result<(Rope, ColumnCloseResult)> {
    let row_limit = props.data_page_row_count_limit().max(1);
    let size_limit = props.data_page_size_limit();
    let batch = props.write_batch_size().max(1);
    let mut pages: Vec<(Range<usize>, Range<usize>)> = Vec::new();
    let (mut r0, mut v0, mut row, mut val) = (0, 0, 0, 0);
    for &len in &v.lens {
        let mut left = len;
        while left > 0 {
            let step = left.min(batch);
            let n = v
                .valid
                .as_ref()
                .map_or(step, |b| b.slice(row, step).count_set_bits());
            (row, val, left) = (row + step, val + n, left - step);
            if row - r0 >= row_limit || (val - v0) / 8 >= size_limit {
                pages.push((r0..row, v0..val));
                (r0, v0) = (row, val);
            }
        }
    }
    if row > r0 {
        pages.push((r0..row, v0..val));
    }
    let mut sink = rope_sink();
    let mut w = ChunkWriter::new(descr, props, &mut sink);
    let mut page_ranges = Vec::with_capacity(pages.len());
    for (rows, vals) in pages {
        let vals = &v.vals[vals];
        let mm = vals
            .iter()
            .fold(None, |acc: Option<(bool, bool)>, &x| match acc {
                None => Some((x, x)),
                Some((lo, hi)) => Some((lo & x, hi | x)),
            });
        page_ranges.push(mm);
        w.data_page(
            rows,
            vals.len(),
            v.valid.as_ref(),
            Encoding::PLAIN,
            None,
            |body| {
                let start = body.len();
                body.resize(start + vals.len().div_ceil(8), 0);
                for (i, &x) in vals.iter().enumerate() {
                    body[start + i / 8] |= (x as u8) << (i % 8);
                }
                mm.map(|(lo, hi)| (vec![lo as u8], vec![hi as u8]))
            },
        )?;
    }
    let chunk = page_ranges
        .iter()
        .flatten()
        .fold(None, |acc: Option<(bool, bool)>, &(a, b)| match acc {
            None => Some((a, b)),
            Some((lo, hi)) => Some((lo & a, hi | b)),
        });
    let order = boundary_order(page_ranges.into_iter());
    let nulls = (v.rows - v.vals.len()) as u64;
    let (lo, hi) = chunk.unzip();
    let stats = Statistics::boolean(lo, hi, None, Some(nulls), false);
    let close = w.finish(v.rows, v.vals.len(), stats, order)?;
    Ok((sink.into_inner()?.finish(), close))
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
) -> Result<(Rope, ColumnCloseResult)> {
    let path = descr.path();
    // the chunk's min and max up front only for a dictionary, which needs
    // the span; else they are folded from the pages' (one pass fewer)
    let dict_on = props.dictionary_enabled(path) && !v.vals.is_empty();
    let chunk_mm = if dict_on { min_max(&v.vals) } else { None };
    // the dictionary's direct table spans every value, NaNs included
    // (statistics leave them out)
    let span = if !dict_on {
        None
    } else if T::FLOAT {
        let lo = v.vals.iter().copied().fold(T::MAX, Ord::min);
        let hi = v.vals.iter().copied().fold(T::MIN, Ord::max);
        Some((lo, hi))
    } else {
        chunk_mm
    };
    let plan = plan(v, props, path, span);
    let mut sink = rope_sink();
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
    let mut chunk_nans = 0u64;
    for p in &plan.pages {
        let (v0, v1) = (p.vals.start, p.vals.end);
        let vals = &v.vals[v0..v1];
        let mm = min_max(vals);
        page_ranges.push(mm);
        let (encoding, idx) = match p.entries {
            Some(n) => (Encoding::RLE_DICTIONARY, Some((n, &plan.idx[v0..v1]))),
            None => (Encoding::PLAIN, None),
        };
        let nans = nan_count(vals).map(|c| c as i64);
        chunk_nans += nans.unwrap_or(0) as u64;
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
    let (lo, hi) = match chunk_mm {
        Some((lo, hi)) => (Some(lo), Some(hi)),
        None => page_ranges
            .iter()
            .flatten()
            .fold((None, None), |(lo, hi), &(a, b)| {
                (
                    fold_stat(lo, a, |x, y| x < y),
                    fold_stat(hi, b, |x, y| x > y),
                )
            }),
    };
    let order = boundary_order(page_ranges.into_iter());
    let nulls = (v.rows - v.vals.len()) as u64;
    let nans = (T::FLOAT && !v.vals.is_empty()).then_some(chunk_nans);
    let close = w.finish(v.rows, v.vals.len(), T::stats(lo, hi, nulls, nans), order)?;
    Ok((sink.into_inner()?.finish(), close))
}

// ---------------------------------------------------------------------------
// Fixed-size lists of numbers

/// The values of each of a chunk's fixed-size list arrays, as slices of
/// the arrays (no copy); an error if any list or value is null (the column
/// is declared required).
fn list_slices<'a, T: 'a>(
    arrays: &'a [ArrayRef],
    values: impl Fn(&'a ArrayRef) -> &'a [T],
) -> Result<Vec<&'a [T]>> {
    arrays
        .iter()
        .map(|a| {
            let l = a.as_fixed_size_list();
            let child = l.values();
            if l.null_count() > 0 || child.null_count() > 0 {
                return Err(ParquetError::General(
                    "null in a required fixed-size list column".into(),
                ));
            }
            let start = l.value_offset_at(0);
            Ok(&values(child)[start..start + l.len() * l.value_length() as usize])
        })
        .collect()
}

/// A chunk's `List` arrays as one list column: offsets rebased and values
/// copied end to end (an error if any list or value is null: the column is
/// declared required), then [`encode_var_list`].
fn list_parts<A>(
    arrays: &[ArrayRef],
    descr: &ColumnDescPtr,
    props: &WriterProperties,
) -> Result<(Rope, ColumnCloseResult)>
where
    A: ArrowPrimitiveType,
    A::Native: Plain,
{
    let rows: usize = arrays.iter().map(|a| a.len()).sum();
    let mut offsets = Vec::with_capacity(rows + 1);
    offsets.push(0i32);
    let mut values: Vec<A::Native> = Vec::new();
    for a in arrays {
        let l = a.as_list::<i32>();
        let child = l.values();
        if l.null_count() > 0 || child.null_count() > 0 {
            return Err(ParquetError::General(
                "null in a required list column".into(),
            ));
        }
        let o = l.value_offsets();
        let (first, last) = (o[0] as usize, o[o.len() - 1] as usize);
        let base = values.len() as i32 - o[0];
        offsets.extend(o[1..].iter().map(|&x| x + base));
        values.extend_from_slice(&child.as_primitive::<A>().values()[first..last]);
    }
    encode_var_list(descr, props, &offsets, &values)
}

/// `f32`s as [`F32`]s, in place.
fn f32_bits(v: &[f32]) -> &[F32] {
    // SAFETY: `F32` is a transparent `u32`, of the size and alignment of
    // `f32`, and every bit pattern is valid for both
    unsafe { from_raw_parts(v.as_ptr().cast(), v.len()) }
}

/// `f64`s as [`F64`]s, in place.
fn f64_bits(v: &[f64]) -> &[F64] {
    // SAFETY: as `f32_bits`
    unsafe { from_raw_parts(v.as_ptr().cast(), v.len()) }
}

/// Fold page minima (or maxima) into the chunk's as arrow-rs does: a NaN
/// gives way to any number, then `better` decides.
fn fold_stat<T: Native>(acc: Option<T>, x: T, better: impl Fn(T, T) -> bool) -> Option<T> {
    Some(match acc {
        None => x,
        Some(a) if a.is_nan() && !x.is_nan() => x,
        Some(a) if !a.is_nan() && x.is_nan() => a,
        Some(a) if better(x, a) => x,
        Some(a) => a,
    })
}

/// A required `FixedSizeList` column of `n` values per record, PLAIN, in
/// pages of whole records (a page never spans two of the input arrays,
/// which are encoded where they are).
fn encode_list<T: Native>(
    descr: &ColumnDescPtr,
    props: &WriterProperties,
    n: usize,
    parts: &[&[T]],
) -> Result<(Rope, ColumnCloseResult)> {
    let per_page = props
        .data_page_row_count_limit()
        .min(props.data_page_size_limit() / (n * T::SIZE))
        .max(1);
    let mut sink = rope_sink();
    let mut w = ChunkWriter::new(descr, props, &mut sink);
    w.list = Some(n);
    let bytes = |x: T| {
        let mut b = Vec::new();
        x.put(&mut b);
        b
    };
    let (mut lo, mut hi) = (None, None);
    let mut nans = 0u64;
    let mut levels = 0;
    let mut page_ranges = Vec::new();
    for part in parts {
        for page in part.chunks(per_page * n) {
            let mm = min_max(page);
            if let Some((a, b)) = mm {
                lo = fold_stat(lo, a, |x, y| x < y);
                hi = fold_stat(hi, b, |x, y| x > y);
            }
            page_ranges.push(mm);
            let page_nans = nan_count(page);
            nans += page_nans.unwrap_or(0) as u64;
            w.list_page(page.len() / n, n, page_nans.map(|c| c as i64), |body| {
                T::put_all(page, body);
                mm.map(|(lo, hi)| (bytes(lo), bytes(hi)))
            })?;
            levels += page.len();
        }
    }
    let order = boundary_order(page_ranges.into_iter());
    let nans = (T::FLOAT && levels > 0).then_some(nans);
    let close = w.finish(levels, levels, T::stats(lo, hi, 0, nans), order)?;
    Ok((sink.into_inner()?.finish(), close))
}

// ---------------------------------------------------------------------------
// Plain slices (the direct distribution writer, no Arrow)

/// A Rust number encoded here, and the physical value it is stored as.
pub(crate) trait Plain: Copy {
    type N: Native;
    /// `v` as physical values, in place.
    fn native(v: &[Self]) -> &[Self::N];
}

impl Plain for i32 {
    type N = i32;
    fn native(v: &[i32]) -> &[i32] {
        v
    }
}

impl Plain for i64 {
    type N = i64;
    fn native(v: &[i64]) -> &[i64] {
        v
    }
}

impl Plain for f32 {
    type N = F32;
    fn native(v: &[f32]) -> &[F32] {
        f32_bits(v)
    }
}

impl Plain for f64 {
    type N = F64;
    fn native(v: &[f64]) -> &[F64] {
        f64_bits(v)
    }
}

/// Whether a column of `descr` can be encoded here under `props`.
pub(crate) fn slices_ok(descr: &ColumnDescriptor, props: &WriterProperties) -> bool {
    common_ok(descr.path(), props)
}

/// A required column of numbers, read in place.
pub(crate) fn encode_values<T: Plain>(
    descr: &ColumnDescPtr,
    props: &WriterProperties,
    v: &[T],
) -> Result<(Rope, ColumnCloseResult)> {
    encode(descr, props, &Values::borrowed(T::native(v)))
}

/// A required string column in Arrow `Utf8` layout (`offsets` has one more
/// entry than there are rows).
pub(crate) fn encode_strings(
    descr: &ColumnDescPtr,
    props: &WriterProperties,
    offsets: &[i32],
    data: &[u8],
) -> Result<(Rope, ColumnCloseResult)> {
    let strs: Vec<&[u8]> = offsets
        .windows(2)
        .map(|w| &data[w[0] as usize..w[1] as usize])
        .collect();
    encode_bytes(descr, props, &Values::borrowed(&strs), &[])
}

/// A required `list<required number>` column (standard 3-level layout, so
/// `max_def = 1`, `max_rep = 1`) from Arrow `List` offsets (a leading 0)
/// and the values, in pages of whole records: a dictionary as for flat
/// columns (falling back to PLAIN at its page size limit), else PLAIN. An
/// empty list is one level with definition 0, counted as a null as arrow-rs
/// counts it.
pub(crate) fn encode_var_list<P: Plain>(
    descr: &ColumnDescPtr,
    props: &WriterProperties,
    offsets: &[i32],
    values: &[P],
) -> Result<(Rope, ColumnCloseResult)> {
    let values = P::native(values);
    let records = offsets.len().saturating_sub(1);
    let base = offsets.first().map_or(0, |&o| o as usize);
    // value range of records `r0..r1`, relative to `values`' start
    let span_of = |r0: usize, r1: usize| offsets[r0] as usize - base..offsets[r1] as usize - base;
    let size = <P::N as Native>::SIZE;
    let path = descr.path();
    let row_limit = props.data_page_row_count_limit().max(1);
    let size_limit = props.data_page_size_limit();
    let batch = props.write_batch_size().max(1);
    let dict_limit = props.dictionary_page_size_limit();
    let n_vals = if records > 0 {
        span_of(0, records).len()
    } else {
        0
    };
    let values = &values[..n_vals];

    let stats = min_max(values);
    let mut dict = if props.dictionary_enabled(path) && n_vals > 0 {
        // the direct table spans every value, NaNs included
        let span = if <P::N as Native>::FLOAT {
            let lo = values.iter().copied().fold(<P::N as Native>::MAX, Ord::min);
            let hi = values.iter().copied().fold(<P::N as Native>::MIN, Ord::max);
            Some((lo, hi))
        } else {
            stats
        };
        span.map(|(lo, hi)| Dict::new(lo, hi, n_vals, dict_limit / size + batch))
    } else {
        None
    };
    let mut idx = Vec::with_capacity(if dict.is_some() { n_vals } else { 0 });
    // pages: whole mini-batches of records until a limit is reached, the
    // dictionary's entry count when each was flushed (`None`: PLAIN)
    let mut pages: Vec<(Range<usize>, Option<usize>)> = Vec::new();
    let mut fell_back = dict.is_none();
    let (mut r0, mut r1) = (0, 0);
    while r1 < records {
        let next = (r1 + batch).min(records);
        if let Some(d) = dict.as_mut().filter(|_| !fell_back) {
            d.encode(&values[span_of(r1, next)], &mut idx);
        }
        r1 = next;
        let entries = dict
            .as_ref()
            .filter(|_| !fell_back)
            .map(|d| d.entries.len());
        let n = span_of(r0, r1).len();
        let bytes = match entries {
            Some(e) => 1 + n * (64 - (e.max(1) as u64 - 1).leading_zeros() as usize) / 8,
            None => n * size,
        };
        let fall_back = entries.is_some_and(|e| e * size >= dict_limit);
        if r1 == records || r1 - r0 >= row_limit || bytes >= size_limit || fall_back {
            pages.push((r0..r1, entries));
            r0 = r1;
        }
        fell_back |= fall_back;
    }

    let mut sink = rope_sink();
    let mut w = ChunkWriter::new(descr, props, &mut sink);
    w.records = Some(records);
    if let Some(d) = &dict {
        let mut body = Vec::with_capacity(d.entries.len() * size);
        <P::N as Native>::put_all(&d.entries, &mut body);
        w.dictionary(body, d.entries.len())?;
    }
    let bytes = |x: P::N| {
        let mut b = Vec::new();
        x.put(&mut b);
        b
    };
    let (mut nans, mut empties, mut levels) = (0u64, 0usize, 0usize);
    let mut page_ranges = Vec::with_capacity(pages.len());
    let (mut rep, mut def) = (Vec::new(), Vec::new());
    let (mut rep_bits, mut def_bits) = (Vec::new(), Vec::new());
    for (recs, entries) in pages {
        let vr = span_of(recs.start, recs.end);
        let vals = &values[vr.clone()];
        // levels straight from the offsets: bitmaps of every level, all 1
        // but 0 at each record's first level (repetition) and at each
        // empty list (definition)
        let most = vals.len() + recs.len();
        rep_bits.clear();
        rep_bits.resize(most.div_ceil(8) + 1, 0xFF);
        def_bits.clear();
        def_bits.resize(most.div_ceil(8) + 1, 0xFF);
        let mut page_empties = 0;
        let mut at = 0;
        for r in recs.clone() {
            let len = (offsets[r + 1] - offsets[r]) as usize;
            let (byte, bit) = (at / 8, !(1u8 << (at % 8)));
            rep_bits[byte] &= bit;
            if len == 0 {
                def_bits[byte] &= bit;
                page_empties += 1;
            }
            at += len.max(1);
        }
        let page_levels = at;
        rep.clear();
        bit_levels(&rep_bits, page_levels, &mut rep);
        def.clear();
        if page_empties == 0 {
            rle(1, page_levels, 1, &mut def);
        } else {
            bit_levels(&def_bits, page_levels, &mut def);
        }
        let mm = min_max(vals);
        page_ranges.push(mm);
        let page_nans = nan_count(vals);
        nans += page_nans.unwrap_or(0) as u64;
        let encoding = match entries {
            Some(_) => Encoding::RLE_DICTIONARY,
            None => Encoding::PLAIN,
        };
        w.levels_page(
            recs.len(),
            page_levels,
            vals.len(),
            &rep,
            &def,
            encoding,
            page_nans.map(|c| c as i64),
            |body| {
                match entries {
                    Some(n) => {
                        // as arrow-rs: the width of the dictionary when the
                        // page was flushed
                        let bits = 64 - (n.saturating_sub(1) as u64).leading_zeros();
                        body.push(bits as u8);
                        hybrid(&idx[vr], bits, body);
                    }
                    None => <P::N as Native>::put_all(vals, body),
                }
                mm.map(|(lo, hi)| (bytes(lo), bytes(hi)))
            },
        )?;
        empties += page_empties;
        levels += page_levels;
    }
    let order = boundary_order(page_ranges.into_iter());
    let nans = (<P::N as Native>::FLOAT && n_vals > 0).then_some(nans);
    let (lo, hi) = stats.unzip();
    let stats = <P::N as Native>::stats(lo, hi, empties as u64, nans);
    let close = w.finish(levels, n_vals, stats, order)?;
    Ok((sink.into_inner()?.finish(), close))
}

impl ChunkWriter<'_> {
    /// A v1 data page of `records` records over `levels` levels (`vals` of
    /// them values), with the levels already encoded (no length prefix),
    /// then the values `put` writes in `encoding`. `put` returns the page's min
    /// and max (column index bytes).
    #[allow(clippy::too_many_arguments)]
    fn levels_page(
        &mut self,
        records: usize,
        levels: usize,
        vals: usize,
        rep: &[u8],
        def: &[u8],
        encoding: Encoding,
        nans: Option<i64>,
        put: impl FnOnce(&mut Vec<u8>) -> Option<(Vec<u8>, Vec<u8>)>,
    ) -> Result<()> {
        let mut body = Vec::with_capacity(8 + rep.len() + def.len() + vals * 8);
        for l in [rep, def] {
            body.extend_from_slice(&(l.len() as u32).to_le_bytes());
            body.extend_from_slice(l);
        }
        let min_max = put(&mut body);
        if encoding == Encoding::PLAIN && !self.encodings.contains(&Encoding::PLAIN) {
            self.encodings.push(Encoding::PLAIN);
        }
        let (buf, size) = self.compress(body)?;
        let page = Page::DataPage {
            buf,
            num_values: levels as u32,
            encoding,
            def_level_encoding: Encoding::RLE,
            rep_level_encoding: Encoding::RLE,
            statistics: None,
        };
        let spec = self.write_page(page, size)?;
        self.data_offset.get_or_insert(spec.offset as i64);
        self.written += spec.bytes_written;
        self.compressed += spec.compressed_size as i64;
        self.uncompressed += spec.uncompressed_size as i64;
        self.offset_index.append_row_count(records as i64);
        self.offset_index
            .append_offset_and_size(spec.offset as i64, spec.compressed_size as i32);
        let nulls = (levels - vals) as i64;
        let (lo, hi) = min_max.unwrap_or_default();
        self.column_index.append(vals == 0, lo, hi, nulls, nans);
        if self.histograms {
            let rep = LevelHistogram::from(vec![records as i64, (levels - records) as i64]);
            let def = LevelHistogram::from(vec![nulls, vals as i64]);
            self.column_index.append_histograms(&Some(rep), &Some(def));
        }
        Ok(())
    }
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
    /// [`view_key`]s already seen and their entries, in an open-addressing
    /// table (`views[h]` is empty when `view_ids[h]` is `u32::MAX`). Many
    /// views can share an entry (the same string at different places).
    /// Freed, and no longer used, once it stops paying (see
    /// [`Self::encode_keyed`]).
    views: Vec<u128>,
    view_ids: Vec<u32>,
    view_bits: u32,
    view_count: usize,
    /// Values looked up through `views`.
    view_lookups: usize,
    keyed: bool,
}

/// Most views [`BytesDict`] keeps; past that the column has so many
/// distinct strings that looking up the bytes alone is cheaper.
const MAX_VIEWS: usize = 1 << 16;

/// A key for a string from its Arrow view `v` and its bytes `x`: the view
/// itself for a short string (its bytes are in it), else its length, prefix
/// and address. Equal keys are equal strings, across arrays too (a long
/// view's buffer index is only meaningful within its array).
#[inline(always)]
fn view_key((&v, x): (&u128, &&[u8])) -> u128 {
    let addr = (x.as_ptr() as usize as u128) << 64;
    if v as u32 > 12 {
        v as u64 as u128 | addr
    } else {
        v
    }
}

/// A hash of a [`view_key`]: no branch on the string's length.
#[inline(always)]
fn view_hash(v: u128, bits: u32) -> usize {
    let (lo, hi) = (v as u64, (v >> 64) as u64);
    let k = (lo.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ hi).wrapping_mul(0xD6E8_FEB8_6659_FD93);
    (k >> (64 - bits)) as usize
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
            views: vec![0; 1 << 10],
            view_ids: vec![u32::MAX; 1 << 10],
            view_bits: 10,
            view_count: 0,
            view_lookups: 0,
            keyed: true,
        }
    }

    /// [`Self::encode`], with `keys` the [`view_key`]s of `xs` (or empty).
    /// The view table is a cache: once most views turn out new (long
    /// strings each at their own address) or there are too many, it is
    /// freed and only the bytes are looked up.
    fn encode_keyed(&mut self, xs: &[&'a [u8]], keys: &[u128], idx: &mut Vec<u32>) {
        if self.keyed
            && (self.view_count >= MAX_VIEWS
                || self.view_lookups >= 4096 && self.view_count * 4 > self.view_lookups)
        {
            self.keyed = false;
            self.views = Vec::new();
            self.view_ids = Vec::new();
        }
        if keys.is_empty() || !self.keyed {
            return self.encode(xs, idx);
        }
        self.view_lookups += xs.len();
        for (&x, &k) in xs.iter().zip(keys) {
            let mask = (1usize << self.view_bits) - 1;
            let mut h = view_hash(k, self.view_bits);
            let i = loop {
                match self.view_ids[h] {
                    u32::MAX => {
                        let i = self.lookup(x);
                        (self.views[h], self.view_ids[h]) = (k, i);
                        self.view_count += 1;
                        if self.view_count * 2 > 1 << self.view_bits {
                            self.grow_views();
                        }
                        break i;
                    }
                    i if self.views[h] == k => break i,
                    _ => h = (h + 1) & mask,
                }
            };
            idx.push(i);
        }
    }

    fn grow_views(&mut self) {
        let old = take(&mut self.views);
        let old_ids = take(&mut self.view_ids);
        self.view_bits += 1;
        let mask = (1usize << self.view_bits) - 1;
        self.views = vec![0; 1 << self.view_bits];
        self.view_ids = vec![u32::MAX; 1 << self.view_bits];
        for (k, i) in old.into_iter().zip(old_ids) {
            if i != u32::MAX {
                let mut h = view_hash(k, self.view_bits);
                while self.view_ids[h] != u32::MAX {
                    h = (h + 1) & mask;
                }
                (self.views[h], self.view_ids[h]) = (k, i);
            }
        }
    }

    fn encode(&mut self, xs: &[&'a [u8]], idx: &mut Vec<u32>) {
        idx.extend(xs.iter().map(|&x| self.lookup(x)));
    }

    /// The entry of `x`, added if new.
    #[inline]
    fn lookup(&mut self, x: &'a [u8]) -> u32 {
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
        if self.entries.len() * 2 > 1 << self.bits {
            self.grow();
        }
        i
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
    keys: &[u128],
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
                Some(d) => d.encode_keyed(xs, keys.get(val..val + n).unwrap_or(&[]), &mut idx),
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
    keys: &[u128],
) -> Result<(Rope, ColumnCloseResult)> {
    let path = descr.path();
    let (pages, dict, idx) = plan_bytes(v, keys, props, path);
    let mut sink = rope_sink();
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
                            let start = body.len();
                            let mut at = body.as_mut_ptr().wrapping_add(start);
                            for (i, &x) in vals.iter().enumerate() {
                                if let Some(ahead) = vals.get(i + 16) {
                                    prefetch(ahead);
                                }
                                // SAFETY: `need` bytes were reserved for the
                                // lengths and bytes of all of `vals`.
                                unsafe {
                                    at.cast::<[u8; 4]>()
                                        .write_unaligned((x.len() as u32).to_le_bytes());
                                    copy_nonoverlapping(x.as_ptr(), at.add(4), x.len());
                                    at = at.add(4 + x.len());
                                }
                                if less(x, lo) {
                                    lo = x;
                                } else if less(hi, x) {
                                    hi = x;
                                }
                            }
                            // SAFETY: all `need` bytes were written
                            unsafe { body.set_len(start + need) };
                            strings = (need - 4 * vals.len()) as i64;
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
    Ok((sink.into_inner()?.finish(), close))
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
            // up to 2^16 slots (1 MiB of 16-byte slots); bigger
            // dictionaries grow from there
            let most = n.min(cap).max(1);
            (2 * most)
                .next_power_of_two()
                .trailing_zeros()
                .clamp(10, 16)
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
    // at most one bit-packed group header per 8 values, plus the packed bits
    out.reserve(n.div_ceil(8) * (w as usize + 2) + 16);
    // A run of `x` is RLE'd when it covers a whole group of 8 counted from
    // the end of the last RLE run (`lit`): from the first group boundary in
    // it to its end. So look only at those groups; the first one whose 8
    // values are equal starts the next RLE run.
    let (mut lit, mut g) = (0, 0);
    while g + 8 <= n {
        let x = v[g];
        if v[g + 1..g + 8].iter().all(|&y| y == x) {
            let mut r = g;
            while r > lit && v[r - 1] == x {
                r -= 1;
            }
            let s = lit + (r - lit).div_ceil(8) * 8;
            let mut j = g + 8;
            while j < n && v[j] == x {
                j += 1;
            }
            if s > lit {
                bitpack(&v[lit..s], w, out);
            }
            rle(x.into(), j - s, w, out);
            (lit, g) = (j, j);
        } else {
            g += 8;
        }
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
    let len = groups * w as usize;
    let start = out.len();
    // 8 bytes of slack: every store below writes a whole word
    out.resize(start + len + 8, 0);
    let buf = &mut out[start..];
    let (mut acc, mut bits, mut at) = (0u64, 0u32, 0usize);
    let mut store = |at: usize, acc: u64| buf[at..at + 8].copy_from_slice(&acc.to_le_bytes());
    for &x in v {
        let x: u64 = x.into();
        acc |= x << bits;
        bits += w;
        if bits >= 64 {
            store(at, acc);
            at += 8;
            bits -= 64;
            // the bits of `x` that did not fit (none if it ended the word)
            acc = if bits == 0 { 0 } else { x >> (w - bits) };
        }
    }
    // the last partial word; padding values are zero bits
    store(at, acc);
    out.truncate(start + len);
}

fn varint(mut x: u64, out: &mut Vec<u8>) {
    while x >= 0x80 {
        out.push(x as u8 | 0x80);
        x >>= 7;
    }
    out.push(x as u8);
}

#[cfg(test)]
mod tests {
    use super::{bitpack, encode_values, hybrid, rle, varint};
    use parquet::basic::{Repetition, Type as PhysicalType};
    use parquet::file::properties::WriterProperties;
    use parquet::schema::types::{ColumnDescriptor, ColumnPath, Type};
    use std::sync::Arc;

    /// Page bodies stay in their own buffers: the chunk is not one copy.
    #[test]
    fn rope_keeps_page_bodies() {
        let t = Type::primitive_type_builder("x", PhysicalType::INT64)
            .with_repetition(Repetition::REQUIRED)
            .build()
            .unwrap();
        let d = Arc::new(ColumnDescriptor::new(
            Arc::new(t),
            0,
            0,
            ColumnPath::from("x"),
        ));
        let props = WriterProperties::builder()
            .set_dictionary_enabled(false)
            .build();
        let v: Vec<i64> = (0..1 << 18).collect();
        let (rope, close) = encode_values(&d, &props, &v).unwrap();
        let pages = 2 * 1024 * 1024 / props.data_page_size_limit();
        assert!(rope.parts.len() >= 2 * pages, "{} parts", rope.parts.len());
        let len: usize = rope.parts.iter().map(|p| p.len()).sum();
        assert_eq!(len as i64, close.metadata.compressed_size());
    }

    /// The straightforward RLE / bit-packing hybrid: scan each run, emit it
    /// as RLE when it covers a whole group, pack one bit at a time.
    fn hybrid_ref(v: &[u32], w: u32, out: &mut Vec<u8>) {
        let pack = |v: &[u32], out: &mut Vec<u8>| {
            let groups = v.len().div_ceil(8);
            varint(((groups as u64) << 1) | 1, out);
            let (mut acc, mut bits) = (0u64, 0u32);
            let pad = groups * 8 - v.len();
            for x in v
                .iter()
                .map(|&x| x as u64)
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
        };
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
                    pack(&v[lit..s], out);
                }
                rle(x.into(), j - s, w, out);
                lit = j;
            }
            i = j;
        }
        if lit < n {
            pack(&v[lit..], out);
        }
    }

    /// xorshift: deterministic test data without a dependency
    fn next(s: &mut u64) -> u64 {
        *s ^= *s << 13;
        *s ^= *s >> 7;
        *s ^= *s << 17;
        *s
    }

    #[test]
    fn hybrid_matches_the_reference_encoding() {
        let mut s = 0x9E37_79B9_7F4A_7C15;
        for w in 0..=32u32 {
            let max = if w == 0 { 0 } else { u32::MAX >> (32 - w) };
            for len in (0..80).chain([255, 256, 257, 1000, 4097]) {
                for runs in [1u64, 2, 5, 13, 40] {
                    // runs of random length (mean `runs`) of random values
                    let mut v = Vec::with_capacity(len);
                    while v.len() < len {
                        let x = (next(&mut s) as u32) & max;
                        let k = (next(&mut s) % (2 * runs)) as usize + 1;
                        v.extend(std::iter::repeat_n(x, k.min(len - v.len())));
                    }
                    let (mut got, mut want) = (Vec::new(), Vec::new());
                    hybrid(&v, w, &mut got);
                    hybrid_ref(&v, w, &mut want);
                    assert_eq!(got, want, "w {w} len {len} runs {runs}");
                    if !v.is_empty() {
                        let (mut got, mut want) = (Vec::new(), Vec::new());
                        bitpack(&v, w, &mut got);
                        // a single literal run of all of `v`
                        let mut only = v.clone();
                        only.dedup();
                        if only.len() == v.len() {
                            hybrid_ref(&v, w, &mut want);
                            assert_eq!(got, want, "bitpack w {w} len {len}");
                        }
                    }
                }
            }
        }
    }
}
