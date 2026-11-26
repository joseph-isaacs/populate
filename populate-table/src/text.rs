//! The TPC text formats (dsdgen's `.dat`, dbgen's `.tbl`), written straight
//! from Arrow batches, and a parallel writer for them.
//!
//! One line per row: every field followed by `|`, NULL as an empty field,
//! decimals with exactly `scale` digits (`-0.05`), dates as `YYYY-MM-DD`.
//! This is ~10x faster than formatting rows with `Display`; populate-gen-tpcds
//! and populate-gen-tpch test it against their reference generators' `Display`
//! output for every table.
//!
//! [`write_text_many`] runs any [`TextChunks`] on all cores and has the
//! workers write the files themselves.

use arrow_array::cast::AsArray;
use arrow_array::types::{Date32Type, Decimal128Type, Int32Type, Int64Type};
use arrow_array::{Array, RecordBatch, StringViewArray};
use arrow_buffer::NullBuffer;
use arrow_schema::{DataType, Field};
use std::collections::BTreeMap;
use std::io::{self, Write};
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Condvar, Mutex};
use std::thread;
use std::time::Instant;

use crate::sched::{Schedule, resolve_threads};
use populate_core::{ChildCols, Date32, Decimal, Nullable, SpreadCols, StrCol, TableCols};

/// Text encoding of `.dat` output.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Encoding {
    /// ISO-8859-1, one byte per character: what dsdgen writes, since it
    /// copies its (Latin-1) distribution files byte for byte. In TPC-DS only
    /// `customer.c_birth_country` has non-ASCII values (`CÔTE D'IVOIRE`,
    /// `RÉUNION`); TPC-H text is all ASCII.
    #[default]
    Latin1,
    /// UTF-8, as `tpcgen-cli` writes.
    Utf8,
}

enum Col<'a> {
    I32(&'a [i32]),
    I64(&'a [i64]),
    /// Values, their scale, and the scale dsdgen prints.
    Dec(&'a [i128], u32, u32),
    Date(&'a [i32]),
    Str(&'a StringViewArray),
}

/// Append `batch` to `out` as text lines, decimals with their own scale.
pub fn write_batch(batch: &RecordBatch, encoding: Encoding, out: &mut Vec<u8>) {
    write_batch_with(batch, encoding, |_| None, out)
}

/// [`write_batch`], printing the decimal columns for which `print_scale`
/// returns `Some(p)` with `p` (at most their scale) decimals: dsdgen prints
/// its GMT offsets with none, dbgen its quantities.
pub fn write_batch_with(
    batch: &RecordBatch,
    encoding: Encoding,
    print_scale: impl Fn(&Field) -> Option<u32>,
    out: &mut Vec<u8>,
) {
    let rows = batch.num_rows();
    let mut bound = rows; // newlines
    let cols: Vec<(Col<'_>, Option<Valid>)> = batch
        .columns()
        .iter()
        .zip(batch.schema_ref().fields())
        .map(|(a, f)| {
            let c = match a.data_type() {
                DataType::Int32 => Col::I32(a.as_primitive::<Int32Type>().values()),
                DataType::Int64 => Col::I64(a.as_primitive::<Int64Type>().values()),
                DataType::Decimal128(_, s) if *s >= 0 => {
                    let print = print_scale(f).map_or(*s as u32, |p| p.min(*s as u32));
                    Col::Dec(
                        a.as_primitive::<Decimal128Type>().values(),
                        *s as u32,
                        print,
                    )
                }
                DataType::Date32 => Col::Date(a.as_primitive::<Date32Type>().values()),
                DataType::Utf8View => Col::Str(a.as_string_view()),
                t => panic!("no text format for {t}"),
            };
            bound += rows
                * match &c {
                    Col::Str(a) => {
                        bound += a.views().iter().map(|v| *v as u32 as usize).sum::<usize>();
                        1 + 12 // separator, inline-copy slack
                    }
                    // up to 39 digits, sign and point for a wide decimal
                    Col::Dec(..) => MAX_DECIMAL + 1,
                    _ => MAX_NUMBER + 1,
                };
            (c, a.nulls().filter(|n| n.null_count() > 0).map(Valid::new))
        })
        .collect();
    out.reserve(bound);
    let start = out.len();
    let mut w = Cursor(out.spare_capacity_mut().as_mut_ptr().cast::<u8>());
    let base = w.0;
    // the long strings of view columns, to prefetch a few rows ahead: they
    // can be anywhere (TPC-H's comments are random slices of a 300 MiB
    // pool), and each would be a cache miss when its row comes
    let views: Vec<&StringViewArray> = cols
        .iter()
        .filter_map(|(c, _)| match c {
            Col::Str(a) if !a.data_buffers().is_empty() => Some(*a),
            _ => None,
        })
        .collect();
    const AHEAD: usize = 8;
    for r in 0..rows {
        if r + AHEAD < rows {
            for a in &views {
                prefetch_view(a, r + AHEAD);
            }
        }
        for (c, nulls) in &cols {
            // SAFETY: `r` is a row of the batch.
            if nulls.as_ref().is_none_or(|n| unsafe { n.get(r) }) {
                // SAFETY: `bound` reserved room for the widest number and the
                // full bytes of every string (Latin-1 never needs more bytes
                // than UTF-8), plus every separator and newline.
                // Every column has `rows` values, so `r` is in bounds.
                unsafe {
                    match c {
                        Col::I32(v) => w.int(*v.get_unchecked(r) as i64),
                        Col::I64(v) => w.int(*v.get_unchecked(r)),
                        Col::Dec(v, s, p) => w.decimal128(*v.get_unchecked(r), *s, *p),
                        Col::Date(v) => w.day(*v.get_unchecked(r)),
                        Col::Str(a) => w.view(a, r, encoding),
                    }
                }
            }
            unsafe { w.byte(b'|') };
        }
        unsafe { w.byte(b'\n') };
    }
    // SAFETY: every byte in `start..start + written` was initialised above.
    unsafe {
        let written = w.0.offset_from(base) as usize;
        debug_assert!(written <= bound);
        out.set_len(start + written);
    }
}

/// First day of [`DATES`]: 1900-01-01.
const DATES_FROM: i32 = -25_567;

/// `YYYY-MM-DD` of every day from 1900 to 2099, a table lookup instead of
/// the calendar arithmetic (TPC-H and TPC-DS dates are all in it).
static DATES: std::sync::LazyLock<Vec<[u8; 10]>> = std::sync::LazyLock::new(|| {
    (DATES_FROM..47_482)
        .map(|d| {
            let mut b = [0u8; MAX_NUMBER];
            let mut c = Cursor(b.as_mut_ptr());
            // SAFETY: a date in this range is 10 bytes
            unsafe { c.date(d) };
            b[..10].try_into().expect("10 bytes")
        })
        .collect()
});

/// Start loading the bytes of string `r` of `a` into cache, if they are
/// not in its view.
#[inline(always)]
fn prefetch_view(a: &StringViewArray, r: usize) {
    let v = a.views()[r];
    if v as u32 > 12 {
        let (buffer, offset) = ((v >> 64) as u32 as usize, (v >> 96) as usize);
        if let Some(b) = a.data_buffers().get(buffer) {
            #[cfg(target_arch = "x86_64")]
            // SAFETY: a prefetch is only a hint; it never faults.
            unsafe {
                use std::arch::x86_64::{_MM_HINT_T0, _mm_prefetch};
                _mm_prefetch::<_MM_HINT_T0>(b.as_ptr().wrapping_add(offset).cast());
            }
            #[cfg(not(target_arch = "x86_64"))]
            let _ = (b, offset);
        }
    }
}

/// Append `v` and its `|`, as [`write_batch`] writes an integer field.
#[inline]
pub fn int_field(out: &mut Vec<u8>, v: i64) {
    field(out, |w| unsafe { w.int(v) });
}

/// Append the decimal `v / 100` and its `|`, as [`write_batch`] writes a
/// decimal(_, 2) field.
#[inline]
pub fn decimal2_field(out: &mut Vec<u8>, v: i64) {
    field(out, |w| unsafe { w.decimal2(v) });
}

/// Run `f` (which writes at most one number) on a cursor at the end of
/// `out`, then append `|`.
#[inline(always)]
fn field(out: &mut Vec<u8>, f: impl FnOnce(&mut Cursor)) {
    out.reserve(MAX_NUMBER + 1);
    let len = out.len();
    // SAFETY: `f` writes at most `MAX_NUMBER` bytes (with the 8-byte stores
    // of `uint`, which fit in it too), then one `|`, into reserved
    // capacity; all of it is initialised before `set_len`.
    unsafe {
        let mut w = Cursor(out.as_mut_ptr().add(len));
        f(&mut w);
        w.byte(b'|');
        let n = w.0.offset_from(out.as_ptr()) as usize;
        out.set_len(n);
    }
}

/// A validity bitmap as a raw pointer and bit offset: `NullBuffer::is_valid`
/// bounds-checks and re-derives both on every call, which was a quarter of
/// the formatting time.
struct Valid(*const u8, usize);

impl Valid {
    fn new(n: &NullBuffer) -> Self {
        Valid(n.inner().values().as_ptr(), n.offset())
    }

    /// # Safety
    /// `r` must be below the bitmap's length.
    #[inline(always)]
    unsafe fn get(&self, r: usize) -> bool {
        let i = r + self.1;
        unsafe { *self.0.add(i / 8) >> (i % 8) & 1 != 0 }
    }
}

/// Widest integer or date we print: `-` and 19 digits (and the 8-byte
/// stores of `Cursor::uint`, which fit in it).
const MAX_NUMBER: usize = 24;
/// Widest decimal we print: `-`, 39 digits, `.`, plus 8 bytes of store slack.
const MAX_DECIMAL: usize = 49;

const DIGITS: &[u8; 200] = b"\
0001020304050607080910111213141516171819\
2021222324252627282930313233343536373839\
4041424344454647484950515253545556575859\
6061626364656667686970717273747576777879\
8081828384858687888990919293949596979899";

/// The 8 decimal digits of `x < 10^8`, with leading zeros, as ASCII bytes
/// of a little-endian `u64` (most significant digit first). Splits into
/// 4-digit, 2-digit and 1-digit lanes with multiply-shift divisions that
/// are exact in those ranges, all lanes at once.
#[inline(always)]
fn eight(x: u32) -> u64 {
    let v = (x / 10_000) as u64 | (((x % 10_000) as u64) << 32);
    let hi = ((v * 10_486) >> 20) & 0x0000_007F_0000_007F; // lane / 100
    let v = hi | ((v - hi * 100) << 16);
    let hi = ((v * 103) >> 10) & 0x000F_000F_000F_000F; // lane / 10
    let v = hi | ((v - hi * 10) << 8);
    v + ZEROS
}

/// The 4 decimal digits of `x < 10^4`, as [`eight`] makes 8.
#[inline(always)]
fn four(x: u32) -> u32 {
    let v = (x / 100) | ((x % 100) << 16);
    let hi = ((v * 103) >> 10) & 0x000F_000F; // lane / 10
    let v = hi | ((v - hi * 10) << 8);
    v + ZEROS as u32
}

/// `"00000000"` as a `u64`.
const ZEROS: u64 = 0x3030_3030_3030_3030;

/// Write cursor into reserved capacity. Every method is `unsafe`: the
/// caller guarantees room for what it writes ([`TextField::text_bytes`]).
pub struct Cursor(*mut u8);

impl Cursor {
    ///
    /// # Safety
    /// The cursor has room for what this writes ([`Cursor`]).
    #[inline(always)]
    pub unsafe fn byte(&mut self, b: u8) {
        unsafe {
            self.0.write(b);
            self.0 = self.0.add(1);
        }
    }

    ///
    /// # Safety
    /// The cursor has room for what this writes ([`Cursor`]).
    #[inline(always)]
    pub unsafe fn bytes(&mut self, b: &[u8]) {
        unsafe {
            std::ptr::copy_nonoverlapping(b.as_ptr(), self.0, b.len());
            self.0 = self.0.add(b.len());
        }
    }

    #[inline(always)]
    unsafe fn pair(&mut self, d: usize) {
        unsafe {
            std::ptr::copy_nonoverlapping(DIGITS.as_ptr().add(d * 2), self.0, 2);
            self.0 = self.0.add(2);
        }
    }

    /// Decimal digits of `v`. Values below 100 use a byte or digit pair;
    /// larger values use four or eight digits at once ([`four`], [`eight`]),
    /// stored with leading zeros removed. The reservation has room for
    /// 8 bytes per number.
    #[inline]
    unsafe fn uint(&mut self, v: u64) {
        unsafe {
            if v < 10 {
                self.byte(b'0' + v as u8);
            } else if v < 100 {
                self.pair(v as usize);
            } else if v < 10_000 {
                // most keys and prices: half the lanes ([`four`]); each
                // field's call site has its own branch, which predicts
                // well, as a column's values are of one size
                let d = four(v as u32);
                let zeros = ((d - ZEROS as u32) | (1 << 24)).trailing_zeros() / 8;
                self.0
                    .cast::<[u8; 4]>()
                    .write_unaligned((d >> (8 * zeros)).to_le_bytes());
                self.0 = self.0.add(4 - zeros as usize);
            } else if v < 100_000_000 {
                // leading zero digits are the low zero bytes (at most 7)
                let d = eight(v as u32);
                let zeros = ((d - ZEROS) | (1 << 56)).trailing_zeros() / 8;
                self.0
                    .cast::<[u8; 8]>()
                    .write_unaligned((d >> (8 * zeros)).to_le_bytes());
                self.0 = self.0.add(8 - zeros as usize);
            } else {
                self.0 = Self::big(self.0, v);
            }
        }
    }

    /// [`Self::uint`] of `v >= 10^8` at `at`: the high digits, then the
    /// low 8; returns the end. It takes and returns the position, not
    /// `&mut self`, so a caller's cursor can stay in a register.
    #[cold]
    #[inline(never)]
    unsafe fn big(at: *mut u8, v: u64) -> *mut u8 {
        let mut w = Cursor(at);
        unsafe {
            w.uint(v / 100_000_000);
            let d = eight((v % 100_000_000) as u32);
            w.0.cast::<[u8; 8]>().write_unaligned(d.to_le_bytes());
            w.0.add(8)
        }
    }

    ///
    /// # Safety
    /// The cursor has room for what this writes ([`Cursor`]).
    #[inline]
    pub unsafe fn int(&mut self, v: i64) {
        unsafe {
            if v < 0 {
                self.byte(b'-');
            }
            self.uint(v.unsigned_abs());
        }
    }

    /// `%.{scale}f` of `v / 10^scale` (what dsdgen's `print_decimal`
    /// prints; `-0.05` keeps its sign).
    ///
    /// # Safety
    /// The cursor has room for what this writes ([`Cursor`]).
    pub unsafe fn decimal(&mut self, v: i64, scale: u32) {
        unsafe {
            if scale == 0 {
                return self.int(v);
            }
            if v < 0 {
                self.byte(b'-');
            }
            let p = 10u64.pow(scale);
            let a = v.unsigned_abs();
            self.uint(a / p);
            self.byte(b'.');
            let mut digits = [0u8; 19];
            let mut frac = a % p;
            for d in digits[..scale as usize].iter_mut().rev() {
                *d = b'0' + (frac % 10) as u8;
                frac /= 10;
            }
            self.bytes(&digits[..scale as usize]);
        }
    }

    /// A `Decimal128` of scale `scale` printed with `print <= scale`
    /// decimals: [`Self::decimal2`] or [`Self::decimal`] when it fits in an
    /// `i64` (every TPC-DS value), else [`Self::wide_decimal`].
    #[inline]
    unsafe fn decimal128(&mut self, v: i128, scale: u32, print: u32) {
        unsafe {
            match i64::try_from(v) {
                Ok(v) if scale == 2 && print == 2 => self.decimal2(v),
                // TPC-H quantities are stored in hundredths but printed whole.
                Ok(v) if scale == 2 && print == 0 => self.int(v / 100),
                Ok(v) if scale <= 18 => self.decimal(v / 10i64.pow(scale - print), print),
                _ => self.0 = Self::wide_decimal(self.0, v / 10i128.pow(scale - print), print),
            }
        }
    }

    /// [`Self::decimal`] for any `i128` (up to 39 digits) at `at`; returns
    /// the end (as [`Self::big`]).
    #[cold]
    #[inline(never)]
    unsafe fn wide_decimal(at: *mut u8, v: i128, scale: u32) -> *mut u8 {
        let mut w = Cursor(at);
        let a = v.unsigned_abs();
        let p = 10u128.pow(scale);
        let mut text = (a / p).to_string();
        if scale > 0 {
            text.push('.');
            let frac = (a % p).to_string();
            text.extend(std::iter::repeat_n('0', scale as usize - frac.len()));
            text.push_str(&frac);
        }
        unsafe {
            if v < 0 {
                w.byte(b'-');
            }
            w.bytes(text.as_bytes());
        }
        w.0
    }

    /// [`Self::decimal`] at scale 2, with constant divisors.
    ///
    /// # Safety
    /// The cursor has room for what this writes ([`Cursor`]).
    #[inline]
    pub unsafe fn decimal2(&mut self, v: i64) {
        unsafe {
            if v < 0 {
                self.byte(b'-');
            }
            let a = v.unsigned_abs();
            if a < 10_000 {
                // as below, in 4 digits
                let d = four(a as u32);
                let zeros = ((d - ZEROS as u32) | (1 << 8)).trailing_zeros() / 8;
                self.0
                    .cast::<[u8; 4]>()
                    .write_unaligned((d >> (8 * zeros)).to_le_bytes());
                self.0 = self.0.add(2 - zeros as usize);
                let frac = b'.' as u32 | (d >> 16) << 8;
                self.0.cast::<[u8; 4]>().write_unaligned(frac.to_le_bytes());
                self.0 = self.0.add(3);
            } else if a < 100_000_000 {
                // all 8 digits at once: the 6 whole ones without their
                // leading zeros (keeping the units), then `.` and the
                // other 2, stored over the digits' tail
                let d = eight(a as u32);
                let zeros = ((d - ZEROS) | (1 << 40)).trailing_zeros() / 8;
                self.0
                    .cast::<[u8; 8]>()
                    .write_unaligned((d >> (8 * zeros)).to_le_bytes());
                self.0 = self.0.add(6 - zeros as usize);
                let frac = b'.' as u32 | ((d >> 48) as u32) << 8;
                self.0.cast::<[u8; 4]>().write_unaligned(frac.to_le_bytes());
                self.0 = self.0.add(3);
            } else {
                self.uint(a / 100);
                self.byte(b'.');
                self.pair((a % 100) as usize);
            }
        }
    }

    /// Days since 1970-01-01 as `YYYY-MM-DD`, looked up in `DATES` for
    /// the years 1900 to 2099.
    ///
    /// # Safety
    /// The cursor has room for what this writes ([`Cursor`]).
    #[inline]
    pub unsafe fn day(&mut self, days: i32) {
        unsafe {
            match DATES.get(days.wrapping_sub(DATES_FROM) as u32 as usize) {
                Some(text) => self.bytes(text),
                None => self.date(days),
            }
        }
    }

    /// Days since 1970-01-01 as `YYYY-MM-DD` (Hinnant's civil_from_days).
    #[inline]
    unsafe fn date(&mut self, days: i32) {
        let z = days as i64 + 719_468;
        let era = z.div_euclid(146_097);
        let doe = z.rem_euclid(146_097);
        let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
        let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        let mp = (5 * doy + 2) / 153;
        let d = (doy - (153 * mp + 2) / 5 + 1) as usize;
        let m = if mp < 10 { mp + 3 } else { mp - 9 } as usize;
        let y = yoe + era * 400 + (m <= 2) as i64;
        unsafe {
            if (0..10_000).contains(&y) {
                let y = y as usize;
                self.pair(y / 100);
                self.pair(y % 100);
            } else {
                // outside dsdgen's range (Date32 spans about +-5.8M years):
                // the year in full, at most `-5877641`
                self.int(y);
            }
            self.byte(b'-');
            self.pair(m);
            self.byte(b'-');
            self.pair(d);
        }
    }

    /// String `r` of a view array. Strings of up to 12 bytes live in the
    /// view itself: copy all 12 and advance by the length (the reservation
    /// has 12 bytes of slack per string for this).
    #[inline]
    unsafe fn view(&mut self, a: &StringViewArray, r: usize, encoding: Encoding) {
        // SAFETY: the caller passes a row of `a`.
        let v = unsafe { *a.views().get_unchecked(r) };
        let len = v as u32 as usize;
        unsafe {
            if len <= 12 {
                let inline = (v >> 32).to_le_bytes();
                if encoding == Encoding::Utf8 || inline.is_ascii() {
                    std::ptr::copy_nonoverlapping(inline.as_ptr(), self.0, 12);
                    self.0 = self.0.add(len);
                    return;
                }
            }
            self.text(a.value(r), encoding)
        }
    }

    /// The first `len <= 12` of 12 bytes: one 12-byte store (the
    /// reservation has 12 bytes of slack per string for this).
    ///
    /// # Safety
    /// The cursor has room for what this writes ([`Cursor`]).
    #[inline(always)]
    pub unsafe fn short(&mut self, b: &[u8; 12], len: usize) {
        unsafe {
            std::ptr::copy_nonoverlapping(b.as_ptr(), self.0, 12);
            self.0 = self.0.add(len);
        }
    }

    /// `s` in `encoding` (Latin-1 is never longer than UTF-8).
    ///
    /// # Safety
    /// The cursor has room for what this writes ([`Cursor`]).
    #[inline]
    pub unsafe fn text(&mut self, s: &str, encoding: Encoding) {
        unsafe {
            if encoding == Encoding::Utf8 || s.is_ascii() {
                self.bytes(s.as_bytes());
            } else {
                // every TPC-DS character is in Latin-1
                for c in s.chars() {
                    self.byte(u8::try_from(c).unwrap_or(b'?'));
                }
            }
        }
    }
}

/// A source of text: chunk `i` of each of its outputs, formatted
/// independently of the others (populate-gen-tpcds generates a sales table and
/// its returns as one source with two outputs).
pub trait TextChunks: Sync {
    /// Number of outputs (files).
    fn outputs(&self) -> usize;

    /// Number of chunks.
    fn num_chunks(&self) -> u64;

    /// Append chunk `i`'s text of output `k` to `bufs[k]` (cleared, one per
    /// output).
    fn chunk_text(&self, i: u64, bufs: &mut [Vec<u8>]);
}

/// Write every source's chunks, in order, to its writers (one per output,
/// sources in order, each source's outputs in order), on one pool of
/// `threads` threads (0 = all cores). Every chunk of every source is a job,
/// biggest remaining work first ([`Schedule`]).
///
/// The workers write the files themselves: a finished chunk is queued on its
/// file, and whichever worker finds the file idle writes everything queued
/// in order, so different files are written in parallel and no thread only
/// writes. Past `2 * threads` queued chunks a worker waits for the writers
/// instead of formatting more.
pub fn write_text_many<W: Write + Send>(
    srcs: &[&dyn TextChunks],
    writers: Vec<W>,
    threads: usize,
) -> io::Result<Vec<W>> {
    let mut first = Vec::with_capacity(srcs.len());
    let mut outputs = 0;
    for s in srcs {
        first.push(outputs);
        outputs += s.outputs();
    }
    assert_eq!(outputs, writers.len(), "one writer per output");
    let threads = resolve_threads(threads);
    let chunks: Vec<u64> = srcs.iter().map(|s| s.num_chunks()).collect();
    let jobs: u64 = chunks.iter().sum();
    // biggest remaining work first, as in [`par_sources`]
    let schedule = Mutex::new(Schedule::new(&chunks));
    let files: Vec<Mutex<TextFile<W>>> = writers
        .into_iter()
        .map(|w| Mutex::new(TextFile::new(w)))
        .collect();
    // Text buffers go back to a pool once written: a fresh multi-MB buffer
    // per chunk costs more in page faults than formatting it.
    let pool = Mutex::new(Vec::<Vec<u8>>::new());
    let queued = (Mutex::new(0usize), Condvar::new());
    let failed = AtomicBool::new(false);
    let work = || {
        while !failed.load(Relaxed) {
            let Some((s, i)) = schedule.lock().expect("schedule").pick() else {
                break;
            };
            let t0 = Instant::now();
            let src = srcs[s];
            let mut bufs: Vec<Vec<u8>> = (0..src.outputs())
                .map(|_| pool.lock().expect("pool").pop().unwrap_or_default())
                .collect();
            src.chunk_text(i, &mut bufs);
            let secs = t0.elapsed().as_secs_f64();
            schedule.lock().expect("schedule").finished(s, secs);
            for (k, buf) in bufs.into_iter().enumerate() {
                *queued.0.lock().expect("queued") += 1;
                let written = TextFile::push(&files[first[s] + k], i, buf, |b| {
                    pool.lock().expect("pool").push(b);
                    *queued.0.lock().expect("queued") -= 1;
                    queued.1.notify_all();
                });
                if written.is_err() {
                    failed.store(true, Relaxed);
                    queued.1.notify_all();
                }
            }
            let mut q = queued.0.lock().expect("queued");
            while *q > 2 * threads && !failed.load(Relaxed) {
                q = queued.1.wait(q).expect("queued");
            }
        }
    };
    if threads <= 1 {
        work();
    } else {
        thread::scope(|s| {
            for _ in 0..threads.min(jobs as usize) {
                s.spawn(work);
            }
        });
    }
    let mut out = Vec::with_capacity(files.len());
    for f in files {
        let f = f.into_inner().expect("file");
        if let Some(e) = f.err {
            return Err(e);
        }
        let mut w = f.w.expect("writer");
        w.flush()?;
        out.push(w);
    }
    Ok(out)
}

/// One text output: its writer, the chunks queued for it, and whether a
/// worker is writing them.
struct TextFile<W> {
    /// `None` while a worker writes to it.
    w: Option<W>,
    next: u64,
    queue: BTreeMap<u64, Vec<u8>>,
    writing: bool,
    err: Option<io::Error>,
}

impl<W: Write> TextFile<W> {
    fn new(w: W) -> Self {
        Self {
            w: Some(w),
            next: 0,
            queue: BTreeMap::new(),
            writing: false,
            err: None,
        }
    }

    /// Queue chunk `i` of `file`; if no one is writing it, write every
    /// chunk that is next in order, without holding the lock while writing.
    /// `done` gets each written buffer back.
    fn push(
        file: &Mutex<Self>,
        i: u64,
        buf: Vec<u8>,
        mut done: impl FnMut(Vec<u8>),
    ) -> io::Result<()> {
        let mut f = file.lock().expect("file");
        f.queue.insert(i, buf);
        if f.writing {
            return Ok(());
        }
        f.writing = true;
        loop {
            let at = f.next;
            let Some(mut b) = f.queue.remove(&at) else {
                f.writing = false;
                return Ok(());
            };
            f.next += 1;
            let mut w = f.w.take().expect("idle writer");
            drop(f);
            let r = w.write_all(&b);
            b.clear();
            done(b);
            f = file.lock().expect("file");
            f.w = Some(w);
            if let Err(e) = r {
                f.err.get_or_insert(e);
                f.writing = false;
                return Err(io::Error::other("write failed"));
            }
        }
    }
}

/// A column that prints its values as text fields, for [`write_columns`]:
/// the text sink for populate's columns, which writes a row's fields
/// straight from the columns, without making Arrow arrays.
pub trait TextField {
    /// Rows in the column.
    fn text_rows(&self) -> usize;

    /// Most bytes all its fields take: for every row the widest number,
    /// or a string's bytes and 12 bytes of slack ([`Cursor::short`]).
    fn text_bytes(&self) -> usize;

    /// Write row `r`'s field, without its separator.
    ///
    /// # Safety
    /// `r < self.text_rows()`, and `w` has room for the field (as
    /// [`text_bytes`](Self::text_bytes) counts it).
    unsafe fn write_field(&self, r: usize, w: &mut Cursor, encoding: Encoding);
}

/// Columns that print a row as fields, each followed by `|`: a
/// [`TextField`], a tuple or [`SpreadCols`] of them, or a child table's
/// [`ChildCols`].
pub trait TextColumns {
    fn text_rows(&self) -> usize;

    /// Most bytes all its rows take, separators included.
    fn text_bytes(&self) -> usize;

    /// Write row `r`'s fields, each followed by `|`.
    ///
    /// # Safety
    /// As [`TextField::write_field`].
    unsafe fn write_row(&self, r: usize, w: &mut Cursor, encoding: Encoding);

    /// Fields in a row.
    const WIDTH: usize;

    /// As [`write_row`](Self::write_row), but field `j` (counted from
    /// `first`) is left empty, NULL, where `null(j)`: for columns whose
    /// NULLs are kept once per row rather than per column.
    ///
    /// # Safety
    /// As [`TextField::write_field`].
    unsafe fn write_row_masked(
        &self,
        r: usize,
        w: &mut Cursor,
        encoding: Encoding,
        first: usize,
        null: &impl Fn(usize) -> bool,
    );
}

impl<T: TextField> TextColumns for T {
    const WIDTH: usize = 1;

    #[inline(always)]
    unsafe fn write_row_masked(
        &self,
        r: usize,
        w: &mut Cursor,
        encoding: Encoding,
        first: usize,
        null: &impl Fn(usize) -> bool,
    ) {
        unsafe {
            if !null(first) {
                self.write_field(r, w, encoding);
            }
            w.byte(b'|');
        }
    }

    #[inline]
    fn text_rows(&self) -> usize {
        TextField::text_rows(self)
    }

    fn text_bytes(&self) -> usize {
        TextField::text_bytes(self) + TextField::text_rows(self)
    }

    #[inline(always)]
    unsafe fn write_row(&self, r: usize, w: &mut Cursor, encoding: Encoding) {
        unsafe {
            self.write_field(r, w, encoding);
            w.byte(b'|');
        }
    }
}

/// Append every row of `cols` to `out` as text lines: the fields, each
/// followed by `|`, then a newline. NULL is an empty field, decimals have
/// exactly their scale's digits, dates are `YYYY-MM-DD`.
pub fn write_columns<C: TextColumns>(cols: &C, encoding: Encoding, out: &mut Vec<u8>) {
    let rows = cols.text_rows();
    let bound = cols.text_bytes() + rows;
    out.reserve(bound);
    let start = out.len();
    let mut w = Cursor(out.spare_capacity_mut().as_mut_ptr().cast::<u8>());
    let base = w.0;
    for r in 0..rows {
        // SAFETY: `r` is a row, and `bound` has room for every field,
        // separator and newline.
        unsafe {
            cols.write_row(r, &mut w, encoding);
            w.byte(b'\n');
        }
    }
    // SAFETY: every byte in `start..start + written` was initialised above.
    unsafe {
        let written = w.0.offset_from(base) as usize;
        debug_assert!(written <= bound);
        out.set_len(start + written);
    }
}

macro_rules! int_fields {
    ($($t:ty),*) => {$(
        impl TextField for Vec<$t> {
            fn text_rows(&self) -> usize {
                self.len()
            }

            fn text_bytes(&self) -> usize {
                self.len() * MAX_NUMBER
            }

            #[inline(always)]
            unsafe fn write_field(&self, r: usize, w: &mut Cursor, _: Encoding) {
                unsafe { w.int(*self.get_unchecked(r) as i64) }
            }
        }
    )*};
}

int_fields!(i8, i16, i32, i64, u8, u16, u32);

impl<const P: u8, const S: i8> TextField for Vec<Decimal<P, S>> {
    fn text_rows(&self) -> usize {
        self.len()
    }

    fn text_bytes(&self) -> usize {
        self.len() * MAX_DECIMAL
    }

    #[inline(always)]
    unsafe fn write_field(&self, r: usize, w: &mut Cursor, _: Encoding) {
        let v = unsafe { self.get_unchecked(r).0 };
        // precision 18 fits an i64; both branches are constant
        unsafe {
            match (P <= 18, S) {
                (true, 2) => w.decimal2(v as i64),
                (true, s) => w.decimal(v as i64, s.max(0) as u32),
                (false, s) => w.decimal128(v, s.max(0) as u32, s.max(0) as u32),
            }
        }
    }
}

impl TextField for Vec<Date32> {
    fn text_rows(&self) -> usize {
        self.len()
    }

    fn text_bytes(&self) -> usize {
        self.len() * MAX_NUMBER
    }

    #[inline(always)]
    unsafe fn write_field(&self, r: usize, w: &mut Cursor, _: Encoding) {
        unsafe { w.day(self.get_unchecked(r).0) }
    }
}

impl TextField for StrCol {
    fn text_rows(&self) -> usize {
        self.len()
    }

    fn text_bytes(&self) -> usize {
        self.data().len()
    }

    #[inline(always)]
    unsafe fn write_field(&self, r: usize, w: &mut Cursor, encoding: Encoding) {
        // SAFETY: `r` is a row, so `r + 1` is an offset; offsets are in
        // bounds and on character boundaries.
        unsafe {
            let o = self.offsets();
            let (a, b) = (
                *o.get_unchecked(r) as usize,
                *o.get_unchecked(r + 1) as usize,
            );
            w.text(
                std::str::from_utf8_unchecked(self.data().get_unchecked(a..b)),
                encoding,
            )
        }
    }
}

impl<C: TextField> TextField for Nullable<C> {
    fn text_rows(&self) -> usize {
        self.values.text_rows()
    }

    fn text_bytes(&self) -> usize {
        self.values.text_bytes()
    }

    /// Nothing for a NULL.
    #[inline(always)]
    unsafe fn write_field(&self, r: usize, w: &mut Cursor, encoding: Encoding) {
        // SAFETY: `r` is a row, so its bit is in the words.
        let valid = unsafe { *self.valid.words().get_unchecked(r / 64) >> (r % 64) & 1 != 0 };
        if valid {
            unsafe { self.values.write_field(r, w, encoding) }
        }
    }
}

impl<C: TextField, const N: usize> TextColumns for SpreadCols<C, N> {
    fn text_rows(&self) -> usize {
        self.0.first().map_or(0, |c| c.text_rows())
    }

    fn text_bytes(&self) -> usize {
        self.0.iter().map(TextColumns::text_bytes).sum()
    }

    #[inline(always)]
    unsafe fn write_row(&self, r: usize, w: &mut Cursor, encoding: Encoding) {
        for c in &self.0 {
            unsafe { c.write_row(r, w, encoding) }
        }
    }

    const WIDTH: usize = N;

    #[inline(always)]
    unsafe fn write_row_masked(
        &self,
        r: usize,
        w: &mut Cursor,
        encoding: Encoding,
        first: usize,
        null: &impl Fn(usize) -> bool,
    ) {
        for (i, c) in self.0.iter().enumerate() {
            unsafe { c.write_row_masked(r, w, encoding, first + i, null) }
        }
    }
}

macro_rules! tuple_text {
    ($($t:ident $i:tt),+) => {
        impl<$($t: TextColumns),+> TextColumns for ($($t,)+) {
            fn text_rows(&self) -> usize {
                self.0.text_rows()
            }

            fn text_bytes(&self) -> usize {
                0 $(+ self.$i.text_bytes())+
            }

            #[inline(always)]
            unsafe fn write_row(&self, r: usize, w: &mut Cursor, encoding: Encoding) {
                unsafe { $(self.$i.write_row(r, w, encoding);)+ }
            }

            const WIDTH: usize = 0 $(+ $t::WIDTH)+;

            #[inline(always)]
            unsafe fn write_row_masked(
                &self,
                r: usize,
                w: &mut Cursor,
                encoding: Encoding,
                first: usize,
                null: &impl Fn(usize) -> bool,
            ) {
                let mut at = first;
                $(
                    unsafe { self.$i.write_row_masked(r, w, encoding, at, null) };
                    at += $t::WIDTH;
                )+
                let _ = at;
            }
        }
    };
}

tuple_text!(A 0);
tuple_text!(A 0, B 1);
tuple_text!(A 0, B 1, C 2);
tuple_text!(A 0, B 1, C 2, D 3);
tuple_text!(A 0, B 1, C 2, D 3, E 4);
tuple_text!(A 0, B 1, C 2, D 3, E 4, F 5);
tuple_text!(A 0, B 1, C 2, D 3, E 4, F 5, G 6);
tuple_text!(A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7);
tuple_text!(A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8);
tuple_text!(A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8, J 9);
tuple_text!(A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8, J 9, K 10);
tuple_text!(A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8, J 9, K 10, L 11);

/// The columns of several tables (a [`TableCols`] of a tuple of
/// [`ChildCols`], as `flat_map` writes them), each table printed to its own
/// buffer.
pub trait TextTables {
    /// Append table `k`'s rows to `bufs[k]`, as [`write_columns`].
    fn write_tables(&self, encoding: Encoding, bufs: &mut [Vec<u8>]);
}

/// A child table's rows: the children's columns.
impl<V: TextColumns> TextColumns for ChildCols<V> {
    const WIDTH: usize = V::WIDTH;

    fn text_rows(&self) -> usize {
        self.values.text_rows()
    }

    fn text_bytes(&self) -> usize {
        self.values.text_bytes()
    }

    #[inline(always)]
    unsafe fn write_row(&self, r: usize, w: &mut Cursor, encoding: Encoding) {
        // SAFETY: as the caller's
        unsafe { self.values.write_row(r, w, encoding) }
    }

    #[inline(always)]
    unsafe fn write_row_masked(
        &self,
        r: usize,
        w: &mut Cursor,
        encoding: Encoding,
        first: usize,
        null: &impl Fn(usize) -> bool,
    ) {
        // SAFETY: as the caller's
        unsafe { self.values.write_row_masked(r, w, encoding, first, null) }
    }
}

impl<V: TextColumns> TextTables for ChildCols<V> {
    fn write_tables(&self, encoding: Encoding, bufs: &mut [Vec<u8>]) {
        write_columns(&self.values, encoding, &mut bufs[0]);
    }
}

impl<C: TextTables> TextTables for TableCols<C> {
    fn write_tables(&self, encoding: Encoding, bufs: &mut [Vec<u8>]) {
        self.tables.write_tables(encoding, bufs);
    }
}

macro_rules! tuple_tables {
    ($($t:ident $i:tt),+) => {
        impl<$($t: TextTables),+> TextTables for ($($t,)+) {
            fn write_tables(&self, encoding: Encoding, bufs: &mut [Vec<u8>]) {
                let mut at = 0;
                $(
                    self.$i.write_tables(encoding, &mut bufs[at..]);
                    at += 1;
                )+
                let _ = at;
            }
        }
    };
}

tuple_tables!(A 0);
tuple_tables!(A 0, B 1);
tuple_tables!(A 0, B 1, C 2);
tuple_tables!(A 0, B 1, C 2, D 3);

#[cfg(test)]
mod tests {
    use super::*;

    fn fmt(f: impl Fn(&mut Cursor)) -> String {
        let mut v = vec![0u8; 64];
        let mut c = Cursor(v.as_mut_ptr());
        f(&mut c);
        let n = unsafe { c.0.offset_from(v.as_ptr()) } as usize;
        v.truncate(n);
        String::from_utf8(v).unwrap()
    }

    #[test]
    fn eight_digits() {
        for x in (0..100_000_000u32)
            .step_by(9_973)
            .chain([0, 9, 10, 99_999_999, 12_345_678])
        {
            assert_eq!(eight(x).to_le_bytes(), *format!("{x:08}").as_bytes(), "{x}");
        }
        for x in 0..10_000u32 {
            assert_eq!(four(x).to_le_bytes(), *format!("{x:04}").as_bytes(), "{x}");
        }
        for v in [
            0u64,
            7,
            9,
            10,
            11,
            99,
            100,
            101,
            1_234,
            9_999,
            10_000,
            99_999_999,
            100_000_000,
            123_456_789_012,
            u64::MAX,
        ] {
            assert_eq!(fmt(|o| unsafe { o.uint(v) }), v.to_string());
        }
    }

    #[test]
    fn numbers_and_dates() {
        unsafe {
            assert_eq!(fmt(|o| o.int(0)), "0");
            assert_eq!(fmt(|o| o.int(-12345)), "-12345");
            assert_eq!(fmt(|o| o.int(i64::MIN)), i64::MIN.to_string());
            assert_eq!(fmt(|o| o.decimal(-5, 2)), "-0.05");
            assert_eq!(fmt(|o| o.decimal(0, 2)), "0.00");
            assert_eq!(fmt(|o| o.decimal(123456, 2)), "1234.56");
            assert_eq!(fmt(|o| o.decimal(-77973, 2)), "-779.73");
            assert_eq!(fmt(|o| o.decimal(-77973, 3)), "-77.973");
            assert_eq!(fmt(|o| o.decimal(-500, 0)), "-500");
            let edges = [
                -100_001,
                -77973,
                -5,
                0,
                7,
                99,
                100,
                9_999,
                10_000,
                99_999_999,
                100_000_000,
            ];
            for v in (-1_000_000..1_000_000)
                .step_by(997)
                .chain(edges)
                .chain([123_456_789])
            {
                assert_eq!(fmt(|o| o.decimal2(v)), fmt(|o| o.decimal(v, 2)), "{v}");
            }
            assert_eq!(fmt(|o| o.date(0)), "1970-01-01");
            assert_eq!(fmt(|o| o.date(-25567)), "1900-01-01");
            assert_eq!(fmt(|o| o.date(47482)), "2100-01-01");
            assert_eq!(fmt(|o| o.date(11016)), "2000-02-29");
            // outside years 0..=9999: the year in full, no table overrun
            assert_eq!(fmt(|o| o.date(2_932_897)), "10000-01-01");
            assert_eq!(fmt(|o| o.date(-719_529)), "-1-12-31");
            assert_eq!(fmt(|o| o.date(i32::MIN)), "-5877641-06-23");
            assert_eq!(fmt(|o| o.date(i32::MAX)), "5881580-07-11");
            for (v, s, p, want) in [
                (-5i128, 2, 2, "-0.05"),
                (123_456, 3, 3, "123.456"),
                (-1_234_567, 2, 0, "-12345"),
                (-99, 2, 0, "0"),
                (99, 2, 0, "0"),
                (100, 2, 0, "1"),
                (i64::MIN as i128, 2, 0, "-92233720368547758"),
                (i64::MAX as i128, 2, 0, "92233720368547758"),
                (i64::MAX as i128 + 1, 2, 0, "92233720368547758"),
                (
                    i128::from(i64::MAX) * 1000 + 7,
                    2,
                    2,
                    "92233720368547758070.07",
                ),
                (
                    -(10i128.pow(37)) - 3,
                    20,
                    20,
                    "-100000000000000000.00000000000000000003",
                ),
                (
                    i128::MAX,
                    38,
                    38,
                    "1.70141183460469231731687303715884105727",
                ),
                (i128::MIN, 0, 0, "-170141183460469231731687303715884105728"),
            ] {
                assert_eq!(fmt(|o| o.decimal128(v, s, p)), want, "{v} {s} {p}");
            }
        }
    }

    #[test]
    fn wide_values_through_write_batch() {
        use arrow_array::{Date32Array, Decimal128Array};
        use arrow_schema::{Field, Schema};
        use std::sync::Arc;
        let schema = Arc::new(Schema::new(vec![
            Field::new("d", DataType::Date32, true),
            Field::new("x", DataType::Decimal128(38, 4), true),
        ]));
        let days = Date32Array::from(vec![Some(i32::MIN), Some(0), None, Some(i32::MAX)]);
        let x = Decimal128Array::from(vec![Some(i128::MIN), Some(-1), Some(i128::MAX), None])
            .with_precision_and_scale(38, 4)
            .unwrap();
        let b = RecordBatch::try_new(schema, vec![Arc::new(days), Arc::new(x)]).unwrap();
        let mut out = Vec::new();
        write_batch(&b, Encoding::Utf8, &mut out);
        assert_eq!(
            String::from_utf8(out).unwrap(),
            "-5877641-06-23|-17014118346046923173168730371588410.5728|\n\
             1970-01-01|-0.0001|\n\
             |17014118346046923173168730371588410.5727|\n\
             5881580-07-11||\n"
        );
    }
}
