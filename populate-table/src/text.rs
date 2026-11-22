//! The TPC text formats (dsdgen's `.dat`, dbgen's `.tbl`), written straight
//! from Arrow batches, and a parallel writer for them.
//!
//! One line per row: every field followed by `|`, NULL as an empty field,
//! decimals with exactly `scale` digits (`-0.05`), dates as `YYYY-MM-DD`.
//! This is ~10x faster than formatting rows with `Display`; populate-tpcds
//! and populate-tpch test it against their reference generators' `Display`
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
use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::{Condvar, Mutex};
use std::thread;

use crate::chunks::Jobs;

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
    for r in 0..rows {
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
                        Col::Date(v) => w.date(*v.get_unchecked(r)),
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

/// `"00000000"` as a `u64`.
const ZEROS: u64 = 0x3030_3030_3030_3030;

/// Write cursor into reserved capacity. Every method is `unsafe`: the
/// caller guarantees room for what it writes.
struct Cursor(*mut u8);

impl Cursor {
    #[inline(always)]
    unsafe fn byte(&mut self, b: u8) {
        unsafe {
            self.0.write(b);
            self.0 = self.0.add(1);
        }
    }

    #[inline(always)]
    unsafe fn bytes(&mut self, b: &[u8]) {
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

    /// Decimal digits of `v`. Up to 8 digits are made at once in one
    /// `u64` ([`eight`]) and stored as 8 bytes, of which all but the leading
    /// zeros are kept (the reservation has room for 8 per number).
    #[inline]
    unsafe fn uint(&mut self, v: u64) {
        unsafe {
            if v < 100_000_000 {
                // leading zero digits are the low zero bytes (at most 7)
                let d = eight(v as u32);
                let zeros = ((d - ZEROS) | (1 << 56)).trailing_zeros() / 8;
                self.0
                    .cast::<[u8; 8]>()
                    .write_unaligned((d >> (8 * zeros)).to_le_bytes());
                self.0 = self.0.add(8 - zeros as usize);
            } else {
                self.big(v);
            }
        }
    }

    /// [`Self::uint`] of `v >= 10^8`: the high digits, then the low 8.
    #[cold]
    #[inline(never)]
    unsafe fn big(&mut self, v: u64) {
        unsafe {
            self.uint(v / 100_000_000);
            let d = eight((v % 100_000_000) as u32);
            self.0.cast::<[u8; 8]>().write_unaligned(d.to_le_bytes());
            self.0 = self.0.add(8);
        }
    }

    #[inline]
    unsafe fn int(&mut self, v: i64) {
        unsafe {
            if v < 0 {
                self.byte(b'-');
            }
            self.uint(v.unsigned_abs());
        }
    }

    /// `%.{scale}f` of `v / 10^scale` (what dsdgen's `print_decimal`
    /// prints; `-0.05` keeps its sign).
    unsafe fn decimal(&mut self, v: i64, scale: u32) {
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
                Ok(v) if scale <= 18 => self.decimal(v / 10i64.pow(scale - print), print),
                _ => self.wide_decimal(v / 10i128.pow(scale - print), print),
            }
        }
    }

    /// [`Self::decimal`] for any `i128` (up to 39 digits).
    #[cold]
    #[inline(never)]
    unsafe fn wide_decimal(&mut self, v: i128, scale: u32) {
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
                self.byte(b'-');
            }
            self.bytes(text.as_bytes());
        }
    }

    /// [`Self::decimal`] at scale 2, with constant divisors.
    #[inline]
    unsafe fn decimal2(&mut self, v: i64) {
        unsafe {
            if v < 0 {
                self.byte(b'-');
            }
            let a = v.unsigned_abs();
            self.uint(a / 100);
            self.byte(b'.');
            self.pair((a % 100) as usize);
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

    #[inline]
    unsafe fn text(&mut self, s: &str, encoding: Encoding) {
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
/// independently of the others (populate-tpcds generates a sales table and
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
/// `threads` threads (0 = all cores). Every chunk of every source is a job.
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
    let threads = match threads {
        0 => thread::available_parallelism().map_or(1, |t| t.get()),
        t => t,
    };
    let jobs = Jobs::new(srcs.iter().map(|s| s.num_chunks()));
    let files: Vec<Mutex<TextFile<W>>> = writers
        .into_iter()
        .map(|w| Mutex::new(TextFile::new(w)))
        .collect();
    // Text buffers go back to a pool once written: a fresh multi-MB buffer
    // per chunk costs more in page faults than formatting it.
    let pool = Mutex::new(Vec::<Vec<u8>>::new());
    let queued = (Mutex::new(0usize), Condvar::new());
    let next = AtomicU64::new(0);
    let failed = AtomicBool::new(false);
    let work = || {
        while !failed.load(Relaxed) {
            let j = next.fetch_add(1, Relaxed);
            if j >= jobs.total() {
                break;
            }
            let (s, i) = jobs.locate(j);
            let src = srcs[s];
            let mut bufs: Vec<Vec<u8>> = (0..src.outputs())
                .map(|_| pool.lock().expect("pool").pop().unwrap_or_default())
                .collect();
            src.chunk_text(i, &mut bufs);
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
            for _ in 0..threads.min(jobs.total() as usize) {
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
        for v in [
            0u64,
            7,
            10,
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
            for v in [-100_001, -77973, -5, 0, 7, 99, 100, 123_456_789] {
                assert_eq!(fmt(|o| o.decimal2(v)), fmt(|o| o.decimal(v, 2)));
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
