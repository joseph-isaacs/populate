//! Per-row sampling kernels shared by the shapes.
//!
//! Every value is a pure function of `(stream, row)`: a [`Stream`] hashes
//! the row to 64 random bits and a kernel here turns the bits into a value
//! with no state, so any slice of rows can be generated on its own and
//! chunked output equals one sequential run, for any chunk or batch size.
//!
//! Continuous distributions are sampled by inverse CDF from a table of
//! quantiles ([`Quantiles`]): one hash, one lookup and one lerp per value,
//! with no `exp`/`ln` in the hot loop.

#[cfg(feature = "formats")]
use arrow_array::{ArrayRef, StringViewArray};
#[cfg(feature = "formats")]
use arrow_buffer::{Buffer, ScalarBuffer};
#[cfg(feature = "formats")]
use arrow_schema::DataType;
use populate_core::RowCount;
use populate_core::Stream;
use populate_core::{Columnar, Distribution, Nullable, Sink, Validity};
#[cfg(feature = "formats")]
use populate_sink_arrow::IntoArrowArray;
use rand::Rng;
use std::ops::Range;
use std::str::from_utf8;
use std::sync::Arc;
#[cfg(not(feature = "formats"))]
type Buffer = Arc<[u8]>;

fn buffer(bytes: Vec<u8>) -> Buffer {
    #[cfg(feature = "formats")]
    {
        Buffer::from_vec(bytes)
    }
    #[cfg(not(feature = "formats"))]
    {
        bytes.into()
    }
}

/// `[0, 1)` from the top 53 bits.
#[inline(always)]
pub fn unit(bits: u64) -> f64 {
    (bits >> 11) as f64 * (1.0 / (1u64 << 53) as f64)
}

/// Inverse of the standard normal CDF (Acklam's rational approximation,
/// relative error below 1.2e-9). Construction-time only.
pub fn normal_icdf(p: f64) -> f64 {
    const A: [f64; 6] = [
        -3.969_683_028_665_376e1,
        2.209_460_984_245_205e2,
        -2.759_285_104_469_687e2,
        1.383_577_518_672_69e2,
        -3.066_479_806_614_716e1,
        2.506_628_277_459_239,
    ];
    const B: [f64; 5] = [
        -5.447_609_879_822_406e1,
        1.615_858_368_580_409e2,
        -1.556_989_798_598_866e2,
        6.680_131_188_771_972e1,
        -1.328_068_155_288_572e1,
    ];
    const C: [f64; 6] = [
        -7.784_894_002_430_293e-3,
        -3.223_964_580_411_365e-1,
        -2.400_758_277_161_838,
        -2.549_732_539_343_734,
        4.374_664_141_464_968,
        2.938_163_982_698_783,
    ];
    const D: [f64; 4] = [
        7.784_695_709_041_462e-3,
        3.224_671_290_700_398e-1,
        2.445_134_137_142_996,
        3.754_408_661_907_416,
    ];
    let tail = |q: f64| {
        (((((C[0] * q + C[1]) * q + C[2]) * q + C[3]) * q + C[4]) * q + C[5])
            / ((((D[0] * q + D[1]) * q + D[2]) * q + D[3]) * q + 1.0)
    };
    if p < 0.02425 {
        tail((-2.0 * p.ln()).sqrt())
    } else if p > 1.0 - 0.02425 {
        -tail((-2.0 * (1.0 - p).ln()).sqrt())
    } else {
        let q = p - 0.5;
        let r = q * q;
        (((((A[0] * r + A[1]) * r + A[2]) * r + A[3]) * r + A[4]) * r + A[5]) * q
            / (((((B[0] * r + B[1]) * r + B[2]) * r + B[3]) * r + B[4]) * r + 1.0)
    }
}

/// Bits of the quantile table index.
const QBITS: u32 = 12;
const QN: usize = 1 << QBITS;

/// A continuous distribution as `2^12 + 1` quantiles, sampled by
/// interpolating between neighbours: the top 12 bits pick the interval,
/// the next 40 the position in it.
#[derive(Debug, Clone)]
pub struct Quantiles {
    /// `(q[i], q[i + 1] - q[i])`, so a sample is one load and one FMA.
    knots: Box<[(f64, f64); QN]>,
}

impl Quantiles {
    /// From an inverse CDF on `(0, 1)`. The outermost quantiles are taken
    /// at `0.25 / 2^12` and `1 - 0.25 / 2^12` so unbounded tails stay
    /// finite.
    pub fn new(icdf: impl Fn(f64) -> f64) -> Self {
        let eps = 0.25 / QN as f64;
        let q: Vec<f64> = (0..=QN)
            .map(|i| icdf((i as f64 / QN as f64).clamp(eps, 1.0 - eps)))
            .collect();
        let mut knots = Box::new([(0.0, 0.0); QN]);
        for (i, k) in knots.iter_mut().enumerate() {
            *k = (q[i], q[i + 1] - q[i]);
        }
        Quantiles { knots }
    }

    /// Normal with mean `mu` and standard deviation `sigma`.
    pub fn normal(mu: f64, sigma: f64) -> Self {
        Self::new(|p| mu + sigma * normal_icdf(p))
    }

    /// Log-normal: `exp(N(mu, sigma))`.
    pub fn lognormal(mu: f64, sigma: f64) -> Self {
        Self::new(|p| (mu + sigma * normal_icdf(p)).exp())
    }

    /// Exponential with mean `mean`.
    pub fn exponential(mean: f64) -> Self {
        Self::new(|p| -mean * (1.0 - p).ln())
    }

    /// Piecewise-uniform over `weights.len()` equal bins of `[lo, hi)`
    /// (e.g. trips per hour of the day).
    pub fn histogram(lo: f64, hi: f64, weights: &[f64]) -> Self {
        let total: f64 = weights.iter().sum();
        let mut cdf = vec![0.0];
        for w in weights {
            cdf.push(cdf.last().unwrap() + w / total);
        }
        let width = (hi - lo) / weights.len() as f64;
        Self::new(|p| {
            let b = cdf.partition_point(|&c| c <= p).clamp(1, weights.len()) - 1;
            let span = cdf[b + 1] - cdf[b];
            let f = if span > 0.0 { (p - cdf[b]) / span } else { 0.0 };
            lo + (b as f64 + f.clamp(0.0, 1.0)) * width
        })
    }

    /// The quantile at `p` in `[0, 1)` (a deterministic transform, e.g. of
    /// a row's position in the day).
    #[inline(always)]
    pub fn at(&self, p: f64) -> f64 {
        let x = p * QN as f64;
        let i = (x as usize).min(QN - 1);
        let (q, d) = self.knots[i];
        q + d * (x - i as f64)
    }

    #[inline(always)]
    pub fn sample(&self, bits: u64) -> f64 {
        let (q, d) = self.knots[(bits >> (64 - QBITS)) as usize];
        let frac = ((bits << QBITS) >> 24) as f64 * (1.0 / (1u64 << 40) as f64);
        q + d * frac
    }
}

/// Standard normal draws, five per 64-bit hash: each 12-bit field of the
/// hash picks one of 4,096 equal-probability quantiles (an L1-resident
/// table). Coarser than [`Quantiles`] but about five times cheaper per
/// value, for bulk noise such as embedding coordinates.
#[derive(Debug, Clone)]
pub struct FastNormal {
    table: Box<[f32; 4096]>,
}

impl Default for FastNormal {
    fn default() -> Self {
        let mut table = Box::new([0.0; 4096]);
        for (i, t) in table.iter_mut().enumerate() {
            *t = normal_icdf((i as f64 + 0.5) / 4096.0) as f32;
        }
        FastNormal { table }
    }
}

impl FastNormal {
    /// Hashes needed for `n` values.
    pub fn hashes(n: usize) -> usize {
        n.div_ceil(5)
    }

    /// Fill `out` from hashes `s.at(first)`, `s.at(first + 1)`, ...
    #[inline]
    pub fn fill(&self, s: Stream, first: u64, out: &mut [f32]) {
        for (k, five) in out.chunks_mut(5).enumerate() {
            let b = s.at(first + k as u64);
            for (l, o) in five.iter_mut().enumerate() {
                *o = self.table[((b >> (12 * l)) & 4095) as usize];
            }
        }
    }
}

/// Zipf-like ranks in `0..n` (rank 0 most frequent) with exponent `s`,
/// from the continuous inverse CDF of `x^-s` on `[1, n + 1)`.
#[derive(Debug, Clone)]
pub struct Zipf {
    q: Quantiles,
    n: u64,
}

impl Zipf {
    pub fn new(n: u64, s: f64) -> Self {
        assert!(n > 0, "zipf over an empty range");
        let m = (n + 1) as f64;
        let q = if (s - 1.0).abs() < 1e-9 {
            Quantiles::new(|p| m.powf(p))
        } else {
            let a = 1.0 - s;
            let top = m.powf(a) - 1.0;
            Quantiles::new(move |p| (top * p + 1.0).powf(1.0 / a))
        };
        Zipf { q, n }
    }

    #[inline(always)]
    pub fn sample(&self, bits: u64) -> u64 {
        ((self.q.sample(bits) - 1.0) as u64).min(self.n - 1)
    }
}

/// A pick from at most a handful of weighted values: the index is the
/// number of cumulative thresholds at or below the draw (branch-free).
#[derive(Debug, Clone)]
pub struct Weighted<T> {
    thresholds: Vec<u64>,
    values: Vec<T>,
}

impl<T: Copy> Weighted<T> {
    pub fn new(items: &[(T, f64)]) -> Self {
        assert!(!items.is_empty(), "weighted pick from no values");
        let total: f64 = items.iter().map(|i| i.1).sum();
        let mut acc = 0.0;
        let mut thresholds = Vec::new();
        for (_, w) in &items[..items.len() - 1] {
            acc += w / total;
            thresholds.push((acc * u64::MAX as f64) as u64);
        }
        Weighted {
            thresholds,
            values: items.iter().map(|i| i.0).collect(),
        }
    }

    #[inline(always)]
    pub fn index(&self, bits: u64) -> usize {
        self.thresholds.iter().map(|&t| (bits >= t) as usize).sum()
    }

    #[inline(always)]
    pub fn sample(&self, bits: u64) -> T {
        self.values[self.index(bits)]
    }
}

/// `true` where the draw is below `p` (`p` in `[0, 1]`).
#[inline(always)]
pub fn below(bits: u64, p: f64) -> bool {
    bits < threshold(p)
}

/// The `u64` threshold for probability `p`.
#[inline(always)]
pub fn threshold(p: f64) -> u64 {
    if p >= 1.0 {
        u64::MAX
    } else {
        (p.max(0.0) * u64::MAX as f64) as u64
    }
}

/// A `Utf8View` string as its 16-byte view: inline (up to 12 bytes) or
/// pointing into a buffer of the [`Views`] column it is written to. Only
/// [`Pool::write`] and [`ViewWriter::write_to`] make long ones, together
/// with the buffers they point into.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct View(pub u128);

/// A `Utf8View` column: views and the buffers their long strings are in.
#[derive(Debug, Clone, Default)]
pub struct Views {
    views: Vec<u128>,
    buffers: Vec<Buffer>,
}

impl Views {
    /// Read a string while retaining shared backing buffers.
    pub fn with_str<R>(&self, i: usize, f: impl FnOnce(&str) -> R) -> R {
        let v = self.views[i];
        let len = v as u32 as usize;
        if len <= 12 {
            let bytes = v.to_le_bytes();
            f(from_utf8(&bytes[4..4 + len]).expect("UTF-8 string"))
        } else {
            let buffer = (v >> 64) as u32 as usize;
            let offset = (v >> 96) as u32 as usize;
            f(from_utf8(&self.buffers[buffer][offset..offset + len]).expect("UTF-8 string"))
        }
    }

    /// Append `views` whose long strings are in `buffers`, renumbering
    /// their buffer indexes past the buffers already here.
    fn append(&mut self, views: impl Iterator<Item = u128>, buffers: &[Buffer]) {
        let first = self.buffers.len() as u128;
        if first == 0 {
            self.views.extend(views);
        } else {
            self.views
                .extend(views.map(|v| if v as u32 > 12 { v + (first << 64) } else { v }));
        }
        self.buffers.extend_from_slice(buffers);
    }
}

impl Columnar for View {
    type Columns = Views;
}

impl Sink<View> for Views {
    fn push(&mut self, v: View) {
        // a long view names a buffer of the column it came from, which
        // this column doesn't have
        assert!(v.0 as u32 <= 12, "a long view without its buffer");
        self.views.push(v.0);
    }

    fn reserve(&mut self, n: usize) {
        self.views.reserve(n);
    }
}

impl RowCount for Views {
    fn rows(&self) -> usize {
        self.views.len()
    }
}

#[cfg(feature = "formats")]
impl IntoArrowArray for Views {
    fn data_type() -> DataType {
        DataType::Utf8View
    }

    fn into_array(self) -> ArrayRef {
        // SAFETY: every view is inline, or a pool's view or a written
        // string's, renumbered to its buffer here (`append`)
        Arc::new(unsafe {
            StringViewArray::new_unchecked(
                ScalarBuffer::from(self.views),
                self.buffers.into(),
                None,
            )
        })
    }
}

/// A fixed set of strings stored once; a column of them is a vector of
/// 16-byte views into the shared buffers, so no string bytes are copied.
#[derive(Debug, Clone)]
pub struct Pool {
    views: Vec<u128>,
    buffers: Arc<[Buffer]>,
}

impl Pool {
    pub fn new<S: AsRef<str>>(values: impl IntoIterator<Item = S>) -> Self {
        #[cfg(feature = "formats")]
        {
            let a = StringViewArray::from_iter_values(values);
            let (views, buffers, _) = a.into_parts();
            Pool {
                views: views.to_vec(),
                buffers,
            }
        }
        #[cfg(not(feature = "formats"))]
        {
            let mut writer = ViewWriter::default();
            for value in values {
                let start = writer.bytes.len();
                writer.bytes.extend_from_slice(value.as_ref().as_bytes());
                writer.finish_value(start);
            }
            let mut out = Views::default();
            writer.write_to(&mut out);
            Pool {
                views: out.views,
                buffers: out.buffers.into(),
            }
        }
    }

    pub fn len(&self) -> usize {
        self.views.len()
    }

    pub fn is_empty(&self) -> bool {
        self.views.is_empty()
    }

    /// The index of the empty string in [`write`](Self::write).
    pub const EMPTY: u32 = u32::MAX;

    /// Append entries `idx` ([`EMPTY`](Self::EMPTY) for `""`) to `out`.
    /// Panics on an index past the end.
    pub fn write(&self, idx: &[u32], out: &mut Views) {
        let views = idx.iter().map(|&i| {
            if i == Self::EMPTY {
                0
            } else {
                self.views[i as usize]
            }
        });
        out.append(views, &self.buffers);
    }
}

/// Writes `Utf8View` strings: bytes go into one buffer, and each row's
/// view is computed as it is written. One writer holds at most 4 GiB of
/// (non-inline) string bytes.
#[derive(Debug, Default)]
pub struct ViewWriter {
    bytes: Vec<u8>,
    views: Vec<u128>,
}

impl ViewWriter {
    pub fn with_capacity(rows: usize, bytes: usize) -> Self {
        ViewWriter {
            bytes: Vec::with_capacity(bytes),
            views: Vec::with_capacity(rows),
        }
    }

    /// The buffer to append the next value to.
    #[inline(always)]
    pub fn buf(&mut self) -> &mut Vec<u8> {
        &mut self.bytes
    }

    /// Finish the value appended since the last call (it starts at `start`,
    /// the buffer length before writing).
    #[inline(always)]
    pub fn finish_value(&mut self, start: usize) {
        let len = self.bytes.len() - start;
        let v = &self.bytes[start..];
        let view = if len <= 12 {
            let mut raw = [0u8; 16];
            raw[..4].copy_from_slice(&(len as u32).to_le_bytes());
            raw[4..4 + len].copy_from_slice(v);
            // inline: the bytes don't stay in the buffer
            self.bytes.truncate(start);
            u128::from_le_bytes(raw)
        } else {
            let prefix = u32::from_le_bytes([v[0], v[1], v[2], v[3]]);
            (len as u128) | ((prefix as u128) << 32) | ((start as u128) << 96)
        };
        self.views.push(view);
    }

    /// Append an empty string.
    #[inline(always)]
    pub fn push_empty(&mut self) {
        self.views.push(0);
    }

    /// Append the strings written to `out`.
    pub fn write_to(self, out: &mut Views) {
        assert!(
            self.bytes.len() <= u32::MAX as usize,
            "string buffer over 4 GiB"
        );
        let buffers = if self.bytes.is_empty() {
            vec![]
        } else {
            vec![buffer(self.bytes)]
        };
        out.append(self.views.into_iter(), &buffers);
    }
}

/// Append `values` to `out`: moved in if `out` is empty (as it is when a
/// block is collected into new columns), copied otherwise.
pub fn put<T>(out: &mut Vec<T>, values: Vec<T>) {
    if out.is_empty() {
        *out = values;
    } else {
        out.extend(values);
    }
}

/// `values` as an `Option` column, present where `present` says.
pub fn put_nullable<T>(out: &mut Nullable<Vec<T>>, values: Vec<T>, present: &Validity) {
    put(&mut out.values, values);
    out.valid.append(present);
}

/// A row number.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct Row(pub u64);

/// Consecutive row numbers, held as the range they are: a block of rows
/// costs no memory, and loops over it count in a register.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Rows(pub Range<u64>);

impl Rows {
    pub fn len(&self) -> usize {
        (self.0.end - self.0.start) as usize
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl Columnar for Row {
    type Columns = Rows;
}

impl Sink<Row> for Rows {
    fn push(&mut self, Row(r): Row) {
        if self.0.is_empty() {
            self.0 = r..r + 1;
        } else {
            assert_eq!(r, self.0.end, "row numbers are consecutive");
            self.0.end += 1;
        }
    }
}

impl RowCount for Rows {
    fn rows(&self) -> usize {
        self.len()
    }
}

/// The row numbers, for writers that work a block at a time.
#[derive(Debug, Clone, Default)]
pub struct RowNumbers {
    next: u64,
}

pub fn row_numbers() -> RowNumbers {
    RowNumbers::default()
}

impl Distribution for RowNumbers {
    type Item = Row;

    fn collect_into(&mut self, cols: &mut Rows, _: &mut impl Rng, n: usize) {
        let end = self.next + n as u64;
        if cols.0.is_empty() {
            cols.0 = self.next..end;
        } else {
            assert_eq!(cols.0.end, self.next, "row numbers are consecutive");
            cols.0.end = end;
        }
        self.next = end;
    }

    fn skip(&mut self, n: u64) {
        self.next += n;
    }
}

/// Round to cents (non-negative inputs; `+ 0.5` then truncate, which
/// vectorises where `round` does not).
#[inline(always)]
pub fn cents(x: f64) -> f64 {
    ((x * 100.0 + 0.5) as i64) as f64 * 0.01
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normal_quantiles_have_the_right_moments() {
        let q = Quantiles::normal(3.0, 2.0);
        let s = Stream::new(1, "n");
        let n = 200_000;
        let v: Vec<f64> = (0..n).map(|r| q.sample(s.at(r))).collect();
        let mean = v.iter().sum::<f64>() / n as f64;
        let var = v.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / n as f64;
        assert!((mean - 3.0).abs() < 0.02, "{mean}");
        assert!((var.sqrt() - 2.0).abs() < 0.02, "{}", var.sqrt());
    }

    #[test]
    fn fast_normal_has_the_right_moments() {
        let f = FastNormal::default();
        let mut v = vec![0.0f32; 500_000];
        f.fill(Stream::new(4, "f"), 0, &mut v);
        let n = v.len() as f64;
        let mean = v.iter().map(|&x| x as f64).sum::<f64>() / n;
        let var = v.iter().map(|&x| (x as f64 - mean).powi(2)).sum::<f64>() / n;
        assert!(mean.abs() < 0.01, "{mean}");
        assert!((var - 1.0).abs() < 0.01, "{var}");
    }

    #[test]
    fn zipf_is_skewed_and_in_range() {
        let z = Zipf::new(1000, 1.1);
        let s = Stream::new(2, "z");
        let mut counts = vec![0u32; 1000];
        for r in 0..100_000 {
            counts[z.sample(s.at(r)) as usize] += 1;
        }
        assert!(counts[0] > counts[9] && counts[9] > counts[99] && counts[99] > counts[999]);
    }

    #[test]
    fn weighted_matches_weights() {
        let w = Weighted::new(&[(1, 0.7), (2, 0.2), (3, 0.1)]);
        let s = Stream::new(3, "w");
        let mut c = [0u32; 4];
        for r in 0..100_000 {
            c[w.sample(s.at(r)) as usize] += 1;
        }
        assert!((69_000..71_000).contains(&c[1]), "{c:?}");
        assert!((19_000..21_000).contains(&c[2]), "{c:?}");
        assert!((9_000..11_000).contains(&c[3]), "{c:?}");
    }

    #[test]
    fn view_writer_inline_and_out_of_line() {
        let mut w = ViewWriter::default();
        for s in ["short", "a value longer than twelve", ""] {
            let start = w.buf().len();
            w.buf().extend_from_slice(s.as_bytes());
            w.finish_value(start);
        }
        let mut col = Views::default();
        Pool::new(["pooled, and long enough"]).write(&[0, Pool::EMPTY], &mut col);
        w.write_to(&mut col);
        let got: Vec<String> = (0..col.rows())
            .map(|i| col.with_str(i, str::to_owned))
            .collect();
        assert_eq!(
            got,
            [
                "pooled, and long enough",
                "",
                "short",
                "a value longer than twelve",
                ""
            ]
        );
    }
}
