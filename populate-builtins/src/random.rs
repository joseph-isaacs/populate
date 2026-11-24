//! Random leaf distributions: [`uniform`], [`choose`], [`weighted`], and the
//! string generators [`text`] and [`chars`].
//!
//! All of them collect column by column: `collect_into` reserves the exact
//! row count once and writes each value straight into the column sink. The
//! string generators write their bytes into the [`StrCol`] buffer directly,
//! so no per-value `String` is ever allocated.

use crate::bounded::Bounded;
use populate_core::{Columnar, Columns, Distribution, Sink, StrBuf, StrCol, in_local};
use rand::Rng;
use rand::distr::{Distribution as RandDistribution, Uniform as RandUniform};
use std::sync::Arc;

// ============================================================================
// Columnar fill
// ============================================================================

/// Values per [`fill`] chunk.
const CHUNK: usize = 256;

/// Push `n` values of `f` into `sink`, made `CHUNK` at a time into a stack
/// buffer and handed over with one `push_n` each. The loop that calls `f`
/// writes only to that local buffer, so `f`'s state (the leaf's sampler,
/// cloned into a local by the caller, and the RNG) stays in registers
/// instead of being stored back after every value.
#[inline(always)]
fn fill<T: Copy, S: Sink<T>>(sink: &mut S, n: usize, mut f: impl FnMut() -> T) {
    sink.reserve(n);
    if n == 0 {
        return;
    }
    // the first value fills the buffer, so `T` needs no `Default`
    let mut buf = [f(); CHUNK];
    let (mut start, mut left) = (1, n - 1);
    loop {
        let m = left.min(CHUNK - start);
        for slot in &mut buf[start..start + m] {
            *slot = f();
        }
        sink.push_n(buf[..start + m].iter().copied());
        left -= m;
        if left == 0 {
            return;
        }
        start = 0;
    }
}

/// Push `n` values `f(index)` into `sink`, the indices drawn by `b` a
/// chunk at a time with [`Bounded::fill`], then mapped (a pass the
/// compiler can vectorise) and handed over with one `push_n`.
#[inline(always)]
fn fill_indices<T, S: Sink<T>>(
    b: &mut Bounded,
    rng: &mut impl Rng,
    sink: &mut S,
    n: usize,
    f: impl Fn(u64) -> T,
) {
    sink.reserve(n);
    let mut idx = [0u64; CHUNK];
    let mut left = n;
    while left > 0 {
        let m = left.min(CHUNK);
        b.fill(rng, &mut idx[..m]);
        sink.push_n(idx[..m].iter().map(|&d| f(d)));
        left -= m;
    }
}

// ============================================================================
// Uniform
// ============================================================================

/// A type [`uniform`] can draw. Integers draw through [`Bounded`], so a
/// small range takes several values from each RNG call; floats (and
/// `char`) use rand's own `Uniform`.
pub trait UniformValue: Copy {
    type Sampler: Clone;
    /// The sampler for `lo..=hi`. Panics if `lo > hi`.
    fn sampler(lo: Self, hi: Self) -> Self::Sampler;
    fn sample(s: &mut Self::Sampler, rng: &mut impl Rng) -> Self;
    /// `n` values into `sink`, the same as `n` calls of [`Self::sample`].
    fn fill<S: Sink<Self>>(s: &mut Self::Sampler, rng: &mut impl Rng, sink: &mut S, n: usize) {
        let mut s2 = s.clone();
        fill(sink, n, || Self::sample(&mut s2, rng));
        *s = s2;
    }
}

/// An integer range: its low end and the offset draws.
#[derive(Clone, Copy, Debug)]
pub struct IntSampler<T> {
    lo: T,
    offset: Bounded,
}

macro_rules! uniform_int {
    ($($t:ty => $u:ty),*) => {$(
        impl UniformValue for $t {
            type Sampler = IntSampler<$t>;

            fn sampler(lo: $t, hi: $t) -> IntSampler<$t> {
                assert!(lo <= hi, "uniform: lo <= hi");
                // values in the range, as u64; 0 means all 2^64
                let span = (hi as $u).wrapping_sub(lo as $u) as u64;
                IntSampler {
                    lo,
                    offset: Bounded::span(span.wrapping_add(1)),
                }
            }

            #[inline(always)]
            fn sample(s: &mut IntSampler<$t>, rng: &mut impl Rng) -> $t {
                (s.lo as $u).wrapping_add(s.offset.next(rng) as $u) as $t
            }

            fn fill<S: Sink<$t>>(
                s: &mut IntSampler<$t>,
                rng: &mut impl Rng,
                sink: &mut S,
                n: usize,
            ) {
                let lo = s.lo as $u;
                fill_indices(&mut s.offset, rng, sink, n, |d| lo.wrapping_add(d as $u) as $t);
            }
        }
    )*};
}

uniform_int!(
    i8 => u8, i16 => u16, i32 => u32, i64 => u64, isize => usize,
    u8 => u8, u16 => u16, u32 => u32, u64 => u64, usize => usize
);

macro_rules! uniform_rand {
    ($($t:ty),*) => {$(
        impl UniformValue for $t {
            type Sampler = RandUniform<$t>;

            fn sampler(lo: $t, hi: $t) -> RandUniform<$t> {
                RandUniform::new_inclusive(lo, hi).expect("uniform: lo <= hi")
            }

            #[inline(always)]
            fn sample(s: &mut RandUniform<$t>, rng: &mut impl Rng) -> $t {
                s.sample(rng)
            }
        }
    )*};
}

uniform_rand!(f32, f64, char);

/// Uniform over `lo..=hi`.
#[derive(Clone)]
pub struct Uniform<T: UniformValue> {
    sampler: T::Sampler,
}

/// Uniform over `lo..=hi`. Panics if `lo > hi`.
pub fn uniform<T: UniformValue>(lo: T, hi: T) -> Uniform<T> {
    Uniform {
        sampler: T::sampler(lo, hi),
    }
}

impl<T: UniformValue + Columnar> Distribution for Uniform<T> {
    type Item = T;

    #[inline]
    fn sample(&mut self, rng: &mut impl Rng) -> T {
        T::sample(&mut self.sampler, rng)
    }

    fn collect_into(&mut self, cols: &mut Columns<T>, rng: &mut impl Rng, n: usize) {
        T::fill(&mut self.sampler, rng, cols, n);
    }
}

// ============================================================================
// Choose — uniform pick from a static list
// ============================================================================

/// Uniform pick from `values`.
#[derive(Clone)]
pub struct Choose<T: 'static> {
    values: &'static [T],
    idx: Bounded,
}

/// Uniform pick from `values` (any `Copy` type, including `&'static str`).
/// Panics if `values` is empty.
pub fn choose<T: Copy>(values: &'static [T]) -> Choose<T> {
    assert!(!values.is_empty(), "choose: empty list");
    Choose {
        values,
        idx: Bounded::new(values.len() as u64),
    }
}

impl<T: Copy + Columnar> Distribution for Choose<T> {
    type Item = T;

    #[inline]
    fn sample(&mut self, rng: &mut impl Rng) -> T {
        self.values[self.idx.next(rng) as usize]
    }

    fn collect_into(&mut self, cols: &mut Columns<T>, rng: &mut impl Rng, n: usize) {
        let values = self.values;
        fill_indices(&mut self.idx, rng, cols, n, |i| values[i as usize]);
    }
}

// ============================================================================
// Weighted — weighted pick from a static list
// ============================================================================

/// Pick from `(value, weight)` pairs with probability proportional to weight.
#[derive(Clone)]
pub struct Weighted<T: 'static> {
    values: &'static [(T, u32)],
    pick: Pick,
    draw: Bounded,
}

/// How a draw in `0..total` becomes an index into the values.
#[derive(Clone)]
enum Pick {
    /// `table[x]` is the index, for totals up to [`TABLE_MAX`].
    Table(Arc<[u16]>),
    /// Running weight totals (`cumulative[i]` is the sum of weights
    /// `0..=i`), binary searched.
    Search(Arc<[u32]>),
}

/// Largest total weight that gets a lookup table (8 KiB of `u16`).
const TABLE_MAX: u32 = 4096;

/// Weighted pick from `(value, weight)` pairs. Panics if the list is empty or
/// every weight is 0.
pub fn weighted<T: Copy>(values: &'static [(T, u32)]) -> Weighted<T> {
    let mut total = 0u32;
    let cumulative: Arc<[u32]> = values
        .iter()
        .map(|&(_, w)| {
            total = total
                .checked_add(w)
                .expect("weighted: total weight overflows u32");
            total
        })
        .collect();
    assert!(total > 0, "weighted: total weight must be > 0");
    let pick = if total <= TABLE_MAX && values.len() <= u16::MAX as usize + 1 {
        let mut table = Vec::with_capacity(total as usize);
        for (i, &(_, w)) in values.iter().enumerate() {
            table.extend(std::iter::repeat_n(i as u16, w as usize));
        }
        Pick::Table(table.into())
    } else {
        Pick::Search(cumulative)
    };
    Weighted {
        values,
        pick,
        draw: Bounded::new(total as u64),
    }
}

impl<T: Copy + Columnar> Distribution for Weighted<T> {
    type Item = T;

    #[inline]
    fn sample(&mut self, rng: &mut impl Rng) -> T {
        let x = self.draw.next(rng) as u32;
        let i = match &self.pick {
            Pick::Table(t) => t[x as usize] as usize,
            Pick::Search(c) => c.partition_point(|&c| c <= x),
        };
        self.values[i].0
    }

    fn collect_into(&mut self, cols: &mut Columns<T>, rng: &mut impl Rng, n: usize) {
        let values = self.values;
        match &self.pick {
            Pick::Table(t) => {
                let t = &**t;
                fill_indices(&mut self.draw, rng, cols, n, |x| {
                    values[t[x as usize] as usize].0
                })
            }
            Pick::Search(c) => {
                let c = &**c;
                fill_indices(&mut self.draw, rng, cols, n, |x| {
                    values[c.partition_point(|&c| c as u64 <= x)].0
                })
            }
        }
    }
}

/// The number of values in `min..=max`. Panics (naming `what`) if `min > max`.
fn span(min: usize, max: usize, what: &str) -> u64 {
    assert!(min <= max, "{what}: min <= max");
    (max - min) as u64 + 1
}

// ============================================================================
// String generators: shared columnar collection
// ============================================================================

/// A string generator that writes one value into a [`StrBuf`].
trait WriteStr {
    fn write(&mut self, rng: &mut impl Rng, out: &mut StrBuf<'_>);
    /// Expected bytes per value, for the up-front data reservation.
    fn mean_len(&self) -> usize;
}

macro_rules! string_distribution {
    ($t:ty) => {
        impl Distribution for $t {
            type Item = String;

            /// Allocates a `String`; collecting writes into the column's
            /// buffer instead.
            fn sample(&mut self, rng: &mut impl Rng) -> String {
                let mut col = StrCol::new();
                col.push_with(|b| self.write(rng, b));
                col.get(0).to_owned()
            }

            fn collect_into(&mut self, cols: &mut StrCol, rng: &mut impl Rng, n: usize) {
                cols.reserve_bytes(n * self.mean_len());
                in_local(self, |w| cols.push_n_with(n, |b| w.write(rng, b)));
            }
        }
    };
}

// ============================================================================
// Text — words from a list, space separated
// ============================================================================

/// Bytes per word block in [`Text`]: words up to 15 bytes long are copied
/// with one 16-byte move.
const WORD_BLOCK: usize = 16;

/// A word and a space, padded to [`WORD_BLOCK`] bytes, and that length.
type WordBlock = ([u8; WORD_BLOCK], u8);

/// `min..=max` words drawn uniformly from a list, joined by single spaces.
#[derive(Clone)]
pub struct Text {
    words: &'static [&'static str],
    /// Each word and a space, padded with spaces to [`WORD_BLOCK`] bytes,
    /// with its length: copied as one fixed-size move. `None` when a word
    /// is too long for a block.
    blocks: Option<Arc<[WordBlock]>>,
    pick: Bounded,
    min: usize,
    count: Bounded,
    mean_len: usize,
}

/// Random text: `min..=max` words from `words`, space separated.
pub fn text(words: &'static [&'static str], min: usize, max: usize) -> Text {
    assert!(!words.is_empty(), "text: empty word list");
    let avg_word = words
        .iter()
        .map(|w| w.len())
        .sum::<usize>()
        .div_ceil(words.len());
    let blocks = words
        .iter()
        .map(|w| {
            let mut b = [b' '; WORD_BLOCK];
            b.get_mut(..w.len())?.copy_from_slice(w.as_bytes());
            // the word and its space fit
            (w.len() < WORD_BLOCK).then_some((b, w.len() as u8 + 1))
        })
        .collect();
    Text {
        words,
        blocks,
        pick: Bounded::new(words.len() as u64),
        min,
        count: Bounded::new(span(min, max, "text")),
        mean_len: (min + max).div_ceil(2) * (avg_word + 1),
    }
}

impl WriteStr for Text {
    #[inline]
    fn write(&mut self, rng: &mut impl Rng, out: &mut StrBuf<'_>) {
        let count = self.min + self.count.next(rng) as usize;
        match &self.blocks {
            Some(blocks) => {
                // every word with a space after it, then the last space off
                for _ in 0..count {
                    let (b, len) = &blocks[self.pick.next(rng) as usize];
                    out.push_padded(b, *len as usize);
                }
                if count > 0 {
                    out.pop_ascii();
                }
            }
            None => {
                for i in 0..count {
                    if i > 0 {
                        out.push_ascii(b' ');
                    }
                    out.push_str(self.words[self.pick.next(rng) as usize]);
                }
            }
        }
    }

    fn mean_len(&self) -> usize {
        self.mean_len
    }
}

string_distribution!(Text);

// ============================================================================
// Chars — random characters from an ASCII alphabet
// ============================================================================

/// `min..=max` characters drawn uniformly from an ASCII alphabet.
#[derive(Clone)]
pub struct Chars {
    alphabet: &'static [u8],
    pick: Bounded,
    min: usize,
    len: Bounded,
    mean_len: usize,
}

/// Random string of `min..=max` characters from `alphabet` (ASCII only;
/// panics otherwise).
pub fn chars(alphabet: &'static [u8], min: usize, max: usize) -> Chars {
    assert!(!alphabet.is_empty(), "chars: empty alphabet");
    assert!(alphabet.is_ascii(), "chars: alphabet must be ASCII");
    Chars {
        alphabet,
        pick: Bounded::new(alphabet.len() as u64),
        min,
        len: Bounded::new(span(min, max, "chars")),
        mean_len: (min + max).div_ceil(2),
    }
}

impl WriteStr for Chars {
    #[inline]
    fn write(&mut self, rng: &mut impl Rng, out: &mut StrBuf<'_>) {
        let (alphabet, pick) = (self.alphabet, &mut self.pick);
        let len = self.min + self.len.next(rng) as usize;
        out.extend_ascii((0..len).map(|_| alphabet[pick.next(rng) as usize]));
    }

    fn mean_len(&self) -> usize {
        self.mean_len
    }
}

string_distribution!(Chars);
