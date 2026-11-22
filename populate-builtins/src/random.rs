//! Random leaf distributions: [`uniform`], [`choose`], [`weighted`], and the
//! string generators [`text`] and [`chars`].
//!
//! All of them collect column by column: `collect_into` reserves the exact
//! row count once and writes each value straight into the column sink. The
//! string generators write their bytes into the [`StrCol`] buffer directly,
//! so no per-value `String` is ever allocated.

use populate_core::string::{StrBuf, StrCol};
use populate_core::traits::{Backend, DefaultSink, Distribution, HCons, HNil, SampleIter, Sink};
use rand::Rng;
use rand::distr::uniform::SampleUniform;
use rand::distr::{Distribution as RandDistribution, Uniform as RandUniform};
use std::sync::Arc;

// ============================================================================
// Uniform
// ============================================================================

/// Uniform over `lo..=hi`.
pub struct Uniform<T: SampleUniform> {
    dist: RandUniform<T>,
}

impl<T: SampleUniform> Clone for Uniform<T>
where
    RandUniform<T>: Clone,
{
    fn clone(&self) -> Self {
        Self {
            dist: self.dist.clone(),
        }
    }
}

/// Uniform over `lo..=hi`. Panics if `lo > hi`.
pub fn uniform<T: SampleUniform>(lo: T, hi: T) -> Uniform<T> {
    Uniform {
        dist: RandUniform::new_inclusive(lo, hi).expect("uniform: lo <= hi"),
    }
}

pub struct UniformIter<T: SampleUniform>(RandUniform<T>);

impl<T: SampleUniform> SampleIter for UniformIter<T> {
    type Item = T;

    #[inline]
    fn next(&mut self, rng: &mut impl Rng) -> T {
        self.0.sample(rng)
    }
}

impl<B: Backend, T> Distribution<B> for Uniform<T>
where
    T: SampleUniform + DefaultSink<B>,
    RandUniform<T>: Clone,
    <T as DefaultSink<B>>::Sink: Sink<T>,
{
    type Item = T;
    type RowIter = UniformIter<T>;

    fn row_iter(&mut self) -> UniformIter<T> {
        UniformIter(self.dist.clone())
    }

    fn collect_into(
        &mut self,
        mut sink: <T as DefaultSink<B>>::Sink,
        rng: &mut impl Rng,
        n: usize,
    ) -> <T as DefaultSink<B>>::Sink {
        sink.reserve(n);
        for _ in 0..n {
            sink.push(self.dist.sample(rng));
        }
        sink
    }
}

// ============================================================================
// Choose — uniform pick from a static list
// ============================================================================

/// Uniform pick from `values`.
#[derive(Clone)]
pub struct Choose<T: 'static> {
    values: &'static [T],
    idx: RandUniform<usize>,
}

/// Uniform pick from `values` (any `Copy` type, including `&'static str`).
/// Panics if `values` is empty.
pub fn choose<T: Copy>(values: &'static [T]) -> Choose<T> {
    assert!(!values.is_empty(), "choose: empty list");
    Choose {
        values,
        idx: RandUniform::new(0, values.len()).expect("non-empty"),
    }
}

impl<T: Copy> SampleIter for Choose<T> {
    type Item = T;

    #[inline]
    fn next(&mut self, rng: &mut impl Rng) -> T {
        self.values[self.idx.sample(rng)]
    }
}

impl<B: Backend, T: Copy + DefaultSink<B>> Distribution<B> for Choose<T>
where
    <T as DefaultSink<B>>::Sink: Sink<T>,
{
    type Item = T;
    type RowIter = Choose<T>;

    fn row_iter(&mut self) -> Choose<T> {
        self.clone()
    }

    fn collect_into(
        &mut self,
        mut sink: <T as DefaultSink<B>>::Sink,
        rng: &mut impl Rng,
        n: usize,
    ) -> <T as DefaultSink<B>>::Sink {
        sink.reserve(n);
        for _ in 0..n {
            sink.push(self.next(rng));
        }
        sink
    }
}

// ============================================================================
// Weighted — weighted pick from a static list
// ============================================================================

/// Pick from `(value, weight)` pairs with probability proportional to weight.
#[derive(Clone)]
pub struct Weighted<T: 'static> {
    values: &'static [(T, u32)],
    /// running weight totals; `cumulative[i]` is the sum of weights `0..=i`
    cumulative: Arc<[u32]>,
    draw: RandUniform<u32>,
}

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
    Weighted {
        values,
        cumulative,
        draw: RandUniform::new(0, total).expect("total > 0"),
    }
}

impl<T: Copy> SampleIter for Weighted<T> {
    type Item = T;

    #[inline]
    fn next(&mut self, rng: &mut impl Rng) -> T {
        let x = self.draw.sample(rng);
        let i = self.cumulative.partition_point(|&c| c <= x);
        self.values[i].0
    }
}

impl<B: Backend, T: Copy + DefaultSink<B>> Distribution<B> for Weighted<T>
where
    <T as DefaultSink<B>>::Sink: Sink<T>,
{
    type Item = T;
    type RowIter = Weighted<T>;

    fn row_iter(&mut self) -> Weighted<T> {
        self.clone()
    }

    fn collect_into(
        &mut self,
        mut sink: <T as DefaultSink<B>>::Sink,
        rng: &mut impl Rng,
        n: usize,
    ) -> <T as DefaultSink<B>>::Sink {
        sink.reserve(n);
        for _ in 0..n {
            sink.push(self.next(rng));
        }
        sink
    }
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

fn collect_str<W: WriteStr>(
    w: &mut W,
    sink: HCons<StrCol, HNil>,
    rng: &mut impl Rng,
    n: usize,
) -> HCons<StrCol, HNil> {
    let HCons(mut col, HNil) = sink;
    Sink::<String>::reserve(&mut col, n);
    col.reserve_bytes(n * w.mean_len());
    for _ in 0..n {
        col.push_with(|b| w.write(rng, b));
    }
    HCons(col, HNil)
}

/// Row-mode iterator for the string generators (allocates one `String` per
/// row; columnar collection never uses it).
pub struct StrIter<W>(W);

impl<W: WriteStr> SampleIter for StrIter<W> {
    type Item = String;

    fn next(&mut self, rng: &mut impl Rng) -> String {
        let mut col = StrCol::new();
        col.push_with(|b| self.0.write(rng, b));
        col.get(0).to_owned()
    }
}

macro_rules! string_distribution {
    ($t:ty) => {
        impl<B: Backend> Distribution<B> for $t {
            type Item = String;
            type RowIter = StrIter<$t>;

            fn row_iter(&mut self) -> Self::RowIter {
                StrIter(self.clone())
            }

            fn collect_into(
                &mut self,
                sink: HCons<StrCol, HNil>,
                rng: &mut impl Rng,
                n: usize,
            ) -> HCons<StrCol, HNil> {
                collect_str(self, sink, rng, n)
            }
        }
    };
}

// ============================================================================
// Text — words from a list, space separated
// ============================================================================

/// `min..=max` words drawn uniformly from a list, joined by single spaces.
#[derive(Clone)]
pub struct Text {
    words: &'static [&'static str],
    pick: RandUniform<usize>,
    count: RandUniform<usize>,
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
    Text {
        words,
        pick: RandUniform::new(0, words.len()).expect("non-empty"),
        count: RandUniform::new_inclusive(min, max).expect("text: min <= max"),
        mean_len: (min + max).div_ceil(2) * (avg_word + 1),
    }
}

impl WriteStr for Text {
    #[inline]
    fn write(&mut self, rng: &mut impl Rng, out: &mut StrBuf<'_>) {
        for i in 0..self.count.sample(rng) {
            if i > 0 {
                out.push_ascii(b' ');
            }
            out.push_str(self.words[self.pick.sample(rng)]);
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
    pick: RandUniform<usize>,
    len: RandUniform<usize>,
    mean_len: usize,
}

/// Random string of `min..=max` characters from `alphabet` (ASCII only;
/// panics otherwise).
pub fn chars(alphabet: &'static [u8], min: usize, max: usize) -> Chars {
    assert!(!alphabet.is_empty(), "chars: empty alphabet");
    assert!(alphabet.is_ascii(), "chars: alphabet must be ASCII");
    Chars {
        alphabet,
        pick: RandUniform::new(0, alphabet.len()).expect("non-empty"),
        len: RandUniform::new_inclusive(min, max).expect("chars: min <= max"),
        mean_len: (min + max).div_ceil(2),
    }
}

impl WriteStr for Chars {
    #[inline]
    fn write(&mut self, rng: &mut impl Rng, out: &mut StrBuf<'_>) {
        for _ in 0..self.len.sample(rng) {
            out.push_ascii(self.alphabet[self.pick.sample(rng)]);
        }
    }

    fn mean_len(&self) -> usize {
        self.mean_len
    }
}

string_distribution!(Chars);
