//! TPC-H leaf columns as populate-2 distributions.
//!
//! A [`Column`] owns its random stream(s) and knows how to produce one row,
//! either as a value (`value`, row mode) or by appending straight into its
//! Arrow sink (`write`, columnar mode, no allocation). [`Col`] adapts any
//! column into a `populate_2::traits::Distribution<VecBackend>`, so tables are
//! ordinary populate-2 tuples of columns.

use crate::rng::{Rand, Stream, scale};
use crate::values::{Dec, DecCol, POOL_BUFFER, Str, StrCol, push_int, text_pool};
use populate_2::hlist::{HCons, HNil};
use populate_2::traits::{DefaultSink, Distribution, SampleIter, Sink, VecBackend};
use rand::Rng;
use std::borrow::Cow;
use std::sync::LazyLock;
use tpchgen::distribution::{Distribution as TpchDistribution, Distributions};

// ============================================================================
// Column + Col adapter
// ============================================================================

pub trait Column: Clone {
    type Item: DefaultSink<VecBackend, Sink = HCons<Self::Sink, HNil>>;
    type Sink: Sink<Self::Item>;
    type State;

    /// Streams positioned at global row `start`.
    fn init(&self, start: i64) -> Self::State;

    /// Row mode: produce the value for global row `row` (0-based).
    fn value(&self, st: &mut Self::State, row: i64) -> Self::Item;

    /// Pre-size the sink for `n` more rows (columnar mode only).
    #[inline]
    fn reserve(&self, _sink: &mut Self::Sink, _n: usize) {}

    /// Columnar mode: append row `row` to `sink`. Override to skip building an
    /// intermediate value.
    #[inline]
    fn write(&self, st: &mut Self::State, row: i64, sink: &mut Self::Sink) {
        sink.push(self.value(st, row));
    }
}

/// A column starting at global row `start`.
#[derive(Clone)]
pub struct Col<C> {
    pub col: C,
    pub start: i64,
}

pub fn col<C: Column>(col: C, start: i64) -> Col<C> {
    Col { col, start }
}

pub struct ColIter<C: Column> {
    col: C,
    st: C::State,
    row: i64,
}

impl<C: Column> SampleIter for ColIter<C> {
    type Item = C::Item;

    #[inline]
    fn next(&mut self, _rng: &mut impl Rng) -> C::Item {
        let v = self.col.value(&mut self.st, self.row);
        self.row += 1;
        v
    }
}

impl<C: Column> Distribution<VecBackend> for Col<C> {
    type Item = C::Item;
    type RowIter = ColIter<C>;

    fn row_iter(&mut self) -> ColIter<C> {
        ColIter {
            st: self.col.init(self.start),
            col: self.col.clone(),
            row: self.start,
        }
    }

    fn collect_into(
        &mut self,
        sink: HCons<C::Sink, HNil>,
        _rng: &mut impl Rng,
        n: usize,
    ) -> HCons<C::Sink, HNil> {
        let HCons(mut s, HNil) = sink;
        let mut st = self.col.init(self.start);
        self.col.reserve(&mut s, n);
        for row in self.start..self.start + n as i64 {
            self.col.write(&mut st, row, &mut s);
        }
        HCons(s, HNil)
    }
}

// ============================================================================
// Weighted word lists (dists.dss), expanded once
// ============================================================================

/// A `dists.dss` distribution expanded so a draw in `[0, total_weight)`
/// indexes the chosen word directly (as dbgen does).
pub struct Picks {
    pub table: Vec<&'static str>,
}

impl Picks {
    fn expand(d: &'static TpchDistribution) -> Self {
        let mut table = Vec::new();
        let mut prev = 0;
        for (i, &value) in d.get_values().iter().enumerate() {
            let cumulative = d.get_weight(i);
            for _ in prev..cumulative {
                table.push(value);
            }
            prev = cumulative;
        }
        Picks { table }
    }
}

pub fn dists() -> &'static Distributions {
    Distributions::static_default()
}

macro_rules! picks {
    ($($name:ident => $dist:ident),* $(,)?) => {
        $(pub static $name: LazyLock<Picks> = LazyLock::new(|| Picks::expand(dists().$dist()));)*
    };
}

picks!(
    PART_TYPES => part_types,
    PART_CONTAINERS => part_containers,
    MARKET_SEGMENTS => market_segments,
);

// ============================================================================
// Bounded int, converted to the column's type
// ============================================================================

/// Value types a bounded int can become, with their single-column sink.
pub trait FromI32: Copy + DefaultSink<VecBackend, Sink = HCons<Self::Col, HNil>> {
    type Col: Sink<Self> + Default;
    fn from_i32(v: i32) -> Self;
}

impl FromI32 for i32 {
    type Col = Vec<i32>;
    #[inline(always)]
    fn from_i32(v: i32) -> Self {
        v
    }
}

impl FromI32 for i64 {
    type Col = Vec<i64>;
    #[inline(always)]
    fn from_i32(v: i32) -> Self {
        v as i64
    }
}

impl FromI32 for Dec {
    type Col = DecCol;
    #[inline(always)]
    fn from_i32(v: i32) -> Self {
        Dec(v as i64)
    }
}

/// `RandomBoundedInt`: one draw per item in `[lo, hi]`.
#[derive(Clone, Copy)]
pub struct Bounded<T> {
    pub seed: i64,
    pub lo: i32,
    pub hi: i32,
    pub seeds_per_row: i32,
    pub group: u32,
    _t: std::marker::PhantomData<T>,
}

pub fn bounded<T>(seed: i64, lo: i32, hi: i32) -> Bounded<T> {
    grouped_bounded(seed, lo, hi, 1, 1)
}

pub fn grouped_bounded<T>(
    seed: i64,
    lo: i32,
    hi: i32,
    seeds_per_row: i32,
    group: u32,
) -> Bounded<T> {
    Bounded {
        seed,
        lo,
        hi,
        seeds_per_row,
        group,
        _t: std::marker::PhantomData,
    }
}

impl<T: FromI32> Column for Bounded<T> {
    type Item = T;
    type Sink = T::Col;
    type State = Stream;

    fn init(&self, start: i64) -> Stream {
        Stream::new(self.seed, self.seeds_per_row, self.group, start)
    }

    #[inline(always)]
    fn value(&self, st: &mut Stream, _row: i64) -> T {
        T::from_i32(st.next_item().next_int(self.lo, self.hi))
    }
}

// ============================================================================
// Pick a weighted word (`RandomString`)
// ============================================================================

#[derive(Clone, Copy)]
pub struct Pick {
    pub seed: i64,
    pub picks: &'static LazyLock<Picks>,
}

pub fn pick(seed: i64, picks: &'static LazyLock<Picks>) -> Pick {
    Pick { seed, picks }
}

impl Pick {
    #[inline(always)]
    fn draw(&self, st: &mut Stream) -> &'static str {
        let table = &self.picks.table;
        table[st.next_item().next_int(0, table.len() as i32 - 1) as usize]
    }
}

impl Column for Pick {
    type Item = Str;
    type Sink = StrCol;
    type State = Stream;

    fn reserve(&self, sink: &mut StrCol, n: usize) {
        sink.reserve(n, 16);
    }

    fn init(&self, start: i64) -> Stream {
        LazyLock::force(self.picks);
        Stream::ungrouped(self.seed, 1, start)
    }

    fn value(&self, st: &mut Stream, _row: i64) -> Str {
        Str::borrowed(self.draw(st))
    }

    #[inline]
    fn write(&self, st: &mut Stream, _row: i64, sink: &mut StrCol) {
        sink.push_str(self.draw(st));
    }
}

// ============================================================================
// Text-pool slice (`RandomText`): every *_comment column
// ============================================================================

#[derive(Clone, Copy)]
pub struct Text {
    pub seed: i64,
    pub min: i32,
    pub max: i32,
    /// `pool size - max`, computed once (not a `OnceLock` lookup per row)
    pub span: i32,
    pub seeds_per_row: i32,
    pub group: u32,
}

pub fn text(seed: i64, average_length: i32) -> Text {
    grouped_text(seed, average_length, 1)
}

/// `group` items per TPC-H row (2 draws each).
pub fn grouped_text(seed: i64, average_length: i32, group: u32) -> Text {
    let avg = average_length as f64;
    Text {
        seed,
        min: (avg * 0.4) as i32,
        max: (avg * 1.6) as i32,
        span: text_pool().size() - (avg * 1.6) as i32,
        seeds_per_row: 2 * group as i32,
        group,
    }
}

impl Text {
    /// (offset, len) into the text pool.
    #[inline(always)]
    pub fn draw(&self, r: &mut Rand) -> (i32, i32) {
        let [a, b] = r.ahead::<2>();
        (scale(a, 0, self.span), scale(b, self.min, self.max))
    }
}

impl Column for Text {
    type Item = Str;
    type Sink = StrCol;
    type State = (Stream, &'static [u8]);

    fn reserve(&self, sink: &mut StrCol, n: usize) {
        sink.reserve(n, 0);
    }

    fn init(&self, start: i64) -> Self::State {
        (
            Stream::new(self.seed, self.seeds_per_row, self.group, start),
            POOL_BUFFER.as_slice(),
        )
    }

    fn value(&self, st: &mut Self::State, _row: i64) -> Str {
        let (offset, len) = self.draw(st.0.next_item());
        Str::borrowed(text_pool().text(offset, offset + len))
    }

    #[inline]
    fn write(&self, st: &mut Self::State, _row: i64, sink: &mut StrCol) {
        let (offset, len) = self.draw(st.0.next_item());
        sink.push_pool(st.1, offset as u32, len as u32);
    }
}

// ============================================================================
// Random alphanumeric (`RandomAlphaNumeric`): addresses
// ============================================================================

const ALPHA_NUMERIC: &[u8; 64] =
    b"0123456789abcdefghijklmnopqrstuvwxyz ABCDEFGHIJKLMNOPQRSTUVWXYZ,";

#[derive(Clone, Copy)]
pub struct AlphaNum {
    pub seed: i64,
    pub min: i32,
    pub max: i32,
}

pub fn alpha_num(seed: i64, average_length: i32) -> AlphaNum {
    assert!(
        average_length <= 25,
        "alpha_num supports average lengths up to 25"
    );
    AlphaNum {
        seed,
        min: (average_length as f64 * 0.4) as i32,
        max: (average_length as f64 * 1.6) as i32,
    }
}

impl AlphaNum {
    /// One length draw; characters come from a *copy* of the stream after it,
    /// so they don't count against the row's 9 reserved draws.
    #[inline(always)]
    fn fill(&self, st: &mut Stream, out: &mut Vec<u8>) {
        let r = st.next_item();
        let len = r.next_int(self.min, self.max) as usize;
        // Characters come from a copy of the stream: draw i serves chars
        // 5i..5i+5. Compute all of them independently (no serial chain).
        // TPC-H addresses (average 25) are at most 40 chars: 8 draws.
        debug_assert!(len <= 40, "alpha_num supports average lengths up to 25");
        let draws = (*r).ahead::<8>();
        // Build in a stack buffer, append once (no per-byte capacity checks).
        let mut buf = [0u8; 40];
        let buf = &mut buf[..len.min(40)];
        for (chunk, &seed) in buf.chunks_mut(5).zip(&draws) {
            let mut char_index = scale(seed, 0, i32::MAX) as i64;
            for b in chunk {
                *b = ALPHA_NUMERIC[(char_index & 0x3f) as usize];
                char_index >>= 6;
            }
        }
        out.extend_from_slice(buf);
    }
}

impl Column for AlphaNum {
    type Item = Str;
    type Sink = StrCol;
    type State = Stream;

    fn reserve(&self, sink: &mut StrCol, n: usize) {
        sink.reserve(n, 25);
    }

    fn init(&self, start: i64) -> Stream {
        Stream::ungrouped(self.seed, 9, start)
    }

    fn value(&self, st: &mut Stream, _row: i64) -> Str {
        let mut out = Vec::new();
        self.fill(st, &mut out);
        Str(Cow::Owned(String::from_utf8(out).expect("ascii")))
    }

    #[inline]
    fn write(&self, st: &mut Stream, _row: i64, sink: &mut StrCol) {
        sink.push_with(|out| self.fill(st, out));
    }
}

// ============================================================================
// Phone number: depends on the row's nation key, so it re-runs the
// nation-key stream itself instead of reading another column
// ============================================================================

#[derive(Clone, Copy)]
pub struct Phone {
    pub nation_seed: i64,
    pub phone_seed: i64,
}

pub fn phone(nation_seed: i64, phone_seed: i64) -> Phone {
    Phone {
        nation_seed,
        phone_seed,
    }
}

impl Phone {
    #[inline(always)]
    fn fill(&self, st: &mut (Stream, Stream), out: &mut Vec<u8>) {
        let nation_key = st.0.next_item().next_int(0, 24) as u64;
        let r = st.1.next_item();
        let (l1, l2, l3) = (
            r.next_int(100, 999),
            r.next_int(100, 999),
            r.next_int(1000, 9999),
        );
        // Fixed "CC-LLL-LLL-LLLL": every field has a known digit count.
        let cc = (10 + nation_key % 90) as u32;
        let (l1, l2, l3) = (l1 as u32, l2 as u32, l3 as u32);
        let d = |v: u32| b'0' + v as u8;
        out.extend_from_slice(&[
            d(cc / 10),
            d(cc % 10),
            b'-',
            d(l1 / 100),
            d(l1 / 10 % 10),
            d(l1 % 10),
            b'-',
            d(l2 / 100),
            d(l2 / 10 % 10),
            d(l2 % 10),
            b'-',
            d(l3 / 1000),
            d(l3 / 100 % 10),
            d(l3 / 10 % 10),
            d(l3 % 10),
        ]);
    }
}

impl Column for Phone {
    type Item = Str;
    type Sink = StrCol;
    type State = (Stream, Stream);

    fn reserve(&self, sink: &mut StrCol, n: usize) {
        sink.reserve(n, 15);
    }

    fn init(&self, start: i64) -> (Stream, Stream) {
        (
            Stream::ungrouped(self.nation_seed, 1, start),
            Stream::ungrouped(self.phone_seed, 3, start),
        )
    }

    fn value(&self, st: &mut (Stream, Stream), _row: i64) -> Str {
        let mut out = Vec::with_capacity(15);
        self.fill(st, &mut out);
        Str(Cow::Owned(String::from_utf8(out).expect("ascii")))
    }

    #[inline]
    fn write(&self, st: &mut (Stream, Stream), _row: i64, sink: &mut StrCol) {
        sink.push_with(|out| self.fill(st, out));
    }
}

// ============================================================================
// "Prefix#000000042"-style names from the row key (no randomness)
// ============================================================================

#[derive(Clone, Copy)]
pub struct KeyName {
    pub prefix: &'static str,
    pub width: usize,
}

pub fn key_name(prefix: &'static str, width: usize) -> KeyName {
    KeyName { prefix, width }
}

impl KeyName {
    #[inline(always)]
    fn fill(&self, row: i64, out: &mut Vec<u8>) {
        out.extend_from_slice(self.prefix.as_bytes());
        push_int(out, (row + 1) as u64, self.width);
    }
}

impl Column for KeyName {
    type Item = Str;
    type Sink = StrCol;
    type State = ();

    fn reserve(&self, sink: &mut StrCol, n: usize) {
        sink.reserve(n, 18);
    }

    fn init(&self, _start: i64) {}

    fn value(&self, _st: &mut (), row: i64) -> Str {
        let mut out = Vec::new();
        self.fill(row, &mut out);
        Str(Cow::Owned(String::from_utf8(out).expect("ascii")))
    }

    #[inline]
    fn write(&self, _st: &mut (), row: i64, sink: &mut StrCol) {
        sink.push_with(|out| self.fill(row, out));
    }
}
