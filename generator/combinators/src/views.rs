//! `Utf8View` string columns over buffers a generator shares between
//! batches: a dictionary of distribution values, a text pool. A string from
//! a shared buffer is a precomputed view ([`Word`]), so collecting it copies
//! no bytes; a string written per row ([`Str`]) goes into the batch's own
//! buffer, [`DATA`].
//!
//! A generator names its buffers with a marker type implementing
//! [`Buffers`]; the column types are generic over it, so the schema still
//! comes from the types alone.
//!
//! A string from a fixed list that is not in a shared buffer is a
//! [`Listed`]: its view is made once, and a batch copies the bytes of the
//! long ones (dsdgen's distributions, in TPC-DS and DSB). Every column
//! also writes itself as a text field ([`TextField`]).
//!
//! ```
//! use populate_combinators::views::{Buffers, Dict, Shared, Word, Picks, static_buffer};
//! use std::sync::LazyLock;
//!
//! #[derive(Debug, Clone, Copy, PartialEq, Default)]
//! struct Mine;
//!
//! static PICKS: LazyLock<(Picks, &'static [u8])> = LazyLock::new(|| {
//!     let mut dict = Dict::new(1);
//!     let p = Picks::new(&["red", "a much longer colour"], |i| [3, 4][i], &mut dict);
//!     (p, dict.leak())
//! });
//! static SHARED: LazyLock<Shared> =
//!     LazyLock::new(|| Shared::new(vec![static_buffer(PICKS.1)]));
//!
//! impl Buffers for Mine {
//!     fn shared() -> &'static Shared {
//!         &SHARED
//!     }
//! }
//!
//! let w: Word<Mine> = Word::new(PICKS.0.view_of(1));
//! # let _ = w;
//! ```

use arrow_array::{ArrayRef, StringViewArray};
use arrow_buffer::{Buffer, ScalarBuffer};
use arrow_schema::DataType;
use populate_core::{Append, Columnar, Nullable, RowCount, Sink, StrBuf, StrSink};
use populate_sink_arrow::IntoArrowArray;
use populate_table::text::{Cursor, Encoding, TextField};
use std::fmt::Debug;
use std::marker::PhantomData;
use std::ptr::NonNull;
use std::sync::Arc;

use crate::lehmer::scale;

/// Buffer index of a batch's own string bytes.
pub const DATA: u32 = 0;

/// The view of `bytes`, stored at `offset` of buffer `buffer` (ignored for
/// strings of up to 12 bytes, which live in the view).
///
/// Written out rather than arrow's `make_view`, which is not inlined and
/// is on every row's path here.
#[inline(always)]
pub fn view(bytes: &[u8], buffer: u32, offset: u32) -> u128 {
    let len = bytes.len();
    if len <= 12 {
        let mut b = [0u8; 16];
        b[4..4 + len].copy_from_slice(bytes);
        u128::from_le_bytes(b) | len as u128
    } else {
        let prefix = u32::from_le_bytes(bytes[..4].try_into().expect("4 bytes"));
        len as u128 | (prefix as u128) << 32 | (buffer as u128) << 64 | (offset as u128) << 96
    }
}

/// `bytes` as an Arrow buffer, without copying.
pub fn static_buffer(bytes: &'static [u8]) -> Buffer {
    let ptr = NonNull::new(bytes.as_ptr() as *mut u8).expect("non-null");
    // SAFETY: the bytes are 'static and never written, so they outlive the
    // buffer and are valid for `len` bytes; the owner is a no-op.
    unsafe { Buffer::from_custom_allocation(ptr, bytes.len(), Arc::new(())) }
}

/// The buffers every string column of a generator points into: [`DATA`]
/// (each batch's own) then the shared ones, numbered from 1.
#[derive(Debug)]
pub struct Shared {
    rest: Vec<Buffer>,
    /// `[empty, rest..]`, for columns with no bytes of their own.
    empty: Arc<[Buffer]>,
}

impl Shared {
    /// Shared buffers `rest`, numbered `1..`.
    pub fn new(rest: Vec<Buffer>) -> Self {
        let empty = std::iter::once(Buffer::from_vec(Vec::<u8>::new()))
            .chain(rest.iter().cloned())
            .collect();
        Shared { rest, empty }
    }

    /// A string column from views and the batch's own bytes.
    ///
    /// Every view must have been made by [`view`] from valid UTF-8 at the
    /// offset of the buffer it names.
    pub fn array(&self, views: Vec<u128>, data: Vec<u8>) -> StringViewArray {
        let buffers = if data.is_empty() {
            self.empty.clone()
        } else {
            std::iter::once(Buffer::from_vec(data))
                .chain(self.rest.iter().cloned())
                .collect()
        };
        // SAFETY: the views are made by `view` from UTF-8 bytes at the
        // offset of the buffer they name (this type's contract).
        unsafe { StringViewArray::new_unchecked(ScalarBuffer::from(views), buffers, None) }
    }
}

/// A generator's string buffers: a marker type, so the column types say
/// which buffers their views point into.
pub trait Buffers: Debug + Clone + Copy + PartialEq + Default + Send + Sync + 'static {
    fn shared() -> &'static Shared;
}

/// The buffers of a generator whose strings are all written per row:
/// none shared, so every view is inline or into the batch's own [`DATA`].
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct Unshared;

impl Buffers for Unshared {
    fn shared() -> &'static Shared {
        static NONE: std::sync::LazyLock<Shared> = std::sync::LazyLock::new(|| Shared::new(vec![]));
        &NONE
    }
}

/// Bytes of a shared dictionary being built: values longer than 12 bytes
/// (shorter ones live in their views).
#[derive(Debug)]
pub struct Dict {
    buffer: u32,
    bytes: Vec<u8>,
}

impl Dict {
    /// A dictionary that will be shared buffer `buffer`.
    pub fn new(buffer: u32) -> Self {
        Dict {
            buffer,
            bytes: Vec::new(),
        }
    }

    /// Add `s`, returning its view.
    pub fn add(&mut self, s: &str) -> u128 {
        let v = view(s.as_bytes(), self.buffer, self.bytes.len() as u32);
        if s.len() > 12 {
            self.bytes.extend_from_slice(s.as_bytes());
        }
        v
    }

    /// The bytes, for the life of the program (for [`static_buffer`]).
    pub fn leak(self) -> &'static [u8] {
        Box::leak(self.bytes.into_boxed_slice())
    }
}

/// `f(i)` once per unit of weight of value `i`, for `n` values whose
/// cumulative weights are `cumulative(i)` (dbgen's distributions): a draw
/// in `0..total weight` then indexes the result directly.
pub fn by_weight<T: Clone>(
    n: usize,
    cumulative: impl Fn(usize) -> i32,
    mut f: impl FnMut(usize) -> T,
) -> Vec<T> {
    let mut out = Vec::new();
    let mut prev = 0;
    for i in 0..n {
        let w = cumulative(i);
        out.extend(std::iter::repeat_n(f(i), (w - prev).max(0) as usize));
        prev = w;
    }
    out
}

/// dbgen's weighted pick (`pick_str`, `Distribution::random_value`): a
/// draw scaled to the total weight, then the value that weight unit
/// belongs to, as a view into a [`Dict`].
#[derive(Debug)]
pub struct Picks {
    /// Value index for each unit of weight.
    picks: Vec<u8>,
    /// Each value's view.
    pub views: Vec<u128>,
    pub values: Vec<&'static str>,
}

impl Picks {
    /// `values`, with cumulative weights `cumulative(i)`, added to `dict`.
    pub fn new(
        values: &[&'static str],
        cumulative: impl Fn(usize) -> i32,
        dict: &mut Dict,
    ) -> Self {
        assert!(values.len() <= 256);
        Picks {
            picks: by_weight(values.len(), cumulative, |i| i as u8),
            views: values.iter().map(|v| dict.add(v)).collect(),
            values: values.to_vec(),
        }
    }

    /// Total weight: picks are `0..weight`.
    #[inline(always)]
    pub fn weight(&self) -> u32 {
        self.picks.len() as u32
    }

    /// The value index draw `x` picks.
    #[inline(always)]
    pub fn index(&self, x: u32) -> usize {
        // weights are small, so the integer mapping is exact (see `scale`)
        self.picks[scale(x, self.weight()) as usize] as usize
    }

    /// The view of value `i`.
    #[inline(always)]
    pub fn view_of(&self, i: usize) -> u128 {
        self.views[i]
    }

    /// The view draw `x` picks.
    #[inline(always)]
    pub fn view(&self, x: u32) -> u128 {
        self.views[self.index(x)]
    }
}

/// A string that is a precomputed view into `B`'s buffers.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct Word<B>(pub u128, PhantomData<B>);

impl<B> Word<B> {
    #[inline(always)]
    pub fn new(view: u128) -> Self {
        Word(view, PhantomData)
    }

    /// A literal of up to 12 bytes.
    pub fn lit(s: &[u8]) -> Self {
        assert!(s.len() <= 12);
        Word::new(view(s, 0, 0))
    }
}

/// Columns of [`Word`]s: their views.
#[derive(Debug, Default, Clone)]
pub struct Words<B>(Vec<u128>, PhantomData<B>);

impl<B: Buffers> Columnar for Word<B> {
    type Columns = Words<B>;
}

impl<B> Sink<Word<B>> for Words<B> {
    #[inline(always)]
    fn push(&mut self, w: Word<B>) {
        self.0.push(w.0);
    }

    fn reserve(&mut self, n: usize) {
        self.0.reserve(n);
    }

    #[inline(always)]
    fn push_n(&mut self, values: impl Iterator<Item = Word<B>>) {
        self.0.extend(values.map(|w| w.0));
    }
}

impl<B> RowCount for Words<B> {
    fn rows(&self) -> usize {
        self.0.len()
    }
}

impl<B> Append for Words<B> {
    fn append_rows(&mut self, other: Self) {
        self.0.append_rows(other.0);
    }
}

impl<B: Buffers> IntoArrowArray for Words<B> {
    fn data_type() -> DataType {
        DataType::Utf8View
    }

    fn into_array(self) -> ArrayRef {
        Arc::new(B::shared().array(self.0, Vec::new()))
    }
}

/// A string with bytes of its own, written into the batch's [`DATA`]
/// buffer. Made by [`format`](populate_builtins::format()).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Str<B>(pub String, PhantomData<B>);

impl<B> From<String> for Str<B> {
    fn from(s: String) -> Self {
        Str(s, PhantomData)
    }
}

impl<B> From<&str> for Str<B> {
    fn from(s: &str) -> Self {
        Str(s.to_owned(), PhantomData)
    }
}

impl<B: Buffers> Columnar for Str<B> {
    type Columns = Views<B>;
}

/// A string column being built: views, and the batch's own bytes.
#[derive(Debug, Default, Clone)]
pub struct Views<B> {
    views: Vec<u128>,
    data: Vec<u8>,
    _buffers: PhantomData<B>,
}

impl<B> Views<B> {
    /// Room for `n` more strings of `bytes` more bytes in all (strings of
    /// up to 12 bytes need none: they live in their views).
    pub fn reserve_bytes(&mut self, n: usize, bytes: usize) {
        self.views.reserve(n);
        self.data.reserve(bytes);
    }

    /// Strings so far.
    pub fn len(&self) -> usize {
        self.views.len()
    }

    pub fn is_empty(&self) -> bool {
        self.views.is_empty()
    }

    /// The views and the batch's own bytes they point into.
    pub fn into_parts(self) -> (Vec<u128>, Vec<u8>) {
        (self.views, self.data)
    }

    /// A copy of string `i`.
    pub fn get(&self, i: usize) -> String {
        let v = self.views[i];
        let len = v as u32 as usize;
        let bytes = if len <= 12 {
            v.to_le_bytes()[4..4 + len].to_vec()
        } else {
            let at = (v >> 96) as usize;
            self.data[at..at + len].to_vec()
        };
        String::from_utf8(bytes).expect("views hold UTF-8")
    }

    /// Append `s`.
    #[inline]
    pub fn push_str(&mut self, s: &str) {
        let s = s.as_bytes();
        let offset = if s.len() > 12 {
            let offset = u32::try_from(self.data.len()).expect("string data exceeds 4GiB");
            self.data.extend_from_slice(s);
            offset
        } else {
            0
        };
        self.views.push(view(s, DATA, offset));
    }

    /// Append `list[i]` for each `i`: the list's long strings are copied
    /// once, and every row shares them, so a row costs a view rather than
    /// its bytes.
    pub fn push_indexed(&mut self, list: &[Listed], idx: impl Iterator<Item = usize>) {
        let data = &mut self.data;
        let views: Vec<u128> = list.iter().map(|w| w.view_in(data)).collect();
        self.views.extend(idx.map(|i| views[i]));
    }

    /// Append the bytes `data[start..]` (just written) as the next string.
    #[inline(always)]
    fn end_string(&mut self, start: usize) {
        let v = view(&self.data[start..], DATA, start as u32);
        if self.data.len() - start <= 12 {
            self.data.truncate(start);
        }
        self.views.push(v);
    }
}

impl<B> StrSink for Views<B> {
    #[inline(always)]
    fn push_with(&mut self, f: impl FnOnce(&mut StrBuf<'_>)) {
        let start = self.data.len();
        f(&mut StrBuf::appending(&mut self.data));
        self.end_string(start);
    }
}

impl<B> Sink<Str<B>> for Views<B> {
    #[inline]
    fn push(&mut self, s: Str<B>) {
        self.push_str(&s.0);
    }

    fn reserve(&mut self, n: usize) {
        self.views.reserve(n);
    }
}

impl<B> RowCount for Views<B> {
    fn rows(&self) -> usize {
        self.len()
    }
}

impl<B: Buffers> IntoArrowArray for Views<B> {
    fn data_type() -> DataType {
        DataType::Utf8View
    }

    fn into_array(self) -> ArrayRef {
        Arc::new(B::shared().array(self.views, self.data))
    }
}

impl<B> TextField for Views<B> {
    fn text_rows(&self) -> usize {
        self.views.len()
    }

    fn text_bytes(&self) -> usize {
        // per row, as a view may share another's bytes; 12 for the store
        // of a short one
        self.views
            .iter()
            .map(|&v| (v as u32 as usize).max(12))
            .sum()
    }

    #[inline(always)]
    unsafe fn write_field(&self, r: usize, w: &mut Cursor, encoding: Encoding) {
        // SAFETY: `r` is a row; a view's bytes are UTF-8, inline or at its
        // offset in `data` (a column's views are all into [`DATA`]).
        unsafe {
            let v = *self.views.get_unchecked(r);
            let len = v as u32 as usize;
            if len <= 12 {
                let inline: [u8; 12] = (v >> 32).to_le_bytes()[..12].try_into().expect("12");
                if encoding == Encoding::Utf8 || inline.is_ascii() {
                    return w.short(&inline, len);
                }
                return w.text(std::str::from_utf8_unchecked(&inline[..len]), encoding);
            }
            let offset = (v >> 96) as u32 as usize;
            let s = self.data.get_unchecked(offset..offset + len);
            w.text(std::str::from_utf8_unchecked(s), encoding)
        }
    }
}

/// Nullable text written in place.
pub trait PushText {
    /// `s`, or NULL.
    fn push_text(&mut self, s: Option<&str>);

    /// The text `f` writes, or NULL if not `valid`.
    fn push_text_with(&mut self, valid: bool, f: impl FnOnce(&mut StrBuf<'_>));
}

impl<B> PushText for Nullable<Views<B>> {
    #[inline]
    fn push_text(&mut self, s: Option<&str>) {
        self.valid.push(s.is_some());
        match s {
            Some(s) => self.values.push_str(s),
            None => self.values.views.push(0),
        }
    }

    #[inline]
    fn push_text_with(&mut self, valid: bool, f: impl FnOnce(&mut StrBuf<'_>)) {
        self.valid.push(valid);
        if valid {
            self.values.push_with(f);
        } else {
            self.values.views.push(0);
        }
    }
}

/// A string from a fixed list (a distribution's values) with no shared
/// buffer, collected into [`Views`]. Its view is made once, with the list,
/// so a row costs a copy of it (and of the bytes, past 12).
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct Listed {
    s: &'static str,
    /// The view, with buffer offset 0.
    view: u128,
}

impl Listed {
    /// `s`, its view made once here.
    pub fn new(s: &'static str) -> Self {
        Listed {
            s,
            view: view(s.as_bytes(), DATA, 0),
        }
    }

    /// Its view in a column whose own bytes are `data`, its bytes
    /// appended there past 12.
    #[inline(always)]
    fn view_in(self, data: &mut Vec<u8>) -> u128 {
        if self.s.len() <= 12 {
            self.view
        } else {
            let offset = u32::try_from(data.len()).expect("string data exceeds 4GiB");
            data.extend_from_slice(self.s.as_bytes());
            self.view | (offset as u128) << 96
        }
    }
}

impl Columnar for Listed {
    type Columns = Views<Unshared>;
}

impl<B> Sink<Listed> for Views<B> {
    #[inline]
    fn push(&mut self, w: Listed) {
        let v = w.view_in(&mut self.data);
        self.views.push(v);
    }

    fn push_n(&mut self, words: impl Iterator<Item = Listed>) {
        let data = &mut self.data;
        self.views.extend(words.map(|w| w.view_in(data)));
    }

    fn reserve(&mut self, n: usize) {
        self.views.reserve(n);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Array;
    use populate_builtins::{format, range};
    use populate_core::Distribution;
    use std::sync::LazyLock;

    #[derive(Debug, Clone, Copy, PartialEq, Default)]
    struct Test;

    static DICT: LazyLock<(Picks, &'static [u8])> = LazyLock::new(|| {
        let mut d = Dict::new(1);
        let p = Picks::new(&["short", "longer than twelve"], |i| [1, 3][i], &mut d);
        (p, d.leak())
    });
    static SHARED: LazyLock<Shared> = LazyLock::new(|| Shared::new(vec![static_buffer(DICT.1)]));

    impl Buffers for Test {
        fn shared() -> &'static Shared {
            &SHARED
        }
    }

    #[test]
    fn words_and_strs_read_back() {
        let p = &DICT.0;
        let mut w = Words::<Test>::default();
        w.push(Word::new(p.view_of(0)));
        w.push(Word::new(p.view_of(1)));
        w.push(Word::lit(b"lit"));
        let a = w.into_array();
        let a = a.as_any().downcast_ref::<StringViewArray>().unwrap();
        let got: Vec<&str> = a.iter().map(|s| s.unwrap()).collect();
        assert_eq!(got, ["short", "longer than twelve", "lit"]);

        let mut rng = rand::rng();
        let s = format::<_, Str<Test>, _>(range(0u64, 1), |&i, out| {
            out.push_ascii_bytes(b"row number ");
            out.push_u64_padded(i, 3);
        })
        .collect(&mut rng, 3)
        .into_array();
        let s = s.as_any().downcast_ref::<StringViewArray>().unwrap();
        let got: Vec<&str> = s.iter().map(|s| s.unwrap()).collect();
        assert_eq!(got, ["row number 000", "row number 001", "row number 002"]);
    }

    #[test]
    fn picks_by_weight() {
        let p = &DICT.0;
        assert_eq!(p.weight(), 3);
        // a draw scales to a weight unit: unit 0 is value 0, units 1, 2 value 1
        assert_eq!(p.index(1), 0);
        assert_eq!(p.index(2_000_000_000), 1);
        assert_eq!(by_weight(3, |i| [2, 2, 5][i], |i| i), [0, 0, 2, 2, 2]);
    }
}
