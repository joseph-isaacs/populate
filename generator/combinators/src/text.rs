//! The text columns of dsdgen's tables (TPC-DS, DSB), collected as Arrow
//! `Utf8View` and printed as `.dat` fields.
//!
//! [`Str`] and [`Word`] go into [`StrViews`]: a string of up to 12 bytes
//! is stored in its view, longer ones are copied into the column's one
//! data buffer. [`Gmt`] is dsdgen's GMT offset, a `decimal(5,2)` printed
//! with no decimals.

use arrow_array::builder::make_view;
use arrow_array::{ArrayRef, StringViewArray};
use arrow_buffer::{Buffer, ScalarBuffer};
use arrow_schema::DataType;
use populate_builtins::ReadRows;
use populate_core::{Columnar, Decimal as Fixed, Nullable, Sink};
use populate_sink_arrow::IntoArrowArray;
use populate_table::text::{Cursor, Encoding, TextField};
use std::sync::Arc;

/// A string row value. Most text is written in place
/// ([`StrViews::push_str`]); this is the value a row of the column reads
/// back as.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Str(pub String);

/// The columns of [`Str`]: a view per row and the bytes of the long ones.
#[derive(Debug, Default, Clone)]
pub struct StrViews {
    views: Vec<u128>,
    data: Vec<u8>,
}

impl StrViews {
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
        self.views.push(make_view(s, 0, offset));
    }

    /// Append the text `f` writes (UTF-8).
    #[inline]
    pub fn push_with(&mut self, f: impl FnOnce(&mut Vec<u8>)) {
        let start = self.data.len();
        f(&mut self.data);
        let s = &self.data[start..];
        if s.len() <= 12 {
            let v = make_view(s, 0, 0);
            self.data.truncate(start);
            self.views.push(v);
        } else {
            let offset = u32::try_from(start).expect("string data exceeds 4GiB");
            self.views.push(make_view(s, 0, offset));
        }
    }

    /// The view of `s`, its bytes stored once, to append as many rows as
    /// needed with [`push_interned`](Self::push_interned): for columns of a
    /// few distinct values, whose long ones are then not copied per row.
    pub fn intern(&mut self, s: &str) -> Interned {
        let views = self.views.len();
        self.push_str(s);
        let v = self.views.pop().expect("just pushed");
        debug_assert_eq!(self.views.len(), views);
        Interned(v)
    }

    /// Append a row of a string [`intern`](Self::intern)ed in this column.
    ///
    /// # Safety
    /// `s` comes from `intern` on this column: its view points into this
    /// column's data.
    #[inline]
    pub unsafe fn push_interned(&mut self, s: Interned) {
        self.views.push(s.0);
    }

    /// The string of row `i`.
    fn get(&self, i: usize) -> String {
        let v = self.views[i];
        let len = v as u32 as usize;
        let bytes = v.to_le_bytes();
        let s = if len <= 12 {
            &bytes[4..4 + len]
        } else {
            let offset = (v >> 96) as u32 as usize;
            &self.data[offset..offset + len]
        };
        String::from_utf8(s.to_vec()).expect("UTF-8")
    }
}

/// A string's view in one [`StrViews`] column, from
/// [`StrViews::intern`].
#[derive(Debug, Clone, Copy)]
pub struct Interned(u128);

impl Columnar for Str {
    type Columns = StrViews;
}

impl From<&str> for Str {
    fn from(s: &str) -> Self {
        Str(s.to_owned())
    }
}

/// A string from a fixed list (a distribution's values), collected into
/// the same columns as [`Str`].
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct Word(pub &'static str);

impl Columnar for Word {
    type Columns = StrViews;
}

impl Sink<Word> for StrViews {
    #[inline]
    fn push(&mut self, s: Word) {
        self.push_str(s.0);
    }

    fn reserve(&mut self, n: usize) {
        self.views.reserve(n);
    }
}

impl TextField for StrViews {
    fn text_rows(&self) -> usize {
        self.views.len()
    }

    fn text_bytes(&self) -> usize {
        // a short string is written with one 12-byte store; a long one is
        // counted per row, as an interned one is written once per row
        self.views
            .iter()
            .map(|&v| (v as u32 as usize).max(12))
            .sum()
    }

    #[inline(always)]
    unsafe fn write_field(&self, r: usize, w: &mut Cursor, encoding: Encoding) {
        // SAFETY: `r` is a row; a view's bytes are UTF-8, inline or at its
        // offset in `data`.
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

impl Sink<Str> for StrViews {
    #[inline]
    fn push(&mut self, s: Str) {
        self.push_str(&s.0);
    }

    fn reserve(&mut self, n: usize) {
        self.views.reserve(n);
    }
}

impl ReadRows for StrViews {
    type Row = Str;

    fn rows(&self) -> usize {
        self.views.len()
    }

    fn rows_from(&self, start: usize) -> impl Iterator<Item = Str> + '_ {
        (start..self.views.len()).map(|i| Str(self.get(i)))
    }
}

impl IntoArrowArray for StrViews {
    fn data_type() -> DataType {
        DataType::Utf8View
    }

    fn into_array(self) -> ArrayRef {
        let views = ScalarBuffer::from(self.views);
        let buffers: Arc<[Buffer]> = Arc::new([Buffer::from_vec(self.data)]);
        // SAFETY: every view was made by `make_view` over UTF-8 bytes of
        // `data` at its offset (or inline), so the views are valid
        Arc::new(unsafe { StringViewArray::new_unchecked(views, buffers, None) })
    }
}

/// Nullable text written in place.
pub trait PushText {
    /// `s`, or NULL.
    fn push_text(&mut self, s: Option<&str>);

    /// The text `f` writes (UTF-8), or NULL if not `valid`.
    fn push_with(&mut self, valid: bool, f: impl FnOnce(&mut Vec<u8>));
}

impl PushText for Nullable<StrViews> {
    #[inline]
    fn push_text(&mut self, s: Option<&str>) {
        self.valid.push(s.is_some());
        match s {
            Some(s) => self.values.push_str(s),
            None => self.values.views.push(0),
        }
    }

    #[inline]
    fn push_with(&mut self, valid: bool, f: impl FnOnce(&mut Vec<u8>)) {
        self.valid.push(valid);
        if valid {
            self.values.push_with(f);
        } else {
            self.values.views.push(0);
        }
    }
}

/// A GMT offset in whole hours, kept as `decimal(5,2)` and printed in
/// `.dat` files with no decimals (`-5`), as dsdgen does.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct Gmt(pub i32);

/// The column of [`Gmt`]: the offsets as `decimal(5,2)` units.
#[derive(Debug, Clone, Default)]
pub struct GmtCol(Vec<Fixed<5, 2>>);

impl Columnar for Gmt {
    type Columns = GmtCol;
}

impl Sink<Gmt> for GmtCol {
    #[inline]
    fn push(&mut self, g: Gmt) {
        self.0.push(Fixed(i128::from(g.0) * 100));
    }

    fn reserve(&mut self, n: usize) {
        self.0.reserve(n);
    }
}

impl ReadRows for GmtCol {
    type Row = Gmt;

    fn rows(&self) -> usize {
        self.0.len()
    }

    fn rows_from(&self, start: usize) -> impl Iterator<Item = Gmt> + '_ {
        self.0[start..].iter().map(|d| Gmt((d.0 / 100) as i32))
    }
}

impl IntoArrowArray for GmtCol {
    fn data_type() -> DataType {
        <Vec<Fixed<5, 2>>>::data_type()
    }

    fn into_array(self) -> ArrayRef {
        IntoArrowArray::into_array(self.0)
    }
}

impl TextField for GmtCol {
    fn text_rows(&self) -> usize {
        self.0.len()
    }

    fn text_bytes(&self) -> usize {
        TextField::text_bytes(&self.0)
    }

    #[inline]
    unsafe fn write_field(&self, r: usize, w: &mut Cursor, _: Encoding) {
        // SAFETY: as the caller's
        unsafe { w.int((self.0.get_unchecked(r).0 / 100) as i64) }
    }
}
