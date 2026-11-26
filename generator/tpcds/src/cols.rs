//! [`Str`]: TPC-DS text, collected as an Arrow `Utf8View` column.
//!
//! A string of up to 12 bytes is stored in its view; longer ones are
//! copied into the column's one data buffer. The column type says it is
//! `Utf8View`, which is where the tables' schemas get their text type from.

use arrow_array::builder::make_view;
use arrow_array::{ArrayRef, StringViewArray};
use arrow_buffer::{Buffer, ScalarBuffer};
use arrow_schema::DataType;
use populate_builtins::ReadRows;
use populate_core::{Columnar, Nullable, Sink};
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

impl Columnar for Str {
    type Columns = StrViews;
}

/// A string from a fixed list (a distribution's values), collected into
/// the same columns as [`Str`]. Its view is made once, with the list, so
/// a row costs a copy of it (and of the bytes, past 12).
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct Word {
    s: &'static str,
    /// The view, with buffer offset 0.
    view: u128,
}

impl Word {
    pub fn new(s: &'static str) -> Self {
        Word {
            s,
            view: make_view(s.as_bytes(), 0, 0),
        }
    }
}

impl Columnar for Word {
    type Columns = StrViews;
}

impl Word {
    /// Its view in a column whose data buffer is `data`, its bytes
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

impl StrViews {
    /// Append `list[i]` for each `i`: the list's long words are copied
    /// once, and every row shares them, as a row of a fixed list costs a
    /// view rather than its bytes.
    pub fn push_indexed(&mut self, list: &[Word], idx: impl Iterator<Item = usize>) {
        let data = &mut self.data;
        let views: Vec<u128> = list.iter().map(|w| w.view_in(data)).collect();
        self.views.extend(idx.map(|i| views[i]));
    }
}

impl Sink<Word> for StrViews {
    #[inline]
    fn push(&mut self, w: Word) {
        let v = w.view_in(&mut self.data);
        self.views.push(v);
    }

    fn push_n(&mut self, words: impl Iterator<Item = Word>) {
        let data = &mut self.data;
        self.views.extend(words.map(|w| w.view_in(data)));
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
pub(crate) trait PushText {
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
