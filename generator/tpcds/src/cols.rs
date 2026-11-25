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
        let buffers = vec![Buffer::from_vec(self.data)];
        // SAFETY: every view was made by `make_view` over UTF-8 bytes of
        // `data` at its offset (or inline), so the views are valid
        Arc::new(unsafe { StringViewArray::new_unchecked(views, buffers, None) })
    }
}

/// Nullable text written in place.
pub(crate) trait PushText {
    /// `s`, or NULL.
    fn push_text(&mut self, s: Option<&str>);
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
}
