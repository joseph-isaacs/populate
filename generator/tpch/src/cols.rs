//! TPC-H's string row types and the `Utf8View` columns they collect into.
//!
//! Every string column is a `StringViewArray` over the shared buffers of
//! [`Text`]: a [`Word`] is a precomputed view (a distribution value in
//! [`DICT`](crate::text::DICT), or a literal of up to 12 bytes), a
//! [`Comment`] a span of the text pool, and [`Bytes`] a string with bytes
//! of its own (names, addresses, phone numbers). Words and bytes are
//! `populate_combinators::views`' columns; comments are TPC-H's own.
//!
//! The column types say they are `Utf8View`, which is where the tables'
//! schemas get their string types from.

use crate::text::{POOL_SIZE, Text, Tpch};
#[cfg(feature = "formats")]
use arrow_array::ArrayRef;
#[cfg(feature = "formats")]
use arrow_schema::DataType;
use populate_combinators::lehmer::bounded;
use populate_combinators::views;
use populate_core::RowCount;
use populate_core::{Columnar, Sink};
#[cfg(feature = "formats")]
use populate_sink_arrow::IntoArrowArray;
#[cfg(feature = "formats")]
use std::sync::Arc;

/// A string that is a precomputed view into [`Text`]'s buffers.
pub type Word = views::Word<Tpch>;

/// A string with bytes of its own (names, addresses, phone numbers).
pub type Bytes = views::Str<Tpch>;

/// A comment: `len` bytes of the text pool from `off`.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct Comment {
    pub off: u32,
    pub len: u32,
}

/// dbgen's comments of average length `avg` (`RandomText`: two draws per
/// comment, offset then length).
#[derive(Debug, Clone, Copy)]
pub(crate) struct CommentLen {
    min: i32,
    max: i32,
}

impl CommentLen {
    pub(crate) fn new(avg: f64) -> Self {
        CommentLen {
            min: (avg * 0.4) as i32,
            max: (avg * 1.6) as i32,
        }
    }

    /// The comment draws `x0`, `x1` make.
    #[inline(always)]
    pub(crate) fn make(&self, x0: u32, x1: u32) -> Comment {
        Comment {
            off: bounded(x0, 0, POOL_SIZE as i32 - self.max) as u32,
            len: bounded(x1, self.min, self.max) as u32,
        }
    }
}

/// Columns of [`Comment`]s. Their views are made when the column becomes
/// an array: each reads the pool at a random offset (a cache miss), and in
/// a pass of their own the loads are independent and overlap.
#[derive(Debug, Default)]
pub struct Comments(pub Vec<Comment>);

impl Columnar for Comment {
    type Columns = Comments;
}

impl Sink<Comment> for Comments {
    #[inline(always)]
    fn push(&mut self, c: Comment) {
        self.0.push(c);
    }

    fn reserve(&mut self, n: usize) {
        self.0.reserve(n);
    }

    fn push_n(&mut self, values: impl Iterator<Item = Comment>) {
        self.0.extend(values);
    }
}

impl RowCount for Comments {
    fn rows(&self) -> usize {
        self.0.len()
    }
}

#[cfg(feature = "formats")]
impl IntoArrowArray for Comments {
    fn data_type() -> DataType {
        DataType::Utf8View
    }

    fn into_array(self) -> ArrayRef {
        let t = Text::get();
        let views = self.0.iter().map(|c| t.pool_view(c.off, c.len)).collect();
        Arc::new(t.shared.array(views, Vec::new()))
    }
}

impl Comment {
    /// Borrow the comment text from the shared pool.
    pub fn as_str(&self) -> &'static str {
        &Text::get().pool[self.off as usize..(self.off + self.len) as usize]
    }
}
