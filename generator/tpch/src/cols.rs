//! TPC-H's string row types and the `Utf8View` columns they collect into.
//!
//! Every string column is a `StringViewArray` over the shared buffers of
//! [`Text`]: a [`Word`] is a precomputed view (a distribution value in
//! [`DICT`](crate::text::DICT), or a literal of up to 12 bytes), a
//! [`Comment`] a span of the text pool, and [`Bytes`] a string with bytes
//! of its own (names, addresses, phone numbers).
//!
//! The column types say they are `Utf8View`, which is where the tables'
//! schemas get their string types from.

use crate::text::{POOL_SIZE, Text, Views, view};
use arrow_array::ArrayRef;
use arrow_schema::DataType;
use populate_builtins::{ReadRows, RowRefs};
use populate_core::{Columnar, Columns, Distribution, Sink};
use populate_sink_arrow::IntoArrowArray;
use rand::Rng;
use std::sync::Arc;

/// A string that is a precomputed view.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub(crate) struct Word(pub(crate) u128);

impl Word {
    /// A literal of up to 12 bytes.
    pub(crate) fn lit(s: &[u8]) -> Self {
        debug_assert!(s.len() <= 12);
        Word(view(s, 0, 0))
    }
}

/// Columns of [`Word`]s: their views.
#[derive(Debug, Default)]
pub(crate) struct Words(Vec<u128>);

impl Columnar for Word {
    type Columns = Words;
}

impl Sink<Word> for Words {
    #[inline(always)]
    fn push(&mut self, w: Word) {
        self.0.push(w.0);
    }

    fn reserve(&mut self, n: usize) {
        self.0.reserve(n);
    }

    fn push_n(&mut self, values: impl Iterator<Item = Word>) {
        self.0.extend(values.map(|w| w.0));
    }
}

impl ReadRows for Words {
    type Row = Word;

    fn rows(&self) -> usize {
        self.0.len()
    }

    fn rows_from(&self, start: usize) -> impl Iterator<Item = Word> + '_ {
        self.0[start..].iter().map(|&v| Word(v))
    }
}

impl IntoArrowArray for Words {
    fn data_type() -> DataType {
        DataType::Utf8View
    }

    fn into_array(self) -> ArrayRef {
        Arc::new(Text::get().array(self.0, Vec::new()))
    }
}

/// A comment: `len` bytes of the text pool from `off`.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub(crate) struct Comment {
    pub(crate) off: u32,
    pub(crate) len: u32,
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
        use populate_combinators::lehmer::bounded;
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
pub(crate) struct Comments(Vec<Comment>);

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

impl ReadRows for Comments {
    type Row = Comment;

    fn rows(&self) -> usize {
        self.0.len()
    }

    fn rows_from(&self, start: usize) -> impl Iterator<Item = Comment> + '_ {
        self.0[start..].iter().copied()
    }
}

impl IntoArrowArray for Comments {
    fn data_type() -> DataType {
        DataType::Utf8View
    }

    fn into_array(self) -> ArrayRef {
        let t = Text::get();
        let views = self.0.iter().map(|c| t.pool_view(c.off, c.len)).collect();
        Arc::new(t.array(views, Vec::new()))
    }
}

/// A string with bytes of its own.
#[derive(Debug, Clone, PartialEq, Default)]
pub(crate) struct Bytes(pub(crate) Vec<u8>);

impl Columnar for Bytes {
    type Columns = Views;
}

impl Sink<Bytes> for Views {
    fn push(&mut self, b: Bytes) {
        let start = self.data.len();
        self.data.extend_from_slice(&b.0);
        self.end_string(start);
    }

    fn reserve(&mut self, n: usize) {
        self.views.reserve(n);
    }
}

impl IntoArrowArray for Views {
    fn data_type() -> DataType {
        DataType::Utf8View
    }

    fn into_array(self) -> ArrayRef {
        Arc::new(self.finish(Text::get()))
    }
}

/// A string written by `f` from each row of `source`. See [`format()`].
#[derive(Debug, Clone)]
pub(crate) struct Format<D, F> {
    source: D,
    f: F,
}

/// `f(row, out)` appends each row's string to `out`. Collected, it writes
/// straight into the column's bytes.
pub(crate) fn format<D, F>(source: D, f: F) -> Format<D, F>
where
    D: Distribution,
    Columns<D::Item>: RowRefs,
    F: for<'a> Fn(<Columns<D::Item> as RowRefs>::Ref<'a>, &mut Vec<u8>),
{
    Format { source, f }
}

impl<D, F> Distribution for Format<D, F>
where
    D: Distribution,
    Columns<D::Item>: RowRefs,
    F: for<'a> Fn(<Columns<D::Item> as RowRefs>::Ref<'a>, &mut Vec<u8>),
{
    type Item = Bytes;

    fn sample(&mut self, rng: &mut impl Rng) -> Bytes {
        let src = self.source.collect(rng, 1);
        let mut out = Vec::new();
        (self.f)(src.row_refs().next().expect("one row"), &mut out);
        Bytes(out)
    }

    fn collect_into(&mut self, cols: &mut Views, rng: &mut impl Rng, n: usize) {
        let src = self.source.collect(rng, n);
        cols.views.reserve(n);
        for row in src.row_refs() {
            let start = cols.data.len();
            (self.f)(row, &mut cols.data);
            cols.end_string(start);
        }
    }

    fn skip(&mut self, n: u64) {
        self.source.skip(n);
    }
}
