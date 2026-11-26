//! SSB's string row types and the `Utf8View` columns they collect into.
//!
//! - a [`Word`] is a precomputed view into the shared dictionary
//!   ([`Dict`](crate::dict::Dict)): every string that is not unique to a
//!   row;
//! - [`Bytes`] is a string with bytes of its own (names, addresses, phone
//!   numbers), written by [`format`](populate_builtins::format()) straight
//!   into the column's buffer;
//! - a [`Shared`] is a view into buffers the column is given
//!   ([`SharedCols::buffers`]): `lineorder_flat`'s names, addresses and
//!   phone numbers, which point into the dimension tables' text.
//!
//! The column types say they are `Utf8View`, which is where the tables'
//! schemas get their string types from.

use crate::dict::Ssb;
#[cfg(feature = "formats")]
use arrow_array::{ArrayRef, StringViewArray};
#[cfg(feature = "formats")]
use arrow_buffer::{Buffer, ScalarBuffer};
#[cfg(not(feature = "formats"))]
type Buffer = Vec<u8>;
#[cfg(feature = "formats")]
use arrow_schema::DataType;
use populate_combinators::views::{self, DATA};
use populate_core::{ChildCols, Children, Columnar, Columns, Distribution, RowCount, Sink};
#[cfg(feature = "formats")]
use populate_sink_arrow::IntoArrowArray;
use rand::Rng;
use std::marker::PhantomData;
use std::str::from_utf8;
use std::sync::Arc;

/// A string that is a view into the dictionary.
pub type Word = views::Word<Ssb>;

/// Columns of [`Word`]s.
pub type Words = views::Words<Ssb>;

/// A string with bytes of its own.
pub type Bytes = views::Str<Ssb>;

/// A column of [`Bytes`].
pub type Views = views::Views<Ssb>;

/// A view into the buffers of a [`SharedCols`]. Only [`SharedCols`]
/// hands them out (reading its rows back).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Shared(u128);

/// One text column of a whole dimension table, made once: its views and
/// the buffers they point into, every byte ASCII.
#[derive(Debug, Default)]
pub(crate) struct SharedText {
    views: Vec<u128>,
    buffers: Arc<[Buffer]>,
}

impl SharedText {
    /// Per-job columns, end to end: job `k`'s bytes become buffer `k`, and
    /// its views (which name buffer `DATA`) are pointed at it.
    ///
    /// # Panics
    ///
    /// If any string is not ASCII, or a view is not within its bytes: what
    /// makes every in-bounds view of these buffers valid UTF-8.
    pub(crate) fn new(parts: Vec<Views>) -> SharedText {
        let mut views = Vec::with_capacity(parts.iter().map(|p| p.len()).sum());
        let mut buffers = Vec::with_capacity(parts.len());
        for (k, p) in parts.into_iter().enumerate() {
            let (p_views, data) = p.into_parts();
            assert!(data.is_ascii(), "dimension text is ASCII");
            let k = u128::from(u32::try_from(k).expect("buffers fit in u32"));
            for v in p_views {
                let len = v as u32;
                if len <= 12 {
                    let inline = v.to_le_bytes();
                    assert!(
                        inline[4..4 + len as usize].is_ascii(),
                        "dimension text is ASCII"
                    );
                    views.push(v);
                } else {
                    let (buffer, offset) = ((v >> 64) as u32, (v >> 96) as u32);
                    assert!(
                        buffer == DATA && offset as usize + len as usize <= data.len(),
                        "a view within its job's bytes"
                    );
                    // the buffer index is bits 64..96 of a long view
                    views.push(v & !(u128::from(u32::MAX) << 64) | k << 64);
                }
            }
            #[cfg(feature = "formats")]
            buffers.push(Buffer::from_vec(data));
            #[cfg(not(feature = "formats"))]
            buffers.push(data);
        }
        SharedText {
            views,
            buffers: Arc::from(buffers),
        }
    }

    /// The rows keyed `rows` (1-based), pushed onto `cols`, which then
    /// point at this column's buffers.
    pub(crate) fn gather(&self, rows: &[i32], cols: &mut SharedCols) {
        if !Arc::ptr_eq(&cols.buffers, &self.buffers) {
            assert!(cols.views.is_empty(), "views into one text's buffers only");
            cols.buffers = self.buffers.clone();
        }
        cols.views
            .extend(rows.iter().map(|&k| k as usize - 1).map(|r| self.views[r]));
    }
}

/// Columns of [`Shared`] views, and the buffers they point into: those of
/// the [`SharedText`] the views were gathered from.
#[derive(Debug, Default)]
pub struct SharedCols {
    views: Vec<u128>,
    buffers: Arc<[Buffer]>,
}

impl SharedCols {
    /// Read a string without copying shared dimension buffers.
    pub fn with_str<R>(&self, i: usize, f: impl FnOnce(&str) -> R) -> R {
        let v = self.views[i];
        let len = v as u32 as usize;
        if len <= 12 {
            let bytes = v.to_le_bytes();
            f(from_utf8(&bytes[4..4 + len]).expect("ASCII view"))
        } else {
            let buffer = (v >> 64) as u32 as usize;
            let offset = (v >> 96) as u32 as usize;
            f(from_utf8(&self.buffers[buffer][offset..offset + len]).expect("ASCII view"))
        }
    }
}

impl Columnar for Shared {
    type Columns = SharedCols;
}

impl Sink<Shared> for SharedCols {
    /// # Panics
    ///
    /// If `s` is not within these columns' buffers (a row of columns over
    /// other ones).
    fn push(&mut self, s: Shared) {
        let len = s.0 as u32 as usize;
        if len > 12 {
            let (buffer, offset) = ((s.0 >> 64) as u32 as usize, (s.0 >> 96) as u32 as usize);
            assert!(
                self.buffers
                    .get(buffer)
                    .is_some_and(|b| offset + len <= b.len()),
                "a shared view outside these columns' buffers"
            );
        }
        self.views.push(s.0);
    }

    fn reserve(&mut self, n: usize) {
        self.views.reserve(n);
    }
}

impl RowCount for SharedCols {
    fn rows(&self) -> usize {
        self.views.len()
    }
}

#[cfg(feature = "formats")]
impl IntoArrowArray for SharedCols {
    fn data_type() -> DataType {
        DataType::Utf8View
    }

    fn into_array(self) -> ArrayRef {
        // SAFETY: every view is from a `SharedText`, which checked each is
        // inline ASCII or within an ASCII buffer (`SharedText::new`), and
        // is within these buffers: gathered from the text they are
        // (`SharedText::gather`), or pushed with that checked (`push`). So
        // each is valid UTF-8, in bounds.
        Arc::new(unsafe {
            StringViewArray::new_unchecked(ScalarBuffer::from(self.views), self.buffers, None)
        })
    }
}

/// Child rows that `f` writes for a block of `source` rows. See
/// [`children_of`].
#[derive(Debug, Clone)]
pub(crate) struct ChildrenOf<D, F, Out> {
    source: D,
    f: F,
    _out: PhantomData<fn() -> Out>,
}

/// Child rows: `f(source_columns, children)` appends the children of a
/// block of `source` rows, however many each has. Like
/// [`flat_map`](populate_builtins::flat_map), but `f` sees the whole block,
/// so it can write a column at a time.
pub(crate) fn children_of<D, Out, F>(source: D, f: F) -> ChildrenOf<D, F, Out>
where
    D: Distribution,
    Out: Columnar,
    F: Fn(&Columns<D::Item>, &mut Columns<Out>),
{
    ChildrenOf {
        source,
        f,
        _out: PhantomData,
    }
}

impl<D, F, Out> Distribution for ChildrenOf<D, F, Out>
where
    D: Distribution,
    Out: Columnar,
    F: Fn(&Columns<D::Item>, &mut Columns<Out>),
{
    type Item = Children<Out>;

    fn collect_into(&mut self, cols: &mut ChildCols<Columns<Out>>, rng: &mut impl Rng, n: usize) {
        let src = self.source.collect(rng, n);
        (self.f)(&src, &mut cols.values);
        cols.sources += n;
    }

    fn skip(&mut self, n: u64) {
        self.source.skip(n);
    }
}
