//! SSB's string row types and the `Utf8View` columns they collect into.
//!
//! - a [`Word`] is a precomputed view into the shared dictionary
//!   ([`Dict`]): every string that is not unique to a row;
//! - [`Bytes`] is a string with bytes of its own (names, addresses, phone
//!   numbers), written by [`format()`] straight into the column's buffer;
//! - a [`Shared`] is a view into buffers the column is given
//!   ([`SharedCols::buffers`]): `lineorder_flat`'s names, addresses and
//!   phone numbers, which point into the dimension tables' text.
//!
//! The column types say they are `Utf8View`, which is where the tables'
//! schemas get their string types from.

use crate::dict::{Dict, Views};
use arrow_array::{ArrayRef, StringViewArray};
use arrow_buffer::{Buffer, ScalarBuffer};
use arrow_schema::DataType;
use populate_builtins::ReadRows;
use populate_core::{ChildCols, Children, Columnar, Columns, Distribution, Sink};
use populate_sink_arrow::IntoArrowArray;
use rand::Rng;
use std::marker::PhantomData;
use std::sync::Arc;

/// A string that is a view into the dictionary.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub(crate) struct Word(pub(crate) u128);

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
        Arc::new(Dict::get().array(self.0, Vec::new()))
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
        Arc::new(self.finish(Dict::get()))
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
    F: Fn(D::Item, &mut Vec<u8>),
{
    Format { source, f }
}

impl<D, F> Distribution for Format<D, F>
where
    D: Distribution,
    Columns<D::Item>: ReadRows<Row = D::Item>,
    F: Fn(D::Item, &mut Vec<u8>),
{
    type Item = Bytes;

    fn sample(&mut self, rng: &mut impl Rng) -> Bytes {
        let mut out = Vec::new();
        (self.f)(self.source.sample(rng), &mut out);
        Bytes(out)
    }

    fn collect_into(&mut self, cols: &mut Views, rng: &mut impl Rng, n: usize) {
        let src = self.source.collect(rng, n);
        cols.views.reserve(n);
        for row in src.rows_from(0) {
            let start = cols.data.len();
            (self.f)(row, &mut cols.data);
            cols.end_string(start);
        }
    }

    fn skip(&mut self, n: u64) {
        self.source.skip(n);
    }
}

/// A view into the buffers of the [`SharedCols`] it is pushed to.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub(crate) struct Shared(pub(crate) u128);

/// Columns of [`Shared`] views, and the buffers they point into. Whoever
/// pushes the views sets the buffers (they are not part of a row).
#[derive(Debug, Default)]
pub(crate) struct SharedCols {
    views: Vec<u128>,
    pub(crate) buffers: Arc<[Buffer]>,
}

impl Columnar for Shared {
    type Columns = SharedCols;
}

impl Sink<Shared> for SharedCols {
    #[inline(always)]
    fn push(&mut self, s: Shared) {
        self.views.push(s.0);
    }

    fn reserve(&mut self, n: usize) {
        self.views.reserve(n);
    }

    fn push_n(&mut self, values: impl Iterator<Item = Shared>) {
        self.views.extend(values.map(|s| s.0));
    }
}

impl ReadRows for SharedCols {
    type Row = Shared;

    fn rows(&self) -> usize {
        self.views.len()
    }

    fn rows_from(&self, start: usize) -> impl Iterator<Item = Shared> + '_ {
        self.views[start..].iter().map(|&v| Shared(v))
    }
}

impl IntoArrowArray for SharedCols {
    fn data_type() -> DataType {
        DataType::Utf8View
    }

    fn into_array(self) -> ArrayRef {
        // Only `flat::Text::gather` pushes views here, and it sets the
        // buffers of the column they came from (a full check would cost
        // as much as the join)
        debug_assert!(
            self.views.iter().all(|&v| {
                let len = v as u32;
                len <= 12 || ((v >> 64) as u32 as usize) < self.buffers.len()
            }),
            "shared views without their buffers"
        );
        // SAFETY: every view comes from a valid `StringViewArray` over
        // these buffers (`flat::Text`), in the same order
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
    Columns<Out>: ReadRows<Row = Out>,
    F: Fn(&Columns<D::Item>, &mut Columns<Out>),
{
    type Item = Children<Out>;

    /// One source row, collected (so read the way `collect` reads it).
    fn sample(&mut self, rng: &mut impl Rng) -> Children<Out> {
        let src = self.source.collect(rng, 1);
        let mut out = Columns::<Out>::default();
        (self.f)(&src, &mut out);
        Children(out.rows_from(0).collect())
    }

    fn collect_into(&mut self, cols: &mut ChildCols<Columns<Out>>, rng: &mut impl Rng, n: usize) {
        let src = self.source.collect(rng, n);
        (self.f)(&src, &mut cols.values);
        cols.sources += n;
    }

    fn skip(&mut self, n: u64) {
        self.source.skip(n);
    }
}
