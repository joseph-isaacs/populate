//! Arrow output for the `VecBackend`.
//!
//! Collection is unchanged: data is generated column by column into plain
//! `Vec`s (which is all an Arrow `PrimitiveBuilder` is internally). At the end
//! the sink HList is converted into Arrow arrays:
//!
//! | sink                     | Arrow array                                  |
//! |--------------------------|----------------------------------------------|
//! | `Vec<T>` (numeric)       | `PrimitiveArray`, buffer moved, no copy      |
//! | `Vec<bool>`              | `BooleanArray` (bit-packed)                  |
//! | `Vec<usize>`             | `UInt64Array`                                |
//! | `StrCol` (strings)       | `StringArray`, both buffers moved, no copy   |
//! | `ListCol<offsets, V>`    | `ListArray` with V's columns as its values;  |
//! |                          | several columns become a `StructArray`       |
//! | top-level HList          | `RecordBatch` (columns `c0`, `c1`, ...)      |
//!
//! So `list<(i32, list<i64>)>` becomes `List<Struct<c0: Int32, c1: List<Int64>>>`.
//! Nothing goes through a per-value `dyn ArrayBuilder` call.
//!
//! ## `ArrowBackend`: generate straight into the final Arrow buffers
//!
//! With `VecBackend` list offsets are `Vec<usize>` end offsets, so building a
//! `ListArray` copies them into a new `i32` buffer with a leading 0.
//! `ArrowBackend` stores offsets as [`ArrowOffsets`] instead: already `i32`,
//! already starting at 0. Combined with exact up-front `reserve`s, a
//! `list<numeric>` column is written into exactly two allocations (offsets and
//! values) and both are moved into the `ListArray` as-is. `Vec<bool>` and
//! `Vec<usize>` columns are still converted (bit-packing / widening).

use arrow_array::types::{
    Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type,
    UInt32Type, UInt64Type,
};
use arrow_array::{
    Array, ArrayRef, ArrowPrimitiveType, BooleanArray, ListArray, PrimitiveArray, RecordBatch,
    StringArray, StructArray, UInt64Array,
};
use arrow_buffer::{ArrowNativeType, Buffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{ArrowError, Field, Fields, Schema, SchemaRef};
use populate_core::hlist::{HCons, HNil};
use populate_core::string::StrCol;
use populate_core::traits::{Backend, DefaultSink, Distribution, ListCol, Sink};
use rand::Rng;
use std::sync::Arc;

// ============================================================================
// ArrowBackend — offsets in Arrow layout from the start
// ============================================================================

/// Backend whose sinks are already in Arrow memory layout, so turning the
/// collected sink into a `RecordBatch` moves buffers instead of copying them.
pub struct ArrowBackend;

impl Backend for ArrowBackend {
    type Prim<T> = Vec<T>;
    type Offsets = ArrowOffsets;
}

/// List offsets in Arrow `ListArray` layout: `i32`, `n + 1` entries, first is 0.
///
/// Starts empty (no allocation); the leading 0 is written together with the
/// first reserve/push, so `reserve(n)` sizes the buffer for all `n + 1`
/// entries in one allocation.
#[derive(Debug, Default, PartialEq)]
pub struct ArrowOffsets(Vec<i32>);

impl ArrowOffsets {
    /// The offsets buffer, including the leading 0 (empty if nothing written).
    pub fn as_slice(&self) -> &[i32] {
        &self.0
    }

    fn start(&mut self, additional: usize) {
        if self.0.is_empty() {
            self.0.reserve(additional + 1);
            self.0.push(0);
        }
    }

    fn into_vec(mut self) -> Vec<i32> {
        self.start(0);
        self.0
    }
}

impl Sink<usize> for ArrowOffsets {
    fn push(&mut self, end: usize) {
        self.start(0);
        self.0.push(
            i32::try_from(end)
                .expect("list values exceed i32 offsets; LargeList not supported yet"),
        );
    }

    fn reserve(&mut self, additional: usize) {
        if self.0.is_empty() {
            self.start(additional);
        } else {
            self.0.reserve(additional);
        }
    }

    fn push_n(&mut self, ends: impl Iterator<Item = usize>) {
        self.start(0);
        self.0.extend(ends.map(|end| {
            i32::try_from(end).expect("list values exceed i32 offsets; LargeList not supported yet")
        }));
    }
}

// ============================================================================
// Single column -> ArrayRef
// ============================================================================

/// A sink that becomes exactly one Arrow array.
pub trait IntoArrowArray {
    fn into_array(self) -> ArrayRef;
}

/// Rust native type -> Arrow primitive type (zero-copy buffer move).
pub trait ArrowNative: ArrowNativeType {
    type Arrow: ArrowPrimitiveType<Native = Self>;
}

macro_rules! impl_arrow_native {
    ($($t:ty => $a:ty),* $(,)?) => {
        $(impl ArrowNative for $t { type Arrow = $a; })*
    };
}

impl_arrow_native!(
    i8 => Int8Type, i16 => Int16Type, i32 => Int32Type, i64 => Int64Type,
    u8 => UInt8Type, u16 => UInt16Type, u32 => UInt32Type, u64 => UInt64Type,
    f32 => Float32Type, f64 => Float64Type,
);

impl<T: ArrowNative> IntoArrowArray for Vec<T> {
    fn into_array(self) -> ArrayRef {
        Arc::new(PrimitiveArray::<T::Arrow>::new(self.into(), None))
    }
}

impl IntoArrowArray for Vec<bool> {
    fn into_array(self) -> ArrayRef {
        Arc::new(BooleanArray::from(self))
    }
}

impl IntoArrowArray for Vec<usize> {
    fn into_array(self) -> ArrayRef {
        Arc::new(UInt64Array::from_iter_values(
            self.into_iter().map(|v| v as u64),
        ))
    }
}

/// `StrCol` -> `StringArray`: offsets and bytes are moved, not copied.
impl IntoArrowArray for StrCol {
    fn into_array(self) -> ArrayRef {
        let (offsets, data) = self.into_parts();
        // SAFETY: `StrCol` only accepts UTF-8 (`&str`, `char`, checked ASCII)
        // and its offsets start at 0 and never decrease.
        let offsets = unsafe { OffsetBuffer::new_unchecked(ScalarBuffer::from(offsets)) };
        let array = unsafe { StringArray::new_unchecked(offsets, Buffer::from_vec(data), None) };
        Arc::new(array)
    }
}

/// `ListCol` -> `ListArray`. Offsets are stored as end offsets (`n` entries);
/// Arrow wants `n + 1` starting at 0, as i32.
impl<V: IntoArrowColumns> IntoArrowArray for ListCol<Vec<usize>, V> {
    fn into_array(self) -> ArrayRef {
        let mut offsets = Vec::with_capacity(self.offsets.len() + 1);
        offsets.push(0i32);
        offsets.extend(self.offsets.iter().map(|&o| {
            i32::try_from(o).expect("list values exceed i32 offsets; LargeList not supported yet")
        }));

        list_array(offsets, self.values)
    }
}

/// `ArrowBackend` list: the offsets buffer is moved into the array, no copy.
impl<V: IntoArrowColumns> IntoArrowArray for ListCol<ArrowOffsets, V> {
    fn into_array(self) -> ArrayRef {
        list_array(self.offsets.into_vec(), self.values)
    }
}

fn list_array<V: IntoArrowColumns>(offsets: Vec<i32>, values: V) -> ArrayRef {
    let values = columns_to_value_array(values);
    let field = Arc::new(Field::new_list_field(values.data_type().clone(), false));
    Arc::new(ListArray::new(
        field,
        OffsetBuffer::new(offsets.into()),
        values,
        None,
    ))
}

/// A sink HList as one array: a single column is returned as is; several
/// columns (a tuple) become a `StructArray` with fields `c0, c1, ...`.
pub fn columns_to_value_array<V: IntoArrowColumns>(values: V) -> ArrayRef {
    let mut cols = Vec::with_capacity(V::LEN);
    values.into_arrays(&mut cols);
    if cols.len() == 1 {
        return cols.pop().expect("one column");
    }
    Arc::new(StructArray::new(column_fields(&cols), cols, None))
}

/// Non-nullable fields `c0, c1, ...` typed after `cols`.
fn column_fields(cols: &[ArrayRef]) -> Fields {
    cols.iter()
        .enumerate()
        .map(|(i, a)| Field::new(format!("c{i}"), a.data_type().clone(), false))
        .collect()
}

// ============================================================================
// HList of sinks -> Vec<ArrayRef>
// ============================================================================

/// A sink HList that becomes a sequence of Arrow arrays, one per column.
pub trait IntoArrowColumns {
    /// The number of columns, so the column `Vec` is allocated once.
    const LEN: usize;
    fn into_arrays(self, out: &mut Vec<ArrayRef>);
}

impl IntoArrowColumns for HNil {
    const LEN: usize = 0;
    fn into_arrays(self, _out: &mut Vec<ArrayRef>) {}
}

impl<H: IntoArrowArray, T: IntoArrowColumns> IntoArrowColumns for HCons<H, T> {
    const LEN: usize = 1 + T::LEN;
    fn into_arrays(self, out: &mut Vec<ArrayRef>) {
        out.push(self.0.into_array());
        self.1.into_arrays(out);
    }
}

/// Top-level sink HList -> `RecordBatch` with columns `c0, c1, ...`.
pub fn into_record_batch<S: IntoArrowColumns>(sink: S) -> Result<RecordBatch, ArrowError> {
    let mut cols = Vec::with_capacity(S::LEN);
    sink.into_arrays(&mut cols);
    let schema = Schema::new(column_fields(&cols));
    RecordBatch::try_new(Arc::new(schema), cols)
}

/// [`into_record_batch`] against a schema made once (for example the first
/// batch's `schema()`), so a stream of batches shares one `Arc<Schema>`
/// instead of building names and fields per batch. The columns are checked
/// against it as by `RecordBatch::try_new`.
pub fn into_record_batch_with<S: IntoArrowColumns>(
    schema: &SchemaRef,
    sink: S,
) -> Result<RecordBatch, ArrowError> {
    let mut cols = Vec::with_capacity(S::LEN);
    sink.into_arrays(&mut cols);
    RecordBatch::try_new(schema.clone(), cols)
}

// ============================================================================
// DistributionArrow — collect straight to a RecordBatch
// ============================================================================

pub trait DistributionArrow<B: Backend>: Distribution<B> {
    /// Collect `n` rows column by column and return them as a `RecordBatch`.
    /// No sink type to spell out: it's derived from `Self::Item`.
    fn collect_record_batch(
        &mut self,
        rng: &mut impl Rng,
        n: usize,
    ) -> Result<RecordBatch, ArrowError>
    where
        <Self::Item as DefaultSink<B>>::Sink: Default + IntoArrowColumns,
    {
        let sink = self.collect_into(Default::default(), rng, n);
        into_record_batch(sink)
    }

    /// [`collect_record_batch`](Self::collect_record_batch) with a schema
    /// reused across batches: see [`into_record_batch_with`].
    fn collect_record_batch_with(
        &mut self,
        schema: &SchemaRef,
        rng: &mut impl Rng,
        n: usize,
    ) -> Result<RecordBatch, ArrowError>
    where
        <Self::Item as DefaultSink<B>>::Sink: Default + IntoArrowColumns,
    {
        let sink = self.collect_into(Default::default(), rng, n);
        into_record_batch_with(schema, sink)
    }
}

impl<B: Backend, D: Distribution<B>> DistributionArrow<B> for D {}
