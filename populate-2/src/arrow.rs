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
//! | `StrCol`                 | `StringArray`, offsets + bytes moved         |
//! | `NullableCol<S>`         | `S`'s array, validity bitmap moved as nulls  |
//! | `DictCol<T>`             | `DictionaryArray<UInt32>`, codes moved       |
//! | `ListCol<offsets, V>`    | `ListArray` with V's columns as its values;  |
//! |                          | several columns become a `StructArray`       |
//! | `FixedCols<S, N>`        | `StructArray` (`f0..`), see [`flatten_named`] |
//! | top-level HList          | `RecordBatch` (columns `c0`, `c1`, ...)      |
//!
//! So `list<(i32, list<i64>)>` becomes `List<Struct<c0: Int32, c1: List<Int64>>>`.
//! Nothing goes through a per-value `dyn ArrayBuilder` call.

use crate::column::{DictCol, NullableCol, StrCol};
use crate::fixed::FixedCols;
use crate::hlist::{HCons, HNil};
use crate::traits::{DefaultSink, Distribution, ListCol, VecBackend};
use arrow_array::types::{
    Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type,
    UInt32Type, UInt64Type,
};
use arrow_array::{
    Array, ArrayRef, ArrowPrimitiveType, BooleanArray, DictionaryArray, Int64Array, ListArray,
    PrimitiveArray, RecordBatch, StringArray, StructArray, TimestampNanosecondArray, UInt32Array,
    UInt64Array, make_array,
};
use arrow_buffer::{
    ArrowNativeType, BooleanBuffer, Buffer, NullBuffer, OffsetBuffer, ScalarBuffer,
};
use arrow_schema::{ArrowError, DataType, Field, Fields, Schema};
use rand::Rng;
use std::sync::Arc;

// ============================================================================
// Single column -> ArrayRef
// ============================================================================

/// A sink that becomes exactly one Arrow array.
pub trait IntoArrowArray {
    /// Whether the resulting field is declared nullable (true for `Option` columns).
    const NULLABLE: bool = false;
    fn into_arrow_array(self) -> ArrayRef;
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
    fn into_arrow_array(self) -> ArrayRef {
        Arc::new(PrimitiveArray::<T::Arrow>::new(self.into(), None))
    }
}

impl IntoArrowArray for Vec<bool> {
    fn into_arrow_array(self) -> ArrayRef {
        Arc::new(BooleanArray::from(self))
    }
}

impl IntoArrowArray for Vec<usize> {
    fn into_arrow_array(self) -> ArrayRef {
        Arc::new(UInt64Array::from_iter_values(
            self.into_iter().map(|v| v as u64),
        ))
    }
}

/// Offsets and bytes move into the `StringArray` as they are.
impl IntoArrowArray for StrCol {
    fn into_arrow_array(self) -> ArrayRef {
        let (offsets, data) = self.into_parts();
        let offsets = OffsetBuffer::new(ScalarBuffer::from(offsets));
        Arc::new(
            StringArray::try_new(offsets, Buffer::from_vec(data), None)
                .expect("StrCol holds whole UTF-8 strings"),
        )
    }
}

/// The inner column's array with the validity bitmap as its null buffer
/// (moved, not rebuilt); no nulls means no null buffer.
impl<S: IntoArrowArray> IntoArrowArray for NullableCol<S> {
    const NULLABLE: bool = true;
    fn into_arrow_array(self) -> ArrayRef {
        let nulls = self.null_count();
        let values = self.values.into_arrow_array();
        if nulls == 0 {
            return values;
        }
        let (words, len) = self.validity.into_parts();
        let validity = NullBuffer::new(BooleanBuffer::new(Buffer::from_vec(words), 0, len));
        let data = values
            .to_data()
            .into_builder()
            .nulls(Some(validity))
            .build()
            .expect("validity matches the column length");
        make_array(data)
    }
}

/// Values a [`DictCol`] can hold: they become its dictionary array.
pub trait DictionaryValues: Sized + Default {
    fn values_array(values: &[Self]) -> ArrayRef;
}

impl DictionaryValues for String {
    fn values_array(values: &[Self]) -> ArrayRef {
        Arc::new(StringArray::from_iter_values(values))
    }
}

impl<T: ArrowNative> DictionaryValues for T {
    fn values_array(values: &[Self]) -> ArrayRef {
        Arc::new(PrimitiveArray::<T::Arrow>::from_iter_values(
            values.iter().copied(),
        ))
    }
}

/// Codes move into the keys array; the value list is converted once.
impl<T: DictionaryValues> IntoArrowArray for DictCol<T> {
    fn into_arrow_array(self) -> ArrayRef {
        let values = if self.dictionary().is_empty() && !self.is_empty() {
            // only null placeholders (code 0): give them a value to point at
            T::values_array(&[T::default()])
        } else {
            T::values_array(self.dictionary())
        };
        let keys = UInt32Array::new(ScalarBuffer::from(self.codes), None);
        Arc::new(DictionaryArray::<UInt32Type>::new(keys, values))
    }
}

/// `ListCol` -> `ListArray`. Offsets are stored as end offsets (`n` entries);
/// Arrow wants `n + 1` starting at 0, as i32.
impl<V: IntoArrowColumns> IntoArrowArray for ListCol<Vec<usize>, V> {
    fn into_arrow_array(self) -> ArrayRef {
        let mut offsets = Vec::with_capacity(self.offsets.len() + 1);
        offsets.push(0i32);
        offsets.extend(self.offsets.iter().map(|&o| {
            i32::try_from(o).expect("list values exceed i32 offsets; LargeList not supported yet")
        }));

        let (values, nullable) = columns_to_value_array(self.values);
        let field = Arc::new(Field::new_list_field(values.data_type().clone(), nullable));
        Arc::new(ListArray::new(
            field,
            OffsetBuffer::new(offsets.into()),
            values,
            None,
        ))
    }
}

/// One column is the list's values directly; several columns (a tuple element)
/// become a `StructArray`.
fn columns_to_value_array<V: IntoArrowColumns>(values: V) -> (ArrayRef, bool) {
    let mut cols = Vec::new();
    values.into_arrays(&mut cols);
    if cols.len() == 1 {
        return cols.pop().expect("one column");
    }
    let (fields, arrays): (Vec<Field>, Vec<ArrayRef>) = cols
        .into_iter()
        .enumerate()
        .map(|(i, (a, nullable))| {
            (
                Field::new(format!("c{i}"), a.data_type().clone(), nullable),
                a,
            )
        })
        .unzip();
    (
        Arc::new(StructArray::new(Fields::from(fields), arrays, None)),
        false,
    )
}

/// Becomes a `StructArray` (children `f0..`); see [`flatten_named`].
impl<S: IntoArrowArray, const N: usize> IntoArrowArray for FixedCols<S, N> {
    fn into_arrow_array(self) -> ArrayRef {
        let (fields, arrays): (Vec<Field>, Vec<ArrayRef>) = self
            .0
            .into_iter()
            .enumerate()
            .map(|(i, s)| {
                let a = s.into_arrow_array();
                (
                    Field::new(format!("f{i}"), a.data_type().clone(), S::NULLABLE),
                    a,
                )
            })
            .unzip();
        Arc::new(StructArray::new(Fields::from(fields), arrays, None))
    }
}

// ============================================================================
// HList of sinks -> Vec<ArrayRef>
// ============================================================================

/// A sink HList that becomes a sequence of Arrow arrays, one per column.
/// Each column is pushed with its declared nullability.
pub trait IntoArrowColumns {
    fn into_arrays(self, out: &mut Vec<(ArrayRef, bool)>);
}

impl IntoArrowColumns for HNil {
    fn into_arrays(self, _out: &mut Vec<(ArrayRef, bool)>) {}
}

impl<H: IntoArrowArray, T: IntoArrowColumns> IntoArrowColumns for HCons<H, T> {
    fn into_arrays(self, out: &mut Vec<(ArrayRef, bool)>) {
        out.push((self.0.into_arrow_array(), H::NULLABLE));
        self.1.into_arrays(out);
    }
}

/// Top-level sink HList -> `RecordBatch` with columns `c0, c1, ...`.
pub fn into_record_batch<S: IntoArrowColumns>(sink: S) -> Result<RecordBatch, ArrowError> {
    into_named_record_batch(sink, &[])
}

/// Top-level sink HList -> `RecordBatch`, naming columns from `names`
/// (columns past the end of `names` fall back to `c{i}`).
pub fn into_named_record_batch<S: IntoArrowColumns>(
    sink: S,
    names: &[&str],
) -> Result<RecordBatch, ArrowError> {
    let mut cols = Vec::new();
    sink.into_arrays(&mut cols);
    let (fields, arrays): (Vec<Field>, Vec<ArrayRef>) = cols
        .into_iter()
        .enumerate()
        .map(|(i, (a, nullable))| {
            let name = names
                .get(i)
                .map_or_else(|| format!("c{i}"), |n| n.to_string());
            (Field::new(name, a.data_type().clone(), nullable), a)
        })
        .unzip();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
}

/// Collect a sink into a flat, named `RecordBatch`: struct columns (from
/// [`Fixed`] groups) are spliced into top-level columns, and `names` are
/// applied to the flattened columns in order. If `timestamp_col` is given,
/// that (Int64, nanoseconds) column becomes `Timestamp(ns, "UTC")`.
pub fn flatten_named<S: IntoArrowColumns>(
    sink: S,
    names: &[&str],
    timestamp_col: Option<usize>,
) -> Result<RecordBatch, ArrowError> {
    let batch = into_record_batch(sink)?;
    let mut cols: Vec<(ArrayRef, bool)> = Vec::new();
    for (field, col) in batch.schema().fields().iter().zip(batch.columns()) {
        match col.as_any().downcast_ref::<StructArray>() {
            Some(s) => cols.extend(
                s.fields()
                    .iter()
                    .zip(s.columns())
                    .map(|(f, c)| (Arc::clone(c), f.is_nullable())),
            ),
            None => cols.push((Arc::clone(col), field.is_nullable())),
        }
    }
    if cols.len() != names.len() {
        return Err(ArrowError::SchemaError(format!(
            "{} columns but {} names",
            cols.len(),
            names.len()
        )));
    }
    let (fields, arrays): (Vec<Field>, Vec<ArrayRef>) = cols
        .into_iter()
        .zip(names)
        .enumerate()
        .map(|(i, ((a, nullable), name))| {
            let a = match timestamp_col {
                Some(t) if t == i && a.data_type() == &DataType::Int64 => {
                    let values = a
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("Int64")
                        .values()
                        .clone();
                    Arc::new(TimestampNanosecondArray::new(values, None).with_timezone("UTC"))
                        as ArrayRef
                }
                _ => a,
            };
            (Field::new(*name, a.data_type().clone(), nullable), a)
        })
        .unzip();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
}

// ============================================================================
// DistributionArrow — collect straight to a RecordBatch
// ============================================================================

pub trait DistributionArrow: Distribution<VecBackend> {
    /// Collect `n` rows column by column and return them as a `RecordBatch`.
    /// No sink type to spell out: it's derived from `Self::Item`.
    fn collect_record_batch(
        &mut self,
        rng: &mut impl Rng,
        n: usize,
    ) -> Result<RecordBatch, ArrowError>
    where
        <Self::Item as DefaultSink<VecBackend>>::Sink: Default + IntoArrowColumns,
    {
        let sink = self.collect_into(Default::default(), rng, n);
        into_record_batch(sink)
    }

    /// Like [`collect_record_batch`](Self::collect_record_batch) with column names.
    fn collect_named_record_batch(
        &mut self,
        rng: &mut impl Rng,
        n: usize,
        names: &[&str],
    ) -> Result<RecordBatch, ArrowError>
    where
        <Self::Item as DefaultSink<VecBackend>>::Sink: Default + IntoArrowColumns,
    {
        let sink = self.collect_into(Default::default(), rng, n);
        into_named_record_batch(sink, names)
    }
}

impl<D: Distribution<VecBackend>> DistributionArrow for D {}
