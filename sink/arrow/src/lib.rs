//! Arrow output: collected columns become Arrow arrays.
//!
//! Collection is unchanged: data is generated column by column into plain
//! `Vec`s (which is all an Arrow `PrimitiveBuilder` is internally), already
//! in Arrow's memory layout. At the end the columns are moved into Arrow
//! arrays:
//!
//! | columns                  | Arrow array                                  |
//! |--------------------------|----------------------------------------------|
//! | `Vec<T>` (numeric)       | `PrimitiveArray`, buffer moved, no copy      |
//! | `Vec<bool>`              | `BooleanArray` (bit-packed)                  |
//! | `Vec<usize>`             | `UInt64Array`                                |
//! | `StrCol` (strings)       | `StringArray`, both buffers moved, no copy   |
//! | `ListCol<V>`             | `ListArray`, offsets moved, with V's columns |
//! |                          | as its values; several columns become a      |
//! |                          | `StructArray`                                |
//! | a tuple of columns       | each field's arrays, in order (nested tuples |
//! |                          | are flattened)                               |
//! | top level                | `RecordBatch` (columns `c0`, `c1`, ...)      |
//!
//! So `list<(i32, list<i64>)>` becomes `List<Struct<c0: Int32, c1: List<Int64>>>`.
//! Nothing goes through a per-value `dyn ArrayBuilder` call. With exact
//! up-front `reserve`s, a `list<numeric>` column is written into exactly two
//! allocations (offsets and values) and both are moved into the `ListArray`
//! as they are. `Vec<bool>` and `Vec<usize>` columns are converted
//! (bit-packing / widening).

use arrow_array::types::{
    Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type,
    UInt32Type, UInt64Type,
};
use arrow_array::{
    Array, ArrayRef, ArrowPrimitiveType, BooleanArray, Date32Array, Decimal128Array, ListArray,
    PrimitiveArray, RecordBatch, StringArray, StructArray, UInt64Array,
};
use arrow_buffer::{ArrowNativeType, Buffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{ArrowError, DataType, Field, Fields, Schema, SchemaRef};
use populate_core::{ChildCols, Columns, Date32, Decimal, Distribution, ListCol, StrCol};
use rand::Rng;
use std::sync::Arc;

// ============================================================================
// Single column -> ArrayRef
// ============================================================================

/// A column that becomes exactly one Arrow array.
pub trait IntoArrowArray {
    /// The type of the array [`into_array`](Self::into_array) makes, known
    /// from the column type alone: a schema needs no rows.
    fn data_type() -> DataType;

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
    fn data_type() -> DataType {
        T::Arrow::DATA_TYPE
    }

    fn into_array(self) -> ArrayRef {
        Arc::new(PrimitiveArray::<T::Arrow>::new(self.into(), None))
    }
}

impl IntoArrowArray for Vec<bool> {
    fn data_type() -> DataType {
        DataType::Boolean
    }

    fn into_array(self) -> ArrayRef {
        Arc::new(BooleanArray::from(self))
    }
}

impl IntoArrowArray for Vec<usize> {
    fn data_type() -> DataType {
        DataType::UInt64
    }

    fn into_array(self) -> ArrayRef {
        Arc::new(UInt64Array::from_iter_values(
            self.into_iter().map(|v| v as u64),
        ))
    }
}

/// `StrCol` -> `StringArray`: offsets and bytes are moved, not copied.
impl IntoArrowArray for StrCol {
    fn data_type() -> DataType {
        DataType::Utf8
    }

    fn into_array(self) -> ArrayRef {
        let (offsets, data) = self.into_parts();
        // SAFETY: `StrCol` only accepts UTF-8 (`&str`, `char`, checked ASCII)
        // and its offsets start at 0 and never decrease.
        let offsets = unsafe { OffsetBuffer::new_unchecked(ScalarBuffer::from(offsets)) };
        let array = unsafe { StringArray::new_unchecked(offsets, Buffer::from_vec(data), None) };
        Arc::new(array)
    }
}

/// `ListCol` -> `ListArray`: the offsets buffer is moved into the array, no
/// copy.
impl<V: IntoArrowColumns> IntoArrowArray for ListCol<V> {
    fn data_type() -> DataType {
        DataType::List(Arc::new(Field::new_list_field(value_type::<V>(), false)))
    }

    fn into_array(self) -> ArrayRef {
        let (offsets, values) = self.into_parts();
        let values = columns_to_value_array(values);
        let field = Arc::new(Field::new_list_field(values.data_type().clone(), false));
        Arc::new(ListArray::new(
            field,
            OffsetBuffer::new(offsets.into()),
            values,
            None,
        ))
    }
}

/// Columns as one array: a single column is returned as is; several
/// columns (a tuple) become a `StructArray` with fields `c0, c1, ...`.
pub fn columns_to_value_array<V: IntoArrowColumns>(values: V) -> ArrayRef {
    let mut cols = Vec::with_capacity(V::LEN);
    values.into_arrays(&mut cols);
    if cols.len() == 1 {
        return cols.pop().expect("one column");
    }
    Arc::new(StructArray::new(column_fields(&cols), cols, None))
}

/// The type [`columns_to_value_array`] gives `V`: its one column's type,
/// or a `Struct` of fields `c0, c1, ...`.
pub fn value_type<V: IntoArrowColumns>() -> DataType {
    let mut types = Vec::with_capacity(V::LEN);
    V::data_types(&mut types);
    if types.len() == 1 {
        return types.pop().expect("one column");
    }
    DataType::Struct(
        types
            .into_iter()
            .enumerate()
            .map(|(i, t)| Field::new(format!("c{i}"), t, false))
            .collect(),
    )
}

/// Non-nullable fields `c0, c1, ...` typed after `cols`.
fn column_fields(cols: &[ArrayRef]) -> Fields {
    cols.iter()
        .enumerate()
        .map(|(i, a)| Field::new(format!("c{i}"), a.data_type().clone(), false))
        .collect()
}

/// A `Vec` of a `#[repr(transparent)]` wrapper of `T` as a `Vec<T>`, without
/// copying.
///
/// # Safety
///
/// `W` must be `#[repr(transparent)]` over `T`.
unsafe fn unwrap_vec<W, T>(v: Vec<W>) -> Vec<T> {
    let mut v = std::mem::ManuallyDrop::new(v);
    // SAFETY: same size, alignment and validity (the caller's guarantee),
    // so the allocation is a valid `Vec<T>` of the same length and capacity.
    unsafe { Vec::from_raw_parts(v.as_mut_ptr().cast::<T>(), v.len(), v.capacity()) }
}

/// `Date32` days, moved into a `Date32Array`.
impl IntoArrowArray for Vec<Date32> {
    fn data_type() -> DataType {
        DataType::Date32
    }

    fn into_array(self) -> ArrayRef {
        // SAFETY: `Date32` is `#[repr(transparent)]` over `i32`
        let days: Vec<i32> = unsafe { unwrap_vec(self) };
        Arc::new(Date32Array::new(days.into(), None))
    }
}

/// `Decimal<P, S>` units, moved into a `Decimal128Array`.
impl<const P: u8, const S: i8> IntoArrowArray for Vec<Decimal<P, S>> {
    fn data_type() -> DataType {
        DataType::Decimal128(P, S)
    }

    fn into_array(self) -> ArrayRef {
        // SAFETY: `Decimal` is `#[repr(transparent)]` over `i128`
        let units: Vec<i128> = unsafe { unwrap_vec(self) };
        Arc::new(
            Decimal128Array::new(units.into(), None)
                .with_precision_and_scale(P, S)
                .expect("valid decimal precision and scale"),
        )
    }
}

// ============================================================================
// Columns -> Vec<ArrayRef>
// ============================================================================

/// Columns that become a sequence of Arrow arrays: one array for a single
/// column, and for a tuple the arrays of each field in order.
pub trait IntoArrowColumns {
    /// The number of arrays, so the `Vec` of them is allocated once.
    const LEN: usize;

    /// The type of each array [`into_arrays`](Self::into_arrays) makes, in
    /// order. This is where a schema's types come from: the sinks a
    /// distribution collects into know what they will become.
    fn data_types(out: &mut Vec<DataType>);

    fn into_arrays(self, out: &mut Vec<ArrayRef>);
}

impl<C: IntoArrowArray> IntoArrowColumns for C {
    const LEN: usize = 1;
    fn data_types(out: &mut Vec<DataType>) {
        out.push(C::data_type());
    }
    fn into_arrays(self, out: &mut Vec<ArrayRef>) {
        out.push(self.into_array());
    }
}

macro_rules! tuple_columns {
    ($($C:ident $idx:tt),+) => {
        impl<$($C: IntoArrowColumns),+> IntoArrowColumns for ($($C,)+) {
            const LEN: usize = 0 $(+ $C::LEN)+;
            fn data_types(out: &mut Vec<DataType>) {
                $($C::data_types(out);)+
            }
            fn into_arrays(self, out: &mut Vec<ArrayRef>) {
                $(self.$idx.into_arrays(out);)+
            }
        }
    };
}

tuple_columns!(A 0, B 1);
tuple_columns!(A 0, B 1, C 2);
tuple_columns!(A 0, B 1, C 2, D 3);
tuple_columns!(A 0, B 1, C 2, D 3, E 4);
tuple_columns!(A 0, B 1, C 2, D 3, E 4, F 5);
tuple_columns!(A 0, B 1, C 2, D 3, E 4, F 5, G 6);
tuple_columns!(A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7);
tuple_columns!(A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8);
tuple_columns!(A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8, J 9);
tuple_columns!(A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8, J 9, K 10);
tuple_columns!(A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8, J 9, K 10, L 11);

/// Top-level columns -> `RecordBatch` with columns `c0, c1, ...`.
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

/// Children are the rows: their columns, as if collected directly.
impl<V: IntoArrowColumns> IntoArrowColumns for ChildCols<V> {
    const LEN: usize = V::LEN;
    fn data_types(out: &mut Vec<DataType>) {
        V::data_types(out);
    }
    fn into_arrays(self, out: &mut Vec<ArrayRef>) {
        self.values.into_arrays(out);
    }
}

// ============================================================================
// Schema
// ============================================================================

/// The schema of the rows `dist` makes, without making any: the names
/// from the distribution ([`Distribution::column_names`]), the types from
/// the columns it collects into ([`IntoArrowColumns::data_types`]).
/// Unnamed columns are `c0, c1, ...`; every field is non-nullable.
///
/// # Panics
///
/// If `dist` names some of its columns but not all.
pub fn schema_of<D: Distribution>(dist: &D) -> Schema
where
    Columns<D::Item>: IntoArrowColumns,
{
    let mut types = Vec::with_capacity(<Columns<D::Item>>::LEN);
    <Columns<D::Item>>::data_types(&mut types);
    let mut names = Vec::with_capacity(types.len());
    dist.column_names(&mut names);
    if names.is_empty() {
        names = (0..types.len()).map(|i| format!("c{i}")).collect();
    }
    assert_eq!(
        names.len(),
        types.len(),
        "the distribution names {} of its {} columns: {names:?}",
        names.len(),
        types.len()
    );
    Schema::new(
        names
            .into_iter()
            .zip(types)
            .map(|(n, t)| Field::new(n, t, false))
            .collect::<Vec<_>>(),
    )
}

// ============================================================================
// DistributionArrow — collect straight to a RecordBatch
// ============================================================================

pub trait DistributionArrow: Distribution {
    /// Collect `n` rows column by column and return them as a `RecordBatch`.
    fn collect_record_batch(
        &mut self,
        rng: &mut impl Rng,
        n: usize,
    ) -> Result<RecordBatch, ArrowError>
    where
        Columns<Self::Item>: IntoArrowColumns,
    {
        into_record_batch(self.collect(rng, n))
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
        Columns<Self::Item>: IntoArrowColumns,
    {
        into_record_batch_with(schema, self.collect(rng, n))
    }
}

impl<D: Distribution> DistributionArrow for D {}

#[cfg(test)]
mod tests {
    use super::*;
    use populate_core::{Children, Sink};
    use rand::SeedableRng;

    /// One `Decimal<9, 2>` and one `Date32` per row.
    #[derive(Clone)]
    struct Row;

    impl Distribution for Row {
        type Item = (Decimal<9, 2>, Date32);
        fn sample(&mut self, _: &mut impl Rng) -> Self::Item {
            (Decimal(1234), Date32(19_000))
        }
        fn column_names(&self, out: &mut Vec<String>) {
            out.extend(["price".into(), "day".into()]);
        }
    }

    #[test]
    fn schema_from_names_and_sinks() {
        let s = schema_of(&Row);
        assert_eq!(s.field(0).name(), "price");
        assert_eq!(s.field(0).data_type(), &DataType::Decimal128(9, 2));
        assert_eq!(s.field(1).data_type(), &DataType::Date32);
        let cols = Row.collect(&mut rand::rngs::SmallRng::seed_from_u64(0), 3);
        let b = into_record_batch_with(&Arc::new(s), cols).expect("schema matches");
        assert_eq!(b.num_rows(), 3);
    }

    #[test]
    fn children_are_the_rows() {
        let mut c = ChildCols::<Vec<i64>>::default();
        c.push(Children(vec![1, 2]));
        c.push(Children(vec![]));
        c.push(Children(vec![3]));
        let mut types = Vec::new();
        ChildCols::<Vec<i64>>::data_types(&mut types);
        assert_eq!(types, [DataType::Int64]);
        let mut out = Vec::new();
        c.into_arrays(&mut out);
        assert_eq!(out[0].len(), 3);
    }
}
