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
//! | `Vec<Date32>`, `Vec<Decimal>`, `Vec<Timestamp>` | `Date32Array`, `Decimal128Array`, timestamp arrays, buffer moved |
//! | `Vec<[T; N]>`            | `FixedSizeListArray` of `N` values per row   |
//! | `Nullable<C>` (`Option`) | `C`'s array with nulls; a nullable field     |
//! | `SpreadCols<C, N>`       | `N` arrays, one per position                 |
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
    Array, ArrayRef, ArrowPrimitiveType, BooleanArray, Date32Array, Decimal128Array,
    FixedSizeListArray, ListArray, PrimitiveArray, RecordBatch, StringArray, StructArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt64Array, make_array,
};
use arrow_buffer::{
    ArrowNativeType, BooleanBuffer, Buffer, NullBuffer, OffsetBuffer, ScalarBuffer,
};
use arrow_schema::{ArrowError, DataType, Field, Fields, Schema, SchemaRef, TimeUnit};
use populate_core::{
    ChildCols, Columns, Date32, Decimal, Distribution, ListCol, Nullable, SpreadCols, StrCol,
    Timestamp, WideCols, time_unit,
};
use rand::Rng;
use std::iter::repeat_n;
use std::sync::Arc;

// ============================================================================
// Single column -> ArrayRef
// ============================================================================

/// A column that becomes exactly one Arrow array.
pub trait IntoArrowArray {
    /// The type of the array [`into_array`](Self::into_array) makes, known
    /// from the column type alone: a schema needs no rows.
    fn data_type() -> DataType;

    /// Whether the array may have nulls (the schema field's nullability).
    fn nullable() -> bool {
        false
    }

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
        DataType::List(Arc::new(Field::new_list_field(
            value_type::<V>(),
            value_nullable::<V>(),
        )))
    }

    fn into_array(self) -> ArrayRef {
        let (offsets, values) = self.into_parts();
        let values = columns_to_value_array(values);
        let field = Arc::new(Field::new_list_field(
            values.data_type().clone(),
            value_nullable::<V>(),
        ));
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
    let null = nullables_of(&values);
    values.into_arrays(&mut cols);
    if cols.len() == 1 {
        return cols.pop().expect("one column");
    }
    Arc::new(StructArray::new(column_fields(&cols, null), cols, None))
}

/// Whether [`columns_to_value_array`] of `V` may hold nulls: its one
/// column's nullability (a struct of several columns is never null itself).
fn value_nullable<V: IntoArrowColumns>() -> bool {
    let mut null = Vec::with_capacity(V::LEN);
    V::nullables(&mut null);
    null.len() == 1 && null[0]
}

/// The type [`columns_to_value_array`] gives `V`: its one column's type,
/// or a `Struct` of fields `c0, c1, ...`.
pub fn value_type<V: IntoArrowColumns>() -> DataType {
    let mut types = Vec::with_capacity(V::LEN);
    V::data_types(&mut types);
    if types.len() == 1 {
        return types.pop().expect("one column");
    }
    let mut null = Vec::with_capacity(V::LEN);
    V::nullables(&mut null);
    DataType::Struct(
        types
            .into_iter()
            .zip(null)
            .enumerate()
            .map(|(i, (t, null))| Field::new(format!("c{i}"), t, null))
            .collect(),
    )
}

/// Whether each of `cols`' arrays may hold nulls, read off the instance
/// ([`IntoArrowColumns::column_types`]) so run-time-width columns count.
fn nullables_of<V: IntoArrowColumns>(cols: &V) -> Vec<bool> {
    let mut types = Vec::with_capacity(V::LEN);
    cols.column_types(&mut types);
    types.into_iter().map(|(_, null)| null).collect()
}

/// Fields `c0, c1, ...` typed after `arrays`, nullable where `null` says.
fn column_fields(arrays: &[ArrayRef], null: Vec<bool>) -> Fields {
    arrays
        .iter()
        .zip(null)
        .enumerate()
        .map(|(i, (a, null))| Field::new(format!("c{i}"), a.data_type().clone(), null))
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

/// `Timestamp<UNIT, UTC>` counts, moved into a timestamp array of that unit
/// (zoned "UTC" when `UTC`).
impl<const UNIT: u8, const UTC: bool> IntoArrowArray for Vec<Timestamp<UNIT, UTC>> {
    fn data_type() -> DataType {
        DataType::Timestamp(arrow_time_unit(UNIT), UTC.then(|| "UTC".into()))
    }

    fn into_array(self) -> ArrayRef {
        // SAFETY: `Timestamp` is `#[repr(transparent)]` over `i64`
        let counts: ScalarBuffer<i64> = unsafe { unwrap_vec::<_, i64>(self) }.into();
        let tz = UTC.then_some("UTC");
        match arrow_time_unit(UNIT) {
            TimeUnit::Second => {
                Arc::new(TimestampSecondArray::new(counts, None).with_timezone_opt(tz))
            }
            TimeUnit::Millisecond => {
                Arc::new(TimestampMillisecondArray::new(counts, None).with_timezone_opt(tz))
            }
            TimeUnit::Microsecond => {
                Arc::new(TimestampMicrosecondArray::new(counts, None).with_timezone_opt(tz))
            }
            TimeUnit::Nanosecond => {
                Arc::new(TimestampNanosecondArray::new(counts, None).with_timezone_opt(tz))
            }
        }
    }
}

fn arrow_time_unit(unit: u8) -> TimeUnit {
    match unit {
        time_unit::SECOND => TimeUnit::Second,
        time_unit::MILLISECOND => TimeUnit::Millisecond,
        time_unit::MICROSECOND => TimeUnit::Microsecond,
        time_unit::NANOSECOND => TimeUnit::Nanosecond,
        u => panic!("timestamp unit {u} is not one of `time_unit`'s"),
    }
}

/// `[T; N]` rows, moved into a `FixedSizeListArray` of `N` non-null values
/// per row.
impl<T: ArrowNative, const N: usize> IntoArrowArray for Vec<[T; N]> {
    fn data_type() -> DataType {
        let item = Field::new_list_field(T::Arrow::DATA_TYPE, false);
        DataType::FixedSizeList(Arc::new(item), N as i32)
    }

    fn into_array(self) -> ArrayRef {
        // the row count is passed on, as `[T; 0]` rows have no values
        let rows = self.len();
        let values: Vec<T> = self.into_flattened();
        let item = Arc::new(Field::new_list_field(T::Arrow::DATA_TYPE, false));
        let values = Arc::new(PrimitiveArray::<T::Arrow>::new(values.into(), None));
        Arc::new(
            FixedSizeListArray::try_new_with_length(item, N as i32, values, None, rows)
                .expect("N values per row"),
        )
    }
}

/// `Option` rows: `C`'s array with the missing rows null (no nulls at all
/// if every row is present).
impl<C: IntoArrowArray> IntoArrowArray for Nullable<C> {
    fn data_type() -> DataType {
        C::data_type()
    }

    fn nullable() -> bool {
        true
    }

    fn into_array(self) -> ArrayRef {
        let array = self.values.into_array();
        assert_eq!(array.len(), self.valid.len(), "a validity flag per row");
        if self.valid.null_count() == 0 {
            return array;
        }
        let (words, len) = self.valid.into_parts();
        let nulls = NullBuffer::new(BooleanBuffer::new(Buffer::from_vec(words), 0, len));
        // an `Option<Option<T>>`'s inner nulls stay null
        let nulls = NullBuffer::union(Some(&nulls), array.nulls());
        let data = array.into_data().into_builder().nulls(nulls);
        // SAFETY: the array was valid, and only a null buffer of its own
        // length is added
        make_array(unsafe { data.build_unchecked() })
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

    /// Whether each array may have nulls, in the order of
    /// [`data_types`](Self::data_types).
    fn nullables(out: &mut Vec<bool>) {
        out.extend(repeat_n(false, Self::LEN));
    }

    /// The type and nullability of each array these columns will make, in
    /// order. The default is [`data_types`](Self::data_types) with
    /// [`nullables`](Self::nullables); columns whose width is set at run
    /// time ([`WideCols`]) answer from the instance, and tuples ask each
    /// field. [`schema_of`] reads this off [`Distribution::columns`].
    fn column_types(&self, out: &mut Vec<(DataType, bool)>) {
        let mut types = Vec::with_capacity(Self::LEN);
        Self::data_types(&mut types);
        let mut nullable = Vec::with_capacity(types.len());
        Self::nullables(&mut nullable);
        out.extend(types.into_iter().zip(nullable));
    }

    fn into_arrays(self, out: &mut Vec<ArrayRef>);
}

impl<C: IntoArrowArray> IntoArrowColumns for C {
    const LEN: usize = 1;
    fn data_types(out: &mut Vec<DataType>) {
        out.push(C::data_type());
    }
    fn nullables(out: &mut Vec<bool>) {
        out.push(C::nullable());
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
            fn nullables(out: &mut Vec<bool>) {
                $($C::nullables(out);)+
            }
            fn column_types(&self, out: &mut Vec<(DataType, bool)>) {
                $(self.$idx.column_types(out);)+
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
    let null = nullables_of(&sink);
    sink.into_arrays(&mut cols);
    let schema = Schema::new(column_fields(&cols, null));
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
    fn nullables(out: &mut Vec<bool>) {
        V::nullables(out);
    }
    fn column_types(&self, out: &mut Vec<(DataType, bool)>) {
        self.values.column_types(out);
    }
    fn into_arrays(self, out: &mut Vec<ArrayRef>) {
        self.values.into_arrays(out);
    }
}

/// `N` positions, each its own columns, in order.
impl<V: IntoArrowColumns, const N: usize> IntoArrowColumns for SpreadCols<V, N> {
    const LEN: usize = N * V::LEN;
    fn data_types(out: &mut Vec<DataType>) {
        for _ in 0..N {
            V::data_types(out);
        }
    }
    fn nullables(out: &mut Vec<bool>) {
        for _ in 0..N {
            V::nullables(out);
        }
    }
    fn column_types(&self, out: &mut Vec<(DataType, bool)>) {
        for c in &self.0 {
            c.column_types(out);
        }
    }
    fn into_arrays(self, out: &mut Vec<ArrayRef>) {
        for c in self.0 {
            c.into_arrays(out);
        }
    }
}

/// One group of columns per element, in order. The width is the
/// instance's, so `WideCols` has no static types: its schema is read by
/// [`column_types`](IntoArrowColumns::column_types) (top level, in tuples,
/// `Spread`s and children), and it can't be a list's values.
impl<C: IntoArrowColumns> IntoArrowColumns for WideCols<C> {
    const LEN: usize = 0;
    fn data_types(_: &mut Vec<DataType>) {
        panic!("a run-time-width group of columns has no static types: use `column_types`");
    }
    fn nullables(_: &mut Vec<bool>) {
        panic!("a run-time-width group of columns has no static types: use `column_types`");
    }
    fn column_types(&self, out: &mut Vec<(DataType, bool)>) {
        for c in &self.0 {
            c.column_types(out);
        }
    }
    fn into_arrays(self, out: &mut Vec<ArrayRef>) {
        for c in self.0 {
            c.into_arrays(out);
        }
    }
}

// ============================================================================
// Schema
// ============================================================================

/// The schema of the rows `dist` makes, without making any: the names
/// from the distribution ([`Distribution::column_names`]), the types from
/// the columns it collects into ([`IntoArrowColumns::column_types`] of
/// [`Distribution::columns`]). Unnamed columns are `c0, c1, ...`; a field
/// is nullable where its column says so (`Option` rows).
///
/// # Panics
///
/// If `dist` names some of its columns but not all.
pub fn schema_of<D: Distribution>(dist: &D) -> Schema
where
    Columns<D::Item>: IntoArrowColumns,
{
    let mut types = Vec::with_capacity(<Columns<D::Item>>::LEN);
    dist.columns().column_types(&mut types);
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
            .map(|(n, (t, null))| Field::new(n, t, null))
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
    use arrow_array::cast::AsArray;
    use populate_core::{Children, Sink, Spread};
    use rand::SeedableRng;
    use rand::rngs::SmallRng;

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
        let cols = Row.collect(&mut SmallRng::seed_from_u64(0), 3);
        let b = into_record_batch_with(&Arc::new(s), cols).expect("schema matches");
        assert_eq!(b.num_rows(), 3);
    }

    #[test]
    fn wide_rows_read_their_schema_off_the_instance() {
        /// Column `name`: `Some(1)` every row, or `None` when not `some`.
        #[derive(Clone)]
        struct C(&'static str, bool);
        impl Distribution for C {
            type Item = Option<i64>;
            fn sample(&mut self, _: &mut impl Rng) -> Option<i64> {
                self.1.then_some(1)
            }
            fn column_names(&self, out: &mut Vec<String>) {
                out.push(self.0.into());
            }
        }
        let d = (Row, vec![C("a", true), C("b", false), C("c", true)]);
        let s = schema_of(&d);
        let names: Vec<_> = s.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, ["price", "day", "a", "b", "c"]);
        assert!(!s.field(0).is_nullable() && s.field(3).is_nullable());
        assert_eq!(s.field(4).data_type(), &DataType::Int64);
        let cols = d
            .clone()
            .collect(&mut rand::rngs::SmallRng::seed_from_u64(0), 2);
        let b = into_record_batch_with(&Arc::new(s), cols).expect("schema matches");
        assert_eq!((b.num_rows(), b.column(3).null_count()), (2, 2));
        let mut rng = rand::rngs::SmallRng::seed_from_u64(0);
        let b = d
            .clone()
            .collect_record_batch(&mut rng, 2)
            .expect("columns");
        assert_eq!(b.num_columns(), 5);
        assert!(b.schema().field(3).is_nullable());
    }

    #[test]
    fn options_timestamps_and_fixed_lists() {
        type Ts = Timestamp<{ time_unit::MILLISECOND }, true>;
        #[derive(Clone)]
        struct R(i64);
        impl Distribution for R {
            type Item = (Option<i32>, Ts, [f32; 2], Spread<i16, 2>);
            fn sample(&mut self, _: &mut impl Rng) -> Self::Item {
                self.0 += 1;
                let x = self.0;
                (
                    (x % 2 == 0).then_some(x as i32),
                    Timestamp(x),
                    [x as f32; 2],
                    Spread([1, 2]),
                )
            }
        }
        let s = schema_of(&R(0));
        assert!(s.field(0).is_nullable() && !s.field(1).is_nullable());
        assert_eq!(
            s.field(1).data_type(),
            &DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
        );
        assert!(matches!(
            s.field(2).data_type(),
            DataType::FixedSizeList(_, 2)
        ));
        assert_eq!(s.fields().len(), 5);
        let cols = R(0).collect(&mut SmallRng::seed_from_u64(0), 3);
        let b = into_record_batch_with(&Arc::new(s), cols).expect("schema matches");
        assert_eq!(b.column(0).null_count(), 2);
        // all present: no null buffer
        let cols = R(1).collect(&mut SmallRng::seed_from_u64(0), 1);
        let b = into_record_batch_with(&Arc::new(schema_of(&R(0))), cols).unwrap();
        assert!(b.column(0).nulls().is_none());
    }

    #[test]
    fn nulls_inside_lists_structs_and_options() {
        let mut c = <Columns<(Option<i32>, Vec<Option<i64>>, Vec<(Option<i8>, i8)>)>>::default();
        c.push((None, vec![Some(1), None], vec![(None, 1)]));
        c.push((Some(2), vec![], vec![(Some(3), 4)]));
        let b = into_record_batch(c).expect("nullable fields");
        assert!(b.schema().field(0).is_nullable());
        assert_eq!(b.column(1).as_list::<i32>().values().null_count(), 1);
        let s = b.column(2).as_list::<i32>().values().as_struct().clone();
        assert!(s.fields()[0].is_nullable() && !s.fields()[1].is_nullable());
        assert_eq!(
            b.column(1).data_type(),
            &<ListCol<Nullable<Vec<i64>>>>::data_type()
        );

        let mut o = <Columns<Option<Option<i32>>>>::default();
        o.push(Some(None));
        o.push(None);
        o.push(Some(Some(5)));
        assert_eq!(o.into_array().null_count(), 2);

        let z: Vec<[f32; 0]> = vec![[]];
        assert_eq!(IntoArrowArray::into_array(z).len(), 1);
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
