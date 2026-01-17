use arrow::array::{
    ArrayRef, BooleanBuilder, ListArray, ListBuilder, PrimitiveBuilder, RecordBatch, StringBuilder,
};
use arrow::datatypes::{
    DataType, Field, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, Schema,
    UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use populate_builtins::List;
use populate_core::{Collector, Distribution, ListSink, SampleIter, Sink};
use rand::Rng;
use std::sync::Arc;

/// Trait for creating Arrow sinks for a type.
pub trait ArrowSinkType: Sized {
    type Sink: Sink<Self, Output = ArrayRef>;
    fn new_sink(capacity: usize) -> Self::Sink;
    fn data_type() -> DataType;
}

/// Primitive sink wrapper.
pub struct PrimitiveSink<T: arrow::datatypes::ArrowPrimitiveType> {
    builder: PrimitiveBuilder<T>,
}

impl<T: arrow::datatypes::ArrowPrimitiveType> PrimitiveSink<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            builder: PrimitiveBuilder::<T>::with_capacity(capacity),
        }
    }
}

impl<T: arrow::datatypes::ArrowPrimitiveType> Sink<T::Native> for PrimitiveSink<T> {
    type Output = ArrayRef;

    fn push(&mut self, value: T::Native) {
        self.builder.append_value(value);
    }

    fn extend(&mut self, iter: impl Iterator<Item = T::Native>) {
        for value in iter {
            self.builder.append_value(value);
        }
    }

    fn finish(mut self) -> Self::Output {
        Arc::new(self.builder.finish())
    }
}

macro_rules! impl_arrow_sink_type {
    ($rust_type:ty, $arrow_type:ty, $data_type:expr) => {
        impl ArrowSinkType for $rust_type {
            type Sink = PrimitiveSink<$arrow_type>;

            fn new_sink(capacity: usize) -> Self::Sink {
                PrimitiveSink::new(capacity)
            }

            fn data_type() -> DataType {
                $data_type
            }
        }
    };
}

impl_arrow_sink_type!(i8, Int8Type, DataType::Int8);
impl_arrow_sink_type!(i16, Int16Type, DataType::Int16);
impl_arrow_sink_type!(i32, Int32Type, DataType::Int32);
impl_arrow_sink_type!(i64, Int64Type, DataType::Int64);
impl_arrow_sink_type!(u8, UInt8Type, DataType::UInt8);
impl_arrow_sink_type!(u16, UInt16Type, DataType::UInt16);
impl_arrow_sink_type!(u32, UInt32Type, DataType::UInt32);
impl_arrow_sink_type!(u64, UInt64Type, DataType::UInt64);
impl_arrow_sink_type!(f32, Float32Type, DataType::Float32);
impl_arrow_sink_type!(f64, Float64Type, DataType::Float64);

// Boolean sink
pub struct BooleanSink {
    builder: BooleanBuilder,
}

impl BooleanSink {
    pub fn new(capacity: usize) -> Self {
        Self {
            builder: BooleanBuilder::with_capacity(capacity),
        }
    }
}

impl Sink<bool> for BooleanSink {
    type Output = ArrayRef;

    fn push(&mut self, value: bool) {
        self.builder.append_value(value);
    }

    fn finish(mut self) -> Self::Output {
        Arc::new(self.builder.finish())
    }
}

impl ArrowSinkType for bool {
    type Sink = BooleanSink;

    fn new_sink(capacity: usize) -> Self::Sink {
        BooleanSink::new(capacity)
    }

    fn data_type() -> DataType {
        DataType::Boolean
    }
}

// String sink
pub struct StringSink {
    builder: StringBuilder,
}

impl StringSink {
    pub fn new(capacity: usize) -> Self {
        Self {
            builder: StringBuilder::with_capacity(capacity, capacity * 16),
        }
    }
}

impl Sink<String> for StringSink {
    type Output = ArrayRef;

    fn push(&mut self, value: String) {
        self.builder.append_value(value);
    }

    fn finish(mut self) -> Self::Output {
        Arc::new(self.builder.finish())
    }
}

impl ArrowSinkType for String {
    type Sink = StringSink;

    fn new_sink(capacity: usize) -> Self::Sink {
        StringSink::new(capacity)
    }

    fn data_type() -> DataType {
        DataType::Utf8
    }
}

// List sink for Arrow
pub struct ArrowListSink<T: arrow::datatypes::ArrowPrimitiveType> {
    builder: ListBuilder<PrimitiveBuilder<T>>,
}

impl<T: arrow::datatypes::ArrowPrimitiveType> ArrowListSink<T> {
    pub fn new() -> Self {
        Self {
            builder: ListBuilder::new(PrimitiveBuilder::<T>::new()),
        }
    }
}

impl<T: arrow::datatypes::ArrowPrimitiveType> Default for ArrowListSink<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: arrow::datatypes::ArrowPrimitiveType> ListSink<T::Native> for ArrowListSink<T> {
    type Output = ListArray;

    fn start_list(&mut self) {
        // Nothing to do at start
    }

    fn push_element(&mut self, value: T::Native) {
        self.builder.values().append_value(value);
    }

    fn end_list(&mut self) {
        self.builder.append(true);
    }

    fn finish(mut self) -> Self::Output {
        self.builder.finish()
    }
}

/// Trait for types that can be used in Arrow list sinks.
pub trait ArrowListSinkType: Sized {
    type Sink: ListSink<Self, Output = ListArray>;
    fn new_list_sink() -> Self::Sink;
}

macro_rules! impl_arrow_list_sink_type {
    ($rust_type:ty, $arrow_type:ty) => {
        impl ArrowListSinkType for $rust_type {
            type Sink = ArrowListSink<$arrow_type>;

            fn new_list_sink() -> Self::Sink {
                ArrowListSink::new()
            }
        }
    };
}

impl_arrow_list_sink_type!(i8, Int8Type);
impl_arrow_list_sink_type!(i16, Int16Type);
impl_arrow_list_sink_type!(i32, Int32Type);
impl_arrow_list_sink_type!(i64, Int64Type);
impl_arrow_list_sink_type!(u8, UInt8Type);
impl_arrow_list_sink_type!(u16, UInt16Type);
impl_arrow_list_sink_type!(u32, UInt32Type);
impl_arrow_list_sink_type!(u64, UInt64Type);
impl_arrow_list_sink_type!(f32, Float32Type);
impl_arrow_list_sink_type!(f64, Float64Type);

// === Marker types for Arrow collection ===

/// Marker for collecting scalar distributions into ArrayRef.
pub struct ArrayRefW;

/// Marker for collecting tuple distributions into RecordBatch (columnar).
pub struct RecordBatchW;

/// Marker for collecting List distributions into ArrayRef (ListArray).
pub struct ListArrayW;

// === Collector implementations ===

/// Collect scalar Distribution into ArrayRef
impl<D> Collector<D> for ArrayRefW
where
    D: Distribution,
    D::Item: ArrowSinkType,
{
    type Output = ArrayRef;

    fn collect(dist: &mut D, rng: &mut impl Rng, n: usize) -> Self::Output {
        let mut iter = dist.sample();
        let mut sink = D::Item::new_sink(n);
        sink.extend((0..n).map(|_| iter.next(rng)));
        sink.finish()
    }
}

/// Collect 2-tuple distribution into RecordBatch
impl<D1, D2> Collector<(D1, D2)> for RecordBatchW
where
    D1: Distribution,
    D2: Distribution,
    D1::Item: ArrowSinkType,
    D2::Item: ArrowSinkType,
{
    type Output = RecordBatch;

    fn collect(dist: &mut (D1, D2), rng: &mut impl Rng, n: usize) -> Self::Output {
        let mut iter = dist.sample();
        let mut s1 = D1::Item::new_sink(n);
        let mut s2 = D2::Item::new_sink(n);

        s1.extend((0..n).map(|_| iter.0.next(rng)));
        s2.extend((0..n).map(|_| iter.1.next(rng)));

        let schema = Schema::new(vec![
            Field::new("c0", D1::Item::data_type(), false),
            Field::new("c1", D2::Item::data_type(), false),
        ]);

        RecordBatch::try_new(Arc::new(schema), vec![s1.finish(), s2.finish()]).unwrap()
    }
}

/// Collect 3-tuple distribution into RecordBatch
impl<D1, D2, D3> Collector<(D1, D2, D3)> for RecordBatchW
where
    D1: Distribution,
    D2: Distribution,
    D3: Distribution,
    D1::Item: ArrowSinkType,
    D2::Item: ArrowSinkType,
    D3::Item: ArrowSinkType,
{
    type Output = RecordBatch;

    fn collect(dist: &mut (D1, D2, D3), rng: &mut impl Rng, n: usize) -> Self::Output {
        let mut iter = dist.sample();
        let mut s1 = D1::Item::new_sink(n);
        let mut s2 = D2::Item::new_sink(n);
        let mut s3 = D3::Item::new_sink(n);

        s1.extend((0..n).map(|_| iter.0.next(rng)));
        s2.extend((0..n).map(|_| iter.1.next(rng)));
        s3.extend((0..n).map(|_| iter.2.next(rng)));

        let schema = Schema::new(vec![
            Field::new("c0", D1::Item::data_type(), false),
            Field::new("c1", D2::Item::data_type(), false),
            Field::new("c2", D3::Item::data_type(), false),
        ]);

        RecordBatch::try_new(
            Arc::new(schema),
            vec![s1.finish(), s2.finish(), s3.finish()],
        )
        .unwrap()
    }
}

/// Collect List distribution into ListArray
impl<L, E> Collector<List<L, E>> for ListArrayW
where
    L: Distribution<Item = usize>,
    E: Distribution,
    E::Item: ArrowListSinkType,
{
    type Output = ListArray;

    fn collect(dist: &mut List<L, E>, rng: &mut impl Rng, n: usize) -> Self::Output {
        let iter = dist.sample();
        populate_core::collect_list_into(
            iter.lengths,
            iter.elements,
            rng,
            n,
            E::Item::new_list_sink(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int32Array};

    #[test]
    fn test_primitive_sink() {
        let mut sink = i32::new_sink(5);
        sink.push(1);
        sink.push(2);
        sink.push(3);
        let array = sink.finish();

        assert_eq!(array.len(), 3);
        let int_array = array.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(int_array.value(0), 1);
        assert_eq!(int_array.value(1), 2);
        assert_eq!(int_array.value(2), 3);
    }

    #[test]
    fn test_list_sink() {
        let mut sink = ArrowListSink::<Int32Type>::new();
        sink.start_list();
        sink.push_element(1);
        sink.push_element(2);
        sink.end_list();
        sink.start_list();
        sink.push_element(3);
        sink.end_list();

        let array = sink.finish();
        assert_eq!(array.len(), 2);
    }
}
