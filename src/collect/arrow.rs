use crate::list::ListIter;
use crate::tuple::{TupleIter2, TupleIter3};
use crate::SampleIter;
use arrow::array::{
    ArrayRef, BooleanBuilder, ListBuilder, PrimitiveBuilder, RecordBatch, StringBuilder,
};
use arrow::datatypes::{
    DataType, Field, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, Schema,
    UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use rand::Rng;
use std::sync::Arc;

/// Collector that outputs Arrow types.
///
/// Arrow has its own methods rather than implementing the generic collector traits
/// because it requires type bounds (ArrowType) that can't be added to the generic traits.
pub struct Arrow;

impl Arrow {
    /// Collect a scalar iterator into an Arrow array.
    pub fn collect<I: SampleIter>(mut iter: I, rng: &mut impl Rng, n: usize) -> ArrayRef
    where
        I::Item: ArrowType,
    {
        let mut builder = I::Item::new_builder(n);
        for _ in 0..n {
            builder.append(iter.next(rng));
        }
        builder.finish()
    }

    /// Collect a 2-tuple iterator into a RecordBatch.
    pub fn collect2<I1: SampleIter, I2: SampleIter>(
        mut iter: TupleIter2<I1, I2>,
        rng: &mut impl Rng,
        n: usize,
    ) -> RecordBatch
    where
        I1::Item: ArrowType,
        I2::Item: ArrowType,
    {
        let mut b1 = I1::Item::new_builder(n);
        let mut b2 = I2::Item::new_builder(n);
        for _ in 0..n {
            b1.append(iter.0.next(rng));
            b2.append(iter.1.next(rng));
        }

        let schema = Schema::new(vec![
            Field::new("c0", I1::Item::data_type(), false),
            Field::new("c1", I2::Item::data_type(), false),
        ]);

        RecordBatch::try_new(Arc::new(schema), vec![b1.finish(), b2.finish()]).unwrap()
    }

    /// Collect a 3-tuple iterator into a RecordBatch.
    pub fn collect3<I1: SampleIter, I2: SampleIter, I3: SampleIter>(
        mut iter: TupleIter3<I1, I2, I3>,
        rng: &mut impl Rng,
        n: usize,
    ) -> RecordBatch
    where
        I1::Item: ArrowType,
        I2::Item: ArrowType,
        I3::Item: ArrowType,
    {
        let mut b1 = I1::Item::new_builder(n);
        let mut b2 = I2::Item::new_builder(n);
        let mut b3 = I3::Item::new_builder(n);
        for _ in 0..n {
            b1.append(iter.0.next(rng));
            b2.append(iter.1.next(rng));
            b3.append(iter.2.next(rng));
        }

        let schema = Schema::new(vec![
            Field::new("c0", I1::Item::data_type(), false),
            Field::new("c1", I2::Item::data_type(), false),
            Field::new("c2", I3::Item::data_type(), false),
        ]);

        RecordBatch::try_new(
            Arc::new(schema),
            vec![b1.finish(), b2.finish(), b3.finish()],
        )
        .unwrap()
    }

    /// Collect a list iterator into a ListArray.
    pub fn collect_list<L: SampleIter<Item = usize>, E: SampleIter>(
        mut iter: ListIter<L, E>,
        rng: &mut impl Rng,
        n: usize,
    ) -> ArrayRef
    where
        E::Item: ArrowPrimitiveType,
    {
        let mut list_builder =
            ListBuilder::new(PrimitiveBuilder::<<E::Item as ArrowPrimitiveType>::Arrow>::new());

        for _ in 0..n {
            let mut elem_iter = iter.elements.clone(); // Fresh state for each list
            let len = iter.lengths.next(rng);
            for _ in 0..len {
                list_builder
                    .values()
                    .append_value(elem_iter.next(rng));
            }
            list_builder.append(true);
        }

        Arc::new(list_builder.finish())
    }
}

/// Trait for types that can be collected into Arrow arrays.
pub trait ArrowType: Sized {
    type Builder: ArrowBuilder<Self>;
    fn new_builder(capacity: usize) -> Self::Builder;
    fn data_type() -> DataType;
}

/// Trait for Arrow primitive types that can be used in ListBuilder.
pub trait ArrowPrimitiveType: Sized {
    type Arrow: arrow::datatypes::ArrowPrimitiveType<Native = Self>;
}

/// Builder trait for Arrow arrays.
pub trait ArrowBuilder<T>: Sized {
    fn append(&mut self, value: T);
    fn finish(self) -> ArrayRef;
}

// === Primitive type implementations ===

macro_rules! impl_arrow_primitive {
    ($rust_type:ty, $arrow_type:ty, $data_type:expr) => {
        impl ArrowBuilder<$rust_type> for PrimitiveBuilder<$arrow_type> {
            fn append(&mut self, value: $rust_type) {
                self.append_value(value);
            }

            fn finish(mut self) -> ArrayRef {
                Arc::new(PrimitiveBuilder::finish(&mut self))
            }
        }

        impl ArrowType for $rust_type {
            type Builder = PrimitiveBuilder<$arrow_type>;

            fn new_builder(capacity: usize) -> Self::Builder {
                PrimitiveBuilder::<$arrow_type>::with_capacity(capacity)
            }

            fn data_type() -> DataType {
                $data_type
            }
        }

        impl ArrowPrimitiveType for $rust_type {
            type Arrow = $arrow_type;
        }
    };
}

impl_arrow_primitive!(i8, Int8Type, DataType::Int8);
impl_arrow_primitive!(i16, Int16Type, DataType::Int16);
impl_arrow_primitive!(i32, Int32Type, DataType::Int32);
impl_arrow_primitive!(i64, Int64Type, DataType::Int64);
impl_arrow_primitive!(u8, UInt8Type, DataType::UInt8);
impl_arrow_primitive!(u16, UInt16Type, DataType::UInt16);
impl_arrow_primitive!(u32, UInt32Type, DataType::UInt32);
impl_arrow_primitive!(u64, UInt64Type, DataType::UInt64);
impl_arrow_primitive!(f32, Float32Type, DataType::Float32);
impl_arrow_primitive!(f64, Float64Type, DataType::Float64);

// === Boolean ===

impl ArrowBuilder<bool> for BooleanBuilder {
    fn append(&mut self, value: bool) {
        self.append_value(value);
    }

    fn finish(mut self) -> ArrayRef {
        Arc::new(BooleanBuilder::finish(&mut self))
    }
}

impl ArrowType for bool {
    type Builder = BooleanBuilder;

    fn new_builder(capacity: usize) -> Self::Builder {
        BooleanBuilder::with_capacity(capacity)
    }

    fn data_type() -> DataType {
        DataType::Boolean
    }
}

// === String ===

impl ArrowBuilder<String> for StringBuilder {
    fn append(&mut self, value: String) {
        self.append_value(value);
    }

    fn finish(mut self) -> ArrayRef {
        Arc::new(StringBuilder::finish(&mut self))
    }
}

impl ArrowType for String {
    type Builder = StringBuilder;

    fn new_builder(capacity: usize) -> Self::Builder {
        StringBuilder::with_capacity(capacity, capacity * 16)
    }

    fn data_type() -> DataType {
        DataType::Utf8
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Constant, Distribution, List};
    use arrow::array::{Array, Int32Array};
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    #[test]
    fn test_element_collect_arrow() {
        let mut dist = Constant::new(42i32);
        let mut rng = StdRng::seed_from_u64(0);

        let array = Arrow::collect(dist.sample(), &mut rng, 5);
        assert_eq!(array.len(), 5);

        let int_array = array.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(int_array.value(0), 42);
        assert_eq!(int_array.value(4), 42);
    }

    #[test]
    fn test_struct_collect_arrow() {
        let mut dist = (Constant::new(1i32), Constant::new(2i64));
        let mut rng = StdRng::seed_from_u64(0);

        let batch = Arrow::collect2(dist.sample(), &mut rng, 3);
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 2);
    }

    #[test]
    fn test_list_collect_arrow() {
        let mut dist = List::new(Constant::new(2usize), Constant::new(7i32));
        let mut rng = StdRng::seed_from_u64(0);

        let array = Arrow::collect_list(dist.sample(), &mut rng, 3);
        assert_eq!(array.len(), 3); // 3 lists
    }
}
