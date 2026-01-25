//! Benchmark comparing different Arrow array write strategies:
//! - Row-by-row for all columns: write all columns for each row before moving to next row
//! - Column-by-column: write each column entirely before moving to next column
//! - Bulk write per column: create arrays directly from pre-generated slices

use arrow_array::{
    ArrayRef, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
    RecordBatch, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
    builder::{
        Float32Builder, Float64Builder, Int8Builder, Int16Builder, Int32Builder, Int64Builder,
        UInt8Builder, UInt16Builder, UInt32Builder, UInt64Builder,
    },
};
use arrow_schema::{DataType, Field, Schema};
use divan::{Bencher, black_box};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::sync::Arc;

fn main() {
    divan::main();
}

const NUM_ROWS: usize = 100_000;
const NUM_COLUMNS: usize = 100;

/// Pre-generated data for all columns
struct TestData {
    i8_cols: Vec<Vec<i8>>,
    u8_cols: Vec<Vec<u8>>,
    i16_cols: Vec<Vec<i16>>,
    u16_cols: Vec<Vec<u16>>,
    i32_cols: Vec<Vec<i32>>,
    u32_cols: Vec<Vec<u32>>,
    f32_cols: Vec<Vec<f32>>,
    i64_cols: Vec<Vec<i64>>,
    u64_cols: Vec<Vec<u64>>,
    f64_cols: Vec<Vec<f64>>,
    schema: Arc<Schema>,
}

impl TestData {
    fn new() -> Self {
        let mut rng = StdRng::seed_from_u64(42);

        let i8_cols: Vec<Vec<i8>> = (0..10)
            .map(|_| (0..NUM_ROWS).map(|_| rng.random()).collect())
            .collect();
        let u8_cols: Vec<Vec<u8>> = (0..10)
            .map(|_| (0..NUM_ROWS).map(|_| rng.random()).collect())
            .collect();
        let i16_cols: Vec<Vec<i16>> = (0..10)
            .map(|_| (0..NUM_ROWS).map(|_| rng.random()).collect())
            .collect();
        let u16_cols: Vec<Vec<u16>> = (0..10)
            .map(|_| (0..NUM_ROWS).map(|_| rng.random()).collect())
            .collect();
        let i32_cols: Vec<Vec<i32>> = (0..10)
            .map(|_| (0..NUM_ROWS).map(|_| rng.random()).collect())
            .collect();
        let u32_cols: Vec<Vec<u32>> = (0..10)
            .map(|_| (0..NUM_ROWS).map(|_| rng.random()).collect())
            .collect();
        let f32_cols: Vec<Vec<f32>> = (0..10)
            .map(|_| (0..NUM_ROWS).map(|_| rng.random()).collect())
            .collect();
        let i64_cols: Vec<Vec<i64>> = (0..10)
            .map(|_| (0..NUM_ROWS).map(|_| rng.random()).collect())
            .collect();
        let u64_cols: Vec<Vec<u64>> = (0..10)
            .map(|_| (0..NUM_ROWS).map(|_| rng.random()).collect())
            .collect();
        let f64_cols: Vec<Vec<f64>> = (0..10)
            .map(|_| (0..NUM_ROWS).map(|_| rng.random()).collect())
            .collect();

        let schema = Arc::new(create_schema());

        Self {
            i8_cols,
            u8_cols,
            i16_cols,
            u16_cols,
            i32_cols,
            u32_cols,
            f32_cols,
            i64_cols,
            u64_cols,
            f64_cols,
            schema,
        }
    }
}

fn create_schema() -> Schema {
    let mut fields = Vec::with_capacity(NUM_COLUMNS);

    for i in 0..10 {
        fields.push(Field::new(format!("i8_{i}"), DataType::Int8, false));
    }
    for i in 0..10 {
        fields.push(Field::new(format!("u8_{i}"), DataType::UInt8, false));
    }
    for i in 0..10 {
        fields.push(Field::new(format!("i16_{i}"), DataType::Int16, false));
    }
    for i in 0..10 {
        fields.push(Field::new(format!("u16_{i}"), DataType::UInt16, false));
    }
    for i in 0..10 {
        fields.push(Field::new(format!("i32_{i}"), DataType::Int32, false));
    }
    for i in 0..10 {
        fields.push(Field::new(format!("u32_{i}"), DataType::UInt32, false));
    }
    for i in 0..10 {
        fields.push(Field::new(format!("f32_{i}"), DataType::Float32, false));
    }
    for i in 0..10 {
        fields.push(Field::new(format!("i64_{i}"), DataType::Int64, false));
    }
    for i in 0..10 {
        fields.push(Field::new(format!("u64_{i}"), DataType::UInt64, false));
    }
    for i in 0..10 {
        fields.push(Field::new(format!("f64_{i}"), DataType::Float64, false));
    }

    Schema::new(fields)
}

/// Strategy 1: Write row-by-row for all columns
/// For each row, we append a value to every column builder before moving to the next row.
#[divan::bench]
fn row_by_row_all_columns(bencher: Bencher) {
    let data = TestData::new();

    bencher.bench(|| {
        let mut i8_builders: Vec<_> = (0..10)
            .map(|_| Int8Builder::with_capacity(NUM_ROWS))
            .collect();
        let mut u8_builders: Vec<_> = (0..10)
            .map(|_| UInt8Builder::with_capacity(NUM_ROWS))
            .collect();
        let mut i16_builders: Vec<_> = (0..10)
            .map(|_| Int16Builder::with_capacity(NUM_ROWS))
            .collect();
        let mut u16_builders: Vec<_> = (0..10)
            .map(|_| UInt16Builder::with_capacity(NUM_ROWS))
            .collect();
        let mut i32_builders: Vec<_> = (0..10)
            .map(|_| Int32Builder::with_capacity(NUM_ROWS))
            .collect();
        let mut u32_builders: Vec<_> = (0..10)
            .map(|_| UInt32Builder::with_capacity(NUM_ROWS))
            .collect();
        let mut f32_builders: Vec<_> = (0..10)
            .map(|_| Float32Builder::with_capacity(NUM_ROWS))
            .collect();
        let mut i64_builders: Vec<_> = (0..10)
            .map(|_| Int64Builder::with_capacity(NUM_ROWS))
            .collect();
        let mut u64_builders: Vec<_> = (0..10)
            .map(|_| UInt64Builder::with_capacity(NUM_ROWS))
            .collect();
        let mut f64_builders: Vec<_> = (0..10)
            .map(|_| Float64Builder::with_capacity(NUM_ROWS))
            .collect();

        // Write row by row - for each row, write to ALL columns
        for row in 0..NUM_ROWS {
            for (col, builder) in i8_builders.iter_mut().enumerate() {
                builder.append_value(data.i8_cols[col][row]);
            }
            for (col, builder) in u8_builders.iter_mut().enumerate() {
                builder.append_value(data.u8_cols[col][row]);
            }
            for (col, builder) in i16_builders.iter_mut().enumerate() {
                builder.append_value(data.i16_cols[col][row]);
            }
            for (col, builder) in u16_builders.iter_mut().enumerate() {
                builder.append_value(data.u16_cols[col][row]);
            }
            for (col, builder) in i32_builders.iter_mut().enumerate() {
                builder.append_value(data.i32_cols[col][row]);
            }
            for (col, builder) in u32_builders.iter_mut().enumerate() {
                builder.append_value(data.u32_cols[col][row]);
            }
            for (col, builder) in f32_builders.iter_mut().enumerate() {
                builder.append_value(data.f32_cols[col][row]);
            }
            for (col, builder) in i64_builders.iter_mut().enumerate() {
                builder.append_value(data.i64_cols[col][row]);
            }
            for (col, builder) in u64_builders.iter_mut().enumerate() {
                builder.append_value(data.u64_cols[col][row]);
            }
            for (col, builder) in f64_builders.iter_mut().enumerate() {
                builder.append_value(data.f64_cols[col][row]);
            }
        }

        // Finish all builders
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(NUM_COLUMNS);
        for mut b in i8_builders {
            arrays.push(Arc::new(b.finish()));
        }
        for mut b in u8_builders {
            arrays.push(Arc::new(b.finish()));
        }
        for mut b in i16_builders {
            arrays.push(Arc::new(b.finish()));
        }
        for mut b in u16_builders {
            arrays.push(Arc::new(b.finish()));
        }
        for mut b in i32_builders {
            arrays.push(Arc::new(b.finish()));
        }
        for mut b in u32_builders {
            arrays.push(Arc::new(b.finish()));
        }
        for mut b in f32_builders {
            arrays.push(Arc::new(b.finish()));
        }
        for mut b in i64_builders {
            arrays.push(Arc::new(b.finish()));
        }
        for mut b in u64_builders {
            arrays.push(Arc::new(b.finish()));
        }
        for mut b in f64_builders {
            arrays.push(Arc::new(b.finish()));
        }

        black_box(RecordBatch::try_new(data.schema.clone(), arrays).unwrap())
    });
}

/// Strategy 2: Write column-by-column using builders
/// Complete one column entirely before moving to the next column.
#[divan::bench]
fn column_by_column(bencher: Bencher) {
    let data = TestData::new();

    bencher.bench(|| {
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(NUM_COLUMNS);

        for col_data in &data.i8_cols {
            let mut builder = Int8Builder::with_capacity(NUM_ROWS);
            for &val in col_data {
                builder.append_value(val);
            }
            arrays.push(Arc::new(builder.finish()));
        }

        for col_data in &data.u8_cols {
            let mut builder = UInt8Builder::with_capacity(NUM_ROWS);
            for &val in col_data {
                builder.append_value(val);
            }
            arrays.push(Arc::new(builder.finish()));
        }

        for col_data in &data.i16_cols {
            let mut builder = Int16Builder::with_capacity(NUM_ROWS);
            for &val in col_data {
                builder.append_value(val);
            }
            arrays.push(Arc::new(builder.finish()));
        }

        for col_data in &data.u16_cols {
            let mut builder = UInt16Builder::with_capacity(NUM_ROWS);
            for &val in col_data {
                builder.append_value(val);
            }
            arrays.push(Arc::new(builder.finish()));
        }

        for col_data in &data.i32_cols {
            let mut builder = Int32Builder::with_capacity(NUM_ROWS);
            for &val in col_data {
                builder.append_value(val);
            }
            arrays.push(Arc::new(builder.finish()));
        }

        for col_data in &data.u32_cols {
            let mut builder = UInt32Builder::with_capacity(NUM_ROWS);
            for &val in col_data {
                builder.append_value(val);
            }
            arrays.push(Arc::new(builder.finish()));
        }

        for col_data in &data.f32_cols {
            let mut builder = Float32Builder::with_capacity(NUM_ROWS);
            for &val in col_data {
                builder.append_value(val);
            }
            arrays.push(Arc::new(builder.finish()));
        }

        for col_data in &data.i64_cols {
            let mut builder = Int64Builder::with_capacity(NUM_ROWS);
            for &val in col_data {
                builder.append_value(val);
            }
            arrays.push(Arc::new(builder.finish()));
        }

        for col_data in &data.u64_cols {
            let mut builder = UInt64Builder::with_capacity(NUM_ROWS);
            for &val in col_data {
                builder.append_value(val);
            }
            arrays.push(Arc::new(builder.finish()));
        }

        for col_data in &data.f64_cols {
            let mut builder = Float64Builder::with_capacity(NUM_ROWS);
            for &val in col_data {
                builder.append_value(val);
            }
            arrays.push(Arc::new(builder.finish()));
        }

        black_box(RecordBatch::try_new(data.schema.clone(), arrays).unwrap())
    });
}

/// Strategy 3: Bulk write per column
/// Create arrays directly from iterators - minimal overhead.
#[divan::bench]
fn bulk_write_per_column(bencher: Bencher) {
    let data = TestData::new();

    bencher.bench(|| {
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(NUM_COLUMNS);

        for col_data in &data.i8_cols {
            arrays.push(Arc::new(Int8Array::from_iter_values(col_data.iter().copied())));
        }
        for col_data in &data.u8_cols {
            arrays.push(Arc::new(UInt8Array::from_iter_values(col_data.iter().copied())));
        }
        for col_data in &data.i16_cols {
            arrays.push(Arc::new(Int16Array::from_iter_values(col_data.iter().copied())));
        }
        for col_data in &data.u16_cols {
            arrays.push(Arc::new(UInt16Array::from_iter_values(col_data.iter().copied())));
        }
        for col_data in &data.i32_cols {
            arrays.push(Arc::new(Int32Array::from_iter_values(col_data.iter().copied())));
        }
        for col_data in &data.u32_cols {
            arrays.push(Arc::new(UInt32Array::from_iter_values(col_data.iter().copied())));
        }
        for col_data in &data.f32_cols {
            arrays.push(Arc::new(Float32Array::from_iter_values(col_data.iter().copied())));
        }
        for col_data in &data.i64_cols {
            arrays.push(Arc::new(Int64Array::from_iter_values(col_data.iter().copied())));
        }
        for col_data in &data.u64_cols {
            arrays.push(Arc::new(UInt64Array::from_iter_values(col_data.iter().copied())));
        }
        for col_data in &data.f64_cols {
            arrays.push(Arc::new(Float64Array::from_iter_values(col_data.iter().copied())));
        }

        black_box(RecordBatch::try_new(data.schema.clone(), arrays).unwrap())
    });
}
