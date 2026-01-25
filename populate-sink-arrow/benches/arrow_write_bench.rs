//! Benchmark comparing different Arrow array write strategies:
//! - Row-by-row for all columns: write all columns for each row before moving to next row
//! - Column-by-column: write each column entirely before moving to next column
//! - Bulk write per column: generate all data first, then create arrays from slices

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

// Column distribution across types (100 columns total):
// - 10 columns each of i8, u8 (20 total, 8-bit)
// - 10 columns each of i16, u16 (20 total, 16-bit)
// - 10 columns each of i32, u32, f32 (30 total, 32-bit)
// - 10 columns each of i64, u64, f64 (30 total, 64-bit)

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
    bencher.bench(|| {
        let mut rng = StdRng::seed_from_u64(42);

        // Create builders for each column type
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
        for _ in 0..NUM_ROWS {
            for builder in &mut i8_builders {
                builder.append_value(rng.random::<i8>());
            }
            for builder in &mut u8_builders {
                builder.append_value(rng.random::<u8>());
            }
            for builder in &mut i16_builders {
                builder.append_value(rng.random::<i16>());
            }
            for builder in &mut u16_builders {
                builder.append_value(rng.random::<u16>());
            }
            for builder in &mut i32_builders {
                builder.append_value(rng.random::<i32>());
            }
            for builder in &mut u32_builders {
                builder.append_value(rng.random::<u32>());
            }
            for builder in &mut f32_builders {
                builder.append_value(rng.random::<f32>());
            }
            for builder in &mut i64_builders {
                builder.append_value(rng.random::<i64>());
            }
            for builder in &mut u64_builders {
                builder.append_value(rng.random::<u64>());
            }
            for builder in &mut f64_builders {
                builder.append_value(rng.random::<f64>());
            }
        }

        // Finish all builders and create RecordBatch
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

        let schema = Arc::new(create_schema());
        black_box(RecordBatch::try_new(schema, arrays).unwrap())
    });
}

/// Strategy 2: Write column-by-column, but still row-by-row within each column
/// Complete one column entirely before moving to the next column.
#[divan::bench]
fn column_by_column_row_by_row(bencher: Bencher) {
    bencher.bench(|| {
        let mut rng = StdRng::seed_from_u64(42);
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(NUM_COLUMNS);

        // Build each column completely before moving to the next
        for _ in 0..10 {
            let mut builder = Int8Builder::with_capacity(NUM_ROWS);
            for _ in 0..NUM_ROWS {
                builder.append_value(rng.random::<i8>());
            }
            arrays.push(Arc::new(builder.finish()));
        }

        for _ in 0..10 {
            let mut builder = UInt8Builder::with_capacity(NUM_ROWS);
            for _ in 0..NUM_ROWS {
                builder.append_value(rng.random::<u8>());
            }
            arrays.push(Arc::new(builder.finish()));
        }

        for _ in 0..10 {
            let mut builder = Int16Builder::with_capacity(NUM_ROWS);
            for _ in 0..NUM_ROWS {
                builder.append_value(rng.random::<i16>());
            }
            arrays.push(Arc::new(builder.finish()));
        }

        for _ in 0..10 {
            let mut builder = UInt16Builder::with_capacity(NUM_ROWS);
            for _ in 0..NUM_ROWS {
                builder.append_value(rng.random::<u16>());
            }
            arrays.push(Arc::new(builder.finish()));
        }

        for _ in 0..10 {
            let mut builder = Int32Builder::with_capacity(NUM_ROWS);
            for _ in 0..NUM_ROWS {
                builder.append_value(rng.random::<i32>());
            }
            arrays.push(Arc::new(builder.finish()));
        }

        for _ in 0..10 {
            let mut builder = UInt32Builder::with_capacity(NUM_ROWS);
            for _ in 0..NUM_ROWS {
                builder.append_value(rng.random::<u32>());
            }
            arrays.push(Arc::new(builder.finish()));
        }

        for _ in 0..10 {
            let mut builder = Float32Builder::with_capacity(NUM_ROWS);
            for _ in 0..NUM_ROWS {
                builder.append_value(rng.random::<f32>());
            }
            arrays.push(Arc::new(builder.finish()));
        }

        for _ in 0..10 {
            let mut builder = Int64Builder::with_capacity(NUM_ROWS);
            for _ in 0..NUM_ROWS {
                builder.append_value(rng.random::<i64>());
            }
            arrays.push(Arc::new(builder.finish()));
        }

        for _ in 0..10 {
            let mut builder = UInt64Builder::with_capacity(NUM_ROWS);
            for _ in 0..NUM_ROWS {
                builder.append_value(rng.random::<u64>());
            }
            arrays.push(Arc::new(builder.finish()));
        }

        for _ in 0..10 {
            let mut builder = Float64Builder::with_capacity(NUM_ROWS);
            for _ in 0..NUM_ROWS {
                builder.append_value(rng.random::<f64>());
            }
            arrays.push(Arc::new(builder.finish()));
        }

        let schema = Arc::new(create_schema());
        black_box(RecordBatch::try_new(schema, arrays).unwrap())
    });
}

/// Strategy 3: Bulk write per column
/// Generate all random data first into Vecs, then create arrays from slices.
#[divan::bench]
fn bulk_write_per_column(bencher: Bencher) {
    bencher.bench(|| {
        let mut rng = StdRng::seed_from_u64(42);
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(NUM_COLUMNS);

        // Generate and create arrays in bulk using from_iter or from_vec
        for _ in 0..10 {
            let data: Vec<i8> = (0..NUM_ROWS).map(|_| rng.random::<i8>()).collect();
            arrays.push(Arc::new(Int8Array::from(data)));
        }

        for _ in 0..10 {
            let data: Vec<u8> = (0..NUM_ROWS).map(|_| rng.random::<u8>()).collect();
            arrays.push(Arc::new(UInt8Array::from(data)));
        }

        for _ in 0..10 {
            let data: Vec<i16> = (0..NUM_ROWS).map(|_| rng.random::<i16>()).collect();
            arrays.push(Arc::new(Int16Array::from(data)));
        }

        for _ in 0..10 {
            let data: Vec<u16> = (0..NUM_ROWS).map(|_| rng.random::<u16>()).collect();
            arrays.push(Arc::new(UInt16Array::from(data)));
        }

        for _ in 0..10 {
            let data: Vec<i32> = (0..NUM_ROWS).map(|_| rng.random::<i32>()).collect();
            arrays.push(Arc::new(Int32Array::from(data)));
        }

        for _ in 0..10 {
            let data: Vec<u32> = (0..NUM_ROWS).map(|_| rng.random::<u32>()).collect();
            arrays.push(Arc::new(UInt32Array::from(data)));
        }

        for _ in 0..10 {
            let data: Vec<f32> = (0..NUM_ROWS).map(|_| rng.random::<f32>()).collect();
            arrays.push(Arc::new(Float32Array::from(data)));
        }

        for _ in 0..10 {
            let data: Vec<i64> = (0..NUM_ROWS).map(|_| rng.random::<i64>()).collect();
            arrays.push(Arc::new(Int64Array::from(data)));
        }

        for _ in 0..10 {
            let data: Vec<u64> = (0..NUM_ROWS).map(|_| rng.random::<u64>()).collect();
            arrays.push(Arc::new(UInt64Array::from(data)));
        }

        for _ in 0..10 {
            let data: Vec<f64> = (0..NUM_ROWS).map(|_| rng.random::<f64>()).collect();
            arrays.push(Arc::new(Float64Array::from(data)));
        }

        let schema = Arc::new(create_schema());
        black_box(RecordBatch::try_new(schema, arrays).unwrap())
    });
}
