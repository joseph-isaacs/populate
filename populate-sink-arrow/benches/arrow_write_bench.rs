//! Comprehensive benchmark comparing Arrow array write strategies across multiple dimensions:
//! - Write patterns: row-by-row vs column-by-column vs bulk
//! - Batch sizes: scaling behavior
//! - Nullability: dense vs sparse nulls vs no nulls
//! - Data types: primitives vs strings vs mixed realistic tables
//! - Incremental writes: fresh builders vs appending to existing

use arrow_array::{
    ArrayRef, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
    builder::{Float64Builder, Int32Builder, Int64Builder, StringBuilder},
};
use arrow_schema::{DataType, Field, Schema};
use divan::{Bencher, black_box};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::sync::Arc;

fn main() {
    divan::main();
}

// =============================================================================
// Test data generation
// =============================================================================

struct PrimitiveData {
    i32_cols: Vec<Vec<i32>>,
    i64_cols: Vec<Vec<i64>>,
    f64_cols: Vec<Vec<f64>>,
}

impl PrimitiveData {
    fn new(num_rows: usize, cols_per_type: usize) -> Self {
        let mut rng = StdRng::seed_from_u64(42);
        Self {
            i32_cols: (0..cols_per_type)
                .map(|_| (0..num_rows).map(|_| rng.random()).collect())
                .collect(),
            i64_cols: (0..cols_per_type)
                .map(|_| (0..num_rows).map(|_| rng.random()).collect())
                .collect(),
            f64_cols: (0..cols_per_type)
                .map(|_| (0..num_rows).map(|_| rng.random()).collect())
                .collect(),
        }
    }
}

struct NullableData {
    values: Vec<i64>,
    nulls: Vec<bool>, // true = value present, false = null
}

impl NullableData {
    fn new(num_rows: usize, null_density: f64) -> Self {
        let mut rng = StdRng::seed_from_u64(42);
        Self {
            values: (0..num_rows).map(|_| rng.random()).collect(),
            nulls: (0..num_rows)
                .map(|_| rng.random::<f64>() >= null_density)
                .collect(),
        }
    }
}

struct StringData {
    short_strings: Vec<String>,  // ~10 chars
    medium_strings: Vec<String>, // ~50 chars
    long_strings: Vec<String>,   // ~200 chars
}

impl StringData {
    fn new(num_rows: usize) -> Self {
        let mut rng = StdRng::seed_from_u64(42);
        let charset: Vec<char> = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
            .chars()
            .collect();

        let gen_string = |rng: &mut StdRng, len: usize| -> String {
            (0..len)
                .map(|_| charset[rng.random_range(0..charset.len())])
                .collect()
        };

        Self {
            short_strings: (0..num_rows).map(|_| gen_string(&mut rng, 10)).collect(),
            medium_strings: (0..num_rows).map(|_| gen_string(&mut rng, 50)).collect(),
            long_strings: (0..num_rows).map(|_| gen_string(&mut rng, 200)).collect(),
        }
    }
}

/// Realistic table: mix of types like a typical analytics table
struct RealisticTableData {
    // IDs and keys
    id: Vec<i64>,
    foreign_key: Vec<i32>,
    // Metrics
    amount: Vec<f64>,
    quantity: Vec<i32>,
    // Strings
    name: Vec<String>,
    category: Vec<String>,
    // Timestamps as i64
    created_at: Vec<i64>,
    updated_at: Vec<i64>,
}

impl RealisticTableData {
    fn new(num_rows: usize) -> Self {
        let mut rng = StdRng::seed_from_u64(42);
        let categories = ["electronics", "clothing", "food", "books", "toys"];
        let names: Vec<String> = (0..1000).map(|i| format!("Product_{}", i)).collect();

        Self {
            id: (0..num_rows as i64).collect(),
            foreign_key: (0..num_rows).map(|_| rng.random_range(1..10000)).collect(),
            amount: (0..num_rows)
                .map(|_| rng.random::<f64>() * 1000.0)
                .collect(),
            quantity: (0..num_rows).map(|_| rng.random_range(1..100)).collect(),
            name: (0..num_rows)
                .map(|_| names[rng.random_range(0..names.len())].clone())
                .collect(),
            category: (0..num_rows)
                .map(|_| categories[rng.random_range(0..categories.len())].to_string())
                .collect(),
            created_at: (0..num_rows)
                .map(|_| rng.random_range(1600000000i64..1700000000i64))
                .collect(),
            updated_at: (0..num_rows)
                .map(|_| rng.random_range(1600000000i64..1700000000i64))
                .collect(),
        }
    }

    fn schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("foreign_key", DataType::Int32, false),
            Field::new("amount", DataType::Float64, false),
            Field::new("quantity", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("category", DataType::Utf8, false),
            Field::new("created_at", DataType::Int64, false),
            Field::new("updated_at", DataType::Int64, false),
        ])
    }
}

// =============================================================================
// BENCHMARK GROUP 1: Batch size scaling (10K, 100K, 1M rows)
// =============================================================================

mod batch_size {
    use super::*;

    const COLS: usize = 10;

    #[divan::bench]
    fn rows_10k_bulk(bencher: Bencher) {
        let data = PrimitiveData::new(10_000, COLS);
        bencher.bench(|| build_bulk(&data));
    }

    #[divan::bench]
    fn rows_10k_builder(bencher: Bencher) {
        let data = PrimitiveData::new(10_000, COLS);
        bencher.bench(|| build_with_builders(&data));
    }

    #[divan::bench]
    fn rows_100k_bulk(bencher: Bencher) {
        let data = PrimitiveData::new(100_000, COLS);
        bencher.bench(|| build_bulk(&data));
    }

    #[divan::bench]
    fn rows_100k_builder(bencher: Bencher) {
        let data = PrimitiveData::new(100_000, COLS);
        bencher.bench(|| build_with_builders(&data));
    }

    #[divan::bench]
    fn rows_1m_bulk(bencher: Bencher) {
        let data = PrimitiveData::new(1_000_000, COLS);
        bencher.bench(|| build_bulk(&data));
    }

    #[divan::bench]
    fn rows_1m_builder(bencher: Bencher) {
        let data = PrimitiveData::new(1_000_000, COLS);
        bencher.bench(|| build_with_builders(&data));
    }

    fn build_bulk(data: &PrimitiveData) -> Vec<ArrayRef> {
        let mut arrays: Vec<ArrayRef> = Vec::new();
        for col in &data.i32_cols {
            arrays.push(Arc::new(Int32Array::from_iter_values(col.iter().copied())));
        }
        for col in &data.i64_cols {
            arrays.push(Arc::new(Int64Array::from_iter_values(col.iter().copied())));
        }
        for col in &data.f64_cols {
            arrays.push(Arc::new(Float64Array::from_iter_values(
                col.iter().copied(),
            )));
        }
        black_box(arrays)
    }

    fn build_with_builders(data: &PrimitiveData) -> Vec<ArrayRef> {
        let num_rows = data.i32_cols[0].len();
        let mut arrays: Vec<ArrayRef> = Vec::new();

        for col in &data.i32_cols {
            let mut builder = Int32Builder::with_capacity(num_rows);
            for &v in col {
                builder.append_value(v);
            }
            arrays.push(Arc::new(builder.finish()));
        }
        for col in &data.i64_cols {
            let mut builder = Int64Builder::with_capacity(num_rows);
            for &v in col {
                builder.append_value(v);
            }
            arrays.push(Arc::new(builder.finish()));
        }
        for col in &data.f64_cols {
            let mut builder = Float64Builder::with_capacity(num_rows);
            for &v in col {
                builder.append_value(v);
            }
            arrays.push(Arc::new(builder.finish()));
        }
        black_box(arrays)
    }
}

// =============================================================================
// BENCHMARK GROUP 2: Null density impact
// =============================================================================

mod nullability {
    use super::*;

    const NUM_ROWS: usize = 100_000;

    #[divan::bench]
    fn no_nulls(bencher: Bencher) {
        let data = NullableData::new(NUM_ROWS, 0.0);
        bencher.bench(|| build_nullable(&data));
    }

    #[divan::bench]
    fn sparse_nulls_1pct(bencher: Bencher) {
        let data = NullableData::new(NUM_ROWS, 0.01);
        bencher.bench(|| build_nullable(&data));
    }

    #[divan::bench]
    fn medium_nulls_10pct(bencher: Bencher) {
        let data = NullableData::new(NUM_ROWS, 0.10);
        bencher.bench(|| build_nullable(&data));
    }

    #[divan::bench]
    fn dense_nulls_50pct(bencher: Bencher) {
        let data = NullableData::new(NUM_ROWS, 0.50);
        bencher.bench(|| build_nullable(&data));
    }

    #[divan::bench]
    fn very_dense_nulls_90pct(bencher: Bencher) {
        let data = NullableData::new(NUM_ROWS, 0.90);
        bencher.bench(|| build_nullable(&data));
    }

    fn build_nullable(data: &NullableData) -> ArrayRef {
        let mut builder = Int64Builder::with_capacity(data.values.len());
        for (&val, &present) in data.values.iter().zip(data.nulls.iter()) {
            if present {
                builder.append_value(val);
            } else {
                builder.append_null();
            }
        }
        black_box(Arc::new(builder.finish()))
    }
}

// =============================================================================
// BENCHMARK GROUP 3: String lengths
// =============================================================================

mod strings {
    use super::*;

    const NUM_ROWS: usize = 100_000;

    #[divan::bench]
    fn short_strings_10_chars(bencher: Bencher) {
        let data = StringData::new(NUM_ROWS);
        bencher.bench(|| build_strings(&data.short_strings));
    }

    #[divan::bench]
    fn medium_strings_50_chars(bencher: Bencher) {
        let data = StringData::new(NUM_ROWS);
        bencher.bench(|| build_strings(&data.medium_strings));
    }

    #[divan::bench]
    fn long_strings_200_chars(bencher: Bencher) {
        let data = StringData::new(NUM_ROWS);
        bencher.bench(|| build_strings(&data.long_strings));
    }

    #[divan::bench]
    fn short_strings_bulk(bencher: Bencher) {
        let data = StringData::new(NUM_ROWS);
        bencher.bench(|| build_strings_bulk(&data.short_strings));
    }

    #[divan::bench]
    fn medium_strings_bulk(bencher: Bencher) {
        let data = StringData::new(NUM_ROWS);
        bencher.bench(|| build_strings_bulk(&data.medium_strings));
    }

    #[divan::bench]
    fn long_strings_bulk(bencher: Bencher) {
        let data = StringData::new(NUM_ROWS);
        bencher.bench(|| build_strings_bulk(&data.long_strings));
    }

    fn build_strings(data: &[String]) -> ArrayRef {
        let mut builder = StringBuilder::with_capacity(data.len(), data.len() * 50);
        for s in data {
            builder.append_value(s);
        }
        black_box(Arc::new(builder.finish()))
    }

    fn build_strings_bulk(data: &[String]) -> ArrayRef {
        black_box(Arc::new(StringArray::from_iter_values(
            data.iter().map(|s| s.as_str()),
        )))
    }
}

// =============================================================================
// BENCHMARK GROUP 4: Wide vs Tall tables
// =============================================================================

mod shape {
    use super::*;

    // Same total cells: 1M
    const WIDE_COLS: usize = 100;
    const WIDE_ROWS: usize = 10_000;

    const TALL_COLS: usize = 10;
    const TALL_ROWS: usize = 100_000;

    #[divan::bench]
    fn wide_100_cols_10k_rows_bulk(bencher: Bencher) {
        let data = PrimitiveData::new(WIDE_ROWS, WIDE_COLS / 3);
        bencher.bench(|| build_bulk(&data));
    }

    #[divan::bench]
    fn wide_100_cols_10k_rows_builder(bencher: Bencher) {
        let data = PrimitiveData::new(WIDE_ROWS, WIDE_COLS / 3);
        bencher.bench(|| build_with_builders(&data));
    }

    #[divan::bench]
    fn tall_10_cols_100k_rows_bulk(bencher: Bencher) {
        let data = PrimitiveData::new(TALL_ROWS, TALL_COLS / 3);
        bencher.bench(|| build_bulk(&data));
    }

    #[divan::bench]
    fn tall_10_cols_100k_rows_builder(bencher: Bencher) {
        let data = PrimitiveData::new(TALL_ROWS, TALL_COLS / 3);
        bencher.bench(|| build_with_builders(&data));
    }

    fn build_bulk(data: &PrimitiveData) -> Vec<ArrayRef> {
        let mut arrays: Vec<ArrayRef> = Vec::new();
        for col in &data.i32_cols {
            arrays.push(Arc::new(Int32Array::from_iter_values(col.iter().copied())));
        }
        for col in &data.i64_cols {
            arrays.push(Arc::new(Int64Array::from_iter_values(col.iter().copied())));
        }
        for col in &data.f64_cols {
            arrays.push(Arc::new(Float64Array::from_iter_values(
                col.iter().copied(),
            )));
        }
        black_box(arrays)
    }

    fn build_with_builders(data: &PrimitiveData) -> Vec<ArrayRef> {
        let num_rows = data.i32_cols[0].len();
        let mut arrays: Vec<ArrayRef> = Vec::new();

        for col in &data.i32_cols {
            let mut builder = Int32Builder::with_capacity(num_rows);
            for &v in col {
                builder.append_value(v);
            }
            arrays.push(Arc::new(builder.finish()));
        }
        for col in &data.i64_cols {
            let mut builder = Int64Builder::with_capacity(num_rows);
            for &v in col {
                builder.append_value(v);
            }
            arrays.push(Arc::new(builder.finish()));
        }
        for col in &data.f64_cols {
            let mut builder = Float64Builder::with_capacity(num_rows);
            for &v in col {
                builder.append_value(v);
            }
            arrays.push(Arc::new(builder.finish()));
        }
        black_box(arrays)
    }
}

// =============================================================================
// BENCHMARK GROUP 5: Realistic mixed-type table
// =============================================================================

mod realistic {
    use super::*;

    const NUM_ROWS: usize = 100_000;

    #[divan::bench]
    fn mixed_table_row_by_row(bencher: Bencher) {
        let data = RealisticTableData::new(NUM_ROWS);
        let schema = Arc::new(RealisticTableData::schema());
        bencher.bench(|| build_row_by_row(&data, &schema));
    }

    #[divan::bench]
    fn mixed_table_column_by_column(bencher: Bencher) {
        let data = RealisticTableData::new(NUM_ROWS);
        let schema = Arc::new(RealisticTableData::schema());
        bencher.bench(|| build_column_by_column(&data, &schema));
    }

    #[divan::bench]
    fn mixed_table_bulk(bencher: Bencher) {
        let data = RealisticTableData::new(NUM_ROWS);
        let schema = Arc::new(RealisticTableData::schema());
        bencher.bench(|| build_bulk(&data, &schema));
    }

    fn build_row_by_row(data: &RealisticTableData, schema: &Arc<Schema>) -> RecordBatch {
        let num_rows = data.id.len();

        let mut id_builder = Int64Builder::with_capacity(num_rows);
        let mut fk_builder = Int32Builder::with_capacity(num_rows);
        let mut amount_builder = Float64Builder::with_capacity(num_rows);
        let mut qty_builder = Int32Builder::with_capacity(num_rows);
        let mut name_builder = StringBuilder::with_capacity(num_rows, num_rows * 20);
        let mut cat_builder = StringBuilder::with_capacity(num_rows, num_rows * 15);
        let mut created_builder = Int64Builder::with_capacity(num_rows);
        let mut updated_builder = Int64Builder::with_capacity(num_rows);

        for i in 0..num_rows {
            id_builder.append_value(data.id[i]);
            fk_builder.append_value(data.foreign_key[i]);
            amount_builder.append_value(data.amount[i]);
            qty_builder.append_value(data.quantity[i]);
            name_builder.append_value(&data.name[i]);
            cat_builder.append_value(&data.category[i]);
            created_builder.append_value(data.created_at[i]);
            updated_builder.append_value(data.updated_at[i]);
        }

        let arrays: Vec<ArrayRef> = vec![
            Arc::new(id_builder.finish()),
            Arc::new(fk_builder.finish()),
            Arc::new(amount_builder.finish()),
            Arc::new(qty_builder.finish()),
            Arc::new(name_builder.finish()),
            Arc::new(cat_builder.finish()),
            Arc::new(created_builder.finish()),
            Arc::new(updated_builder.finish()),
        ];

        black_box(RecordBatch::try_new(schema.clone(), arrays).unwrap())
    }

    fn build_column_by_column(data: &RealisticTableData, schema: &Arc<Schema>) -> RecordBatch {
        let num_rows = data.id.len();

        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(8);

        let mut builder = Int64Builder::with_capacity(num_rows);
        for &v in &data.id {
            builder.append_value(v);
        }
        arrays.push(Arc::new(builder.finish()));

        let mut builder = Int32Builder::with_capacity(num_rows);
        for &v in &data.foreign_key {
            builder.append_value(v);
        }
        arrays.push(Arc::new(builder.finish()));

        let mut builder = Float64Builder::with_capacity(num_rows);
        for &v in &data.amount {
            builder.append_value(v);
        }
        arrays.push(Arc::new(builder.finish()));

        let mut builder = Int32Builder::with_capacity(num_rows);
        for &v in &data.quantity {
            builder.append_value(v);
        }
        arrays.push(Arc::new(builder.finish()));

        let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 20);
        for v in &data.name {
            builder.append_value(v);
        }
        arrays.push(Arc::new(builder.finish()));

        let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 15);
        for v in &data.category {
            builder.append_value(v);
        }
        arrays.push(Arc::new(builder.finish()));

        let mut builder = Int64Builder::with_capacity(num_rows);
        for &v in &data.created_at {
            builder.append_value(v);
        }
        arrays.push(Arc::new(builder.finish()));

        let mut builder = Int64Builder::with_capacity(num_rows);
        for &v in &data.updated_at {
            builder.append_value(v);
        }
        arrays.push(Arc::new(builder.finish()));

        black_box(RecordBatch::try_new(schema.clone(), arrays).unwrap())
    }

    fn build_bulk(data: &RealisticTableData, schema: &Arc<Schema>) -> RecordBatch {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from_iter_values(data.id.iter().copied())),
            Arc::new(Int32Array::from_iter_values(
                data.foreign_key.iter().copied(),
            )),
            Arc::new(Float64Array::from_iter_values(data.amount.iter().copied())),
            Arc::new(Int32Array::from_iter_values(data.quantity.iter().copied())),
            Arc::new(StringArray::from_iter_values(
                data.name.iter().map(|s| s.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                data.category.iter().map(|s| s.as_str()),
            )),
            Arc::new(Int64Array::from_iter_values(
                data.created_at.iter().copied(),
            )),
            Arc::new(Int64Array::from_iter_values(
                data.updated_at.iter().copied(),
            )),
        ];

        black_box(RecordBatch::try_new(schema.clone(), arrays).unwrap())
    }
}

// =============================================================================
// BENCHMARK GROUP 6: Incremental appends (streaming scenario)
// =============================================================================

mod incremental {
    use super::*;

    const CHUNK_SIZE: usize = 10_000;
    const NUM_CHUNKS: usize = 10;

    #[divan::bench]
    fn single_batch_100k(bencher: Bencher) {
        let data: Vec<i64> = (0..100_000).map(|i| i as i64).collect();
        bencher.bench(|| {
            let mut builder = Int64Builder::with_capacity(100_000);
            for &v in &data {
                builder.append_value(v);
            }
            black_box(Arc::new(builder.finish()))
        });
    }

    #[divan::bench]
    fn ten_batches_10k_each(bencher: Bencher) {
        let chunks: Vec<Vec<i64>> = (0..NUM_CHUNKS)
            .map(|c| {
                ((c * CHUNK_SIZE)..((c + 1) * CHUNK_SIZE))
                    .map(|i| i as i64)
                    .collect()
            })
            .collect();

        bencher.bench(|| {
            let mut results: Vec<ArrayRef> = Vec::with_capacity(NUM_CHUNKS);
            for chunk in &chunks {
                let mut builder = Int64Builder::with_capacity(CHUNK_SIZE);
                for &v in chunk {
                    builder.append_value(v);
                }
                results.push(Arc::new(builder.finish()));
            }
            black_box(results)
        });
    }

    #[divan::bench]
    fn growing_builder_100k(bencher: Bencher) {
        let chunks: Vec<Vec<i64>> = (0..NUM_CHUNKS)
            .map(|c| {
                ((c * CHUNK_SIZE)..((c + 1) * CHUNK_SIZE))
                    .map(|i| i as i64)
                    .collect()
            })
            .collect();

        bencher.bench(|| {
            let mut builder = Int64Builder::with_capacity(CHUNK_SIZE); // Start small
            for chunk in &chunks {
                for &v in chunk {
                    builder.append_value(v);
                }
            }
            black_box(Arc::new(builder.finish()))
        });
    }

    #[divan::bench]
    fn preallocated_builder_100k(bencher: Bencher) {
        let chunks: Vec<Vec<i64>> = (0..NUM_CHUNKS)
            .map(|c| {
                ((c * CHUNK_SIZE)..((c + 1) * CHUNK_SIZE))
                    .map(|i| i as i64)
                    .collect()
            })
            .collect();

        bencher.bench(|| {
            let mut builder = Int64Builder::with_capacity(CHUNK_SIZE * NUM_CHUNKS);
            for chunk in &chunks {
                for &v in chunk {
                    builder.append_value(v);
                }
            }
            black_box(Arc::new(builder.finish()))
        });
    }
}
