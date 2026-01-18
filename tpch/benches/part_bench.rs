use arrow_array::RecordBatch;
use arrow_array::builder::{Int32Builder, Int64Builder, StringBuilder};
use arrow_schema::{DataType, Field, Schema};
use divan::{Bencher, black_box};
use populate_core::{Distribution, SampleIter};
use rand::SeedableRng;
use rand::rngs::StdRng;
use std::sync::Arc;
use tpch::tables::part::parts;
use tpchgen::generators::PartGenerator;

fn main() {
    divan::main();
}

const ROWS: usize = 10_000;

/// Only benchmark columns that are verified correct in tests.rs:
/// partkey, name, type, size, container, mfgr, brand, retailprice
/// (excludes p_comment which is not implemented)

#[divan::bench]
fn tpchgen_arrow(bencher: Bencher) {
    bencher.bench(|| {
        let mut key_builder = Int64Builder::with_capacity(ROWS);
        let mut name_builder = StringBuilder::with_capacity(ROWS, ROWS * 32);
        let mut type_builder = StringBuilder::with_capacity(ROWS, ROWS * 24);
        let mut size_builder = Int32Builder::with_capacity(ROWS);
        let mut container_builder = StringBuilder::with_capacity(ROWS, ROWS * 12);
        let mut mfgr_builder = Int32Builder::with_capacity(ROWS);
        let mut brand_builder = Int32Builder::with_capacity(ROWS);
        let mut price_builder = Int64Builder::with_capacity(ROWS);

        for p in PartGenerator::new(1.0, 1, 1)
            .into_iter()
            .take(black_box(ROWS))
        {
            key_builder.append_value(p.p_partkey);
            name_builder.append_value(p.p_name.to_string());
            type_builder.append_value(p.p_type);
            size_builder.append_value(p.p_size);
            container_builder.append_value(p.p_container);
            // Extract mfgr number from "Manufacturer#N"
            let mfgr: i32 = p
                .p_mfgr
                .to_string()
                .trim_start_matches("Manufacturer#")
                .parse()
                .unwrap();
            mfgr_builder.append_value(mfgr);
            // Extract brand number from "Brand#NN"
            let brand: i32 = p
                .p_brand
                .to_string()
                .trim_start_matches("Brand#")
                .parse()
                .unwrap();
            brand_builder.append_value(brand);
            price_builder.append_value(p.p_retailprice.0);
        }

        let schema = Schema::new(vec![
            Field::new("p_partkey", DataType::Int64, false),
            Field::new("p_name", DataType::Utf8, false),
            Field::new("p_type", DataType::Utf8, false),
            Field::new("p_size", DataType::Int32, false),
            Field::new("p_container", DataType::Utf8, false),
            Field::new("p_mfgr", DataType::Int32, false),
            Field::new("p_brand", DataType::Int32, false),
            Field::new("p_retailprice", DataType::Int64, false),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(key_builder.finish()),
                Arc::new(name_builder.finish()),
                Arc::new(type_builder.finish()),
                Arc::new(size_builder.finish()),
                Arc::new(container_builder.finish()),
                Arc::new(mfgr_builder.finish()),
                Arc::new(brand_builder.finish()),
                Arc::new(price_builder.finish()),
            ],
        )
        .unwrap();

        black_box(batch)
    });
}

#[divan::bench]
fn populate_arrow(bencher: Bencher) {
    bencher.bench(|| {
        let mut rng = StdRng::seed_from_u64(42);
        let mut dist = parts();
        let mut iter = dist.sample();

        let mut key_builder = Int64Builder::with_capacity(ROWS);
        let mut name_builder = StringBuilder::with_capacity(ROWS, ROWS * 32);
        let mut type_builder = StringBuilder::with_capacity(ROWS, ROWS * 24);
        let mut size_builder = Int64Builder::with_capacity(ROWS);
        let mut container_builder = StringBuilder::with_capacity(ROWS, ROWS * 12);
        let mut mfgr_builder = Int32Builder::with_capacity(ROWS);
        let mut brand_builder = Int32Builder::with_capacity(ROWS);
        let mut price_builder = Int64Builder::with_capacity(ROWS);

        for _ in 0..black_box(ROWS) {
            let (key, price, name, typ, size, container, mfgr, brand) = iter.next(&mut rng);
            key_builder.append_value(key);
            name_builder.append_value(name);
            type_builder.append_value(typ.as_str());
            size_builder.append_value(size);
            container_builder.append_value(container.as_str());
            mfgr_builder.append_value(mfgr);
            brand_builder.append_value(brand);
            price_builder.append_value(price);
        }

        let schema = Schema::new(vec![
            Field::new("p_partkey", DataType::Int64, false),
            Field::new("p_name", DataType::Utf8, false),
            Field::new("p_type", DataType::Utf8, false),
            Field::new("p_size", DataType::Int64, false),
            Field::new("p_container", DataType::Utf8, false),
            Field::new("p_mfgr", DataType::Int32, false),
            Field::new("p_brand", DataType::Int32, false),
            Field::new("p_retailprice", DataType::Int64, false),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(key_builder.finish()),
                Arc::new(name_builder.finish()),
                Arc::new(type_builder.finish()),
                Arc::new(size_builder.finish()),
                Arc::new(container_builder.finish()),
                Arc::new(mfgr_builder.finish()),
                Arc::new(brand_builder.finish()),
                Arc::new(price_builder.finish()),
            ],
        )
        .unwrap();

        black_box(batch)
    });
}
