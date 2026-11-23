//! Integer, date and decimal columns are encoded by populate-table, not
//! arrow-rs: they must read back exactly, with the statistics and page
//! arrow-rs would write and correct page indexes.

use arrow_array::cast::AsArray;
use arrow_array::types::{Date32Type, Decimal128Type, Int32Type, Int64Type};
use arrow_array::{
    Array, ArrayRef, Date32Array, Decimal128Array, Int32Array, Int64Array, RecordBatch,
    StringArray, StringViewArray,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use bytes::Bytes;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::{
    ArrowReaderOptions, ParquetRecordBatchReaderBuilder, RowSelection, RowSelector,
};
use parquet::file::metadata::PageIndexPolicy;
use parquet::file::page_index::column_index::ColumnIndexMetaData;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::serialized_reader::ReadOptionsBuilder;
use populate_table::{Chunks, write_parquet};
use std::sync::Arc;

/// Row groups of given batches.
struct Groups(SchemaRef, Vec<Vec<RecordBatch>>);

impl Chunks for Groups {
    fn schema(&self) -> SchemaRef {
        self.0.clone()
    }
    fn num_chunks(&self) -> u64 {
        self.1.len() as u64
    }
    fn chunk(&self, i: u64, emit: &mut dyn FnMut(RecordBatch)) {
        self.1[i as usize].iter().cloned().for_each(emit);
    }
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("small", DataType::Int32, true),
        Field::new("wide", DataType::Int32, true),
        Field::new("required", DataType::Int32, false),
        Field::new("all_null", DataType::Int32, true),
        Field::new("distinct", DataType::Int64, true),
        Field::new("day", DataType::Date32, true),
        Field::new("price", DataType::Decimal128(7, 2), true),
        Field::new("big_price", DataType::Decimal128(15, 2), true),
        Field::new("name", DataType::Utf8, true),
        Field::new("view", DataType::Utf8View, true),
        Field::new("long_view", DataType::Utf8View, true),
        Field::new("required_view", DataType::Utf8View, false),
    ]))
}

/// `n` rows from `start`, nulls in runs and scattered, negative values,
/// one column too distinct for a dictionary.
fn batch(start: i64, n: usize) -> RecordBatch {
    let rows = start..start + n as i64;
    let null = |r: i64| r % 7 == 3 || (r / 1000) % 5 == 2;
    let col = |f: &dyn Fn(i64) -> i64| -> Vec<Option<i64>> {
        rows.clone().map(|r| (!null(r)).then(|| f(r))).collect()
    };
    let i32s = |f: &dyn Fn(i64) -> i64| -> ArrayRef {
        Arc::new(Int32Array::from_iter(
            col(f).into_iter().map(|v| v.map(|v| v as i32)),
        ))
    };
    let columns: Vec<ArrayRef> = vec![
        i32s(&|r| r % 50 - 25),
        i32s(&|r| (r * 2_654_435_761) % 1_000_000_007 - 500_000_000),
        Arc::new(Int32Array::from_iter_values(
            rows.clone().map(|r| (r % 3) as i32),
        )),
        Arc::new(Int32Array::new_null(n)),
        Arc::new(Int64Array::from(col(&|r| r * 1_000_003 - (1 << 40)))),
        Arc::new(Date32Array::from_iter(
            col(&|r| 10_000 + r % 400)
                .into_iter()
                .map(|v| v.map(|v| v as i32)),
        )),
        Arc::new(
            Decimal128Array::from_iter(
                col(&|r| r % 20_000 - 10_000)
                    .into_iter()
                    .map(|v| v.map(i128::from)),
            )
            .with_precision_and_scale(7, 2)
            .unwrap(),
        ),
        Arc::new(
            Decimal128Array::from_iter(
                col(&|r| r * 977 - 3_000_000)
                    .into_iter()
                    .map(|v| v.map(i128::from)),
            )
            .with_precision_and_scale(15, 2)
            .unwrap(),
        ),
        Arc::new(StringArray::from_iter(
            rows.clone()
                .map(|r| (!null(r)).then(|| format!("n{}", r % 13))),
        )),
        // few distinct values, some inline in the view and some not
        Arc::new(StringViewArray::from_iter(rows.clone().map(|r| {
            (!null(r))
                .then(|| ["a", "bb", "a longer string value", ""][(r % 4) as usize].to_string())
        }))),
        // all distinct and longer than the 64-byte statistics truncation,
        // with multi-byte characters around the cut
        Arc::new(StringViewArray::from_iter(rows.clone().map(|r| {
            (!null(r)).then(|| {
                format!(
                    "{:0>62}é€{}{}",
                    r * 7919 % 100_003,
                    "x".repeat((r % 50) as usize),
                    r
                )
            })
        }))),
        Arc::new(StringViewArray::from_iter_values(
            rows.clone().map(|r| format!("v{}", r % 1000)),
        )),
    ];
    RecordBatch::try_new(schema(), columns).unwrap()
}

fn data() -> Groups {
    // row groups of uneven batches, one spanning several 20000-row pages
    let mut groups = Vec::new();
    let mut start = 0;
    for sizes in [vec![1000, 1, 2047], vec![30_000, 25_000, 7], vec![9]] {
        let mut g = Vec::new();
        for n in sizes {
            g.push(batch(start, n));
            start += n as i64;
        }
        groups.push(g);
    }
    Groups(schema(), groups)
}

fn props() -> WriterProperties {
    WriterProperties::builder()
        .set_dictionary_page_size_limit(64 * 1024)
        .set_statistics_enabled(EnabledStatistics::Page)
        .build()
}

/// The same data written by arrow-rs alone, one row group per chunk.
fn arrow_rs(d: &Groups) -> Vec<u8> {
    let mut w = ArrowWriter::try_new(Vec::new(), d.0.clone(), Some(props())).unwrap();
    for g in &d.1 {
        for b in g {
            w.write(b).unwrap();
        }
        w.flush().unwrap();
    }
    w.into_inner().unwrap()
}

fn read(file: &[u8], selection: Option<RowSelection>) -> RecordBatch {
    let opts = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required);
    let mut b =
        ParquetRecordBatchReaderBuilder::try_new_with_options(Bytes::from(file.to_vec()), opts)
            .unwrap();
    if let Some(s) = selection {
        b = b.with_row_selection(s);
    }
    let batches: Vec<_> = b.build().unwrap().map(Result::unwrap).collect();
    arrow_select::concat::concat_batches(&schema(), &batches).unwrap()
}

#[test]
fn reads_back_like_arrow_rs() {
    let d = data();
    let ours = write_parquet(&d, Vec::new(), 2, Some(props())).unwrap();
    let theirs = arrow_rs(&d);
    assert_eq!(read(&ours, None), read(&theirs, None));
    // page skipping through the offset index
    let sel = RowSelection::from(vec![
        RowSelector::skip(21_000),
        RowSelector::select(5_000),
        RowSelector::skip(30_000),
        RowSelector::select(100),
    ]);
    assert_eq!(read(&ours, Some(sel.clone())), read(&theirs, Some(sel)));
}

#[test]
fn statistics_match_arrow_rs_and_page_indexes_match_the_data() {
    let d = data();
    let ours = write_parquet(&d, Vec::new(), 1, Some(props())).unwrap();
    let theirs = arrow_rs(&d);
    let open = |f: &[u8]| {
        let opts = ReadOptionsBuilder::new().with_page_index().build();
        SerializedFileReader::new_with_options(Bytes::from(f.to_vec()), opts).unwrap()
    };
    let (a, b) = (open(&ours), open(&theirs));
    let (a, b) = (a.metadata(), b.metadata());
    assert_eq!(a.num_row_groups(), b.num_row_groups());
    for (ga, gb) in a.row_groups().iter().zip(b.row_groups()) {
        assert_eq!(ga.num_rows(), gb.num_rows());
        for (ca, cb) in ga.columns().iter().zip(gb.columns()) {
            let name = ca.column_path().string();
            assert_eq!(ca.statistics(), cb.statistics(), "{name}");
            assert_eq!(ca.num_values(), cb.num_values(), "{name}");
            assert_eq!(
                ca.definition_level_histogram(),
                cb.definition_level_histogram(),
                "{name}"
            );
        }
    }
    // page indexes: each page's min, max and null count are its rows'
    let all = read(&ours, None);
    let mut start = 0;
    for g in 0..a.num_row_groups() {
        let pi = a.page_index_for_row_group(g);
        let rows = a.row_group(g).num_rows() as usize;
        for c in 0..a.row_group(g).num_columns() {
            let ci = pi.column_index(c).expect("column index");
            let pages = pi.offset_index(c).expect("offset index").page_locations();
            let col = raw(all.column(c).as_ref());
            for (p, loc) in pages.iter().enumerate() {
                let from = start + loc.first_row_index as usize;
                let to = pages
                    .get(p + 1)
                    .map_or(start + rows, |n| start + n.first_row_index as usize);
                let vals: Vec<i64> = col[from..to].iter().flatten().copied().collect();
                assert_eq!(ci.null_count(p), Some((to - from - vals.len()) as i64));
                assert_eq!(ci.is_null_page(p), vals.is_empty());
                let (lo, hi) = (vals.iter().min(), vals.iter().max());
                let (want_lo, want_hi) = match ci {
                    ColumnIndexMetaData::INT32(x) => (
                        x.min_value(p).map(|&v| v as i64),
                        x.max_value(p).map(|&v| v as i64),
                    ),
                    ColumnIndexMetaData::INT64(x) => {
                        (x.min_value(p).copied(), x.max_value(p).copied())
                    }
                    _ => continue,
                };
                assert_eq!(
                    (lo.copied(), hi.copied()),
                    (want_lo, want_hi),
                    "group {g} column {c} page {p}"
                );
            }
        }
        start += rows;
    }
}

/// A column's raw integer values (unscaled for decimals).
fn raw(a: &dyn Array) -> Vec<Option<i64>> {
    let get = |i| -> i64 {
        match a.data_type() {
            DataType::Int32 => a.as_primitive::<Int32Type>().value(i) as i64,
            DataType::Date32 => a.as_primitive::<Date32Type>().value(i) as i64,
            DataType::Int64 => a.as_primitive::<Int64Type>().value(i),
            DataType::Decimal128(..) => a.as_primitive::<Decimal128Type>().value(i) as i64,
            _ => 0,
        }
    };
    (0..a.len())
        .map(|i| a.is_valid(i).then(|| get(i)))
        .collect()
}

#[test]
fn no_dictionary_and_no_statistics() {
    let d = data();
    let props = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .set_statistics_enabled(EnabledStatistics::None)
        .set_compression(parquet::basic::Compression::UNCOMPRESSED)
        .build();
    let ours = write_parquet(&d, Vec::new(), 1, Some(props)).unwrap();
    assert_eq!(read(&ours, None), read(&arrow_rs(&d), None));
}
