//! `write_dist_parquet`: the values of each row group are the ones the same
//! distribution collects into Arrow with the same RNG, the file does not
//! depend on the thread count, and settings the fast encoder does not cover
//! go through the `parquet` crate's writer with the same values.

use arrow_array::ArrayRef;
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::serialized_reader::ReadOptionsBuilder;
use parquet::file::statistics::Statistics;
use populate_builtins::{choose, constant, list, range, text, uniform};
use populate_core::Distribution;
use populate_sink_arrow::IntoArrowColumns;
use populate_table::direct::group_rng;
use populate_table::{DirectColumns, ParquetOptions, write_dist_parquet};

const WORDS: &[&str] = &["alpha", "beta", "gamma", "delta", "a much longer word"];

type Row = (
    i64,
    i32,
    f64,
    &'static str,
    String,
    Vec<i64>,
    Vec<f32>,
    Vec<i32>,
);

fn dist() -> impl Distribution<Item = Row> + Clone + Sync {
    (
        range(0i64, 1),
        uniform(0i32, 100),
        uniform(-1.0f64, 1.0),
        choose(&["x", "yy", "zzz", "a string longer than twelve"]),
        text(WORDS, 0, 6),
        list(uniform(0usize, 6), uniform(-1000i64, 1000)),
        list(uniform(0usize, 3), uniform(0.0f32, 1.0)),
        list(constant(2usize), range(0i32, 1)),
    )
}

fn opts(threads: usize, props: WriterProperties) -> ParquetOptions {
    ParquetOptions {
        rows_per_group: 3_000,
        threads,
        seed: 7,
        props,
    }
}

fn small_pages() -> WriterProperties {
    WriterProperties::builder()
        .set_data_page_row_count_limit(500)
        .set_write_batch_size(128)
        .set_statistics_enabled(EnabledStatistics::Page)
        .build()
}

/// Every value of every column, as text.
fn show(cols: &[ArrayRef]) -> Vec<Vec<String>> {
    let o = FormatOptions::default();
    cols.iter()
        .map(|a| {
            let f = ArrayFormatter::try_new(a.as_ref(), &o).unwrap();
            (0..a.len()).map(|i| f.value(i).to_string()).collect()
        })
        .collect()
}

fn read_groups(bytes: &[u8]) -> Vec<Vec<Vec<String>>> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(bytes))
        .unwrap()
        .with_batch_size(3_000)
        .build()
        .unwrap();
    reader
        .map(|b| show(b.unwrap().columns()))
        .collect::<Vec<_>>()
}

/// Row group `g` collected into Arrow arrays, as the existing path does.
fn reference(rows: u64, per: u64, seed: u64) -> Vec<Vec<Vec<String>>> {
    (0..rows.div_ceil(per))
        .map(|g| {
            let mut d = dist();
            d.skip(g * per);
            let len = (rows - g * per).min(per) as usize;
            let mut arrays = Vec::new();
            d.collect(&mut group_rng(seed, g), len)
                .into_arrays(&mut arrays);
            show(&arrays)
        })
        .collect()
}

fn write(rows: u64, o: &ParquetOptions) -> Vec<u8> {
    write_dist_parquet(&dist(), rows, Vec::new(), o).unwrap()
}

#[test]
fn values_match_the_arrow_columns() {
    let rows = 10_000;
    for props in [WriterProperties::default(), small_pages()] {
        let got = read_groups(&write(rows, &opts(4, props)));
        assert_eq!(got.len(), 4);
        assert_eq!(got, reference(rows, 3_000, 7));
    }
}

#[test]
fn same_file_for_any_thread_count() {
    let one = write(20_000, &opts(1, small_pages()));
    for threads in [2, 3, 8] {
        assert!(
            one == write(20_000, &opts(threads, small_pages())),
            "{threads}"
        );
    }
}

#[test]
fn fallback_settings_give_the_same_values() {
    let fast = read_groups(&write(7_000, &opts(2, small_pages())));
    // v2 pages: every column falls back
    let v2 = WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .build();
    // dictionaries off: lists use the fast path, others PLAIN
    let plain = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .build();
    for props in [v2, plain] {
        assert_eq!(read_groups(&write(7_000, &opts(2, props))), fast);
    }
}

/// Chunk statistics and page indexes of the fast list encoder agree with
/// the `parquet` crate's writer (v2 pages force it).
#[test]
fn list_statistics_match_the_parquet_writer() {
    let meta = |props: WriterProperties| {
        let bytes = Bytes::from(write(9_000, &opts(1, props)));
        let r = SerializedFileReader::new_with_options(
            bytes,
            ReadOptionsBuilder::new().with_page_index().build(),
        )
        .unwrap();
        let m = r.metadata();
        let mut out = Vec::new();
        for (i, g) in m.row_groups().iter().enumerate() {
            let pi = m.page_index_for_row_group(i);
            for c in 5..8 {
                let col = g.column(c);
                let nulls = col.statistics().and_then(Statistics::null_count_opt);
                // the pages' null counts add up to the chunk's, and their
                // rows to the group's
                let ci = pi.column_index(c).expect("column index");
                let pages = pi.offset_index(c).expect("offset index").page_locations();
                let page_nulls: i64 = (0..pages.len()).map(|p| ci.null_count(p).unwrap()).sum();
                assert_eq!(Some(page_nulls as u64), nulls);
                assert_eq!(pages[0].first_row_index, 0);
                assert!(pages.last().unwrap().first_row_index < g.num_rows());
                out.push((
                    g.num_rows(),
                    col.num_values(),
                    nulls,
                    col.definition_level_histogram().cloned(),
                    col.repetition_level_histogram().cloned(),
                ));
            }
        }
        out
    };
    for dict in [false, true] {
        let fast = WriterProperties::builder()
            .set_dictionary_enabled(dict)
            .set_data_page_row_count_limit(700)
            .build();
        let slow = WriterProperties::builder()
            .set_dictionary_enabled(dict)
            .set_writer_version(WriterVersion::PARQUET_2_0)
            .build();
        assert_eq!(meta(fast), meta(slow), "dictionary {dict}");
    }
}

#[test]
fn list_min_max_match_the_parquet_writer() {
    let stats = |props: WriterProperties| {
        let bytes = Bytes::from(write(9_000, &opts(1, props)));
        let r = SerializedFileReader::new(bytes).unwrap();
        let mut out = Vec::new();
        for g in r.metadata().row_groups() {
            for c in 5..8 {
                let s = g.column(c).statistics().unwrap();
                out.push((
                    s.min_bytes_opt().map(<[u8]>::to_vec),
                    s.max_bytes_opt().map(<[u8]>::to_vec),
                    s.null_count_opt(),
                ));
            }
        }
        out
    };
    for dict in [false, true] {
        let fast = WriterProperties::builder()
            .set_dictionary_enabled(dict)
            .build();
        let slow = WriterProperties::builder()
            .set_dictionary_enabled(dict)
            .set_writer_version(WriterVersion::PARQUET_2_0)
            .build();
        assert_eq!(stats(fast), stats(slow), "dictionary {dict}");
    }
}

#[test]
fn empty_and_tiny() {
    let bytes = write(0, &opts(4, WriterProperties::default()));
    let r = SerializedFileReader::new(Bytes::from(bytes)).unwrap();
    assert_eq!(r.metadata().num_row_groups(), 0);
    let got = read_groups(&write(1, &opts(4, WriterProperties::default())));
    assert_eq!(got, reference(1, 3_000, 7));
}

#[test]
fn columns_are_named_in_order() {
    let bytes = write(10, &opts(1, WriterProperties::default()));
    let b = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes)).unwrap();
    let names: Vec<String> = b
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    assert_eq!(names, ["c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7"]);
    assert_eq!(<populate_core::Columns<Row> as DirectColumns>::COLUMNS, 8);
}
