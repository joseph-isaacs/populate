use arrow_array::cast::AsArray;
use arrow_array::types::{Date32Type, Decimal64Type, Decimal128Type, Int32Type, Int64Type};
use arrow_array::{Array, RecordBatch};
use arrow_schema::DataType;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::properties::WriterProperties;
use populate_builtins::{choose, list, text, uniform, weighted};
use populate_table::{DecimalRepr, Stream, Table};

fn sales(rows: u64) -> Table {
    let s = Stream::new(0, "sales");
    Table::new("sales", rows)
        .key("id")
        .col("store", choose(&["north", "south", "east"]))
        .col("sold", uniform(10_000i32, 10_365))
        .date()
        .col("qty", uniform(1i64, 10))
        .col("price", uniform(100i64, 9_999))
        .decimal(7, 2)
        .derive2("total", ["qty", "price"], |q: i64, p: i64| q * p)
        .decimal(9, 2)
        .col("_secret", uniform(0i64, 9))
        .hidden()
        .derive("secret_plus_one", "_secret", |x: i64| x + 1)
        .row("bucket", move |r| s.below(r, 4) as i32)
        .row_str("code", |r, b| {
            b.push_str("C-");
            b.push_u64(r);
        })
        .col("note", text(&["a", "bb", "ccc"], 0, 3))
        .nullable(0.25)
        .col("odd_null", weighted(&[(1i32, 1), (2, 3)]))
        .null_if(|r| r % 2 == 1)
}

#[test]
fn schema_and_values() {
    let t = sales(1000);
    let s = t.schema();
    let names: Vec<_> = s.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(
        names,
        [
            "id",
            "store",
            "sold",
            "qty",
            "price",
            "total",
            "secret_plus_one",
            "bucket",
            "code",
            "note",
            "odd_null"
        ]
    );
    assert_eq!(s.field(2).data_type(), &DataType::Date32);
    assert_eq!(s.field(4).data_type(), &DataType::Decimal128(7, 2));
    assert!(s.field(9).is_nullable() && s.field(10).is_nullable() && !s.field(0).is_nullable());

    let b = t.collect();
    let id = b.column(0).as_primitive::<Int64Type>();
    assert_eq!(
        id.values().iter().copied().collect::<Vec<_>>(),
        (1..=1000).collect::<Vec<_>>()
    );
    let sold = b.column(2).as_primitive::<Date32Type>();
    assert!(sold.values().iter().all(|d| (10_000..=10_365).contains(d)));
    let qty = b.column(3).as_primitive::<Int64Type>();
    let price = b.column(4).as_primitive::<Decimal128Type>();
    let total = b.column(5).as_primitive::<Decimal128Type>();
    for i in 0..1000 {
        assert_eq!(total.value(i), qty.value(i) as i128 * price.value(i));
    }
    let plus = b.column(6).as_primitive::<Int64Type>();
    assert!(plus.values().iter().all(|v| (1..=10).contains(v)));
    assert_eq!(b.column(8).as_string::<i32>().value(42), "C-42");
    let nulls = b.column(9).null_count();
    assert!((150..350).contains(&nulls), "{nulls}");
    let odd = b.column(10).as_primitive::<Int32Type>();
    for i in 0..1000 {
        assert_eq!(odd.is_null(i), i % 2 == 1);
    }
}

#[test]
fn batches_are_independent_of_order_and_threads() {
    let t = sales(10_000);
    let seq: Vec<RecordBatch> = t.batches(1000).collect();
    assert_eq!(seq.len(), 10);
    assert_eq!(t.batch(7, 1000), seq[7]);
    for threads in [2, 3, 8] {
        let mut par = Vec::new();
        t.for_each_batch(1000, threads, |b| par.push(b));
        assert_eq!(par, seq, "threads = {threads}");
    }
    // the last batch is short
    assert_eq!(
        sales(2500)
            .batches(1000)
            .map(|b| b.num_rows())
            .collect::<Vec<_>>(),
        [1000, 1000, 500]
    );
    // row columns don't depend on the batch size at all
    let a = sales(4000).collect();
    let b: Vec<_> = sales(4000).batches(999).collect();
    let codes: Vec<_> = b
        .iter()
        .flat_map(|b| {
            b.column(8)
                .as_string::<i32>()
                .iter()
                .map(|v| v.unwrap().to_owned())
                .collect::<Vec<_>>()
        })
        .collect();
    let all: Vec<_> = a
        .column(8)
        .as_string::<i32>()
        .iter()
        .map(|v| v.unwrap().to_owned())
        .collect();
    assert_eq!(codes, all);
}

#[test]
fn early_error_stops_workers() {
    let t = sales(100_000);
    let mut seen = 0;
    let r: Result<(), &str> = t.try_for_each_batch(100, 4, |_| {
        seen += 1;
        if seen == 3 { Err("stop") } else { Ok(()) }
    });
    assert_eq!(r, Err("stop"));
    assert_eq!(seen, 3);
}

#[test]
fn decimal64_is_a_reinterpretation() {
    let t = sales(100).decimal_repr(DecimalRepr::Decimal64);
    let b = t.collect();
    assert_eq!(b.schema().field(4).data_type(), &DataType::Decimal64(7, 2));
    let p = b.column(4).as_primitive::<Decimal64Type>();
    assert!(p.values().iter().all(|v| (100..=9_999).contains(v)));
}

#[test]
fn tuple_and_list_distributions_become_struct_and_list_columns() {
    let t = Table::new("nested", 100)
        .col("pair", (uniform(0i32, 9), choose(&["x", "y"])))
        .col("tags", list(uniform(0usize, 3), uniform(0i64, 5)));
    let b = t.collect();
    assert!(matches!(b.schema().field(0).data_type(), DataType::Struct(f) if f.len() == 2));
    assert!(matches!(b.schema().field(1).data_type(), DataType::List(_)));
}

#[test]
#[should_panic(expected = "not declared before it")]
fn derive_from_unknown_column_panics_at_declaration() {
    let _ = Table::new("t", 1).derive("y", "x", |x: i64| x);
}

#[test]
#[should_panic(expected = "input `x` is Int32, but the closure takes Int64")]
fn derive_type_mismatch_is_reported() {
    let t = Table::new("t", 1)
        .col("x", uniform(0i32, 1))
        .derive("y", "x", |x: i64| x);
    t.schema();
}

#[test]
fn parquet_is_identical_for_any_thread_count_and_reads_back() {
    let t = sales(50_000);
    let props = || {
        Some(
            WriterProperties::builder()
                .set_max_row_group_row_count(Some(8_000))
                .build(),
        )
    };
    let one = t.write_parquet(Vec::new(), 4_000, 1, props()).unwrap();
    let four = t.write_parquet(Vec::new(), 4_000, 4, props()).unwrap();
    assert_eq!(one, four);

    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(one))
        .unwrap()
        .with_batch_size(4_000)
        .build()
        .unwrap();
    let back: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
    let want: Vec<RecordBatch> = t.batches(4_000).collect();
    assert_eq!(back.len(), want.len());
    for (a, b) in back.iter().zip(&want) {
        assert_eq!(a.columns(), b.columns());
    }
}
