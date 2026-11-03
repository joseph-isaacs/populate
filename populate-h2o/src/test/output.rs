use crate::csv::fwrite_f64;
use crate::{FastSampler, GroupByConfig, GroupByTable};
use arrow_array::cast::AsArray;
use arrow_schema::DataType;
use rand::SeedableRng;
use rand::rngs::StdRng;

/// Probed from `fwrite(data.table(x = c(...)))` in R.
#[test]
fn fwrite_double_format() {
    let cases = [
        (0.000123, "0.000123"),
        (1e-6, "1e-06"),
        (0.0001, "1e-04"),
        (0.00012345, "0.00012345"),
        (1e-5, "1e-05"),
        (0.001, "0.001"),
        (0.0012, "0.0012"),
        (100.0, "100"),
        (99.999999, "99.999999"),
        (1.0, "1"),
        (0.5, "0.5"),
        (12_345_678.5, "12345678.5"),
        (1e15, "1e+15"),
        (1.5e-7, "1.5e-07"),
        (0.1 + 0.2, "0.3"),
        (1.0 / 3.0, "0.333333333333333"),
    ];
    for (x, want) in cases {
        let mut s = String::new();
        fwrite_f64(x, &mut s);
        assert_eq!(s, want, "{x}");
    }
}

#[test]
fn groupby_record_batch() {
    let t = GroupByTable::generate(
        GroupByConfig::new(1000, 10, 5, false),
        FastSampler,
        &mut StdRng::seed_from_u64(1),
    );
    let rb = t.into_table().into_record_batch().unwrap();
    let schema = rb.schema();
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(
        names,
        vec!["id1", "id2", "id3", "id4", "id5", "id6", "v1", "v2", "v3"]
    );
    let types: Vec<&DataType> = schema.fields().iter().map(|f| f.data_type()).collect();
    assert_eq!(types[0], &DataType::Utf8);
    assert_eq!(types[3], &DataType::Int32);
    assert_eq!(types[8], &DataType::Float64);
    assert_eq!(rb.num_rows(), 1000);
    // 5% of 1000 rows of v1 are null.
    assert_eq!(rb.column(6).null_count(), 50);
    assert!(
        rb.column(0)
            .as_string::<i32>()
            .iter()
            .flatten()
            .all(|s| s.len() == 5)
    );
}
