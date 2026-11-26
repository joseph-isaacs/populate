use populate_core::RowCount;
use populate_gen_ssb::Ssb;

#[test]
fn native_dimensions_and_joined_strings() {
    let s = Ssb::new(0.01);
    assert_eq!(s.part().batch(0, 2).0, [1, 2]);
    assert_eq!(s.supplier().batch(0, 1).1.get(0), "Supplier#000000001");
    assert_eq!(s.customer().batch(0, 1).0.1.get(0), "Customer#000000001");
    let date = s.date().batch(0, 1);
    assert_eq!(date.0.0, [19920101]);
    date.0.1.with_str(0, |v| assert_eq!(v, "January 1, 1992"));
    let lines = s.lineorder().batch(0, 2);
    let flat = s.lineorder_flat().batch(0, 2);
    assert_eq!(lines.values.0.0, flat.values.0.0.0);
    assert!(flat.values.rows() >= 2);
    flat.values
        .1
        .0
        .0
        .with_str(0, |v| assert!(v.starts_with("Customer#")));
    flat.values
        .1
        .1
        .0
        .with_str(0, |v| assert!(v.starts_with("Supplier#")));
    let raw = s.part().chunk_rows(4).batch_rows(3);
    let mut keys = Vec::new();
    raw.chunk(1, |c| keys.extend(c.0));
    assert_eq!(keys, [5, 6, 7, 8]);
}

#[cfg(feature = "formats")]
mod adapters {
    use super::*;
    use arrow_select::concat::concat_batches;
    use populate_gen_ssb::TableName;
    use populate_sink_arrow::{into_record_batch_with, schema_of};
    use std::sync::Arc;

    #[test]
    fn every_table_including_flat_matches() {
        let s = Ssb::new(0.01);
        macro_rules! check {
            ($method:ident, $name:ident) => {{
                let raw = s.$method();
                let n = raw.rows().min(10);
                let schema = Arc::new(schema_of(raw.dist()));
                let actual = into_record_batch_with(&schema, raw.batch(0, n)).unwrap();
                let expected = s.table(TableName::$name).limit_source_rows(n).collect(1);
                assert_eq!(
                    actual,
                    concat_batches(&schema, &expected).unwrap(),
                    stringify!($name)
                );
            }};
        }
        check!(part, Part);
        check!(supplier, Supplier);
        check!(customer, Customer);
        check!(date, Date);
        check!(lineorder, LineOrder);
        check!(lineorder_flat, LineOrderFlat);
    }
}
