use populate_core::RowCount;
use populate_gen_tpch::Tpch;

#[test]
fn raw_values_and_strings_are_accessible_without_an_adapter() {
    let h = Tpch::new(0.01);
    let nation = h.nation().batch(0, 3);
    assert_eq!(nation.0, [0, 1, 2]);
    nation.1.with_str(0, |s| assert_eq!(s, "ALGERIA"));
    assert!(nation.3.0[0].as_str().starts_with(" haggle."));
    let source = h.nation().chunk_rows(3).batch_rows(2);
    let mut ids = Vec::new();
    source.chunk(1, |c| ids.extend(c.0));
    assert_eq!(ids, [3, 4, 5]);
    assert_eq!(h.region().batch(0, 2).0, [0, 1]);
    assert_eq!(h.part().batch(0, 2).0, [1, 2]);
    assert_eq!(h.supplier().batch(0, 2).0, [1, 2]);
    assert_eq!(h.customer().batch(0, 2).0, [1, 2]);
    assert_eq!(h.partsupp().batch(0, 2).0.values, [1, 1, 1, 1, 2, 2, 2, 2]);
    assert_eq!(h.orders().batch(0, 2).0, [1, 2]);
    let lines = h.lineitem().batch(0, 2);
    assert!(lines.rows() >= 2);
    assert!(lines.values.0.0.iter().all(|k| *k == 1 || *k == 2));
}

#[cfg(feature = "formats")]
mod adapters {
    use super::*;
    use arrow_select::concat::concat_batches;
    use populate_gen_tpch::TableName;
    use populate_sink_arrow::{into_record_batch_with, schema_of};
    use std::sync::Arc;

    #[test]
    fn every_table_matches_the_existing_adapter() {
        let h = Tpch::new(0.01);
        macro_rules! check {
            ($method:ident, $name:ident) => {{
                let raw = h.$method();
                let n = raw.rows().min(10);
                let schema = Arc::new(schema_of(raw.dist()));
                let actual = into_record_batch_with(&schema, raw.batch(0, n)).unwrap();
                let expected = h.table(TableName::$name).limit_source_rows(n).collect(1);
                assert_eq!(
                    actual,
                    concat_batches(&schema, &expected).unwrap(),
                    stringify!($name)
                );
            }};
        }
        check!(nation, Nation);
        check!(region, Region);
        check!(part, Part);
        check!(supplier, Supplier);
        check!(customer, Customer);
        check!(partsupp, PartSupp);
        check!(orders, Orders);
        check!(lineitem, LineItem);
    }
}
