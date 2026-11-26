use populate_core::RowCount;
use populate_gen_tpcc::Tpcc;

#[test]
fn native_keys_money_timestamps_and_strings() {
    let t = Tpcc::new(2).seed(42).load_time(123_456);
    let w = t.warehouse().batch(0, 2);
    assert_eq!(w.0, [1, 2]);
    assert!(w.1.get(0).len() >= 6);
    assert_eq!(w.4[0].0, 30_000_000);
    assert_eq!(t.district().batch(9, 2).0, [10, 1]);
    assert_eq!(t.customer().batch(0, 1).1.1[0].0, 123_456);
    assert_eq!(t.history().batch(0, 1).5[0].0, 123_456);
    assert_eq!(t.new_order().batch(0, 2).0, [2101, 2102]);
    assert_eq!(t.orders().batch(0, 2).0, [1, 2]);
    assert_eq!(t.item().batch(0, 2).0, [1, 2]);
    assert_eq!(t.stock().batch(99_999, 2).0.0, [100_000, 1]);
    let lines = t.order_line().batch(0, 2);
    assert!((10..=30).contains(&lines.values.rows()));
    assert!(lines.values.6.valid.get(0));
    let pending = t.order_line().batch(2100, 1);
    assert!(!pending.values.6.valid.get(0));
    let source = t.district().chunk_rows(3).batch_rows(2);
    let mut ids = Vec::new();
    source.chunk(2, |c| ids.extend(c.0));
    assert_eq!(ids, [7, 8, 9]);
}

#[cfg(feature = "formats")]
mod adapters {
    use super::*;
    use arrow_select::concat::concat_batches;
    use populate_gen_tpcc::TpccTableName;
    use populate_sink_arrow::{into_record_batch_with, schema_of};
    use std::sync::Arc;

    #[test]
    fn all_nine_tables_match() {
        let t = Tpcc::new(2).seed(42);
        macro_rules! check {
            ($method:ident, $table:ident) => {{
                let raw = t.$method();
                let n = raw.rows().min(10);
                let schema = Arc::new(schema_of(raw.dist()));
                let actual = into_record_batch_with(&schema, raw.batch(0, n)).unwrap();
                let mut expected = Vec::new();
                t.table(TpccTableName::$table)
                    .limit_source_rows(n)
                    .for_each_batch(1, |b| expected.push(b));
                assert_eq!(
                    actual,
                    concat_batches(&schema, &expected).unwrap(),
                    stringify!($table)
                );
            }};
        }
        check!(warehouse, Warehouse);
        check!(district, District);
        check!(customer, Customer);
        check!(history, History);
        check!(new_order, NewOrder);
        check!(orders, Orders);
        check!(order_line, OrderLine);
        check!(item, Item);
        check!(stock, Stock);
    }
}
