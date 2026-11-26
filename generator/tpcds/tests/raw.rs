use populate_core::RowCount;
use populate_gen_tpcds::Tpcds;

#[test]
fn raw_fields_nulls_and_chunk_positions_are_accessible() {
    let ds = Tpcds::new(1.0);
    let source = ds.income_band().chunk_rows(3).batch_rows(2);
    let mut ids = Vec::new();
    source.chunk(1, |cols| ids.extend(cols.0));
    assert_eq!(ids, [4, 5, 6]);
    let inventory = ds.inventory().batch(0, 7);
    assert_eq!(inventory.0.0.len(), 7);
    assert_eq!(inventory.1.rows(), 7);
    let sales = ds.store_sales_returns().batch(0, 10);
    let cols = &sales.tables.0.values;
    assert!(cols.rows() >= 10);
    for i in 0..cols.rows() {
        assert_eq!(cols.is_null(i, 0), cols.nbm[i] & 1 != 0);
    }
    let text = ds.ship_mode().batch(0, 1);
    assert_eq!(text.0, [1]);
    assert!(!text.1.get(0).is_empty());
}

#[cfg(feature = "formats")]
mod adapters {
    use super::*;
    use arrow_array::RecordBatch;
    use arrow_select::concat::concat_batches;
    use populate_gen_tpcds::TableName;
    use populate_sink_arrow::{IntoArrowTables, into_record_batch_with, schema_of, schemas_of};
    use std::sync::Arc;

    #[test]
    fn every_raw_table_matches_the_existing_adapter() {
        let ds = Tpcds::new(1.0);
        macro_rules! one {
            ($method:ident, $table:ident) => {{
                let raw = ds.$method();
                let n = raw.rows().min(10);
                let schema = Arc::new(schema_of(raw.dist()));
                let actual = into_record_batch_with(&schema, raw.batch(0, n)).unwrap();
                let expected = ds
                    .table(TableName::$table)
                    .limit_source_rows(n)
                    .batch_size(n as usize)
                    .collect(1);
                assert_eq!(expected, [actual], stringify!($table));
            }};
        }
        one!(call_center, CallCenter);
        one!(catalog_page, CatalogPage);
        one!(customer, Customer);
        one!(customer_address, CustomerAddress);
        one!(customer_demographics, CustomerDemographics);
        one!(date_dim, DateDim);
        one!(household_demographics, HouseholdDemographics);
        one!(income_band, IncomeBand);
        one!(inventory, Inventory);
        one!(item, Item);
        one!(promotion, Promotion);
        one!(reason, Reason);
        one!(ship_mode, ShipMode);
        one!(store, Store);
        one!(time_dim, TimeDim);
        one!(warehouse, Warehouse);
        one!(web_page, WebPage);
        one!(web_site, WebSite);
        macro_rules! pair {
            ($method:ident, $sales:ident, $returns:ident) => {{
                let raw = ds.$method();
                let schemas = schemas_of(raw.dist());
                let mut arrays = Vec::new();
                raw.batch(0, 10).tables.into_tables(&mut arrays);
                for ((schema, cols), name) in schemas
                    .into_iter()
                    .zip(arrays)
                    .zip([TableName::$sales, TableName::$returns])
                {
                    let actual = RecordBatch::try_new(Arc::new(schema), cols).unwrap();
                    let expected = ds
                        .table(name)
                        .limit_source_rows(10)
                        .batch_size(10)
                        .collect(1);
                    assert_eq!(
                        concat_batches(&actual.schema(), &expected).unwrap(),
                        actual,
                        "{name}"
                    );
                }
            }};
        }
        pair!(store_sales_returns, StoreSales, StoreReturns);
        pair!(catalog_sales_returns, CatalogSales, CatalogReturns);
        pair!(web_sales_returns, WebSales, WebReturns);
    }
}
