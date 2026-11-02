//! Value-for-value parity with tpcdsgen-arrow: whole RecordBatches equal.

use arrow_array::{Array, RecordBatch};
use populate_2_tpcds::tables::Scale;
use populate_2_tpcds::{CustomerDemographics, Inventory, Table, batches};
use tpcdsgen::config::{Session, SessionBuilder};
use tpcdsgen_arrow::{CustomerDemographicsArrow, InventoryArrow};

fn session(sf: f64) -> Session {
    SessionBuilder::new().with_scale_factor(sf).build().unwrap()
}

fn assert_same<T: Table>(
    sf: f64,
    batch_size: usize,
    reference: impl Iterator<Item = Result<RecordBatch, arrow_schema::ArrowError>>,
) {
    let ours: Vec<RecordBatch> = batches::<T>(sf, batch_size).collect();
    let theirs: Vec<RecordBatch> = reference.collect::<Result<_, _>>().unwrap();
    assert_eq!(ours.len(), theirs.len(), "batch count");
    let mut nulls = 0;
    for (i, (o, t)) in ours.iter().zip(&theirs).enumerate() {
        assert_eq!(o.schema(), t.schema(), "schema, batch {i}");
        for c in 0..o.num_columns() {
            assert_eq!(
                o.column(c).as_ref(),
                t.column(c).as_ref(),
                "batch {i}, column {}",
                o.schema().field(c).name()
            );
            nulls += t.column(c).null_count();
        }
        assert_eq!(o, t);
    }
    let rows: usize = ours.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows as i64, T::row_count(&Scale::new(sf)));
    println!("{rows} rows, {nulls} nulls");
}

#[test]
fn inventory_sf1() {
    assert_same::<Inventory>(1.0, 8000, InventoryArrow::new(session(1.0)));
}

#[test]
fn inventory_odd_batches() {
    assert_same::<Inventory>(
        1.0,
        7777,
        InventoryArrow::new(session(1.0)).with_batch_size(7777),
    );
}

#[test]
fn customer_demographics_sf1() {
    assert_same::<CustomerDemographics>(1.0, 8000, CustomerDemographicsArrow::new(session(1.0)));
}

#[test]
fn customer_demographics_odd_batches() {
    assert_same::<CustomerDemographics>(
        1.0,
        999,
        CustomerDemographicsArrow::new(session(1.0)).with_batch_size(999),
    );
}
