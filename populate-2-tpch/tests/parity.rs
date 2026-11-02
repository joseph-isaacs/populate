//! Value-for-value parity with tpchgen-arrow: whole RecordBatches must be
//! equal (schema, types, every value).

use arrow_array::RecordBatch;
use populate_2_tpch::{Customer, Nation, Part, PartSupp, Region, Supplier, Table, batches};
use tpchgen::generators::{
    CustomerGenerator, NationGenerator, PartGenerator, PartSuppGenerator, RegionGenerator,
    SupplierGenerator,
};
use tpchgen_arrow::{
    CustomerArrow, NationArrow, PartArrow, PartSuppArrow, RegionArrow, SupplierArrow,
};

fn assert_same<T: Table>(sf: f64, batch_size: usize, reference: impl Iterator<Item = RecordBatch>) {
    let ours: Vec<RecordBatch> = batches::<T>(sf, batch_size).collect();
    let theirs: Vec<RecordBatch> = reference.collect();
    assert_eq!(ours.len(), theirs.len(), "batch count");
    let mut rows = 0;
    for (i, (o, t)) in ours.iter().zip(&theirs).enumerate() {
        assert_eq!(o.schema(), t.schema(), "schema, batch {i}");
        for c in 0..o.num_columns() {
            assert_eq!(
                o.column(c).as_ref(),
                t.column(c).as_ref(),
                "batch {i}, column {}",
                o.schema().field(c).name()
            );
        }
        assert_eq!(o, t, "batch {i}");
        rows += o.num_rows();
    }
    assert_eq!(rows as i64, T::row_count(sf));
}

#[test]
fn region() {
    assert_same::<Region>(1.0, 8000, RegionArrow::new(RegionGenerator::new(1.0, 1, 1)));
}

#[test]
fn nation() {
    assert_same::<Nation>(1.0, 8000, NationArrow::new(NationGenerator::new(1.0, 1, 1)));
}

#[test]
fn part() {
    let sf = 0.1;
    assert_same::<Part>(sf, 8000, PartArrow::new(PartGenerator::new(sf, 1, 1)));
}

#[test]
fn part_small_batches() {
    let sf = 0.01;
    assert_same::<Part>(
        sf,
        333,
        PartArrow::new(PartGenerator::new(sf, 1, 1)).with_batch_size(333),
    );
}

#[test]
fn supplier() {
    // sf 1: 10k suppliers, so ~10 get BBB comments spliced in
    let sf = 1.0;
    assert_same::<Supplier>(
        sf,
        8000,
        SupplierArrow::new(SupplierGenerator::new(sf, 1, 1)),
    );
}

#[test]
fn customer() {
    let sf = 0.1;
    assert_same::<Customer>(
        sf,
        8000,
        CustomerArrow::new(CustomerGenerator::new(sf, 1, 1)),
    );
}

#[test]
fn partsupp() {
    let sf = 0.1;
    assert_same::<PartSupp>(
        sf,
        8000,
        PartSuppArrow::new(PartSuppGenerator::new(sf, 1, 1)),
    );
}

#[test]
fn partsupp_small_batches() {
    let sf = 0.01;
    assert_same::<PartSupp>(
        sf,
        100,
        PartSuppArrow::new(PartSuppGenerator::new(sf, 1, 1)).with_batch_size(100),
    );
}
