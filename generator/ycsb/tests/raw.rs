#[cfg(feature = "formats")]
use arrow_array::RecordBatch;
#[cfg(feature = "formats")]
use populate_core::Distribution;
use populate_core::RowCount;
use populate_gen_ycsb::{Ycsb, YcsbRun};
#[cfg(feature = "formats")]
use populate_sink_arrow::IntoArrowColumns;
#[cfg(feature = "formats")]
use populate_table::{DistTable, chunks::for_each_batch};

#[test]
fn load_strings_and_chunk_positions_are_accessible() {
    let y = Ycsb::new(11)
        .ordered(true)
        .field_count(2)
        .field_length(40)
        .data_integrity(true)
        .chunk_rows(4)
        .batch_size(3);
    let raw = y.raw();
    assert_eq!(raw.num_chunks(), 3);
    let c = raw.batch(2, 2);
    assert_eq!(c.0.get(0), "user2");
    assert!(c.1.0[0].get(0).starts_with("user2:field0:"));
    assert!(c.1.0[1].get(1).starts_with("user3:field1:"));
    let mut keys = Vec::new();
    raw.for_each_batch(2, |c| keys.extend((0..c.0.rows()).map(|i| c.0.get(i))));
    assert_eq!(
        keys,
        (0..11).map(|i| format!("user{i}")).collect::<Vec<_>>()
    );
}

#[test]
fn operations_expose_children_and_nulls() {
    let run = YcsbRun::workload('f', Ycsb::new(100).field_count(2), 11)
        .proportions(0.0, 0.0, 0.0, 0.0, 1.0)
        .chunk_ops(4)
        .batch_size(3);
    let raw = run.raw();
    let c = raw.batch(3, 2);
    assert_eq!(c.sources, 2);
    assert_eq!(c.values.rows(), 4);
    assert_eq!(c.values.0.get(0), "READ");
    assert_eq!(c.values.0.get(1), "UPDATE");
    assert_eq!(c.values.1.get(0), c.values.1.get(1));
    assert_eq!(c.values.4.0.iter().filter(|f| f.valid.get(1)).count(), 1);
    assert!(c.values.4.0.iter().all(|f| !f.valid.get(0)));
    let mut rows = 0;
    raw.for_each_batch(2, |c| rows += c.values.rows());
    assert_eq!(rows, 22);
}

#[cfg(feature = "formats")]
#[test]
fn native_values_match_both_adapters() {
    fn compare<D>(dist: D, n: u64, expected: Vec<RecordBatch>)
    where
        D: Distribution + Clone + Send + Sync + 'static,
        populate_core::Columns<D::Item>: IntoArrowColumns + Send,
    {
        let source = DistTable::new("raw", n, dist).batch_rows(3).chunk_rows(4);
        let mut actual = Vec::new();
        for_each_batch(&source, 1, |b| actual.push(b));
        assert_eq!(actual, expected);
    }
    for integrity in [false, true] {
        let y = Ycsb::new(11)
            .field_count(2)
            .data_integrity(integrity)
            .chunk_rows(4)
            .batch_size(3);
        let mut expected = Vec::new();
        y.for_each_batch(1, |b| expected.push(b));
        compare(y.dist(), 11, expected);
        for workload in 'a'..='f' {
            let run = YcsbRun::workload(workload, y.clone(), 11)
                .chunk_ops(4)
                .batch_size(3);
            let mut expected = Vec::new();
            run.for_each_batch(1, |b| expected.push(b));
            compare(run.dist(), 11, expected);
        }
    }
}
