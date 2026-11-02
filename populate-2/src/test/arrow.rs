// ========================================================================
// Arrow output — collect_record_batch over the same helpers as list tests
// ========================================================================

use crate::arrow::DistributionArrow;
use crate::test::Log;
use crate::test::dist::{
    gen_list, gen_list_in_tuple, gen_list_of_struct_of_list, gen_list_of_tuples, gen_nested_list,
    gen_struct_of_list_of_struct, make_4tuple, make_tracked_list_of_struct_of_list,
};
use crate::traits::VecBackend;
use arrow_array::cast::AsArray;
use arrow_array::types::{Float64Type, Int32Type, Int64Type};
use arrow_array::{Array, ListArray, StructArray};
use arrow_schema::{DataType, Field, Fields};
use rand::rng;
use std::sync::Arc;

fn list_of(dt: DataType) -> DataType {
    DataType::List(Arc::new(Field::new_list_field(dt, false)))
}

fn struct_of(dts: &[DataType]) -> DataType {
    DataType::Struct(Fields::from(
        dts.iter()
            .enumerate()
            .map(|(i, dt)| Field::new(format!("c{i}"), dt.clone(), false))
            .collect::<Vec<_>>(),
    ))
}

fn i64s(a: &dyn Array) -> Vec<i64> {
    a.as_primitive::<Int64Type>().values().to_vec()
}

fn i32s(a: &dyn Array) -> Vec<i32> {
    a.as_primitive::<Int32Type>().values().to_vec()
}

#[test]
fn scalar_tuple_to_record_batch() {
    let mut d = make_4tuple();
    let rb = d.collect_record_batch(&mut rng(), 3).unwrap();
    assert_eq!(rb.num_rows(), 3);
    assert_eq!(rb.num_columns(), 4);
    assert_eq!(i32s(rb.column(0)), vec![1, 1, 1]);
    assert_eq!(i64s(rb.column(1)), vec![0, 10, 20]);
    assert_eq!(rb.column(2).as_boolean().true_count(), 3);
    assert_eq!(
        rb.column(3).as_primitive::<Float64Type>().values().to_vec(),
        vec![2.5; 3]
    );
}

#[test]
fn list_to_list_array() {
    let mut d = gen_list();
    let rb = d.collect_record_batch(&mut rng(), 4).unwrap();
    let l: &ListArray = rb.column(0).as_list();
    assert_eq!(l.data_type(), &list_of(DataType::Int64));
    assert_eq!(l.value_offsets(), &[0, 0, 1, 3, 6]);
    assert_eq!(i64s(l.values()), vec![0, 1, 2, 3, 4, 5]);
}

#[test]
fn list_in_tuple_to_record_batch() {
    let mut d = gen_list_in_tuple::<VecBackend>();
    let rb = d.collect_record_batch(&mut rng(), 2).unwrap();
    assert_eq!(i32s(rb.column(0)), vec![7, 7]);
    let l: &ListArray = rb.column(1).as_list();
    assert_eq!(l.value_offsets(), &[0, 2, 4]);
    assert_eq!(i64s(l.values()), vec![0, 1, 2, 3]);
}

#[test]
fn list_of_tuples_to_list_of_struct() {
    let mut d = gen_list_of_tuples();
    let rb = d.collect_record_batch(&mut rng(), 2).unwrap();
    let l: &ListArray = rb.column(0).as_list();
    assert_eq!(
        l.data_type(),
        &list_of(struct_of(&[DataType::Int64, DataType::Boolean]))
    );
    assert_eq!(l.value_offsets(), &[0, 2, 4]);
    let s: &StructArray = l.values().as_struct();
    assert_eq!(i64s(s.column(0)), vec![0, 1, 2, 3]);
    assert_eq!(s.column(1).as_boolean().true_count(), 4);
}

#[test]
fn nested_list_to_list_of_list() {
    let mut d = gen_nested_list();
    let rb = d.collect_record_batch(&mut rng(), 2).unwrap();
    let outer: &ListArray = rb.column(0).as_list();
    assert_eq!(outer.data_type(), &list_of(list_of(DataType::Int64)));
    assert_eq!(outer.value_offsets(), &[0, 2, 4]);
    let inner: &ListArray = outer.values().as_list();
    assert_eq!(inner.value_offsets(), &[0, 1, 3, 6, 10]);
    assert_eq!(i64s(inner.values()), (0..10).collect::<Vec<_>>());
}

#[test]
fn struct_of_list_of_struct_to_record_batch() {
    let mut d = gen_struct_of_list_of_struct::<VecBackend>();
    let rb = d.collect_record_batch(&mut rng(), 3).unwrap();
    assert_eq!(i32s(rb.column(0)), vec![0, 1, 2]);
    let l: &ListArray = rb.column(1).as_list();
    assert_eq!(
        l.data_type(),
        &list_of(struct_of(&[DataType::Int64, DataType::Boolean]))
    );
    assert_eq!(l.value_offsets(), &[0, 2, 4, 6]);
    let s: &StructArray = l.values().as_struct();
    assert_eq!(i64s(s.column(0)), vec![0, 1, 2, 3, 4, 5]);
    assert_eq!(s.column(1).as_boolean().true_count(), 6);
}

#[test]
fn list_of_struct_of_list_to_arrow() {
    let mut d = gen_list_of_struct_of_list::<VecBackend>();
    let rb = d.collect_record_batch(&mut rng(), 2).unwrap();
    let l: &ListArray = rb.column(0).as_list();
    assert_eq!(
        l.data_type(),
        &list_of(struct_of(&[DataType::Int32, list_of(DataType::Int64)]))
    );
    // rows: [ [{0,[0]}, {1,[1,2]}], [{2,[3,4,5]}, {3,[6,7,8,9]}] ]
    assert_eq!(l.value_offsets(), &[0, 2, 4]);
    let s: &StructArray = l.values().as_struct();
    assert_eq!(i32s(s.column(0)), vec![0, 1, 2, 3]);
    let tags: &ListArray = s.column(1).as_list();
    assert_eq!(tags.value_offsets(), &[0, 1, 3, 6, 10]);
    assert_eq!(i64s(tags.values()), (0..10).collect::<Vec<_>>());
}

#[test]
fn arrow_output_keeps_columnar_tick_order() {
    let log = Log::default();
    let mut d = make_tracked_list_of_struct_of_list(&log);
    let rb = d.collect_record_batch(&mut rng(), 2).unwrap();
    let l: &ListArray = rb.column(0).as_list();
    assert_eq!(l.value_offsets(), &[0, 2, 4]);
    let s: &StructArray = l.values().as_struct();
    assert_eq!(i32s(s.column(0)), vec![0, 1, 2, 3]);
    assert_eq!(
        i64s(s.column(1).as_list::<i32>().values()),
        (0..10).collect::<Vec<_>>()
    );

    let runs: Vec<_> = log
        .borrow()
        .chunk_by(|a, b| a == b)
        .map(|r| (r[0], r.len()))
        .collect();
    assert_eq!(
        runs,
        vec![("OUTER", 2), ("ID", 4), ("TLEN", 4), ("TAG", 10)]
    );
}
