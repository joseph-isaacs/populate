#[cfg(feature = "table")]
use arrow_array::cast::AsArray;
#[cfg(feature = "table")]
use arrow_array::types::{Int32Type, Int64Type};
use populate::dist::{flat_map, named, range, uniform};
#[cfg(feature = "table")]
use populate::table::{Chunks, DistTable};
use populate::{Children, Columns, Distribution};

fn rows() -> impl Distribution<Item = (i64, i32)> + Clone + Sync {
    (
        named("id", range(0i64, 1)),
        named("value", uniform(0i32, 99)),
    )
}

fn children() -> impl Distribution<Item = Children<i64>> + Clone + Sync {
    flat_map::<Children<i64>, _, _>(
        range(0i64, 1),
        |id: &i64, cols: &mut Columns<Children<i64>>| {
            for _ in 0..id % 3 {
                cols.values.push(*id);
            }
        },
    )
}

#[test]
fn child_rows_stay_raw_and_chunk_sizes_count_source_rows() {
    let chunks = children().chunks(10).chunk_rows(3).batch_rows(2);
    for threads in [1, 3] {
        let mut values = Vec::new();
        chunks.for_each_batch(threads, |cols| values.extend(cols.values));
        assert_eq!(values, [1, 2, 2, 4, 5, 5, 7, 8, 8]);
    }
}

#[test]
fn tuple_columns_can_be_consumed_without_a_table() {
    let chunks = rows().chunks(10).chunk_rows(4).batch_rows(3);
    let mut ids = Vec::new();
    let mut values = Vec::new();
    chunks.for_each_batch(2, |(id, value)| {
        ids.extend(id);
        values.extend(value);
    });
    assert_eq!(ids, (0..10).collect::<Vec<_>>());
    assert_eq!(values.len(), 10);
    assert!(values.iter().all(|v| (0..=99).contains(v)));
}

#[cfg(feature = "table")]
#[test]
fn raw_table_chunks_match_arrow_values_and_boundaries() {
    let table = DistTable::new("rows", 37, rows())
        .chunk_rows(7)
        .batch_rows(3);
    let raw = table.raw();
    assert_eq!(raw.num_chunks(), Chunks::num_chunks(&table));
    for i in 0..raw.num_chunks() {
        let mut columns = Vec::new();
        raw.chunk(i, |cols| columns.push(cols));
        let mut arrays = Vec::new();
        Chunks::chunk(&table, i, &mut |batch| {
            arrays.push((
                batch
                    .column(0)
                    .as_primitive::<Int64Type>()
                    .values()
                    .to_vec(),
                batch
                    .column(1)
                    .as_primitive::<Int32Type>()
                    .values()
                    .to_vec(),
            ));
        });
        assert_eq!(columns, arrays);
    }
}
