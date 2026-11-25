//! Arrow list output: columnar collection vs row-by-row into Arrow builders.
//!
//! Every variant produces the same Arrow array. Row variants make the same
//! rows by hand, one `Vec` per row (what a row-based generator hands you),
//! and append them to a builder. Same hygiene as `collect.rs`: black-boxed
//! params, setup in `with_inputs`, outputs kept alive and dropped after timing.

use arrow_array::builder::{ArrayBuilder, Int32Builder, Int64Builder, ListBuilder, StructBuilder};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field};
use divan::counter::ItemsCount;
use divan::{Bencher, black_box};
use populate::Distribution;
use populate::arrow::DistributionArrow;
use populate::dist::{constant, derive, list, range};
use std::sync::Arc;

fn main() {
    divan::main();
}

const NS: &[usize] = &[1 << 10, 1 << 16];

/// lengths cycle 0..16 (mean 7.5)
fn d_list() -> impl Distribution<Item = Vec<i64>> {
    list(
        derive(range(black_box(0i32), 1), |&i: &i32| i & 15),
        range(black_box(0i64), black_box(3)),
    )
}

/// list<{ id: i32, tags: list<i64> }>: 4 structs per row, tag lens cycle 0..8
fn d_list_struct_list() -> impl Distribution<Item = Vec<(i32, Vec<i64>)>> {
    list(
        constant(black_box(4i32)),
        (
            range(black_box(0i32), 1),
            list(
                derive(range(black_box(0i32), 1), |&i: &i32| i & 7),
                range(black_box(0i64), black_box(3)),
            ),
        ),
    )
}

/// [`d_list`]'s rows, one `Vec` each.
fn list_rows(n: usize) -> impl Iterator<Item = Vec<i64>> {
    let (mut v, step) = (black_box(0i64), black_box(3));
    (0..n).map(move |i| {
        (0..i & 15)
            .map(|_| {
                v += step;
                v - step
            })
            .collect()
    })
}

/// [`d_list_struct_list`]'s rows, one `Vec` each.
fn struct_list_rows(n: usize) -> impl Iterator<Item = Vec<(i32, Vec<i64>)>> {
    let (mut id, mut inner, mut v, step) = (black_box(0i32), 0usize, black_box(0i64), 3);
    (0..n).map(move |_| {
        (0..4)
            .map(|_| {
                let tags = (0..inner & 7)
                    .map(|_| {
                        v += step;
                        v - step
                    })
                    .collect();
                inner += 1;
                id += 1;
                (id - 1, tags)
            })
            .collect()
    })
}

type Dyn = Box<dyn ArrayBuilder>;

fn dyn_vals(b: &mut ListBuilder<Dyn>) -> &mut Int64Builder {
    b.values()
        .as_any_mut()
        .downcast_mut::<Int64Builder>()
        .expect("Int64Builder")
}

mod list_i64 {
    use super::*;

    /// generate offsets column + values column, move Vecs into Arrow buffers
    #[divan::bench(args = NS)]
    fn columnar(b: Bencher, n: usize) {
        b.counter(ItemsCount::new(n))
            .with_inputs(|| (d_list(), rand::rng()))
            .bench_local_refs(|(d, rng)| -> RecordBatch {
                d.collect_record_batch(rng, black_box(n)).unwrap()
            });
    }

    /// row Vec per row, copied into a typed ListBuilder
    #[divan::bench(args = NS)]
    fn row_static_builder(b: Bencher, n: usize) {
        b.counter(ItemsCount::new(n)).bench_local(|| -> ArrayRef {
            let mut lb = ListBuilder::new(Int64Builder::new());
            for row in list_rows(black_box(n)) {
                lb.values().append_slice(&row);
                lb.append(true);
            }
            Arc::new(lb.finish())
        });
    }

    /// row Vec per row, `dyn ArrayBuilder` downcast once per row
    #[divan::bench(args = NS)]
    fn row_dyn_per_row(b: Bencher, n: usize) {
        b.counter(ItemsCount::new(n)).bench_local(|| -> ArrayRef {
            let mut lb = ListBuilder::new(Box::new(Int64Builder::new()) as Dyn);
            for row in list_rows(black_box(n)) {
                dyn_vals(&mut lb).append_slice(&row);
                lb.append(true);
            }
            Arc::new(lb.finish())
        });
    }

    /// `dyn ArrayBuilder` downcast once per *value*: what a generic per-push
    /// `Sink` over a dyn builder does
    #[divan::bench(args = NS)]
    fn row_dyn_per_value(b: Bencher, n: usize) {
        b.counter(ItemsCount::new(n)).bench_local(|| -> ArrayRef {
            let mut lb = ListBuilder::new(Box::new(Int64Builder::new()) as Dyn);
            for row in list_rows(black_box(n)) {
                for v in row {
                    dyn_vals(&mut lb).append_value(v);
                }
                lb.append(true);
            }
            Arc::new(lb.finish())
        });
    }
}

mod list_struct_list {
    use super::*;

    #[divan::bench(args = NS)]
    fn columnar(b: Bencher, n: usize) {
        b.counter(ItemsCount::new(n))
            .with_inputs(|| (d_list_struct_list(), rand::rng()))
            .bench_local_refs(|(d, rng)| -> RecordBatch {
                d.collect_record_batch(rng, black_box(n)).unwrap()
            });
    }

    /// rows of Vec<(i32, Vec<i64>)> into ListBuilder<StructBuilder> — Arrow's
    /// StructBuilder holds fields as `Box<dyn ArrayBuilder>`, so this is the
    /// realistic row path for nested data (downcast per field per struct)
    #[divan::bench(args = NS)]
    fn row_builder(b: Bencher, n: usize) {
        b.counter(ItemsCount::new(n)).bench_local(|| -> ArrayRef {
            let tags_field = Field::new_list_field(DataType::Int64, true);
            let fields = vec![
                Field::new("c0", DataType::Int32, false),
                Field::new("c1", DataType::List(Arc::new(tags_field)), false),
            ];
            let sb = StructBuilder::new(
                fields,
                vec![
                    Box::new(Int32Builder::new()) as Dyn,
                    Box::new(ListBuilder::new(Int64Builder::new())) as Dyn,
                ],
            );
            let mut lb = ListBuilder::new(sb);
            for row in struct_list_rows(black_box(n)) {
                for (id, tags) in row {
                    let s = lb.values();
                    s.field_builder::<Int32Builder>(0).unwrap().append_value(id);
                    let t = s.field_builder::<ListBuilder<Int64Builder>>(1).unwrap();
                    t.values().append_slice(&tags);
                    t.append(true);
                    s.append(true);
                }
                lb.append(true);
            }
            Arc::new(lb.finish())
        });
    }
}
