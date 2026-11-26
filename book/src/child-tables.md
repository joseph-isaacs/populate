# Child tables

An order has one to seven line items; a part has four suppliers. The child
table's rows are driven by the parent's: for each source row, some number
of child rows. In populate that is a distribution of `Children<T>`, whose
columns hold only the children, end to end. A table built from it has the
children as its rows.

## `flat_map`: any number of rows per source row

`flat_map::<Out, _, _>(source, f)` collects the source, then calls
`f(row, out)` once per source row; `f` pushes that row's children, none
included, straight into the child columns `out.values`.

```rust
use populate::{Children, Columns, Distribution, Sink};
use populate::dist::{flat_map, range};

type Lines = Children<(i64, i32)>;

/// Order `k` has `k % 4` lines: (order key, line number).
fn lines() -> impl Distribution<Item = Lines> {
    flat_map::<Lines, _, _>(range(0i64, 1), |&k: &i64, out: &mut Columns<Lines>| {
        for j in 0..k % 4 {
            out.values.push((k, j as i32 + 1));
        }
    })
    .reserving(&[1.5])
}

let c = lines().collect(&mut rand::rng(), 4);
assert_eq!(c.values.0, [1, 2, 2, 3, 3, 3]);
assert_eq!(c.values.1, [1, 1, 2, 1, 2, 3]);
assert_eq!(c.sources, 4);
```

`.reserving(&[mean])` sizes the child columns for `mean` children per
source row before the loop, so they are allocated once instead of grown.
Leave it off and they grow as usual.

`flat_map` fits children whose columns share work: in TPC-H, a line's ship
date decides its status, its return flag and its receipt date, so one
closure writes all of them.

## `children`: one column at a time, from fixed slots

When every child column is independent and a source row has at most `K`
children, `children(counts, slots)` keeps the first `count` of the `K`
values `slots` makes for each row. It reproduces dbgen, whose line streams
reserve seven draws per order whether or not the order uses them. Each
column is one `children` over the same counts; a tuple of them is the
child table.

```rust
use populate::{Children, Distribution};
use populate::dist::{children, constant, derive, range};

/// Four suppliers per part: (part key, supplier).
fn part_supp() -> impl Distribution<Item = (Children<i64>, Children<i64>)> {
    (
        children(constant(4usize), derive(range(1i64, 1), |&p| [p; 4])),
        children(constant(4usize), derive(range(1i64, 1), |&p| [p * 10, p * 10 + 1, p * 10 + 2, p * 10 + 3])),
    )
}

let (keys, supps) = part_supp().collect(&mut rand::rng(), 2);
assert_eq!(keys.values, [1, 1, 1, 1, 2, 2, 2, 2]);
assert_eq!(supps.values, [10, 11, 12, 13, 20, 21, 22, 23]);
```

A child table goes into `DistTable` like any other distribution;
`rows` then counts source rows, and each batch holds the children of its
source rows.

## Several tables from one pass

Sometimes one loop makes rows for two tables: a sale and, for about one
sale in ten, its return, decided while making the sale. Wrap a tuple of
`Children` in `Tables` and `flat_map` writes each field as its own table
(`out.0.values`, `out.1.values`, ...). A plain tuple of `Children` is one
child table with those columns; `Tables` is what makes them separate.

`DistTables` is `DistTable` for these: one name per table, one schema per
table, and a batch per table for every range of source rows.

```rust
use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use populate::{Children, Columns, Distribution, Sink, Tables};
use populate::dist::{flat_map, named_all, range};
use populate::table::DistTables;

type SalesAndReturns = Tables<(Children<i64>, Children<(i64, i64)>)>;

/// Sale `k`; a return of `k` for every third sale.
fn sales() -> impl Distribution<Item = SalesAndReturns> + Clone + Sync {
    let d = flat_map::<SalesAndReturns, _, _>(
        range(0i64, 1),
        |&k: &i64, out: &mut Columns<SalesAndReturns>| {
            let (sales, returns) = &mut out.tables;
            sales.values.push(k);
            if k % 3 == 0 {
                returns.values.push((k, -k));
            }
        },
    );
    named_all(&["s_key", "r_key", "r_amount"], d)
}

let t = DistTables::new(&["sales", "returns"], 10, sales());
assert_eq!(t.schemas()[1].field(1).name(), "r_amount");

let b = t.batches(0, 10);
assert_eq!(b[0].num_rows(), 10);
assert_eq!(b[1].column(0).as_primitive::<Int64Type>().values(), &[0, 3, 6, 9]);
```

`named_all` over a `Tables` distribution names every table's columns in
order; `DistTables` splits them by table.
