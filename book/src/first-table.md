# A first table

## Leaves

A *leaf* is a distribution with no source of its own. `populate::dist`
(the `populate-builtins` crate) has the common ones:

| leaf | rows |
|---|---|
| `constant(v)` | `v` every row |
| `range(start, step)` | `start`, `start + step`, ... |
| `uniform(lo, hi)` | uniform in `lo..=hi` |
| `choose(&[..])`, `weighted(&[(v, w), ..])` | one of a fixed set of values |
| `text(words, min, max)`, `chars(alphabet, min, max)` | random strings |
| `list(lengths, elements)` | a list per row |
| `draws::<_, K>(stream)` | `K` draws of a seekable RNG stream per row |

The rustdoc of `populate_builtins` lists them all, with their column types.

## Naming columns

A table's schema is read off its distribution: the column types come from
the columns its rows are collected into, and the names from `named`
(one column) or `named_all` (one name per field of a tuple). Name every
column or none: a distribution that names none gets `c0`, `c1`, ..., and
one that names only some panics when its schema is read.

```rust
use populate::Distribution;
use populate::dist::{choose, named, named_all, range, uniform};

fn sales() -> impl Distribution<Item = (i64, &'static str, (i64, i32))> + Clone + Sync {
    (
        named("id", range(1i64, 1)),
        named("store", choose(&["north", "south", "east"])),
        named_all(&["qty", "day"], (uniform(1i64, 10), uniform(0i32, 364))),
    )
}
# let _ = sales();
```

## `DistTable`: a table that is one distribution

`populate::table::DistTable` runs a distribution as a table of `rows`
rows. It works out the schema once, without generating a row, and makes any
range of rows on request:

```rust
# use populate::Distribution;
# use populate::dist::{choose, named, named_all, range, uniform};
# fn sales() -> impl Distribution<Item = (i64, &'static str, (i64, i32))> + Clone + Sync {
#     (
#         named("id", range(1i64, 1)),
#         named("store", choose(&["north", "south", "east"])),
#         named_all(&["qty", "day"], (uniform(1i64, 10), uniform(0i32, 364))),
#     )
# }
use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use populate::table::DistTable;
use populate::table::chunks::for_each_batch;

let t = DistTable::new("sales", 100_000, sales());
let names: Vec<_> = t.schema().fields().iter().map(|f| f.name().as_str()).collect();
assert_eq!(names, ["id", "store", "qty", "day"]);

// rows 500..503, made on their own: the distribution cloned, skipped 500
// rows and collected 3
let b = t.batch(500, 3);
assert_eq!(b.column(0).as_primitive::<Int64Type>().values(), &[501, 502, 503]);

// every batch, generated on all cores (0) and handed back in order
let mut rows = 0;
for_each_batch(&t, 0, |batch| rows += batch.num_rows());
assert_eq!(rows, 100_000);
```

The table is split into chunks (`chunk_rows`, 256Ki rows by default), each
generated independently and made of batches of `batch_rows` (8Ki). Because
a chunk depends only on its index, the output is the same for any thread
count. With the `parquet` feature, `populate::table::write_parquet(&t,
file, threads, None)` writes one row group per chunk.

A generator with many tables of different types keeps them as
`DynTable`s: the same driver with the distribution's type erased.
