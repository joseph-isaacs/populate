# populate: design

`populate` generates tabular data column by column. You describe a row
with combinators; collecting writes each column straight into the buffer
it will be stored in.

## The three traits

All of the core is in `populate-core` (re-exported from `populate`).

```rust
/// A stream of rows.
pub trait Distribution {
    type Item: Columnar;

    /// The next row.
    fn sample(&mut self, rng: &mut impl Rng) -> Self::Item;

    /// The next `n` rows, appended column by column. Defaults to `n`
    /// samples; leaves override it to fill a whole column at once.
    fn collect_into(&mut self, cols: &mut Columns<Self::Item>, rng: &mut impl Rng, n: usize);

    /// The next `n` rows as new columns.
    fn collect(&mut self, rng: &mut impl Rng, n: usize) -> Columns<Self::Item>;

    /// Move past the next `n` rows without producing them. Defaults to `n`
    /// samples, dropped; override it wherever that can be done directly.
    fn skip(&mut self, n: u64);
}

/// A row type and the columns that hold many of it.
pub trait Columnar: Sized {
    type Columns: Sink<Self> + Default;
}

/// Something values are appended to.
pub trait Sink<T> {
    fn push(&mut self, value: T);
    fn reserve(&mut self, additional: usize);
    fn push_n(&mut self, values: impl Iterator<Item = T>);
}
```

`Columns<T>` is short for `<T as Columnar>::Columns`.

## Rows and their columns

| row type                  | columns                                          |
|---------------------------|--------------------------------------------------|
| `i32`, `f64`, `bool`, ... | `Vec<T>`                                         |
| `String`, `&'static str`  | `StrCol`: `i32` offsets and bytes (Arrow `Utf8`) |
| `Vec<T>`                  | `ListCol<Columns<T>>`: `i32` offsets and `T`'s columns (Arrow `List`) |
| `(A, B, ...)`             | `(Columns<A>, Columns<B>, ...)`                  |
| `Date32`, `Decimal<P, S>` | `Vec` of them: `i32` days, `i128` units (Arrow `Date32`, `Decimal128(P, S)`) |
| `[T; N]`                  | `Vec<[T; N]>`: `N` values per row, end to end    |
| `Children<T>`             | `ChildCols`: `T`'s columns, every child end to end |

The columns nest the way the rows do, and they are already in Arrow's
memory layout. `populate-sink-arrow` moves the buffers into Arrow arrays
without copying (nested tuples become flat columns at the top level and
struct fields inside a list), and `populate-sink-parquet` encodes Parquet
pages straight out of them.

## Two ways to get rows

- `sample` gives one row. A tuple samples each field in turn, so the
  draws interleave: `a0 b0 a1 b1 ...`.
- `collect_into` gives many. A tuple fills its first column completely,
  then the second: `a0 a1 ... b0 b1 ...`. A list collects all its lengths
  into the offsets, then hands the element columns to the element
  distribution for the total count, so nesting stays columnar all the way
  down.

Both give valid rows, but for the same seeded RNG they are different
rows, because the draws happen in a different order.

`map` has only `sample`: it needs a whole row of its source to call the
closure. `with_column` is the columnar alternative: the parent collects
its columns, then the new column is computed in one pass over them.

## A distribution is a stream

Sampling moves a distribution on. `range(0, 1)` collected twice gives
`0..n` and then `n..2n`, and the random leaves keep unused random bits
between calls. Clone a distribution to start again from the same point.

`skip(n)` moves a distribution past `n` rows without producing them.
`Table::col` clones the distribution for every batch and skips to the
batch's first row, so a batch depends only on its index and seed, and a
`range` column still counts through the whole table. Every built-in skips
in constant time: the random leaves do nothing, `range` adds `n * step`,
and `map`, `with_column` and tuples pass `n` on. `list` skips `n` lengths
and leaves its elements alone, because how many elements `n` rows hold is
random.

The default `skip` samples `n` rows and drops them. It is there so that
any distribution works in a table, but it is slow (batch `k` pays for the
`k` batches before it), so a distribution whose rows depend on their
position should override it. It draws from a throwaway RNG, so it lands
exactly on row `n` only when the state doesn't depend on the values drawn:
a counter does, a random walk doesn't.

## Schemas come from the distribution

`schema_of(&dist)` (`populate-sink-arrow`) is a table's schema without
generating a row. The types come from the sinks: every column type's
`IntoArrowColumns::data_types` says what Arrow array it becomes, so
`Vec<i64>` is `Int64`, `Vec<Date32>` is `Date32` and a tuple of columns
lists its fields' types in order, and `IntoArrowColumns::nullables` whether
each may hold nulls (a column type that stores nulls says so). The names
come from the distribution:
`Distribution::column_names`, which `named` / `named_all` set and a tuple
concatenates. `DistTable` (`populate-table`) runs a distribution over any
range of rows (clone, `skip`, collect) with that schema; `DynTable` is the
same with the distribution's type erased, so a data set's tables share one
type.

## Seekable streams

A `SeekRng` is a stream of draws that can jump `n` draws ahead.
`draws::<K>(stream)` is the leaf that reserves `K` draws per row (or
`draws_reserving` for rows that use the first `K` of more): its rows are
`[Draw; K]`, collected by one `SeekRng::fill` and skipped by
`advance(n * per_row)`. It ignores the caller's RNG, so a row depends only
on its position. This is how the reference generators seed their columns
(dbgen gives every column its own Lehmer stream), and why a chunk of such a
table starts at its first row in `O(log n)`.

## Streams that can't jump, and columns built whole

Some reference generators' streams have no fast way to row `n`: R's
`sample()` rejects draws, so a row takes a varying number of them.
`checkpointed(inner, every, rows)` runs `inner` over its rows once (by its
own `skip`), saving its state every `every` rows; a skip then restarts
from the last saved state before the target and skips fewer than `every`
rows. Some columns are only known whole (a shuffle, a sort): `column(values)`
reads a column in memory, shared by every clone, and `column_as` converts
each value as it is collected, so several columns of different types can
read one stored column.

## Columns from columns, and child rows

- `derive(source, f)` collects `source` column by column, then maps each
  row: a column computed from others, with the source columns as scratch.
  Rows are read in place (`RowRefs`: a row of a `Vec<T>` is a `&T`).
- `derive_columns(source, f)` is the same a block at a time: `f` reads
  the source columns and writes one output row per source row, for
  columns that share intermediate values.
- `children(counts, slots)` is a child table column: for every source
  row, the first `count` of the `K` values `slots` makes for it (each line
  stream of an order reserves 7 slots). A tuple of `children` over the
  same counts is the child table.
- `flat_map(source, f)` writes any number of children for each source
  row straight into the child columns, for children whose columns share
  work, its source rows read in place too. `.reserving(per_source)`
  allocates the child columns once from a mean per source row.

A distribution of `Children<T>` is a table of `T` rows: its columns are
the children's, and the source rows only drive them. `Tables<T>`, with
`T` a tuple of `Children`, is several such tables made in one pass (a
sale and its return): `flat_map` writes each, and `schemas_of` and
`DistTables` give one schema and one batch per table. A plain tuple of
`Children` stays one table whose columns are its fields.

## Writing a leaf

Implement `sample`. That is enough: the default `collect_into` reserves
`n` once and pushes `n` samples with one `push_n`, which for a `Vec` is a
single `extend` of known length.

The default runs its loop inside `in_local`, which moves the distribution
into a local variable for the loop and back afterwards. Through `&mut
self` the compiler must assume each value written to a column could
overwrite the distribution's own state, so it would store and reload that
state for every value; in a local the state stays in registers and the
loop vectorises. This is why `range` and `map` need no hand-written
`collect_into` to be as fast as a hand-written loop.

Override `collect_into` when a column can be made faster than one value
at a time: the random leaves draw indices in batches (`fill_indices` in
`populate-builtins/src/random.rs`), and strings write their bytes
straight into the `StrCol` buffer.

## Crates

| crate                   | contents                                              |
|-------------------------|-------------------------------------------------------|
| `populate-core`         | the three traits, `SeekRng`, `StrCol`, `ListCol`, `Date32`, `Decimal`, `Children`, tuple impls |
| `populate-builtins`     | `constant`, `range`, `map`, `list`, `with_column`, `draws`, `derive`, `derive_columns`, `children`, `flat_map`, `format`, `named`, `checkpointed`, `column`, random leaves |
| `populate-sink-arrow`   | columns to Arrow arrays and `RecordBatch`, `schema_of` |
| `populate-sink-parquet` | columns to Parquet without copying                    |
| `populate-table`        | `Table`: named, typed columns generated in parallel batches; `DistTable` / `DynTable`: a table that is one distribution; `Chunks` and the drivers every benchmark generator runs on; `cli`: the generators' shared command-line flags and report |
| `populate`              | facade re-exporting the above                         |
| `populate-combinators`  | what several generators share (`generator/combinators`): dbgen's and dsdgen's Lehmer streams, `Utf8View` columns over shared buffers |

TPC-H (`populate-tpch`) and H2O (`populate-h2o`) are built on
`Distribution`: each table is one distribution run by `DynTable` (TPC-H)
or `DistTable` (H2O), its schema read off it. The other
benchmark generators (TPC-DS and the rest) implement `Chunks` from
`populate-table` directly.
