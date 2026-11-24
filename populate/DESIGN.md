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
| `populate-core`         | the three traits, `StrCol`, `ListCol`, tuple impls    |
| `populate-builtins`     | `constant`, `range`, `map`, `list`, `with_column`, random leaves |
| `populate-sink-arrow`   | columns to Arrow arrays and `RecordBatch`             |
| `populate-sink-parquet` | columns to Parquet without copying                    |
| `populate-table`        | `Table`: named, typed columns generated in parallel batches; `Chunks` and the drivers every benchmark generator runs on |
| `populate`              | facade re-exporting the above                         |

The benchmark generators (TPC-H, TPC-DS and the rest) implement `Chunks`
from `populate-table` directly and do not use `Distribution`.
