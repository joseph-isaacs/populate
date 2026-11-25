# populate-py

Python bindings for populate: build a table from populate's distributions
in Python, or take a generator's table (TPC-H, SSB), and read it as Arrow
or write it to Parquet. Batches are generated on all cores and handed to
Python through the [Arrow PyCapsule
interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html),
so pyarrow, polars and DuckDB read the buffers populate wrote, without a
copy.

All PyO3 code is behind the crate's `python` feature. Without it the crate
is plain Rust ([`dynamic`](src/dynamic.rs): distributions whose types are
chosen at run time) and has no PyO3 dependency.

```sh
cd python
pip install maturin pyarrow
maturin develop --release
python examples/demo.py
```

```python
from datetime import date
import pyarrow as pa
import populate as p

sales = p.table(
    10_000_000,
    {
        "id": p.range(0),
        "store": p.choose(["north", "south", "east", "west"], weights=[4, 3, 2, 1]),
        "sold": p.uniform(date(1992, 1, 1), date(1998, 12, 31)),
        "qty": p.uniform(1, 10),
        "price": p.normal(100.0, 15.0),
        "note": p.text(["gift", "rush", "fragile"], 0, 3).nullable(0.2),
        "tags": p.list(p.uniform(0, 4), p.uniform(0, 1_000)),
    },
    seed=42,
)
table = pa.table(sales)            # or polars.DataFrame(sales)
sales.write_parquet("sales.parquet")

lineitem = pa.table(p.tpch("lineitem", sf=1))
```

## API

| Python | Rust underneath |
|---|---|
| `range(start=0, step=1, dtype=None)` | `populate_builtins::range` |
| `constant(value)` | `constant` |
| `uniform(lo, hi)`: ints, floats or `datetime.date`s | `uniform` |
| `choose(values, weights=None)` | `choose`, `weighted` |
| `text(words, min, max)`, `chars(alphabet, min, max)` | `text`, `chars` |
| `list(lengths, values)` | `list` |
| `normal(mean, std)` | `dynamic::Normal`, a `Distribution` written in this crate |
| `d.nullable(rate)`, `d.date()` | `dynamic::NullRate`, `Date32` |
| `d.collect(n, seed=0)`: an Arrow array | `Distribution::collect` |
| `table(rows, {name: dist}, seed=0)`: a `Source` | `dynamic::DynTable` (a `Chunks`) |
| `tpch(table, sf)`, `ssb(table, sf)`: a `Source` | `TpchTable`, `SsbTable` |

A `Source` is an Arrow stream (`__arrow_c_stream__`): each read generates
the table again, the same rows every time, with the workers a few chunks
ahead of the reader. `src.schema`, `src.with_threads(n)` and
`src.write_parquet(path, compression=None)` (one row group per chunk,
encoded on the workers) are the rest of it. dtypes are `int32`, `int64`,
`float64`, `bool`, `string` and `date32`, plus nullable versions of each
and lists of `int64`, `float64` and `string`.

A table's rows depend on `seed` and `batch_rows`, not on the thread count:
each batch of each column is the column's distribution skipped to the
batch's first row and collected with an RNG seeded from the seed, the
column and the row.

## How the types are erased

populate's distributions are generic; a Python script picks its columns'
types when it runs. `dynamic::BoxDist<T>` is any `Distribution<Item = T>`
behind a trait object, and is a `Distribution` itself, so collecting it
still hands the whole column to the distribution inside: one virtual call
and one RNG seed per column per batch, not one per row. `dynamic::AnyDist`
is an enum of `BoxDist`s, one per supported row type, so a column's type
is a value.

## Your own distributions

A Rust crate can expose its own `Distribution` to Python through the same
types, in its own PyO3 module:

```rust
use populate_py::py::{Dist, Source};

#[pyfunction]
fn zipf(n: u64, s: f64) -> Dist {
    Dist::from_dist(MyZipf::new(n, s)) // any Distribution<Item = i64>, f64, String, ...
}

#[pyfunction]
fn my_table(sf: f64) -> Source {
    Source::new(MyGenerator::new(sf)) // any populate_table::Chunks
}
```

Values passed to `choose`, `constant("...")` and `text` are kept for the
life of the process: populate's pickers hold `&'static` slices.

## Tests

```sh
pip install pytest
pytest tests
```

The Rust tests (`cargo test -p populate-py`) cover `dynamic` without
Python.

## Performance

`python examples/demo.py` on a 4-core Intel Xeon @ 2.10 GHz, Python 3.13,
pyarrow 26:

| step | time |
|---|---|
| 10M-row `sales` table above to a `pyarrow.Table` | 0.29 s |
| the same to Parquet (Snappy) | 0.77 s |
| TPC-H SF1 `lineitem` to a `pyarrow.Table` | 0.48 s |
| TPC-H SF1 `lineitem` to Parquet | 0.92 s |
