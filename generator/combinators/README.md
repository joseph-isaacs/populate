# populate-combinators

Distributions shared by populate's benchmark generators, on top of
`populate-core` and `populate-builtins`. General-purpose distributions
belong in `populate-builtins`; this crate holds what several reference
tools share but nothing else would use.

- `lehmer`: the Park–Miller LCG (`x' = 16807 * x mod (2^31 - 1)`) that
  dbgen and dsdgen give every column, as a `SeekRng` (`Lcg`) that jumps to
  any draw in `O(log n)` and fills draws with 16 independent lanes (AVX2
  where available), plus dbgen's bounding (`bounded`, `scale`,
  `bound_all`) and the `Ints` column most TPC-H streams make. Used by
  `populate-tpch` and `populate-ssb`.
- `views`: `Utf8View` string columns over buffers a generator shares
  between batches (a dictionary, a text pool). `Word` is a precomputed
  view, so collecting it copies nothing; `Str` is written per row by
  `populate_builtins::format` into the batch's own buffer; `Picks` is
  dbgen's weighted pick as views into a `Dict`. A generator names its
  buffers with a marker type implementing `Buffers`, so the column types
  still give the schema. Used by `populate-tpch`.

```sh
cargo test -p populate-combinators
```

The tests check the streams against tpchgen's `RowRandomInt`, from any row,
and the view columns by reading them back from Arrow.
