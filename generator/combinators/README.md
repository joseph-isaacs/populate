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

```sh
cargo test -p populate-combinators
```

The tests check the streams against tpchgen's `RowRandomInt`, from any row.
