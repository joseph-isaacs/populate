# populate-h2o

H2O.ai / DuckDB Labs [db-benchmark](https://github.com/duckdblabs/db-benchmark)
data generators (`groupby-datagen.R`, `join-datagen.R`) on populate-2
combinators.

```rust
use populate_h2o::{FastSampler, GroupByConfig, GroupByTable, JoinConfig, JoinTables};

// Byte-identical to `Rscript groupby-datagen.R 1e7 1e2 0 0`
let g1 = GroupByTable::reference(GroupByConfig::new(10_000_000, 100, 0, false));
let batch = g1.into_table().into_record_batch()?;

// Same distribution with any rand::Rng, using rand's own sampling.
let g1 = GroupByTable::generate(GroupByConfig::new(1_000_000, 10, 5, true), FastSampler, &mut rng);

// J1 tables: [x, small, medium, big]
let [x, small, medium, big] = JoinTables::reference(JoinConfig::new(10_000_000, 0, false)).into_tables();
```

Two modes share one generator graph and differ only in the `Sampler` type:

* `RRng` + `RSampler`: a port of R's Mersenne-Twister (`set.seed` scrambling
  included), `unif_rand`, `R_unif_index` rejection sampling, `sample` with and
  without replacement (including the hash variant for `n > 1e7`), `runif`
  and R 4.x `round`. The output matches R exactly.
* any `Rng` + `FastSampler`: `rand`'s uniform sampling.

## Validation

`validate/validate.sh WORKDIR` runs the R reference scripts and compares SHA-256
hashes of R's CSVs with ours (`examples/h2o_datagen.rs` writes fwrite-format
CSV). With R 4.3.3 and data.table 1.14.10, all configurations below match
byte for byte:

| script | configs (N K nas sort) |
|--------|------------------------|
| groupby | 1e4 1e1 0 0, 1e4 1e2 0 0, 1e4 1e1 5 1, 1e4 1e2 5 0, 1e5 1e1 0 1, 1e5 1e2 5 0, 1e5 1e2 5 1, 1e6 1e2 0 0, 1e6 1e1 5 1, 1e6 2e0 50 1, 1e7 1e2 0 0, 1e7 1e1 5 1 |
| join | 1e7 0 0 0, 1e7 0 0 1, 1e7 0 5 0, 1e7 0 5 1 |

`cargo test` checks R-produced golden values (RNG primitives, `round`,
`split_xlr`/`sample_all`, CSV heads) without needing R.

## Extensions beyond the reference

* `JoinConfig::scaled(n, small, medium, nas, sort)`: choose the RHS sizes
  instead of `n / 1e6` and `n / 1e3`. The reference also needs `n >= 1e7`.
* `FastSampler` mode with any seedable `Rng`.
