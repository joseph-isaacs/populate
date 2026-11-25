# Direct Parquet writer: benchmark

Distribution to Parquet three ways, on the 4-core container, mimalloc, best of 3 runs. Times include generation. Output goes to a byte counter. Row groups are 1Mi rows on every path.

- **Direct**: `write_dist_parquet`, from the collected `Vec`s, no Arrow.
- **Table**: the same columns as a `Table`, through `Table::write_parquet` (batches of 8,192 rows, then populate-table's encoder, with arrow-rs for lists).
- **arrow-rs**: the same `RecordBatch`es through arrow-rs alone. One thread is `ArrowWriter`; more threads is one row group per worker through `ArrowColumnWriter`s.

Shapes: **numbers** is a sequence `i64`, a full-range random `i64`, an `i32` in 0..1000 and an `f64`. **strings** is a category from 6 values, 2 to 8 words of text, and 5 to 20 random letters. **lists** is `list<i64>` of 0 to 8 values and `list<f32>` of 0 to 4. **mixed** is all 9 columns.

The Table column already includes the shared-encoder speedups from this work (fewer passes, no copy of finished pages), so it is faster than on main.

## Default writer settings (dictionaries on, uncompressed)

**numbers**

| Rows | Threads | Direct | Table | arrow-rs | Table / direct | arrow-rs / direct |
|---|---|---|---|---|---|---|
| 1M | 1 | 0.030 s | 0.035 s | 0.041 s | 1.17x | 1.36x |
| 1M | 2 | 0.025 s | 0.032 s | 0.037 s | 1.27x | 1.44x |
| 1M | 4 | 0.020 s | 0.036 s | 0.038 s | 1.85x | 1.95x |
| 4M | 1 | 0.107 s | 0.138 s | 0.154 s | 1.28x | 1.43x |
| 4M | 2 | 0.066 s | 0.071 s | 0.079 s | 1.07x | 1.18x |
| 4M | 4 | 0.042 s | 0.048 s | 0.047 s | 1.15x | 1.11x |
| 16M | 1 | 0.427 s | 0.502 s | 0.633 s | 1.18x | 1.48x |
| 16M | 2 | 0.261 s | 0.290 s | 0.368 s | 1.11x | 1.41x |
| 16M | 4 | 0.157 s | 0.169 s | 0.219 s | 1.08x | 1.39x |

**strings**

| Rows | Threads | Direct | Table | arrow-rs | Table / direct | arrow-rs / direct |
|---|---|---|---|---|---|---|
| 1M | 1 | 0.138 s | 0.172 s | 0.164 s | 1.24x | 1.19x |
| 1M | 2 | 0.130 s | 0.182 s | 0.178 s | 1.40x | 1.37x |
| 1M | 4 | 0.120 s | 0.172 s | 0.181 s | 1.43x | 1.51x |
| 4M | 1 | 0.562 s | 0.706 s | 0.729 s | 1.25x | 1.30x |
| 4M | 2 | 0.293 s | 0.377 s | 0.319 s | 1.28x | 1.09x |
| 4M | 4 | 0.179 s | 0.212 s | 0.197 s | 1.19x | 1.10x |
| 16M | 1 | 2.169 s | 2.837 s | 2.615 s | 1.31x | 1.21x |
| 16M | 2 | 1.108 s | 1.455 s | 1.354 s | 1.31x | 1.22x |
| 16M | 4 | 0.608 s | 0.718 s | 0.775 s | 1.18x | 1.28x |

**lists**

| Rows | Threads | Direct | Table | arrow-rs | Table / direct | arrow-rs / direct |
|---|---|---|---|---|---|---|
| 1M | 1 | 0.081 s | 0.141 s | 0.146 s | 1.74x | 1.79x |
| 1M | 2 | 0.058 s | 0.156 s | 0.161 s | 2.70x | 2.79x |
| 1M | 4 | 0.054 s | 0.129 s | 0.160 s | 2.41x | 2.98x |
| 4M | 1 | 0.281 s | 0.542 s | 0.677 s | 1.93x | 2.41x |
| 4M | 2 | 0.164 s | 0.281 s | 0.331 s | 1.72x | 2.02x |
| 4M | 4 | 0.100 s | 0.175 s | 0.201 s | 1.75x | 2.01x |
| 16M | 1 | 1.193 s | 2.233 s | 2.618 s | 1.87x | 2.19x |
| 16M | 2 | 0.600 s | 1.221 s | 1.342 s | 2.03x | 2.24x |
| 16M | 4 | 0.318 s | 0.641 s | 0.645 s | 2.02x | 2.03x |

**mixed**

| Rows | Threads | Direct | Table | arrow-rs | Table / direct | arrow-rs / direct |
|---|---|---|---|---|---|---|
| 1M | 1 | 0.225 s | 0.367 s | 0.402 s | 1.63x | 1.79x |
| 1M | 2 | 0.196 s | 0.366 s | 0.396 s | 1.87x | 2.02x |
| 1M | 4 | 0.175 s | 0.364 s | 0.356 s | 2.07x | 2.03x |
| 4M | 1 | 0.924 s | 1.513 s | 1.685 s | 1.64x | 1.82x |
| 4M | 2 | 0.593 s | 0.874 s | 0.882 s | 1.47x | 1.49x |
| 4M | 4 | 0.368 s | 0.461 s | 0.514 s | 1.26x | 1.40x |
| 16M | 1 | 4.132 s | 6.419 s | 6.330 s | 1.55x | 1.53x |
| 16M | 2 | 2.331 s | 3.293 s | 3.233 s | 1.41x | 1.39x |
| 16M | 4 | 1.256 s | 1.797 s | 1.997 s | 1.43x | 1.59x |

## Dictionaries off (uncompressed)

**numbers**

| Rows | Threads | Direct | Table | arrow-rs | Table / direct | arrow-rs / direct |
|---|---|---|---|---|---|---|
| 1M | 1 | 0.021 s | 0.031 s | 0.027 s | 1.45x | 1.25x |
| 1M | 2 | 0.019 s | 0.024 s | 0.026 s | 1.29x | 1.39x |
| 1M | 4 | 0.015 s | 0.031 s | 0.027 s | 2.12x | 1.81x |
| 4M | 1 | 0.084 s | 0.094 s | 0.097 s | 1.13x | 1.16x |
| 4M | 2 | 0.045 s | 0.054 s | 0.061 s | 1.19x | 1.34x |
| 4M | 4 | 0.030 s | 0.037 s | 0.030 s | 1.20x | 0.99x |
| 16M | 1 | 0.334 s | 0.399 s | 0.389 s | 1.20x | 1.16x |
| 16M | 2 | 0.161 s | 0.209 s | 0.219 s | 1.29x | 1.36x |
| 16M | 4 | 0.107 s | 0.116 s | 0.121 s | 1.08x | 1.14x |

**lists**

| Rows | Threads | Direct | Table | arrow-rs | Table / direct | arrow-rs / direct |
|---|---|---|---|---|---|---|
| 1M | 1 | 0.073 s | 0.127 s | 0.191 s | 1.74x | 2.62x |
| 1M | 2 | 0.051 s | 0.152 s | 0.144 s | 3.01x | 2.84x |
| 1M | 4 | 0.058 s | 0.158 s | 0.141 s | 2.71x | 2.42x |
| 4M | 1 | 0.213 s | 0.529 s | 0.534 s | 2.48x | 2.50x |
| 4M | 2 | 0.128 s | 0.293 s | 0.317 s | 2.28x | 2.47x |
| 4M | 4 | 0.067 s | 0.149 s | 0.168 s | 2.23x | 2.52x |
| 16M | 1 | 0.872 s | 2.163 s | 2.503 s | 2.48x | 2.87x |
| 16M | 2 | 0.460 s | 1.195 s | 1.335 s | 2.60x | 2.90x |
| 16M | 4 | 0.256 s | 0.611 s | 0.662 s | 2.39x | 2.58x |

**mixed**

| Rows | Threads | Direct | Table | arrow-rs | Table / direct | arrow-rs / direct |
|---|---|---|---|---|---|---|
| 1M | 1 | 0.200 s | 0.329 s | 0.364 s | 1.64x | 1.82x |
| 1M | 2 | 0.187 s | 0.345 s | 0.357 s | 1.85x | 1.91x |
| 1M | 4 | 0.163 s | 0.382 s | 0.340 s | 2.35x | 2.09x |
| 4M | 1 | 0.994 s | 1.439 s | 1.426 s | 1.45x | 1.44x |
| 4M | 2 | 0.461 s | 0.717 s | 0.775 s | 1.56x | 1.68x |
| 4M | 4 | 0.298 s | 0.399 s | 0.450 s | 1.34x | 1.51x |
| 16M | 1 | 3.652 s | 5.499 s | 5.538 s | 1.51x | 1.52x |
| 16M | 2 | 1.715 s | 2.719 s | 2.933 s | 1.59x | 1.71x |
| 16M | 4 | 0.955 s | 1.438 s | 1.592 s | 1.51x | 1.67x |

## Snappy (dictionaries on)

All three paths compress each page with Snappy after encoding it. The direct path and the Table encoder use the `snap` crate, and arrow-rs uses its own Snappy codec. The files come out the same size, so they are compressed the same way.

**numbers**

| Rows | Threads | Direct | Table | arrow-rs | Table / direct | arrow-rs / direct | MB direct / Table / arrow-rs |
|---|---|---|---|---|---|---|---|
| 1M | 1 | 0.055 s | 0.052 s | 0.057 s | 0.95x | 1.04x | 22.1 / 22.1 / 22.1 |
| 1M | 2 | 0.039 s | 0.053 s | 0.059 s | 1.36x | 1.51x | 22.1 / 22.1 / 22.1 |
| 1M | 4 | 0.027 s | 0.052 s | 0.063 s | 1.94x | 2.34x | 22.1 / 22.1 / 22.1 |
| 4M | 1 | 0.214 s | 0.199 s | 0.268 s | 0.93x | 1.25x | 88.3 / 88.3 / 88.3 |
| 4M | 2 | 0.103 s | 0.107 s | 0.125 s | 1.04x | 1.21x | 88.3 / 88.3 / 88.3 |
| 4M | 4 | 0.071 s | 0.061 s | 0.073 s | 0.85x | 1.02x | 88.3 / 88.3 / 88.3 |
| 16M | 1 | 0.809 s | 0.865 s | 1.015 s | 1.07x | 1.25x | 353.2 / 353.2 / 353.2 |
| 16M | 2 | 0.444 s | 0.482 s | 0.525 s | 1.09x | 1.18x | 353.2 / 353.2 / 353.2 |
| 16M | 4 | 0.247 s | 0.269 s | 0.290 s | 1.09x | 1.17x | 353.2 / 353.2 / 353.2 |

**strings**

| Rows | Threads | Direct | Table | arrow-rs | Table / direct | arrow-rs / direct | MB direct / Table / arrow-rs |
|---|---|---|---|---|---|---|---|
| 1M | 1 | 0.257 s | 0.293 s | 0.287 s | 1.14x | 1.12x | 28.4 / 28.4 / 28.4 |
| 1M | 2 | 0.178 s | 0.276 s | 0.300 s | 1.55x | 1.68x | 28.4 / 28.4 / 28.4 |
| 1M | 4 | 0.190 s | 0.281 s | 0.293 s | 1.48x | 1.54x | 28.4 / 28.4 / 28.4 |
| 4M | 1 | 0.991 s | 1.149 s | 1.140 s | 1.16x | 1.15x | 113.6 / 113.6 / 113.6 |
| 4M | 2 | 0.532 s | 0.604 s | 0.645 s | 1.13x | 1.21x | 113.6 / 113.6 / 113.6 |
| 4M | 4 | 0.308 s | 0.312 s | 0.322 s | 1.01x | 1.05x | 113.6 / 113.6 / 113.6 |
| 16M | 1 | 3.908 s | 4.483 s | 4.675 s | 1.15x | 1.20x | 454.4 / 454.4 / 454.4 |
| 16M | 2 | 2.076 s | 2.483 s | 2.557 s | 1.20x | 1.23x | 454.4 / 454.4 / 454.4 |
| 16M | 4 | 1.080 s | 1.310 s | 1.267 s | 1.21x | 1.17x | 454.4 / 454.4 / 454.4 |

**lists**

| Rows | Threads | Direct | Table | arrow-rs | Table / direct | arrow-rs / direct | MB direct / Table / arrow-rs |
|---|---|---|---|---|---|---|---|
| 1M | 1 | 0.150 s | 0.236 s | 0.249 s | 1.57x | 1.66x | 34.6 / 34.7 / 34.7 |
| 1M | 2 | 0.137 s | 0.227 s | 0.237 s | 1.65x | 1.73x | 34.6 / 34.7 / 34.7 |
| 1M | 4 | 0.118 s | 0.231 s | 0.260 s | 1.96x | 2.20x | 34.6 / 34.7 / 34.7 |
| 4M | 1 | 0.617 s | 0.932 s | 1.036 s | 1.51x | 1.68x | 138.6 / 138.7 / 138.8 |
| 4M | 2 | 0.307 s | 0.497 s | 0.524 s | 1.62x | 1.71x | 138.6 / 138.7 / 138.8 |
| 4M | 4 | 0.206 s | 0.334 s | 0.279 s | 1.62x | 1.35x | 138.6 / 138.7 / 138.8 |
| 16M | 1 | 2.587 s | 3.866 s | 4.396 s | 1.49x | 1.70x | 554.1 / 554.8 / 554.9 |
| 16M | 2 | 1.278 s | 2.084 s | 2.257 s | 1.63x | 1.77x | 554.1 / 554.8 / 554.9 |
| 16M | 4 | 0.767 s | 1.149 s | 1.234 s | 1.50x | 1.61x | 554.1 / 554.8 / 554.9 |

**mixed**

| Rows | Threads | Direct | Table | arrow-rs | Table / direct | arrow-rs / direct | MB direct / Table / arrow-rs |
|---|---|---|---|---|---|---|---|
| 1M | 1 | 0.458 s | 0.608 s | 0.597 s | 1.33x | 1.30x | 85.1 / 85.2 / 85.1 |
| 1M | 2 | 0.309 s | 0.589 s | 0.599 s | 1.91x | 1.94x | 85.1 / 85.2 / 85.1 |
| 1M | 4 | 0.245 s | 0.583 s | 0.634 s | 2.38x | 2.59x | 85.1 / 85.2 / 85.1 |
| 4M | 1 | 1.933 s | 2.476 s | 2.552 s | 1.28x | 1.32x | 340.4 / 340.6 / 340.6 |
| 4M | 2 | 0.942 s | 1.315 s | 1.317 s | 1.40x | 1.40x | 340.4 / 340.6 / 340.6 |
| 4M | 4 | 0.527 s | 0.774 s | 0.685 s | 1.47x | 1.30x | 340.4 / 340.6 / 340.6 |
| 16M | 1 | 7.540 s | 9.879 s | 10.494 s | 1.31x | 1.39x | 1361.6 / 1362.5 / 1362.5 |
| 16M | 2 | 3.973 s | 5.112 s | 5.631 s | 1.29x | 1.42x | 1361.6 / 1362.5 / 1362.5 |
| 16M | 4 | 2.029 s | 2.714 s | 3.103 s | 1.34x | 1.53x | 1361.6 / 1362.5 / 1362.5 |

## TPC-DS (Table encoder, before and after this PR)

TPC-DS is built from Arrow batches, not from one `Distribution`, so it keeps the Table path and does not use `write_dist_parquet`. It does share the encoder changes, which are no copy of finished pages and one pass fewer when dictionaries are off. This table is `tpcds-gen` with its defaults (Snappy, all tables) on PR 49's head and on this PR, run alternately to tmpfs. SF 1 is the best of 3 runs and SF 10 the best of 2. The output files are byte-identical.

| SF | Threads | Before | After | Speedup | Files identical | Output MB |
|---|---|---|---|---|---|---|
| 1 | 1 | 3.29 s | 3.28 s | 1.00x | yes | 376 |
| 1 | 4 | 0.93 s | 0.90 s | 1.04x | yes | 376 |
| 10 | 1 | 34.81 s | 33.39 s | 1.04x | yes | 3899 |
| 10 | 4 | 9.49 s | 9.19 s | 1.03x | yes | 3899 |

## File sizes, default settings (direct / Table / arrow-rs)

| Shape | 1M | 4M | 16M |
|---|---|---|---|
| numbers | 26.1 / 26.1 / 26.1 MB | 104.3 / 104.3 / 104.3 MB | 417.1 / 417.1 / 417.2 MB |
| strings | 62.6 / 62.6 / 62.6 MB | 250.5 / 250.5 / 250.5 MB | 1001.8 / 1001.9 / 1001.8 MB |
| lists | 48.3 / 48.4 / 48.4 MB | 193.4 / 193.6 / 193.7 MB | 773.5 / 774.4 / 774.6 MB |
| mixed | 137.0 / 137.1 / 137.1 MB | 548.0 / 548.3 / 548.3 MB | 2192.3 / 2193.4 / 2193.4 MB |

The list files are about 0.1% smaller on the direct path because its list pages split differently from arrow-rs's. The values are the same.

## Run it

```
cargo run --release -p populate-bench --bin direct_parquet -- --rows 1000000,4000000,16000000 --threads 1,2,4 --reps 3 [--plain] [--snappy]
```
