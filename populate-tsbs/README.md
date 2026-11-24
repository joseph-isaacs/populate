# populate-tsbs

[TSBS](https://github.com/timescale/tsbs) data, byte-identical to
`tsbs_generate_data`, on all cores.

All five data use cases (`devops`, `cpu-only`, `cpu-single`,
`devops-generic`, `iot`) for any `--seed`, scale, initial scale, time range,
log interval, `--max-data-points`, `--max-metric-count` and interleaved
group, written as TSBS's text formats or as Arrow tables and Parquet files
(one per measurement).

```sh
# the same bytes as tsbs_generate_data with the same flags
cargo run -p populate-tsbs --release --bin tsbs-gen -- \
    --use-case devops --scale 100 --seed 123 --format influx \
    --timestamp-start 2016-01-01T00:00:00Z --timestamp-end 2016-01-02T00:00:00Z > devops.influx

# one Parquet file per measurement: tsbs/cpu.parquet, tsbs/mem.parquet, ...
cargo run -p populate-tsbs --release --bin tsbs-gen -- \
    --use-case iot --scale 1000 --seed 123 --format parquet --out tsbs
```

| `--format` | serializer | |
|---|---|---|
| `influx`, `victoriametrics`, `questdb` | line protocol | |
| `timescaledb`, `clickhouse` | `tags,...` line + values line | with the tag/field header |
| `timestream` | the timescaledb serializer | no header |
| `cratedb` | TSV with JSON tags | with the header |
| `cassandra` | one CSV line per field | devops use cases (Go panics on IoT's float tags) |
| `prometheus` | a version byte, then a length-prefixed protobuf `TimeSeries` per field | devops use cases |
| `siridb` | qpack, with length headers | devops use cases |
| `mongo` | length-prefixed `MongoPoint` flatbuffers (Go's builder ported, vtable sharing included) | devops use cases |
| `parquet` | one file per measurement | `--out DIR` |
| `arrow` | generate the Arrow tables and drop them | for timing |

`akumuli` panics in `tsbs_generate_data` itself, so there is nothing to
match.

## How it matches Go

TSBS draws every value from Go's global `math/rand` source, seeded once by
`rand.Seed(seed)`, in one fixed order: host construction, then each tick
host by host, measurement by measurement, field by field, and for IoT the
batch decisions (missing, out-of-order, zeroed entries) in between. So the
simulation is a faithful port:

- `gorand`: Go's additive lagged Fibonacci source (607 words, tap 273) with
  its `seedrand` seeding and cooked table, and the `Rand` methods TSBS calls
  (`Int63n`, `Intn`, `Float64`, `NormFloat64`'s ziggurat, `Zipf`), with the
  same rejection loops so the stream stays in step.
- `math.Exp` and `math.Log` as Go's **amd64 assembly** computes them (not
  the portable Go code, and not libm), including the FMA variant Go selects
  at run time on CPUs with AVX and FMA. They reach the output through the
  ziggurat's tail and the Zipf metric counts.
- Go's conversions (`int64(f)` is `CVTTSD2SQ`, `uint64(f)` its two-step
  variant) and its float formatting (`strconv`'s shortest digits: exact ties
  go to the even digit, which Rust's `Display` doesn't do).
- The quirks: the shared `loadUD` that makes every truck's `current_load`
  the last value drawn by any truck, `postgresl`, cpu fields as integers,
  IoT running past `maxPoints` while it fills a batch, and so on.

`tests/golden.rs` holds the length and FNV-1a hash of `tsbs_generate_data`'s
output for every use case and text format plus the flags that change the
run; with `TSBS_GENERATE_DATA=/path/to/tsbs_generate_data` it also compares
204 runs byte for byte against the binary.

## Parallel, though the RNG is sequential

Point `n` depends on every draw before it, so chunks can't be computed from
their index alone. Instead a chunk is a resumable stretch of the run:
chunk `k` starts where the simulator's `madePoints` first reaches a
threshold, and a clone of the simulator there is all a worker needs.

A *frontier* simulator skips through the chunks in order, building no
points, and leaves a checkpoint at each boundary; a worker that wants chunk
`k` takes its checkpoint or advances the frontier to it, then generates and
formats the chunk. For devops the frontier's work is just the ticks (the
`NormFloat64`s and walks), and chunks are whole epochs, so workers only
format; the host state is behind an `Arc`, so a checkpoint costs a pointer
and the 5 KB generator state. IoT's frontier runs the full batch simulation,
which is still much cheaper than the text.

Chunks go through `populate-table`'s drivers: `write_text_many` for text
(workers write the file in order), `MultiChunks` and `write_parquet_many`
for Parquet (row groups encoded on the workers). The output is identical
for any thread count and chunk size (`tests/chunks.rs` checks chunk sizes
from 1 point up, against one sequential run that mirrors Go's
`runSimulator` loop).

## Performance

`tsbs_generate_data` (Go 1.24) against `tsbs-gen`, one day at the default
10 s interval, seed 123, output to `/dev/null`, 4-core x86-64:

| use case | scale | format | output | Go | tsbs-gen, 1 thread | tsbs-gen, 4 threads |
|---|---:|---|---:|---:|---:|---:|
| devops | 100 | influx | 3.32 GB | 11.05 s | 1.76 s (6.3x) | 0.58 s (19x) |
| devops | 100 | timescaledb | 2.13 GB | 10.16 s | 1.69 s (6.0x) | 0.49 s (21x) |
| devops | 100 | cratedb | 2.45 GB | 10.84 s | 1.57 s (6.9x) | 0.51 s (21x) |
| devops | 100 | cassandra | 21.3 GB | 36.93 s | 3.48 s (11x) | 1.29 s (29x) |
| devops | 100 | prometheus | 23.0 GB | 100.6 s | 4.75 s (21x) | 1.79 s (56x) |
| devops | 100 | siridb | 4.50 GB | 36.89 s | 3.14 s (12x) | 0.86 s (43x) |
| devops | 100 | mongo | 7.88 GB | 21.87 s | 6.93 s (3.2x) | 2.00 s (11x) |
| cpu-only | 1000 | influx | 2.99 GB | 9.92 s | 1.73 s (5.7x) | 0.61 s (16x) |
| iot | 100 | influx | 0.37 GB | 4.17 s | 0.48 s (8.6x) | 0.22 s (19x) |
| devops-generic | 200 | influx | 0.44 GB | 2.19 s | 0.36 s (6.2x) | 0.18 s (12x) |

Where the time goes: the frontier's work is Go's generator (batched: up
to 273 lagged-Fibonacci steps per vectorised pass) and the walks; workers
replay the frontier's tick states rather than redoing them, and format
with every host's tags and every measurement's keys pre-rendered.

## Arrow and Parquet

One table per measurement (`cpu`, `diskio`, ..., `redis`; `readings`,
`diagnostics`; `generic_metrics`) with a UTC nanosecond `time` column, a
`Utf8` column per tag (IoT's capacities are `Float32`), then the fields as
`Int64` or `Float64`. Rows are in the order the text formats write them, and
`--interleaved-generation-group-id` filters them the same way. IoT's
cleared tags and fields are nulls, as are the metrics a devops-generic host
doesn't have.
