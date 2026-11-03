# Validating populate-tsbs against the reference TSBS generator

Two references, both produced by real Go code:

1. **`gorand/`**: a Go program that imports only the standard library. It dumps
   `math/rand` outputs (`Int63`, `Float64`, `Intn`, `Int63n`, `NormFloat64`,
   `Uint32`) for several seeds. `tests/golden/gorand.txt` is its output, and the
   `gorand_matches_go_stdlib` test checks `GoRand` + `GoRandExt` against it bit
   for bit.

       go run ./validate/gorand/main.go > tests/golden/gorand.txt

2. **`diff_reference.sh`**: runs `tsbs_generate_data --format=influx` and our
   `examples/tsbs_generate.rs` with the same flags, then byte-diffs the two
   outputs for cpu-only, devops and iot across seeds, scales, intervals, time
   ranges, `--initial-scale` and `--max-data-points`. `LARGE=1` adds runs of
   up to 864k lines, compared by checksum.

       # in a clone of github.com/timescale/tsbs
       go build -o /tmp/tsbs_generate_data ./cmd/tsbs_generate_data
       validate/diff_reference.sh /tmp/tsbs_generate_data

The small files `tests/golden/*.influx` are verbatim excerpts of the reference
binary's output (the flags are in each test), so `cargo test` checks exactness
without needing Go.
