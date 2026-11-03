#!/usr/bin/env bash
# Byte-for-byte diff of populate-tsbs influx output against the reference
# TSBS generator across use cases, seeds, scales and time ranges.
#
# Usage: validate/diff_reference.sh /path/to/tsbs_generate_data
# (build it from github.com/timescale/tsbs: go build ./cmd/tsbs_generate_data)
set -euo pipefail
REF=${1:?path to tsbs_generate_data}
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
cargo build -q --release -p populate-tsbs --example tsbs_generate --manifest-path "$ROOT/Cargo.toml"
OURS="$ROOT/target/release/examples/tsbs_generate"
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

pass=0; fail=0
run() {
  local args="$*"
  "$REF" $args --format=influx > "$TMP/ref.txt" 2>/dev/null
  "$OURS" $args > "$TMP/ours.txt"
  local lines; lines=$(wc -l < "$TMP/ref.txt")
  if cmp -s "$TMP/ref.txt" "$TMP/ours.txt"; then
    pass=$((pass+1)); echo "SAME  ($lines lines) $args"
  else
    fail=$((fail+1)); echo "DIFF  ($lines lines) $args"
    diff "$TMP/ref.txt" "$TMP/ours.txt" | head -4
  fi
}

S=2016-01-01T00:00:00Z
for uc in cpu-only devops iot; do
  for seed in 1 123 -7 987654321; do
    run --use-case=$uc --seed=$seed --scale=1 --timestamp-start=$S --timestamp-end=2016-01-01T01:00:00Z --log-interval=10s
    run --use-case=$uc --seed=$seed --scale=10 --timestamp-start=$S --timestamp-end=2016-01-01T00:30:00Z --log-interval=10s
    run --use-case=$uc --seed=$seed --scale=37 --timestamp-start=$S --timestamp-end=2016-01-01T06:00:00Z --log-interval=1m
  done
  # long runs: walks hit their clamps, IoT queues fill up
  run --use-case=$uc --seed=42 --scale=5 --timestamp-start=$S --timestamp-end=2016-01-08T00:00:00Z --log-interval=5m
  # ramping host count, point limit, odd interval
  run --use-case=$uc --seed=9 --scale=20 --initial-scale=3 --timestamp-start=$S --timestamp-end=2016-01-01T02:00:00Z --log-interval=30s
  run --use-case=$uc --seed=9 --scale=20 --timestamp-start=$S --timestamp-end=2016-01-01T02:00:00Z --log-interval=30s --max-data-points=1234
  run --use-case=$uc --seed=3 --scale=4 --timestamp-start=2017-03-05T07:11:13Z --timestamp-end=2017-03-05T09:00:00Z --log-interval=7s
done
# LARGE=1: bigger runs compared by checksum (nothing written to disk).
if [ "${LARGE:-0}" = 1 ]; then
  for args in \
    "--use-case=cpu-only --seed=2024 --scale=100 --timestamp-start=$S --timestamp-end=2016-01-02T00:00:00Z --log-interval=10s" \
    "--use-case=devops --seed=77 --scale=25 --timestamp-start=$S --timestamp-end=2016-01-01T04:00:00Z --log-interval=10s" \
    "--use-case=iot --seed=5 --scale=100 --timestamp-start=$S --timestamp-end=2016-01-02T00:00:00Z --log-interval=1m" \
    "--use-case=iot --seed=11 --scale=1000 --timestamp-start=$S --timestamp-end=2016-01-01T01:00:00Z --log-interval=10s"; do
    r=$("$REF" $args --format=influx 2>/dev/null | sha256sum)
    o=$("$OURS" $args | sha256sum)
    if [ "$r" = "$o" ]; then pass=$((pass+1)); echo "SAME  (sha256) $args"
    else fail=$((fail+1)); echo "DIFF  (sha256) $args"; fi
  done
fi
echo "exact matches: $pass, mismatches: $fail"
[ "$fail" -eq 0 ]
