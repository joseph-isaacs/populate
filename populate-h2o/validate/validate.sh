#!/usr/bin/env bash
# Byte-for-byte check of populate-h2o against the db-benchmark R reference.
#
#   populate-h2o/validate/validate.sh WORKDIR [groupby|join|all]
#
# Needs Rscript with data.table, and curl (fetches the reference scripts from
# duckdblabs/db-benchmark). R's CSVs are written to WORKDIR, hashed and
# deleted; ours are streamed straight into sha256sum.
set -euo pipefail

work=${1:?usage: validate.sh WORKDIR [groupby|join|all]}
what=${2:-all}
root=$(cd "$(dirname "$0")/../.." && pwd)
mkdir -p "$work"
work=$(cd "$work" && pwd)

for f in groupby-datagen.R join-datagen.R; do
  [ -f "$work/$f" ] || curl -sSf -o "$work/$f" \
    "https://raw.githubusercontent.com/duckdblabs/db-benchmark/main/_data/$f"
done

cargo build -q --release -p populate-h2o --example h2o_datagen --manifest-path "$root/Cargo.toml"
ours="$root/target/release/examples/h2o_datagen"

fail=0
check() { # label r_hash our_hash
  if [ "$2" = "$3" ]; then echo "MATCH    $1"; else echo "MISMATCH $1 (R $2, ours $3)"; fail=1; fi
}

if [ "$what" = groupby ] || [ "$what" = all ]; then
  for cfg in "1e4 1e1 0 0" "1e4 1e2 0 0" "1e4 1e1 5 1" "1e4 1e2 5 0" \
             "1e5 1e1 0 1" "1e5 1e2 5 0" "1e5 1e2 5 1" "1e6 1e2 0 0" \
             "1e6 1e1 5 1" "1e6 2e0 50 1" "1e7 1e2 0 0" "1e7 1e1 5 1"; do
    (cd "$work" && Rscript groupby-datagen.R $cfg >/dev/null)
    f=$(ls -t "$work"/G1_*.csv | head -1)
    r=$(sha256sum "$f" | cut -d' ' -f1); rm -f "$f"
    o=$("$ours" groupby $cfg - | sha256sum | cut -d' ' -f1)
    check "groupby $cfg" "$r" "$o"
  done
fi

if [ "$what" = join ] || [ "$what" = all ]; then
  for cfg in "1e7 0 0 0" "1e7 0 0 1" "1e7 0 5 0" "1e7 0 5 1"; do
    read -r n _ nas sort <<<"$cfg"
    (cd "$work" && Rscript join-datagen.R $cfg >/dev/null)
    r=$(cd "$work" && cat J1_${n}_NA_${nas}_${sort}.csv J1_${n}_1e1_${nas}_${sort}.csv \
          J1_${n}_1e4_${nas}_${sort}.csv J1_${n}_${n}_${nas}_${sort}.csv | sha256sum | cut -d' ' -f1)
    rm -f "$work"/J1_*.csv
    o=$("$ours" join $cfg - | sha256sum | cut -d' ' -f1)
    check "join $cfg" "$r" "$o"
  done
fi
exit $fail
