#!/usr/bin/env bash
# populate-gen-ssb against ssb-dbgen: generate every SSB table at each scale
# factor with both, `cmp` the .tbl files, and time both.
#
#   scripts/ssb-check.sh [--sf "0.01 1 10"] [--threads 0] [--out DIR] [--dbgen PATH]
#
# Without --dbgen, clones eyalroz/ssb-dbgen into $out/ssb-dbgen and builds
# it (default CMake options: `|` separators with a trailing `|`). Output
# goes to --out (default /dev/shm/ssb-check) and is deleted per scale
# factor. Prints a Markdown table; exits non-zero on any difference.
set -euo pipefail

sfs="0.01 1" threads=0 out=/dev/shm/ssb-check dbgen=""
while [ $# -gt 0 ]; do
    case "$1" in
        --sf) sfs=$2; shift 2 ;;
        --threads) threads=$2; shift 2 ;;
        --out) out=$2; shift 2 ;;
        --dbgen) dbgen=$2; shift 2 ;;
        *) echo "unknown flag $1" >&2; exit 2 ;;
    esac
done

root=$(cd "$(dirname "$0")/.." && pwd)
mkdir -p "$out"
if [ -z "$dbgen" ]; then
    src="$out/ssb-dbgen"
    [ -d "$src" ] || git clone --quiet --depth 1 https://github.com/eyalroz/ssb-dbgen "$src"
    cmake -S "$src" -B "$src/build" -DCMAKE_BUILD_TYPE=Release > /dev/null
    cmake --build "$src/build" -j > /dev/null
    dbgen="$src/build/dbgen"
fi
dists="$(dirname "$dbgen")/dists.dss"
[ -f "$dists" ] || dists="$(dirname "$dbgen")/../dists.dss"
cargo build --release --quiet --manifest-path "$root/Cargo.toml" -p populate-gen-ssb --bin ssb-gen
ours="$root/target/release/ssb-gen"

command -v bc > /dev/null || { echo "needs bc" >&2; exit 2; }
secs() { local t0 t1; t0=$(date +%s.%N); "$@" > /dev/null 2>&1 || return 1; t1=$(date +%s.%N); echo "$t1 - $t0" | bc; }

status=0
echo "| SF | ssb-dbgen | populate-gen-ssb | speedup | identical |"
echo "|---:|---:|---:|---:|---|"
for sf in $sfs; do
    rm -rf "$out/ref" "$out/ours"; mkdir -p "$out/ref" "$out/ours"
    t_ref=$(cd "$out/ref" && secs "$dbgen" -q -f -s "$sf" -b "$dists") || { echo "dbgen -s $sf failed" >&2; exit 1; }
    t_ours=$(secs "$ours" --sf "$sf" --format tbl --threads "$threads" --out "$out/ours") || { echo "ssb-gen --sf $sf failed" >&2; exit 1; }
    differ=""
    for t in part supplier customer date lineorder; do
        cmp -s "$out/ref/$t.tbl" "$out/ours/$t.tbl" || { differ="$differ $t"; status=1; }
    done
    same=$([ -z "$differ" ] && echo yes || echo "no:$differ")
    printf '| %s | %.2f s | %.2f s | %.1fx | %s |\n' "$sf" "$t_ref" "$t_ours" \
        "$(echo "$t_ref / $t_ours" | bc -l)" "$same"
    rm -rf "$out/ref" "$out/ours"
done
exit $status
