#!/usr/bin/env bash
# End-to-end generation benchmark: populate-tpch / populate-tpcds against
# tpcgen-cli (tpchgen-rs), every table of a scale factor to files.
#
#   scripts/e2e-bench.sh [--suite tpch|tpcds|both] [--sf "1 10"] [--threads "1 4"]
#                        [--formats "text parquet"] [--runs 3] [--out DIR] [--cli PATH]
#
# Each configuration runs both tools alternately (ours, theirs, ours, ...)
# `--runs` times and keeps each one's best wall time, so drift in a noisy
# machine hits both alike. Output goes to `--out` (default /dev/shm, tmpfs)
# and is deleted after every run. `text` is .tbl for TPC-H and .dat for
# TPC-DS (UTF-8, as tpcgen-cli writes it). Prints a Markdown table.
#
# Needs `tpcgen-cli` (cargo install tpcgen-cli) on PATH or as --cli.
set -euo pipefail

suite=both sfs="1" threads="1 4" formats="text parquet" runs=3
out=/dev/shm/e2e-bench cli=$(command -v tpcgen-cli || true)
while [ $# -gt 0 ]; do
    case "$1" in
        --suite) suite=$2; shift 2 ;;
        --sf) sfs=$2; shift 2 ;;
        --threads) threads=$2; shift 2 ;;
        --formats) formats=$2; shift 2 ;;
        --runs) runs=$2; shift 2 ;;
        --out) out=$2; shift 2 ;;
        --cli) cli=$2; shift 2 ;;
        *) echo "unknown flag $1" >&2; exit 2 ;;
    esac
done
[ -x "$cli" ] || { echo "tpcgen-cli not found: cargo install tpcgen-cli, or pass --cli" >&2; exit 2; }

root=$(cd "$(dirname "$0")/.." && pwd)
cargo build --release --quiet --manifest-path "$root/Cargo.toml" \
    -p populate-tpch --bin tpch-gen -p populate-tpcds --bin tpcds-gen
bin="$root/target/release"

# wall seconds of one run of "$@", output wiped before and after
run() {
    rm -rf "$out"; mkdir -p "$out"
    local t0 t1
    t0=$(date +%s.%N)
    "$@" > /dev/null 2>&1
    t1=$(date +%s.%N)
    sync
    du -sb "$out" | cut -f1 > "$out.size"
    rm -rf "$out"
    echo "$t1 - $t0" | bc
}

min() { printf '%s\n' "$@" | sort -g | head -1; }

echo "| suite | SF | format | threads | tpcgen-cli | populate | speedup | output MB (cli / ours) |"
echo "|---|---:|---|---:|---:|---:|---:|---|"
for s in $( [ "$suite" = both ] && echo "tpch tpcds" || echo "$suite" ); do
    for sf in $sfs; do
        for f in $formats; do
            for t in $threads; do
                if [ "$s" = tpch ]; then
                    ff=$([ "$f" = text ] && echo tbl || echo parquet)
                    ours=("$bin/tpch-gen" --sf "$sf" --out "$out" --format "$ff" --threads "$t")
                    theirs=("$cli" tpch "$ff" -s "$sf" --output-dir "$out" --num-threads "$t")
                else
                    ff=$([ "$f" = text ] && echo dat || echo parquet)
                    ours=("$bin/tpcds-gen" --sf "$sf" --out "$out" --format "$ff" --threads "$t")
                    [ "$f" = text ] && ours+=(--encoding utf8)
                    theirs=("$cli" tpcds "$ff" -s "$sf" --output-dir "$out" --num-threads "$t")
                fi
                a=() b=()
                for _ in $(seq "$runs"); do
                    a+=("$(run "${theirs[@]}")"); sa=$(cat "$out.size")
                    b+=("$(run "${ours[@]}")"); sb=$(cat "$out.size")
                done
                ta=$(min "${a[@]}") tb=$(min "${b[@]}")
                printf '| %s | %s | %s | %s | %.2f s | %.2f s | %.1fx | %d / %d |\n' \
                    "$s" "$sf" "$ff" "$t" "$ta" "$tb" "$(echo "$ta / $tb" | bc -l)" \
                    $((sa / 1000000)) $((sb / 1000000))
            done
        done
    done
done
rm -f "$out.size"
