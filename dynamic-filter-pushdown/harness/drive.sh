#!/bin/bash
# usage: drive.sh engine bench
cd "$(dirname "$0")"; mkdir -p results
e=$1; b=$2; out=results/${e}_${b}.jsonl; : > $out
one() { # name file idx
  r=$(timeout 300 python3 run.py $e $b "$2" "$3" 2>/dev/null) || r='{"ok": false, "error": "timeout/crash"}'
  echo "{\"q\": \"$1\", \"r\": $r}" >> $out
}
case $e-$b in
  duckdb-tpch) for i in $(seq -w 1 22); do one q$i ddb/extension/tpch/dbgen/queries/q$i.sql -; done;;
  duckdb-tpcds) for i in $(seq -w 1 99); do one q$i ddb/extension/tpcds/dsdgen/queries/$i.sql -; done;;
  dfcli-*-tpch) for i in $(seq 1 22); do one q$(printf %02d $i) dfr/benchmarks/queries/q$i.sql -; done;;
  dfcli-*-tpcds) for i in $(seq 1 99); do one q$(printf %02d $i) dfr/datafusion/core/tests/tpc-ds/$i.sql -; done;;
  dfcli-*-clickbench) for i in $(seq 0 42); do one q$(printf %02d $i) cb/datafusion/queries.sql $i; done;;
  datafusion-tpch) for i in $(seq 1 22); do one q$(printf %02d $i) dfr/benchmarks/queries/q$i.sql -; done;;
  datafusion-tpcds) for i in $(seq 1 99); do one q$(printf %02d $i) dfr/datafusion/core/tests/tpc-ds/$i.sql -; done;;
  duckdb-clickbench) for i in $(seq 0 42); do one q$(printf %02d $i) cb/duckdb-parquet/queries.sql $i; done;;
  datafusion-clickbench) for i in $(seq 0 42); do one q$(printf %02d $i) cb/datafusion/queries.sql $i; done;;
esac
echo done $e $b
