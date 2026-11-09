#!/bin/bash
# compare sorted query results of base vs new; on mismatch, check if base is itself unstable (ties at LIMIT)
cd "$(dirname "$0")"
cs="CREATE EXTERNAL TABLE hits_raw STORED AS PARQUET LOCATION '$PWD/data/clickbench/hits.parquet' OPTIONS ('binary_as_string' 'true'); CREATE VIEW hits AS SELECT * EXCEPT (\"EventDate\"), CAST(CAST(\"EventDate\" AS INTEGER) AS DATE) AS \"EventDate\" FROM hits_raw;"
run() { # bin bench sql-file-or-query
  if [ $2 = clickbench ]; then timeout 280 ./$1 -q --format csv -c "$cs" -c "$3" 2>&1 | sort | md5sum | cut -c1-12
  else timeout 280 ./$1 -q --format csv -f cli/setup_$2.sql -f "$3" 2>&1 | sort | md5sum | cut -c1-12; fi; }
cmp1() { a=$(run dfcli-base $1 "$2"); n=$(run dfcli-new $1 "$2")
  if [ "$a" = "$n" ]; then echo "same $1 $3"; return; fi
  for k in 1 2 3; do a2=$(run dfcli-base $1 "$2"); [ "$a2" = "$n" ] && { echo "same $1 $3 (after rerun)"; return; }; [ "$a2" != "$a" ] && { echo "TIES $1 $3"; return; }; done
  echo "DIFF $1 $3"; }
for i in $(seq 1 22); do cmp1 tpch dfr/benchmarks/queries/q$i.sql q$i; done
for i in $(seq 1 99); do cmp1 tpcds dfr/datafusion/core/tests/tpc-ds/$i.sql q$i; done
i=0; while IFS= read -r q; do [ -z "$q" ] && continue; cmp1 clickbench "$q" q$i; i=$((i+1)); done < cb/datafusion/queries.sql
