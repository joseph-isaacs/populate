"""Compose a table from distributions, read it as Arrow, write Parquet.

    cd python && maturin develop --release && python examples/demo.py
"""

import time
from datetime import date

import pyarrow as pa
import pyarrow.parquet as pq

import populate as p

sales = p.table(
    10_000_000,
    {
        "id": p.range(0),
        "store": p.choose(["north", "south", "east", "west"], weights=[4, 3, 2, 1]),
        "sold": p.uniform(date(1992, 1, 1), date(1998, 12, 31)),
        "qty": p.uniform(1, 10),
        "price": p.normal(100.0, 15.0),
        "note": p.text(["gift", "rush", "fragile", "return"], 0, 3).nullable(0.2),
        "tags": p.list(p.uniform(0, 4), p.uniform(0, 1_000)),
    },
    seed=42,
)
print(sales)


def timed(label, f):
    t = time.perf_counter()
    out = f()
    print(f"{label:<32} {time.perf_counter() - t:6.2f} s")
    return out


t = timed("sales -> pyarrow.Table", lambda: pa.table(sales))
for row in t.slice(0, 3).to_pylist():
    print(row)
timed("sales -> sales.parquet", lambda: sales.write_parquet("sales.parquet"))
print(pq.read_metadata("sales.parquet").num_rows, "rows in sales.parquet")

lineitem = p.tpch("lineitem", sf=1)
n = timed("TPC-H SF1 lineitem -> pyarrow", lambda: pa.table(lineitem).num_rows)
print(n, "lineitem rows")
timed("TPC-H SF1 lineitem -> parquet", lambda: lineitem.write_parquet("lineitem.parquet"))
