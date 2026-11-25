# compare-parquet.py REF_OUT MINE_DIR: compare a Spark datagen run's raw Parquet
# (REF_OUT/graphs/parquet/raw/composite-merged-fk, part files in partition
# order, i.e. block order) with `ldbc-gen --format parquet --out MINE_DIR`,
# value by value, one column at a time. Needs pyarrow.
import sys, glob, re, os
import pyarrow as pa, pyarrow.parquet as pq, pyarrow.compute as pc
ref = sys.argv[1] + '/graphs/parquet/raw/composite-merged-fk'
mine = sys.argv[2]
def norm(t):
    cols = []
    for c in t.columns:
        ty = c.type
        if pa.types.is_timestamp(ty): c = c.cast(pa.timestamp('ms')).cast(pa.int64())
        elif pa.types.is_date32(ty): c = pc.multiply(c.cast(pa.int32()).cast(pa.int64()), 86400000)
        cols.append(c.combine_chunks() if hasattr(c, 'combine_chunks') else c)
    return cols
bad = 0
for kind in ['dynamic', 'static']:
    for d in sorted(p for p in glob.glob(f'{ref}/{kind}/*') if os.path.isdir(p)):
        name = os.path.basename(d)
        parts = sorted(glob.glob(d + '/part_*.parquet'), key=lambda p: [int(x) for x in re.findall(r'part_(\d+)_(\d+)', p)[0]])
        mine_f = pq.ParquetFile(f'{mine}/{kind}/{name}.parquet')
        rschema = pq.read_schema(parts[0])
        rrows = sum(pq.ParquetFile(p).metadata.num_rows for p in parts)
        mrows = mine_f.metadata.num_rows
        ok = rrows == mrows and len(rschema) == len(mine_f.schema_arrow)
        why = '' if ok else f'rows {rrows} vs {mrows}, cols {len(rschema)} vs {len(mine_f.schema_arrow)}'
        # one column at a time, to keep memory small
        for i in range(len(rschema)) if ok else []:
            a = norm(pa.concat_tables([pq.read_table(p, columns=[rschema.names[i]]) for p in parts]))[0]
            b = norm(pq.read_table(f'{mine}/{kind}/{name}.parquet', columns=[mine_f.schema_arrow.names[i]]))[0]
            if a.type != b.type:
                try: b = b.cast(a.type)
                except Exception as e: ok, why = False, f'col {rschema.names[i]} {a.type} vs {b.type}'; break
            if not a.equals(b):
                eq = pc.fill_null(pc.equal(a, b), False)
                both_null = pc.and_(pc.is_null(a), pc.is_null(b))
                idx = pc.index(pc.invert(pc.or_(eq, both_null)), True).as_py()
                if idx >= 0:
                    ok, why = False, f'col {rschema.names[i]} row {idx}: {a[idx]} vs {b[idx]}'; break
        print(('exact ' if ok else 'DIFF  ') + f'{kind}/{name} ({rrows} rows, {len(parts)} parts) {why}')
        bad += not ok
sys.exit(1 if bad else 0)
