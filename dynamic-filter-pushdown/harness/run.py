"""Run one query under EXPLAIN ANALYZE in DuckDB or DataFusion and emit a JSON description of
every dynamic-filter *producer* (hash join, TopK, min/max aggregate) and every dynamic filter that
reached a scan.

usage: run.py <duckdb|datafusion> <tpch|tpcds|clickbench> <query-file> [<index-in-file>] [pushdown]
"""
import glob, json, os, re, sys

W = os.path.dirname(os.path.abspath(__file__))
engine, bench, qfile = sys.argv[1:4]
qidx = int(sys.argv[4]) if len(sys.argv) > 4 and sys.argv[4] != "-" else None
pushdown = len(sys.argv) > 5 and sys.argv[5] == "pushdown"
text = open(qfile).read()
if qidx is not None:
    text = [l for l in text.splitlines() if l.strip()][qidx]
# some files contain several statements (TPC-DS 14/23/24/39); run each, keep all
text = re.sub(r"--[^\n]*", "", text)
stmts = [s.strip() for s in text.split(";")]
stmts = [s for s in stmts if s]

TOKEN = lambda name: re.compile(r"(?<![\w.])" + re.escape(name) + r"(?![\w])", re.I)


def base(col):
    col = re.sub(r"@\d+", "", col.strip()).strip('"')
    return col.split(".")[-1]


def classify_df(s):
    if "Dynamic Filter (" in s:
        return "topk/agg"
    if re.search(r"hash_lookup|PRF\(|IN \(SET\)|\bIN\s*\(|>=.*<=", s):
        return "join"
    return "topk/agg"


# ------------------------------------------------------------------ DuckDB
def duck():
    import duckdb
    con = duckdb.connect()
    con.sql("SET threads=4")
    if bench == "clickbench":
        p = os.path.join(W, "data/clickbench/hits.parquet")
        con.sql(f"CREATE VIEW hits AS SELECT * REPLACE (make_date(EventDate) AS EventDate) FROM read_parquet('{p}', binary_as_string=True)")
        con.sql("CREATE MACRO toDateTime(t) AS epoch_ms(t * 1000)")
    else:
        for f in glob.glob(os.path.join(W, f"data/{bench}/*.parquet")):
            con.sql(f"CREATE VIEW {os.path.basename(f)[:-8]} AS SELECT * FROM '{f}'")
    prof = os.path.join(W, f"prof_{os.getpid()}.json")
    con.sql("PRAGMA enable_profiling='json'")
    con.sql(f"PRAGMA profiling_output='{prof}'")
    producers, scans = [], []
    for s in stmts:
        con.sql(s).fetchall()
        if not os.path.exists(prof):
            continue  # answered from metadata, nothing profiled
        root = json.load(open(prof))["operator"][0]
        walk_duck(root, producers, scans, [])
    if os.path.exists(prof):
        os.remove(prof)
    return producers, scans


def duck_scans(n):
    out = []
    if n["type"] in ("TABLE_SCAN",):
        out.append(n)
    for c in n.get("children", []):
        out += duck_scans(c)
    return out


def duck_dfs(n):
    ei = n.get("extra_info", {})
    out = as_list(ei.get("Dynamic Filters"))
    # Top-N dynamic filters are rendered inside "Filters" as "optional: Dynamic Filter (col)"
    out += [f for f in as_list(ei.get("Filters")) for f in split_top(f) if "Dynamic Filter" in f]
    return out


def as_list(v):
    if v is None:
        return []
    if isinstance(v, list):
        return v
    return [x for x in str(v).split("\n") if x.strip()]


def walk_duck(n, producers, scans, path):
    t = n["type"]
    ei = n.get("extra_info", {})
    if t == "TABLE_SCAN":
        dfs = duck_dfs(n)
        scans.append(dict(table=ei.get("Table") or ei.get("Filename(s)") or ei.get("Function"),
                          dynamic_filters=dfs, rg=ei.get("Row Groups Scanned")))
    kids = n.get("children", [])
    if "JOIN" in t or t in ("CROSS_PRODUCT", "POSITIONAL_SCAN"):
        conds = as_list(ei.get("Conditions"))
        eq = [c for c in conds if re.search(r"(?<![<>!])=(?!=)", c) and "DISTINCT" not in c]
        probe_scans = duck_scans(kids[0]) if kids else []
        hits = []
        for c in eq:
            probe = base(c.split("=")[0])
            for sc in probe_scans:
                for d in duck_dfs(sc):
                    if TOKEN(probe).search(d):
                        hits.append(d)
        paths = []
        for c in eq:
            paths.append(path_to(kids[0], base(c.split("=")[0]), duck_has_col, lambda x: x["type"], lambda x: x.get("children", [])))
        producers.append(dict(kind="join", op=t, join_type=ei.get("Join Type"), conds=conds, paths=paths,
                              probe_info_missing=any("Function" not in sc.get("extra_info", {}) for sc in probe_scans),
                              build_rows=kids[1].get("operator_cardinality", kids[1].get("intermediate_rows")) if len(kids) > 1 else None,
                              equi=bool(eq), pushed=bool(hits), filters=hits,
                              probe_child=kids[0]["type"] if kids else None,
                              probe_leaves=sorted({x["type"] for x in leaves(kids[0])}) if kids else []))
    if t == "TOP_N":
        keys = as_list(ei.get("Order By"))
        col = base(keys[0].split()[0]) if keys else ""
        hits = [d for sc in duck_scans(n) for d in duck_dfs(sc)
                if "Dynamic Filter (" in d]
        producers.append(dict(kind="topk", op=t, keys=keys, pushed=bool(hits), filters=hits,
                              probe_child=kids[0]["type"] if kids else None))
    if t == "UNGROUPED_AGGREGATE":
        aggs = " ".join(as_list(ei.get("Aggregates")))
        if re.search(r"\b(min|max)\(", aggs):
            hits = [d for sc in duck_scans(n) for d in duck_dfs(sc)
                    if classify_df(d) != "join"]
            producers.append(dict(kind="minmax_agg", op=t, aggs=aggs, pushed=bool(hits), filters=hits))
    for c in kids:
        walk_duck(c, producers, scans, path + [t])


def duck_has_col(n, col):
    if n["type"] != "TABLE_SCAN":
        return False
    pr = as_list(n.get("extra_info", {}).get("Projections"))
    pr = [x for p in pr for x in split_top(p)]
    return any(base(x).lower() == col.lower() for x in pr)


def df_has_col(n, col):
    if n["name"] != "DataSourceExec":
        return False
    m = re.search(r"projection=\[(.*?)\]", n["line"])
    return bool(m) and any(base(x).lower() == col.lower() for x in split_top(m.group(1)))


def path_to(n, col, has, name, kids, acc=None):
    """Operator names from n down to the first scan that outputs `col` (None if not found)."""
    acc = (acc or []) + [name(n)]
    if has(n, col):
        return acc
    for c in kids(n):
        r = path_to(c, col, has, name, kids, acc)
        if r:
            return r
    return None


def leaves(n):
    k = n.get("children", [])
    if not k:
        return [n]
    out = []
    for c in k:
        out += leaves(c)
    return out


# ------------------------------------------------------------------ DataFusion
def datafusion_cli(binary):
    """Same as `datafusion()`, but runs a datafusion-cli binary (for patched builds)."""
    import subprocess, tempfile
    setup = []
    if pushdown:
        setup.append("SET datafusion.execution.parquet.pushdown_filters = true;")
    if bench == "clickbench":
        p = os.path.join(W, "data/clickbench/hits.parquet")
        setup.append(f"CREATE EXTERNAL TABLE hits_raw STORED AS PARQUET LOCATION '{p}' OPTIONS ('binary_as_string' 'true');")
        setup.append('CREATE VIEW hits AS SELECT * EXCEPT ("EventDate"), CAST(CAST("EventDate" AS INTEGER) AS DATE) AS "EventDate" FROM hits_raw;')
    else:
        for f in sorted(glob.glob(os.path.join(W, f"data/{bench}/*.parquet"))):
            setup.append(f"CREATE EXTERNAL TABLE {os.path.basename(f)[:-8]} STORED AS PARQUET LOCATION '{f}';")
    producers, scans = [], []
    for s in stmts:
        if s.lower().startswith(("create", "drop")):
            setup.append(s + ";")
            continue
        with tempfile.NamedTemporaryFile("w", suffix=".sql", dir=W, delete=False) as f:
            f.write("\n".join(setup) + "\nEXPLAIN ANALYZE " + s + ";\n")
            path = f.name
        try:
            out = subprocess.run([binary, "-q", "--format", "json", "-f", path],
                                 capture_output=True, text=True, timeout=280)
        finally:
            os.remove(path)
        rows = None
        for line in out.stdout.splitlines()[::-1]:
            line = line.strip()
            if line.startswith("["):
                rows = json.loads(line)
                break
        if not rows:
            raise RuntimeError((out.stderr or out.stdout)[-400:])
        plan = next(r["plan"] for r in rows if r["plan_type"] == "Plan with Metrics")
        walk_df(parse_df(plan), producers, scans)
    return producers, scans


def datafusion():
    from datafusion import SessionConfig, SessionContext
    cfg = SessionConfig().with_target_partitions(4).with_information_schema(True)
    ctx = SessionContext(cfg)
    if pushdown:
        ctx.sql("SET datafusion.execution.parquet.pushdown_filters = true").collect()
    if bench == "clickbench":
        p = os.path.join(W, "data/clickbench/hits.parquet")
        ctx.sql(f"CREATE EXTERNAL TABLE hits_raw STORED AS PARQUET LOCATION '{p}' OPTIONS ('binary_as_string' 'true')").collect()
        ctx.sql('CREATE VIEW hits AS SELECT * EXCEPT ("EventDate"), CAST(CAST("EventDate" AS INTEGER) AS DATE) AS "EventDate" FROM hits_raw').collect()
    else:
        for f in glob.glob(os.path.join(W, f"data/{bench}/*.parquet")):
            ctx.register_parquet(os.path.basename(f)[:-8], f)
    producers, scans = [], []
    for s in stmts:
        if s.lower().startswith(("create", "drop")):
            ctx.sql(s).collect()
            continue
        b = ctx.sql("EXPLAIN ANALYZE " + s).collect()
        plan = b[0].column(1)[0].as_py()
        tree = parse_df(plan)
        walk_df(tree, producers, scans)
        # infer_unclaimed(tree, producers)  # disabled: false positives on outer joins
    return producers, scans


def parse_df(plan):
    root = {"name": "ROOT", "line": "", "children": []}
    stack = [(-1, root)]
    for line in plan.splitlines():
        if not line.strip():
            continue
        ind = len(line) - len(line.lstrip(" "))
        node = {"name": re.match(r"\s*(\w+)", line).group(1), "line": line.strip(), "children": []}
        while stack[-1][0] >= ind:
            stack.pop()
        stack[-1][1]["children"].append(node)
        stack.append((ind, node))
    return root["children"][0]


def extract_dfs(line):
    out, i = [], 0
    while True:
        i = line.find("DynamicFilter [", i)
        if i < 0:
            return out
        j, depth = i + len("DynamicFilter "), 0
        for k in range(j, len(line)):
            if line[k] == "[":
                depth += 1
            elif line[k] == "]":
                depth -= 1
                if depth == 0:
                    out.append(line[i:k + 1])
                    break
        i = k + 1


def df_scans(n):
    out = [n] if n["name"] == "DataSourceExec" else []
    for c in n["children"]:
        out += df_scans(c)
    return out


def df_leaves(n):
    if not n["children"]:
        return [n]
    out = []
    for c in n["children"]:
        out += df_leaves(c)
    return out


def split_top(s, sep=","):
    out, depth, cur = [], 0, ""
    for ch in s:
        if ch in "([":
            depth += 1
        elif ch in ")]":
            depth -= 1
        if ch == sep and depth == 0:
            out.append(cur.strip()); cur = ""
        else:
            cur += ch
    if cur.strip():
        out.append(cur.strip())
    return out


def parse_on(line):
    i = line.find("on=[")
    if i < 0:
        return []
    depth, j = 0, i + 3
    for k in range(j, len(line)):
        depth += line[k] == "["
        depth -= line[k] == "]"
        if depth == 0:
            break
    pairs = []
    for g in split_top(line[j + 1:k]):
        parts = split_top(g.strip()[1:-1])
        if len(parts) == 2:
            pairs.append(tuple(parts))
    return pairs


def probe_col(expr):
    m = re.findall(r"([\w\.\"]+)@\d+", expr)
    return base(m[0]) if m else base(expr)


def alias_map(n, m=None):
    """alias -> set(source columns) from every Projection/Aggregate 'x@i as y' in the subtree."""
    m = {} if m is None else m
    if n["name"] in ("ProjectionExec", "AggregateExec"):
        for src, al in re.findall(r"([\w\.\"()]+@\d+(?: AS \w+\))?) as ([\w\.\"]+)", n["line"]):
            cols = re.findall(r"([\w\.\"]+)@\d+", src)
            if cols:
                m.setdefault(base(al).lower(), set()).add(base(cols[0]).lower())
    for c in n["children"]:
        alias_map(c, m)
    return m


def resolve(col, m):
    out, todo = set(), [col.lower()]
    while todo:
        c = todo.pop()
        if c in out:
            continue
        out.add(c)
        todo += list(m.get(c, ()))
    return out


def join_nodes(n):
    out = [n] if "prod" in n else []
    for c in n["children"]:
        out += join_nodes(c)
    return out


def infer_unclaimed(n, producers):
    """A join whose filter is written in terms of renamed columns (union branches, folded
    projections) isn't matched by name. Credit it with join-type filters in its probe subtree
    that no join inside that subtree claims."""
    for c in n["children"]:
        infer_unclaimed(c, producers)
    if "prod" not in n or n["hits"] or len(n["children"]) < 2:
        return
    probe = n["children"][1]
    claimed = {d for j in join_nodes(probe) for d in j["hits"]}
    un = [d for sc in df_scans(probe) for d in extract_dfs(sc["line"])
          if d not in claimed and classify_df(d) == "join"]
    if un:
        n["hits"] = un
        p = producers[n["prod"]]
        p["pushed"] = True
        p["filters"] = un
        p["inferred"] = True


def walk_df(n, producers, scans):
    name, line, kids = n["name"], n["line"], n["children"]
    if name == "DataSourceExec":
        pred = re.search(r"predicate=(.*?), (pruning_predicate|metrics|required_guarantees|dynamic_rg)", line)
        m = re.search(r"row_groups_pruned_statistics=(\d+) total → (\d+) matched", line)
        dm = re.search(r"row_groups_pruned_dynamic_filter=(\d+)", line)
        pr = re.search(r"pushdown_rows_pruned=([\d.]+ ?[KM]?)", line)
        f = re.search(r"/([\w]+)\.parquet", line)
        scans.append(dict(table=f.group(1) if f else None, dynamic_filters=extract_dfs(line),
                          rg=m.groups() if m else None, rg_pruned_dynamic=dm.group(1) if dm else None,
                          pushdown_rows_pruned=pr.group(1) if pr else None))
    if name in ("HashJoinExec", "NestedLoopJoinExec", "SortMergeJoinExec", "CrossJoinExec",
                "SymmetricHashJoinExec", "PiecewiseMergeJoinExec"):
        jt = re.search(r"join_type=(\w+)", line)
        on = parse_on(line)
        probe = kids[1] if len(kids) > 1 else None
        hits = []
        if probe is not None:
            amap = alias_map(probe)
            for _, pcol in on:
              for pc in resolve(probe_col(pcol), amap):
                for sc in df_scans(probe):
                    for d in extract_dfs(sc["line"]):
                        if TOKEN(pc).search(d) and d not in hits:
                            hits.append(d)
        paths = [path_to(probe, probe_col(pc), df_has_col, lambda x: x["name"], lambda x: x["children"]) for _, pc in on] if probe else []
        if not hits and probe is not None:
            # filters with no column reference (build side empty -> "false", or never filled -> "empty")
            for sc in df_scans(probe):
                for d in extract_dfs(sc["line"]):
                    if re.fullmatch(r"DynamicFilter \[ (false|true|empty) \]", d) and d not in hits:
                        hits.append(d)
        br = re.search(r"build_input_rows=([\d.]+ ?[KM]?)[,\]]", line)
        n["hits"] = hits
        n["prod"] = len(producers)
        producers.append(dict(kind="join", op=name, join_type=jt.group(1) if jt else None, paths=paths,
                              build_rows=br.group(1).strip() if br else None,
                              mode=(re.search(r"mode=(\w+)", line) or [None, None])[1],
                              conds=[f"{a} = {b}" for a, b in on], equi=bool(on), pushed=bool(hits),
                              filters=hits, probe_child=probe["name"] if probe else None,
                              probe_leaves=sorted({x["name"] for x in df_leaves(probe)}) if probe else []))
    if name == "SortExec" and "TopK" in line:
        keys = re.search(r"expr=\[(.*?)\], preserve", line)
        keys = keys.group(1) if keys else ""
        col = base(keys.split(",")[0].split()[0]) if keys else ""
        hits = [d for sc in df_scans(n) for d in extract_dfs(sc["line"])
                if (col and TOKEN(col).search(d) and classify_df(d) != "join") or d == "DynamicFilter [ empty ]"]
        producers.append(dict(kind="topk", op=name, keys=keys, pushed=bool(hits), filters=hits,
                              probe_child=kids[0]["name"] if kids else None))
    if name == "AggregateExec" and "gby=[]" in line and re.search(r"aggr=\[[^\]]*\b(min|max)\(", line) and "mode=Partial" in line:
        hits = [d for sc in df_scans(n) for d in extract_dfs(sc["line"]) if classify_df(d) != "join"]
        producers.append(dict(kind="minmax_agg", op=name, pushed=bool(hits), filters=hits))
    for c in kids:
        walk_df(c, producers, scans)


try:
    if engine == "duckdb":
        p, s = duck()
    elif engine.startswith("dfcli-"):
        p, s = datafusion_cli(os.path.join(W, engine))
    else:
        p, s = datafusion()
    print(json.dumps(dict(ok=True, producers=p, scans=s)))
except Exception as e:  # noqa: BLE001
    print(json.dumps(dict(ok=False, error=str(e)[:500])))
