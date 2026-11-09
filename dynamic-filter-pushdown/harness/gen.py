"""Synthetic data generator for TPC-H (SF1), TPC-DS (SF1 row counts) and ClickBench (hits).

Schemas/row counts/key ranges follow the specs; value distributions are synthetic but string
columns are drawn from a pool that includes the literals used by the benchmark queries so that
filters are selective-but-non-empty (empty build sides would short-circuit joins and hide
dynamic-filter behaviour).
"""
import glob, os, re, sys
import duckdb

W = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(W, "data")
con = duckdb.connect()
con.sql("SET threads=4; SET preserve_insertion_order=true")


def out(bench, table, select_sql, order_by=None):
    d = os.path.join(DATA, bench)
    os.makedirs(d, exist_ok=True)
    path = os.path.join(d, f"{table}.parquet")
    if os.path.exists(path):
        return
    q = f"SELECT * FROM ({select_sql})" + (f" ORDER BY {order_by}" if order_by else "")
    con.sql(f"COPY ({q}) TO '{path}' (FORMAT parquet, ROW_GROUP_SIZE 122880)")
    print("wrote", bench, table, file=sys.stderr)


def pick(arr, seed):
    lst = ",".join("'" + s.replace("'", "''") + "'" for s in arr)
    return f"([{lst}])[1 + (hash(i * 1000003 + {seed} * 7919) % {len(arr)})::int]"


def rnd(lo, hi, seed):
    return f"({lo} + (hash(i * 1000003 + {seed} * 7919) % ({hi - lo + 1}))::bigint)"


# --------------------------------------------------------------------------- TPC-H
def tpch():
    nations = ["ALGERIA", "ARGENTINA", "BRAZIL", "CANADA", "EGYPT", "ETHIOPIA", "FRANCE", "GERMANY",
               "INDIA", "INDONESIA", "IRAN", "IRAQ", "JAPAN", "JORDAN", "KENYA", "MOROCCO", "MOZAMBIQUE",
               "PERU", "CHINA", "ROMANIA", "SAUDI ARABIA", "VIETNAM", "RUSSIA", "UNITED KINGDOM",
               "UNITED STATES"]
    nreg = [0, 1, 1, 1, 4, 0, 3, 3, 2, 2, 4, 4, 2, 4, 0, 0, 0, 1, 2, 3, 4, 2, 3, 3, 1]
    regions = ["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"]
    words = ["almond", "antique", "aquamarine", "azure", "beige", "bisque", "black", "blanched", "blue",
             "blush", "brown", "burlywood", "burnished", "chartreuse", "chiffon", "chocolate", "coral",
             "cornflower", "cornsilk", "cream", "cyan", "dark", "deep", "dim", "dodger", "drab",
             "firebrick", "floral", "forest", "frosted", "gainsboro", "ghost", "goldenrod", "green",
             "grey", "honeydew", "hot", "indian", "ivory", "khaki", "lace", "lavender", "lawn", "lemon",
             "light", "lime", "linen", "magenta", "maroon", "medium", "metallic", "midnight", "mint"]
    types1 = ["STANDARD", "SMALL", "MEDIUM", "LARGE", "ECONOMY", "PROMO"]
    types2 = ["ANODIZED", "BURNISHED", "PLATED", "POLISHED", "BRUSHED"]
    types3 = ["TIN", "NICKEL", "BRASS", "STEEL", "COPPER"]
    cont1 = ["SM", "LG", "MED", "JUMBO", "WRAP"]
    cont2 = ["CASE", "BOX", "BAG", "JAR", "PKG", "PACK", "CAN", "DRUM"]
    seg = ["AUTOMOBILE", "BUILDING", "FURNITURE", "MACHINERY", "HOUSEHOLD"]
    prio = ["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"]
    modes = ["REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB"]
    instr = ["DELIVER IN PERSON", "COLLECT COD", "NONE", "TAKE BACK RETURN"]
    comments = ["carefully final deposits", "special requests sleep", "furiously pending packages",
                "Customer slyly Complaints", "quickly bold accounts", "ironic foxes nag",
                "blithely regular theodolites"]
    S, C, P, O = 10_000, 150_000, 200_000, 1_500_000
    nlist = ",".join(f"({k},'{n}',{r})" for k, (n, r) in enumerate(zip(nations, nreg)))
    out("tpch", "region", f"SELECT i::int r_regionkey, ({regions!r})[i+1] r_name, 'c' r_comment FROM range(5) t(i)".replace('"', "'"))
    out("tpch", "nation", f"SELECT k::int n_nationkey, n n_name, r::int n_regionkey, 'c' n_comment FROM (VALUES {nlist}) v(k,n,r)")
    out("tpch", "supplier", f"""SELECT (i+1)::bigint s_suppkey, printf('Supplier#%09d', i+1) s_name, 'addr' s_address,
        {rnd(0,24,1)}::int s_nationkey, printf('%d-555-0000', 10+{rnd(0,24,1)}) s_phone,
        ({rnd(-99999,999999,2)}/100.0)::decimal(15,2) s_acctbal, {pick(comments,3)} s_comment FROM range({S}) t(i)""")
    out("tpch", "customer", f"""SELECT (i+1)::bigint c_custkey, printf('Customer#%09d', i+1) c_name, 'addr' c_address,
        {rnd(0,24,1)}::int c_nationkey, printf('%d-555-0000', 10+{rnd(0,24,1)}) c_phone,
        ({rnd(-99999,999999,2)}/100.0)::decimal(15,2) c_acctbal, {pick(seg,4)} c_mktsegment, {pick(comments,3)} c_comment
        FROM range({C}) t(i)""")
    out("tpch", "part", f"""SELECT (i+1)::bigint p_partkey,
        {pick(words,1)} || ' ' || {pick(words,2)} || ' ' || {pick(words,3)} || ' ' || {pick(words,4)} || ' ' || {pick(words,5)} p_name,
        printf('Manufacturer#%d', {rnd(1,5,6)}) p_mfgr, printf('Brand#%d%d', {rnd(1,5,6)}, {rnd(1,5,7)}) p_brand,
        {pick(types1,8)} || ' ' || {pick(types2,9)} || ' ' || {pick(types3,10)} p_type, {rnd(1,50,11)}::int p_size,
        {pick(cont1,12)} || ' ' || {pick(cont2,13)} p_container, ((90000 + ((i+1)/10) % 20001 + 100 * ((i+1) % 1000))/100.0)::decimal(15,2) p_retailprice,
        'c' p_comment FROM range({P}) t(i)""")
    out("tpch", "partsupp", f"""SELECT (i//4+1)::bigint ps_partkey, (((i//4) + (i%4) * ({S}//4 + (i//4)//{S})) % {S} + 1)::bigint ps_suppkey,
        {rnd(1,9999,1)}::int ps_availqty, ({rnd(100,100000,2)}/100.0)::decimal(15,2) ps_supplycost, 'c' ps_comment
        FROM range({4*P}) t(i)""")
    out("tpch", "orders", f"""SELECT (i+1)::bigint o_orderkey, {rnd(1,C,1)} o_custkey, {pick(['F','O','P'],2)} o_orderstatus,
        ({rnd(85000,55545000,3)}/100.0)::decimal(15,2) o_totalprice, (DATE '1992-01-01' + {rnd(0,2405,4)}::int) o_orderdate,
        {pick(prio,5)} o_orderpriority, printf('Clerk#%09d', {rnd(1,1000,6)}) o_clerk, 0 o_shippriority, {pick(comments,7)} o_comment
        FROM range({O}) t(i)""")
    # lineitem: ~4 lines per order, ordered by orderkey like dbgen output
    out("tpch", "lineitem", f"""SELECT l.*, (DATE '1992-01-01' + (hash((l_orderkey - 1) * 1000003 + 4 * 7919) % 2406)::int + {rnd(1,121,30)}::int) l_shipdate,
           (DATE '1992-01-01' + (hash((l_orderkey - 1) * 1000003 + 4 * 7919) % 2406)::int + {rnd(30,90,31)}::int) l_commitdate,
           (DATE '1992-01-01' + (hash((l_orderkey - 1) * 1000003 + 4 * 7919) % 2406)::int + {rnd(1,121,30)}::int + {rnd(1,30,32)}::int) l_receiptdate,
           {pick(instr,33)} l_shipinstruct, {pick(modes,34)} l_shipmode, 'c' l_comment
        FROM (SELECT i, (i//4+1)::bigint l_orderkey, {rnd(1,P,1)} l_partkey, {rnd(1,S,2)} l_suppkey, (i%4+1)::int l_linenumber,
           {rnd(1,50,3)}::decimal(15,2) l_quantity, ({rnd(90000,10494950,5)}/100.0)::decimal(15,2) l_extendedprice,
           ({rnd(0,10,6)}/100.0)::decimal(15,2) l_discount, ({rnd(0,8,7)}/100.0)::decimal(15,2) l_tax,
           {pick(['R','A','N'],8)} l_returnflag, {pick(['O','F'],9)} l_linestatus FROM range({4*O}) t(i)) l""".replace("SELECT l.*", "SELECT * EXCLUDE (i) FROM (SELECT l.*") + ")",
        order_by=None)


# --------------------------------------------------------------------------- TPC-DS
DS_ROWS = dict(call_center=6, catalog_page=11718, catalog_returns=144067, catalog_sales=1441548,
               customer=100000, customer_address=50000, customer_demographics=1920800, date_dim=73049,
               household_demographics=7200, income_band=20, inventory=11745000, item=18000,
               promotion=300, reason=35, ship_mode=20, store=12, store_returns=287514,
               store_sales=2880404, time_dim=86400, warehouse=5, web_page=60, web_returns=71763,
               web_sales=719384, web_site=30)
FK = [("date_sk", None), ("time_sk", None), ("item_sk", "item"), ("customer_sk", "customer"),
      ("cdemo_sk", "customer_demographics"), ("hdemo_sk", "household_demographics"),
      ("addr_sk", "customer_address"), ("store_sk", "store"), ("promo_sk", "promotion"),
      ("warehouse_sk", "warehouse"), ("call_center_sk", "call_center"),
      ("catalog_page_sk", "catalog_page"), ("ship_mode_sk", "ship_mode"),
      ("web_page_sk", "web_page"), ("site_sk", "web_site"), ("reason_sk", "reason"),
      ("income_band_sk", "income_band")]
FACTS = {"store_sales", "store_returns", "catalog_sales", "catalog_returns", "web_sales",
         "web_returns", "inventory"}


def tpcds():
    # per-column domains: the literals each column is compared against in the 99 queries,
    # padded with as many filler values so every predicate is selective but non-empty
    qdir = os.path.join(W, "ddb/extension/tpcds/dsdgen/queries")
    dom = {}
    lit = r"'([^']*)'"
    for f in glob.glob(qdir + "/*.sql"):
        sql = re.sub(r"\s+", " ", open(f).read())
        for col, rest in re.findall(r"(?:substr\(|substring\()?\b([a-z]+_[a-z_]+)\b(?:\s*,\s*\d+\s*,\s*\d+\))?\s*(?:=|<>|!=|not like|like|not in|in|between)\s*(\(?\s*'[^)]*?'(?:\s*(?:,|and)\s*'[^']*')*\s*\)?)", sql, flags=re.I):
            dom.setdefault(col.lower(), set()).update(x.replace("%", "") for x in re.findall(lit, rest))
    for f in sorted(glob.glob(os.path.join(W, "ddb/extension/tpcds/dsdgen/schema/*.sql"))):
        con.sql(open(f).read().replace("not null", ""))
    for t in sorted(DS_ROWS):
        n = DS_ROWS[t]
        cols = con.sql(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name='{t}' ORDER BY ordinal_position").fetchall()
        exprs = []
        for ci, (c, ty) in enumerate(cols):
            seed = ci + 100
            ty = ty.upper()
            if t == "date_dim":
                e = {"d_date_sk": "2415022 + i", "d_date": "DATE '1900-01-02' + i::int",
                     "d_month_seq": "(year(DATE '1900-01-02' + i::int)-1900)*12 + month(DATE '1900-01-02' + i::int) - 1",
                     "d_week_seq": "i//7 + 1", "d_quarter_seq": "(year(DATE '1900-01-02' + i::int)-1900)*4 + quarter(DATE '1900-01-02' + i::int)",
                     "d_year": "year(DATE '1900-01-02' + i::int)", "d_fy_year": "year(DATE '1900-01-02' + i::int)",
                     "d_dow": "dayofweek(DATE '1900-01-02' + i::int)", "d_moy": "month(DATE '1900-01-02' + i::int)",
                     "d_dom": "day(DATE '1900-01-02' + i::int)", "d_qoy": "quarter(DATE '1900-01-02' + i::int)",
                     "d_day_name": "dayname(DATE '1900-01-02' + i::int)",
                     "d_quarter_name": "year(DATE '1900-01-02' + i::int)::varchar || 'Q' || quarter(DATE '1900-01-02' + i::int)::varchar"}.get(c)
            elif t == "time_dim":
                e = {"t_time_sk": "i", "t_time": "i", "t_hour": "i//3600", "t_minute": "(i//60)%60",
                     "t_second": "i%60", "t_am_pm": "CASE WHEN i < 43200 THEN 'AM' ELSE 'PM' END"}.get(c)
            else:
                e = None
            if e is None:
                if ci == 0 and c.endswith("_sk") and t not in FACTS:
                    e = "i + 1"
                elif c.endswith("_sk"):
                    tgt = next((tt for suf, tt in FK if c.endswith(suf)), "?")
                    if c.endswith("date_sk"):
                        e = rnd(2450816, 2452642, seed)  # 1998-01-01 .. 2002-12-31
                    elif c.endswith("time_sk"):
                        e = rnd(0, 86399, seed)
                    elif tgt in DS_ROWS:
                        e = rnd(1, DS_ROWS[tgt], seed)
                    else:
                        e = rnd(1, 100, seed)
                elif c.endswith("ticket_number") or c.endswith("order_number"):
                    e = "i // 10 + 1" if t in ("store_sales", "catalog_sales", "web_sales") else rnd(1, n, seed)
                elif ty.startswith("VARCHAR") and c.endswith("_id"):
                    e = "printf('AAAAAAAA%08d', i + 1)"
                elif ty.startswith("VARCHAR"):
                    suf = c.split("_", 1)[1]
                    d = sorted(set().union(*[v for k, v in dom.items() if k.split("_", 1)[-1] == suf]))
                    # tiny dimension tables: no filler values, otherwise filtered builds come out empty
                    d = d + [f"{c}_{k}" for k in range(0 if n <= 100 and d else max(2, len(d)))]
                    e = pick(d, seed)
                elif ty == "DATE":
                    e = f"DATE '1998-01-01' + {rnd(0, 1825, seed)}::int"
                elif "gmt_offset" in c:
                    e = rnd(-6, -5, seed)
                elif ty.startswith("DECIMAL"):
                    e = f"({rnd(0, 20000, seed)} / 100.0)"
                elif "manufact_id" in c or "manager_id" in c:
                    e = rnd(1, 1000, seed)
                elif "gmt_offset" in c:
                    e = rnd(-10, -5, seed)
                else:
                    e = rnd(0, 100, seed)
            exprs.append(f"CAST({e} AS {ty}) AS {c}")
        out("tpcds", t, f"SELECT {', '.join(exprs)} FROM range({n}) t(i)")


# --------------------------------------------------------------------------- ClickBench
def clickbench(n=10_000_000):
    con.sql(open(os.path.join(W, "cb/duckdb/create.sql")).read().replace("hits", "hits_schema"))
    lits = set(re.findall(r"'([^']*)'", open(os.path.join(W, "cb/duckdb-parquet/queries.sql")).read()))
    pool = sorted(l.replace("%", "") for l in lits if not re.fullmatch(r"[\d\-]+", l))
    urls = [f"http://{p}example{k}.com/page" for p in ["", "www.google.", "google.", "yandex."] for k in range(5)]
    strpool = pool + urls + [""] * 20
    cols = con.sql("SELECT column_name, data_type FROM information_schema.columns WHERE table_name='hits_schema' ORDER BY ordinal_position").fetchall()
    exprs = []
    for ci, (c, ty) in enumerate(cols):
        seed = ci + 500
        if c == "EventTime":
            e = f"(1372636800 + {rnd(0, 31*86400-1, seed)})::BIGINT"  # epoch seconds, July 2013
        elif c == "EventDate":
            e = f"(15887 + {rnd(0, 30, seed)})::USMALLINT"  # days since epoch (2013-07-01 = 15887)
        elif c == "CounterID":
            e = f"([62, 1, 2, 3, 4, 5, 6, 7])[1 + (hash(i * 1000003 + {seed} * 7919) % 8)::int]::INTEGER"
        elif c in ("UserID", "WatchID", "URLHash", "RefererHash", "FUniqID"):
            e = f"({rnd(0, 2**40, seed)})::BIGINT"
        elif c == "ClientIP":
            e = f"({rnd(-2**31, 2**31-1, seed)})::INTEGER"
        elif ty == "VARCHAR":
            e = pick(strpool, seed)
        elif ty in ("SMALLINT", "INTEGER", "BIGINT"):
            e = f"({rnd(0, 20, seed)})::{ty}"
        else:
            e = f"NULL::{ty}"
        exprs.append(f"{e} AS {c}")
    # real hits.parquet is sorted by the ClickHouse primary key
    out("clickbench", "hits", f"SELECT {', '.join(exprs)} FROM range({n}) t(i)",
        order_by="CounterID, EventDate, UserID, EventTime, WatchID")


if __name__ == "__main__":
    for b in sys.argv[1:]:
        globals()[b]()
