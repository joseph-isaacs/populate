from datetime import date

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

import populate as p


def sales(rows=10_000, seed=1):
    return p.table(
        rows,
        {
            "id": p.range(0),
            "store": p.choose(["north", "south"], weights=[3, 1]),
            "sold": p.uniform(date(1992, 1, 1), date(1998, 12, 31)),
            "price": p.uniform(1.0, 100.0),
            "note": p.chars("abc", 0, 4).nullable(0.5),
            "tags": p.list(p.uniform(0, 3), p.range(0)),
        },
        seed=seed,
        chunk_rows=3_000,
        batch_rows=1_000,
    )


def test_schema_comes_from_the_distributions():
    s = pa.schema(sales().schema)
    assert s.names == ["id", "store", "sold", "price", "note", "tags"]
    assert s.field("id").type == pa.int64() and not s.field("id").nullable
    assert s.field("sold").type == pa.date32()
    assert s.field("note").nullable
    assert s.field("tags").type.value_type == pa.int64()


def test_rows_are_the_same_on_any_thread_count():
    one = pa.table(sales().with_threads(1))
    assert one.num_rows == 10_000
    assert one.equals(pa.table(sales().with_threads(4)))
    assert one.column("id").to_pylist() == list(range(10_000))
    assert not one.equals(pa.table(sales(seed=2)))
    lo, hi = pc.min_max(one.column("sold")).values()
    assert date(1992, 1, 1) <= lo.as_py() and hi.as_py() <= date(1998, 12, 31)
    assert 0 < one.column("note").null_count < 10_000


def test_parquet_matches_the_stream(tmp_path):
    path = tmp_path / "sales.parquet"
    sales().write_parquet(path)
    assert pq.read_metadata(path).num_row_groups == 4
    assert pq.read_table(path).equals(pa.table(sales()))


def test_a_reader_can_stop_early():
    r = pa.RecordBatchReader.from_stream(sales(rows=1_000_000))
    assert r.read_next_batch().num_rows == 1_000
    r.close()


def test_collect_is_an_arrow_array():
    a = pa.array(p.constant("x").collect(3))
    assert a.to_pylist() == ["x", "x", "x"]
    assert p.normal().type == "float64"


def test_bad_arguments_raise():
    with pytest.raises(ValueError):
        p.uniform(5, 1)
    with pytest.raises(TypeError):
        p.list(p.normal(), p.range(0))
    with pytest.raises(TypeError):
        p.range(0).nullable(0.1).nullable(0.1)


def test_tpch():
    nation = pa.table(p.tpch("nation", sf=0.01))
    assert nation.num_rows == 25
    assert nation.column(1)[0].as_py() == "ALGERIA"
    assert pa.table(p.tpch("lineitem", sf=0.01)).num_rows == 60_175
    with pytest.raises(ValueError):
        p.tpch("nope")
