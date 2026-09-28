"""Arrow and Polars results (plan M1.6 Task 4 rules 9-12, Ruling 17)."""

from __future__ import annotations

import base64
import datetime
import sys
import uuid
from typing import Any

import httpx
import pytest

pa = pytest.importorskip("pyarrow")

import operon  # noqa: E402
from operon import q  # noqa: E402

pytestmark = pytest.mark.arrow

TOKEN = "v1:s1/p0@7"
UUID = "0190f5c4-7a3e-7c1d-9b2a-5e4f3d2c1b0a"


def _hit(pk: object, score: float, **extra: object) -> dict[str, Any]:
    return {"pk": pk, "score": score, "sort_values": [score, pk], **extra}


def _search(hits: list[dict[str, Any]]) -> operon.SearchResponse:
    body = {"hits": hits, "total": None, "aggregations": None, "groups": None, "read_token": TOKEN}

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=body)

    with operon.Client("http://operon.test", transport=httpx.MockTransport(handler)) as client:
        return client.namespace("n").search("c").retrieve(q.vector("e", [1, 0, 0], k=10)).execute()


def _sql(columns: list[tuple[str, str]], rows: list[list[Any]]) -> operon.SqlResult:
    body = {
        "columns": [{"name": n, "type": t} for n, t in columns],
        "rows": rows,
        "truncated": False,
    }

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=body)

    with operon.Client("http://operon.test", transport=httpx.MockTransport(handler)) as client:
        return client.namespace("n").sql("SELECT 1")


def _documented() -> operon.SearchResponse:
    return _search(
        [
            _hit(1, 0.9, source={"t": "é"}, vectors={"e": [1.0, 0.0, 0.0]}),
            _hit(
                "k-str",
                0.5,
                source=None,
                vectors={},
                sparse_vectors={"s": {"indices": [3, 1], "values": [0.5, 0.25]}},
            ),
            _hit({"uuid": UUID}, 0.25, source={"t": "x"}, vectors={"e": [0.0, 1.0, 0.0]}),
        ]
    )


def test_search_response_to_arrow_has_the_documented_schema() -> None:
    table = _documented().to_arrow()
    sparse = pa.struct(
        [pa.field("indices", pa.list_(pa.uint32())), pa.field("values", pa.list_(pa.float32()))]
    )
    assert table.schema.names == ["_id", "_score", "_source", "e", "s"]
    assert table.schema.field("_id").type == pa.string()
    assert table.schema.field("_score").type == pa.float32()
    assert table.schema.field("_source").type == pa.string()
    assert table.schema.field("e").type == pa.list_(pa.float32(), 3)
    assert table.schema.field("s").type == sparse
    assert table.schema.metadata == {b"operon.read_token": TOKEN.encode()}
    assert table.column("_id").to_pylist() == ["1", "k-str", UUID]
    assert table.column("_source").to_pylist() == ['{"t":"é"}', None, '{"t":"x"}']
    assert table.column("e").to_pylist() == [[1.0, 0.0, 0.0], None, [0.0, 1.0, 0.0]]
    assert table.column("s").to_pylist() == [
        None,
        {"indices": [3, 1], "values": [0.5, 0.25]},
        None,
    ]


def test_source_columns_mode() -> None:
    response = _search([_hit(1, 1.0, source={"a": 1, "b": "x"}), _hit(2, 0.5, source={"a": 2})])
    table = response.to_arrow(source="columns")
    assert table.schema.names == ["_id", "_score", "a", "b"]
    assert table.schema.field("a").type == pa.int64()
    assert table.schema.field("b").type == pa.string()
    assert table.column("b").to_pylist() == ["x", None]
    with pytest.raises(ValueError, match="source"):
        response.to_arrow(source="rows")  # type: ignore[arg-type]


def test_vectors_of_mixed_length_become_lists() -> None:
    response = _search(
        [_hit(1, 1.0, vectors={"e": [1.0, 2.0]}), _hit(2, 0.5, vectors={"e": [1.0, 2.0, 3.0]})]
    )
    table = response.to_arrow()
    assert table.schema.field("e").type == pa.list_(pa.float32())
    assert table.column("e").to_pylist() == [[1.0, 2.0], [1.0, 2.0, 3.0]]


def test_an_empty_search_is_an_empty_table() -> None:
    table = _search([]).to_arrow()
    assert table.num_rows == 0
    assert table.schema.names == ["_id", "_score", "_source"]


def test_sql_result_to_arrow_maps_column_types() -> None:
    raw = b"\x00\xffbytes"
    encoded = base64.b64encode(raw).decode()
    columns = [
        ("b", "Boolean"),
        ("i8", "Int8"),
        ("i16", "Int16"),
        ("i32", "Int32"),
        ("i64", "Int64"),
        ("u8", "UInt8"),
        ("u16", "UInt16"),
        ("u32", "UInt32"),
        ("u64", "UInt64"),
        ("f32", "Float32"),
        ("f64", "Float64"),
        ("s", "Utf8"),
        ("sv", "Utf8View"),
        ("ls", "LargeUtf8"),
        ("bin", "Binary"),
        ("lbin", "LargeBinary"),
        ("bv", "BinaryView"),
        ("d32", "Date32"),
        ("d64", "Date64"),
        ("ts_us_utc", 'Timestamp(µs, "UTC")'),
        ("ts_ns", "Timestamp(ns)"),
        ("ts_s_zone", 'Timestamp(s, "+05:30")'),
        ("ts_ms", "Timestamp(ms)"),
        ("fsl", "FixedSizeList(3 x Float32)"),
        ("dec", "Decimal128(10, 2)"),
    ]
    row = [
        True,
        -8,
        -16,
        -32,
        -(2**63),
        8,
        16,
        32,
        2**64 - 1,
        1.5,
        None,
        "s",
        "sv",
        "ls",
        encoded,
        encoded,
        encoded,
        "2024-01-02",
        "2024-01-03",
        "2024-01-02T03:04:05.123456Z",
        "2024-01-02T03:04:05.123456789Z",
        "2024-01-02T03:04:05Z",
        "2024-01-02T03:04:05.123Z",
        [1.0, 2.0, 3.0],
        "12.34",
    ]
    table = _sql(columns, [row, [None] * len(row)]).to_arrow()
    expected = [
        pa.bool_(),
        pa.int8(),
        pa.int16(),
        pa.int32(),
        pa.int64(),
        pa.uint8(),
        pa.uint16(),
        pa.uint32(),
        pa.uint64(),
        pa.float32(),
        pa.float64(),
        pa.string(),
        pa.string(),
        pa.large_string(),
        pa.binary(),
        pa.large_binary(),
        pa.binary(),
        pa.date32(),
        pa.date32(),
        pa.timestamp("us", "UTC"),
        pa.timestamp("ns"),
        pa.timestamp("s", "+05:30"),
        pa.timestamp("ms"),
        pa.list_(pa.float32(), 3),
        pa.string(),
    ]
    assert table.schema.names == [n for n, _ in columns]
    assert [f.type for f in table.schema] == expected
    # Nanoseconds do not fit a datetime: ts_ns is checked through its int64 value.
    first = {n: table.column(n)[0].as_py() for n in table.schema.names if n != "ts_ns"}
    assert first["u64"] == 2**64 - 1
    assert first["i64"] == -(2**63)
    assert first["bin"] == raw
    assert first["lbin"] == raw
    assert first["bv"] == raw
    assert first["d32"] == datetime.date(2024, 1, 2)
    assert first["d64"] == datetime.date(2024, 1, 3)
    utc = datetime.timezone.utc
    assert first["ts_us_utc"] == datetime.datetime(2024, 1, 2, 3, 4, 5, 123456, tzinfo=utc)
    assert table.column("ts_ns").cast(pa.int64())[0].as_py() == 1704164645123456789
    zoned = first["ts_s_zone"]
    assert zoned == datetime.datetime(2024, 1, 2, 3, 4, 5, tzinfo=utc)
    assert zoned.utcoffset() == datetime.timedelta(hours=5, minutes=30)
    assert first["ts_ms"] == datetime.datetime(2024, 1, 2, 3, 4, 5, 123000)
    assert first["fsl"] == [1.0, 2.0, 3.0]
    assert first["dec"] == "12.34"
    assert all(table.column(name)[1].as_py() is None for name in table.schema.names)


def test_other_sql_types_fall_back_to_inference() -> None:
    table = _sql([("l", "List(Int64)")], [[[1, 2]], [[3]]]).to_arrow()
    assert table.schema.field("l").type == pa.list_(pa.int64())
    assert table.column("l").to_pylist() == [[1, 2], [3]]


def test_results_export_the_arrow_c_stream() -> None:
    response = _documented()
    sql = _sql([("n", "Int64"), ("s", "Utf8")], [[1, "a"], [2, None]])
    for result in (response, sql):
        expected = result.to_arrow()
        assert pa.table(result).equals(expected)
        assert pa.RecordBatchReader.from_stream(result).read_all().equals(expected)


def test_to_polars_matches_the_arrow_table() -> None:
    polars = pytest.importorskip("polars")
    response = _documented()
    sql = _sql([("n", "Int64"), ("f", "Float64")], [[1, 0.5], [2, None]])
    for result in (response, sql):
        expected = polars.from_arrow(result.to_arrow())
        assert result.to_polars().equals(expected)
        assert polars.DataFrame(result).equals(expected)


def test_missing_extras_raise_helpful_import_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    response = _documented()
    sql = _sql([("n", "Int64")], [[1]])
    with monkeypatch.context() as m:
        m.setitem(sys.modules, "pyarrow", None)
        m.delitem(sys.modules, "operon.arrow", raising=False)
        for result in (response, sql):
            with pytest.raises(ImportError, match=r"operon-client\[arrow\]"):
                result.to_arrow()
    with monkeypatch.context() as m:
        m.setitem(sys.modules, "polars", None)
        for result in (response, sql):
            with pytest.raises(ImportError, match=r"operon-client\[polars\]"):
                result.to_polars()


def test_to_arrow_against_the_server(kb: operon.Collection, ns: operon.Namespace) -> None:
    response = (
        kb.search()
        .retrieve(q.vector("embedding", [1, 0, 0], k=10), q.text(q.match("body", "refund"), k=10))
        .limit(3)
        .execute()
    )
    table = response.to_arrow()
    assert table.column("_id").to_pylist() == ["1", "3", "2"]
    scores = table.column("_score").to_pylist()
    assert scores == sorted(scores, reverse=True)
    counted = ns.sql("SELECT count(*) AS n FROM kb").to_arrow()
    assert counted.schema.field("n").type == pa.int64()
    assert counted.column("n").to_pylist() == [3]


def test_uuid_ids_use_the_hyphenated_form() -> None:
    table = _search([_hit({"uuid": UUID.upper()}, 1.0)]).to_arrow()
    assert table.column("_id").to_pylist() == [str(uuid.UUID(UUID))]
