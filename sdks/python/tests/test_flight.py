"""Flight SQL queries and ingest through the `flight` extra (plan M1.6 Task 4 rules 1-8)."""

from __future__ import annotations

import subprocess
import sys
import time
import uuid
from collections.abc import Iterator

import pytest


def test_importing_operon_does_not_import_pyarrow() -> None:
    code = (
        "import operon, sys; "
        "assert not {'pyarrow', 'polars', 'adbc_driver_manager'} & set(sys.modules)"
    )
    subprocess.run([sys.executable, "-c", code], check=True)


pytest.importorskip("adbc_driver_flightsql")

import adbc_driver_flightsql.dbapi as flightsql_dbapi  # noqa: E402
import pyarrow as pa  # noqa: E402
from conftest import kb_schema  # noqa: E402

import operon  # noqa: E402
from operon import Document, Schema, q, schema  # noqa: E402
from operon.flight import FlightSqlClient  # noqa: E402


@pytest.fixture
def flight(flight_uri: str, ns_name: str) -> Iterator[FlightSqlClient]:
    """A Flight SQL client of the fresh namespace."""
    with FlightSqlClient(flight_uri, namespace=ns_name) as client:
        yield client


def _count(flight: FlightSqlClient, table: str, **kwargs: object) -> int:
    result = flight.sql(f"SELECT count(*) AS n FROM {table}", **kwargs)  # type: ignore[arg-type]
    value = result.column("n")[0].as_py()
    assert isinstance(value, int)
    return value


def test_the_timeout_bounds_query_fetch_and_update_calls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: dict[str, object] = {}

    def connect(uri: str, **kwargs: object) -> object:
        seen.update(kwargs)
        return object()

    monkeypatch.setattr(flightsql_dbapi, "connect", connect)
    FlightSqlClient("grpc://127.0.0.1:1", namespace="n", timeout=7.5)
    db_kwargs = seen["db_kwargs"]
    assert isinstance(db_kwargs, dict)
    for kind in ("query", "fetch", "update"):
        assert db_kwargs[f"adbc.flight.sql.rpc.timeout_seconds.{kind}"] == "7.5"


@pytest.mark.flight
def test_flight_sql_returns_an_arrow_table(kb: operon.Collection, flight: FlightSqlClient) -> None:
    table = flight.sql("SELECT count(*) AS n FROM kb")
    assert isinstance(table, pa.Table)
    assert table.num_rows == 1
    assert table.column("n")[0].as_py() == 3


@pytest.mark.flight
def test_flight_sql_reads_at_least_a_token(kb: operon.Collection, flight: FlightSqlClient) -> None:
    result = kb.upsert([Document(4, {"body": "new", "tenant": "c", "n": 4})])
    assert _count(flight, "kb", consistency=result.token) == 4
    assert _count(flight, "kb", consistency=str(result.token)) == 4
    # The token header is removed after the statement: a strong read still answers.
    assert _count(flight, "kb") == 4


@pytest.mark.flight
def test_flight_sql_batches_stream_record_batches(
    kb: operon.Collection, flight: FlightSqlClient
) -> None:
    reader = flight.sql_batches("SELECT * FROM kb")
    assert isinstance(reader, pa.RecordBatchReader)
    assert sum(batch.num_rows for batch in reader) == 3


@pytest.mark.flight
def test_flight_errors_map_to_operon_errors(kb: operon.Collection, flight: FlightSqlClient) -> None:
    with pytest.raises(operon.InvalidArgumentError) as caught:
        flight.sql("SELEC 1")
    assert caught.value.__cause__ is not None
    with pytest.raises(operon.InvalidArgumentError):
        flight.sql_batches("SELEC 1")
    # The connection still works after an error.
    assert _count(flight, "kb") == 3


@pytest.mark.flight
def test_flight_namespace_header_selects_the_namespace(
    kb: operon.Collection, client: operon.Client, flight_uri: str, flight: FlightSqlClient
) -> None:
    name = "t2-" + uuid.uuid4().hex[:12]
    client.create_namespace(name)
    ns2 = client.namespace(name)
    ns2.create_collection("kb", kb_schema(), partitions=1)
    ns2.collection("kb").upsert([Document(1, {"body": "only", "tenant": "a", "n": 1})])
    with FlightSqlClient(flight_uri, namespace=ns2.name) as second:
        assert _count(second, "kb") == 1
    assert _count(flight, "kb") == 3


def _kb2_schema() -> Schema:
    return Schema(fields=[schema.text("title")], vectors=[schema.vector("v", 3)], dynamic="ignore")


@pytest.mark.flight
def test_flight_ingest_loads_a_collection(ns: operon.Namespace, flight: FlightSqlClient) -> None:
    ns.create_collection("kb2", _kb2_schema(), partitions=1)
    table = pa.table(
        {
            "_id": pa.array([1, 2, 3], type=pa.int64()),
            "title": ["red apple", "green pear", "blue plum"],
            "v": pa.array(
                [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 1.0]],
                type=pa.list_(pa.float32(), 3),
            ),
        }
    )
    assert flight.ingest("kb2", table) == 3
    assert _count(flight, "kb2") == 3
    found = (
        ns.collection("kb2")
        .search()
        .retrieve(q.vector("v", [0, 1, 0], k=3), q.text(q.match("title", "pear"), k=3))
        .limit(1)
        .execute()
    )
    assert [h.id for h in found.hits] == [2]


@pytest.mark.flight
def test_flight_ingest_errors_map_to_operon_errors(
    ns: operon.Namespace, flight: FlightSqlClient
) -> None:
    ns.create_collection("kb2", _kb2_schema(), partitions=1)
    without_id = pa.table({"title": ["x"]})
    with pytest.raises(operon.InvalidArgumentError) as caught:
        flight.ingest("kb2", without_id)
    assert caught.value.__cause__ is not None
    with pytest.raises(operon.NotFoundError):
        flight.ingest("nope", pa.table({"_id": [1], "title": ["x"]}))
    with pytest.raises(ValueError, match="id_type"):
        flight.ingest("kb2", pa.table({"_id": [1]}), id_type="i64")  # type: ignore[arg-type]


@pytest.mark.flight
def test_flight_ingest_stream_appends_records(
    ns: operon.Namespace, flight: FlightSqlClient
) -> None:
    ns.create_stream("events", 1)
    table = pa.table(
        {
            "key": pa.array([b"k1", b"k2"], type=pa.binary()),
            "value": pa.array([b"v1", b"v2"], type=pa.binary()),
        }
    )
    assert flight.ingest_stream("events", table) == 2
    fetched = ns.fetch("events", 0, 0)
    assert [(r.key, r.value) for r in fetched.records] == [(b"k1", b"v1"), (b"k2", b"v2")]


def _settled(collection: operon.Collection) -> operon.ScanPlan:
    deadline = time.monotonic() + 30
    while True:
        plan = collection.scan_plan()
        if not plan.tail and plan.lance is not None:
            return plan
        if time.monotonic() > deadline:
            pytest.fail(f"the scan plan still has a tail after 30 s: {plan.raw}")
        time.sleep(0.1)


@pytest.mark.flight
def test_flight_sql_reads_a_pin(kb: operon.Collection, flight: FlightSqlClient) -> None:
    plan = _settled(kb)
    assert _count(flight, "kb", consistency=plan.pin) == plan.live_rows == 3
    kb.upsert([Document(9, {"body": "later", "tenant": "z", "n": 9})])
    assert _count(flight, "kb", consistency=plan.pin) == 3
    assert _count(flight, "kb") == 4
    with pytest.raises(ValueError, match="strong, at-least-token or pinned"):
        flight.sql("SELECT 1", consistency="eventual")
    with pytest.raises(ValueError, match="consistency token"):
        flight.sql("SELECT 1", consistency="v1:nonsense")


@pytest.mark.flight
def test_search_to_arrow_round_trips_through_ingest(
    kb: operon.Collection, ns: operon.Namespace, flight: FlightSqlClient
) -> None:
    response = (
        kb.search()
        .retrieve(q.vector("embedding", [1, 0, 0], k=10))
        .select(operon.Projection(vectors=["embedding"]))
        .limit(10)
        .execute()
    )
    table = response.to_arrow()
    ns.create_collection("kb3", kb_schema(), partitions=2)
    assert flight.ingest("kb3", table, id_type="u64") == 3
    kb3 = ns.collection("kb3")
    ids = [1, 2, 3]
    select = operon.Projection(vectors=["embedding"])
    originals = kb.get(ids, select=select)
    copies = kb3.get(ids, select=select)
    assert [d.source if d else None for d in copies] == [d.source if d else None for d in originals]
    assert [d.vectors if d else None for d in copies] == [
        d.vectors if d else None for d in originals
    ]
