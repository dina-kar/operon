"""The SDK sends exactly the requests of `sdks/fixtures/` (plan M1.6 Task 3, rows T1-2 and T1-3)."""

from __future__ import annotations

import datetime as dt
import json
import uuid
from collections.abc import Callable
from pathlib import Path

import httpx
import pytest

import operon
from operon import Document, Projection, Record, Schema, SparseVector, _wire, q, schema
from operon._wire import Json

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures"
NS = "wire"
UUID = uuid.UUID("0190f5c4-6c1e-7b3a-9d2e-4f5a6b7c8d9e")
U64_MAX = 2**64 - 1

_INFO = {
    "id": 11,
    "name": "kb",
    "schema": {
        "fields": [],
        "vectors": [],
        "sparse_vectors": [],
        "dynamic": "ignore",
        "max_fields": 1000,
        "version": 1,
    },
    "partitions": 2,
    "live_doc_count": 0,
}
_SEARCH: dict[str, Json] = {"hits": [], "total": None, "aggregations": None, "groups": None}
_SCAN = {
    "namespace": NS,
    "collection": "sc",
    "collection_id": 13,
    "manifest_version": 0,
    "schema_version": 1,
    "lance": None,
    "fragments": [],
    "live_rows": 0,
    "columns": [],
    "pk_encoding": "operon_canonical_v1",
    "tail": False,
    "tail_records": 0,
    "offsets": [{"partition": 0, "applied": 0, "target": 0}],
    "durable_token": "v1:s9/p0@0",
    "pin": {"manifest_version": 0, "token": "v1:s9/p0@0"},
    "planned_at_ms": 0,
    "expires_at_ms": None,
}


def _written(n: int) -> dict[str, Json]:
    return {"results": ["accepted"] * n, "positions": [None] * n}


# Per SDK-producible step: status and body of the canned answer. Every answer
# also carries a distinct token (header, and `token`/`read_token` where the
# real server has one), so `{token:<step>}` can be checked.
CANNED: dict[str, tuple[int, dict[str, Json]]] = {
    "create_namespace": (201, {"id": 1}),
    "create_stream": (201, {"id": 7}),
    "produce": (200, {"base_offset": 0, "last_offset": 0, "token": []}),
    "fetch": (200, {"records": [], "next_offset": 0, "high_watermark": 0, "log_start_offset": 0}),
    "create_collection": (201, _INFO),
    "list_collections": (200, {"collections": [_INFO]}),
    "get_collection": (200, _INFO),
    "write_docs": (200, _written(6)),
    "get_docs": (200, {"documents": [None] * 5}),
    "query_hybrid": (200, _SEARCH),
    "query_filter_only": (200, _SEARCH),
    "patch": (200, _written(1)),
    "delete": (200, _written(1)),
    "get_after_changes": (200, {"documents": [None, None]}),
    "sql_count": (200, {"columns": [{"name": "n", "type": "Int64"}], "rows": [[5]]}),
    "drop_collection": (200, {"dropped": True}),
    "create_sparse_collection": (201, _INFO),
    "write_sparse_docs": (200, _written(3)),
    "query_sparse": (200, _SEARCH),
    "query_sparse_hybrid": (200, _SEARCH),
    "get_sparse_docs": (200, {"documents": [None]}),
    "drop_sparse_collection": (200, {"dropped": True}),
    "create_scan_collection": (201, _INFO),
    "scan_plan_fresh": (200, _SCAN),
    "drop_scan_collection": (200, {"dropped": True}),
}
_TOKEN_KEY = {"write_docs", "patch", "delete", "write_sparse_docs"}
_READ_TOKEN_KEY = {
    "get_docs",
    "get_after_changes",
    "query_hybrid",
    "query_filter_only",
    "query_sparse",
    "query_sparse_hybrid",
    "get_sparse_docs",
}


def _token(index: int) -> str:
    return f"v1:s1/p0@{index + 100}"


def _steps() -> dict[str, dict[str, Json]]:
    data = json.loads((FIXTURES / "scenario.json").read_text())
    return {step["name"]: step for step in data["steps"]}


def _replace_ns(value: Json) -> Json:
    if isinstance(value, str):
        return value.replace("{ns}", NS)
    if isinstance(value, list):
        return [_replace_ns(v) for v in value]
    if isinstance(value, dict):
        return {k: _replace_ns(v) for k, v in value.items()}
    return value


class Recorder:
    """Answers each request with the next step's canned reply, and records it."""

    def __init__(self, names: list[str]) -> None:
        self.names = names
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        index = len(self.requests)
        self.requests.append(request)
        name = self.names[index]
        status, body = CANNED[name]
        body = dict(body)
        if name in _TOKEN_KEY:
            body["token"] = _token(index)
        if name in _READ_TOKEN_KEY:
            body["read_token"] = _token(index)
        return httpx.Response(status, json=body, headers={_wire.TOKEN_HEADER: _token(index)})


def _run_scenario(client: operon.Client) -> None:
    """Every SDK-producible step, in order, through the public API."""
    client.create_namespace(NS)
    ns = client.namespace(NS)
    ns.create_stream("events", 2)
    ns.produce(
        "events",
        0,
        [Record(key=b"k", value="v", headers=[("h", b"x")], timestamp_ms=1700000000000)],
    )
    ns.fetch("events", 0, 0, max_bytes=1048576, max_wait_ms=0)
    kb_schema = Schema(
        fields=[schema.text("body"), schema.keyword("tenant"), schema.i64("n")],
        vectors=[schema.vector("embedding", 3)],
        dynamic="ignore",
    )
    ns.create_collection("kb", kb_schema, partitions=2)
    ns.list_collections()
    ns.get_collection("kb")
    kb = ns.collection("kb")
    written = kb.upsert(
        [
            Document(
                1, {"body": "refund policy", "tenant": "a", "n": 1}, {"embedding": [1.0, 0, 0]}
            ),
            Document(
                2, {"body": "shipping times", "tenant": "a", "n": 2}, {"embedding": [0.9, 0.1, 0]}
            ),
            Document(
                3, {"body": "refund window", "tenant": "b", "n": 3}, {"embedding": [0, 0, 1.0]}
            ),
            Document(U64_MAX, {"tenant": "c"}),
            Document("k-str", {"tenant": "c"}),
            Document(UUID, {"tenant": "c"}),
        ]
    )
    kb.get([1, U64_MAX, "k-str", UUID, 999], consistency=written.token)
    (
        ns.search("kb")
        .retrieve(q.vector("embedding", [1.0, 0.0, 0.0], k=10))
        .retrieve(q.text(q.match("body", "refund"), k=10))
        .limit(3)
        .execute()
    )
    kb.search().filter(q.term("tenant", "b")).execute()
    kb.patch(1, {"meta": {"x": 1}}, delete_keys=["tenant"])
    deleted = kb.delete([2])
    kb.get([1, 2], consistency=deleted.token)
    ns.sql("SELECT count(*) AS n FROM kb")
    ns.drop_collection("kb")

    sp_schema = Schema(
        vectors=[schema.vector("e", 2)],
        sparse_vectors=[schema.sparse_vector("s")],
        dynamic="ignore",
    )
    ns.create_collection("sp", sp_schema)
    sp = ns.collection("sp")
    sparse_written = sp.upsert(
        [
            Document(1, {}, {"e": [1.0, 0.0]}, {"s": SparseVector([5, 1], [2.0, 1.0])}),
            Document(2, {}, {"e": [0.0, 1.0]}, {"s": SparseVector([5], [0.5])}),
            Document(3, {}, {"e": [0.8, 0.6]}, {"s": SparseVector([7], [3.0])}),
        ]
    )
    sparse = q.sparse("s", [5], [1.0], k=10)
    sp.search().retrieve(sparse).execute()
    sp.search().retrieve(sparse, q.vector("e", [1.0, 0.0], k=10)).limit(3).execute()
    sp.get(
        [1],
        select=Projection(source="none", vectors=["s"]),
        consistency=sparse_written.token,
    )
    ns.drop_collection("sp")

    ns.create_collection(
        "sc", Schema(vectors=[schema.vector("e", 2)], dynamic="ignore"), partitions=1
    )
    ns.collection("sc").scan_plan()
    ns.drop_collection("sc")


def test_sdk_requests_equal_the_wire_fixtures() -> None:
    steps = _steps()
    names = list(CANNED)
    recorder = Recorder(names)
    with operon.Client("http://operon.test", transport=httpx.MockTransport(recorder)) as client:
        _run_scenario(client)
    assert len(recorder.requests) == len(names)
    index_of = {name: i for i, name in enumerate(names)}
    for name, request in zip(names, recorder.requests, strict=True):
        step = steps[name]
        assert request.method == step["method"], name
        assert request.url.raw_path.decode("ascii") == _replace_ns(step["path"]), name
        expected_token = None
        for key, value in step["headers"].items():
            assert key.lower() == "operon-consistency-token", name
            source = value.removeprefix("{token:").removesuffix("}")
            expected_token = _token(index_of[source])
        assert request.headers.get(_wire.TOKEN_HEADER) == expected_token, name
        body = json.loads(request.content) if request.content else None
        assert body == _replace_ns(step["body"]), name


# One builder expression per entry of queries.json, in the file's order.
BUILDERS: dict[str, Callable[[], q.Query]] = {
    "match_all": q.match_all,
    "match_none": q.match_none,
    "match": lambda: q.match("title", "hello world"),
    "match_phrase": lambda: q.match_phrase("title", "hello world"),
    "multi_match": lambda: q.multi_match([("title", 2.0), ("tag", 1.0)], "hello"),
    "term": lambda: q.term("tag", "a"),
    "terms": lambda: q.terms("n", [1, 2]),
    "range": lambda: q.range_("n", gte=1, lt=10),
    "exists": lambda: q.exists("meta"),
    "is_null": lambda: q.is_null("meta.k"),
    "is_empty": lambda: q.is_empty("tag"),
    "values_count": lambda: q.values_count("meta.k", gte=1),
    "prefix": lambda: q.prefix("tag", "a"),
    "wildcard": lambda: q.wildcard("tag", "a*"),
    "fuzzy": lambda: q.fuzzy("title", "helo", fuzziness=1),
    "ids": lambda: q.ids(1, "k-str", UUID, U64_MAX),
    "query_string": lambda: q.query_string("title:hello AND tag:a"),
    "bool": lambda: q.bool_(
        must=[q.match("title", "hello")],
        should=[q.term("flag", True)],
        must_not=[q.term("tag", "z")],
        filter=[q.range_("n", gte=1)],
    ),
    "boost": lambda: q.boost(q.term("tag", "a"), 2.0),
    "constant_score": lambda: q.constant_score(q.term("tag", "a"), 1.5),
    "match_fuzzy_auto": lambda: q.match("title", "helo", fuzziness="auto"),
    "range_dates": lambda: q.range_(
        "ts", gte=dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc), lt=dt.date(2027, 1, 1)
    ),
}


def _queries() -> list[tuple[str, Json]]:
    data = json.loads((FIXTURES / "queries.json").read_text())
    return [(entry["name"], entry["query"]) for entry in data["queries"]]


def test_every_fixture_query_has_a_builder() -> None:
    assert [name for name, _ in _queries()] == list(BUILDERS)


@pytest.mark.parametrize(("name", "expected"), _queries())
def test_every_fixture_query_is_produced_by_the_builder(name: str, expected: Json) -> None:
    assert _wire.encode_query(BUILDERS[name]()) == expected
