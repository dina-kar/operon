"""Scan plans (W15, Ruling 18)."""

from __future__ import annotations

import json
import time

import httpx
import pytest

import operon
from operon import Document, Schema, _wire, schema

_FRESH = {
    "namespace": "n",
    "collection": "sc",
    "collection_id": 4,
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


def _sc_schema() -> Schema:
    return Schema(vectors=[schema.vector("e", 2)], dynamic="ignore")


def test_scan_points_encode_the_wire_form() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json=_FRESH, headers={_wire.TOKEN_HEADER: "v1:s9/p0@0"})

    token = operon.ConsistencyToken.parse("v1:s9/p0@3")
    with operon.Client("http://operon.test", transport=httpx.MockTransport(handler)) as client:
        sc = client.namespace("n").collection("sc")
        sc.scan_plan()
        sc.scan_plan(at=5)
        sc.scan_plan(at=token)
        sc.scan_plan(at="v1:s9/p0@4")
        with pytest.raises(TypeError):
            sc.scan_plan(at=True)
        with pytest.raises(ValueError, match="consistency token"):
            sc.scan_plan(at="v2:x")
        with pytest.raises(ValueError, match="manifest version"):
            sc.scan_plan(at=-1)
    assert [json.loads(r.content) for r in requests] == [
        {"at": "current"},
        {"at": {"manifest_version": 5}},
        {"at": {"token": "v1:s9/p0@3"}},
        {"at": {"token": "v1:s9/p0@4"}},
    ]
    assert all(r.url.path == "/v1/namespaces/n/collections/sc/scan" for r in requests)
    assert all(_wire.TOKEN_HEADER not in r.headers for r in requests)


def test_scan_plan_of_a_fresh_collection(ns: operon.Namespace) -> None:
    ns.create_collection("sc", _sc_schema(), partitions=1)
    plan = ns.collection("sc").scan_plan()
    assert plan.collection == "sc"
    assert plan.manifest_version == 0
    assert plan.lance is None
    assert plan.fragments == []
    assert plan.live_rows == 0
    assert plan.tail is False
    assert plan.raw["offsets"][0]["applied"] == 0
    assert plan.pin.manifest_version == 0
    assert plan.expires_at_ms is None
    assert plan.durable_token == plan.pin.token


def _settled(collection: operon.Collection, **kwargs: object) -> operon.ScanPlan:
    deadline = time.monotonic() + 30
    while True:
        plan = collection.scan_plan(**kwargs)  # type: ignore[arg-type]
        if not plan.tail and plan.lance is not None:
            return plan
        if time.monotonic() > deadline:
            pytest.fail(f"the scan plan still has a tail after 30 s: {plan.raw}")
        time.sleep(0.1)


def _written(ns: operon.Namespace) -> tuple[operon.Collection, operon.WriteResult]:
    ns.create_collection("kb", _sc_schema(), partitions=1)
    kb = ns.collection("kb")
    kb.upsert([Document(i, {"i": i}, {"e": [1.0, float(i)]}) for i in (1, 2, 3)])
    kb.upsert([Document(1, {"i": 10}, {"e": [1.0, 0.0]})])
    return kb, kb.delete([2])


def test_scan_plan_after_writes_reports_lance_and_fragments(ns: operon.Namespace) -> None:
    kb, last = _written(ns)
    plan = _settled(kb)
    assert plan.lance is not None
    assert plan.lance.uri is not None
    assert plan.lance.uri.startswith("file://")
    assert plan.live_rows == 2
    assert plan.durable_token == plan.pin.token
    assert plan.fragments
    assert all(f.files for f in plan.fragments)
    vectors = [c for c in plan.columns if c.name == "_vector_0"]
    assert vectors
    assert vectors[0].vector == "e"
    assert vectors[0].dim == 2
    at_token = kb.scan_plan(at=last.token)
    assert at_token.manifest_version >= plan.manifest_version
    at_version = kb.scan_plan(at=plan.manifest_version)
    assert at_version.fragments == plan.fragments


def test_a_pin_reads_the_same_state(ns: operon.Namespace) -> None:
    kb, _ = _written(ns)
    plan = _settled(kb)
    assert ns.sql("SELECT count(*) AS n FROM kb", consistency=plan.pin).rows == [[2]]
    later = kb.upsert([Document(4, {"i": 4}, {"e": [0.0, 1.0]})])
    assert ns.sql("SELECT count(*) AS n FROM kb", consistency=later.token).rows == [[3]]
    assert ns.sql("SELECT count(*) AS n FROM kb", consistency=plan.pin).rows == [[2]]
    assert kb.get([4], consistency=plan.pin) == [None]
    hits = kb.search().consistency(plan.pin).execute().hits
    assert {h.id for h in hits} == {1, 3}


def test_scan_plan_opens_with_pylance(ns: operon.Namespace) -> None:
    lance = pytest.importorskip("lance")
    kb, _ = _written(ns)
    plan = _settled(kb)
    assert plan.lance is not None
    dataset = lance.dataset(plan.lance.uri, version=plan.lance.version)
    assert dataset.count_rows() == plan.live_rows
    keys = dataset.to_table(columns=["_pk"]).column("_pk").to_pylist()
    ids = set()
    for key in keys:
        assert key[0] == 0x01
        ids.add(int.from_bytes(key[1:], "big"))
    assert ids == {1, 3}


@pytest.mark.anyio
async def test_async_scan_plan(operon_url: str, ns_name: str) -> None:
    async with operon.AsyncClient(operon_url) as client:
        ns = client.namespace(ns_name)
        await ns.create_collection("sc", _sc_schema(), partitions=1)
        plan = await ns.collection("sc").scan_plan(at="current")
        assert plan.manifest_version == 0
        assert plan.pin.token == plan.durable_token
