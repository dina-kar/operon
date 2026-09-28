"""Collections and documents against a spawned `operon dev` (plan M1.6 Task 3)."""

from __future__ import annotations

import json
import uuid

import httpx
import pytest
from conftest import kb_docs, kb_schema

import operon
from operon import Document, Projection, Schema, SparseVector, _wire, q, schema


def test_collection_lifecycle(ns: operon.Namespace) -> None:
    info = ns.create_collection("kb", kb_schema(), partitions=2)
    assert info.name == "kb"
    assert info.partitions == 2
    assert info.schema.dynamic == "ignore"
    assert info.schema.version == 1
    assert [f.name for f in info.schema.fields] == ["body", "tenant", "n"]
    assert info.schema.fields[0].kind == schema.Text()
    assert info.schema.vectors[0] == schema.Vector("embedding", 3, "cosine")
    assert info.raw["name"] == "kb"
    got = ns.get_collection("kb")
    assert got.id == info.id
    assert got.schema.fields == info.schema.fields
    assert "kb" in [c.name for c in ns.list_collections()]
    assert ns.drop_collection("kb") is True
    assert ns.drop_collection("kb") is False
    with pytest.raises(operon.NotFoundError):
        ns.get_collection("kb")


def test_create_collection_is_retry_safe(ns: operon.Namespace) -> None:
    first = ns.create_collection("kb", kb_schema())
    again = ns.create_collection("kb", kb_schema())
    assert again.id == first.id
    other = Schema(fields=[schema.keyword("tenant")], dynamic="ignore")
    with pytest.raises(operon.AlreadyExistsError) as info:
        ns.create_collection("kb", other)
    assert info.value.id is None


def test_upsert_then_get_reads_its_own_write(ns: operon.Namespace) -> None:
    ns.create_collection("kb", kb_schema())
    collection = ns.collection("kb")
    result = collection.upsert(kb_docs())
    assert result.results == ["accepted"] * 3
    assert result.token.items
    docs = collection.get([1, 2, 3])
    assert [d.source["body"] if d else None for d in docs] == [
        "refund policy",
        "shipping times",
        "refund window",
    ]
    again = collection.get([1], consistency=result.token, select=Projection(vectors=["embedding"]))
    assert again[0] is not None
    assert again[0].vectors == {"embedding": [1.0, 0.0, 0.0]}
    eventual = collection.get([1], consistency="eventual")
    assert len(eventual) == 1


def test_patch_merge_deep_and_delete_keys(kb: operon.Collection) -> None:
    result = kb.patch(1, {"meta": {"x": 1}}, delete_keys=["tenant"])
    [doc] = kb.get([1], consistency=result.token)
    assert doc is not None
    assert doc.source["meta"] == {"x": 1}
    assert "tenant" not in doc.source
    assert doc.source["body"] == "refund policy"


def test_delete_then_get_returns_none(kb: operon.Collection) -> None:
    result = kb.delete([2])
    assert kb.get([2, 1], consistency=result.token)[0] is None


def test_hybrid_search_fuses_with_rrf(kb: operon.Collection) -> None:
    response = (
        kb.search()
        .retrieve(q.vector("embedding", [1, 0, 0], k=10), q.text(q.match("body", "refund"), k=10))
        .limit(3)
        .execute()
    )
    assert [h.id for h in response.hits] == [1, 3, 2]
    scores = [h.score for h in response.hits]
    assert scores == sorted(scores, reverse=True)
    assert response.hits[0].source is not None
    assert response.hits[0].source["body"] == "refund policy"
    assert isinstance(response.read_token, operon.ConsistencyToken)


def test_filter_applies_to_every_retriever(kb: operon.Collection) -> None:
    response = (
        kb.search()
        .retrieve(q.vector("embedding", [1, 0, 0], k=10), q.text(q.match("body", "refund"), k=10))
        .filter(q.term("tenant", "a"))
        .execute()
    )
    assert [h.id for h in response.hits] == [1, 2]


def test_u64_ids_above_2_53_round_trip(kb: operon.Collection) -> None:
    big = 2**63 + 5
    result = kb.upsert([Document(big, {"tenant": "c"})])
    [doc] = kb.get([big], consistency=result.token)
    assert doc is not None
    assert doc.id == big
    hits = kb.search().filter(q.ids(big)).execute().hits
    assert [h.id for h in hits] == [big]


def test_uuid_and_string_ids_round_trip(kb: operon.Collection) -> None:
    key = uuid.UUID("0190f5c4-6c1e-7b3a-9d2e-4f5a6b7c8d9e")
    result = kb.upsert([Document(key, {"tenant": "c"}), Document("k-str", {"tenant": "c"})])
    got = kb.get([key, "k-str"], consistency=result.token)
    assert [d.id if d else None for d in got] == [key, "k-str"]
    assert isinstance(got[0].id if got[0] else None, uuid.UUID)


def test_a_503_on_a_collection_write_is_retried() -> None:
    requests: list[httpx.Request] = []
    written = {"token": "v1:s1/p0@1", "results": ["accepted"], "positions": [None]}
    answers = [
        httpx.Response(503, json={"error": "unavailable", "message": "later"}),
        httpx.Response(200, json=written, headers={_wire.TOKEN_HEADER: "v1:s1/p0@1"}),
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return answers.pop(0)

    with operon.Client(
        "http://operon.test", transport=httpx.MockTransport(handler), retry_sleep=lambda _: None
    ) as client:
        result = client.namespace("n").collection("c").upsert([Document(1, {"a": 1})])
    assert result.results == ["accepted"]
    assert str(result.token) == "v1:s1/p0@1"
    assert len(requests) == 2
    assert json.loads(requests[0].content) == json.loads(requests[1].content)


def test_an_empty_write_sends_nothing() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(500)

    with operon.Client("http://operon.test", transport=httpx.MockTransport(handler)) as client:
        collection = client.namespace("n").collection("c")
        with pytest.raises(ValueError, match="empty"):
            collection.upsert([])
        with pytest.raises(ValueError, match="empty"):
            collection.delete([])
    assert requests == []


def test_wrong_vector_dimension_raises_an_invalid_argument_error(kb: operon.Collection) -> None:
    with pytest.raises(operon.InvalidArgumentError):
        kb.upsert([Document(7, {}, {"embedding": [1.0, 2.0]})])


@pytest.mark.anyio
async def test_async_collection_round_trip(operon_url: str, ns_name: str) -> None:
    async with operon.AsyncClient(operon_url) as client:
        ns = client.namespace(ns_name)
        info = await ns.create_collection("kb", kb_schema())
        assert (await ns.get_collection("kb")).id == info.id
        assert [c.name for c in await ns.list_collections()] == ["kb"]
        collection = ns.collection("kb")
        result = await collection.upsert(kb_docs())
        patched = await collection.patch(3, {"meta": {"y": 2}})
        deleted = await collection.delete([2])
        merged = result.token.merge(patched.token, deleted.token)
        docs = await collection.get([1, 2, 3], consistency=merged)
        assert docs[1] is None
        assert docs[2] is not None
        assert docs[2].source["meta"] == {"y": 2}
        response = await (
            collection.search()
            .retrieve(
                q.vector("embedding", [1, 0, 0], k=10), q.text("refund", k=10, fields=["body"])
            )
            .limit(3)
            .execute()
        )
        assert [h.id for h in response.hits] == [1, 3]
        sql = await ns.sql("SELECT count(*) AS n FROM kb", consistency=merged)
        assert sql.rows == [[2]]
        assert await ns.drop_collection("kb") is True


def test_sparse_and_hybrid_search(ns: operon.Namespace) -> None:
    sp_schema = Schema(
        vectors=[schema.vector("e", 2)],
        sparse_vectors=[schema.sparse_vector("s")],
        dynamic="ignore",
    )
    info = ns.create_collection("sp", sp_schema)
    assert info.schema.sparse_vectors == (schema.SparseVectorField("s", "none"),)
    sp = ns.collection("sp")
    sp.upsert(
        [
            Document(1, {}, {"e": [1.0, 0.0]}, {"s": SparseVector([5, 1], [2.0, 1.0])}),
            Document(2, {}, {"e": [0.0, 1.0]}, {"s": SparseVector([5], [0.5])}),
            Document(3, {}, {"e": [0.8, 0.6]}, {"s": SparseVector([7], [3.0])}),
        ]
    )
    sparse = q.sparse("s", [5], [1.0], k=10)
    only = sp.search().retrieve(sparse).execute()
    assert [h.id for h in only.hits] == [1, 2]
    assert [h.score for h in only.hits] == [2.0, 0.5]
    hybrid = sp.search().retrieve(sparse, q.vector("e", [1.0, 0.0], k=10)).limit(3).execute()
    assert [h.id for h in hybrid.hits] == [1, 2, 3]
    [doc] = sp.get([1], select=Projection(source="none", vectors=["s"]))
    assert doc is not None
    assert doc.source == {}
    assert doc.sparse_vectors["s"] == SparseVector((1, 5), (1.0, 2.0))
