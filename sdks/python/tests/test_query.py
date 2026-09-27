"""The query builder and the encoder's checks (plan M1.6 Task 3 rules 2-5)."""

from __future__ import annotations

import datetime as dt
import json
import math
from collections.abc import Iterator

import httpx
import pytest

import operon
from operon import Document, SparseVector, _wire, q

_SEARCH: dict[str, object] = {
    "hits": [],
    "total": None,
    "aggregations": None,
    "groups": None,
    "read_token": "v1:",
}


class Recorder:
    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if request.url.path.endswith("/query"):
            return httpx.Response(200, json=_SEARCH)
        written = {"token": "v1:s1/p0@1", "results": ["accepted"], "positions": [None]}
        return httpx.Response(200, json=written)


@pytest.fixture
def recorder() -> Recorder:
    return Recorder()


@pytest.fixture
def ns(recorder: Recorder) -> Iterator[operon.Namespace]:
    with operon.Client("http://operon.test", transport=httpx.MockTransport(recorder)) as client:
        yield client.namespace("n")


def _body(request: httpx.Request) -> dict[str, object]:
    body = json.loads(request.content)
    assert isinstance(body, dict)
    return body


class ListLike:
    """A numpy-like vector: only `tolist()`."""

    def __init__(self, values: list[float]) -> None:
        self.values = values

    def tolist(self) -> list[float]:
        return self.values


def test_two_retrievers_default_to_rrf_60(ns: operon.Namespace) -> None:
    builder = ns.search("kb").retrieve(q.vector("e", [1.0], k=5), q.text("refund", k=5))
    assert builder.build().fusion == q.Rrf(60)
    assert ns.search("kb").retrieve(q.vector("e", [1.0], k=5)).build().fusion is None
    assert ns.search("kb").build().fusion is None
    fused = builder.fuse(q.dbsf()).build()
    assert fused.fusion == q.Dbsf()


def test_builder_methods_do_not_mutate(ns: operon.Namespace, recorder: Recorder) -> None:
    base = ns.search("kb").filter(q.term("tenant", "a"))
    three = base.limit(3)
    seven = base.limit(7)
    three.execute()
    seven.execute()
    assert [_body(r)["limit"] for r in recorder.requests] == [3, 7]
    assert base.build().limit == 10
    assert base.retrieve(q.text("x", k=1)) is not base
    assert base.build().retrievers == ()


def test_text_with_a_string_picks_match_multi_match_or_query_string() -> None:
    assert q.text("refund", k=3) == q.TextRetriever(q.QueryString("refund"), 3)
    assert q.text("refund", k=3, fields=["body"]) == q.TextRetriever(q.Match("body", "refund"), 3)
    assert q.text("refund", k=3, fields=["body", "title"]) == q.TextRetriever(
        q.MultiMatch((("body", 1.0), ("title", 1.0)), "refund"), 3
    )
    assert q.text(q.match("body", "x"), k=2) == q.TextRetriever(q.Match("body", "x"), 2)
    assert _wire.encode_retriever(q.text("refund", k=3)) == {
        "text": {
            "query": {
                "query_string": {
                    "query": "refund",
                    "default_fields": [],
                    "default_operator": "or",
                }
            },
            "k": 3,
        }
    }


@pytest.mark.parametrize("bad", [math.nan, math.inf, -math.inf])
def test_nan_in_a_vector_is_rejected_before_sending(
    ns: operon.Namespace, recorder: Recorder, bad: float
) -> None:
    with pytest.raises(ValueError, match="'e'"):
        ns.search("kb").retrieve(q.vector("e", [1.0, bad], k=3)).execute()
    with pytest.raises(ValueError, match="'e'"):
        ns.collection("kb").upsert([Document(1, {}, {"e": [bad, 0.0]})])
    with pytest.raises(ValueError, match="'e'"):
        ns.collection("kb").patch(1, vectors={"e": [bad]})
    assert recorder.requests == []


def test_numpy_like_vectors_are_accepted(ns: operon.Namespace, recorder: Recorder) -> None:
    ns.collection("kb").upsert([Document(1, {}, {"e": ListLike([1, 0.5])})])
    ns.search("kb").retrieve(q.vector("e", ListLike([1, 0.5]), k=3)).execute()
    upsert = _body(recorder.requests[0])["ops"]
    assert upsert == [
        {"upsert": {"id": 1, "source": {}, "vectors": {"e": [1.0, 0.5]}, "sparse_vectors": {}}}
    ]
    retriever = _body(recorder.requests[1])["retrievers"]
    assert isinstance(retriever, list)
    assert retriever[0]["vector"]["query"] == [1.0, 0.5]
    assert all(isinstance(x, float) for x in retriever[0]["vector"]["query"])


def test_a_bool_in_a_vector_is_rejected(ns: operon.Namespace, recorder: Recorder) -> None:
    with pytest.raises(ValueError, match="'e'"):
        ns.collection("kb").upsert([Document(1, {}, {"e": [True, 0.0]})])
    with pytest.raises(ValueError, match="'e'"):
        ns.search("kb").retrieve(q.vector("e", ["1.0"], k=3)).execute()  # type: ignore[list-item]
    assert recorder.requests == []


def test_limit_below_one_is_rejected(ns: operon.Namespace, recorder: Recorder) -> None:
    with pytest.raises(ValueError, match="limit"):
        ns.search("kb").limit(0).build()
    with pytest.raises(ValueError, match="offset"):
        ns.search("kb").offset(-1).build()
    with pytest.raises(ValueError, match="k"):
        ns.search("kb").retrieve(q.text("x", k=0)).build()
    with pytest.raises(ValueError, match="limit"):
        ns.query(q.SearchRequest("kb", limit=0))
    assert recorder.requests == []


def test_naive_datetime_is_rejected(ns: operon.Namespace, recorder: Recorder) -> None:
    with pytest.raises(ValueError, match="naive"):
        ns.search("kb").filter(q.range_("ts", gte=dt.datetime(2026, 1, 1))).execute()
    assert recorder.requests == []


def test_dates_encode_as_rfc_3339() -> None:
    utc = dt.datetime(2026, 1, 2, 3, 4, 5, tzinfo=dt.timezone.utc)
    plus2 = dt.datetime(2026, 1, 2, 3, 4, 5, 6, tzinfo=dt.timezone(dt.timedelta(hours=2)))
    assert _wire.encode_field_value(utc) == {"date": "2026-01-02T03:04:05Z"}
    assert _wire.encode_field_value(plus2) == {"date": "2026-01-02T03:04:05.000006+02:00"}
    assert _wire.encode_field_value(dt.date(2026, 1, 2)) == {"date": "2026-01-02T00:00:00Z"}
    assert _wire.encode_field_value(2**63) == 2**63
    with pytest.raises(ValueError, match="2\\*\\*64"):
        _wire.encode_field_value(2**64)


@pytest.mark.parametrize(
    ("indices", "values"),
    [
        ([1, 1], [1.0, 2.0]),
        ([1, 2], [1.0]),
        ([1], [math.nan]),
        ([-1], [1.0]),
        ([2**32], [1.0]),
    ],
)
def test_sparse_vectors_are_validated_before_sending(
    ns: operon.Namespace, recorder: Recorder, indices: list[int], values: list[float]
) -> None:
    with pytest.raises(ValueError, match="sparse vector"):
        SparseVector(indices, values)
    with pytest.raises(ValueError, match="sparse vector"):
        ns.search("sp").retrieve(q.sparse("s", indices, values, k=10)).execute()
    assert recorder.requests == []


def test_sparse_vectors_accept_numpy_arrays() -> None:
    np = pytest.importorskip("numpy")
    indices = np.array([5, 1], dtype=np.int64)
    values = np.array([2.0, 0.5], dtype=np.float32)
    vector = SparseVector(indices, values)
    assert vector == SparseVector([5, 1], [2.0, 0.5])
    assert all(type(i) is int for i in vector.indices)
    assert all(type(v) is float for v in vector.values)
    assert _wire.encode_retriever(q.sparse("s", indices, values, k=3))["sparse"]["query"] == {
        "indices": [5, 1],
        "values": [2.0, 0.5],
    }
    # The checks still apply after the conversion.
    with pytest.raises(ValueError, match="unique"):
        SparseVector(np.array([1, 1]), np.array([1.0, 2.0]))
    with pytest.raises(ValueError, match="finite"):
        SparseVector(np.array([1]), np.array([np.nan]))
    with pytest.raises(ValueError, match="an int"):
        SparseVector(np.array([True]), np.array([1.0]))


def test_sparse_retriever_encodes_the_wire_form() -> None:
    assert _wire.encode_retriever(q.sparse("s", [5], [1.0], k=10)) == {
        "sparse": {
            "field": "s",
            "query": {"indices": [5], "values": [1.0]},
            "k": 10,
            "filter": None,
            "params": {"idf_corpus": None},
        }
    }


def test_sort_keys_encode_the_wire_form() -> None:
    assert _wire.encode_sort_key(q.score_sort()) == {"score": {"order": "desc"}}
    assert _wire.encode_sort_key(q.field_sort("n", order="desc")) == {
        "field": {"field": "n", "order": "desc", "missing": "last"}
    }
    assert _wire.encode_sort_key(q.pk_sort()) == {"pk": {"order": "asc"}}


def test_every_search_request_key_is_sent(ns: operon.Namespace, recorder: Recorder) -> None:
    (
        ns.search("kb")
        .retrieve(
            q.fused(
                q.vector("e", [1.0], k=4), q.text("x", k=4), fusion=q.weighted_sum(0.7, 0.3), k=5
            ),
            q.rescore(q.vector("e", [1.0], k=4, exact=True, ef=16), "e", [0.5], k=3),
        )
        .fuse(q.rrf(10))
        .sort(q.score_sort(), q.pk_sort("desc"))
        .offset(2)
        .limit(4)
        .search_after([1.5, {"uuid": "0190f5c4-6c1e-7b3a-9d2e-4f5a6b7c8d9e"}])
        .score_threshold(0.25)
        .select(q.Projection(source=q.SourcePaths(include=["a"]), vectors=["e"], fields=["n"]))
        .aggregations({"t": {"terms": {"field": "tenant"}}})
        .highlight({"fields": [{"field": "body"}]})
        .group_by({"field": "tenant", "group_size": 1, "limit": 2})
        .track_total_hits(q.UpTo(100))
        .consistency("eventual")
        .execute()
    )
    body = _body(recorder.requests[0])
    assert list(body) == [
        "collection",
        "consistency",
        "retrievers",
        "fusion",
        "filter",
        "sort",
        "offset",
        "limit",
        "search_after",
        "score_threshold",
        "select",
        "aggregations",
        "highlight",
        "group_by",
        "track_total_hits",
    ]
    assert body["consistency"] == "eventual"
    assert body["fusion"] == {"rrf": {"k": 10}}
    assert body["sort"] == [{"score": {"order": "desc"}}, {"pk": {"order": "desc"}}]
    assert body["select"] == {
        "source": {"include": ["a"], "exclude": []},
        "vectors": ["e"],
        "fields": ["n"],
    }
    assert body["track_total_hits"] == {"up_to": 100}
    retrievers = body["retrievers"]
    assert isinstance(retrievers, list)
    assert retrievers[0]["fused"]["fusion"] == {"weighted_sum": {"weights": [0.7, 0.3]}}
    assert retrievers[1]["rescore"]["input"]["vector"]["params"] == {
        "exact": True,
        "nprobes": None,
        "refine_factor": None,
        "ef": 16,
        "oversampling": None,
        "distance": None,
    }
    assert _wire.TOKEN_HEADER.lower() not in recorder.requests[0].headers


def test_a_token_is_sent_in_the_header_and_as_at_least(
    ns: operon.Namespace, recorder: Recorder
) -> None:
    ns.search("kb").consistency("v1:s1/p0@3").execute()
    token = operon.ConsistencyToken.parse("v1:s1/p0@4")
    ns.query(q.SearchRequest("kb"), consistency=token)
    pin = operon.Pin(5, operon.ConsistencyToken.parse("v1:s1/p0@6"))
    ns.search("kb").consistency(pin).execute()
    first, second, third = recorder.requests
    assert first.headers[_wire.TOKEN_HEADER] == "v1:s1/p0@3"
    assert _body(first)["consistency"] == {"at_least": "v1:s1/p0@3"}
    assert second.headers[_wire.TOKEN_HEADER] == "v1:s1/p0@4"
    assert _body(third)["consistency"] == {"pinned": {"manifest_version": 5, "token": "v1:s1/p0@6"}}
    assert _wire.TOKEN_HEADER not in third.headers
    with pytest.raises(ValueError, match="consistency token"):
        ns.search("kb").consistency("v2:nope").execute()
    assert len(recorder.requests) == 3
