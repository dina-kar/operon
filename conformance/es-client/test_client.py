"""The elasticsearch-py 8.19 client against Loams's Elasticsearch API (plan
M1.5 Task 11).

Only the public client API is used (no raw HTTP), so every check also
proves the client-side steps each framework request passes through: the
product check, the media types, response parsing and the error classes.
This is M1.5's own test over the Phase A surface, not elasticsearch-py's
test suite (D48).
"""

from __future__ import annotations

import math

import pytest
from elasticsearch import (
    BadRequestError,
    ConflictError,
    NotFoundError,
    helpers,
)


def ids_of(response):
    return [hit["_id"] for hit in response["hits"]["hits"]]


def knn_tolerance(is_loams):
    """Loams scores knn hits exactly; Elasticsearch 8.19 quantizes float
    vectors (`int8_hnsw` by default), so its scores are close only."""
    return 1e-5 if is_loams else 5e-3

def cos(a, b):
    dot = sum(x * y for x, y in zip(a, b))
    return dot / (math.sqrt(sum(x * x for x in a)) * math.sqrt(sum(y * y for y in b)))


def test_info_passes_the_product_check(es, is_loams):
    info = es.info()
    assert info["tagline"] == "You Know, for Search"
    assert info["version"]["number"].startswith("8.19.")
    if is_loams:
        assert info["version"]["number"] == "8.19.0"


def test_index_get_update_delete(es, index):
    doc = {"title": "hello", "n": 1, "tags": ["a", "b"]}
    assert es.index(index=index, id="1", document=doc, refresh=True)["result"] == "created"
    assert es.get(index=index, id="1")["_source"] == doc
    assert es.update(index=index, id="1", doc={"n": 2}, refresh=True)["result"] == "updated"
    assert es.get(index=index, id="1")["_source"]["n"] == 2
    # Unchanged: a noop (detect_noop defaults to true).
    assert es.update(index=index, id="1", doc={"n": 2})["result"] == "noop"
    # `doc_as_upsert` creates a missing document from `doc`.
    r = es.update(index=index, id="2", doc={"title": "two"}, doc_as_upsert=True, refresh=True)
    assert r["result"] == "created"
    assert es.get(index=index, id="2")["_source"] == {"title": "two"}
    # `upsert` creates from `upsert`, and merges `doc` when the document exists.
    r = es.update(index=index, id="3", doc={"n": 9}, upsert={"title": "three"}, refresh=True)
    assert r["result"] == "created"
    assert es.get(index=index, id="3")["_source"] == {"title": "three"}
    r = es.update(index=index, id="3", doc={"n": 9}, upsert={"title": "x"}, refresh=True)
    assert r["result"] == "updated"
    assert es.get(index=index, id="3")["_source"] == {"title": "three", "n": 9}
    # Updating a missing document without an upsert is 404.
    with pytest.raises(NotFoundError) as e:
        es.update(index=index, id="nope", doc={"n": 1})
    assert e.value.error == "document_missing_exception"
    assert es.delete(index=index, id="1", refresh=True)["result"] == "deleted"
    with pytest.raises(NotFoundError):
        es.get(index=index, id="1")
    assert not es.exists(index=index, id="1")
    assert es.exists(index=index, id="2")
    assert es.get_source(index=index, id="2") == {"title": "two"}


def test_create_conflict_raises_conflict_error(es, index):
    es.create(index=index, id="1", document={"a": 1}, refresh=True)
    with pytest.raises(ConflictError) as e:
        es.create(index=index, id="1", document={"a": 2})
    assert e.value.meta.status == 409
    assert e.value.error == "version_conflict_engine_exception"
    assert es.get(index=index, id="1")["_source"] == {"a": 1}


def test_helpers_bulk_and_mget(es, index):
    actions = ({"_index": index, "_id": str(i), "a": i, "b": f"v{i}"} for i in range(500))
    assert helpers.bulk(es, actions, refresh=True) == (500, [])
    assert es.count(index=index)["count"] == 500
    docs = es.mget(index=index, ids=["3", "nope", "7"], source_includes=["a"])["docs"]
    assert [d["_id"] for d in docs] == ["3", "nope", "7"]
    assert [d["found"] for d in docs] == [True, False, True]
    assert docs[0]["_source"] == {"a": 3}
    assert docs[2]["_source"] == {"a": 7}


def test_bulk_index_error_on_bad_vector(es, index):
    es.indices.create(
        index=index,
        mappings={"properties": {"v": {"type": "dense_vector", "dims": 3, "similarity": "cosine"}}},
    )
    actions = [
        {"_index": index, "_id": "ok", "v": [1.0, 0.0, 0.0]},
        {"_index": index, "_id": "bad", "v": [1.0, 0.0]},
    ]
    with pytest.raises(helpers.BulkIndexError) as e:
        helpers.bulk(es, actions, refresh=True)
    errors = e.value.errors
    assert len(errors) == 1
    assert errors[0]["index"]["_id"] == "bad"
    assert errors[0]["index"]["error"]["type"] == "document_parsing_exception"
    assert es.count(index=index)["count"] == 1


DOCS = {
    "1": {"title": "the quick brown fox", "tag": "animal", "n": 1, "when": "2026-01-01"},
    "2": {"title": "the lazy dog", "tag": "animal", "n": 2, "when": "2026-02-01"},
    "3": {"title": "quick silver", "tag": "metal", "n": 3},
    "4": {"title": "brown bread", "tag": "food", "n": 4, "when": "2026-04-01"},
}


def _dsl_index(es, index):
    es.indices.create(
        index=index,
        mappings={
            "properties": {
                "title": {"type": "text"},
                "tag": {"type": "keyword"},
                "n": {"type": "long"},
                "when": {"type": "date"},
            }
        },
    )
    helpers.bulk(es, ({"_index": index, "_id": k, **v} for k, v in DOCS.items()), refresh=True)


@pytest.mark.parametrize(
    "query,expected",
    [
        ({"match": {"title": "quick"}}, {"1", "3"}),
        ({"match_phrase": {"title": "lazy dog"}}, {"2"}),
        ({"multi_match": {"query": "brown", "fields": ["title"]}}, {"1", "4"}),
        (
            {"bool": {"must": [{"match": {"title": "brown"}}], "filter": [{"term": {"tag": "food"}}]}},
            {"4"},
        ),
        ({"bool": {"must_not": [{"term": {"tag": "animal"}}]}}, {"3", "4"}),
        ({"range": {"n": {"gte": 2, "lt": 4}}}, {"2", "3"}),
        ({"range": {"when": {"gte": "2026-02-01"}}}, {"2", "4"}),
        ({"terms": {"tag": ["metal", "food"]}}, {"3", "4"}),
        ({"exists": {"field": "when"}}, {"1", "2", "4"}),
        ({"prefix": {"tag": "ani"}}, {"1", "2"}),
        ({"wildcard": {"tag": "*e*"}}, {"3"}),
        ({"fuzzy": {"tag": {"value": "animel"}}}, {"1", "2"}),
        ({"ids": {"values": ["2", "4", "9"]}}, {"2", "4"}),
        ({"query_string": {"query": "title:quick AND tag:metal"}}, {"3"}),
        ({"constant_score": {"filter": {"term": {"tag": "animal"}}, "boost": 2}}, {"1", "2"}),
    ],
)
def test_search_dsl(es, index, query, expected):
    _dsl_index(es, index)
    r = es.search(index=index, query=query, size=10)
    assert set(ids_of(r)) == expected
    assert r["hits"]["total"] == {"value": len(expected), "relation": "eq"}


def test_aggregations_are_phase_b(es, index, is_loams):
    if not is_loams:
        pytest.skip("Loams refuses aggregations as Phase B")
    _dsl_index(es, index)
    with pytest.raises(BadRequestError):
        es.search(index=index, aggs={"t": {"terms": {"field": "tag"}}})


VECTORS = {
    "a": [1.0, 0.0, 0.0],
    "b": [0.8, 0.6, 0.0],
    "c": [0.0, 1.0, 0.0],
    "d": [0.0, 0.0, 1.0],
}
TEXTS = {"a": "red apple", "b": "green apple", "c": "green pear", "d": "blue sky"}


def _vector_index(es, index):
    es.indices.create(
        index=index,
        mappings={
            "properties": {
                "text": {"type": "text"},
                "v": {"type": "dense_vector", "dims": 3, "index": True, "similarity": "cosine"},
            }
        },
    )
    helpers.bulk(
        es,
        ({"_index": index, "_id": k, "text": TEXTS[k], "v": v} for k, v in VECTORS.items()),
        refresh=True,
    )


def test_knn_and_hybrid(es, index, is_loams):
    _vector_index(es, index)
    q = [1.0, 0.2, 0.0]
    r = es.search(index=index, knn={"field": "v", "query_vector": q, "k": 3, "num_candidates": 10})
    assert ids_of(r) == ["a", "b", "c"]
    for hit in r["hits"]["hits"]:
        assert hit["_score"] == pytest.approx((1 + cos(q, VECTORS[hit["_id"]])) / 2, rel=knn_tolerance(is_loams))
    # The vector comes back in `_source`.
    assert r["hits"]["hits"][0]["_source"]["v"] == VECTORS["a"]
    # RRF: `b` is in both lists (second by vector), `a` only in the knn
    # one and `c` only in the text one.
    r = es.search(
        index=index,
        retriever={
            "rrf": {
                "retrievers": [
                    {"standard": {"query": {"match": {"text": "green"}}}},
                    {"knn": {"field": "v", "query_vector": q, "k": 2, "num_candidates": 10}},
                ],
                "rank_constant": 60,
            }
        },
        size=3,
    )
    assert ids_of(r)[0] == "b"
    assert set(ids_of(r)) <= {"a", "b", "c"}
    # The ES score sum: knn score + BM25.
    knn_only = es.search(index=index, knn={"field": "v", "query_vector": q, "k": 4, "num_candidates": 10})
    text_only = es.search(index=index, query={"match": {"text": "apple"}})
    knn_scores = {h["_id"]: h["_score"] for h in knn_only["hits"]["hits"]}
    text_scores = {h["_id"]: h["_score"] for h in text_only["hits"]["hits"]}
    r = es.search(
        index=index,
        knn={"field": "v", "query_vector": q, "k": 4, "num_candidates": 10},
        query={"match": {"text": "apple"}},
    )
    for hit in r["hits"]["hits"]:
        want = knn_scores.get(hit["_id"], 0) + text_scores.get(hit["_id"], 0)
        assert hit["_score"] == pytest.approx(want, rel=1e-4)


def test_count_and_msearch(es, index):
    _dsl_index(es, index)
    assert es.count(index=index)["count"] == 4
    assert es.count(index=index, query={"term": {"tag": "animal"}})["count"] == 2
    r = es.msearch(
        index=index,
        searches=[
            {},
            {"query": {"match": {"title": "quick"}}},
            {"index": "nope_does_not_exist"},
            {"query": {"match_all": {}}},
            {"index": index},
            {"query": {"term": {"tag": "food"}}, "size": 1},
        ],
    )
    statuses = [item.get("status") for item in r["responses"]]
    assert statuses == [200, 404, 200]
    assert set(ids_of(r["responses"][0])) == {"1", "3"}
    assert r["responses"][1]["error"]["type"] == "index_not_found_exception"
    assert ids_of(r["responses"][2]) == ["4"]
    # The C47 BEIR batch: one header and one body per query, `_source: false`.
    searches = []
    for word in ["quick", "dog", "bread", "silver"] * 32:
        searches.append({"index": index})
        searches.append({"query": {"multi_match": {"query": word, "fields": ["title"]}}, "size": 10, "_source": False})
    r = es.msearch(searches=searches)
    assert len(r["responses"]) == 128
    assert all(item["status"] == 200 for item in r["responses"])
    assert ids_of(r["responses"][1]) == ["2"]
    assert "_source" not in r["responses"][1]["hits"]["hits"][0]


def test_search_after_paging(es, index, is_loams):
    es.indices.create(
        index=index,
        mappings={"properties": {"session_id": {"type": "keyword"}, "created_at": {"type": "long"}, "history": {"type": "text"}}},
    )
    made = []
    for n in range(14):
        r = es.index(
            index=index,
            document={"session_id": "s", "created_at": 1_700_000_000_000 + n, "history": f"m{n}"},
            refresh=True,
        )
        made.append(r["_id"])
    query = {"term": {"session_id": "s"}}
    seen = []
    after = None
    while True:
        kwargs = {"search_after": after} if after else {}
        r = es.search(index=index, query=query, sort="created_at:asc", size=5, **kwargs)
        hits = r["hits"]["hits"]
        if not hits:
            break
        seen.extend(h["_id"] for h in hits)
        after = hits[-1]["sort"]
    assert seen == made
    if is_loams:
        with pytest.raises(BadRequestError):
            es.open_point_in_time(index=index, keep_alive="1m")


def test_delete_by_query(es, index):
    _dsl_index(es, index)
    r = es.delete_by_query(index=index, query={"term": {"tag": "animal"}}, refresh=True)
    assert r["deleted"] == 2
    assert r["total"] == 2
    assert r["failures"] == []
    assert es.count(index=index)["count"] == 2


def test_indices_admin(es, names, is_loams):
    a = names()
    es.indices.create(
        index=a,
        mappings={"properties": {"title": {"type": "text"}}},
        settings={"number_of_shards": 1, "number_of_replicas": 0},
    )
    assert es.indices.exists(index=a)
    assert not es.indices.exists(index=names())
    mapping = es.indices.get_mapping(index=a)[a]["mappings"]
    assert mapping["properties"]["title"] == {"type": "text"}
    es.indices.put_mapping(index=a, properties={"tag": {"type": "keyword"}})
    mapping = es.indices.get_mapping(index=a)[a]["mappings"]
    assert mapping["properties"]["tag"] == {"type": "keyword"}
    alias = names("alias")
    es.indices.put_alias(index=a, name=alias)
    assert es.indices.exists_alias(name=alias)
    b = names()
    es.indices.create(index=b, settings={"number_of_replicas": 0})
    es.indices.update_aliases(actions=[{"add": {"index": b, "alias": alias}}, {"remove": {"index": a, "alias": alias}}])
    assert list(es.indices.get_alias(name=alias)) == [b]
    es.indices.delete_alias(index=b, name=alias)
    assert not es.indices.exists_alias(name=alias)
    everything = es.indices.get(index="_all")
    assert a in everything and b in everything
    es.indices.refresh(index=a)
    with pytest.raises(BadRequestError) as e:
        es.indices.delete(index="test_*")
    assert e.value.error == "illegal_argument_exception"
    es.indices.delete(index=b)
    assert not es.indices.exists(index=b)
    health = es.cluster.health()
    if is_loams:
        assert health["status"] == "green"
    else:
        assert health["status"] in ("green", "yellow")


def test_alias_on_two_indices_like_the_langchain_caches(es, names):
    i1, i2, i3 = names("test_index1"), names("test_index2"), names("test_index3")
    alias = names("test_alias")
    for i in (i1, i2):
        es.indices.create(index=i, settings={"number_of_replicas": 0})
    es.indices.put_alias(index=i1, name=alias)
    es.indices.put_alias(index=i2, name=alias, is_write_index=True)
    r = es.index(index=alias, id="k1", document={"llm_output": "one"}, require_alias=True, refresh=True)
    assert r["_index"] == i2
    ok, errors = helpers.bulk(
        es,
        [
            {"_op_type": "index", "_id": "k2", "llm_output": "two"},
            {"_op_type": "index", "_id": "k3", "llm_output": "three"},
            {"_op_type": "delete", "_id": "k3"},
        ],
        index=alias,
        require_alias=True,
        refresh=True,
    )
    assert (ok, errors) == (3, [])
    es.index(index=i1, id="old", document={"llm_output": "old"}, refresh=True)
    assert es.count(index=alias)["count"] == 3
    r = es.search(index=alias, query={"ids": {"values": ["k1", "k2", "old"]}})
    assert sorted(ids_of(r)) == ["k1", "k2", "old"]
    r = es.delete_by_query(index=alias, query={"ids": {"values": ["k2", "old"]}}, refresh=True)
    assert r["deleted"] == 2
    assert es.count(index=alias)["count"] == 1
    es.indices.create(index=i3, settings={"number_of_replicas": 0})
    es.indices.update_aliases(actions=[{"add": {"index": i3, "alias": alias}}])
    got = es.indices.get_alias(name=alias)
    assert set(got) == {i1, i2, i3}
    assert got[i2]["aliases"][alias] == {"is_write_index": True}
    assert got[i1]["aliases"][alias] == {}
    assert got[i3]["aliases"][alias] == {}
    es.indices.delete_alias(index=f"{i1},{i2}", name=alias)
    assert set(es.indices.get_alias(name=alias)) == {i3}
    # Two members and no write index: a write is refused.
    es.indices.put_alias(index=i1, name=alias)
    with pytest.raises(BadRequestError) as e:
        es.index(index=alias, id="x", document={"llm_output": "x"})
    assert e.value.error == "illegal_argument_exception"


def test_errors_map_to_client_classes(es, index):
    es.indices.create(index=index)
    with pytest.raises(BadRequestError) as e:
        es.search(index=index, query={"no_such_query": {}})
    assert e.value.error == "parsing_exception"
    with pytest.raises(NotFoundError) as e:
        es.search(index="missing_index_for_sure")
    assert e.value.error == "index_not_found_exception"
