"""elasticsearch-py's `helpers.vectorstore` against Loams (plan M1.5 Task
11): the retrieval strategies LangChain builds its requests with (C5–C7,
C14, C18–C25). Each check builds a `VectorStore` with fixed 3-dimensional
vectors and asserts the top results and the score formulas.
"""

from __future__ import annotations

import math

import pytest
from elasticsearch import NotFoundError
from elasticsearch.helpers.vectorstore import (
    BM25Strategy,
    DenseVectorScriptScoreStrategy,
    DenseVectorStrategy,
    DistanceMetric,
    SparseVectorStrategy,
    VectorStore,
)

TEXTS = ["foo", "bar", "baz", "foo bar"]
VECTORS = [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 1.0], [0.6, 0.8, 0.0]]
METADATA = [{"page": 0, "kind": "a"}, {"page": 1, "kind": "b"}, {"page": 2, "kind": "a"}, {"page": 3, "kind": "b"}]
IDS = ["0", "1", "2", "3"]


def store(es, index, strategy):
    return VectorStore(client=es, index=index, retrieval_strategy=strategy, num_dimensions=3)


def fill(vs):
    assert vs.add_texts(TEXTS, metadatas=METADATA, vectors=VECTORS, ids=IDS) == IDS


def ids_of(hits):
    return [hit["_id"] for hit in hits]


def knn_tolerance(is_loams):
    """Loams scores knn hits exactly; Elasticsearch 8.19 quantizes float
    vectors (`int8_hnsw` by default), so its scores are close only."""
    return 1e-5 if is_loams else 5e-3

def cos(a, b):
    dot = sum(x * y for x, y in zip(a, b))
    return dot / (math.sqrt(sum(x * x for x in a)) * math.sqrt(sum(y * y for y in b)))


def test_dense_vector_strategy_knn(es, index, is_loams):
    vs = store(es, index, DenseVectorStrategy())
    fill(vs)
    q = [0.9, 0.1, 0.0]
    hits = vs.search(query_vector=q, k=2)
    assert ids_of(hits) == ["0", "3"]
    for hit in hits:
        assert hit["_score"] == pytest.approx((1 + cos(q, VECTORS[int(hit["_id"])])) / 2, rel=knn_tolerance(is_loams))
    assert hits[0]["_source"] == {"text_field": "foo", "metadata": {"page": 0, "kind": "a"}}
    # A filter narrows the knn.
    hits = vs.search(query_vector=q, k=2, filter=[{"term": {"metadata.kind": "b"}}])
    assert ids_of(hits) == ["3", "1"]


def test_dense_vector_strategy_hybrid_rrf(es, index):
    vs = store(es, index, DenseVectorStrategy(hybrid=True))
    fill(vs)
    hits = vs.search(query="bar", query_vector=[0.0, 1.0, 0.0], k=2)
    # "1" is first in both lists; "3" is second in both.
    assert ids_of(hits) == ["1", "3"]


def test_dense_vector_strategy_hybrid_sum(es, index):
    vs = store(es, index, DenseVectorStrategy(hybrid=True, rrf=False))
    fill(vs)
    q = [0.0, 1.0, 0.0]
    hits = vs.search(query="foo", query_vector=q, k=4)
    knn = {h["_id"]: h["_score"] for h in store(es, index, DenseVectorStrategy()).search(query_vector=q, k=4)}
    bm25 = {h["_id"]: h["_score"] for h in store(es, index, BM25Strategy()).search(query="foo", k=4)}
    assert set(ids_of(hits)) == set(IDS)
    for hit in hits:
        assert hit["_score"] == pytest.approx(knn.get(hit["_id"], 0) + bm25.get(hit["_id"], 0), rel=1e-4)
    # "3" matches the text and is close to the vector.
    assert ids_of(hits)[0] == "3"


@pytest.mark.parametrize(
    "distance,formula",
    [
        (DistanceMetric.COSINE, lambda q, v: cos(q, v) + 1.0),
        (
            DistanceMetric.EUCLIDEAN_DISTANCE,
            lambda q, v: 1 / (1 + math.sqrt(sum((a - b) ** 2 for a, b in zip(q, v)))),
        ),
        (
            DistanceMetric.DOT_PRODUCT,
            lambda q, v: 1 / (1 + math.exp(-sum(a * b for a, b in zip(q, v)))),
        ),
    ],
    ids=["cosine", "l2", "dot"],
)
def test_script_score_strategy_cosine_l2_dot(es, index, distance, formula):
    vs = store(es, index, DenseVectorScriptScoreStrategy(distance=distance))
    fill(vs)
    q = [0.5, 0.5, 0.1]
    hits = vs.search(query_vector=q, k=4)
    want = sorted(IDS, key=lambda i: -formula(q, VECTORS[int(i)]))
    assert ids_of(hits)[:2] == want[:2]
    for hit in hits:
        assert hit["_score"] == pytest.approx(formula(q, VECTORS[int(hit["_id"])]), rel=1e-4)
    hits = vs.search(query_vector=q, k=4, filter=[{"term": {"metadata.kind": "a"}}])
    assert set(ids_of(hits)) == {"0", "2"}


def test_bm25_strategy_with_filter(es, index):
    vs = store(es, index, BM25Strategy(k1=1.2, b=0.75))
    assert vs.add_texts(TEXTS, metadatas=METADATA, ids=IDS) == IDS
    hits = vs.search(query="foo", k=4)
    assert ids_of(hits) == ["0", "3"]
    assert hits[0]["_score"] > hits[1]["_score"]
    hits = vs.search(query="foo", k=4, filter=[{"term": {"metadata.kind": "b"}}])
    assert ids_of(hits) == ["3"]


def test_delete_by_ids_and_query(es, index):
    vs = store(es, index, DenseVectorStrategy())
    fill(vs)
    assert vs.delete(ids=["0", "nope"])
    assert es.count(index=index)["count"] == 3
    assert vs.delete(query={"term": {"metadata.kind": "b"}})
    assert es.count(index=index)["count"] == 1
    assert ids_of(vs.search(query_vector=[0.0, 0.0, 1.0], k=4)) == ["2"]


def test_mmr_reads_vectors_from_source(es, index):
    vs = store(es, index, DenseVectorStrategy())
    fill(vs)
    q = [1.0, 0.1, 0.0]
    hits = vs.max_marginal_relevance_search(query_embedding=q, vector_field="vector_field", k=2, num_candidates=4, lambda_mult=0.1)
    assert len(hits) == 2
    assert hits[0]["_id"] == "0"
    # Diversity: the second pick is not the near-duplicate "3".
    assert hits[1]["_id"] != "3"
    assert "vector_field" not in hits[0]["_source"]


def test_sparse_strategy_reports_model_missing(es, index):
    vs = store(es, index, SparseVectorStrategy(model_id="no-such-model"))
    with pytest.raises(NotFoundError):
        vs.add_texts(["x"])
