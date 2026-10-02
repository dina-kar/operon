"""The official Python `qdrant-client` against a running Loams (plan M1.4
Task 10).

`--mode legacy` runs with qdrant-client 1.15.1 (the legacy search, recommend
and discover methods, REST); `--mode modern` runs with 1.19.1 (the query
API), every check once over REST and once with `prefer_grpc=True`.

Plain asserts, one function per check. Each check works in fresh
collections named `smoke_<check>_<uuid4 hex>` and deletes them afterwards.
Expected results come from brute force over the points the check wrote,
with Qdrant's formulas, so a result that parses but differs from Qdrant's
fails too.
"""

from __future__ import annotations

import argparse
import math
import random
import sys
import traceback
import uuid
import warnings
from importlib.metadata import version as package_version
from urllib.parse import urlparse

import grpc
import httpx
from qdrant_client import QdrantClient, models
from qdrant_client.common.version_check import get_server_version, is_compatible
from qdrant_client.http.exceptions import UnexpectedResponse

SERVER_VERSION = "1.19.1"
F32_EPSILON = 1.1920929e-07
CHECKS = []


def check(*modes):
    """Registers a check for `modes` (both when none are given)."""

    def register(fn):
        CHECKS.append((fn.__name__, fn, modes or ("legacy", "modern")))
        return fn

    return register


class Ctx:
    """One check's client and the collections it made."""

    def __init__(self, args, check_name, grpc_on):
        self.args = args
        self.check_name = check_name
        self.grpc = grpc_on
        self.client = client_for(args, grpc_on)
        self.rest = self.client if not grpc_on else client_for(args, False)
        self.made = []

    def name(self, suffix=""):
        n = f"smoke_{self.check_name}{suffix}_{uuid.uuid4().hex}"
        self.made.append(n)
        return n

    def close(self):
        for n in self.made:
            try:
                self.rest.delete_collection(n)
            except Exception:  # noqa: BLE001 - best-effort cleanup
                pass
        self.client.close()
        if self.rest is not self.client:
            self.rest.close()


def client_for(args, grpc_on):
    return QdrantClient(
        url=args.url,
        grpc_port=args.grpc_port,
        prefer_grpc=grpc_on,
        timeout=60,
    )


# ----- Qdrant's formulas (plan M1.4, "Scoring formulas") -----


def dot(a, b):
    return sum(x * y for x, y in zip(a, b))


def normalize(v):
    n2 = dot(v, v)
    if n2 < F32_EPSILON or abs(n2 - 1.0) <= 1e-6:
        return list(v)
    n = math.sqrt(n2)
    return [x / n for x in v]


def sim(distance, a, b):
    if distance in ("Cosine", "Dot"):
        return dot(a, b)
    if distance == "Euclid":
        return -sum((x - y) ** 2 for x, y in zip(a, b))
    return -sum(abs(x - y) for x, y in zip(a, b))


def nearest_score(distance, q, v):
    s = sim(distance, q, v)
    if distance == "Euclid":
        return math.sqrt(abs(s))
    if distance == "Manhattan":
        return abs(s)
    return s


def sig(x):
    return 0.5 * (x / (1 + abs(x)) + 1)


def best_score(d, c, pos, neg):
    p = max((sim(d, c, x) for x in pos), default=-math.inf)
    n = max((sim(d, c, x) for x in neg), default=-math.inf)
    return sig(p) if p > n else -sig(n)


def sum_scores(d, c, pos, neg):
    return sum(sim(d, c, x) for x in pos) - sum(sim(d, c, x) for x in neg)


def discover_score(d, c, target, pairs):
    rank = 0
    for p, n in pairs:
        sp, sn = sim(d, c, p), sim(d, c, n)
        rank += (sp > sn) - (sp < sn)
    return rank + sig(sim(d, c, target))


def context_score(d, c, pairs):
    total = 0.0
    for p, n in pairs:
        x = min(sim(d, c, p) - sim(d, c, n) - F32_EPSILON, 0.0)
        total += x / (1 + abs(x))
    return total


def ranked(points, score, exclude=(), limit=10, offset=0, ascending=False):
    """`[(id, score)]` of `points` (`{id: vector}`) by `score(vector)`."""
    scored = [(i, score(v)) for i, v in points.items() if i not in exclude]
    scored.sort(key=lambda t: ((t[1] if ascending else -t[1]), t[0]))
    return scored[offset : offset + limit]


def got(points):
    """`[(id, score)]` of client results (a list, or a `QueryResponse`)."""
    points = getattr(points, "points", points)
    return [(p.id, p.score) for p in points]


def assert_ranked(actual, expected, tol=1e-4):
    actual = got(actual)
    assert [i for i, _ in actual] == [i for i, _ in expected], f"{actual} != {expected}"
    for (i, a), (_, e) in zip(actual, expected):
        assert abs(a - e) <= tol * max(1.0, abs(e)), f"point {i}: score {a} != {e}"


def random_vectors(n, dim, seed, start=1):
    rng = random.Random(seed)
    return {i: [rng.uniform(-1, 1) for _ in range(dim)] for i in range(start, start + n)}


def dense_collection(ctx, distance="Dot", n=60, dim=4, seed=7, payload=None, suffix=""):
    """A collection of `n` points `1..n` under the unnamed vector; the
    stored vectors (normalized for Cosine) by id."""
    name = ctx.name(suffix)
    ctx.client.create_collection(
        name, vectors_config=models.VectorParams(size=dim, distance=models.Distance(distance))
    )
    vectors = random_vectors(n, dim, seed)
    ctx.client.upsert(
        name,
        points=[
            models.PointStruct(id=i, vector=v, payload=(payload(i) if payload else {"n": i}))
            for i, v in vectors.items()
        ],
        wait=True,
    )
    if distance == "Cosine":
        vectors = {i: normalize(v) for i, v in vectors.items()}
    return name, vectors


def expect_error(fn, http_status, grpc_code, text=None):
    """Runs `fn`, which must fail with `http_status` over REST or
    `grpc_code` over gRPC; `text` must be in the message."""
    try:
        fn()
    except UnexpectedResponse as e:
        assert e.status_code == http_status, f"{e.status_code} != {http_status}: {e.content!r}"
        if text:
            assert text in e.content.decode(), e.content
        return
    except grpc.RpcError as e:
        assert e.code() == grpc_code, f"{e.code()} != {grpc_code}: {e.details()}"
        if text:
            assert text in e.details(), e.details()
        return
    raise AssertionError("the call did not fail")


# ----- both modes -----


@check()
def server_version(ctx):
    rest_uri = ctx.args.url
    try:
        version = get_server_version(rest_uri, {}, None, 5)
    except TypeError:  # 1.15.1 takes no timeout
        version = get_server_version(rest_uri, {}, None)
    assert version == SERVER_VERSION, version
    compatible = is_compatible(package_version("qdrant-client"), version)
    # 1.15.1 is four minors behind: its users see the version warning.
    assert compatible is (ctx.args.mode == "modern"), compatible
    info = ctx.client.info()
    assert (info.title, info.version) == ("qdrant - vector search engine", SERVER_VERSION)


@check()
def collections(ctx):
    single = ctx.name("_single")
    ctx.client.create_collection(
        single,
        vectors_config=models.VectorParams(size=4, distance=models.Distance.COSINE, on_disk=True),
        optimizers_config=models.OptimizersConfigDiff(memmap_threshold=1000),
        on_disk_payload=True,
        shard_number=1,
        replication_factor=1,
        write_consistency_factor=1,
    )
    assert ctx.client.collection_exists(single)
    info = ctx.client.get_collection(single)
    params = info.config.params
    assert (params.vectors.size, params.vectors.distance) == (4, models.Distance.COSINE)
    assert params.vectors.on_disk is True
    assert params.on_disk_payload is True
    assert info.config.optimizer_config.memmap_threshold == 1000
    assert info.points_count == 0
    names = {c.name for c in ctx.client.get_collections().collections}
    assert single in names
    expect_error(
        lambda: ctx.client.create_collection(
            single, vectors_config=models.VectorParams(size=4, distance=models.Distance.COSINE)
        ),
        409,
        grpc.StatusCode.ALREADY_EXISTS,
        "already exists",
    )

    named = ctx.name("_named")
    ctx.client.create_collection(
        named,
        vectors_config={
            "": models.VectorParams(size=4, distance=models.Distance.COSINE),
            "img": models.VectorParams(size=3, distance=models.Distance.EUCLID),
        },
    )
    vectors = ctx.client.get_collection(named).config.params.vectors
    assert set(vectors) == {"", "img"}, vectors
    assert (vectors["img"].size, vectors["img"].distance) == (3, models.Distance.EUCLID)

    # Binary quantization with every named setting: the gRPC enum names and
    # the REST strings (a Task 3 carry item) round-trip through both.
    dot_ = ctx.name("_dot")
    binary = models.BinaryQuantization(
        binary=models.BinaryQuantizationConfig(
            always_ram=True,
            encoding=models.BinaryQuantizationEncoding.TWO_BITS,
            query_encoding=models.BinaryQuantizationQueryEncoding.SCALAR8BITS,
        )
    )
    ctx.client.create_collection(
        dot_,
        vectors_config=models.VectorParams(size=2, distance=models.Distance.DOT),
        quantization_config=binary,
    )
    info = ctx.client.get_collection(dot_)
    assert info.config.params.vectors.distance == models.Distance.DOT
    assert info.config.quantization_config == binary, info.config.quantization_config
    for encoding in models.BinaryQuantizationEncoding:
        for query_encoding in models.BinaryQuantizationQueryEncoding:
            q = ctx.name("_binary")
            config = models.BinaryQuantization(
                binary=models.BinaryQuantizationConfig(encoding=encoding, query_encoding=query_encoding)
            )
            ctx.client.create_collection(
                q, vectors_config=models.VectorParams(size=2, distance=models.Distance.DOT), quantization_config=config
            )
            assert ctx.client.get_collection(q).config.quantization_config == config, (encoding, query_encoding)
            ctx.client.delete_collection(q)

    assert ctx.client.delete_collection(dot_) is True
    assert not ctx.client.collection_exists(dot_)
    assert ctx.client.delete_collection(f"smoke_missing_{uuid.uuid4().hex}") is False


@check()
def aliases(ctx):
    name = ctx.name()
    ctx.client.create_collection(
        name, vectors_config=models.VectorParams(size=2, distance=models.Distance.DOT)
    )
    a, b = f"{name}_a", f"{name}_b"
    ctx.client.update_collection_aliases(
        change_aliases_operations=[
            models.CreateAliasOperation(
                create_alias=models.CreateAlias(collection_name=name, alias_name=a)
            )
        ]
    )
    pairs = {(x.alias_name, x.collection_name) for x in ctx.client.get_aliases().aliases}
    assert (a, name) in pairs
    assert ctx.client.collection_exists(a)
    ctx.client.update_collection_aliases(
        change_aliases_operations=[
            models.RenameAliasOperation(
                rename_alias=models.RenameAlias(old_alias_name=a, new_alias_name=b)
            )
        ]
    )
    own = [(x.alias_name, x.collection_name) for x in ctx.client.get_collection_aliases(name).aliases]
    assert own == [(b, name)], own
    ctx.client.update_collection_aliases(
        change_aliases_operations=[
            models.DeleteAliasOperation(delete_alias=models.DeleteAlias(alias_name=b))
        ]
    )
    assert ctx.client.get_collection_aliases(name).aliases == []


@check()
def upsert_and_retrieve(ctx):
    name = ctx.name()
    ctx.client.create_collection(
        name,
        vectors_config={
            "": models.VectorParams(size=2, distance=models.Distance.DOT),
            "img": models.VectorParams(size=2, distance=models.Distance.EUCLID),
        },
    )
    simple = uuid.uuid4()
    dashed = uuid.uuid4()
    payload = {"title": "a", "metadata": {"page": 3, "details": {"page": None}}, "tags": ["x", None]}
    result = ctx.client.upsert(
        name,
        points=[
            models.PointStruct(id=simple.hex, vector={"": [1.0, 2.0], "img": [3.0, 4.0]}, payload=payload),
            models.PointStruct(id=str(dashed), vector={"": [0.5, 0.5]}, payload={"k": None}),
            models.PointStruct(id=7, vector={"img": [1.0, 1.0]}),
        ],
        wait=True,
    )
    assert result.status == models.UpdateStatus.COMPLETED
    records = ctx.client.retrieve(name, ids=[simple.hex, 7, str(dashed), 999], with_vectors=True)
    by_id = {r.id: r for r in records}
    assert set(by_id) == {str(simple), str(dashed), 7}, by_id
    assert by_id[str(simple)].payload == payload
    assert by_id[str(simple)].vector == {"": [1.0, 2.0], "img": [3.0, 4.0]}
    assert by_id[str(dashed)].payload == {"k": None}
    assert by_id[7].vector == {"img": [1.0, 1.0]}
    one = ctx.client.retrieve(name, ids=[7], with_payload=False, with_vectors=["img"])[0]
    # gRPC carries an empty payload map, which the client reads as {}.
    assert not one.payload and one.vector == {"img": [1.0, 1.0]}

    plain = ctx.name("_plain")
    ctx.client.create_collection(
        plain, vectors_config=models.VectorParams(size=3, distance=models.Distance.COSINE)
    )
    vectors = random_vectors(150, 3, 11)
    ctx.client.upload_points(
        plain,
        points=[models.PointStruct(id=i, vector=v, payload={"i": i}) for i, v in vectors.items()],
        batch_size=64,
        wait=True,
    )
    assert ctx.client.count(plain, exact=True).count == 150
    stored = ctx.client.retrieve(plain, ids=[5], with_vectors=True)[0].vector
    assert all(abs(a - b) < 1e-6 for a, b in zip(stored, normalize(vectors[5]))), stored


@check()
def scroll_pages(ctx):
    name = ctx.name()
    ctx.client.create_collection(
        name, vectors_config=models.VectorParams(size=2, distance=models.Distance.DOT)
    )
    ids = list(range(1, 76)) + [str(uuid.uuid4()) for _ in range(75)]
    ctx.client.upsert(
        name,
        points=[models.PointStruct(id=i, vector=[1.0, 0.0], payload={"n": k}) for k, i in enumerate(ids)],
        wait=True,
    )
    seen, offset, pages = [], None, 0
    while True:
        points, offset = ctx.client.scroll(name, limit=17, offset=offset, with_payload=False)
        seen.extend(p.id for p in points)
        pages += 1
        if offset is None:
            break
    assert pages == 9, pages
    assert len(seen) == 150 and len(set(seen)) == 150
    assert set(seen) == set(ids)
    # Numbers come before UUIDs, each in order.
    assert seen[:75] == list(range(1, 76))
    assert seen[75:] == sorted(seen[75:])
    assert ctx.client.count(name, exact=True).count == 150
    points, _ = ctx.client.scroll(name, limit=2, with_vectors=True)
    assert points[0].vector == [1.0, 0.0]


@check()
def payload_and_vector_writes(ctx):
    name = ctx.name()
    ctx.client.create_collection(
        name,
        vectors_config={
            "": models.VectorParams(size=2, distance=models.Distance.DOT),
            "img": models.VectorParams(size=2, distance=models.Distance.DOT),
        },
    )
    ctx.client.upsert(
        name,
        points=[
            models.PointStruct(id=i, vector={"": [float(i), 1.0], "img": [1.0, float(i)]}, payload={"n": i, "keep": True})
            for i in range(1, 11)
        ],
        wait=True,
    )

    def payload_of(i):
        return ctx.client.retrieve(name, ids=[i])[0].payload

    ctx.client.set_payload(name, payload={"a": 1}, points=[1], wait=True)
    assert payload_of(1) == {"n": 1, "keep": True, "a": 1}
    ctx.client.set_payload(name, payload={"b": 2}, key="nested.inner", points=[1], wait=True)
    assert payload_of(1)["nested"] == {"inner": {"b": 2}}, payload_of(1)
    ctx.client.overwrite_payload(name, payload={"only": 1}, points=[2], wait=True)
    assert payload_of(2) == {"only": 1}
    ctx.client.delete_payload(name, keys=["keep"], points=[3], wait=True)
    assert payload_of(3) == {"n": 3}
    ctx.client.clear_payload(name, points_selector=models.PointIdsList(points=[4]), wait=True)
    assert payload_of(4) == {}
    ctx.client.update_vectors(
        name, points=[models.PointVectors(id=5, vector={"img": [9.0, 9.0]})], wait=True
    )
    assert ctx.client.retrieve(name, ids=[5], with_vectors=True)[0].vector == {"": [5.0, 1.0], "img": [9.0, 9.0]}
    ctx.client.delete_vectors(name, vectors=["img"], points=[5], wait=True)
    assert ctx.client.retrieve(name, ids=[5], with_vectors=True)[0].vector == {"": [5.0, 1.0]}
    # Setting a payload by filter touches exactly the matches.
    ctx.client.set_payload(
        name,
        payload={"big": True},
        points=models.Filter(must=[models.FieldCondition(key="n", range=models.Range(gte=9))]),
        wait=True,
    )
    assert [p.id for p in ctx.client.scroll(name, scroll_filter=models.Filter(must=[models.FieldCondition(key="big", match=models.MatchValue(value=True))]))[0]] == [9, 10]

    results = ctx.client.batch_update_points(
        name,
        update_operations=[
            models.UpsertOperation(upsert=models.PointsList(points=[models.PointStruct(id=11, vector={"": [1.0, 1.0]}, payload={"n": 11})])),
            models.SetPayloadOperation(set_payload=models.SetPayload(payload={"s": 1}, points=[11])),
            models.OverwritePayloadOperation(overwrite_payload=models.SetPayload(payload={"o": 1}, points=[6])),
            models.DeletePayloadOperation(delete_payload=models.DeletePayload(keys=["n"], points=[7])),
            models.ClearPayloadOperation(clear_payload=models.PointIdsList(points=[8])),
            models.UpdateVectorsOperation(update_vectors=models.UpdateVectors(points=[models.PointVectors(id=6, vector={"img": [2.0, 2.0]})])),
            models.DeleteVectorsOperation(delete_vectors=models.DeleteVectors(points=[7], vector=["img"])),
            models.DeleteOperation(delete=models.PointIdsList(points=[10])),
        ],
        wait=True,
    )
    assert len(results) == 8 and all(r.status == models.UpdateStatus.COMPLETED for r in results)
    assert payload_of(11) == {"n": 11, "s": 1}
    assert payload_of(6) == {"o": 1}
    assert payload_of(7) == {"keep": True}
    assert payload_of(8) == {}
    assert ctx.client.retrieve(name, ids=[6], with_vectors=["img"])[0].vector == {"img": [2.0, 2.0]}
    assert ctx.client.retrieve(name, ids=[7], with_vectors=True)[0].vector == {"": [7.0, 1.0]}
    assert ctx.client.retrieve(name, ids=[10]) == []

    ctx.client.delete(name, points_selector=models.PointIdsList(points=[1]), wait=True)
    ctx.client.delete(
        name,
        points_selector=models.FilterSelector(filter=models.Filter(should=[models.HasIdCondition(has_id=[2, 3])])),
        wait=True,
    )
    ctx.client.delete(
        name,
        points_selector=models.FilterSelector(filter=models.Filter(must=[models.FieldCondition(key="n", match=models.MatchValue(value=9))])),
        wait=True,
    )
    left = sorted(p.id for p in ctx.client.scroll(name, limit=100)[0])
    assert left == [4, 5, 6, 7, 8, 11], left


@check()
def payload_indexes(ctx):
    name = ctx.name()
    ctx.client.create_collection(
        name, vectors_config=models.VectorParams(size=2, distance=models.Distance.DOT)
    )
    ctx.client.upsert(
        name,
        points=[models.PointStruct(id=1, vector=[1.0, 0.0], payload={"doc_id": "a", "page": "3", "text": "Hello world"})],
        wait=True,
    )
    for field, schema in [
        ("doc_id", models.PayloadSchemaType.KEYWORD),
        ("page", models.PayloadSchemaType.INTEGER),
        ("text", models.PayloadSchemaType.TEXT),
    ]:
        result = ctx.client.create_payload_index(name, field_name=field, field_schema=schema, wait=True)
        assert result.status == models.UpdateStatus.COMPLETED
    # Idempotent, as LlamaIndex relies on.
    ctx.client.create_payload_index(name, field_name="page", field_schema=models.PayloadSchemaType.INTEGER, wait=True)
    schema = ctx.client.get_collection(name).payload_schema
    kinds = {k: v.data_type for k, v in schema.items()}
    assert kinds == {
        "doc_id": models.PayloadSchemaType.KEYWORD,
        "page": models.PayloadSchemaType.INTEGER,
        "text": models.PayloadSchemaType.TEXT,
    }, kinds
    # Filters never depend on an index: the string "3" does not match 3.
    count = ctx.client.count(name, count_filter=models.Filter(must=[models.FieldCondition(key="page", match=models.MatchValue(value=3))]))
    assert count.count == 0
    hits = ctx.client.count(name, count_filter=models.Filter(must=[models.FieldCondition(key="text", match=models.MatchText(text="hello"))]))
    assert hits.count == 1


def filter_payload(i):
    rng = random.Random(i)
    p = {
        "n": i,
        "tag": rng.choice(["red", "green", "blue"]),
        "tags": rng.sample(["a", "b", "c", "d"], rng.randint(0, 3)),
        "metadata": {"page": i % 5, "details": {"page": i % 3}},
    }
    if i % 4 == 0:
        p["maybe"] = None
    elif i % 4 == 1:
        p["maybe"] = "x"
    return p


@check()
def filters(ctx):
    name, _ = dense_collection(ctx, n=40, payload=filter_payload)
    payloads = {i: filter_payload(i) for i in range(1, 41)}
    FC, M = models.FieldCondition, models
    cases = [
        (M.Filter(must=[FC(key="tag", match=M.MatchValue(value="red"))]), lambda p, i: p["tag"] == "red"),
        (M.Filter(must=[FC(key="metadata.page", match=M.MatchValue(value=2))]), lambda p, i: p["metadata"]["page"] == 2),
        (M.Filter(must=[FC(key="metadata.details.page", match=M.MatchValue(value=0))]), lambda p, i: p["metadata"]["details"]["page"] == 0),
        (M.Filter(must=[FC(key="tags", match=M.MatchAny(any=["a", "d"]))]), lambda p, i: bool({"a", "d"} & set(p["tags"]))),
        (M.Filter(must=[FC(key="tag", match=M.MatchExcept(**{"except": ["red", "blue"]}))]), lambda p, i: p["tag"] not in ("red", "blue")),
        (M.Filter(must=[FC(key="n", range=M.Range(gt=5, lte=20))]), lambda p, i: 5 < i <= 20),
        (M.Filter(must=[FC(key="n", range=M.Range(gte=30.5))]), lambda p, i: i >= 30.5),
        (M.Filter(must=[M.IsEmptyCondition(is_empty=M.PayloadField(key="tags"))]), lambda p, i: not p["tags"]),
        (M.Filter(must=[M.IsEmptyCondition(is_empty=M.PayloadField(key="maybe"))]), lambda p, i: p.get("maybe") is None),
        (M.Filter(must=[M.HasIdCondition(has_id=[1, 5, 9, 99])]), lambda p, i: i in (1, 5, 9)),
        (
            M.Filter(
                should=[FC(key="tag", match=M.MatchValue(value="green")), FC(key="n", range=M.Range(lt=4))],
                must_not=[FC(key="metadata.page", match=M.MatchValue(value=0))],
            ),
            lambda p, i: (p["tag"] == "green" or i < 4) and p["metadata"]["page"] != 0,
        ),
        (
            M.Filter(must=[M.Filter(should=[FC(key="tag", match=M.MatchValue(value="red")), M.Filter(must=[FC(key="tags", match=M.MatchValue(value="b"))])])]),
            lambda p, i: p["tag"] == "red" or "b" in p["tags"],
        ),
        (M.Filter(should=[]), lambda p, i: True),
    ]
    for f, want in cases:
        expected = sorted(i for i, p in payloads.items() if want(p, i))
        points, _ = ctx.client.scroll(name, scroll_filter=f, limit=100, with_payload=False)
        assert sorted(p.id for p in points) == expected, (f, [p.id for p in points], expected)
        assert ctx.client.count(name, count_filter=f, exact=True).count == len(expected), f


@check()
def snapshots(ctx):
    name = ctx.name()
    ctx.client.create_collection(
        name, vectors_config=models.VectorParams(size=2, distance=models.Distance.DOT)
    )
    ctx.client.upsert(name, points=[models.PointStruct(id=1, vector=[1.0, 0.0])], wait=True)
    snap = ctx.client.create_snapshot(name, wait=True)
    assert snap.name.startswith(f"{name}-") and snap.name.endswith(".snapshot"), snap.name
    listed = [s.name for s in ctx.client.list_snapshots(name)]
    assert snap.name in listed, listed


@check()
def sparse_vectors(ctx):
    name = ctx.name()
    ctx.client.create_collection(
        name,
        vectors_config={},
        sparse_vectors_config={"s": models.SparseVectorParams(modifier=models.Modifier.IDF)},
    )
    params = ctx.client.get_collection(name).config.params
    assert set(params.sparse_vectors) == {"s"}
    assert params.sparse_vectors["s"].modifier == models.Modifier.IDF
    docs = {
        1: ([3, 1], [0.5, 1.0]),
        2: ([1, 2], [2.0, 1.0]),
        3: ([2, 4], [1.0, 1.0]),
        4: ([5], [1.0]),
    }
    ctx.client.upsert(
        name,
        points=[
            models.PointStruct(id=i, vector={"s": models.SparseVector(indices=ix, values=vs)}, payload={"group": i % 2})
            for i, (ix, vs) in docs.items()
        ],
        wait=True,
    )
    v = ctx.client.retrieve(name, ids=[1], with_vectors=True)[0].vector["s"]
    assert (v.indices, v.values) == ([1, 3], [1.0, 0.5]), v
    ctx.client.update_vectors(
        name, points=[models.PointVectors(id=4, vector={"s": models.SparseVector(indices=[1, 5], values=[1.0, 1.0])})], wait=True
    )
    docs[4] = ([1, 5], [1.0, 1.0])
    ctx.client.delete_vectors(name, vectors=["s"], points=[3], wait=True)
    assert ctx.client.retrieve(name, ids=[3], with_vectors=True)[0].vector == {}
    del docs[3]
    points, _ = ctx.client.scroll(name, limit=10, with_vectors=True)
    assert [p.id for p in points] == [1, 2, 3, 4]
    assert ctx.client.count(name, exact=True).count == 4

    # IDF over the points that have a non-empty vector (1, 2, 4).
    def idf(index, among):
        n = len(among)
        df = sum(1 for i in among if index in docs[i][0])
        return math.log((n - df + 0.5) / (df + 0.5) + 1)

    def score(q, i, among):
        stored = dict(zip(*docs[i]))
        return sum(w * idf(k, among) * stored[k] for k, w in q.items() if k in stored)

    q = {1: 1.0, 5: 2.0}
    all_ids = list(docs)
    expected = sorted(((i, score(q, i, all_ids)) for i in all_ids if set(q) & set(docs[i][0])), key=lambda t: (-t[1], t[0]))
    query = models.SparseVector(indices=list(q), values=list(q.values()))
    if ctx.args.mode == "legacy":
        hits = ctx.client.search(name, query_vector=models.NamedSparseVector(name="s", vector=query), limit=10)
        assert_ranked(hits, expected)
    else:
        assert_ranked(ctx.client.query_points(name, query=query, using="s", limit=10), expected)
        # IDF statistics over a corpus filter: the points with group 0.
        corpus = [i for i in all_ids if i % 2 == 0]
        expected = sorted(((i, score(q, i, corpus)) for i in all_ids if set(q) & set(docs[i][0])), key=lambda t: (-t[1], t[0]))
        hits = ctx.client.query_points(
            name,
            query=query,
            using="s",
            limit=10,
            search_params=models.SearchParams(
                idf=models.IdfCorpusParams(corpus=models.Filter(must=[models.FieldCondition(key="group", match=models.MatchValue(value=0))]))
            ),
        )
        assert_ranked(hits, expected)


@check()
def query_points_dense(ctx):
    name, vectors = dense_collection(ctx, distance="Cosine", n=30, dim=3)
    q = [0.3, -0.2, 0.9]
    expected = ranked(vectors, lambda v: nearest_score("Cosine", normalize(q), v), limit=5)
    assert_ranked(ctx.client.query_points(name, query=q, using="", limit=5), expected)


@check()
def recommend_discover_context_mmr_queries(ctx):
    """The gateway-scored query kinds (Ruling 10) through the query API, on
    both client versions (1.15.1 has the query API too)."""
    d = "Dot"
    name, vectors = dense_collection(ctx, distance=d, n=60, dim=4, seed=21)
    V = vectors
    cases = [
        (models.RecommendQuery(recommend=models.RecommendInput(positive=[1, 2], negative=[3], strategy=models.RecommendStrategy.BEST_SCORE)),
         lambda c: best_score(d, c, [V[1], V[2]], [V[3]]), (1, 2, 3)),
        # Negatives only: kept (owner ruling); every score is −sig(n).
        (models.RecommendQuery(recommend=models.RecommendInput(negative=[3], strategy=models.RecommendStrategy.BEST_SCORE)),
         lambda c: best_score(d, c, [], [V[3]]), (3,)),
        (models.RecommendQuery(recommend=models.RecommendInput(positive=[1], negative=[2, 3], strategy=models.RecommendStrategy.SUM_SCORES)),
         lambda c: sum_scores(d, c, [V[1]], [V[2], V[3]]), (1, 2, 3)),
        # Negatives only, accepted as in Qdrant (owner ruling on row T8-7).
        (models.RecommendQuery(recommend=models.RecommendInput(negative=[2, 3], strategy=models.RecommendStrategy.SUM_SCORES)),
         lambda c: sum_scores(d, c, [], [V[2], V[3]]), (2, 3)),
        (models.RecommendQuery(recommend=models.RecommendInput(positive=[[0.1, 0.2, 0.3, 0.4]], negative=[[0.4, 0.3, 0.2, 0.1]], strategy=models.RecommendStrategy.BEST_SCORE)),
         lambda c: best_score(d, c, [[0.1, 0.2, 0.3, 0.4]], [[0.4, 0.3, 0.2, 0.1]]), ()),
        (models.DiscoverQuery(discover=models.DiscoverInput(target=5, context=[models.ContextPair(positive=6, negative=7), models.ContextPair(positive=8, negative=9)])),
         lambda c: discover_score(d, c, V[5], [(V[6], V[7]), (V[8], V[9])]), (5, 6, 7, 8, 9)),
        (models.ContextQuery(context=[models.ContextPair(positive=10, negative=11), models.ContextPair(positive=12, negative=13)]),
         lambda c: context_score(d, c, [(V[10], V[11]), (V[12], V[13])]), (10, 11, 12, 13)),
    ]
    for query, score, exclude in cases:
        expected = ranked(V, score, exclude=exclude, limit=7, offset=2)
        assert_ranked(ctx.client.query_points(name, query=query, limit=7, offset=2), expected)
    # average_vector: nearest of avg(pos) + avg(pos) − avg(neg).
    avg = [2 * (a + b) / 2 - c for a, b, c in zip(V[1], V[2], V[3])]
    expected = ranked(V, lambda c: dot(avg, c), exclude=(1, 2, 3), limit=5)
    query = models.RecommendQuery(recommend=models.RecommendInput(positive=[1, 2], negative=[3]))
    assert_ranked(ctx.client.query_points(name, query=query, limit=5), expected)
    # An empty context scores every point 0 (owner ruling on row T8-7).
    hits = got(ctx.client.query_points(name, query=models.ContextQuery(context=[]), limit=4))
    assert len(hits) == 4 and all(s == 0.0 for _, s in hits), hits

    # MMR with diversity 0 on Euclid is the relevance order.
    e_name, e_vectors = dense_collection(ctx, distance="Euclid", n=40, dim=3, seed=22, suffix="_euclid")
    q = [0.1, 0.5, -0.3]
    expected = ranked(e_vectors, lambda v: nearest_score("Euclid", q, v), limit=6, ascending=True)
    mmr = models.NearestQuery(nearest=q, mmr=models.Mmr(diversity=0.0, candidates_limit=20))
    assert_ranked(ctx.client.query_points(e_name, query=mmr, limit=6, with_vectors=True), expected)
    diverse = got(ctx.client.query_points(e_name, query=models.NearestQuery(nearest=q, mmr=models.Mmr(diversity=0.9, candidates_limit=20)), limit=6))
    assert len(diverse) == 6 and diverse[0][0] == expected[0][0], diverse
    top20 = {i for i, _ in ranked(e_vectors, lambda v: nearest_score("Euclid", q, v), limit=20, ascending=True)}
    assert {i for i, _ in diverse} <= top20


@check()
def query_groups(ctx):
    def payload(i):
        return {"doc": f"d{i % 7}", "n": i}

    name, vectors = dense_collection(ctx, n=50, payload=payload)
    lookup = ctx.name("_lookup")
    ctx.client.create_collection(lookup, vectors_config=models.VectorParams(size=2, distance=models.Distance.DOT))
    q = [0.2, 0.4, -0.1, 0.3]
    result = ctx.client.query_points_groups(
        name,
        group_by="doc",
        query=q,
        limit=3,
        group_size=2,
        with_payload=True,
        with_lookup=models.WithLookup(collection=lookup, with_payload=False),
    )
    groups = result.groups
    order = ranked(vectors, lambda v: dot(q, v), limit=50)
    # Groups in the order of their best hits, each with its two best hits.
    expected = []
    for i, _ in order:
        key = f"d{i % 7}"
        if key not in [k for k, _ in expected]:
            expected.append((key, [j for j, _ in order if f"d{j % 7}" == key][:2]))
    expected = expected[:3]
    assert [(g.id, [h.id for h in g.hits]) for g in groups] == expected, groups
    assert all(h.payload["doc"] == g.id for g in groups for h in g.hits)
    # No key is a point id, so no lookups are attached.
    assert all(g.lookup is None for g in groups)


# ----- legacy (1.15.1) -----


@check("legacy")
def legacy_search(ctx):
    name, vectors = dense_collection(ctx, distance="Cosine", n=40, dim=3, payload=lambda i: {"n": i, "even": i % 2 == 0})
    q = [0.5, -0.4, 0.2]
    nq = normalize(q)
    expected = ranked(vectors, lambda v: nearest_score("Cosine", nq, v), limit=5, offset=2)
    assert_ranked(ctx.client.search(name, query_vector=q, limit=5, offset=2), expected)
    assert_ranked(ctx.client.search(name, query_vector=models.NamedVector(name="", vector=q), limit=5, offset=2), expected)
    even = {i: v for i, v in vectors.items() if i % 2 == 0}
    f = models.Filter(must=[models.FieldCondition(key="even", match=models.MatchValue(value=True))])
    expected = ranked(even, lambda v: nearest_score("Cosine", nq, v), limit=4)
    hits = ctx.client.search(name, query_vector=q, query_filter=f, limit=4, with_vectors=True, with_payload=True)
    assert_ranked(hits, expected)
    assert all(h.payload["even"] and len(h.vector) == 3 for h in hits)
    threshold = expected[2][1]
    kept = [t for t in ranked(even, lambda v: nearest_score("Cosine", nq, v), limit=40) if t[1] > threshold]
    assert_ranked(ctx.client.search(name, query_vector=q, query_filter=f, limit=10, score_threshold=threshold), kept)
    batch = ctx.client.search_batch(
        name,
        requests=[
            models.SearchRequest(vector=q, limit=3),
            models.SearchRequest(vector=models.NamedVector(name="", vector=[1.0, 0.0, 0.0]), limit=2, filter=f),
        ],
    )
    assert len(batch) == 2
    assert_ranked(batch[0], ranked(vectors, lambda v: nearest_score("Cosine", nq, v), limit=3))
    assert_ranked(batch[1], ranked(even, lambda v: v[0], limit=2))
    # The legacy query_points of 1.15.1 with `using=""`.
    assert_ranked(ctx.client.query_points(name, query=q, using="", limit=3), ranked(vectors, lambda v: nearest_score("Cosine", nq, v), limit=3))


@check("legacy")
def legacy_recommend(ctx):
    d = "Dot"
    name, V = dense_collection(ctx, distance=d, n=60, dim=4, seed=31)
    S = models.RecommendStrategy
    avg = [2 * (a + b) / 2 - c for a, b, c in zip(V[1], V[2], V[3])]
    cases = [
        (dict(positive=[1, 2], negative=[3]), lambda c: dot(avg, c), (1, 2, 3)),
        (dict(positive=[1, 2], negative=[3], strategy=S.AVERAGE_VECTOR), lambda c: dot(avg, c), (1, 2, 3)),
        (dict(positive=[1, 2], negative=[3], strategy=S.BEST_SCORE), lambda c: best_score(d, c, [V[1], V[2]], [V[3]]), (1, 2, 3)),
        (dict(negative=[3], strategy=S.BEST_SCORE), lambda c: best_score(d, c, [], [V[3]]), (3,)),
        (dict(positive=[1], negative=[2, 3], strategy=S.SUM_SCORES), lambda c: sum_scores(d, c, [V[1]], [V[2], V[3]]), (1, 2, 3)),
        (dict(negative=[2, 3], strategy=S.SUM_SCORES), lambda c: sum_scores(d, c, [], [V[2], V[3]]), (2, 3)),
        (dict(positive=[[0.1, 0.2, 0.3, 0.4]], negative=[4], strategy=S.BEST_SCORE), lambda c: best_score(d, c, [[0.1, 0.2, 0.3, 0.4]], [V[4]]), (4,)),
    ]
    for kwargs, score, exclude in cases:
        expected = ranked(V, score, exclude=exclude, limit=6, offset=1)
        assert_ranked(ctx.client.recommend(name, limit=6, offset=1, **kwargs), expected)
    batch = ctx.client.recommend_batch(
        name,
        requests=[
            models.RecommendRequest(positive=[1, 2], negative=[3], limit=3),
            models.RecommendRequest(positive=[1], negative=[2, 3], strategy=S.SUM_SCORES, limit=4),
        ],
    )
    assert_ranked(batch[0], ranked(V, lambda c: dot(avg, c), exclude=(1, 2, 3), limit=3))
    assert_ranked(batch[1], ranked(V, lambda c: sum_scores(d, c, [V[1]], [V[2], V[3]]), exclude=(1, 2, 3), limit=4))
    expect_error(lambda: ctx.client.recommend(name, negative=[3], limit=3), 400, grpc.StatusCode.INVALID_ARGUMENT)


@check("legacy")
def legacy_discover(ctx):
    d = "Dot"
    name, V = dense_collection(ctx, distance=d, n=60, dim=4, seed=41)
    # The legacy discover takes `ContextExamplePair`s.
    pairs = [models.ContextExamplePair(positive=6, negative=7), models.ContextExamplePair(positive=8, negative=9)]
    expected = ranked(V, lambda c: discover_score(d, c, V[5], [(V[6], V[7]), (V[8], V[9])]), exclude=(5, 6, 7, 8, 9), limit=5)
    assert_ranked(ctx.client.discover(name, target=5, context=pairs, limit=5), expected)
    expected_ctx = ranked(V, lambda c: context_score(d, c, [(V[6], V[7]), (V[8], V[9])]), exclude=(6, 7, 8, 9), limit=5)
    assert_ranked(ctx.client.discover(name, context=pairs, limit=5), expected_ctx)
    batch = ctx.client.discover_batch(
        name,
        requests=[
            models.DiscoverRequest(target=5, context=pairs, limit=5),
            models.DiscoverRequest(context=pairs, limit=5),
        ],
    )
    assert_ranked(batch[0], expected)
    assert_ranked(batch[1], expected_ctx)
    expect_error(lambda: ctx.client.discover(name, limit=3), 400, grpc.StatusCode.INVALID_ARGUMENT, "target and/or context_pairs")


@check("legacy")
def legacy_groups(ctx):
    def payload(i):
        return {"doc": i % 5, "n": i}

    name, V = dense_collection(ctx, n=40, payload=payload)
    lookup = ctx.name("_lookup")
    ctx.client.create_collection(lookup, vectors_config=models.VectorParams(size=2, distance=models.Distance.DOT))
    ctx.client.upsert(lookup, points=[models.PointStruct(id=k, vector=[1.0, 0.0], payload={"title": f"doc {k}"}) for k in range(5)], wait=True)
    q = [0.3, 0.1, -0.2, 0.4]
    order = ranked(V, lambda v: dot(q, v), limit=40)

    def expect(order, n_groups, size):
        out = []
        for i, _ in order:
            if i % 5 not in [k for k, _ in out]:
                out.append((i % 5, [j for j, _ in order if j % 5 == i % 5][:size]))
        return out[:n_groups]

    result = ctx.client.search_groups(name, query_vector=q, group_by="doc", limit=3, group_size=2, with_lookup=lookup)
    assert [(g.id, [h.id for h in g.hits]) for g in result.groups] == expect(order, 3, 2), result.groups
    assert all(g.lookup is not None and g.lookup.payload == {"title": f"doc {g.id}"} for g in result.groups)
    rec_order = ranked(V, lambda c: best_score("Dot", c, [V[1]], [V[2]]), exclude=(1, 2), limit=40)
    result = ctx.client.recommend_groups(name, group_by="doc", positive=[1], negative=[2], strategy=models.RecommendStrategy.BEST_SCORE, limit=2, group_size=3)
    assert [(g.id, [h.id for h in g.hits]) for g in result.groups] == expect(rec_order, 2, 3), result.groups


# ----- modern (1.19.1), over REST and gRPC -----


@check("modern")
def info_rest_equals_grpc(ctx):
    rest = client_for(ctx.args, False)
    over_grpc = client_for(ctx.args, True)
    try:
        assert rest.info() == over_grpc.info(), (rest.info(), over_grpc.info())
    finally:
        rest.close()
        over_grpc.close()


@check("modern")
def query_nearest(ctx):
    name = ctx.name()
    ctx.client.create_collection(
        name,
        vectors_config={
            "": models.VectorParams(size=3, distance=models.Distance.DOT),
            "e": models.VectorParams(size=3, distance=models.Distance.EUCLID),
        },
    )
    V = random_vectors(30, 3, 51)
    E = random_vectors(30, 3, 52)
    ctx.client.upsert(
        name,
        points=[models.PointStruct(id=i, vector={"": V[i], "e": E[i]}, payload={"n": i, "meta": {"a": i, "b": -i}}) for i in V],
        wait=True,
    )
    q = [0.2, -0.5, 0.7]
    assert_ranked(ctx.client.query_points(name, query=q, limit=5, offset=3), ranked(V, lambda v: dot(q, v), limit=5, offset=3))
    # By id: the stored vector, the id excluded.
    assert_ranked(ctx.client.query_points(name, query=4, limit=5), ranked(V, lambda v: dot(V[4], v), exclude=(4,), limit=5))
    # `using` a Euclid vector: distances, ascending; thresholds keep `<`.
    expected = ranked(E, lambda v: nearest_score("Euclid", q, v), limit=30, ascending=True)
    t = expected[6][1]
    assert_ranked(ctx.client.query_points(name, query=q, using="e", limit=10, score_threshold=t), [x for x in expected if x[1] < t][:10])
    f = models.Filter(must=[models.FieldCondition(key="n", range=models.Range(gte=20))])
    assert_ranked(
        ctx.client.query_points(name, query=q, query_filter=f, limit=4),
        ranked({i: v for i, v in V.items() if i >= 20}, lambda v: dot(q, v), limit=4),
    )
    t = ranked(V, lambda v: dot(q, v), limit=30)[4][1]
    assert_ranked(ctx.client.query_points(name, query=q, limit=10, score_threshold=t), [x for x in ranked(V, lambda v: dot(q, v), limit=30) if x[1] > t][:10])
    hits = ctx.client.query_points(name, query=q, limit=2, with_payload=models.PayloadSelectorInclude(include=["meta.a"]), with_vectors=["e"]).points
    assert all(h.payload == {"meta": {"a": h.id}} and set(h.vector) == {"e"} for h in hits), hits
    hits = ctx.client.query_points(name, query=q, limit=2, with_payload=models.PayloadSelectorExclude(exclude=["meta"]), with_vectors=True).points
    assert all(h.payload == {"n": h.id} and set(h.vector) == {"", "e"} for h in hits), hits


def rrf(lists, k=2):
    scores = {}
    for lst in lists:
        for pos, (i, _) in enumerate(lst):
            scores[i] = scores.get(i, 0.0) + 1.0 / (pos + k)
    return sorted(scores.items(), key=lambda t: (-t[1], t[0]))


def dbsf(lists):
    scores = {}
    for lst in lists:
        vals = [s for _, s in lst]
        n = len(vals)
        mean = sum(vals) / n
        sd = math.sqrt(sum((v - mean) ** 2 for v in vals) / (n - 1)) if n > 1 else 0.0
        lo, hi = mean - 3 * sd, mean + 3 * sd
        for i, s in lst:
            norm = 0.5 if hi == lo else (s - lo) / (hi - lo)
            scores[i] = scores.get(i, 0.0) + norm
    return sorted(scores.items(), key=lambda t: (-t[1], t[0]))


@check("modern")
def query_fusion_and_rescore(ctx):
    name = ctx.name()
    ctx.client.create_collection(
        name,
        vectors_config={
            "a": models.VectorParams(size=3, distance=models.Distance.DOT),
            "b": models.VectorParams(size=3, distance=models.Distance.DOT),
        },
        sparse_vectors_config={"s": models.SparseVectorParams()},
    )
    A = random_vectors(40, 3, 61)
    B = random_vectors(40, 3, 62)
    rng = random.Random(63)
    SP = {i: dict(zip(rng.sample(range(10), 3), [rng.uniform(0.1, 1.0) for _ in range(3)])) for i in A}
    ctx.client.upsert(
        name,
        points=[
            models.PointStruct(id=i, vector={"a": A[i], "b": B[i], "s": models.SparseVector(indices=list(SP[i]), values=list(SP[i].values()))})
            for i in A
        ],
        wait=True,
    )
    qa, qb = [0.3, 0.3, -0.6], [-0.2, 0.9, 0.1]
    qs = {1: 1.0, 4: 0.5, 7: 2.0}
    la = ranked(A, lambda v: dot(qa, v), limit=10)
    lb = ranked(B, lambda v: dot(qb, v), limit=10)
    ls = sorted(((i, sum(w * SP[i][k] for k, w in qs.items() if k in SP[i])) for i in A if set(qs) & set(SP[i])), key=lambda t: (-t[1], t[0]))[:10]
    dense_prefetch = [models.Prefetch(query=qa, using="a", limit=10), models.Prefetch(query=qb, using="b", limit=10)]
    hybrid_prefetch = [
        models.Prefetch(query=qa, using="a", limit=10),
        models.Prefetch(query=models.SparseVector(indices=list(qs), values=list(qs.values())), using="s", limit=10),
    ]
    for prefetch, lists in [(dense_prefetch, [la, lb]), (hybrid_prefetch, [la, ls])]:
        assert_ranked(ctx.client.query_points(name, prefetch=prefetch, query=models.FusionQuery(fusion=models.Fusion.RRF), limit=8), rrf(lists)[:8])
        assert_ranked(ctx.client.query_points(name, prefetch=prefetch, query=models.FusionQuery(fusion=models.Fusion.DBSF), limit=8), dbsf(lists)[:8], tol=1e-3)
    # Rescore: the prefetch's candidates, reordered by the root query.
    candidates = {i: B[i] for i, _ in ranked(A, lambda v: dot(qa, v), limit=15)}
    assert_ranked(
        ctx.client.query_points(name, prefetch=models.Prefetch(query=qa, using="a", limit=15), query=qb, using="b", limit=5),
        ranked(candidates, lambda v: dot(qb, v), limit=5),
    )


@check("modern")
def query_batch_and_groups(ctx):
    name, V = dense_collection(ctx, n=40, payload=lambda i: {"doc": f"d{i % 4}", "n": i})
    lookup = ctx.name("_lookup")
    ctx.client.create_collection(lookup, vectors_config=models.VectorParams(size=2, distance=models.Distance.DOT))
    ctx.client.upsert(lookup, points=[models.PointStruct(id=str(uuid.UUID(int=k)), vector=[1.0, 0.0], payload={"k": k}) for k in range(4)], wait=True)
    q1, q2 = [0.1, 0.2, 0.3, 0.4], [-0.4, 0.3, -0.2, 0.1]
    f = models.Filter(must=[models.FieldCondition(key="n", range=models.Range(lt=20))])
    results = ctx.client.query_batch_points(
        name,
        requests=[models.QueryRequest(query=q1, limit=3), models.QueryRequest(query=q2, filter=f, limit=4)],
    )
    assert len(results) == 2
    assert_ranked(results[0], ranked(V, lambda v: dot(q1, v), limit=3))
    assert_ranked(results[1], ranked({i: v for i, v in V.items() if i < 20}, lambda v: dot(q2, v), limit=4))
    result = ctx.client.query_points_groups(name, group_by="doc", query=q1, limit=2, group_size=3, with_lookup=lookup)
    order = ranked(V, lambda v: dot(q1, v), limit=40)
    expected = []
    for i, _ in order:
        key = f"d{i % 4}"
        if key not in [k for k, _ in expected]:
            expected.append((key, [j for j, _ in order if f"d{j % 4}" == key][:3]))
    assert [(g.id, [h.id for h in g.hits]) for g in result.groups] == expected[:2], result.groups
    # The group keys are not point ids: no lookups.
    assert all(g.lookup is None for g in result.groups)


@check("modern")
def legacy_routes_on_1_19(ctx):
    """1.19.1 dropped the legacy methods, but the 1.19.1 server (and the
    gateway, Ruling 1) still serves them: over REST (parsed with the 1.19.1
    models) and over the deprecated gRPC methods."""
    d = "Dot"
    name, V = dense_collection(ctx, distance=d, n=50, dim=4, seed=71, payload=lambda i: {"doc": i % 5})
    q = [0.4, -0.1, 0.2, 0.3]
    near = ranked(V, lambda v: dot(q, v), limit=5)
    best = ranked(V, lambda c: best_score(d, c, [V[1]], [V[2]]), exclude=(1, 2), limit=5)
    sums = ranked(V, lambda c: sum_scores(d, c, [], [V[2]]), exclude=(2,), limit=5)
    disc = ranked(V, lambda c: discover_score(d, c, V[3], [(V[4], V[5])]), exclude=(3, 4, 5), limit=5)
    # The two best groups by `doc`, each with its best hit.
    group_heads = []
    for i, _ in ranked(V, lambda v: dot(q, v), limit=50):
        if i % 5 not in [k for k, _ in group_heads]:
            group_heads.append((i % 5, i))
    group_heads = group_heads[:2]
    if not ctx.grpc:
        base = ctx.args.url.rstrip("/") + f"/collections/{name}/points"

        def post(route, body):
            r = httpx.post(f"{base}/{route}", json=body, timeout=30)
            assert r.status_code == 200, r.text
            return r.json()["result"]

        def parsed(result):
            return [models.ScoredPoint(**p) for p in result]

        assert_ranked(parsed(post("search", {"vector": q, "limit": 5})), near)
        assert_ranked(parsed(post("recommend", {"positive": [1], "negative": [2], "strategy": "best_score", "limit": 5})), best)
        assert_ranked(parsed(post("recommend", {"negative": [2], "strategy": "sum_scores", "limit": 5})), sums)
        assert_ranked(parsed(post("discover", {"target": 3, "context": [{"positive": 4, "negative": 5}], "limit": 5})), disc)
        batch = post("search/batch", {"searches": [{"vector": q, "limit": 5}, {"vector": q, "limit": 2}]})
        assert_ranked(parsed(batch[0]), near)
        assert_ranked(parsed(batch[1]), near[:2])
        groups = models.GroupsResult(**post("search/groups", {"vector": q, "group_by": "doc", "limit": 2, "group_size": 1}))
        assert [(g.id, g.hits[0].id) for g in groups.groups] == group_heads, groups
        return
    from qdrant_client import grpc as pb

    points = ctx.client.grpc_points

    def pid(n):
        return pb.PointId(num=n)

    def g(result):
        return [(p.id.num, p.score) for p in result]

    def check_grpc(actual, expected):
        assert [i for i, _ in actual] == [i for i, _ in expected], (actual, expected)
        for (_, a), (_, e) in zip(actual, expected):
            assert abs(a - e) <= 1e-4 * max(1.0, abs(e)), (a, e)

    check_grpc(g(points.Search(pb.SearchPoints(collection_name=name, vector=q, limit=5)).result), near)
    check_grpc(
        g(points.Recommend(pb.RecommendPoints(collection_name=name, positive=[pid(1)], negative=[pid(2)], strategy=pb.RecommendStrategy.BestScore, limit=5)).result),
        best,
    )
    check_grpc(
        g(points.Recommend(pb.RecommendPoints(collection_name=name, negative=[pid(2)], strategy=pb.RecommendStrategy.SumScores, limit=5)).result),
        sums,
    )
    target = pb.TargetVector(single=pb.VectorExample(id=pid(3)))
    pair = pb.ContextExamplePair(positive=pb.VectorExample(id=pid(4)), negative=pb.VectorExample(id=pid(5)))
    check_grpc(g(points.Discover(pb.DiscoverPoints(collection_name=name, target=target, context=[pair], limit=5)).result), disc)
    groups = points.SearchGroups(pb.SearchPointGroups(collection_name=name, vector=q, group_by="doc", limit=2, group_size=1)).result.groups
    heads = [(gr.id.unsigned_value, gr.hits[0].id.num) for gr in groups]
    assert heads == group_heads, heads


@check("modern")
def create_vector_name(ctx):
    """`create_vector_name` (1.19.1) is the Task 3 route `PUT
    /collections/{c}/vectors/{v}` and gRPC `Points/CreateVectorName`: its
    body is `{"dense": {size, distance, ...}}` or `{"sparse": {...}}`."""
    name = ctx.name()
    ctx.client.create_collection(name, vectors_config=models.VectorParams(size=2, distance=models.Distance.DOT))
    result = ctx.client.create_vector_name(
        name,
        vector_name="extra",
        vector_name_config=models.DenseVectorNameConfig(dense=models.DenseVectorConfig(size=3, distance=models.Distance.EUCLID)),
    )
    assert result.status == models.UpdateStatus.COMPLETED
    vectors = ctx.client.get_collection(name).config.params.vectors
    assert set(vectors) == {"", "extra"}, vectors
    assert (vectors["extra"].size, vectors["extra"].distance) == (3, models.Distance.EUCLID)
    ctx.client.upsert(name, points=[models.PointStruct(id=1, vector={"": [1.0, 0.0], "extra": [1.0, 2.0, 3.0]})], wait=True)
    hits = ctx.client.query_points(name, query=[1.0, 2.0, 2.0], using="extra", limit=1).points
    assert hits[0].id == 1 and abs(hits[0].score - 1.0) < 1e-5, hits
    expect_error(
        lambda: ctx.client.create_vector_name(
            name, vector_name="sp", vector_name_config=models.SparseVectorNameConfig(sparse=models.SparseVectorConfig())
        ),
        501,
        grpc.StatusCode.UNIMPLEMENTED,
        "Unsupported in Loams",
    )


# ----- driver -----


def main():
    # The legacy methods warn on every call.
    warnings.filterwarnings("ignore", category=DeprecationWarning)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["legacy", "modern"], required=True)
    parser.add_argument("--url", default="http://127.0.0.1:6333")
    parser.add_argument("--grpc-port", type=int, default=None)
    parser.add_argument("-k", dest="only", default=None, help="run the checks whose name contains this")
    args = parser.parse_args()
    if args.grpc_port is None:
        args.grpc_port = (urlparse(args.url).port or 6333) + 1
    client_version = package_version("qdrant-client")
    transports = [False, True] if args.mode == "modern" else [False]
    passed, failed = 0, []
    for name, fn, modes in CHECKS:
        if args.mode not in modes or (args.only and args.only not in name):
            continue
        for grpc_on in transports:
            label = f"{name} ({'grpc' if grpc_on else 'rest'})"
            ctx = Ctx(args, name, grpc_on)
            try:
                fn(ctx)
                passed += 1
                print(f"ok   {label}")
            except Exception:  # noqa: BLE001 - report every failure
                failed.append(label)
                print(f"FAIL {label}")
                traceback.print_exc()
            finally:
                ctx.close()
    if failed:
        print(f"qdrant-client {client_version} {args.mode}: {len(failed)} checks failed: {', '.join(failed)}")
        sys.exit(1)
    print(f"qdrant-client {client_version} {args.mode}: {passed} checks passed")


if __name__ == "__main__":
    main()
