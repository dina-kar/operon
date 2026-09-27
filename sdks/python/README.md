# operon-client

The Python client for Operon's native REST API: namespaces, streams, collections,
documents (dense and sparse vectors), hybrid search through a typed query builder,
SQL, consistency tokens, typed errors and retries. Sync (`Client`) and async
(`AsyncClient`) with the same methods.

`operon-client` is a working name and is not published yet (it is renamed with the
product after M1).

## Install

```sh
pip install operon-client               # REST only: depends on httpx and anyio
pip install "operon-client[flight]"     # + Arrow results over Flight SQL
pip install "operon-client[arrow]"      # + to_arrow() on results
pip install "operon-client[polars]"     # + to_polars() on results
```

Until it is published, install it from a checkout:

```sh
uv pip install -e sdks/python
```

Python 3.10 or newer. `import operon` loads none of the extras' packages.

## Quickstart

Start a server with `operon dev` (native REST on `http://127.0.0.1:8080`), then:

```python
import operon
from operon import Document, q, schema

with operon.Client("http://127.0.0.1:8080") as client:
    client.create_namespace("docs", exist_ok=True)
    ns = client.namespace("docs")

    ns.create_collection(
        "kb",
        operon.Schema(
            fields=[schema.text("body"), schema.keyword("tenant"), schema.i64("n")],
            vectors=[schema.vector("embedding", 3)],
        ),
    )
    kb = ns.collection("kb")
    written = kb.upsert(
        [
            Document(
                1, {"body": "refund policy", "tenant": "a", "n": 1}, {"embedding": [1.0, 0.0, 0.0]}
            ),
            Document(
                2, {"body": "shipping times", "tenant": "a", "n": 2}, {"embedding": [0.9, 0.1, 0.0]}
            ),
        ]
    )

    # Hybrid search: two retrievers fuse with RRF (k = 60) unless you call .fuse().
    response = (
        kb.search()
        .retrieve(
            q.vector("embedding", [1.0, 0.0, 0.0], k=50), q.text("refund", k=50, fields=["body"])
        )
        .filter(q.term("tenant", "a"))
        .limit(10)
        .execute()
    )
    for hit in response.hits:
        print(hit.id, hit.score, hit.source)

    print(kb.get([1, 2], consistency=written.token))
    print(ns.sql("SELECT tenant, count(*) AS n FROM kb GROUP BY tenant").to_dicts())
```

Every builder method returns a new builder, so a base query can be reused.
`q` has one constructor per query of the IR (`q.match`, `q.term`, `q.range_`,
`q.bool_`, …), the retrievers (`q.vector`, `q.text`, `q.sparse`, `q.fused`,
`q.rescore`), the fusions (`q.rrf`, `q.dbsf`, `q.weighted_sum`) and the sort keys
(`q.score_sort`, `q.field_sort`, `q.pk_sort`). Sparse vectors go in
`Document.sparse_vectors` as `operon.SparseVector(indices, values)`.

Vectors may be lists, tuples or anything with `.tolist()` (a numpy array). A NaN
or infinity is refused before anything is sent. Dates are aware `datetime`s (a
naive one is refused) or `date`s.

## Ids

A document id is an `int` in `0..2**64` (sent exactly, also above 2**53), a `str`,
or a `uuid.UUID`. A string that looks like a UUID stays a string id; pass
`uuid.UUID(...)` for a UUID id. `True` is not id `1`: a `bool` is refused.

## Consistency

Reads are strong by default. Pass `consistency=` to `get`, `query`, `sql` and
`search().consistency(...)`:

- `"strong"` (the default): sees every acknowledged write;
- `"eventual"`: may lag, answers faster;
- a `ConsistencyToken` (or its text, `"v1:s7/p3@918274"`): sees at least the
  writes that token covers. Every write returns one (`result.token`); merge tokens
  with `a.merge(b)`;
- a `Pin` (a scan plan's `plan.pin`): exactly one state, see "Scan plans".

## Retries

A request that fails with 503 or a connection error is retried with capped,
jittered exponential backoff (0.1 s base, x2, 2 s cap, 3 retries); a `Retry-After`
header is honoured up to 30 s. Collection writes are keyed upserts, patches and
deletes, so they are retried too: applying one twice gives the same state.

**Stream produce is never retried** once the request may have reached the
server: a 503 on produce may already be committed, and a blind retry would
duplicate records. If you retry produce yourself, dedupe by key.

## Errors

Every error is an `operon.OperonError` with `code`, `status`, `message` and the
JSON `body`. Subclasses: `InvalidArgumentError` (and its `SchemaViolationError`),
`NotFoundError`, `AlreadyExistsError` (`id` on namespace and stream conflicts),
`ConflictError`, `OffsetOutOfRangeError`, `ResourceExhaustedError`
(`retry_after_ms`), `UnavailableError`, `OperonTimeoutError`, `InternalError`, and
`TransportError` for failures before a response (the httpx error is `__cause__`).

## Sync and async

`AsyncClient`, `AsyncNamespace`, `AsyncCollection` and `AsyncSearchBuilder` have
the same methods as their sync twins; each method that makes a request is
`async def`:

```python
async with operon.AsyncClient() as client:
    kb = client.namespace("docs").collection("kb")
    response = await kb.search().retrieve(q.text("refund", k=10)).execute()
```

## Scan plans

A scan plan (D53) resolves a collection into what an external reader needs to
read one state of it directly: the Lance dataset URI and version, its fragments
with their data and deletion files, and the columns.

```python
plan = kb.scan_plan()  # or at=<manifest version> or at=<token>
if plan.tail:
    # The Lance version lacks recent writes; read them through the pin instead.
    print(plan.raw["tail_records"], "records not yet in Lance")
rows = ns.sql("SELECT count(*) FROM kb", consistency=plan.pin)  # the same state, tail included
time_left_ms = (plan.expires_at_ms or 0) - plan.planned_at_ms  # how long the plan is retained
```

`tail` says the requested state holds writes the Lance version does not yet
have; `consistency=plan.pin` reads exactly the planned state, tail included, over
REST or Flight SQL. `expires_at_ms - planned_at_ms` is how long the plan's
manifest stays retained (both on the server's metastore clock). A plan carries no
credentials: in M1 a reader brings its own access to the object store.

With pylance (the release matching the server's Lance, 12.0.x):

```python
import lance

dataset = lance.dataset(plan.lance.uri, version=plan.lance.version)
print(dataset.count_rows())  # == plan.live_rows
```

The SDK does not open Lance itself. Ray, Polars and torch readers built on scan
plans arrive in M2.
