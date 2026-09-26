# Operon

**One bucket, every index.**

Operon is an open-source, object-storage-native, multi-model data engine for AI applications. It combines **streams (Kafka), full-text search (Elasticsearch), vector search (Qdrant), graph (Neo4j) and analytics (ClickHouse)** in one Rust engine over open formats — Apache Iceberg, Lance and Tantivy — stored in *your* S3/GCS/Azure bucket, with stateless, independently scalable compute. Agent runs get **durable execution** through the [Resonate](https://github.com/resonatehq/resonate) protocol, in the same bucket.

> **Status: M0 foundation complete; M1 collections in progress.** Operon has no usable release yet: the metastore, the internal log, workers, links, the PK index and garbage collection are built and pass the M0 exit gates ([report](docs/plans/m0-exit-report.md)). M1.1 and M1.2 add collections (Lance + Tantivy under one manifest), the query engine, the native collection API and Flight SQL; the Qdrant and Elasticsearch surfaces follow in M1.4 and M1.5. The architecture is specified in [`docs/design`](docs/design/README.md) and the implementation plans are in [`docs/plans`](docs/plans/). Expect breaking changes everywhere.

## Why

A typical production AI app runs Kafka, Elasticsearch, Qdrant, Neo4j and ClickHouse side by side: five stateful clusters, four or five copies of the same data, connector pipelines between them, and retrieval logic glued together in application code. Operon replaces that with:

- **Object storage as the only source of truth.** Compute nodes hold only caches. Storage costs object-storage prices, with no 3× block-storage replication.
- **The log is the spine.** Every write lands in a stream. Tables, collections and graphs are materializations maintained by declarative *links*, with no connector zoo. Every write returns a *consistency token* you can use to read your own writes on any surface.
- **Hot tiers everywhere.** Open formats on S3 are cheap by default. Derived, rebuildable node-local structures make the hot data fast: HNSW for vectors, pinned splits for text, ClickHouse-style projections for Iceberg tables and in-RAM CSR for graphs.
- **Compatibility where it helps adoption.** Kafka wire protocol, an Elasticsearch REST subset, the Qdrant API, Bolt with a Cypher subset, and the ClickHouse HTTP interface. There's also a native hybrid-retrieval API that does vector + BM25 + filter + graph expansion + fusion in one planned query.
- **Durable agent runs.** The Resonate SDKs (TypeScript, Python, Rust, Go, Java) work against Operon unmodified: each step of an agent run is a durable promise, so a crashed run resumes where it stopped instead of repeating model calls.
- **Changes as streams.** Any keyed table or collection can expose its row-level changes as a changelog stream, readable by Kafka clients.

## Collections quick start

M1.2 serves collections through the native API and Flight SQL. Start a dev server (HTTP on `127.0.0.1:8080`, Flight SQL on `127.0.0.1:8082`, data in `.operon/`):

```sh
cargo run --release -p operon -- dev
```

Create a collection, write documents and run a hybrid query (every write answers a consistency token; reads are strong by default, so they see it at once):

```sh
curl -s localhost:8080/v1/namespaces/demo/collections -H 'content-type: application/json' -d '{
  "name": "kb",
  "schema": {
    "fields": [
      {"name": "body", "source_path": "body", "kind": {"text": {"analyzer": "standard", "positions": true}}, "indexed": true, "fast": false},
      {"name": "tenant", "source_path": "tenant", "kind": "keyword", "indexed": true, "fast": true}
    ],
    "vectors": [{"name": "embedding", "dim": 3, "distance": "cosine"}],
    "sparse_vectors": [], "dynamic": "ignore", "max_fields": 1000
  }
}'

curl -s localhost:8080/v1/namespaces/demo/collections/kb/documents -H 'content-type: application/json' -d '{"ops": [
  {"upsert": {"id": 1, "source": {"body": "refund policy", "tenant": "a"}, "vectors": {"embedding": [1.0, 0.0, 0.0]}}},
  {"upsert": {"id": 2, "source": {"body": "shipping times", "tenant": "a"}, "vectors": {"embedding": [0.9, 0.1, 0.0]}}},
  {"upsert": {"id": 3, "source": {"body": "refund window", "tenant": "b"}, "vectors": {"embedding": [0.0, 0.0, 1.0]}}}
]}'

curl -s localhost:8080/v1/namespaces/demo/query -H 'content-type: application/json' -d '{
  "from": "collections.kb",
  "retrieve": [
    {"vector": {"field": "embedding", "query": [1.0, 0.0, 0.0], "k": 10}},
    {"text": {"field": "body", "query": "refund", "k": 10}}
  ],
  "filter": {"term": {"tenant": "a"}},
  "fuse": {"method": "rrf", "k": 60},
  "limit": 3
}'

curl -s localhost:8080/v1/namespaces/demo/sql -H 'content-type: application/json' -d @- <<'JSON'
{"query": "SELECT _id, body, _score FROM rrf(vector_search('kb', [1.0, 0.0, 0.0], 'embedding', 10), text_search('kb', 'refund', 'body', 10)) LIMIT 3"}
JSON
```

Connect with Flight SQL, for example through the ADBC driver (`pip install adbc-driver-flightsql pyarrow`; the ADBC drivers are gated in M1.7):

```python
import adbc_driver_flightsql.dbapi as flight_sql

with flight_sql.connect(
    "grpc://127.0.0.1:8082",
    db_kwargs={"adbc.flight.sql.rpc.call_header.operon-namespace": "demo"},
) as conn, conn.cursor() as cur:
    cur.execute("SELECT _id, body FROM kb ORDER BY _id")
    print(cur.fetch_arrow_table())
```

Fetch a scan plan and read the pinned Lance version directly with pylance (`pip install pylance==12.0.0`, the release that matches Lance 12.0.0):

```python
import json, urllib.request
import lance

request = urllib.request.Request(
    "http://127.0.0.1:8080/v1/namespaces/demo/collections/kb/scan",
    data=b"{}", headers={"content-type": "application/json"},
)
plan = json.load(urllib.request.urlopen(request))
if plan["lance"] is not None:  # null until the first commit
    dataset = lance.dataset(plan["lance"]["uri"], version=plan["lance"]["version"])
    print(dataset.count_rows(), "live documents; tail:", plan["tail_records"], "records")
```

The plan's `pin` reads the writes the Lance version does not hold yet (the tail) through the native API or Flight SQL ([§17 §3.7](docs/design/17-ai-data-ecosystem.md)).

## Architecture at a glance

```
 Kafka │ ES REST │ Qdrant │ Bolt/Cypher │ ClickHouse HTTP │ Resonate │ native gRPC/REST/Flight SQL
                               │  gateway
            ┌──────────────────┴──────────────────┐
         log (WAL: standard │ express │ quorum)   query (DataFusion + hot tier)
            └──────────────────┬──────────────────┘
          object storage: log segments · Iceberg · Lance · Tantivy splits · graph sidecars · workflow state
            workers: links · indexing · compaction · GC      meta: embedded Raft
```

Start with the [pitch](docs/design/00-pitch.md) and the [architecture](docs/design/01-architecture.md).

## Roadmap

| Milestone | Scope |
|---|---|
| M0 | Foundation: metastore, object-store I/O, internal log, cache, workers, links |
| M1 | Collections: Elasticsearch and Qdrant surfaces, hybrid retrieval, vector hot tier |
| M2 | Graph: Cypher subset, Bolt, traversal, graph algorithms; durable execution (Resonate surface) |
| M3 | Streams: Kafka compatibility, `express` WAL, changelog streams |
| M4 | Analytics: Iceberg tables via Lakekeeper, ClickHouse HTTP, Iceberg hot tier, columnar stream segments, durable-execution search and execution graphs |
| M5 | Scale and reliability: `quorum` WAL, Kafka transactions, distributed execution, multi-tenancy at scale |

Details and exit gates: [docs/design/12-roadmap-testing-risks.md](docs/design/12-roadmap-testing-risks.md).

## Contributing

We welcome design feedback and contributions. See [CONTRIBUTING.md](CONTRIBUTING.md) and our [Code of Conduct](CODE_OF_CONDUCT.md). Report security issues as described in [SECURITY.md](SECURITY.md).

## License

Apache License 2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
