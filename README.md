# Loam

**One bucket, every index: hybrid retrieval, reactive data and durable agent runs on object storage.**

Loam is an open-source, AI-native data platform built in Rust. It stores everything in *your* object-storage bucket (RustFS, S3, GCS, Azure Blob or a local directory) in open formats: Lance, Tantivy and Apache Iceberg. Compute is stateless and holds only caches, so it scales independently and can be replaced at any time.

> **Early and moving fast.** Loam has no stable release yet, and APIs, formats and flags change without notice. The code still uses the working name **Operon**: the crates are `operon-*` and the binary is `operon`. They will be renamed to `loamdb` in one pass. The design is public in [`docs/design`](docs/design/README.md).

## Why Loam

A typical production AI application runs Elasticsearch for keyword search, Qdrant for vectors, Neo4j for the knowledge graph, and Kafka for events, and often a workflow engine for agent runs too. That means four or five stateful clusters, several copies of the same data, connector pipelines between them, and retrieval logic glued together in application code across three network hops.

Loam replaces that stack with one engine on one bucket:

- **Object storage is the only source of truth.** Data at rest lives in open formats in your bucket. You pay object-storage prices, with no 3× block-storage replication, and losing a node loses no data.
- **The log is the spine.** Every write, whether native, Qdrant, Elasticsearch or Flight SQL, lands in a log first. Collections are materializations of that log, maintained by declarative *links*, so there is no connector zoo. Every write returns a **consistency token** that any later read can use to see it.
- **Hybrid retrieval as one planned query.** Dense vectors, BM25 full text and filters, fused with reciprocal rank fusion in one DataFusion plan. Graph expansion for GraphRAG is next.
- **Hot tiers where it matters.** Node-local, rebuildable acceleration in RAM and NVMe: HNSW graphs for vectors and pinned Tantivy splits for text, over a cheap durable tier on the bucket.
- **Drop-in where it helps adoption.** Existing Qdrant and Elasticsearch clients, and the LangChain and LlamaIndex integrations built on them, work unmodified against the subset Loam implements.
- **More than retrieval.** *Loam Live* is a reactive document database on TiKV. *Loam Durable* embeds a [Resonate](https://github.com/resonatehq/resonate) server, so agent runs survive crashes without repeating model calls.

Read the [pitch](docs/design/00-pitch.md) and the [architecture](docs/design/01-architecture.md) for the full reasoning.

## Architecture

<p align="center">
  <img src="docs/assets/architecture.svg" alt="Loam architecture. Clients and protocols reach a stateless gateway and router. Behind it sit the retrieval engine, Loam Live, Loam Durable, the runtime and jobs, and Loam Postgres. TiKV holds metadata, transactions and the hot WAL. Object storage holds all data in open formats. Dotted outlines are in progress; dashed outlines are planned." width="100%">
</p>

- **Clients and protocols.** The native REST API, Arrow Flight SQL, and the Qdrant and Elasticsearch APIs, with Postgres and Kafka wire protocols to come.
- **Gateway and router.** A stateless layer that speaks every protocol, issues consistency tokens and routes each request to the node that owns the data.
- **Compute.** The retrieval engine, which covers the log, links, collections, the query engine, the hot tier and background workers. Beside it run Loam Live, Loam Durable and, later, a CPU-time functions runtime, jobs and Loam Postgres.
- **TiKV.** Metadata (the catalog, leases and the timestamp oracle), Live's transactions, and a hot WAL. Single-node deployments use an embedded Raft metastore instead.
- **Object storage.** Log segments, Lance datasets, Tantivy splits, manifests and hot-tier artifacts. Iceberg tables and Postgres pages come later.

## Features

Statuses reflect the `main` branch. **Available** means it is built and tested in CI, not that it is production ready.

| Feature | What you get | Status |
|---|---|---|
| Collections | Documents with a schema, stored as Lance (vectors, columns) plus Tantivy (full text) under one manifest; upserts and deletes by id | Available |
| Hybrid retrieval | Vector kNN, BM25 and filters fused with RRF in one query; SQL table functions `vector_search`, `text_search` and `rrf` | Available |
| Native REST API | Namespaces, streams, collections, links, hybrid query and SQL | Available |
| Arrow Flight SQL | SQL and Arrow results for any language with an ADBC or Flight SQL driver | Available |
| Qdrant API | Qdrant REST and gRPC, tested with `qdrant-client` 1.15 and 1.19 | Available |
| Elasticsearch subset | Document APIs, `_bulk`, `_search` with the core Query DSL, `knn`, hybrid and RRF, tested with `elasticsearch-py` 8.19 | Available |
| Streams and links | Partitioned streams with a native produce and fetch API; links apply streams to targets exactly once | Available |
| Hot tier | HNSW vector artifacts and pinned text splits in RAM and NVMe, with delta indexes for fresh writes | Available |
| Pinned scans | Read a collection's pinned Lance version directly with pylance, Ray, Polars or PyTorch | Available |
| Cluster mode | `operon cluster` with roles, rendezvous placement, read forwarding and write backpressure | Available |
| TiKV metastore | The catalog, leases and timestamp oracle on TiKV (`--meta tikv://…`, cargo feature `tikv`) | Available (opt-in) |
| Loam Durable | An embedded Resonate server: durable promises and tasks on SQLite (cargo feature `durable`) | Available (opt-in) |
| Durable store on TiKV | Durable execution state on TiKV for clusters | In progress |
| Loam Live | A reactive document database on TiKV: transactions, indexes, live queries and a TypeScript SDK | In progress |
| Postgres and MySQL wire | SQL over the Postgres and MySQL wire protocols | In progress |
| Streaming gRPC API | Idempotent produce and streaming subscribe over gRPC | In progress |
| Web console | The Loam console and its design system, built against a mock of the console API | In progress |
| Graph expansion | GraphRAG expansion of 1 to 2 hops, planned together with the retrieval query | Planned |
| Iceberg analytics | Iceberg tables through a REST catalog, readable by DuckDB, Spark, Trino and ClickHouse | Planned |
| Kafka wire and OTLP | Kafka clients and OpenTelemetry logs ingest into streams | Planned |
| Auth and tenancy | API keys, authorization, tenant quotas and a namespace router | Planned |
| Python SDK and MCP | A native Python client, `to_arrow()` and `to_polars()`, and an MCP server | Planned |
| Loam Functions | A CPU-time serverless runtime on workerd, wasmtime and gVisor, with a Rust Dapr-style API | Planned |
| Loam Jobs | Celery, BullMQ v6, PySpark (through Sail) and Flink SQL (through RisingWave) jobs run durably on Loam | Planned |
| Loam Postgres | A fork of Neon whose WAL lives on TiKV and the bucket | Planned |
| Self-hosting with GitOps | A Helm chart, a Kubernetes operator and Argo CD layouts | Planned |

## Quick start

### Prerequisites

- **Rust.** Install [rustup](https://rustup.rs); the toolchain pinned in [`rust-toolchain.toml`](rust-toolchain.toml) installs on first build.
- **protoc**, the Protocol Buffers compiler: `apt install protobuf-compiler`, `brew install protobuf`, or `pacman -S protobuf`.
- **Memory.** The first build compiles DataFusion, Lance and Tantivy, so it is heavy. On machines with less than 32 GB of RAM, limit parallel jobs with `-j 4`.

### Run a dev server

`operon dev` runs everything in one process, with data in a local directory (`.operon/` by default):

```sh
cargo run --release -p operon -- dev
```

It serves these listeners, each of which you can move or turn off (`operon dev --help`):

| Surface | Default address | Flag |
|---|---|---|
| Native HTTP API | `127.0.0.1:8080` | `--listen` |
| Arrow Flight SQL | `127.0.0.1:8082` | `--flight-sql-listen`, `--no-flight-sql` |
| Qdrant REST and gRPC | `127.0.0.1:6333`, `127.0.0.1:6334` | `--qdrant-listen`, `--qdrant-grpc-listen`, `--no-qdrant` |
| Elasticsearch REST | `127.0.0.1:9200` | `--es-listen`, `--no-es` |
| Resonate (with `--features durable`) | `127.0.0.1:8001` | `--durable-listen`, `--no-durable` |

To run against a bucket instead of a local directory, use `operon standalone --bucket s3://bucket/prefix`. For a multi-node deployment, use `operon cluster`.

### Hybrid search with the native API

Create a collection, write documents and run a hybrid query. Every write returns a consistency token, and reads are strong by default, so they see the write at once.

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
```

The same query in SQL:

```sh
curl -s localhost:8080/v1/namespaces/demo/sql -H 'content-type: application/json' -d @- <<'JSON'
{"query": "SELECT _id, body, _score FROM rrf(vector_search('kb', [1.0, 0.0, 0.0], 'embedding', 10), text_search('kb', 'refund', 'body', 10)) LIMIT 3"}
JSON
```

### Use an existing Qdrant client

```python
# pip install qdrant-client
from qdrant_client import QdrantClient, models

client = QdrantClient(url="http://localhost:6333")
client.create_collection("docs", vectors_config=models.VectorParams(size=3, distance=models.Distance.COSINE))
client.upsert("docs", points=[models.PointStruct(id=1, vector=[1.0, 0.0, 0.0], payload={"title": "refunds"})], wait=True)
print(client.query_points("docs", query=[1.0, 0.0, 0.0], limit=1))
```

Requests without an `Operon-Namespace` header go to the `default` namespace. The Elasticsearch API on port 9200 works the same way with `elasticsearch-py`.

### Query over Flight SQL

```python
# pip install adbc-driver-flightsql pyarrow
import adbc_driver_flightsql.dbapi as flight_sql

with flight_sql.connect(
    "grpc://127.0.0.1:8082",
    db_kwargs={"adbc.flight.sql.rpc.call_header.operon-namespace": "demo"},
) as conn, conn.cursor() as cur:
    cur.execute("SELECT _id, body FROM kb ORDER BY _id")
    print(cur.fetch_arrow_table())
```

### Read the bucket directly

`POST /v1/namespaces/{ns}/collections/{c}/scan` returns a pinned Lance version, which pylance reads straight from the bucket (`pip install pylance==12.0.0`):

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

Writes that the pinned version does not hold yet (the plan's *tail*) are readable through the native API or Flight SQL.

## API surfaces

| Surface | Protocol | Use it for | Code |
|---|---|---|---|
| Native API | REST (JSON) | Namespaces, streams, collections, links, hybrid query, SQL, scan plans | [`crates/operon/src/api`](crates/operon/src/api) |
| Flight SQL | Arrow Flight over gRPC | SQL from any ADBC or Flight SQL client, with Arrow results | [`crates/operon`](crates/operon) |
| Qdrant | REST and gRPC | Existing Qdrant clients and framework integrations | [`crates/operon-qdrant`](crates/operon-qdrant) |
| Elasticsearch subset | REST (JSON, NDJSON) | Existing Elasticsearch 8 clients, LangChain and LlamaIndex stores | [`crates/operon-es`](crates/operon-es) |
| Resonate | HTTP | Durable promises and tasks from the Resonate SDKs (TypeScript, Python, Rust, Go, Java) | [`crates/operon-durable`](crates/operon-durable) |
| Loam Live | Connect / gRPC (`loam.live.v1`) | Reactive documents and live queries (in progress) | [`proto/loam`](proto/loam), [`sdks/live-typescript`](sdks/live-typescript) |
| Console API | REST (OpenAPI) | The web console (in progress, served by a mock for now) | [`api/console`](api/console) |

Each gateway documents where it differs from the original in its crate's module docs.

## Project layout

| Crate | What it does |
|---|---|
| [`operon`](crates/operon) | The server binary: the native HTTP API, Flight SQL, the gateways, and the `dev`, `standalone` and `cluster` commands |
| [`operon-common`](crates/operon-common) | Identifier and schema types shared by all crates |
| [`operon-store`](crates/operon-store) | Object-storage access: conditional writes, range reads and fault injection |
| [`operon-cache`](crates/operon-cache) | Read-through RAM + NVMe byte-range cache over immutable objects |
| [`operon-meta`](crates/operon-meta) | The embedded metastore on Raft: namespaces, streams, the sequencer, leases and pointers |
| [`operon-meta-tikv`](crates/operon-meta-tikv) | The metastore on TiKV |
| [`operon-meta-conformance`](crates/operon-meta-conformance) | A backend-agnostic conformance suite for the metastore (test only) |
| [`operon-tikv`](crates/operon-tikv) | The TiKV client layer: transactions, the tuple codec, the timestamp oracle and keyspace bootstrap |
| [`operon-log`](crates/operon-log) | The internal log: WAL objects, segments, the write and fetch paths, and retention |
| [`operon-worker`](crates/operon-worker) | Lease-fenced background tasks with priorities and fair share |
| [`operon-link`](crates/operon-link) | Links: exactly-once apply of streams into targets |
| [`operon-pk`](crates/operon-pk) | The primary-key index on SlateDB |
| [`operon-collection`](crates/operon-collection) | Collections: documents, the catalog, and Lance + Tantivy storage under one manifest |
| [`operon-text`](crates/operon-text) | Tantivy integration: analyzers, splits on object storage and delete bitmaps |
| [`operon-quickwit`](crates/operon-quickwit) | Quickwit's split, directory, query and merge-policy code, vendored and adapted |
| [`operon-query`](crates/operon-query) | The read side: the search IR, hybrid query planning, SQL and the tail |
| [`operon-hnsw`](crates/operon-hnsw) | HNSW index traits, an exact flat engine and the qdrant-edge engine |
| [`operon-hot`](crates/operon-hot) | The hot tier: HNSW artifacts, pinned splits, budgets, placement and read forwarding |
| [`operon-qdrant`](crates/operon-qdrant) | The Qdrant-compatible REST and gRPC gateway |
| [`operon-es`](crates/operon-es) | The Elasticsearch-compatible REST gateway |
| [`operon-durable`](crates/operon-durable) | Loam Durable: the Resonate server embedded in process |
| [`operon-live`](crates/operon-live) | Loam Live: the reactive document database on TiKV |
| [`operon-live-proto`](crates/operon-live-proto) | Loam Live's `loam.live.v1` protos and generated service code |
| [`operon-sim`](crates/operon-sim) | Seeded cluster simulation and a linearizability checker (test only) |
| [`operon-console-mock`](crates/operon-console-mock) | A mock of the console API with seed data |

Other directories:

| Path | Contents |
|---|---|
| [`docs/design`](docs/design/README.md) | The design documents and the decision log |
| [`docs/plans`](docs/plans/README.md) | Implementation plans, task by task |
| [`web`](web/README.md) | The web console and the `@loam/ui` design system |
| [`sdks`](sdks) | Client SDKs (the Loam Live TypeScript SDK) |
| [`proto`](proto), [`api`](api) | Protobuf and OpenAPI contracts |
| [`conformance`](conformance) | External client conformance suites |
| [`deploy`](deploy) | Local compose files for TiKV and companion services |
| [`scripts`](scripts) | Development and CI helper scripts |

## Roadmap

In order: hybrid retrieval hardened for production (auth, quotas, telemetry, multi-node clusters, the Kubernetes operator), then graph expansion, streams with Kafka compatibility, and Iceberg analytics. Loam Live, Loam Durable, the functions runtime, jobs and Loam Postgres advance on their own tracks. The details, with exit criteria for each stage, are in [the roadmap](docs/design/12-roadmap-testing-risks.md) and [the implementation plans](docs/plans/README.md). Architectural decisions are recorded in [the decision log](docs/design/13-decision-log.md).

## Contributing

Contributions of every size are welcome: bug reports, compatibility reports from your Qdrant or Elasticsearch client, docs fixes, tests and code.

- Read [CONTRIBUTING.md](CONTRIBUTING.md) for the dev setup, tests and the PR flow.
- Look for issues labelled [`good first issue`](../../issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22) or [`help wanted`](../../issues?q=is%3Aissue+is%3Aopen+label%3A%22help+wanted%22).
- For anything that changes a format, a protocol or a design decision, open an issue first.

## Community

- **Questions, bugs and ideas:** [GitHub issues](../../issues).
- **Security issues:** please report them privately, as described in [SECURITY.md](SECURITY.md).
- **Conduct:** everyone who takes part agrees to the [Code of Conduct](CODE_OF_CONDUCT.md).
- **Governance and maintainers:** [GOVERNANCE.md](GOVERNANCE.md) and [MAINTAINERS.md](MAINTAINERS.md).

## Built on

Loam builds on great open-source work, including [DataFusion](https://datafusion.apache.org), [Lance](https://github.com/lancedb/lance), [Tantivy](https://github.com/quickwit-oss/tantivy), [Quickwit](https://github.com/quickwit-oss/quickwit), [qdrant-edge](https://github.com/qdrant/qdrant), [SlateDB](https://slatedb.io), [openraft](https://github.com/databendlabs/openraft), [TiKV](https://tikv.org) and [Resonate](https://github.com/resonatehq/resonate). Where Loam needs patches before upstream ships them, it uses pinned forks under [ostrium-labs](https://github.com/ostrium-labs): [resonate](https://github.com/ostrium-labs/resonate), [client-rust](https://github.com/ostrium-labs/client-rust), [neon](https://github.com/ostrium-labs/neon) and [sqlx](https://github.com/ostrium-labs/sqlx). Attributions are in [NOTICE](NOTICE).

## License

Loam is licensed under the [Apache License 2.0](LICENSE). See [NOTICE](NOTICE) for third-party attributions.

**Open core.** Everything you need to self-host Loam for a single organisation is open source in this repository. Only what is needed to run Loam as a multi-tenant paid cloud (billing, metering and the hosted control plane) lives in a separate, proprietary platform. The boundary is spelled out in [docs/open-core.md](docs/open-core.md).
