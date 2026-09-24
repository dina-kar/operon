# 00 — Pitch

> **Operon: one bucket, every index.**
> The open-source, S3-native data engine for AI apps — streams, search, vectors, graph, analytics and durable agent workflows over open formats in *your* bucket, with stateless compute.

Status: **Approved** · 2026-09-22

---

## 1. The problem

A production AI application today typically runs five stateful systems — six, once agent runs need a workflow engine — each with its own cluster, replicas, upgrade cadence, security model and on-call burden:

| Need | Typical system | What it holds |
|---|---|---|
| Event ingest, agent traces, CDC | **Kafka** | Raw events |
| Keyword / hybrid search | **Elasticsearch** | Copy #2 of documents |
| Semantic retrieval | **Qdrant** | Copy #3 (embeddings + payload) |
| Knowledge graph / GraphRAG / agent memory | **Neo4j** | Copy #4 (entities, relations) |
| Product analytics, evals, cost/usage dashboards | **ClickHouse** | Copy #5 |
| Durable agent runs: retries, long waits, human-in-the-loop, fan-out | **Temporal**, or a queue + cron + Postgres | Workflow state, in yet another database |

The consequences:

1. **Cost.** Each system replicates 2–3× on block storage (EBS gp3 ≈ $0.08/GB-month ⇒ ≈ $0.16–0.24 effective) versus S3 at ≈ $0.023. Clusters are sized for peak and run 24/7. Kafka additionally pays cross-AZ replication traffic on every byte.
2. **Copies and drift.** The same entity lives in four stores glued by connectors and CDC jobs. When they drift, agents retrieve stale or contradictory context — a correctness bug, not just an ops annoyance.
3. **Retrieval glue lives in app code.** A single "hybrid GraphRAG" retrieval is BM25 (ES) + ANN (Qdrant) + k-hop expansion (Neo4j) + fusion in Python across three network hops. Nothing can plan or optimize it as a whole.
4. **No cross-store consistency.** "I just wrote this memory; can the next agent step see it in search, vectors and graph?" has no answer in a five-store stack.
5. **Agent runs are not durable.** A crash in step 7 of a 10-step agent run repeats paid model calls or loses the run, unless a sixth system tracks workflow state.

## 2. What Operon is

A single Rust engine with **five first-class objects** — *streams, tables, collections, graphs, links* — over **open formats on object storage**:

| Replaces | Operon object | Durable format | Compatible surface |
|---|---|---|---|
| Kafka | Stream | Operon log segments on S3 | Kafka wire protocol |
| Elasticsearch | Collection (text) | Tantivy splits | ES REST subset (`_bulk`, `_search`, aggs) |
| Qdrant | Collection (vectors) | Lance | Qdrant REST + gRPC |
| Neo4j | Graph | CSR sidecars over tables/collections | Bolt + Cypher subset |
| ClickHouse | Table | **Apache Iceberg** (via Lakekeeper) | ClickHouse HTTP interface + dialect |
| Connectors/CDC glue | Link, changelog stream | — | Declarative DDL; changelogs readable as Kafka topics |
| Temporal / queue + cron for agent runs | Durable promises (a service, §14) | One document per workflow origin on S3 | **Resonate protocol** (TS, Python, Rust, Go, Java SDKs) |

Plus a **native API/SDK** where one request does vector + BM25 + filter + graph expansion + fusion as a single planned query.

Agent code uses the Resonate SDKs unmodified: every step of an agent run is a durable promise stored in the same bucket as the agent's memory, so a crashed run resumes where it stopped and a step's result can carry the consistency token of the memory it wrote.

## 3. Three core ideas

1. **Object storage is the only source of truth.** Every byte at rest lives in S3/GCS/Azure Blob in open formats (Iceberg, Lance, Tantivy, Parquet). Compute nodes are stateless; losing any node loses no data. Storage costs S3 prices; compute scales independently and to zero per namespace.
2. **The log is the spine.** Every write — a Kafka produce, an ES `_bulk`, a Qdrant upsert, a Cypher `CREATE`, a ClickHouse `INSERT` — lands in a stream first. Everything else is a *materialization* of the log, maintained by declared **links**. That kills the connector zoo and gives every write a **consistency token** usable in any read on any object.
3. **Hot tiers everywhere.** Durable tier = open format on S3; hot tier = derived, node-local, rebuildable acceleration (Qdrant-style HNSW for vectors, pinned splits for text, ClickHouse-style sorted projections for Iceberg tables, in-RAM CSR for graphs) + an in-memory **tail** of not-yet-indexed data. Cheap by default, fast where it matters, always fresh.

## 4. Why now

| Enabler | Status (2026) |
|---|---|
| S3 conditional writes (`If-None-Match`, `If-Match`) | GA since 2024; extended to CopyObject (2025). GCS generation preconditions, Azure ETags equivalent. |
| Low-latency object storage | S3 Express One Zone (single-digit ms, append support), GCS Rapid (zonal, appendable) |
| Rust data stack maturity | DataFusion 55, Lance 12, Tantivy 0.26, SlateDB 0.16, openraft, iceberg-rust 0.10, kafka-protocol 0.18, foyer |
| Iceberg as the lakehouse lingua franca | v3 (deletion vectors, variant, row lineage) shipping in Snowflake, Databricks, AWS |
| Durable execution as an open protocol | Resonate (Apache-2.0, 2025–26): formally specified distributed async/await, with a server that runs on nothing but a bucket |
| The best designs are closed | turbopuffer (closed), WarpStream (proprietary), Bufstream (acquired by CoreWeave), LanceDB Enterprise serving layer (closed), AutoMQ low-latency WAL (commercial-only), Kuzu (archived after Apple acquisition), Neon (public repo dormant after Databricks acquisition) |

The architecture has been **proven in production by closed products** (turbopuffer, WarpStream, ClickHouse Cloud's stateless compute). No open-source project combines it across models. That is the slot.

## 5. Positioning

**"The open-source turbopuffer + WarpStream + ClickHouse Cloud — in one engine, on open formats, with drop-in compatibility for your existing Kafka, Qdrant, Elasticsearch, Neo4j and ClickHouse clients, and durable agent workflows through the Resonate SDKs."**

Primary buyer: platform teams at companies running AI apps at scale who are paying for (and operating) 4–5 data systems. Primary user: application engineers building RAG, agents, GraphRAG and eval/observability pipelines.

## 6. Competitive landscape

| Competitor | License | Overlap | Gap Operon exploits |
|---|---|---|---|
| turbopuffer | Closed SaaS | Search + vector on S3 | Closed; no graph, streams, SQL analytics |
| LanceDB | OSS format; closed serving | Vector + FTS on S3 | Distributed serving/caching/indexing closed; no streams, graph |
| Milvus 3.0 | Apache-2.0 (Go/C++) | Lake-native vector DB | Vector-first; no graph, Kafka, ClickHouse surface |
| HelixDB | Apache-2.0 (since 2026-05) | Rust graph+vector on SlateDB/S3 | Own DSL; no SQL/analytics/Kafka/compat layers. **Closest OSS rival — watch closely or collaborate.** |
| AutoMQ | Apache-2.0 (Java) | Kafka on S3 | OSS edition only has S3 WAL (~500 ms p99); low-latency WAL commercial; streaming only |
| WarpStream / Bufstream | Proprietary | Kafka on S3 | Closed; streaming only |
| Elastic Serverless | Proprietary | Search on object storage | Closed, expensive, search only |
| ClickHouse Cloud | Proprietary (engine OSS) | Stateless analytics on S3 | SharedMergeTree/distributed cache closed; text index has no BM25 |
| Qdrant | Apache-2.0 | Vector | Local-disk architecture; vector only |
| Apache Fluss | Apache-2.0 (Java, incubating) | Streaming storage for the lakehouse: columnar Arrow log, primary-key tables with changelogs, tiering to Iceberg/Paimon/Lance | JVM + ZooKeeper, data on tablet-server disks with S3 as a tier; no search, vector serving or graph; Flink-centric |

## 7. What Operon is *not* (non-goals)

- **Not an OLTP database.** No multi-statement interactive transactions with millisecond commits over mutable rows. Keep a Postgres for application state; stream its CDC into Operon.
- **Not a full Elasticsearch/Neo4j/ClickHouse clone.** Compatibility is scoped by external conformance suites (client libraries, framework integrations), not by feature parity. No Kibana, Painless, APOC-at-large, or ClickHouse Native TCP in v1.
- **Not a stream processor.** Stateless transforms and mergeable aggregates in links, yes; windowed joins with checkpointed state, no. **RisingWave is the supported companion** (§09 §8): it reads Operon topics and changelog streams over the Kafka surface and writes results back as Iceberg tables through Lakekeeper or as topics. Arroyo and Flink work the same way.

## 8. Governance and business model (recommendation)

- **License:** Apache-2.0 for the entire engine, all gateways and the operator. Big-company adoption requires it; AGPL/BSL/SSPL dependencies are excluded (§11).
- **Governance:** start company-led, plan for a foundation (LF AI & Data — as Vortex did — or CNCF) once there are ≥3 corporate contributors.
- **Monetization (if a company forms):** managed cloud (the ClickHouse/Confluent model) — multi-region control plane, autoscaling, SSO/audit UI, support. **Do not** withhold reliability (quorum WAL) or performance (hot tiers) features from OSS; that is exactly the AutoMQ/LanceDB gap Operon wins on.

## 9. Launch demo

A GraphRAG agent stack (e.g., Graphiti or LightRAG + LangChain) running unmodified against **one `operon` binary and one bucket**, side-by-side with the usual docker-compose of Kafka + Elasticsearch + Qdrant + Neo4j + ClickHouse — same answers, one-fifth the moving parts, a fraction of the storage cost, and a consistency token proving read-your-writes across search, vectors and graph. The agent's run loop is a Resonate workflow: `kill -9` the agent mid-run and it resumes at the step it was on, without repeating model calls.
