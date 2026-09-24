# Operon

**One bucket, every index.**

Operon is an open-source, object-storage-native, multi-model data engine for AI applications. It combines **streams (Kafka), full-text search (Elasticsearch), vector search (Qdrant), graph (Neo4j) and analytics (ClickHouse)** in one Rust engine over open formats — Apache Iceberg, Lance and Tantivy — stored in *your* S3/GCS/Azure bucket, with stateless, independently scalable compute. Agent runs get **durable execution** through the [Resonate](https://github.com/resonatehq/resonate) protocol, in the same bucket.

> **Status: M0 foundation complete.** Operon has no usable release yet: the metastore, the internal log, workers, links, the PK index and garbage collection are built and pass the M0 exit gates ([report](docs/plans/m0-exit-report.md)); the query surfaces start with M1. The architecture is specified in [`docs/design`](docs/design/README.md) and the implementation plans are in [`docs/plans`](docs/plans/). Expect breaking changes everywhere.

## Why

A typical production AI app runs Kafka, Elasticsearch, Qdrant, Neo4j and ClickHouse side by side: five stateful clusters, four or five copies of the same data, connector pipelines between them, and retrieval logic glued together in application code. Operon replaces that with:

- **Object storage as the only source of truth.** Compute nodes hold only caches. Storage costs object-storage prices, with no 3× block-storage replication.
- **The log is the spine.** Every write lands in a stream. Tables, collections and graphs are materializations maintained by declarative *links*, with no connector zoo. Every write returns a *consistency token* you can use to read your own writes on any surface.
- **Hot tiers everywhere.** Open formats on S3 are cheap by default. Derived, rebuildable node-local structures make the hot data fast: HNSW for vectors, pinned splits for text, ClickHouse-style projections for Iceberg tables and in-RAM CSR for graphs.
- **Compatibility where it helps adoption.** Kafka wire protocol, an Elasticsearch REST subset, the Qdrant API, Bolt with a Cypher subset, and the ClickHouse HTTP interface. There's also a native hybrid-retrieval API that does vector + BM25 + filter + graph expansion + fusion in one planned query.
- **Durable agent runs.** The Resonate SDKs (TypeScript, Python, Rust, Go, Java) work against Operon unmodified: each step of an agent run is a durable promise, so a crashed run resumes where it stopped instead of repeating model calls.
- **Changes as streams.** Any keyed table or collection can expose its row-level changes as a changelog stream, readable by Kafka clients.

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
