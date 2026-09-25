# 12 — Roadmap, Testing & Risks

Status: **Approved** · 2026-09-22

Build order follows the pain point: **search + vector first (ES + Qdrant), then graph (Neo4j), then Kafka, then analytics (ClickHouse/Iceberg), then scale/reliability hardening.** The internal log exists from M0 because everything depends on it.

---

## 1. Milestones

| M | Name | Scope | Exit gates |
|---|---|---|---|
| **M0** | Foundation | `operon dev/standalone`; meta on openraft; `object_store` abstraction with fault-injection wrapper; internal stream engine (`standard` class, leaderless sequencing, segmenter); foyer cache (H0/H1); worker leases + task framework; link framework (exactly-once apply); PkIndex on SlateDB; GC; DST harness | kill -9 at every step of write/commit/segment paths → no acknowledged-data loss, no torn state; object-store PUT/GET/412/409 fault matrix passes; linearizability check on sequencer & manifest-pointer CAS |
| **M1** | Collections (ES + Qdrant) | Lance + Tantivy splits under one manifest; upserts/deletes; tail indexes; native hybrid API + Python/TS SDK; Arrow Flight SQL; Qdrant API Phase A with sparse vectors (exact, IDF; pulled from Phase B 2026-09-25); ES API Phase A; hot tier: pinned splits + Qdrant-derived HNSW artifacts; affinity routing | LangChain + LlamaIndex vector-store tests (ES and Qdrant backends, sparse and hybrid tests included) pass unmodified; BEIR nDCG@10 within 1 pt of ES BM25; Recall@10 within 1% of Qdrant at equal hot-tier latency; results identical with hot tier on/off |
| **M2** | Graph (Neo4j) + durable execution | Mapped + native graphs; ID maps; CSR/CSC sidecars; edge overlay; Expand/VarExpand/ShortestPath; Cypher Phase A; Bolt 5; Neo4j procedure shims; `leiden`/`pagerank`/`wcc` table functions. **Resonate surface Phase A** (§14): forked Resonate gateway + blob server on `operon-store`, namespace auth, origin-affinity routing | Graphiti, LightRAG, LangChain `Neo4jGraph`, LlamaIndex property-graph tests pass unmodified; LDBC SNB IS1–IS7 on SF1; openCypher TCK pass-rate tracked; Resonate TS + Python SDK suites pass unmodified; Resonate differential/linearizability harness passes against 3 gateways over one bucket with fault injection |
| **M3** | Streams (Kafka) | Kafka gateway: produce/fetch/metadata/admin, consumer groups (classic + KIP-848), idempotent producers, compacted topics, SASL/mTLS/ACLs; `express` WAL class; zone-aware routing; **changelog streams** from keyed collections/tables (`upsert` and `full` modes, fenced appends) | librdkafka, Java, franz-go, kcat, Kafka Connect, Flink (at-least-once), **RisingWave** (Kafka source in plain, `UPSERT` and `DEBEZIUM` formats over Operon topics and changelog streams) client matrix; OpenMessaging Benchmark vs AutoMQ OSS/Kafka; Jepsen-style tests (no lost acknowledged writes, no reordering, no duplicates with idempotence) across node and AZ kills |
| **M4** | Analytics (ClickHouse + Iceberg) | Lakekeeper integration; Iceberg writes incl. DV writer; keyed tables; stream→table links; MVs with mergeable states; ClickHouse HTTP + dialect + function-compat; Iceberg hot tier T0–T3; **`arrow` segment encoding** for table implicit streams; **Resonate Phase B** (§14: change stream, `system.durable_*` tables, execution graph, cluster-wide timer shards) | ClickBench (hot) median within 2–3× ClickHouse OSS; TPC-H SF100 completes; clickhouse-connect + Grafana over HTTP work; Spark/Trino/DuckDB read Operon tables via Lakekeeper; RisingWave's Iceberg sink (append and upsert, REST catalog) writes tables that Operon serves correctly, including equality deletes |
| **M5** | Scale & reliability | `quorum` WAL (Raft journals); Kafka transactions + read_committed; datafusion-distributed; meta sharding (multi-Raft); multi-tenancy hardening (quotas, fair share, 1M namespaces); K8s operator; DR restore; OpenFGA authZ | Failover RTO < 5 s (quorum) with RPO 0; chaos suite green for 72 h; 1M-namespace test; tenant-isolation tests; restore-from-bucket drill |
| **W** | Agent workspaces (§15), parallel track | W0 (with M1): MCP server. W1 (after M2): Git on the bucket with O(1) forks, code-index links, credential vending, agent telemetry. W2: `operon-sandbox` with copy-on-write environment images, package caches, registry proxy; the 100-agent fleet demo (§16), staged as α after M2 + W1 and β after M4 + W2. W3 (Phase C): `jj` backend, REAPI, VM snapshots | Per phase in §15 §13 |
| Phase C | Research | SPFresh incremental IVF; DiskANN hot tier; WCOJ/factorized graph joins; ClickHouse Native TCP; region failover; Vortex hot encoding | — |

## 2. Testing strategy

1. **Deterministic simulation testing (DST)** for meta, sequencer, journals, link apply and manifest commits — simulated network, clock, disk and object store (evaluate `madsim` vs `turmoil`; Iggy and FoundationDB/TigerBeetle as practice references). Every merged PR runs a DST seed sweep. *As built in M0 (D28):* a **seeded in-process simulation** (`operon-sim`), not a bit-exact deterministic one: seeded workloads, network partitions (`Router`), worker crashes and object-store faults (`FaultyStore::random`) on a single-threaded runtime, with real time and real I/O underneath, and a Wing–Gong–Lowe linearizability check of the recorded histories. A failing seed reports its full schedule but may not replay exactly; a full DST port stays an option.
2. **Object-store fault injection:** an `object_store` wrapper injecting latency, 5xx, throttling (503 SlowDown), 412/409 on conditional writes, partial reads, and crashes between PUT and commit.
3. **Crash-consistency tests:** kill -9 at instrumented failpoints (`fail` crate) across write, segment, commit, compaction and GC paths.
4. **Property-based tests (`proptest`):** WAL/segment encode/decode, offset index, manifest evolution, deletion bitmap algebra, CSR build vs. naive adjacency, tail merge vs. full rebuild.
5. **Differential testing:**
   - Hot tier on/off must return identical results (random disabling in CI).
   - Operon vs. reference engines: ES (BM25 rankings on fixed corpora), Qdrant (recall), Neo4j (Cypher results on LDBC/fixture graphs), ClickHouse (query results on ClickBench data), DataFusion-on-Parquet baseline.
6. **Jepsen-style tests** for Kafka semantics and cross-object consistency tokens (write via Kafka/ES/Cypher → strong read in another surface must reflect it); changelog streams checked against a full-rebuild diff (no lost or duplicated change across crashes).
   - Durable execution reuses Resonate's own harness: the differential test against the executable oracle, the linearizability search, and the Lean trace checker run against Operon's server plugin. Its trace-checking approach is also a reference for our own DST checks.
7. **Conformance suites** per gateway (client libraries and framework integrations listed in each milestone gate) — these define compatibility scope.
8. **Benchmarks in CI (nightly):** OpenMessaging Benchmark, BEIR, VectorDBBench, LDBC SNB, ClickBench, TPC-H; tracked for regressions, including S3 request counts per operation (cost regressions are bugs).

## 3. Risk register

| # | Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|---|
| 1 | Scope: five pillars is five products | High | High | Strict milestone order; compat scope defined by conformance suites, not feature requests; ship M0+M1 as a useful product on its own |
| 2 | Lance vendor control / API churn | Medium | High | Pin format 2.1 and crate versions; `CollectionStore` trait boundary; contribute upstream; Vortex/own format as long-term fallback |
| 3 | Lance small-commit cost | High (if misused) | Medium | Never commit per write; batch via Operon log |
| 4 | iceberg-rust write gaps (DVs, RowDelta) | Certain | Medium | RisingWave fork + own DV writer, upstreamed; append-only tables unblocked meanwhile |
| 5 | Lakekeeper requires Postgres | Medium | Medium | Verify pluggable backend; else bundle managed Postgres for catalog only |
| 6 | Meta Raft becomes the bottleneck at high partition/namespace counts | Medium | High | Batch proposals per node flush; coalesce offset commits; multi-Raft sharding in M5; FoundationDB backend option |
| 7 | Cold-query latency disappoints (0.5–1 s) | Medium | Medium | Affinity routing, published hot artifacts, prewarm API, clear "pin" UX; document the cold/warm contract |
| 8 | Cross-AZ costs on `quorum` class | Certain | Low–Medium | Default classes avoid it; placement hints; document pricing |
| 9 | Qdrant fork maintenance (large codebase) | Medium | Medium | Take minimal subset (HNSW, quantization, filter planner); evaluate `qdrant-edge` boundary first |
| 10 | Quickwit fork divergence | Medium | Low–Medium | Fork only storage/directories/DSL crates; pin; periodic rebase |
| 11 | Competitive: HelixDB / LanceDB / Milvus / Databend / Apache Fluss move into the same slot (Databend now markets analytics + search + vector "for agents" on S3; Fluss is an ASF streaming lakehouse with Lance tiering "for AI") | Medium | High | Speed to M1; differentiate on compat gateways + Kafka + Iceberg + fully open serving layer (Databend has ELv2 parts) + S3-only state (Fluss keeps data on tablet-server disks) + durable execution; consider collaboration with HelixDB |
| 12 | Compatibility long tail (ES DSL, Cypher, ClickHouse functions) | High | Medium | Usage-driven prioritization from target integrations; clear, documented unsupported-feature errors |
| 13 | S3 provider behavior differences (conditional writes, Express/Rapid semantics) | Medium | Medium | Provider conformance tests in CI against real S3/GCS/Azure/MinIO |
| 14 | Correctness bugs in tail merge / consistency tokens | Medium | High | DST + differential tests + Jepsen-style cross-surface checks |
| 15 | Resonate upstream churn or abandonment (seed-stage vendor, git-only crates, protocol still evolving) | Medium | Medium | Pin a git revision; Operon code only behind the `ResonateServer` trait; the protocol is formally specified, so a fork stays checkable; Phase A has no Operon-specific storage format |
| 16 | Scope: durable execution adds a sixth surface | Medium | Medium | Phase A is integration of a forked server, not new storage; Phase B is gated on M4; no workflow DSL or SDK of our own (§14 §7) |
| 17 | Scope: agent workspaces (§15) add a Git server, a filesystem layer and a registry proxy | High | High | Run as a parallel track after M1 ships; W0 (MCP) alone first; buy nydus, gitoxide and runtimes; no runtime, POSIX FS or GitHub UI of our own |

## 4. Suggested first 90 days (M0 kickoff)

1. Repo + workspace skeleton (`operon-meta`, `operon-log`, `operon-store`, `operon-cache`, `operon-worker`, `operon-link`, `operon-pk`, `operon-sim`), CI with DST harness.
2. `object_store` fault-injection wrapper + provider conformance tests.
3. Meta state machine on openraft (namespaces, streams, sequencer, leases, manifest pointers) with snapshots to S3.
4. Leaderless `standard` WAL write path + segmenter + fetch path (native API).
5. Link framework + exactly-once apply against a toy target; PkIndex on SlateDB.
6. Crash/fault gates for M0.
