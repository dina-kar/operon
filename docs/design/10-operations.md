# 10 — Operations

Status: **Approved** · 2026-09-22

---

## 1. Deployment modes

| Mode | Command | Object store | Meta | Use |
|---|---|---|---|---|
| Dev | `operon dev` | Local filesystem (`object_store` LocalFileSystem) or embedded MinIO | Single-node Raft | Laptop, CI |
| Standalone | `operon standalone --bucket s3://…` | S3/GCS/Azure/MinIO | Single-node Raft (snapshots to bucket) | Small prod, edge |
| Cluster | `operon --roles gateway,query,…` | Cloud object storage | 3 or 5 `meta` nodes across AZs | Production |
| Kubernetes | Helm chart + `operon-operator` | Cloud object storage | StatefulSet (meta only) | Production |

All roles ship in one binary. Kubernetes: `meta` is the only StatefulSet (small PVCs for Raft log); every other role is a Deployment with local NVMe (ephemeral) for cache, autoscaled by HPA/KEDA on role-specific signals (§3.1 of 01).

## 2. Configuration surface (essentials)

```toml
[cluster]
name = "prod-us-east-1"
bucket = "s3://acme-operon/prod"
zones = ["use1-az1", "use1-az2", "use1-az4"]

[meta]
backend = "raft"            # raft | foundationdb | postgres
peers = ["meta-0:7400", "meta-1:7400", "meta-2:7400"]

[wal.express]
buckets = { "use1-az1" = "s3://acme-wal--use1-az1--x-s3", "use1-az2" = "…", "use1-az4" = "…" }
write_quorum = 2

[cache]
ram = "48GiB"
nvme_path = "/mnt/nvme/operon"
nvme = "1.5TiB"

[catalog.iceberg]
lakekeeper_url = "http://lakekeeper:8181/catalog"
warehouse = "prod"

[gateways]
kafka = { listen = "0.0.0.0:9092" }
elasticsearch = { listen = "0.0.0.0:9200" }
qdrant = { rest = "0.0.0.0:6333", grpc = "0.0.0.0:6334" }
bolt = { listen = "0.0.0.0:7687" }
clickhouse_http = { listen = "0.0.0.0:8123" }
native = { rest = "0.0.0.0:8080", grpc = "0.0.0.0:8081", flight_sql = "0.0.0.0:8082" }
```

Each gateway is individually enabled; disabled gateways load no code paths (feature-gated at build time as well).

## 3. Multi-tenancy

- **Namespace isolation:** separate key prefixes, manifests, PK/ID-map instances, caches keyed by namespace; no cross-namespace reads without explicit grants.
- **Quotas per namespace:** storage bytes, ingest bytes/s, produce/fetch request rates, query concurrency, CPU-seconds, hot-tier RAM/NVMe budget, worker task concurrency.
- **Fair scheduling:** weighted fair queuing in query admission and worker scheduling (§05 §7, §09 §6).
- **Scale target:** 1M+ namespaces per cluster; a cold namespace costs only its S3 bytes plus a few KB of metadata.
- **Encryption:** SSE-KMS per namespace key (bucket-level default), optional client-side envelope encryption for data objects (Phase B), TLS for all traffic.

## 4. Security

- **AuthN:** API keys, OIDC/JWT; Kafka SASL (PLAIN, SCRAM, OAUTHBEARER) and mTLS; ES/ClickHouse/Qdrant API keys or basic auth; Bolt basic/bearer.
- **AuthZ:** RBAC with namespace → object → action (`read`, `write`, `admin`) grants; field-level masking for collections/tables (Phase B). Evaluate **OpenFGA** (used by Lakekeeper) for a shared authorization model across Operon and the Iceberg catalog.
- **Audit:** every admin action and (optionally) every data access written to an audit stream in a system namespace.
- **Credential vending** for external Iceberg readers via Lakekeeper (scoped, short-lived S3 credentials).

## 5. Observability

- OpenTelemetry traces (gateway → query operators → object-store calls), Prometheus metrics endpoint, structured JSON logs.
- Key metrics: produce/fetch latency per WAL class, link lag, compaction debt, cache hit ratio per layer (H0–H3) per namespace, S3 requests/bytes per namespace (cost attribution), hot-tier memory per object, query latency by surface.
- System tables: `system.queries`, `system.links`, `system.tasks`, `system.streams`, `system.collections`, `system.tables`, `system.parts`, `system.cache`, `system.namespaces`.
- Per-query profiles (DataFusion metrics tree) retrievable by query id.

## 6. Backup, DR and time travel

- **Data** is already in object storage: enable bucket versioning + lifecycle; cross-region replication (S3 CRR / GCS dual-region / Azure GRS) for DR.
- **Metadata:** meta snapshots to the bucket every N minutes + Raft log shipping; restore = new meta cluster from latest snapshot + log.
- **Point-in-time restore:** collections/graphs via retained manifests; tables via Iceberg snapshots; streams via retention.
- **Region failover (Phase C):** restore meta in the DR region against the replicated bucket; RPO = replication lag.

## 7. Upgrades

- Rolling upgrades role by role; wire protocols between roles are versioned (N/N−1 compatibility).
- Format changes are opt-in and rolled forward by compaction (§03 §6).
- Meta state machine migrations are versioned and applied via Raft.

## 8. Cost model (illustrative, AWS us-east-1 list prices)

| Component | Driver | Notes |
|---|---|---|
| Storage | $0.023/GB-month (S3 Standard) | No replication multiplier; compare ≈ $0.16–0.24 effective for 2–3× EBS replication |
| Writes | PUTs ($0.005/1k) | Batched: WAL flushes, large segments, large Iceberg/Lance files |
| Reads | GETs ($0.0004/1k) | Cache hit ratio is the lever; range reads coalesced |
| Express WAL | Storage $0.11/GB-month (seconds-lived), PUT $0.00113/1k, upload $0.0032/GB | Per `express` stream |
| Cross-AZ | ≈ $0.01/GB each direction | ~0 for `standard`/`express` with zone-aware routing; `quorum` pays for 2 replica copies |
| Compute | Stateless, autoscaled, spot-friendly (except meta) | Scale to zero per namespace for idle tenants |

Per-namespace cost attribution (requests, bytes, CPU) is exported so platform teams can charge back.
