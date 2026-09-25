# 10 — Operations

Status: **Approved** · 2026-09-22 · revised 2026-09-25 (architecture review: surfaces D42–D45, M2 hardening D46, metastore backends D47)

---

## 1. Deployment modes

| Mode | Command | Object store | Meta | Use |
|---|---|---|---|---|
| Dev | `operon dev` | Local filesystem (`object_store` LocalFileSystem) or embedded MinIO | Single-node Raft | Laptop, CI |
| Standalone | `operon standalone --bucket s3://…` | S3/GCS/Azure/MinIO | Single-node Raft (snapshots to bucket) | Small prod, edge |
| Cluster | `operon --roles gateway,query,…` | Cloud object storage | 3 or 5 `meta` nodes across AZs, or Postgres (M2) | Production |
| Kubernetes | Helm chart + `operon-operator` (M2) | Cloud object storage | StatefulSet (openraft `meta` only), or external Postgres | Production |

Lakekeeper is deployed next to Operon, not inside it (bundled in the Helm chart and the `docker-compose` examples from M4).

All roles ship in one binary. Kubernetes: `meta` is the only StatefulSet (small PVCs for the Raft log), and it disappears with an external metastore backend; every other role is a Deployment with local NVMe (ephemeral) for cache, autoscaled by HPA/KEDA on role-specific signals (§01 §3.1). The operator (M2) deploys and scales roles, replaces lost nodes and drives rolling upgrades (§7); its end-to-end tests run on kind (deploy, scale, upgrade, node loss).

### 1.1 Metastore backends

The metastore is chosen per cluster behind `trait MetaStore` (§01 §3.2, D47):

| Backend | Milestone | Operated as | High availability | Backup |
|---|---|---|---|---|
| `raft` (embedded openraft) | Default | The `meta` role: 1 node (dev, standalone) or 3/5 nodes across AZs | Raft majority | Snapshots to the bucket + Raft log (§6) |
| `postgres` | M2 | An existing managed Postgres; no `meta` role | The provider's (Multi-AZ, Aurora, Cloud SQL HA) | The provider's backups and point-in-time recovery |
| `foundationdb` | M6 | A FoundationDB cluster | FoundationDB's replication | FoundationDB backup |

A cluster does not switch backends in place in v1.0; moving an existing cluster between backends is not yet designed.

## 2. Configuration surface (essentials)

```toml
[cluster]
name = "prod-us-east-1"
bucket = "s3://acme-operon/prod"
zones = ["use1-az1", "use1-az2", "use1-az4"]

[meta]
backend = "raft"            # raft | postgres (M2) | foundationdb (M6)
peers = ["meta-0:7400", "meta-1:7400", "meta-2:7400"]   # raft only
# postgres = { url = "postgres://operon@pg.internal:5432/operon", pool = 32 }

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
native = { rest = "0.0.0.0:8080", grpc = "0.0.0.0:8081", flight_sql = "0.0.0.0:8082" }   # MCP at /mcp on rest
qdrant = { rest = "0.0.0.0:6333", grpc = "0.0.0.0:6334" }
elasticsearch = { listen = "0.0.0.0:9200" }
resonate = { listen = "0.0.0.0:8001" }   # durable execution (§14); Resonate SDK default port
admin = { listen = "0.0.0.0:8090" }      # /metrics, /health, diagnostic dump (§5); not a data surface

[tls]                                     # M2: applies to every listener
cert = "/etc/operon/tls/tls.crt"
key = "/etc/operon/tls/tls.key"
client_ca = "/etc/operon/tls/ca.crt"      # set to require client certificates (mTLS)
```

Each gateway is individually enabled; disabled gateways load no code paths (feature-gated at build time as well). The surfaces and their scope are listed in §01 §3.3.

**Clocks.** Every node must run NTP. Leases, retention, the segmenter and the WAL commit window (§02 §3) use wall-clock time stamped by the proposing node, and the metastore clock never goes back. The meta leader therefore refuses any command stamped more than `max_clock_skew` (default 5 min) ahead of its own clock (`ClockSkew`): a node whose clock runs ahead cannot write until its clock is fixed, but it cannot stop the other nodes' writes either. The bound works in both directions: a leader whose own clock is more than `max_clock_skew` *behind* refuses correct proposers too, which is why the default is generous. The leader's own clock must be right: a leader far ahead of real time would make WAL commits from correct clocks stale, and one far behind refuses them. (A race-free bound, with the leader stamping its own time into each entry, is planned for M5.) This describes the openraft backend; how the Postgres backend bounds skew is part of its design (Q17).

## 3. Multi-tenancy

- **Namespace isolation:** separate key prefixes, manifests, PK/ID-map instances, caches keyed by namespace; no cross-namespace reads without explicit grants.
- **Quotas per namespace:** M2 enforces request rate (per surface), storage bytes and concurrent queries; ingest bytes/s, CPU-seconds, hot-tier RAM/NVMe budget and worker task concurrency follow with fair share at scale (M6). A request over quota is refused with each surface's own throttling error (HTTP 429, gRPC `RESOURCE_EXHAUSTED`).
- **Fair scheduling:** weighted fair queuing in query admission and worker scheduling (§05 §7, §09 §6).
- **Scale target:** 1M+ namespaces per cluster (M6 gate); a cold namespace costs only its S3 bytes plus a few KB of metadata.
- **Encryption:** SSE-KMS per namespace key (bucket-level default), optional client-side envelope encryption for data objects (Phase B), TLS for all traffic.

## 4. Security

Security ships in M2, before v1.0; its gate is that unauthenticated and cross-tenant requests are rejected on every surface (native, Flight SQL, Qdrant, ES, MCP).

- **AuthN (M2):** API tokens on every surface, each carried the way that surface's clients already send credentials:

  | Surface | Credential |
  |---|---|
  | Native REST/gRPC, MCP | `Authorization: Bearer <token>` (gRPC metadata `authorization`) |
  | Flight SQL | Bearer token in the `authorization` header (the ADBC drivers' token option); Flight basic-auth handshake returning a bearer token |
  | Qdrant | `api-key` header (REST and gRPC), as the Qdrant clients send |
  | Elasticsearch | `Authorization: ApiKey <key>` or basic auth, as the ES clients send |
  | Resonate | Per-namespace auth replacing `resonate-auth` (§14; Q11) |

  **TLS** on every listener; **mTLS** between nodes (and to an external metastore where the backend supports it), and optionally for clients (`client_ca`). OIDC/JWT validation is a later addition.
- **AuthZ (M2):** namespace-scoped **RBAC**: roles grant actions (`read`, `write`, `admin`) over namespaces and collections, and every surface maps each request onto one of them. Field-level masking for collections/tables (Phase B). Fine-grained authorization with **OpenFGA** (used by Lakekeeper), shared across Operon and the Iceberg catalog, is M6.
- **Audit:** every admin action and (optionally) every data access written to an audit stream in a system namespace.
- **Credential vending**: scoped, short-lived object-store credentials for direct Lance fragment reads through scan plans (M2, §17 §3) and for external Iceberg readers via Lakekeeper (M4).

## 5. Observability

The M2 baseline, on every node:

- **Prometheus metrics** at `/metrics` on the admin listener; **OpenTelemetry traces** exported over OTLP (gateway → query operators → object-store calls, and metastore calls); **structured JSON logs** with trace ids.
- A **live diagnostic dump** (`GET /debug/dump` on the admin listener, `admin` role): the node's roles and build, metastore status (backend, leader, applied index), held leases and running tasks, link lag, cache and hot-tier occupancy, in-flight queries, and recent errors, as one JSON document for bug reports. It holds no document contents or credentials.
- `/health/live` and `/health/ready` on the admin listener for the operator and load balancers.
- Key metrics: append and fetch latency per WAL class, link lag, compaction debt, cache hit ratio per layer (H0–H3) per namespace, S3 requests/bytes per namespace (cost attribution), hot-tier memory per object, query latency by surface (native, Flight SQL, Qdrant, ES), rejected requests by reason (auth, quota), metastore operation latency per backend, changelog lag, durable-execution transitions/s, conditional-write conflicts (412/409) and timer lag per namespace.
- System tables: `system.queries`, `system.links`, `system.tasks`, `system.streams`, `system.collections`, `system.tables`, `system.parts`, `system.cache`, `system.namespaces`; from §14 Phase B, `system.durable_promises` and `system.durable_tasks` (`system.tasks` stays the worker task table).
- Per-query profiles (DataFusion metrics tree) retrievable by query id.

## 6. Backup, DR and time travel

- **Data** is already in object storage: enable bucket versioning + lifecycle; cross-region replication (S3 CRR / GCS dual-region / Azure GRS) for DR.
- **Metadata (openraft):** meta snapshots to the bucket every N minutes + Raft log shipping; restore = new meta cluster from latest snapshot + log.
- **Metadata (Postgres, FoundationDB):** the backend's own backups and point-in-time recovery.
- A metadata restore to a point older than GC's grace period (§03 §7) references objects GC may have deleted since; bucket versioning recovers them.
- **Restore from bucket** is an M2 drill: a new cluster is brought up from the bucket alone (openraft snapshots live in it), or from the bucket and the backend's backup.
- **Point-in-time restore:** collections/graphs via retained manifests; tables via Iceberg snapshots; streams via retention.
- **Region failover (Phase C):** restore meta in the DR region against the replicated bucket; RPO = replication lag.

## 7. Upgrades

- **Zero-downtime rolling upgrades (M2)**, node by node and role by role, driven by the operator; wire protocols between roles are versioned (N/N−1 compatibility). The M2 gate: a rolling upgrade of a 3-node cluster under load loses no acknowledged write and fails no read beyond client retries.
- **Format-version checks:** every Operon format carries `magic + format_version` and readers support N and N−1 (§03). A node refuses to start if the cluster's enabled format versions are outside what it reads, and a new format is enabled only after every node runs a version that reads it.
- Format changes are opt-in and rolled forward by compaction (§03 §6).
- Metastore migrations are versioned: applied through the Raft log (openraft) or as schema migrations (Postgres, FoundationDB).

## 8. Cost model (illustrative, AWS us-east-1 list prices)

| Component | Driver | Notes |
|---|---|---|
| Storage | $0.023/GB-month (S3 Standard) | No replication multiplier; compare ≈ $0.16–0.24 effective for 2–3× EBS replication |
| Writes | PUTs ($0.005/1k) | Batched: WAL flushes, large segments, large Iceberg/Lance files |
| Reads | GETs ($0.0004/1k) | Cache hit ratio is the lever; range reads coalesced |
| Express WAL | Storage $0.11/GB-month (seconds-lived), PUT $0.00113/1k, upload $0.0032/GB | Per `express` stream |
| Durable execution | One conditional PUT per origin transition batch, plus timer PUT/DELETEs | ≈ $10–15 per million workflow steps before group commit (§14 §6) |
| Cross-AZ | ≈ $0.01/GB each direction | ~0 for `standard`/`express` with zone-aware routing; `quorum` pays for 2 replica copies |
| Compute | Stateless, autoscaled, spot-friendly (except meta) | Scale to zero per namespace for idle tenants |

Per-namespace cost attribution (requests, bytes, CPU) is exported so platform teams can charge back.
