# 10 — Operations

Status: **Approved** · 2026-09-22 · revised 2026-09-25 (architecture review: surfaces D42–D45, M2 hardening D46, metastore backends D47) · revised 2026-09-26 (backends, RustFS, BYOC, tenancy, authorization, erasure: D58–D70, §18) · amended 2026-09-26 (turbopuffer gap analysis: backpressure and weighted concurrency quotas, customer-managed keys, audit events, SSO and private networking, performance and usage metrics, branches; D86, D90, D92, D96, D98, D100, D102, D103)

---

## 1. Deployment modes

| Mode | Command | Object store | Meta | Use |
|---|---|---|---|---|
| Dev | `operon dev` | Local filesystem (`object_store` LocalFileSystem), or RustFS started next to it by the compose file (D61) | Single-node Raft | Laptop, CI |
| Standalone | `operon standalone --bucket s3://…` | S3/GCS/Azure, or RustFS self-hosted | Single-node Raft (snapshots to bucket) | Small prod, edge |
| Cluster | `operon --roles gateway,query,…` | Cloud object storage, or RustFS on premises | 3 or 5 `meta` nodes across AZs, or Postgres or DynamoDB (M2) | Production |
| Kubernetes | Helm chart + `operon-operator` (M2) | Cloud object storage, or RustFS | StatefulSet (openraft `meta` only), or external Postgres or DynamoDB | Production |
| BYOC-managed-meta (M2.x) | Data plane in the customer's VPC | The customer's bucket | Hosted, through `operon-meta-remote` to `operon-control` | Managed service; the control plane is on the write path (§18 §8) |
| BYOC-local-meta (M2.x) | Data plane and metastore in the customer's VPC | The customer's bucket | openraft, or the customer's Postgres or DynamoDB | Managed service; a pull-based ops agent only (§18 §8) |

Lakekeeper is deployed next to Operon, not inside it (bundled in the Helm chart and the `docker-compose` examples from M4). RustFS (`rustfs/rustfs:1.0.x`, Apache-2.0) is the default self-hosted object store in the docs, the docker-compose dev stack, the Helm chart and the agent-fleet demo; it replaces MinIO, whose community edition is archived (D61).

All roles ship in one binary. Kubernetes: `meta` is the only StatefulSet (small PVCs for the Raft log), and it disappears with an external metastore backend; every other role is a Deployment with local NVMe (ephemeral) for cache, autoscaled by HPA/KEDA on role-specific signals (§01 §3.1). The operator (M2) deploys and scales roles, replaces lost nodes and drives rolling upgrades (§7); its end-to-end tests run on kind (deploy, scale, upgrade, node loss).

### 1.1 Metastore backends

The metastore is chosen per cluster behind `trait MetaStore` (§01 §3.2, D47):

| Backend | Milestone | Operated as | High availability | Backup |
|---|---|---|---|---|
| `raft` (embedded openraft) | Default | The `meta` role: 1 node (dev, standalone) or 3/5 nodes across AZs | Raft majority | Snapshots to the bucket + Raft log (§6) |
| `postgres` | M2 | An existing managed Postgres; no `meta` role | The provider's (Multi-AZ, Aurora, Cloud SQL HA) | The provider's backups and point-in-time recovery |
| `dynamodb` | M2 | One DynamoDB table (on demand or provisioned); no `meta` role | DynamoDB's multi-AZ replication | Point-in-time recovery, on-demand backups |
| `tidb` | M6 | A TiDB cluster or TiDB Cloud, over the MySQL protocol | TiKV's Raft replication | TiDB's BR backups and point-in-time recovery |
| `remote` | M2.x | `operon-meta-remote` to the hosted control plane (BYOC-managed-meta) | The control plane's | The control plane's |

Every backend serves the same relaxed contract (D59, §18 §3).

A cluster does not switch backends in place in v1.0; moving an existing cluster between backends is not yet designed.

## 2. Configuration surface (essentials)

```toml
[cluster]
name = "prod-us-east-1"
bucket = "s3://acme-operon/prod"
zones = ["use1-az1", "use1-az2", "use1-az4"]

[meta]
backend = "raft"            # raft | postgres (M2) | dynamodb (M2) | tidb (M6) | remote (M2.x)
peers = ["meta-0:7400", "meta-1:7400", "meta-2:7400"]   # raft only
# postgres = { url = "postgres://operon@pg.internal:5432/operon", read_pool = 10, write_pool = 5 }
# dynamodb = { table = "loam-meta", region = "us-east-1" }

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
otlp = { http = "0.0.0.0:4318", grpc = "0.0.0.0:4317" }   # logs only (M2, D73)
kafka = { listen = "0.0.0.0:9092" }   # Kafka wire protocol (M5, D74)
resonate = { listen = "0.0.0.0:8001" }   # durable execution (§14); Resonate SDK default port
admin = { listen = "0.0.0.0:8090" }      # /metrics, /health, diagnostic dump (§5); not a data surface

[tls]                                     # M2: applies to every listener
cert = "/etc/operon/tls/tls.crt"
key = "/etc/operon/tls/tls.key"
client_ca = "/etc/operon/tls/ca.crt"      # set to require client certificates (mTLS)
```

Each gateway is individually enabled; disabled gateways load no code paths (feature-gated at build time as well). The surfaces and their scope are listed in §01 §3.3.

**Clocks.** Every node must run NTP. Leases, retention, the segmenter and the WAL commit window (§02 §3) use wall-clock time stamped by the proposing node, and the metastore clock never goes back. The meta leader therefore refuses any command stamped more than `max_clock_skew` (default 5 min) ahead of its own clock (`ClockSkew`): a node whose clock runs ahead cannot write until its clock is fixed, but it cannot stop the other nodes' writes either. The bound works in both directions: a leader whose own clock is more than `max_clock_skew` *behind* refuses correct proposers too, which is why the default is generous. The leader's own clock must be right: a leader far ahead of real time would make WAL commits from correct clocks stale, and one far behind refuses them. (A race-free bound, with the leader stamping its own time into each entry, is planned for M5.) From M2 the contract every backend offers is bounded skew, not one monotonic clock (D59): commands carry the proposer's stamp, WAL commit records are pruned only after `2·window + max_clock_skew`, and GC claims, not clock order, keep GC from deleting an object that a concurrent command makes reachable (§18 §3.2). The openraft backend keeps its monotonic clock, but callers do not rely on it.

## 3. Multi-tenancy

- **Tenancy model (M2, D65):** org (tenant and billing unit) → namespaces → collections. Each namespace belongs to one org. Orgs, API keys, role bindings, quotas and usage rollups live in the **`ControlStore`**, a trait separate from `MetaStore`, so BYOC can host it remotely (M2.x).
- **Namespace isolation:** separate key prefixes, manifests, PK/ID-map instances, caches keyed by namespace; no cross-namespace reads without explicit grants.
- **Quotas per org and per namespace:** M2 enforces request rate (per surface), ingest bytes/s, concurrent queries, storage bytes (a soft limit that counts bytes held only by tags) and metadata operations (collection creates, alias updates, leases), which protect the shared metastore (D65, §18 §6), plus unapplied data per collection (the write backpressure budget that M1.3 enforces with fixed defaults, D86); concurrent queries are a cost-weighted semaphore per collection, 16 slots by default, with an 800 ms wait (D98); CPU-seconds, hot-tier RAM/NVMe budget and worker task concurrency follow with fair share at scale (M6). A request over quota is refused with each surface's own throttling error (HTTP 429, gRPC `RESOURCE_EXHAUSTED`).
- **Fair scheduling:** weighted fair queuing in query admission and worker scheduling (§05 §7, §09 §6).
- **Scale target:** 1M+ namespaces per cluster (M6 gate); a cold namespace costs only its S3 bytes plus a few KB of metadata. The path there is staged: M2 removes the O(N) catalog readers, M2.x lets nodes cache only the namespaces they own, M6 shards the metastore by namespace (D63, §18 §5).
- **Encryption (M2, D96):** TLS for all traffic and bucket-default encryption at rest. A namespace may name a customer-managed KMS key at creation (AWS KMS, GCP Cloud KMS, Azure Key Vault, behind a `KeyProvider` trait). Objects under `ns/<id>/` are then written with the provider's per-object KMS key (S3 SSE-KMS key id, GCS `kmsKeyName`, Azure encryption scope), which external Lance readers with a grant on the key can still read. WAL objects span namespaces (D25), so their chunks are envelope-encrypted: a data key per chunk, wrapped by a per-namespace key-encryption key that the KMS issues once per namespace, node and hour and that is held in memory only (§02 §3). Caches key entries by namespace and encrypt CMEK namespaces' NVMe entries. Destroying or revoking the key makes the namespace's bytes in the bucket unreadable at once, WAL chunks and noncurrent versions included (crypto-shredding; D69's encryption clause, decided by D96). Nodes notice the revocation at the latest at the hourly key refresh, and on any KMS refusal before it; each then zeroizes the namespace's in-memory key-encryption key and drops its memory and NVMe cache entries. Shredding is complete when every node has done so, within one refresh interval of the revocation; until then a node may still serve what it holds. Revocation is data loss, and a KMS outage fails only that namespace's requests, retryably. The key is fixed at creation; moving to another key is a copy (D90).

## 4. Security

Security ships in M2, before v1.0; its gate is that unauthenticated and cross-tenant requests are rejected on every surface (native, Flight SQL, Qdrant, ES, MCP, OTLP).

- **AuthN (M2):** API keys on every surface, each carried the way that surface's clients already send credentials. A key has the form `loam_<key_id>_<secret>`; the `ControlStore` keeps only a hash of the secret, with the key's org, scopes, expiry, creator and last use. Gateways cache resolved keys for 30–60 s, and revocations are pushed through the `ControlStore`'s change feed (D65):

  | Surface | Credential |
  |---|---|
  | Native REST/gRPC, MCP | `Authorization: Bearer <token>` (gRPC metadata `authorization`) |
  | OTLP (logs) | `Authorization: Bearer <token>`, set in the exporter's headers (gRPC metadata `authorization`) |
  | Kafka (M5) | SASL over TLS carrying the API key; the mechanism is Q26 |
  | Flight SQL | Bearer token in the `authorization` header (the ADBC drivers' token option); Flight basic-auth handshake returning a bearer token |
  | Qdrant | `api-key` header (REST and gRPC), as the Qdrant clients send |
  | Elasticsearch | `Authorization: ApiKey <key>` or basic auth, as the ES clients send |
  | Resonate | Per-namespace auth replacing `resonate-auth` (§14; Q11) |

  **TLS** on every listener; **mTLS** between nodes (and to an external metastore where the backend supports it), and optionally for clients (`client_ca`). OIDC/JWT validation for API callers, console SSO (SAML 2.0 and OIDC), per-org IP allowlists at the gateways, and AWS PrivateLink and GCP Private Service Connect endpoints come with the hosted service in M2.x (D102).
- **AuthZ (M2):** every surface maps each request to `(action, resource)` and asks the **`Authorizer`** trait (`check`, `batch_check`, `filter_visible`, lifecycle hooks; D66). M2 ships `AllowAll` (dev) and built-in namespace-scoped **RBAC**: roles grant actions (`read`, `write`, `admin`) over namespaces and collections, with bindings in the `ControlStore`. Fine-grained authorization with **OpenFGA** (through `openfga-client`, with a model adapted from Lakekeeper's and a tenant fence) moves from M6 to **M2.x** with the control plane, sharing one OpenFGA store with Lakekeeper (D67, default). Tuple writes go through a transactional outbox, never before or after the resource's own commit (§18 §7). Field-level masking for collections/tables (Phase B).
- **Audit (M2, D100):** one audit record per admin action or security event (API keys created and revoked, role bindings changed, failed authentications (rate-limited), namespaces and collections created and dropped, erasure requests and completions, key changes, backpressure overrides), written to the stream `_audit` of a system namespace, with the actor, org, action, resource, outcome, time, request id and source address and never document contents; kept 30 days by default and readable by the org's `admin` role. Data-access auditing is optional. M2.x adds a console view and exports to object storage, HTTPS, Datadog, Splunk and Sentinel.
- **BYOC data boundary (M2.x, D64):** the hosted control plane may see namespace and collection names, schemas, object paths, offsets, pointers and lease keys, in clear; never documents, vectors, text or bucket credentials. GC runs inside the customer's VPC.
- **Credential vending**: scoped, short-lived object-store credentials for direct Lance fragment reads through scan plans (M2, §17 §3) and for external Iceberg readers via Lakekeeper (M4).

### 4.1 GDPR erasure (M2, D68)

A delete hides data at once but leaves its bytes in WAL objects, segments, Lance fragments, splits, retained and tagged manifests, hot artifacts, caches and noncurrent object versions. An **erasure** removes them within a deadline (§18 §9):

1. `erase` (by key or filter) deletes, and records an erasure request `{ns, collection, key hashes, offset, deadline}` in the metastore.
2. A worker's **forced purge** materializes the deletions (Lance compaction, a re-indexing merge of the affected splits, rewritten PK state and dead letters, rebuilt hot artifacts) and drops time travel before the erasure point.
3. The implicit stream is **trimmed** past the erasure offset; segments and WAL objects below it are retired.
4. **GC** deletes the retired objects after its grace period and evicts them from the RAM and NVMe caches explicitly.
5. On versioned buckets, GC deletes each noncurrent version of the retired and rewritten objects by version id, on the replica bucket too, and the erasure completes only after a version listing shows none remain. Lifecycle expiry is asynchronous and is only a backstop (§18 §9).
6. The **erasure log** keeps keyed hashes of the keys (HMAC-SHA256 under a per-org key in the `ControlStore`), the request and completion times, and what was rewritten. It is readable only by the org's `admin` role and the operator's audit role.

Defaults, owner-overridable (D69): a tagged manifest is rewritten onto a purged copy and the tag records it; completion within **30 days**, targeting days.

## 5. Observability

The M2 baseline, on every node:

- **Prometheus metrics** at `/metrics` on the admin listener; **OpenTelemetry traces** exported over OTLP (gateway → query operators → object-store calls, and metastore calls); **structured JSON logs** with trace ids.
- A **live diagnostic dump** (`GET /debug/dump` on the admin listener, `admin` role): the node's roles and build, metastore status (backend, leader, applied index), held leases and running tasks, link lag, cache and hot-tier occupancy, in-flight queries, and recent errors, as one JSON document for bug reports. It holds no document contents or credentials.
- `/health/live` and `/health/ready` on the admin listener for the operator and load balancers.
- Key metrics: append and fetch latency per WAL class, link lag, compaction debt, cache hit ratio per layer (H0–H3) per namespace, S3 requests/bytes per namespace (cost attribution), hot-tier memory per object, query latency by surface (native, Flight SQL, Qdrant, ES), rejected requests by reason (auth, quota), metastore operation latency per backend, changelog lag, durable-execution transitions/s, conditional-write conflicts (412/409) and timer lag per namespace.
- System tables: `system.queries`, `system.links`, `system.tasks`, `system.streams`, `system.collections`, `system.tables`, `system.parts`, `system.cache`, `system.namespaces`; from §14 Phase B, `system.durable_promises` and `system.durable_tasks` (`system.tasks` stays the worker task table).
- Per-query profiles (DataFusion metrics tree) retrievable by query id.
- **Per-response performance (M1.6, D92):** every native search response carries `performance` (timings, queue wait, tail and stale records, rows scanned per retriever, hot structures used, H1 cache hit ratio, object-store requests); `POST …/collections/{c}/recall` measures ANN recall on demand (M1.7), and M2 exports sampled continuous recall per collection.
- **Backlog (M1.3, D86):** every collection write response carries `Operon-Unapplied-Records` and `Operon-Unapplied-Bytes`; `CollectionInfo` reports the backlog and the backpressure state; M2 exports them as gauges with a counter of throttled writes.
- **Usage (M2, D103):** per-namespace logical bytes written, stored, queried and returned, queries and hot-tier GB-hours, rolled up in the `ControlStore`; the hosted service bills on them (M2.x).

## 6. Backup, DR and time travel

- **Data** is already in object storage: enable bucket versioning + lifecycle; cross-region replication (S3 CRR / GCS dual-region / Azure GRS) for DR. **With versioning on, an erasure deletes the noncurrent versions of the objects it retires by version id, in the replica bucket too, and verifies they are gone before it completes** (§4.1). A lifecycle rule that expires noncurrent versions within the erasure deadline is the backstop; S3 applies it asynchronously.
- **Metadata (openraft):** meta snapshots to the bucket every N minutes + Raft log shipping; restore = new meta cluster from latest snapshot + log.
- **Metadata (Postgres, DynamoDB, TiDB):** the backend's own backups and point-in-time recovery. Metadata holds no documents; erasure requests hold keyed key hashes only.
- A metadata restore to a point older than GC's grace period (§03 §7) references objects GC may have deleted since; bucket versioning recovers them.
- **Restore from bucket** is an M2 drill: a new cluster is brought up from the bucket alone (openraft snapshots live in it), or from the bucket and the backend's backup.
- **Point-in-time restore:** collections/graphs via retained manifests; tables via Iceberg snapshots; streams via retention.
- **Branches and copies (D90):** a branch (M2) is a constant-time, isolated copy of a retained manifest, useful before a risky change; a copy (M2.x) writes a collection into another namespace, bucket, region or org under the target's key, as an asynchronous operation, and serves as a logical backup.
- **Region failover (Phase C):** restore meta in the DR region against the replicated bucket; RPO = replication lag.

## 7. Upgrades

- **Zero-downtime rolling upgrades (M2)**, node by node and role by role, driven by the operator; wire protocols between roles are versioned (N/N−1 compatibility). The M2 gate: a rolling upgrade of a 3-node cluster under load loses no acknowledged write and fails no read beyond client retries.
- **Format-version checks:** every Operon format carries `magic + format_version` and readers support N and N−1 (§03). A node refuses to start if the cluster's enabled format versions are outside what it reads, and a new format is enabled only after every node runs a version that reads it.
- Format changes are opt-in and rolled forward by compaction (§03 §6).
- Metastore migrations are versioned: applied through the Raft log (openraft), as `sqlx` schema migrations (Postgres, TiDB), or as item-format versions read N and N−1 (DynamoDB).

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
