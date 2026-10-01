# 28 — Loam Postgres: a Neon Fork with Loam's Control Plane and Loam's WAL

Status: **Approved direction** · 2026-09-29. On 2026-09-29 the owner amended §23 (PR #111) with these decisions:

- The showcase apps run on CloudNativePG.
- Neon is forked, with Loam's control plane as the primary control plane.
- PgDog does the routing, and only as an unmodified separate service.
- Loam's own WAL replaces Neon's safekeepers.

The owner then chose the WAL design:

- **Option A.** TiKV is the quorum hot tier, and Loam's log group-commits to the bucket behind it.
- **A Loam crate speaks Neon's safekeeper protocol**, so walproposer and the pageserver stay unmodified.
- **A pgbench merge gate** gates the switch-over.

These are decisions **D230–D236**, all owner-approved on 2026-09-29. The WAL replacement (D233) is approved *direction*: it becomes the default only if the benchmark gate passes. The concrete WAL design is **D237–D241**. They are Loam's proposals within the owner's decisions and are marked as such. Open questions are **Q110–Q119**. Arm A of the WAL (§7.2, 2026-09-30) adds D263–D269 and D271 (D270 is the CloudEvents decision; D263, compio for the data path, is the owner's) and Q261–Q264.

This document **supersedes §23's D149** (Neon for the showcase apps) and **D151** (fork only when needed). It **amends D150** (the control plane grows from a client into the primary control plane) and **D153** (PgDog, not Loam's pg listener, splices Postgres connections to computes). D148, D152, D154 and D155 stand. D156 and D157's WeSQL row are not affected.

Markers:

- **(spike)** means measured on 2026-09-29 on the development machine. That machine is a 14-core, 15 GB laptop with consumer NVMe and btrfs, and other sessions' builds were running, with a load average between 1.5 and 24. The notes are in `.superpowers/research/loam-postgres-spike.md`, local and not committed. Spike numbers compare paths *on the same hardware*. They are not production figures.
- **(verify)** means not checked against a primary source.
- **(estimate)** means computed, not measured.
- Paths of the form `neon/…` point into `dina-kar/neon` at `fa504217c` (2026-08-31, identical to upstream `main`).

---

## 1. Summary

| # | Decision | Status |
|---|---|---|
| D230 | **The showcase apps run on CloudNativePG** (Apache-2.0, v1.30.1) with plain **Postgres 17** (17.11). Backups and PITR go to RustFS through the **Barman Cloud CNPG-I plugin** (v0.15.0), not the in-tree `barmanObjectStore`, which is deprecated and removed in 1.31. **Replaces D149** | Approved (owner, 2026-09-29) |
| D231 | **Loam Postgres is a fork of Neon**, `dina-kar/neon` (created 2026-09-29 as a GitHub fork of `neondatabase/neon`, Apache-2.0). Loam owns its releases, its Postgres patch rebases and its images. Nothing is posted upstream. **Replaces D151** | Approved (owner, 2026-09-29) |
| D232 | **Loam's control plane is Loam Postgres' primary control plane.** It replaces Neon's closed one: tenant, timeline and branch lifecycle; compute specs and compute start; the storage controller's hooks; the proxy's auth API when the proxy is used (§5). **Amends D150** | Approved (owner, 2026-09-29) |
| D233 | **Loam's WAL replaces Neon's safekeepers**, behind a feature, and becomes the default only when the pgbench gate passes: p99 commit latency no worse than the safekeeper baseline, and throughput no worse (§6, §7) | Approved direction (owner, 2026-09-29), gated on benchmarks |
| D234 | **Option A with TiKV as the quorum hot tier.** A Postgres WAL record is acknowledged once it is durable in TiKV (Raft quorum). Loam's log then group-commits it to the bucket. TiKV's copy is trimmed after the bucket upload *and* the pageserver's `remote_consistent_lsn` pass it. The target is single-digit-ms commit acks | Approved (owner, 2026-09-29) |
| D235 | **Loam speaks Neon's safekeeper protocol through a clean Rust API.** A Loam crate, `operon-safekeeper`, implements the proposer–acceptor protocol (v3), the replication protocol the pageserver and computes read from, and the storage-broker publication. Walproposer and the pageserver run unmodified. P4 splits into P4a (the protocol crate and its TiKV backend), P4b (the benchmark harness) and P4c (the switch-over behind a feature, merged only when the gate passes) | Approved (owner, 2026-09-29) |
| D236 | **PgDog routes, as an unmodified separate service only** (AGPL-3.0, v0.1.60). It is never linked, vendored or patched. Loam generates its config. The fallbacks are Neon's proxy (Apache-2.0) and PgBouncer (ISC). **Amends D153** for Postgres OLTP | Approved (owner, 2026-09-29) |
| D237 | **TxnKV with 1PC and async commit, not RawKV**, for the WAL hot tier. Only a transaction can check the term fence and write the WAL in one atomic step. RawKV needs a separate CAS round trip, and it was not faster in the spike (§6.4) | Proposed (Loam's design within D234) |
| D238 | **Key layout:** a dedicated keyspace `loam_pgwal`; per timeline, a head key followed by chunk keys by LSN; one split boundary per timeline; batches of 128 KiB chunks up to 1 MiB per transaction (§6.5) | Proposed |
| D239 | **One logical acceptor per timeline, served by a stateless, shared WAL service** (the `wal` role, one pool per AZ). It is not a compute sidecar. The compute's `neon.safekeepers` names one endpoint (quorum of one). Durability comes from TiKV's Raft quorum, not from three acceptors (§6.3) | Proposed |
| D240 | **The pageserver reads WAL from the WAL service** over the unchanged `START_REPLICATION` interpreted protocol. The service serves from its tail cache, then TiKV, then the Loam log in the bucket. Trimming waits for both the bucket commit and the pageserver's `remote_consistent_lsn` (§6.7) | Proposed |
| D241 | **The fork's Postgres is rebased every quarter.** Postgres 17 and 18 are supported, and 16 until it reaches end of life. The minor-release merge runs in CI on upstream's release day, and the compute images are published within a week (§10) | Proposed |

## 2. Goals and non-goals

### 2.1 Goals

1. **Real Postgres for the showcase apps now.** Plain Postgres on CloudNativePG, with PITR to the same RustFS store. It takes no fork work (D230).
2. **Serverless, branchable Postgres on the bucket as a Loam product.** Neon's storage (pageserver, layers in the bucket, copy-on-write branches) runs under Loam's control plane, with no dependency on a vendor's closed services (D231, D232).
3. **A WAL service that beats safekeepers where it counts.**
   - **Operations:** no stateful safekeeper fleet, and no membership migrations.
   - **Durability:** the bucket copy is seconds old, not segments old.
   - **Cost:** TiKV nodes are shared with the metastore and Loam Live.
   - **Latency:** commit latency stays at or under the baseline, and the gate enforces it (D233).
4. **License-clean.** Apache-2.0 for everything Loam links (D11). PgDog, which is AGPL-3.0, runs only as an unmodified process (D236).

### 2.2 Non-goals

- **Changing walproposer or the pageserver's WAL receiver.** Option C (a Loam protocol inside walproposer) is rejected (§6.2).
- **Merging branches.** As in §23: Postgres cannot merge timelines (D155).
- **Neon's hosted extras.** The console, the Data API, Neon Auth and autoscaling VMs are out of scope. Scale-to-zero is Q113.
- **Upstream contributions.** Nothing is posted to `neondatabase`, `pgdogdev` or `cloudnative-pg` without the owner's approval.
- **Modifying TiKV or PD.** D126 holds: official releases, configured, never patched. Client-side work goes into Loam's `client-rust` fork.

## 3. Architecture

```
  apps / agents / psql                                   showcase apps (Plane, Zulip, GlitchTip,
        │ postgres://…/<db>[__<branch>]                    Keycloak, OpenFGA)
        ▼                                                      │ postgres://
 ┌──────────────────────────────┐                     ┌────────┴──────────────────────────────┐
 │ PgDog (AGPL-3.0, unmodified, │                     │ CloudNativePG cluster (P1, D230)      │
 │ separate pod): pools, routes │                     │ Postgres 17.11, primary + replica     │
 │ by database name, balances   │                     │ Barman Cloud plugin ──► RustFS (PITR) │
 │ reads across replicas        │                     └───────────────────────────────────────┘
 └──────┬───────────────────────┘
        │ config + RELOAD from Loam's control plane
        ▼
 ┌──────────────────────────────┐   spec (GET …/computes/{id}/spec)   ┌─────────────────────────────┐
 │ Loam Postgres compute        │◄────────────────────────────────────┤ Loam control plane (D232)   │
 │ (fork image, compute_ctl,    │                                     │ in `operon`, state in TiKV  │
 │ walproposer, unmodified)     │                                     │ tenants / timelines /       │
 └──────┬──────────────┬────────┘                                     │ branches / computes;        │
        │ safekeeper   │ GetPage@LSN                                  │ /notify-attach,             │
        │ protocol v3  │                                              │ /notify-safekeepers,        │
        ▼              ▼                                              │ proxy auth API (optional)   │
 ┌──────────────────────────┐   ┌─────────────────────────────────┐   └──────┬──────────────────────┘
 │ Loam WAL service (P4)    │   │ pageserver (fork, unmodified    │          │ mgmt API
 │ `operon-safekeeper`,     ├──►│ WAL receiver)                   │◄─────────┤ storage controller
 │ role `wal`, stateless,   │   │ layers ──► bucket               │          │ (fork; its DB on CNPG,
 │ one pool per AZ          │   └─────────────────────────────────┘          │ TiKV later, P5)
 │ ├─ 1PC txn ──► TiKV keyspace `loam_pgwal` (3 replicas, leader in the compute's AZ)
 │ └─ group commit (250 ms) ──► Loam log (operon-log `standard`) ──► bucket
 │ publishes SafekeeperTimelineInfo ──► storage broker (fork, unmodified)
 └──────────────────────────┘
      stock safekeepers ×3 remain the default until the P4c gate passes (P2)

 RustFS: bucket `loam` (Loam, including the WAL log), bucket `neon` (layers), bucket `cnpg` (Barman)
```

### 3.1 Components

| Component | Source | License | Role |
|---|---|---|---|
| CloudNativePG operator | `cloudnative-pg/cloudnative-pg` v1.30.1 (2026-09-23) | Apache-2.0 | The showcase's Postgres (P1) and the storage controller's database (P2) |
| Barman Cloud plugin | `cloudnative-pg/plugin-barman-cloud` v0.15.0 (2026-09-03) | Apache-2.0 | WAL archiving and base backups to RustFS; PITR |
| Loam Postgres | `dina-kar/neon` (fork of `neondatabase/neon` at `fa504217c`) | Apache-2.0 | Pageserver, storage broker, storage controller, `compute_ctl`, compute images, and stock safekeepers until P4c |
| Loam Postgres' Postgres | `dina-kar/postgres` (to fork from `neondatabase/postgres` in P2, Q110) | PostgreSQL License | The `vendor/postgres-v1x` submodules |
| Loam control plane | `operon`, extending §23's `operon-neon` | Apache-2.0 | §5 |
| Loam WAL service | `operon-safekeeper` (new, P4a) | Apache-2.0 | §6 |
| PgDog | `pgdogdev/pgdog` v0.1.60 (2026-09-24), image pinned by digest | **AGPL-3.0** | §8. Unmodified, separate process |
| TiKV and PD | v8.5.x, official releases (D126) | Apache-2.0 | WAL hot tier (a keyspace of the existing cluster) |
| RustFS | 1.0.0 | Apache-2.0 | The object store (D61) |

## 4. CloudNativePG for the showcase apps (D230, P1)

**Why.** The showcase apps need Postgres now. Neon's fork work (§10) and the WAL service (§6) take quarters, and D-SC-12's Postgres front end over TiKV takes years. CloudNativePG is plain Postgres with a mature operator, PITR to object storage, and no fork. That is the same "plain Postgres as the exit" that D151 described.

**Verified 2026-09-29:**

| Fact | Evidence |
|---|---|
| **Apache-2.0**; CNCF **Sandbox** since 2025-01-21 (incubation applied, not yet granted) | `gh api repos/cloudnative-pg/cloudnative-pg` (`license.spdx_id`); cncf.io/projects/cloudnativepg |
| Latest **v1.30.1** and v1.29.3 (both 2026-09-23). 1.30.x supports PG 14–18 and Kubernetes 1.34–1.36, EOL about Dec 2026. 1.29.x reaches EOL 2026-09-29 | GitHub releases; cloudnative-pg.io/docs/devel/supported_releases |
| Default operand `18.6-system-trixie`. The `17.11-{minimal,standard,system}-trixie` images exist. *system* images are deprecated | v1.30.1 notes; ghcr manifests; `cloudnative-pg/postgres-containers` |
| In-tree `barmanObjectStore` deprecated since 1.26, **removal planned for 1.31.0** | v1.30.1 release notes; docs/backup |
| Barman Cloud plugin: Apache-2.0, v0.15.0 (Barman 3.20.0). `ObjectStore` CRD `barmancloud.cnpg.io/v1` with `endpointURL`, `endpointCA`, `s3Credentials` | `gh api repos/cloudnative-pg/plugin-barman-cloud/releases`; plugin docs |
| S3-compatible stores: MinIO is documented. Newer boto3 checksums can fail with `x-amz-content-sha256` errors, and the fix is `AWS_REQUEST_CHECKSUM_CALCULATION=when_required` and `AWS_RESPONSE_CHECKSUM_VALIDATION=when_required`. **No RustFS report found**, so P1 tests it (§23 §9.1 hit the same header with RustFS) | plugin docs, object_stores |

**Layout.**

- **One `Cluster`, `commons-pg`**, for the suite. It has instances 2 (a primary and a streaming replica), image `ghcr.io/cloudnative-pg/postgresql:17.11-standard-trixie` pinned by digest, and one database and owner role per app. This keeps §22 §6.4's "one Postgres 17 with a database per app".
- **Zulip needs PGroonga**, which the standard image lacks. It gets an image built on the standard image with PGroonga, or a separate Cluster (Q111).
- **Backups:**
  - An `ObjectStore` named `commons-rustfs` sets `destinationPath: s3://cnpg/commons-pg`, `endpointURL: http://rustfs:9000`, credentials from a Secret, `wal.compression: zstd` and the two checksum variables above.
  - The Cluster declares `plugins: [{name: barman-cloud.cloudnative-pg.io, isWALArchiver: true, parameters: {barmanObjectName: commons-rustfs}}]`.
  - A `ScheduledBackup` with `method: plugin` runs daily.
- **PITR** is a new Cluster with `bootstrap.recovery.source: origin` and `recoveryTarget.targetTime`, and `externalClusters: [{name: origin, plugin: {name: barman-cloud.cloudnative-pg.io, parameters: {barmanObjectName: commons-rustfs, serverName: commons-pg}}}]`. P1's test restores to a timestamp between two known writes.
- **Dev without Kubernetes** is not supported by CNPG. The suite's compose keeps a plain `postgres:17.11` container. P1 adds `deploy/cnpg/` with a kind or k3d manifest set for the PITR test.

**Loam integration.** The same as §23 for everything that addresses "a Postgres backend": the `x/` record carries `engine = postgres` (D152), and the pgoutput bridge (D154) runs against CNPG's primary, with `wal_level = logical` set in the Cluster's `postgresql.parameters`. Branch-per-workspace (D155) needs Loam Postgres and is not offered on CNPG.

## 5. The Loam control plane (D232)

§23's `operon-neon` was a client of Neon's APIs. Here Loam *is* the control plane that Neon's components call, so it serves as well as calls. The endpoints below come from the fork's code, which is the only specification.

### 5.1 What Loam calls

| Target | Call | Purpose |
|---|---|---|
| Storage controller (or a pageserver directly in dev) | `POST /v1/tenant/{t}/timeline` (create or branch: `new_timeline_id`, `ancestor_timeline_id`, `ancestor_start_lsn`, `pg_version`), `DELETE …/timeline/{tl}`, `PUT /v1/tenant/{t}/location_config`, `GET /v1/tenant/{t}/timeline` | Tenant, timeline and branch lifecycle (§23 §6.1) |
| Storage controller | `POST/GET /control/v1/safekeeper[/:id]`, `…/scheduling_policy`, `…/safekeeper_migrate` | Stock safekeepers only, from P2 until P4c. The Loam WAL service needs none of these (§6.3) |
| `compute_ctl` (per compute, JWT from `compute_ctl_config`'s JWKS) | `POST /configure` (`ConfigurationRequest{spec, compute_ctl_config}`), `GET /status`, `POST /terminate`, `POST /promote` | Push a new spec (for example a new pageserver after a migration), stop, promote a replica |
| `ComputeRuntime` (§23 §6.1) | `start`, `stop`, `status` | Start a compute pod with `--control-plane-uri` pointing at Loam |
| PgDog admin database | `RELOAD` after writing `pgdog.toml` and `users.toml` | Routing (§8) |

### 5.2 What Loam serves

| Caller | Endpoint | Request → response | Loam's handler |
|---|---|---|---|
| `compute_ctl` at start | `GET {cp}/compute/api/v2/computes/{compute_id}/spec`, `Authorization: Bearer $NEON_CONTROL_PLANE_TOKEN`. It retries up to 3 times on 502/503 | → `ControlPlaneConfigResponse{spec?: ComputeSpec, status: "empty" \| "attached", compute_ctl_config}` (`neon/libs/compute_api/src/responses.rs`) | Builds `ComputeSpec` from the `x/` and `X/` records: `tenant_id`, `timeline_id`, `mode`, `pageserver_connection_info`, `safekeeper_connstrings` (+ `safekeepers_generation`), `storage_auth_token`, roles, databases, settings (`wal_level = logical`, `synchronous_standby_names = 'walproposer'`) |
| Storage controller | `PUT {cp}/notify-attach` with `{tenant_id, preferred_az?, stripe_size?, shards: [{node_id, shard_number}]}` | 2xx; 423 Busy; 429 SlowDown | Records the pageserver per shard and pushes `/configure` to the tenant's computes |
| Storage controller | `PUT {cp}/notify-safekeepers` with `{tenant_id, timeline_id, generation, safekeepers: [{id, hostname?}]}` | as above | Stock safekeepers only (P2–P4c): pushes a new `safekeeper_connstrings` and generation to computes |
| Neon proxy (only if Q113 chooses it) | `GET {cp}/get_endpoint_access_control?session_id&application_name&endpointish&role` → `{role_secret, allowed_ips?, allowed_vpc_endpoint_ids?, block_public_connections?, rate_limits, …}`; `GET {cp}/wake_compute?…` → `{address, server_name?, aux: {endpoint_id, project_id, branch_id, compute_id, cold_start_info}}`; `GET {cp}/endpoints/{endpoint}/jwks` | `neon/proxy/src/control_plane/client/cplane_proxy_v1.rs`, `messages.rs` | Role secrets come from the credential store (Q30). `wake_compute` starts a compute through `ComputeRuntime` and waits for `/status` |

The pageserver's upcalls (`POST /upcall/v1/re-attach`, `POST /upcall/v1/validate`) go to the **storage controller**, which issues the generations that fence S3 writes (Neon RFC 025). Loam does not replace them in P2. The storage controller runs from the fork, with its Postgres database on CNPG (Q48 answered: a small CNPG Cluster). A TiKV-backed replacement is P5 (§9).

### 5.3 State

| Record | Key (`operon-meta-tikv`) | Change from §23 |
|---|---|---|
| Database | `x/<ns>/<db>` | Adds `engine = loam-pg \| postgres`, `wal = safekeepers \| loam` (per database, so P4c can switch one database at a time), `storage_controller` endpoint |
| Branch | `X/<ns>/<db>/<branch>` | Adds `compute_id`, `pageserver` shard map from `notify-attach` |
| Compute | `C/<compute_id>` (new; prefix checked as unused in `crates/operon-meta-tikv/src/keys.rs` at `e69ab38`) | Spec version, `status`, endpoint, JWT key id, created and last-seen times |

Writes are compare-and-set on a version, as in §23 §6.2. The spec endpoint is idempotent: it returns the same spec for the same compute id until the branch record changes.

## 6. Replacing the safekeepers (D233–D235, D237–D241)

### 6.1 What safekeepers do today

Read from the fork (`neon/pgxn/neon/walproposer*.c`, `neon/safekeeper/src/`, `neon/pageserver/src/tenant/timeline/walreceiver/`, `neon/docs/walservice.md`, RFCs 013, 035 and 041):

- **Commit path.**
  - walproposer is a background worker that poses as a synchronous standby (`synchronous_standby_names = 'walproposer'`).
  - A committing backend waits in stock `SyncRepWaitForLSN`.
  - walproposer computes `commitLsn` as the quorum (`n/2 + 1`) of the safekeepers' `AppendResponse.flush_lsn`, counting only WAL of its own term, and releases the waiters through `ProcessStandbyReply`.
  - A commit returns when a majority has **fdatasync**ed its WAL (`walproposer.c` `GetAcknowledgedByQuorumWALPosition`; `walproposer_pg.c` `walprop_pg_process_safekeeper_feedback`).
- **Safekeeper write path.**
  - Each `AppendRequest` (at most 128 KiB) is written without fsync.
  - One fdatasync covers everything queued when the inbound queue drains, or after at most 1 s (`receive_wal.rs` `FLUSH_INTERVAL`).
  - Segments are preallocated, so fdatasync suffices.
  - The control file is saved every 300 s or every segment, and at once on votes and term changes.
- **Offload.**
  - One elected offloader per timeline uploads **full 16 MiB segments** once `commit_lsn` passes them.
  - The current partial segment is uploaded within **15 minutes** (`wal_backup.rs`, `wal_backup_partial.rs`).
  - Local WAL is removed below `min(remote_consistent_lsn, backup_lsn, commit_lsn, flush_lsn)`.
- **Consumers.**
  - The pageserver finds safekeepers through the storage broker (`SafekeeperTimelineInfo`, pushed every 1 s).
  - It picks the one with the highest `commit_lsn`, preferring its own AZ.
  - It reads with `START_REPLICATION PHYSICAL` using the **interpreted** protocol (protobuf and zstd; records decoded and sharded on the safekeeper; `libs/wal_decoder`).
  - It reports `remote_consistent_lsn` back in a `'z'` status update.
- **The machinery around it:**
  - term histories and truncation;
  - peer recovery;
  - timeline eviction;
  - partial-segment GC;
  - two-phase membership changes with generations and `pull_timeline`;
  - a storage-controller scheduler for safekeepers.

**What the repository does not say.** Neither the fork nor Neon's site publishes a safekeeper commit latency. Neon's architecture page says only that "commit latency is primarily quorum/network-bound" (neon.com/docs/introduction/architecture-overview). The one third-party figure found is ClickHouse's PostgresBench (2026-07-21): Neon at 16 vCPU ran 7,802 TPS with p50 32.8 ms and p99 56.3 ms. That is pgbench latency under load, not a commit round trip (clickhouse.com/blog/postgresbench-ha). The production baseline must therefore be **measured** (P4b). The estimate for three safekeepers across AZs is one cross-AZ round trip plus an NVMe fdatasync, about **1–3 ms** at p50 (estimate, §6.6).

**Measured on this machine (spike).** The stack was `deploy/neon/` with **one** safekeeper (fsync on, WAL in the container filesystem), Postgres 16.9, and pgbench run inside the compute:

| Workload | Clients | TPS | p50 | p90 | p99 |
|---|---|---|---|---|---|
| Single-row `INSERT`, one commit per transaction | 1 | 220 | 3.57 ms | 5.79 ms | 22.4 ms |
| Same | 16 | 1 474 | 8.53 ms | 18.3 ms | 40.3 ms |
| Same, `synchronous_commit = off` | 1 | 6 743 | 0.09 ms | 0.32 ms | 0.81 ms |
| TPC-B (`-s 10`) | 1 | 111 | 7.98 ms | 11.0 ms | 23.2 ms |
| TPC-B (`-s 10`) | 16 | 495 | 24.5 ms | 59.2 ms | 133 ms |

So the safekeeper ack costs about **3.5 ms at p50** on this hardware, almost all of it the fdatasync of a consumer SSD under btrfs.

### 6.2 Options

| Option | What changes | For | Against |
|---|---|---|---|
| **A.** Loam's hot tier acks at quorum, and Loam's log group-commits to the bucket | A WAL store under the safekeeper role | Keeps a low-latency ack; reuses TiKV and the Loam log | Needs a protocol front end: B or C |
| **B.** The safekeeper wire protocol, implemented by Loam | New Loam service; walproposer, pageserver and broker unchanged | No change to the patched compute; upgrades are drop-in; the benchmark compares like with like | Loam must match a subtle protocol (terms, truncation, interpreted sending) |
| **C.** walproposer speaks Loam's protocol | The C extension in `pgxn/neon`; the pageserver's receiver | Could drop the Paxos layer completely | Forks the most delicate C code in Neon; every Postgres rebase carries it; the pageserver changes too |
| Pure bucket WAL (`standard` or `express` class) | Ack after a bucket PUT and a metastore commit | Cheapest; no hot tier | Measured 22 ms p50 and 148 ms p99 at a 1 ms flush interval (§6.4). S3 Standard is tens of ms per PUT (AWS: "median latencies are often in the tens of milliseconds"). `express` targets 20–50 ms (§02 §2). All of these are an order of magnitude over safekeepers for OLTP |

**Chosen: A behind B** (D234, D235, owner-approved). TiKV provides the quorum, and a Loam crate speaking the safekeeper protocol sits in front. C is rejected: it moves the risk into C code that every rebase must carry.

### 6.3 One logical acceptor per timeline (D239)

Walproposer is Paxos over *n* independent acceptors. Three Loam acceptors writing to one TiKV store would triple every write for no gain, because TiKV already replicates across three AZs. So:

- **The compute's `neon.safekeepers` names one endpoint.** It is a per-AZ Service address of the WAL pool, so walproposer runs with n = 1 and quorum 1. Neon runs this way in development today: `deploy/neon` has one safekeeper, and so does the spike.
- **The acceptor's state lives in TiKV**, not in a process: term, term history, `flush_lsn`, `commit_lsn`, `peer_horizon_lsn`, `backup_lsn` and `remote_consistent_lsn`. Any instance of the pool can serve any timeline. An instance that picks up a connection behaves exactly like a safekeeper that restarted with its disk intact: it answers the greeting from TiKV's state, refuses a vote for a term it has already voted in, and accepts `ProposerElected` from the current proposer.
- **Losing an instance is a reconnect**, not a failover. walproposer reconnects after `neon.safekeeper_reconnect_timeout` (default 1 000 ms; a `PGC_SIGHUP` GUC with minimum 0, `walproposer_pg.c`; Loam sets 100 ms). Nothing is copied, and nothing needs electing on the Loam side.
- **What disappears:**
  - membership changes and generations (the generation is fixed at 1 with one member, the Service);
  - `pull_timeline` and peer recovery;
  - timeline eviction and partial-segment uploads;
  - the storage controller's safekeeper scheduler.

  That is thousands of lines of Neon's safekeeper and controller code, and the operational runbooks that go with them.
- **Sidecar rejected.** A WAL process in the compute pod saves one in-AZ hop (about 0.1–0.2 ms, estimate). But it dies with the compute, while the pageserver still has to read the tail after the compute stops. It also multiplies instances by computes. The shared pool is stateless and needs no compute integration.

### 6.4 The TiKV API (D237)

**The fence.** A WAL write is valid only if the timeline's term is still the proposer's term when it lands. This is `check_fence`'s rule in `operon-meta-tikv` (`leases.rs`): read the fence key *for update* in the same transaction as the write, compare the epoch, and abort on mismatch. The WAL service applies it to the timeline's **head** key:

```
append(tl, term, begin_lsn, bytes, commit_lsn):
  txn (optimistic, use_async_commit, try_one_pc):
    head = get(H(tl))                      # fence read
    if head.term != term  -> abort: reply AppendResponse{term: head.term}   # deposed
    if end_lsn <= head.flush_lsn -> fully covered retry: write no chunk; put the head only if
                                    commit_lsn grew; reply with the existing flush_lsn
    if head.flush_lsn > begin_lsn -> drop the overlapping prefix (same term: same bytes)
    if head.flush_lsn < begin_lsn -> error (gap), the proposer reconnects
    put(W(tl, max(begin_lsn, head.flush_lsn)), remaining bytes)   # one key per ≤128 KiB chunk
    put(H(tl), head{flush_lsn = end_lsn, commit_lsn = max(head.commit_lsn, commit_lsn)})
                                           # end_lsn > head.flush_lsn here: flush_lsn only advances
  commit (1PC when H and W are in one region, else async commit)
  -> AppendResponse{term, flush_lsn = end_lsn, commit_lsn, …}
```

- The head is **written**, not only read. A concurrent term bump (a vote handled by another instance) therefore conflicts write-write in either order:
  - If the bump committed before this transaction's `start_ts`, the fence read sees the new term.
  - If it committed after, one of the two prewrites fails.

  No timing assumption is involved, which a lease-and-wait scheme would need.
- **A vote** is the same transaction shape: read the head, grant the vote only if `head.term < term`, and write the head with the new term. The term is durable before `VoteResponse`, as in `handle_vote_request`.
- **`ProposerElected`** truncates at `start_streaming_at`, in one transaction. A chunk key covers a range of LSNs, so the chunk that contains `start_streaming_at` is rewritten with only its bytes below that LSN; every chunk starting at or above it is deleted; and the head gets the new term history and `flush_lsn = start_streaming_at`. Committed WAL is never truncated (`start_streaming_at ≥ commit_lsn` is asserted, as in `handle_elected`).

**Why not RawKV.** A RawKV write cannot check another key. The fenced raw scheme is "put the chunk, then CAS the head", which is two sequential Raft writes in atomic-CAS mode. A raw put without the CAS lets a deposed proposer's write be acknowledged. With one acceptor, a deposed compute would then count a commit that the new term truncates: an acknowledged transaction lost. The spike measured the options on one TiKV store (playground v8.5.8, API v2, default configuration, 1 000 sequential operations, two runs):

| 8 KiB WAL chunk | p50 (run 1 / run 2) | p99 (run 1 / run 2) |
|---|---|---|
| **TxnKV 1PC + async commit: get head, put chunk** (the design) | **6.55 / 7.14 ms** | **25.2 / 26.0 ms** |
| TxnKV 1PC blind put (no fence; the floor) | 4.62 / 10.9 ms | 18.3 / 46.5 ms |
| TxnKV 2PC: get head, put chunk | 25.8 / 19.0 ms | 57.4 / 74.2 ms |
| RawKV put (no fence) | 14.9 / 6.44 ms | 50.5 / 33.3 ms |
| RawKV put, atomic-CAS mode (no fence) | 10.9 / 13.8 ms | 55.3 / 59.8 ms |
| RawKV CAS(head) + put (fenced) | 29.8 / 13.8 ms | 84.9 / 595 ms |
| 128 KiB chunk, TxnKV 1PC: get head, put chunk | 7.17 / 7.00 ms | 35.9 / 21.1 ms |

On this machine RawKV was **not faster** than a 1PC transaction. The fenced raw scheme was 2–4× slower at p50, and 2PC was about 3× slower than 1PC. The single-store playground has no network replication, so every figure here is "local Raft fsync plus apply plus client". The absolute values are inflated by the shared, loaded machine and must be re-measured on a three-node cluster (Q114). The ordering is the result: **TxnKV 1PC** (D237).

**Published figures** agree on the ordering:

- **Async commit and 1PC** (TiDB 5.0 release notes): Sysbench update-index average latency fell 41.7% (12.04 → 7.01 ms), and oltp-insert latency fell 37.3%. A single-region transaction skips the second phase altogether.
- **Atomic-mode raw writes** (`raw_compare_and_swap_atomic`, `raw_batch_put_atomic`) run as scheduler commands that take per-key latches (`tikv/src/storage/txn/commands/atomic_store.rs`). TiKV's docs say that "write operations like put or delete in atomic mode are more expensive" (tikv.org/docs/7.1/develop/rawkv/cas).
- **RawKV on API v2** takes a causal timestamp from a TiKV-local TSO cache, refreshed every 100 ms, plus a concurrency-manager guard (`tikv/src/storage/mod.rs`). It avoids the PD round trip that a transaction's `start_ts` needs, but not the fence problem.
- **No current TiKV page gives RawKV put p99** for WAL-sized values. The v6.1 performance overview gives throughput only: 43.2 K updates/s on YCSB workload A on 3 × 40 vCPU, and "at most 200,000 OPS within 10 ms latency" (tikv.org/docs/6.1/deploy/performance/overview). Q114 measures it.

### 6.5 Key layout (D238)

- **Keyspace `loam_pgwal`**, pre-allocated like the others (`deploy/tikv/pd.toml`). It keeps the WAL out of the metastore's GC safe point and placement, and gives the WAL its own quotas.
- **Keys per timeline** `tl = (tenant_id, timeline_id)`, 32 bytes:
  - `H/<tl>`: the head (postcard-encoded): `term`, `term_history`, `flush_lsn`, `commit_lsn`, `peer_horizon_lsn`, `backup_lsn` (bucket), `remote_consistent_lsn`, `pg_version`, `system_id`, `wal_seg_size`, `timeline_start_lsn`, version.
  - `W/<tl>/<begin_lsn: u64 BE>`: WAL bytes `[begin_lsn, end_lsn)` of one chunk, at most 128 KiB (one walproposer `MAX_SEND_SIZE`).
- **One region range per timeline.** Timeline creation asks PD to split at `H/<tl>` and at the next timeline's prefix. The head and the tail chunks then usually share a region, so the append is 1PC. When a size split separates them, the commit falls back to async commit: prewrites go to both regions in parallel, and the ack comes after the prewrites, so latency rises by little (estimate).
- **Hotspots.** Each timeline's tail is a sequential write hotspot, as a Raft log is. A timeline cannot be spread over regions without breaking ordering, and it does not need to be: one region leader handles tens of MB/s (verify). Many timelines spread across stores through PD's leader and region balancing.
- **Splits.** **Load-based split does not help here.** It counts *read* QPS, bytes and CPU only, over 10 s (`split.qps-threshold` 3000, `byte-threshold` 30 MiB/s; docs "Load Base Split"), so it never splits a write-hot WAL region. The per-timeline pre-split above is what spreads the load. Size splits happen at `region-split-size` (256 MiB since v8.4). PD merges small regions after `split-merge-interval` (1 h), and the trimmed timelines leave small regions for it to merge.
- **Batching.** While one append transaction is in flight for a timeline, arriving `AppendRequest`s queue. The next transaction takes the whole queue, up to 1 MiB (8 chunks). That fits `raft-max-size-per-msg` (1 MiB) and is well under `raft-entry-max-size` (8 MiB). This is the safekeeper's own "fsync when the queue drains" group commit. With 5 ms per transaction and 1 MiB per batch, one timeline tops out at about 200 MiB/s (estimate). Pipelining more than one in-flight transaction per timeline is P4 optimisation work (Q115).
- **Values.** WAL compresses well, but Postgres' WAL may already hold compressed full-page images (`wal_compression`). Chunks are stored as they arrive, and the bucket copy is compressed (§6.7).

### 6.6 TiKV tuning and the latency budget

**Configuration of the `loam_pgwal` cluster.** The WAL runs on the same TiKV release as the rest of Loam (D126). When the WAL shares the metastore's cluster, it gets its own stores through placement rules (Q116). The defaults below are TiKV v8.5's, read from `pingcap/docs` `release-8.5` (`tikv-configuration-file.md`).

| Setting | v8.5 default | For the WAL | Why |
|---|---|---|---|
| Raft log fsync | Always. `raftstore.sync-log` was **removed in v5.0** ("forcibly set to `true`") | Unchanged | A write is acknowledged only after a quorum has persisted it. Never relaxed |
| `raft-engine.enable` | `true` (since v6.1). `enable-log-recycle = true` | Unchanged. `raft-engine.dir` on its own NVMe drive with power-loss protection | An append-only multi-Raft log. PingCAP reports 20% lower tail latency and 25–40% less write IO than RocksDB (Raft Engine blog). Log recycling "reduces the long tail latency on write workloads" |
| `raftstore.store-io-pool-size` | **1** (async IO; 0 before v8.0) | 1–2. Keep store writer and raftstore threads under 80% CPU | Without it, the raftstore thread fsyncs, which "increases commit duration by about one IO+fsync time" (tikv#10540) |
| `raftstore.store-pool-size` / `apply-pool-size` | 2 / 2 | Raise when `propose wait` or `apply wait` grows | The `tikv_raftstore_store_wf_*` waterfall shows which stage waits (docs "Latency breakdown": `async write = propose + commit + apply_wait + apply_log`, and with async IO `commit = max(wait by write worker, replicate log)`) |
| `raftstore.store-max-batch-size` / `apply-max-batch-size`, `raft-write-size-limit` | 256 / 256, 1 MiB | Unchanged | Batching inside TiKV already matches the WAL's 1 MiB batches |
| `server.grpc-concurrency` / `grpc-raft-conn-num` | `grpc-raft-conn-num × 3 + 2` / `max(1, min(4, cores/8))` (v8.5.4+; 5 / 1 before) | Unchanged at first | Client and Raft RPC threads |
| `raft-base-tick-interval`, `raft-heartbeat-ticks`, `raft-election-timeout-ticks` | 1 s, 2, 10: **an election after about 10 s** | Shorter ticks for WAL stores, for example 200 ms (about 2 s to elect), with `hibernate-regions` (default on) to bound the CPU (Q117) | A crashed leader **stalls every timeline it leads until the election**. Safekeepers do not stall when one of three fails. This is the WAL's weakest point (§12 row 10) |
| `causal-ts.*` (API v2) | `renew-interval` 100 ms, `alloc-ahead-buffer` 3 s | Raise `alloc-ahead-buffer` and the renew batch sizes as the docs advise, so a PD leader failover does not show as a write-latency spike ("about 15%" QPS drop otherwise) | API v2 keyspaces need the causal-timestamp cache |
| Storage | — | NVMe with power-loss protection | fdatasync of 16 KB takes **0.7–10 µs** on PLP drives (Intel D7-P5520, Samsung PM9A3) and **0.45–2.8 ms** on consumer drives (Crucial T500, Samsung 990 Pro) (Small Datum, 2026-01). The spike ran on a consumer drive, which is why both paths measured in milliseconds |
| Placement | Placement rules on (`enable-placement-rules`) | Per timeline range (or per AZ group, Q117): one rule `role: leader, count: 1, label_constraints: zone in [<compute's AZ>]`, and one `role: follower, count: 2` in the other AZs. Keys are the memcomparable-encoded `'x' + keyspace id` bounds, in hex. Loam's control plane writes the rule when it places the compute | The client→leader hop stays in the AZ. Quorum needs one cross-AZ follower |

**Client side (Loam's `client-rust` fork).**

- **No RPC batching.** `client-rust` sends every request as its own unary gRPC call (`src/store/request.rs`). It has no `BatchCommands` stream like client-go's (`max-batch-size` 128, `max-batch-wait-time`). One append per timeline in flight is unaffected. Many timelines per WAL instance may need it (Q118).
- **No follower or stale reads**, and no zone-aware routing. Every request goes to the leader (upstream PR #562). The leader placement rule is what keeps the WAL in the AZ.
- **Keep a TSO prefetch.** A 1PC transaction still needs a `start_ts` from PD. The fork's TSO oracle batches requests (R1 plan row F1). With PD's leader in another AZ, a TSO costs one cross-AZ round trip, so the WAL pool prefetches.
- **The async-commit and 1PC fixes are already in the fork.** `max_commit_ts`, and the 1PC fallback that commits instead of failing, are in `docs/plans/r1-dependency-spike.md` §(i). The WAL relies on both.

**Latency budget (estimate).** One commit record. The TiKV leader is in the compute's AZ and the followers are in two other AZs. The disks are NVMe with power-loss protection. AWS inter-AZ RTTs measured 0.39–2.42 ms across regions, "the vast majority" under 1 ms (bitsand.cloud), and AWS itself says "single-digit millisecond".

| Step | Loam WAL | Safekeepers ×3 (AZ a, b, c) |
|---|---|---|
| Compute → acceptor | 0.1–0.2 ms (in AZ) | 0.1 ms to `a`, and in parallel one cross-AZ RTT to `b` |
| TSO for `start_ts` | 0–0.5 ms (prefetched; one cross-AZ RTT if PD's leader is elsewhere and not prefetched) | — |
| Fence read of the head (leader) | 0.2–0.4 ms | — |
| Durable write | Leader's Raft append and fsync, in parallel with one follower's: cross-AZ RTT (0.4–1 ms) + fsync (µs) + batching, **0.6–1.5 ms**; then apply, 0.1–0.5 ms | fdatasync on `a` and `b`; the quorum is complete when `b`'s ack arrives: one cross-AZ RTT + fsync, **0.5–1.2 ms** |
| Ack back | 0.1–0.2 ms | included |
| **Total, p50** | **about 1.2–3.3 ms** | **about 0.6–1.5 ms** |
| **p99 target** | **< 5 ms** (owner target) | to be measured (P4b) |

**The honest reading.**

- **The extra steps.** At equal hardware, the Loam WAL's commit path has one more read, one TSO and TiKV's apply step. The p50 is likely **about 0.5–1.5 ms worse** than three safekeepers.
- **What the spike showed.** On this laptop:
  - the single safekeeper acked in 3.57 ms at p50 and 22 ms at p99;
  - a fenced 1PC write to a single TiKV store took 6.5–7.1 ms at p50 and 25–26 ms at p99.

  That is about 2× at p50, and close at p99, both dominated by a consumer SSD's fsync.
- **What the gate compares.** The gate (§7) is on p99. There, async IO, Raft Engine and PLP disks can bring the Loam WAL level with a safekeeper whose fsync is the tail.
- **Failover.** A TiKV leader crash stalls its timelines for the election timeout, where losing one of three safekeepers stalls nothing. The P4b fault run measures commit latency through a leader kill as well as steady state (§7).

**Where Loam wins without argument:**

- **Operations.** A stateless pool on a TiKV cluster that Loam already runs, instead of a stateful three-node safekeeper fleet per region with membership changes, eviction and a scheduler.
- **Durability.** The bucket copy lags by the group-commit interval (250 ms). Safekeepers upload full 16 MiB segments, and partial segments only every 15 minutes. After a total loss of the hot tier, the RPO is 250 ms instead of up to 16 MiB or 15 minutes of WAL.
- **Cost.** Idle timelines cost nothing in the WAL tier, because their keys are trimmed. Safekeeper nodes are provisioned for peak, with three NVMe volumes each.
- **Throughput.** Group commit is per timeline, and TiKV scales out across stores.

If the gate fails on p99, the WAL stays behind its feature and stock safekeepers remain the default (D233). Nothing else in this document depends on the WAL.

### 6.7 How the pageserver and computes read it; retention; recovery (D240)

- **Replication for readers.** The WAL service implements `START_REPLICATION [PHYSICAL] X/Y [(term='N')]` like `send_wal.rs`:
  - Without a term, it serves up to `commit_lsn`, for the pageserver and replicas.
  - With a term, it serves up to `flush_lsn` within that term, for the compute's `neon_walreader` and recovery.
  - The pageserver hard-codes the **interpreted** protocol (`timeline.rs` `launch_wal_receiver`: protobuf, zstd level 1, per-shard filtering). The WAL service therefore decodes WAL with Neon's `wal_decoder` crate (Apache-2.0).
  - **Confirmed in P4a:** vanilla ingestion was removed from the pageserver (neon #12126). A compute on the WAL service without an interpreted sender stalls on its first read of a page it wrote, because the pageserver never gets the WAL. Until the WAL service links `wal_decoder`, `operon-safekeeper`'s **feeder** acts as walproposer towards one stock safekeeper and streams it the committed WAL, off the commit path. That safekeeper runs with `--no-sync`, publishes to the broker and serves the pageserver. This is interim (Q112).
- **Sources, in order:**
  1. the in-memory tail of recently appended chunks (written through at append time);
  2. TiKV (`scan W/<tl>/begin..`);
  3. the Loam log in the bucket for ranges already trimmed from TiKV.
- **Feedback.** The pageserver's `'z'` updates carry `remote_consistent_lsn`. They are written into the head, coalesced to at most one write per second per timeline, and returned to the compute in `AppendResponse.pageserver_feedback`, which drives walproposer's backpressure (`max_replication_*_lag`). Hot-standby feedback is passed through the same way.
- **Discovery.** Every instance publishes `SafekeeperTimelineInfo` for the timelines it serves to the unmodified storage broker, every second: `safekeeper_id` = the pool's logical id, `safekeeper_connstr` = the pool's Service, and `commit_lsn`, `flush_lsn`, `remote_consistent_lsn` and `availability_zone` from the head. It also answers `SafekeeperDiscoveryRequest` for any timeline in `loam_pgwal`.
- **Group commit to the bucket.**
  - A per-timeline task appends committed chunks to the Loam log (`operon-log` `standard` class, D72's idempotent producer = `(timeline, term)`, sequence = `begin_lsn`), with a 250 ms flush interval.
  - After each acknowledged log append, it advances `head.backup_lsn`.
  - The stream is `pgwal/<tenant>` with one partition per timeline slot (Q119), so retention, segmenting, encryption (D96) and GC come from the log unchanged.
- **Trimming.** Chunks below `min(backup_lsn, remote_consistent_lsn, commit_lsn)` are deleted from TiKV in batched transactions. MVCC GC for the keyspace (`operon-tikv` `gc.rs`) reclaims them. A timeline with no compute and no lag holds no TiKV keys except its head.
- **Recovery.**
  - *An instance crash* is a reconnect (§6.3).
  - *A TiKV leader failure* is a Raft election (the election timeout). Appends stall and then resume, and nothing acknowledged is lost.
  - *Losing a TiKV AZ* keeps quorum.
  - *Losing TiKV entirely* loses at most the last group-commit interval of acknowledged WAL. The timeline is recovered from the bucket's copy up to `backup_lsn`, and the compute must be restarted from that LSN (`recover_from_bucket`, an operator action). With safekeepers, the same event loses everything since the last uploaded segment.

### 6.8 The public Rust API (D235)

A trait-level sketch, for P4a to refine:

```rust
/// A timeline: (tenant, timeline) in Neon's ids.
pub struct TimelineId { pub tenant: [u8; 16], pub timeline: [u8; 16] }
pub type Lsn = u64;
pub type Term = u64;

/// The acceptor's durable state: what a safekeeper keeps in its control file.
pub struct AcceptorState {
    pub term: Term,
    pub term_history: Vec<(Term, Lsn)>,
    pub flush_lsn: Lsn,
    pub commit_lsn: Lsn,
    pub peer_horizon_lsn: Lsn,
    pub backup_lsn: Lsn,
    pub remote_consistent_lsn: Lsn,
    pub timeline_start_lsn: Lsn,
    pub server: ServerInfo, // pg_version, system_id, wal_seg_size
}

pub enum Fenced { Term { current: Term } }

/// The storage behind the acceptor. TiKV implements it (P4a); an in-memory
/// implementation drives the protocol tests and the TLA+-derived traces.
#[async_trait]
pub trait WalStore: Send + Sync + 'static {
    async fn create(&self, tl: &TimelineId, server: ServerInfo, start: Lsn) -> Result<AcceptorState>;
    async fn load(&self, tl: &TimelineId) -> Result<Option<AcceptorState>>;
    /// Grant a vote if `term > state.term`; durable before returning.
    async fn vote(&self, tl: &TimelineId, term: Term) -> Result<(bool, AcceptorState)>;
    /// Adopt the elected proposer's history; truncate above `start`.
    async fn elected(&self, tl: &TimelineId, term: Term, start: Lsn,
                     history: Vec<(Term, Lsn)>) -> Result<Result<AcceptorState, Fenced>>;
    /// Fenced, contiguous append of `[begin, begin + wal.len())`; durable on Ok.
    async fn append(&self, tl: &TimelineId, term: Term, begin: Lsn, wal: &[Bytes],
                    commit_lsn: Lsn, truncate_lsn: Lsn) -> Result<Result<AcceptorState, Fenced>>;
    /// Read WAL `[from, to)` from the tail cache, TiKV or the bucket.
    fn read(&self, tl: &TimelineId, from: Lsn, to: Lsn) -> BoxStream<'static, Result<Bytes>>;
    async fn record_feedback(&self, tl: &TimelineId, fb: PageserverFeedback) -> Result<()>;
    /// Delete WAL below `lsn` once `backup_lsn` and `remote_consistent_lsn` have passed it.
    async fn trim(&self, tl: &TimelineId, lsn: Lsn) -> Result<()>;
}

/// The service: the safekeeper wire protocol over a `WalStore`.
pub struct WalService<S: WalStore> { /* … */ }
impl<S: WalStore> WalService<S> {
    pub fn new(store: S, broker: BrokerClient, config: WalServiceConfig) -> Self;
    /// `START_WAL_PUSH` (proposer) and `START_REPLICATION` (readers) on one listener,
    /// plus `IDENTIFY_SYSTEM` and `TIMELINE_STATUS`, over the Postgres protocol.
    pub async fn serve(self, listener: TcpListener, shutdown: CancellationToken) -> Result<()>;
}
```

- **Crate layout.**
  - `proto`: the message codecs for v2 and v3. It is pure Rust, with no Postgres headers.
  - `acceptor`: the state machine, ported from `neon/safekeeper/src/safekeeper.rs` with its tests.
  - `tikv`: the `WalStore` implementation.
  - `send`: readers, and the interpreted sender behind the feature `interpreted`.
  - `broker`: the storage-broker gRPC client, built from `storage_broker/proto/broker.proto`.
- **The interpreted sender depends on the fork's `wal_decoder`, `postgres_ffi` and `utils`**, as a git dependency pinned to a fork revision. `postgres_ffi`'s `build.rs` runs bindgen against Postgres server headers (`POSTGRES_INSTALL_DIR/v1x/include/postgresql/server`), so that feature needs the fork's Postgres headers at build time. CI builds them once per fork pin and caches them. The default Operon build does not enable the feature (Q112).
- **Attribution.** Ported code keeps Neon's Apache-2.0 notice in `NOTICE`.

### 6.9 Mapping the safekeeper protocol to Loam operations

| Message (direction) | Fields that matter | Loam operation |
|---|---|---|
| `START_WAL_PUSH (proto_version '3', allow_timeline_creation …)` (P→A) | Query on a replication connection with `tenant_id` and `timeline_id` in options | Open the session; `WalStore::load` (or `create` if allowed and absent) |
| `ProposerGreeting` `'g'` (P→A) | `tenant_id`, `timeline_id`, `mconf` (generation, members), `pg_version`, `system_id`, `wal_seg_size` | Validate against the head (version, segment size, system id); the membership is fixed at generation 1 with one member |
| `AcceptorGreeting` `'g'` (A→P) | `node_id`, `mconf`, `term` | From the head: `term`; `node_id` = the pool's logical id |
| `VoteRequest` `'v'` (P→A) | `generation`, `term` | `WalStore::vote`: one transaction; read the head, write `term` if higher |
| `VoteResponse` `'v'` (A→P) | `term`, `vote_given`, `flush_lsn`, `truncate_lsn`, `term_history` | From the head after the vote |
| `ProposerElected` `'e'` (P→A) | `term`, `start_streaming_at`, `term_history` | `WalStore::elected`: check `find_highest_common_point`; rewrite the chunk containing `start_streaming_at` to end there and delete later chunks (never below `commit_lsn`); write the history, in one transaction |
| `AppendRequest` `'a'` (P→A) | `term`, `begin_lsn`, `end_lsn`, `commit_lsn`, `truncate_lsn`, WAL bytes | Queue. The group-commit task takes the queue into one `WalStore::append` (§6.4). An empty request is a heartbeat: it updates `commit_lsn` in the next batch or in a coalesced head write |
| `AppendResponse` `'a'` (A→P) | `term`, `flush_lsn`, `commit_lsn`, `hs_feedback`, `pageserver_feedback` | Sent after the append transaction commits, and unsolicited when new pageserver feedback arrives (as `network_write` does) |
| `START_REPLICATION PHYSICAL X/Y` (reader) | Options `protocol = interpreted`, `shard_*`, `availability_zone` | Stream from `WalStore::read` up to `commit_lsn` (interpreted: decode and filter per shard) |
| `START_REPLICATION … (term='N')` (compute `neon_walreader`) | term | Stream up to `flush_lsn` while the head's term equals N |
| `'r'` StandbyReply, `'h'` HotStandbyFeedback, `'z'` NEON_STATUS_UPDATE (reader→A) | Write/flush/apply LSNs; xmin; `PageserverFeedback` (`remote_consistent_lsn`, …) | `record_feedback`, coalesced; relayed to the proposer |
| `TIMELINE_STATUS`, `IDENTIFY_SYSTEM` | — | From the head |
| Safekeeper HTTP API: `POST /v1/tenant/timeline` | `mconf`, `pg_version`, `start_lsn` | `WalStore::create`. It is called by Loam's control plane, not by the storage controller |
| Storage broker `PublishSafekeeperInfo`, `SafekeeperDiscoveryRequest` | `commit_lsn`, `flush_lsn`, `remote_consistent_lsn`, `safekeeper_connstr`, `availability_zone` | Publish every second from the head; answer discovery for any timeline in `loam_pgwal` |
| `term_bump`, `membership`, `pull_timeline`, `snapshot`, partial backup, eviction | — | **Not needed** with one logical acceptor (§6.3). `term_bump` is kept for operators as a head write |

### 6.10 The upgrade path

1. **P2:** the fork runs with **stock safekeepers** under Loam's control plane (`x/….wal = safekeepers`).
2. **P4a–P4b:** the WAL service runs beside them on the benchmark topology only.
3. **P4c:** a database is switched by setting `wal = loam` in its `x/` record. The switch runs:
   - At a quiesced point: stop the compute and wait until the safekeepers' `commit_lsn = flush_lsn`.
   - Loam's control plane creates the timeline in `loam_pgwal` at that LSN (`start_lsn`, with the term history copied from the safekeepers' `TIMELINE_STATUS`).
   - The compute spec's `safekeeper_connstrings` becomes the WAL pool's Service, and the compute starts: `sync-safekeepers` runs against one acceptor, then the basebackup.
   - The safekeeper timeline is deleted after the pageserver's `remote_consistent_lsn` passes the switch point.
4. **Rollback is the same procedure in reverse.**
5. Branches inherit the parent database's setting.

## 7. The benchmark gate (P4b, D233)

**The gate.** On the same hardware and topology, with pgbench against stock safekeepers ×3 versus the Loam WAL:

- **p99 commit latency (Loam) ≤ p99 commit latency (safekeepers)**, and
- **throughput (Loam) ≥ throughput (safekeepers)**, for every workload below, within the run-to-run noise measured by repeating the baseline 3 times. P4c merges only when both hold.

**Topology** (`deploy/loam-pg-bench/`: a compose file for a single host, and a Kubernetes manifest for three nodes):

| Tier | Baseline | Candidate |
|---|---|---|
| Compute | 1 × fork compute image, pinned `shared_buffers` and `max_connections`, `fsync = on` | same image and settings |
| WAL | 3 × safekeeper (fork image), each on its own node or volume | WAL pool ×2 (one per node) + TiKV ×3 (Raft Engine, async IO, the leader rule of §6.6) + PD ×1 (or ×3) |
| Storage | pageserver ×1, storage broker, RustFS | same |
| Client | pgbench in a separate container on the compute's node | same |

On a single host, `tc netem` adds a fixed delay between containers to model AZs: 0.5 ms one way for "cross-AZ" pairs. Both runs use the same delays.

**Workloads** (`scripts/loam-pg-bench/`):

| Name | Script | Clients | Why |
|---|---|---|---|
| `commit-1` | `INSERT INTO t(v) VALUES (repeat('x',100))`, one per transaction | 1 | The pure commit round trip |
| `commit-16` | same | 16 | Group commit |
| `tpcb-16` | built-in TPC-B, `-s 50` | 16 | A realistic OLTP mix |
| `tpcb-64` | same | 64 | Saturation, which decides the throughput criterion |
| `bulk` | One transaction inserting about 1 GB | 1 | Sustained WAL throughput (MB/s); counts toward pass or fail |
| `bulk-burst` | One transaction inserting about 250 MB | 1 | A burst, reported only; it does not count toward pass or fail |

The `bulk` gate is a sustained write (owner decision, 2026-10-01) because a 250 MB burst measures the drive cache: on the same disk the stock safekeepers reach 118 to 182 MB/s on 250 MB and 22 to 26 MB/s on 1 GB.

Each run lasts 5 minutes after a 1-minute warm-up, with `pgbench -l` per-transaction logs. p50, p90, p99 and p99.9 come from the logs, not from pgbench's averages. The spike's scripts, `pgb.sh` and `pgb-async.sh`, are the starting point.

**Fault run** (reported, not part of the pass/fail gate unless the owner makes it one): `commit-16` for 5 minutes while one TiKV leader (or one safekeeper, for the baseline) is killed at minute 2. It records the longest commit stall and the p99.9.

**Recording.** Each run writes `bench/results/<date>-<git-sha>-<variant>.json`, containing:

- the topology and versions (fork sha, TiKV version, kernel, disk model);
- the settings;
- the percentiles and TPS per workload;
- the TiKV latency-breakdown metrics (propose, append, commit and apply waits) and the safekeeper `flush_wal` histograms.

A summary table goes into the P4c PR description. The run is a manual workflow (`workflow_dispatch`) on a dedicated runner, because shared CI runners are too noisy for p99s. It is also runnable locally. The spike's single-host numbers (§6.1, §6.4) are the first data point, not the gate.

### 7.1 First results: laptop, 2026-09-29 (P4a/P4b; not gate data)

**Implemented:** `crates/operon-safekeeper`:
- the v3 codecs, the acceptor and `WalStore`;
- the TiKV store, as fenced 1PC transactions;
- the service, HTTP API, feeder and the `loam-wal` binary.

The harness is in `deploy/loam-pg-bench` and `scripts/loam-pg-bench`, and the raw results in `bench/results/`.

**Verified:** an unmodified Neon compute (`compute-node-v16` at 77e22e4):
- starts through `sync-safekeepers` against `loam-wal` on TiKV v8.5.8;
- commits through it;
- is ingested by the pageserver through the feeder.

**Setup:**
- **Host:** one laptop, 14 cores, 15 GB of RAM, a consumer Samsung NVMe without power-loss protection, btrfs.
- **Placement:** every tier on the same disk, with no injected cross-AZ delay.
- **Other load:** other work shared the host.
- **Runs:** 60 s per workload after a 10 s warm-up. Baseline and candidate were interleaved three times each.
- **Compute settings:** `shared_buffers = 2GB`, compute `fsync = off`.

Means over the three repeats:

| Workload | rf | Safekeepers p50 / p99 (ms) | Safekeepers TPS | Loam p50 / p99 (ms) | Loam TPS |
|---|---|---|---|---|---|
| `commit-1` | 1 | 2.9 / 20.0 | 199 | 6.5 / 43.1 | 109 |
| `commit-16` | 1 | 6.0 / 28.9 | 1 969 | 17.0 / 66.3 | 791 |
| `tpcb-16` (`-s 10`) | 1 | 15.7 / 125 | 687 | 28.8 / 237 | 365 |
| `bulk` (WAL MB/s) | 1 | – | 97.5 | – | 20.4 |
| `commit-1` | 3 | 5.6 / 37.1 | 117 | 13.8 / 96.2 | 58 |
| `commit-16` | 3 | 11.5 / 63.6 | 983 | 32.4 / 127 | 474 |
| `tpcb-16` (`-s 10`) | 3 | 29.0 / 300 | 369 | 56.1 / 443 | 215 |
| `bulk` (WAL MB/s) | 3 | – | 76.9 | – | 11.9 |

rf 1 compares one safekeeper with one TiKV store; rf 3 compares three safekeepers with three stores.

**These are not gate results.** They are laptop data points outside the §7 protocol: one host, a consumer disk, 60 s runs instead of 5 minutes, `-s 10`, and no `tpcb-64`. The gate itself needs the §7 topology on a dedicated runner (the manual `loam-pg-bench` workflow).

**On this laptop, the comparison script's criteria fail for every workload at both replication factors.** The Loam WAL's p99 is about 1.5–2.6× the safekeepers', and its throughput about 0.2–0.6× (`bench/results/gate-rf*.md`). This gap is far wider than the noise, so a §7 run is not worth doing until the levers below are in. Meanwhile, D233 applies: the WAL stays behind its feature, and P4c does not start.

**Where the time goes.** TiKV's own metrics, from a `commit-1` run:
- **Raft log persist dominates:** p50 ≤ 2.6 ms, p99 up to 82 ms. That is the same consumer-SSD fsync the safekeeper pays, but with TiKV's scheduler, apply and 1PC steps added, plus three client RPCs per append: TSO, the fenced head read and the prewrite.
- **Single-flight appends per timeline:** they cap group commit. `bulk` shows it most, at 12–20 MB/s against 77–98 MB/s.
- **The interim feeder** adds a second WAL stream on the same disk.

**Before the gate can pass, the levers are:**
1. **Pipelined appends (Q115).** Several appends in flight per timeline, fenced without serialising on the head key. This is the main lever for `commit-16`, `tpcb` and `bulk`.
2. **No PD round trip per append.** Reuse the previous commit timestamp as the next `start_ts`. This is safe here: a stale `start_ts` only causes extra write conflicts. It needs a `begin_at(ts)` in the `client-rust` fork.
3. **The in-process interpreted sender (Q112).** It removes the feeder's second stream.
4. **Re-run on the §7 topology.** Server NVMe with PLP (fsync in µs, not ms), three nodes and real or `netem` AZ delays (Q114). The owner's < 5 ms p99 target is a server-hardware number. Both variants on this laptop are far above it.

### 7.2 Arm A: a native leaderless WAL on local NVMe (D263–D269 and D271, 2026-09-30)

On 2026-09-30 the owner decided that the Loam WAL targets **native leaderless-WAL performance** instead of tuning the TiKV hot tier. Two arms are measured against the same gate:

- **Arm A**, this section, takes TiKV off the commit path.
- **Arm B** keeps TiKV and removes its per-append overheads.

The owner also asked for Arm A's store to be a tiered, low-level I/O layer, with runtime detection and fallback (D266), and chose compio as its runtime (D263). The decisions below are Loam's proposals within that direction. They supersede D239 (one logical acceptor per timeline) for Arm A only, and leave D237–D238 in force for Arm B.

**The causes in §7.1, and how Arm A removes each one:**

| §7.1 cause | Arm A |
|---|---|
| TiKV's Raft hop and apply on every append | No TiKV on the commit path. Each acceptor writes to its own local NVMe |
| Three client round trips per append (TSO, fenced head read, prewrite) | None. The term fence is an in-memory check on the acceptor, made durable by the vote |
| One append in flight per timeline | Many in flight. Writes are issued while earlier ones are still syncing, and acks follow the durable position (Q115; adoption awaits P4b results) |
| The feeder shares the disk | The feeder stays until Q112 is solved, but its safekeeper moves to another filesystem; a separate device is needed to remove device contention (D271) |

#### The design

- **D263. compio is the runtime of the Arm A data path** (owner decision, 2026-09-30; final). The Arm A build enables compio's `io-uring` and `polling` features.
  - **Shards.** Each shard is one thread running a compio runtime, and **owns a set of timelines** (by hash of the timeline id, over a shard count fixed at first start). A shard also owns its own journal (D265), so group commit is per shard and needs no locks across cores.
  - **What runs on the owning shard:** the timeline's `START_WAL_PUSH` connection, its journal writes, and its durable writes. An accept thread reads the startup packet, which names the timeline, then hands the socket to the owning shard.
  - **The driver is io_uring.** compio's own opcodes cover sockets. Custom `OpCode`s cover what compio does not expose: `WRITE`/`WRITE_FIXED` with `RWF_DSYNC`, and registered buffers. The raw `io-uring` crate and `io_uring_register` on the ring's fd are used only where compio cannot help: buffer registration and file-table updates.
  - **Linked writes.** The safe compio 0.19 API used here cannot pass the `IO_LINK` flag on an operation. So the `WRITE` → `FSYNC` pair for FUA-less devices is two back-to-back operations on the shard, and the `FSYNC` is submitted only after the `WRITE` completes successfully. An `AppendResponse` never precedes durability. (The low-level `Extra` / `Proactor::push_with_extra` path could link the pair; it is a later optimisation.)
  - **When the `polling` feature is enabled and the driver selects it,** compio uses its epoll driver when io_uring is unavailable; durable writes use the `pwritev2` pool.
  - **tokio stays for the control plane:** the TiKV metadata client, S3 offload, the admin HTTP API (axum) and the feeder. It is bridged to the shards by bounded channels, off the commit path. The existing tokio WAL service stays as the fallback front end (`--runtime tokio`), and is benchmarked as tier (a).

  | Crate | License | Latest release (crates.io, 2026-09-30) | Model | Role |
  |---|---|---|---|---|
  | `compio` | MIT | 0.19.2 stable, 2026-08-18 (0.20.0-beta.1, 2026-09-29) | Thread-per-core, completion-based (io_uring, epoll fallback) | **The data path** (D263). Apache Iggy moved to it (verify) |
  | `io-uring` (tokio-rs) | MIT OR Apache-2.0 | 0.7.15, 2026-09-07; already in `Cargo.lock` through qdrant-edge and compio | Raw rings | Only where compio does not reach |
  | `monoio` (ByteDance) | MIT OR Apache-2.0 | 0.2.4, 2024-08-20 | Thread-per-core | Not used. No release for over two years; dropped from the benchmark (owner) |
  | `glommio` (Datadog) | Apache-2.0 OR MIT | 0.9.0, 2024-03-25 | Thread-per-core | Not used. No release for over two years |
  | `tokio-uring` | MIT | 0.5.0, 2024-05-27 | io_uring under tokio | Not used. No release for over two years |

- **D264. walproposer is the only sequencer.**
  - The compute's `neon.safekeepers` names **three `loam-wal` acceptors**, and walproposer runs its own Paxos over them, exactly as over safekeepers: terms, votes, `ProposerElected`, and `commitLsn` as the quorum of `flush_lsn`.
  - **Commit is 1 round trip and 1 durable write:** a majority of acceptors has the WAL on local disk.
  - An acceptor's term check is in memory. It is raised only after the vote is durable (D268), which is the Paxos acceptor's promise.
  - The vote handler first waits until the timeline's written WAL is durable. So `VoteResponse.flush_lsn` is what the disk holds, as in Neon's `handle_vote_request`.
  - After the vote, appends of a lower term are refused, and any later response carries the higher term, so a deposed proposer steps down.
  - Losing one of the three acceptors stalls nothing, which removes §12 row 10's TiKV leader-election stall for Arm A.
- **D265. One shared journal per shard, for every timeline the shard owns.**
  - **Precedent:** TiKV's `raft-engine`, which puts all regions in one log.
  - **Segments.** The journal is a directory of fixed-size segment files, 64 MiB by default. A background thread prepares the next two:
    - it `fallocate`s each one, then **pre-zeroes** it with large direct writes and one `fdatasync`, so the first append never converts an unwritten extent (a metadata journal commit);
    - after that, segments are **recycled** instead of created.
    - On btrfs the directory gets `NOCOW` (`FS_NOCOW_FL`, as systemd-journald does), so overwrites stay in place.
  - **Records** are framed: kind, timeline, term, LSN, length, and a CRC32C seeded with the segment's sequence number, so a recycled segment's old records never validate. The kinds are:
    - *Append*: WAL bytes, plus the proposer's `commit_lsn`.
    - *Truncate*: a logical truncation of one timeline, on election.
    - *Progress*: `commit_lsn`, `backup_lsn` and `remote_consistent_lsn` learned off the append path.
  - **Flush units.** Records are written in **flush units aligned to 4 KiB**, zero-padded to the block size. This is what `O_DIRECT` needs. It also lets several units be in flight at once without two writes ever touching the same block. The cost is space: at most one block per unit, on WAL that is offloaded and recycled.
  - **Group commit.** One writer coalesces every pending record, from every timeline and every connection on the shard, into the next unit (up to 1 MiB), and keeps up to `--io-depth` units in flight. The shard's durable position is the end of the longest prefix of completed units. By contrast, a safekeeper fsyncs each timeline's segment file separately, and writes nothing while its fsync runs.
  - **Recovery** replays segments in order and stops at the first record that fails its CRC (the torn tail). Units complete out of order, but an ack only covers a prefix of completed units, so nothing acknowledged is behind a torn unit. Each timeline keeps only the WAL contiguous from its trimmed point, and its `flush_lsn` is where that WAL ends.
  - **Reads.** An in-memory index maps each timeline's LSNs to records. Reads are `pread`s.
- **D266. A tiered I/O layer, chosen at startup.** `loam-wal` probes the journal directory and its block device, logs the tier it picked, and takes `--io auto|uring|pwritev2|buffered` as an override.

  | Tier | Durable write | Used when |
  |---|---|---|
  | **`uring`** | io_uring on the owning compio shard (D263), with **registered files** (segment slots updated on rollover) and **registered, aligned buffers** (`WRITE_FIXED`) on `O_DIRECT` segments. The write carries **`RWF_DSYNC`**: on pre-zeroed blocks the kernel sends one **FUA** write when the device has FUA, and write + flush when it does not. The alternative is a **linked `WRITE` → `FSYNC(DATASYNC)`** pair (`--uring-sync linked`, chosen automatically when `queue/fua` is 0). Optionally **SQPOLL** (`--uring-sqpoll`). **IOPOLL** is used only when the device has NVMe poll queues (`queue/io_poll` = 1) | io_uring is allowed (`io_uring_setup` succeeds and the opcodes probe) and the filesystem takes `O_DIRECT` |
  | **`pwritev2`** | `O_DIRECT` + `pwritev2(RWF_DSYNC)` from a small thread pool (one thread per in-flight unit) | io_uring is blocked, as by Docker's default seccomp profile and GKE Autopilot's containerd `RuntimeDefault` seccomp profile |
  | **`buffered`** | `pwrite` into the page cache; a sync thread runs `fdatasync` back to back while unsynced data exists | No `O_DIRECT` (tmpfs, some overlay filesystems), or chosen by `--io buffered` |
  | *Future:* NVMe passthrough | `uring_cmd` on `/dev/ng*`: NVMe write commands with FUA, no filesystem | A raw namespace per node. Documented only; no SPDK |

  - **This drive.** The laptop's drive reports `fua = 1`, `write_cache = write back`, `io_poll = 0` (no poll queues), and 512-byte logical blocks.
  - **Network.** Every connection sets `TCP_NODELAY`. `SO_BUSY_POLL` can be set with `--busy-poll-us`, and is logged and ignored when the kernel refuses it.
  - **io_uring networking.** The shard's sockets go through compio's io_uring driver. Multishot `recv` with provided buffer rings (compio's buffer pool) is the next step (Q264).
  - **kTLS** is the cheap route for issue #148: a rustls handshake, then `TLS_TX`/`TLS_RX` offload, so the data path keeps plain writes. That is a note only, not built.
- **D267. Pipelined appends.**
  - The service writes each group of queued `AppendRequest`s as soon as it arrives, and keeps reading.
  - An `AppendResponse` goes out whenever the durable position passes some of the timeline's writes. It carries the highest durable `flush_lsn`.
  - Heartbeats are answered at once with the durable position.
  - Stores that are durable per call (memory, TiKV) keep the old behaviour through the trait's default method.
- **D268. TiKV holds only metadata.**
  - **What:** per `(node, timeline)`, the acceptor record: term, term history, membership, server info, the start LSNs and `trimmed_lsn`.
  - **When:** it is written on timeline creation, vote, `ProposerElected` and trim.
  - **How:** these are 2PC transactions, which is fine at one or two writes per election, off the commit path.
  - **Order on election:** a *Truncate* record is made durable first, then the metadata. A crash between the two leaves a shorter WAL under the old history, which the next election repairs.
  - **Development:** a local control-file backend (write, fsync, rename, fsync the directory) serves single-node setups and tests.
- **D269. Offload and trim.**
  - **Offloader:** one acceptor per timeline, chosen by a lease in TiKV (`B/<tl>`: owner, expiry, `backup_lsn`). It uploads committed WAL to the bucket every 250 ms (or at 16 MiB).
  - **Objects:** `pgwal/<tenant>/<timeline>/<begin>-<end>.lwal`. The format follows `operon-log`'s segment conventions: a magic and version header, a CRC32C trailer, and a zstd body. The Kafka `RecordBatch` framing is not used, because WAL is one byte stream per timeline.
  - **`backup_lsn`** is advanced in the lease record, and the other acceptors read it from there.
  - **Trim.** A segment is recycled once every timeline in it has passed `min(backup_lsn, remote_consistent_lsn, commit_lsn)`.
  - **Pinning.** An idle timeline whose pageserver lags pins old segments. Rewriting its live records forward, as `raft-engine` purges, is left for later (Q262).
- **D271. The pageserver feed stays the feeder for now (Q112).**
  - **Why not in-process decoding yet:** it needs Neon's `wal_decoder`, `postgres_ffi` (bindgen against the fork's Postgres server headers), `utils` and `pageserver_api`, which bring Neon's workspace dependency pins into Operon's. That is not feasible within this arm.
  - **Instead:**
    - exactly one designated acceptor runs the feeder;
    - it feeds up to `min(commit_lsn, its durable flush_lsn)`;
    - its stock `--no-sync` safekeeper keeps its data on a **different filesystem** from the acceptors' journals.
  - **On the laptop** that filesystem is on the same physical drive, so the feeder's writeback still competes for the device.

#### 1PC, 2PC or raw: where each write goes

The commit path is the only place where latency is gated. TiKV's modes are measured in §6.4 (single store, spike). The local-disk figures are from 2026-09-30 on the same laptop: 400 writes to a 64 MiB pre-zeroed file, with another session's build running.

| Write mode | Work per commit | Client round trips | Fence | p50 / p99 on the laptop | Arm A uses it for |
|---|---|---|---|---|---|
| TiKV TxnKV **1PC** (P4a) | TSO, fenced head read, prewrite, Raft append + apply | 3 | Write-write conflict on the head | 6.5–7.1 / 25–26 ms | Nothing |
| TiKV TxnKV **2PC** | TSO, read, prewrite, commit, two Raft appends | 4 | Same | 19–26 / 57–74 ms | **Metadata**: create, vote, elected, trim (one or two writes per election) |
| TiKV **RawKV** put, or CAS + put | One (or two) Raft appends | 1–2 | None, or a CAS on the head | 6.4–14.9 / 33–51 ms; fenced 13.8–29.8 / 85–595 ms | Nothing. Unfenced writes can lose acknowledged commits with one acceptor (§6.4) |
| **Local, buffered** `pwrite` + shared `fdatasync` | One `pwrite` and a share of one sync, on 2 of 3 acceptors in parallel | 1 (compute ↔ acceptor) | In memory, behind a durable vote | btrfs 2.4 / 3.0 ms per sync, `NOCOW` 2.1 / 2.8; ext4 0.9 / 1.3 | **The commit path**, tier `buffered` |
| **Local, direct** `O_DIRECT` + `RWF_DSYNC` (FUA), 4 KiB | One write, FUA or write + flush | 1 | Same | btrfs 2.2–2.7 / 3.6–4.4 ms at queue depth 1; ext4 0.9 / 1.5 | **The commit path**, tiers `uring` and `pwritev2` |
| Same, 16 writes in flight | — | — | — | btrfs 4.0–17.6 / 10.7–27.7 ms; ext4 2.4 / 49.6 ms | Not as a default. `--io-depth` stays small, and one unit carries many commits |

**What the table says.**
- **On this drive a FUA write costs about as much as a cache flush.** So on equal filesystems, the direct tiers and `fdatasync` land close together.
- **btrfs adds its log-tree commit** to every durable write: 2.1–2.7 ms here against about 0.9 ms on ext4, on the same drive.
- **A power-loss-protected drive matters more than any tier.** Its durable write is microseconds (§6.6), and that cannot be shown on the laptop.
- **What the tiers remove is everything else:** allocation, `fsync`s and renames on the commit path, a sync that waits for earlier writes, and one sync per timeline. The gate measures each tier separately (§7.3), and also on an ext4 partition, to separate the btrfs cost.

#### Open questions (Q261–Q264)

| # | Question | Needed by |
|---|---|---|
| Q261 | Membership changes for Arm A: reuse Neon's generations and `pull_timeline`, or have the control plane write a new member set in TiKV and re-sync the new member from the bucket plus a peer? | P4c |
| Q262 | Rewrite (purge) the live records of idle timelines forward, so they stop pinning old journal segments? | Before multi-tenant production |
| Q263 | Tune the defaults on PLP NVMe, where the durable write is microseconds and per-syscall overhead dominates: `--io-depth`, SQPOLL, and IOPOLL with poll queues | The three-node gate run |
| Q264 | Multishot `recv` with provided buffer rings on the shards' sockets, and `SO_BUSY_POLL` by default? | After Q263, if network or scheduling shows in the p99 |

The results of the Arm A gate runs are in §7.3.

## 8. PgDog routing (D236)

**Verified 2026-09-29.**

- **License:** `pgdogdev/pgdog` is **AGPL-3.0** (`LICENSE` is the GNU AGPL v3 text; `gh api repos/pgdogdev/pgdog`). There is no CLA. A closed **Enterprise Edition** exists (control plane, query monitoring, QoS), built from a private repo (docs.pgdog.dev/enterprise_edition).
- **Activity:** v0.1.60 (2026-09-24). Releases are weekly, and the last commit was 2026-09-28.
- **Features** (README; docs.pgdog.dev):
  - transaction and session pooling;
  - load balancing across replicas (round robin, random, least connections) with health checks;
  - failover detection from replication state (it is "not a replacement for Patroni");
  - sharding (hash compatible with Postgres partitioning, list, range, schema);
  - cross-shard queries with partial aggregate support, two-phase commit, and online resharding over logical replication;
  - prepared statements in transaction mode;
  - SCRAM, MD5 and TLS;
  - an admin database, OpenMetrics and a Helm chart (MIT).
- **Routing by database name:** each `[[databases]] name` is what clients connect to, with its own `host`, `port`, `database_name` and pool (docs.pgdog.dev/configuration/pgdog.toml/databases). That is exactly §23's D153 routing, without Loam writing a splice.

**Deployment.**

- **Where it runs:** PgDog is a Deployment in the Loam Postgres namespace, with an image from `ghcr.io/pgdogdev/pgdog` pinned by digest.
- **Config:** Loam's control plane renders `pgdog.toml` and `users.toml` from the `x/` and `X/` records into a ConfigMap and a Secret, and sends `RELOAD` to PgDog's admin database after each change.
  - **Database names:** `<db>` for `main` and `<db>__<branch>` for branches (this answers Q49's syntax; the `options` form stays unsupported).
  - **Hosts:** the branch's compute, plus read replicas (Neon replica computes) with `role = replica`.
- **Auth:**
  - PgDog terminates client auth (SCRAM) against `users.toml`. Loam provisions the same role and password into the compute spec, and PgDog uses them for server connections.
  - PgDog's *passthrough* auth forces `auth_method = plain` (docs.pgdog.dev/features/authentication), so it is not used.
  - Credentials come from the auth plan's credential store (Q30).
  - **TLS on both hops, with peer verification.** Clients reach PgDog only over TLS, with a certificate for PgDog's service name that clients verify (`sslmode=verify-full`); PgDog's non-TLS listener is not exposed. PgDog connects to computes over TLS with full verification of the compute's certificate against the cluster CA, and the compute spec accepts only TLS connections (`hostssl`). Where a service mesh provides mutual TLS between the pods, that satisfies the backend hop instead. Plaintext is refused on both hops.
- **Pooling mode:** transaction mode by default. Session mode is used for migrations, `LISTEN`/`NOTIFY` and advisory locks. Each database entry sets its own mode.
- **What PgDog cannot do:** it cannot wake a stopped compute, so there is no scale-to-zero. Scale-to-zero needs Neon's proxy and the `wake_compute` API (§5.2, Q113).

**AGPL risk, and the rules Loam follows.**

- **Never linked or vendored.** No PgDog crate appears in any `Cargo.lock`. `deny.toml` already rejects AGPL, and the rule is extended to the Loam Commons and loam-platform repos. No PgDog source is copied into any Loam repo.
- **Never modified.** Loam uses upstream images and configuration only. A needed fix is reported upstream (with the owner's approval) or worked around with configuration. If neither is possible, the fallback is used.
- **Why:**
  - AGPL-3.0 §13 requires anyone who **modifies** PgDog and lets users interact with it over a network to offer those users the **modified source**. Running Loam cloud's routing through PgDog is exactly "interaction over a network".
  - An unmodified PgDog carries no source obligation beyond the upstream source and license already being public. Images that Loam **distributes** (BYOC, the showcase's compose) must still ship the license and point to the corresponding upstream source.
  - Anyone who links or vendors PgDog into Loam's code would put that code under AGPL (D11).
- **Recorded in:**
  - `LICENSES.md` in the Loam Commons and deploy repos;
  - the license-check CI job (D-SC-6), which checks deploy images;
  - a `deploy/pgdog/README.md` that states these rules.

**Fallbacks.**

| | PgDog | Neon proxy | PgBouncer |
|---|---|---|---|
| License | AGPL-3.0 (service only) | Apache-2.0 (in the fork; may be modified) | ISC |
| Pooling | Transaction and session | None (it is a router) | Transaction and session |
| Route by database name | Yes | By endpoint (SNI or `options=endpoint=`) through `wake_compute` | Yes (`[databases]`) |
| Replica load balancing | Yes | No | No |
| Sharding | Yes | No | No |
| Scale-to-zero (wake) | No | **Yes**, through Loam's `wake_compute` (§5.2) | No |
| Loam work | Config rendering | The control-plane auth API (§5.2) | Config rendering |
| Latest | v0.1.60 (2026-09-24) | the fork (last upstream proxy fix 2026-05-25) | 1.26.0 (2026-09-23, fixes 3 CVEs) |

For production scale-to-zero, the expected shape is Neon's proxy in front, which wakes the compute and routes, and PgDog behind it per database for pooling. Q113 decides.

## 9. Where Loam can beat Neon beyond the WAL (P5)

Each item below is proposed. P5 plans each one separately, after P4.

| Idea | What | Expected gain | Cost and risk |
|---|---|---|---|
| **Shared object-store cache for pageservers** | Serve on-demand layer downloads (`remote_storage` reads) through an AZ-local cache tier built on `operon-cache` (RAM and NVMe, checksummed ranges over immutable objects) and `foyer`, shared by every pageserver in the AZ | Cold-tenant attach and on-demand layer download at NVMe latency after the first reader; fewer bucket GETs. Neon's layers are immutable, which is the case the cache is built for | A `remote_storage` backend in the fork (Neon's `GenericRemoteStorage` enum), with a small patch |
| **Layer files on Loam's formats** | Store image and delta layers as Loam objects | Little: Neon's layer format is tuned for page versions and GetPage@LSN, and Loam's columnar formats are not | **Not recommended.** Kept here because the owner asked |
| **The storage controller on TiKV** | Replace its Postgres (Diesel; tables for tenant shards, nodes, safekeepers, timelines and metadata health) with a persistence trait over TiKV | One fewer database to run; the control plane and the controller share TiKV | A large patch across `storage_controller/src/persistence.rs` that every fork rebase carries. Only after the fork is stable (Q48) |
| **Branch per agent workspace** | D155 on Loam Postgres (§15 §9) | Already designed; P2 enables it | — |
| **Scale-to-zero** | `wake_compute` in Loam's control plane, and Neon's proxy (Q113) | Idle databases cost only bucket bytes, which the WAL design (§6.7) makes true of the WAL tier as well | The proxy's control-plane API (§5.2) |

## 10. Maintaining the fork (D231, D241)

**The starting point, verified 2026-09-29.**

| Fact | Evidence |
|---|---|
| `dina-kar/neon` created as a GitHub fork. Upstream `main` is at `fa504217c` (2026-08-31, a typo fix). About 6 commits since October 2025, against 100+ a month through July 2025 | `gh repo fork`; `gh api repos/neondatabase/neon/commits` |
| Neon's staff: "our engineering team is currently 100% focused on unifying Neon and Lakebase on Databricks infrastructure … This is happening outside of the open source repos" (Discord, 2025-09-03, quoted in Discussion #12835). No open-source roadmap since | `neondatabase/neon` Discussion #12835 |
| Neon `main` pins its Postgres at **16.9 / 17.5** (May 2025) and has no `vendor/postgres-v18` | `neon/vendor/revisions.json`, `.gitmodules` |
| `neondatabase/postgres` has moved on: `REL_16_STABLE_neon`, `REL_17_STABLE_neon` and **`REL_18_STABLE_neon`**, last committed 2026-04-08/09, based on **16.12 / 17.8 / 18.2** | `gh api repos/neondatabase/postgres/branches/…` |
| Neon's patch size over upstream: 16 → 153 commits, 149 files, +6 325/−693; 17 → 155 commits, 149 files, +6 446/−818; 18 → 106 commits, 157 files, +7 606/−632 (compared against the merged upstream stamp) | `gh api repos/neondatabase/postgres/compare/<stamp>...REL_1x_STABLE_neon` |
| `neon/docs/core_changes.md` lists 23 core changes. Only two serve walproposer: the backpressure hook in `ProcessInterrupts`, and shutting walproposer down after the checkpointer | `neon/docs/core_changes.md` |
| Upstream today: **18.6 / 17.11 / 16.15** (2026-08-13, fixing 28 CVEs). 18.5 was never shipped. Next minors on 2026-11-12, 2027-02-11 and 2027-05-13. PG 14 reaches EOL on 2026-11-12. PG 18 GA was 2025-09-25 | postgresql.org/support/versioning, /developer/roadmap, the 2026-08-13 release note |
| So Neon's shipped compute is **6 minors behind on 17** (17.5 → 17.11). Even Neon's newest branches are 3 minors behind, and miss the May and August 2026 CVE fixes (at least 39) | the above |

**The `.gitmodules` URLs are relative** (`../postgres.git`), so `dina-kar/neon`'s submodules resolve to `dina-kar/postgres`, which P2 must create as a fork of `neondatabase/postgres` (Q110).

**Plan.**

1. **Catch-up** (P2, estimate: **2–3 engineer-weeks**).
   - Fork `neondatabase/postgres`.
   - Move the submodules to the `REL_16/17_STABLE_neon` heads. These are newer than `main`'s pin and may need extension changes that were never published (Q110).
   - Merge upstream 16.15 and 17.11.
   - Build the compute images in the fork's CI.
   - Run Neon's `pg_regress` and the Python `test_runner` subset for compute and safekeepers, plus `deploy/neon`'s smoke.
   - Drop v14 and v15, which are out of Loam's scope.
2. **PG 18 enablement** (P2 or later, estimate: **3–6 engineer-weeks**). Neon `main` has no v18 wiring. It needs:
   - `vendor/postgres-v18` from `REL_18_STABLE_neon` (18.2) merged to 18.6;
   - v18 in `libs/postgres_ffi` (bindgen, `pg_constants`, WAL record layouts);
   - `libs/wal_decoder` and the pageserver's per-version `walingest` paths;
   - `pgxn/neon` built against 18;
   - the compute Dockerfile and extensions.

   CNPG covers PG 18 for the showcase meanwhile.
3. **Steady state** (D241).
   - **Nightly job:** try merging upstream `REL_17_STABLE` and `REL_18_STABLE` into the `_neon` branches. Report conflicts. Build the compute image. Run `pg_regress`.
   - **Release day** (the second Thursday of Feb/May/Aug/Nov): merge the tagged minor, run the full suite, and publish `ghcr.io/dina-kar/compute-node-v17:<minor>` and `-v18` within a week.
   - **Out-of-cycle releases** follow the same path.
   - **Estimate:** **2–4 engineer-days per quarter** for two majors, because minor releases rarely touch the areas Neon patches (smgr, WAL redo, SLRU). Plus **about 1 engineer-week a year** for a new major's catch-up, and the Rust toolchain and dependency upkeep of the Neon workspace (`cargo deny` on the fork).
4. **Owning the rest.** Storage-side fixes in the pageserver, broker and controller are taken as needed, with a test per fix, like the `client-rust` fork (§20). The fork's `README` states that it is independent of Neon Inc.

## 11. Roadmap: phases (D233, D235)

Each phase is small stacked PRs. The P4 phases are behind the feature `loam-wal` and change no default path.

| Phase | Scope | Depends on | Done when |
|---|---|---|---|
| **P1** | CNPG for the showcase: `deploy/cnpg/` (kind or k3d, operator 1.30.1, Barman Cloud plugin 0.15.0, `ObjectStore` on RustFS), the `commons-pg` Cluster, a PITR test; §22's compose keeps plain `postgres:17.11` | — | PITR restores to a timestamp between two writes; OpenFGA's and GlitchTip's migrations run |
| **P2a** | Fork upkeep: `dina-kar/postgres`, submodule move, 16.15/17.11 merge, fork CI and images (§10 step 1) | Q110 | The fork's compute images pass `pg_regress` and `deploy/neon`'s smoke |
| **P2b** | The Loam control plane (§5) driving stock safekeepers: the spec endpoint, `notify-attach` and `notify-safekeepers`, the `C/` records, the storage controller with its database on CNPG | P2a; §23 N1–N2 | A compute started by Loam serves a branch; a pageserver migration pushes a new spec |
| **P3** | PgDog routing: config rendering, `RELOAD`, the branch-name scheme, `deploy/pgdog/` with the license rules | P2b | psql, psycopg 3, node-postgres and JDBC reach `main` and a branch through PgDog; transaction pooling holds |
| **P4a** | `operon-safekeeper`: codecs, acceptor, `WalStore`, the TiKV backend, readers and the interpreted sender, broker publishing; protocol tests from Neon's safekeeper tests and TLA+ traces; fault tests (instance kill mid-append, TiKV leader kill, term bump race) | P2b; Q112 | A compute runs against the WAL service in `deploy/neon`; the pageserver ingests; no acknowledged commit is lost under the fault set |
| **P4b** | The benchmark harness (§7): `deploy/loam-pg-bench/`, `scripts/loam-pg-bench/`, result files, the manual workflow | P4a | Baseline repeated 3 times; noise band recorded |
| **P4c** | The switch-over (§6.10) behind `loam-wal`, per database | P4b gate passes | Merged only with a results table that meets §7 |
| **P5** | Pageserver efficiency (§9): the shared cache first, then scale-to-zero (Q113), then the controller on TiKV | P4c (or P2b for the cache) | Per item |

## 12. Risks

| # | Risk | Mitigation |
|---|---|---|
| 1 | **The Loam WAL misses the p99 gate** (§6.6: one more read and one TSO than a safekeeper) | D233: it stays behind the feature, and safekeepers stay. Tuning (async IO, leader placement, TSO prefetch, PLP NVMe) comes first. Pipelined appends (Q115) come next |
| 2 | **Protocol subtlety**: terms, truncation and the basebackup-LSN PANIC path in walproposer | Port `safekeeper.rs`'s state machine with its tests. Replay Neon's TLA+ model (`safekeeper/spec/`) as trace tests. Run Neon's Python safekeeper tests against the service in the fork's CI |
| 3 | **Building `wal_decoder` needs Postgres headers** in the Loam build | Feature-gated (`interpreted`); headers cached per fork pin in CI (Q112) |
| 4 | **TiKV becomes the WAL's single point of failure** for every timeline | Three AZs; the WAL gets its own stores through placement rules (Q116); the bucket copy bounds RPO at 250 ms if TiKV is lost |
| 5 | **TiKV MVCC and compaction churn** from short-lived WAL keys | A dedicated keyspace with its own GC safe point; batched deletes; monitor RocksDB write amplification; Titan for large values if needed (verify) |
| 6 | **Fork upkeep outgrows the estimate** (PG 18 enablement; hidden dependencies between the `_neon` branches and unpublished extension code) | §10 steps 1–2 first, estimated separately. CNPG stays the showcase's Postgres regardless (D230) |
| 7 | **AGPL exposure** if someone patches or links PgDog | §8 rules, `deny.toml`, the license-check job; Neon proxy and PgBouncer as fallbacks |
| 8 | **RustFS with the Barman Cloud plugin** fails on SigV4 checksum headers | The two `AWS_*_CHECKSUM_*` variables; P1 tests it first |
| 9 | **Leader placement per timeline** costs PD rules at scale (one rule per timeline) | Group timelines by compute AZ into a few key ranges per AZ, not one rule each (Q117) |
| 10 | **A TiKV leader crash stalls commits** on its timelines until the election (about 10 s with the default ticks). Losing one of three safekeepers stalls nothing | Shorter Raft ticks on the WAL stores (Q117); PD leader transfer before planned restarts; P4b's fault run reports commit latency through a leader kill, and the owner decides whether that failover time is acceptable alongside the p99 gate |

## 13. Open questions

| # | Question | Needed by |
|---|---|---|
| Q110 | Fork `neondatabase/postgres` as `dina-kar/postgres` (required by the fork's relative submodule URLs), and base the catch-up on the `REL_1x_STABLE_neon` heads (16.12/17.8, which may need unpublished extension changes) or on `main`'s pins (16.9/17.5)? | P2a |
| Q111 | PGroonga for Zulip on CNPG: a custom image on `17.11-standard-trixie`, or a separate Cluster with an image that has it? | P1 |
| Q112 | Where the interpreted sender builds: in Operon behind a feature, with the fork's Postgres headers in CI, or as a small binary crate inside the fork's workspace that links `operon-safekeeper`'s `WalStore`? *P4a:* interim answer is the feeder (§6.7); the published `neon` image ships Postgres 14–17 server headers under `/usr/local/v1x/include`, so CI can extract them for `postgres_ffi` | P4a plan |
| Q113 | Scale-to-zero: run Neon's proxy (with Loam's `wake_compute`) in front of PgDog, or accept always-on computes for Loam Postgres in the first release? (Supersedes Q47) | P3 plan |
| Q114 | Re-measure RawKV and TxnKV 1PC write latency on a three-node, three-AZ TiKV cluster with PLP NVMe, and collect published TiKV p99 figures for WAL-sized values | P4a |
| Q115 | Pipelining: more than one in-flight append per timeline (for example a separate term key with `Lock` mutations and a flush marker per batch), if the single-flight group commit limits throughput | P4b results |
| Q116 | The WAL on the metastore's TiKV cluster (own stores through placement rules) or on a dedicated TiKV cluster? | P4a plan |
| Q117 | Leader placement granularity (per timeline or per AZ group) and whether to shorten TiKV's election timeout for WAL stores | P4b |
| Q118 | Add `BatchCommands` RPC batching to Loam's `client-rust` fork (client-go's `max-batch-wait-time`), if P4b shows RPC overhead | P4b |
| Q119 | The Loam log layout for the bucket copy: one stream per tenant with a partition per timeline, or a stream per timeline (streams are cheap; partitions are fixed at creation) | P4a |

## 14. Contradictions with earlier decisions, and how they are resolved

| Earlier | Conflict | Resolution |
|---|---|---|
| D149 (§23): Neon for the showcase apps | D230: CNPG | **Superseded** (owner, 2026-09-29). Q45 is answered "no". D-SC-12's Postgres front end over TiKV stays long-term, as §23 §5 described for this case |
| D151 (§23): fork only when needed | D231: fork now | **Superseded** |
| D150 (§23): Loam as a client of Neon's APIs | D232: Loam serves the control-plane API | **Amended**: it extends D150 |
| D153 (§23): Loam's pg listener splices to computes | D236: PgDog routes | **Amended** for Postgres OLTP. The pg listener keeps analytics (PG1). Q49's syntax becomes `<db>__<branch>` |
| D11: no AGPL dependencies | PgDog is AGPL-3.0 | Not a dependency: an unmodified separate process, as for WeSQL (D148) and Alternator (D60) |
| D126: TiKV unmodified | The WAL tunes TiKV | Configuration only; no fork of TiKV or PD |
| D2 / D130: OLTP out of scope | Loam Postgres is OLTP | As in §23 §14: a separate service. D130's TiKV-backed product-line exception covers the WAL's hot tier |
| §22 §6.4: "one Postgres 17" | CNPG | Consistent: CNPG runs Postgres 17 (§22 is amended to name CNPG) |
| Q47, Q48 (§23) | §5, §8 | Q47 → Q113. Q48 answered: the storage controller's database is a small CNPG Cluster; TiKV later (§9) |

## 15. Sources

Read on 2026-09-29.

- **Neon (fork `dina-kar/neon` at `fa504217c`):**
  - `pgxn/neon/walproposer.c`, `walproposer.h`, `walproposer_pg.c`, `neon_walreader.c`;
  - `safekeeper/src/{safekeeper.rs, receive_wal.rs, wal_storage.rs, control_file.rs, send_wal.rs, send_interpreted_wal.rs, wal_backup.rs, wal_backup_partial.rs, remove_wal.rs, timeline_manager.rs, broker.rs, handler.rs, http/routes.rs}`, and `safekeeper/spec/*.tla`;
  - `storage_broker/proto/broker.proto`;
  - `pageserver/src/tenant/timeline/walreceiver/{connection_manager.rs, walreceiver_connection.rs}`, `pageserver/src/walingest.rs`;
  - `libs/wal_decoder/`, `libs/postgres_ffi/build.rs`, `libs/compute_api/src/{spec.rs, responses.rs}`;
  - `compute_tools/src/{spec.rs, config.rs, http/server.rs, compute.rs}`;
  - `storage_controller/src/{compute_hook.rs, http.rs}`;
  - `proxy/src/control_plane/client/cplane_proxy_v1.rs`, `messages.rs`;
  - `docs/{walservice.md, core_changes.md, safekeeper-protocol.md}` (partly out of date), and RFCs 009, 013, 025, 035 and 041;
  - `vendor/revisions.json`, `.gitmodules`.
- **Neon activity and statements:**
  - the `neondatabase/neon` and `neondatabase/postgres` commit, branch and compare APIs;
  - Discussion #12835;
  - neon.com/docs/introduction/architecture-overview, neon.com/blog/paxos, neon.com/blog/wal-s3-lakebase-storage-for-the-era-of-agents;
  - jack-vanlightly.com (2023-11-15, 2025-02-19; no latency figures);
  - clickhouse.com/blog/postgresbench-ha.
- **CloudNativePG:**
  - `cloudnative-pg/cloudnative-pg` (license, releases v1.30.1 and v1.30.0);
  - cloudnative-pg.io/docs/devel/{supported_releases, backup, recovery};
  - `cloudnative-pg/plugin-barman-cloud` (releases, docs/usage, docs/object_stores);
  - `cloudnative-pg/postgres-containers`;
  - cncf.io/projects/cloudnativepg.
- **PgDog:** `pgdogdev/pgdog` (`LICENSE`, `README.md`, `CONTRIBUTING.md`, releases, `CHANGELOG-ENTERPRISE.md`); docs.pgdog.dev (databases, authentication, prepared statements, enterprise edition); `pgdogdev/helm`.
- **PgBouncer:** `pgbouncer/pgbouncer` (`COPYRIGHT`, the 1.26.0 release).
- **Postgres:** postgresql.org/support/versioning, /developer/roadmap, and the 2026-08-13 and 2026-05-14 release announcements.
- **Object storage and log latency:**
  - AWS S3 performance guidelines ("tens of milliseconds");
  - the S3 Express One Zone product page ("single-digit millisecond");
  - docs.warpstream.com low-latency clusters, and warpstream.com/blog (2026-02-04);
  - docs.automq.com WAL storage.
- **TiKV:**
  - `pingcap/docs` `release-8.5`: `tikv-configuration-file.md`, `pd-configuration-file.md`, `configure-placement-rules.md`, `configure-load-base-split.md`, `latency-breakdown.md`, `tune-tikv-thread-performance.md`, and the 5.0, 5.3 and 8.0 release notes;
  - `tikv/tikv` `release-8.5` (`src/storage/mod.rs`, `src/storage/txn/commands/atomic_store.rs`, `components/api_version/src/api_v2.rs`), and tikv#10540;
  - tikv.org/docs/7.1/develop/rawkv/cas, tikv.org/docs/6.1/deploy/performance/overview;
  - the PingCAP Raft Engine blog;
  - `tikv/client-rust` (`src/store/request.rs`, PR #562), and `tikv/client-go` `config/client.go`;
  - bitsand.cloud/posts/cross-az-latencies; the AWS fault-isolation whitepaper (AZs); smalldatum.blogspot.com (2026-01, SSD power-loss protection and fsync); `dina-kar/client-rust` at `1f8962b` (`src/raw/client.rs`, `src/transaction/transaction.rs`, `src/config.rs`); `docs/plans/r1-dependency-spike.md` §(i).
- **Loam:**
  - §02 (WAL classes), §04, §20 (D126, D130), §22 §6.4, §23;
  - `crates/operon-log/src/writer.rs` (the `standard` write path: one flush in flight, ack after the PUT and `CommitWal`);
  - `crates/operon-meta-tikv/src/leases.rs` (`check_fence`);
  - `crates/operon-tikv`;
  - `deploy/neon/`, `deploy/tikv/`;
  - `deny.toml`;
  - D11, D60, D61, D72, D96, D126, D130, D148–D157, D-SC-6, D-SC-12, D-SC-16; Q30, Q45–Q49.
