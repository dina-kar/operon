# 23 — Neon and WeSQL: Postgres and MySQL on the Bucket, Beside Loams

Status: **Proposed** · 2026-09-29; **amended the same day by [§28 Loams Postgres](28-loams-postgres.md)** (owner decisions D230–D236). D149 (Neon for the showcase apps) and D151 (fork only when needed) are **superseded**: the showcase apps run plain Postgres 17 on CloudNativePG (D230), and Neon is forked now as `dina-kar/neon`, with Loams's control plane as its primary control plane (D231, D232). D150 and D153 are **amended**: Loams serves the control-plane API, and PgDog, not Loams's pg listener, routes Postgres OLTP connections (D236). Q45–Q48 are answered and Q49's syntax is settled there. The rest of this document stands; where the two disagree, §28 wins. **[§29](29-wesql-oltp.md) (2026-10-01, Proposed) proposes to amend D156**: WeSQL would get a four-milestone plan (foreign keys, a durable commit in the Loams WAL quorum, failover, an Iceberg bridge). Until §29's WS1 and WS2 pass their acceptance tests, **D156 and the Q50 gate stand unchanged**; WS2's design is a proposed answer to Q50, not a closure of it.

This document comes from a conversation with the owner about running **Neon** (serverless Postgres whose storage lives on object storage) and **WeSQL** (MySQL whose storage lives on object storage) next to Loams. Two points were settled in that conversation: neither engine is linked into the Loams binary, and both run unmodified as sidecar services on the same RustFS store while Loams integrates with them from the outside. Everything else here is a **proposal**, decisions **D148–D157** and open questions **Q45–Q51**. D149 proposed amending D-SC-12 and D-SC-16; the owner **declined** it on 2026-09-29, and **D230 supersedes it**: the showcase apps run plain Postgres on CloudNativePG, not Neon. The other decisions stay proposals, except where §28 amends them.

The spike of 2026-09-29 (§9) ran both engines on RustFS. Neon worked: psql, a branch created through the pageserver API, isolation between branches, pgoutput logical replication, OpenFGA and GlitchTip migrations, and recovery after the pageserver's disk was wiped. WeSQL ran basic SQL, but Forgejo's migrations failed on foreign keys, and commits made after the last snapshot were lost when the container was replaced without its local volume. The spike also found that **Neon's public repository has been nearly dormant since August 2025** (§4.1). That fact shapes D151.

Markers: **(spike)** means measured in the spike (notes: `.superpowers/research/neon-wesql-spike.md` in the design worktree, not committed). **(verify)** means not checked against a primary source; the PR that depends on it checks it first. **(estimate)** means computed, not measured. Paths of the form `neon/…` point into `neondatabase/neon` `main` as of 2026-09-29 (last commit `fa504217c`, 2026-08-31). Paths of the form `wesql/…` point into `wesql/wesql` branch `8.0` (last commit `3e29b8850`, 2026-08-29).

---

## 1. Summary

| # | Proposal | Status |
|---|---|---|
| D148 | **Neon and WeSQL run as separate, unmodified services** on Loams's RustFS store (their own buckets or prefixes). Loams never links either one; it talks to them over their HTTP admin APIs and wire protocols | Proposed (the sidecar model is the owner's direction) |
| D149 | **Superseded by D230 (§28 §4): CloudNativePG, not Neon.** Was: **Neon backs the showcase apps' Postgres OLTP** (Plane, Zulip, GlitchTip, Keycloak, OpenFGA; §22 §6.4). The other suite apps keep their own databases: Forgejo as shipped (TiDB is dropped by D-SC-16, and WeSQL fails its migrations, D156), Matomo on MariaDB (D-SC-14), and OpenPanel on its own Postgres until it is tested on Neon. The Postgres front end over TiKV (D-SC-12) narrows to **Loams Live's reactive, Convex-style API** and is not built as a general Postgres. **Amends D-SC-12 and D-SC-16** | Proposed; **needs the owner's confirmation** (Q45) |
| D150 | **Amended by D232 (§28 §5): Loams also serves the control-plane API.** **Loams is Neon's control plane.** A new crate, `loams-neon` (feature `neon`), drives the pageserver and storage-controller management APIs, writes compute specs and starts computes through a `ComputeRuntime` trait. One Neon tenant per (namespace, database); branches are timelines of that tenant | Proposed |
| D151 | **Superseded by D231 (§28 §10): the fork is owned now.** Was: **Neon is pinned and owned, with plain Postgres as the exit.** Images are pinned by digest to the last public build (2025-08-26). A fork (`dina-kar/neon`) is opened only when a fix is needed. Loams's routing (D153) and bridges (D154) address "a Postgres backend", so vanilla Postgres (without branching) can replace Neon for them by configuration: an `engine = postgres` record (D152) names an operator-provisioned instance with no branches, and D153 routes its database straight to that endpoint instead of to a branch compute. Provisioning that instance and a non-Neon `ComputeRuntime` are not designed here (N1 covers Neon only); branching (D155) has no plain-Postgres equivalent. The exit changes no Loams code path except branching | Proposed |
| D152 | **Mapping records in the TiKV metastore** (`loams-meta-tikv`): namespace database → engine, Neon tenant and default timeline (or WeSQL instance); branch → timeline, parent, ancestor LSN, compute endpoint and owner; bridge checkpoints | Proposed |
| D153 | **Amended by D236 (§28 §8): PgDog routes Postgres OLTP.** **Routing by database name in Loams's wire listeners.** The pg listener reads the `database` startup parameter. A mapped name is spliced byte for byte to the branch's compute, and any other name goes to Loams's analytics (PG1). The MySQL listener must terminate the handshake to see the database, so WeSQL routing authenticates in Loams and reconnects with a stored credential | Proposed |
| D154 | **Change bridges into Loams collections**: pgoutput logical replication from Neon, and the row-based binlog from WeSQL. They append `DocOp`s to a collection's implicit stream with idempotent producer sequences (D72), next to the TiKV bridge (D129), and give exactly-once delivery end to end | Proposed |
| D155 | **A Neon branch per agent workspace** (§15 §9), created and removed as Resonate saga steps (§21 §6.3) with deterministic ids and compensation. Branches are never merged: a workspace's result reaches `main` as code (migrations), not as data | Proposed |
| D156 | **WeSQL is gated and lower priority.** Not for Forgejo (it fails on foreign keys, §9.2). It is a candidate for Matomo only after Q50 and Q51 are answered. Until then MariaDB stays (D-SC-14), and WeSQL ships only as a dev compose | Proposed |
| D157 | **Track N**, six small PRs after PG1's read-only half: N1 `loams-neon` client; N2 metastore records; N3 pg routing; N4 logical-replication bridge; N5 branch-per-workspace saga; N6 WeSQL deployment, binlog bridge and MySQL routing | Proposed |

## 2. Goals and non-goals

### 2.1 Goals

1. **Real Postgres for the showcase apps, on the bucket.** Plane, Zulip, GlitchTip, Keycloak and OpenFGA need Postgres OLTP with interactive transactions, JSONB, arrays, `SELECT … FOR UPDATE` and, for Zulip, PGroonga (§22 §13a). Neon is Postgres (16.9 and 17.5 in its last build), so these apps run unmodified, and their data lives in the same RustFS store as Loams's.
2. **Branching as a Loams feature.** Every agent workspace gets a copy-on-write Postgres branch in milliseconds (36 ms in the spike, **(spike)**). This is the database counterpart of §15's Git branches and copy-on-write environments.
3. **One data plane.** Loams creates the databases, holds the mapping in its metastore, routes connections through its own listeners and copies changes into collections. Search, the activity feed and the assistant (§22 §8) then see the apps' data without asking the apps.
4. **License-clean.** Neon is Apache-2.0. WeSQL is GPL-2.0-only and runs only as a separate, unmodified process (D148).

### 2.2 Non-goals

- **Linking Neon or WeSQL into Loams.** Neither engine is a library, and WeSQL's GPL-2.0-only license rules out linking under D11.
- **Building a Postgres or MySQL engine.** Loams's listeners route and bridge. They do not execute OLTP SQL (D2 holds for the retrieval engine; D130).
- **Loams Live on Neon.** Loams Live stays on TiKV (§20). D149 only removes the need for a general Postgres over TiKV.
- **Neon's hosted features.** The console, the Data API, Neon Auth, autoscaling VMs and scale-to-zero through the proxy are out of scope in track N. Scale-to-zero is Q47.
- **Merging branches.** Neither Neon nor Postgres can merge data between timelines, and Loams does not pretend otherwise (D155).
- **Upstream work without approval.** No issues or PRs go to `neondatabase` or `apecloud` without the owner's explicit go-ahead.

## 3. Architecture

```
   apps (Plane, Zulip, GlitchTip, Keycloak, OpenFGA)     agents / psql / BI         Forgejo? Matomo?
                 │ postgres://…/<db>                          │                        │ mysql://…/<db>
                 ▼                                            ▼                        ▼
 ┌──────────────────────────────────────────────────────────────────────────────────────────────────┐
 │ Loams                                                                                            │
 │  pg listener (PG1, datafusion-postgres)           mysql listener (opensrv-mysql)                 │
 │   StartupMessage.database ─┬─ mapped? ── splice ──► Neon compute     terminate auth ── WeSQL     │
 │                            └─ else ── Loams analytics (collections, search functions)            │
 │                                                                                                  │
 │  loams-neon (feature `neon`): pageserver / storage-controller API, compute specs, ComputeRuntime  │
 │  Loams Durable (§21): branch sagas ─► create timeline → start compute → record → return DSN      │
 │  bridges: pgoutput (Neon) / binlog (WeSQL) ──► DocOps ──► collection implicit stream (D72, D129) │
 │  loams-meta-tikv: x/ databases, X/ branches, b/ bridge checkpoints                               │
 └──────┬───────────────────────────────┬───────────────────────────────────────┬───────────────────┘
        │ HTTP :9898 (mgmt)              │ Postgres wire :55433                  │ MySQL wire
 ┌──────┴──────────────────────┐  ┌──────┴────────────────────────────┐   ┌──────┴───────────────────┐
 │ Neon storage                │  │ Neon computes (stateless Postgres)│   │ WeSQL (single node,      │
 │ pageserver, safekeepers ×3, │◄─┤ one per (database, branch) in use │   │ SmartEngine on S3,       │
 │ storage broker, storage     │  │ compute_ctl + spec from Loams     │   │ local volume kept)       │
 │ controller                  │  └───────────────────────────────────┘   └──────┬───────────────────┘
 └──────┬──────────────────────┘                                                  │
        │ S3 (layers, WAL offload)                                                │ S3 (snapshots, binlog)
 ┌──────┴─────────────────────────────────────────────────────────────────────────┴───────────────────┐
 │ RustFS (Apache-2.0): bucket `loams` (Loams), bucket `neon`, bucket `wesql`                         │
 └────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

### 3.1 Components

| Component | Source | Role |
|---|---|---|
| Neon pageserver | `neon/pageserver`, image `ghcr.io/neondatabase/neon` | Serves pages to computes; writes image and delta layers to the bucket; its local disk is a cache (§9.1) |
| Neon safekeepers | `neon/safekeeper` | Paxos-replicated WAL service; offloads WAL to the bucket. Three in production, one in dev |
| Neon storage broker | `neon/storage_broker` | gRPC pub/sub between safekeepers and pageservers |
| Neon storage controller | `neon/storage_controller` | Tenant placement, generations, shard splits, pageserver failover. It needs its own Postgres database (`--database-url`). Not run in the spike (Q48) |
| Neon compute | image `compute-node-v16`/`v17` | Postgres with Neon's storage manager; started by `compute_ctl` from a JSON spec. Stateless |
| Neon proxy | `neon/proxy` | SNI routing, SCRAM, `wake_compute` for scale-to-zero. Its production backend calls a control-plane HTTP API that was never open source. Not used in N1–N5 (Q47) |
| WeSQL | image `apecloud/wesql-server` | MySQL 8.0.35 with SmartEngine on object storage; single node (multi-replica Raft removed on 2026-08-22) |
| RustFS | `rustfs/rustfs:1.0.0` | The one object store (D61) |

### 3.2 The Neon layout: tenants, timelines, computes (D150)

- **One Neon tenant per (namespace, database).** Neon branches whole Postgres clusters: a timeline holds every database in the cluster. If Plane and Zulip shared a tenant, a Plane branch would copy Zulip's data. Separate tenants keep branches, quotas and erasure per app. They cost nothing when idle: a tenant with no compute is only layers in the bucket.
- **The default timeline is `main`.** A branch is a timeline whose `ancestor_timeline_id` is its parent, optionally at an `ancestor_start_lsn` for point-in-time branches.
- **A compute per (database, branch) in use.** Loams writes the spec (`neon.tenant_id`, `neon.timeline_id`, the safekeeper list, the pageserver connstring, roles, `wal_level = logical`) and starts `compute_ctl` through a `ComputeRuntime`. The dev runtime uses Compose or Podman (`deploy/neon/`). A Kubernetes runtime comes later. A compute is replaced, never restarted (§9.1).
- **Ids.** Tenant and timeline ids are 16 random bytes in Neon. Loams derives them deterministically, the first 16 bytes of `SHA-256("neon/tenant/" + ns + "/" + db)` (`sha2` is already a workspace dependency) and the same for branches, so a retried saga step creates the same timeline rather than a second one (D155).

### 3.3 The WeSQL layout

One WeSQL instance per (namespace, database), each with its own bucket prefix (`WESQL_OBJECTSTORE_ROOT_DIR`) and a **kept local volume** (§9.2). WeSQL has an `objectstore_branch_id`, but branching was not tested in the spike and is not part of this design.

## 4. Upstream status, verified 2026-09-29

### 4.1 Neon

| Fact | Evidence |
|---|---|
| License **Apache-2.0**, with a `NOTICE` file | `neon/LICENSE`; GitHub `license.spdx_id` |
| Databricks announced the acquisition on **2025-05-14**. Neon's architecture now underlies Databricks' Lakebase Postgres | neon.com/blog/neon-and-databricks; neon.com/docs/introduction/neon-and-lakebase |
| Commits on `main`: 100 or more a month until **July 2025**, then 1 (Aug), 5 (Sep), 1 (Oct), 0, 0, 1, 1, 1, 0, 1, 0, 0, 1 (Aug 2026), 0 (Sep 2026) | `gh api repos/neondatabase/neon/commits?since=…` per month |
| The last day of normal staff activity is 2025-07-31 (commits tagged "Hadron", "BRC-…", "lakebase"). Since then: external contributions (a GCS provider, Direct IO alignment, typo fixes), README updates, one storage fix (`6a35a3e9f`, 2026-03-25), one proxy fix (`8f60b04da`, 2026-05-25) | commit log |
| The last releases are `release-9129` (2025-07-25), `release-compute-9073` (2025-07-28) and `release-proxy-8853` (2025-07-29) | GitHub releases |
| The last image is `ghcr.io/neondatabase/neon:latest`, created 2025-08-26, digest `sha256:ead56a7b…` | `podman image inspect` |
| The compute's Postgres is **16.9 / 17.5** (both May 2025); Neon carries patched Postgres forks as submodules | `neon/vendor/revisions.json` |
| The control plane (projects, endpoints, the proxy's auth API) is closed; the open parts are pageserver, safekeeper, broker, storage controller, `compute_ctl`, proxy and extensions | `neon/proxy/src/control_plane/client/cplane_proxy_v1.rs`; `neon/docker-compose/README.md` |

**Reading.** The public repository is effectively dormant, and development moved in-house. Apache-2.0 allows a fork, but Loams would own every fix, including Postgres minor releases rebased onto Neon's patched Postgres trees. This is the largest risk in this document (§12, row 1), and it is why D151 keeps plain Postgres as a working exit.

### 4.2 WeSQL

| Fact | Evidence |
|---|---|
| License **GPL-2.0-only** ("specifically available only under version 2 … without the 'any later version' clause … inherited from MySQL"). There is no top-level `LICENSE` file, so GitHub shows no SPDX id | `wesql/README.md` "Licensing" |
| The repository is `wesql/wesql`; `apecloud/wesql-server` no longer resolves on GitHub (404). The Docker Hub image is still `apecloud/wesql-server` | GitHub API; Docker Hub |
| Created 2024-09-23. Commits: 5 in Jan 2025, 2 in Mar 2025, **none from April 2025 to July 2026**, then 24 in August 2026 (the three-replica Raft removed, "keep single-node archive", #83; a MySQL 8.0.46 overlay and its designs, #90–#93). None since | commit log |
| No releases. Tags `8.0.35-0.1.0_beta1` … `beta5`. The newest image is `8.0.35-0.1.0_beta5.40`, from 2025-01-21 | tags; Docker Hub |
| SmartEngine carries RocksDB's BSD notice | `wesql/storage/smartengine/core/LICENSE` |

**Reading.** WeSQL is a beta from one company, with a long gap and a recent burst that removed multi-replica HA. It is usable for experiments, not for data the suite cannot lose. This supports D156.

## 5. Why Neon for the showcase apps, and what it changes (D149)

> **Superseded 2026-09-29** by D230 ([§28](28-loams-postgres.md) §4). The owner declined D149: the showcase apps run plain Postgres 17 on CloudNativePG with PITR to RustFS. This section is kept as the record of the proposal. The last paragraph ("If the owner declines D149") describes the path taken.

§22 wants the suite's apps on Loams end to end. D-SC-12 asks for Postgres write compatibility, and D-SC-16 points it at a Postgres front end over TiKV. That front end would have to be a Postgres-compatible OLTP engine: multi-statement transactions, `FOR UPDATE`, sequences, JSONB, arrays, `pg_trgm`, PGroonga and a catalog good enough for Django, Rails-like ORMs and Keycloak's Hibernate. That is years of work, and each app would still move only when its own test suite passed (§22 §13a).

Neon gives that compatibility today because it *is* Postgres, and it keeps D1's spirit: its durable state is in the bucket (layers and offloaded WAL), and its disks are caches, except for the safekeepers' WAL window. The spike ran OpenFGA's and GlitchTip's migrations on Neon unchanged (§9.1).

The proposal:

1. **Showcase apps use Neon** through Loams's routing (D153), and their changes flow into collections (D154).
2. **The Postgres front end over TiKV narrows** to Loams Live's reactive, Convex-style API (§20). The general Postgres front end is not built.
3. **D-SC-16 gains an exception.** "Transactional storage is TiKV only" becomes "TiKV for Loams Live, the metastore and durable state; Neon for Postgres-wire OLTP". There is still no TiDB.

| Earlier | Here | Resolution if the owner confirms |
|---|---|---|
| D-SC-12: Postgres write compatibility in Loams, TiKV the candidate store | Neon serves Postgres OLTP; Loams routes and bridges | D-SC-12 is met by Neon behind Loams's listener. The TiKV front end is limited to Loams Live's API |
| D-SC-16: no TiDB, transactional storage TiKV only | Neon is a second transactional store | Amended: TiKV for Live, meta and durable state; Neon for Postgres OLTP. There is still no TiDB |
| D2: OLTP out of scope | Neon is OLTP | D2 holds for the retrieval engine. Neon is a separate service, as D130 already allows for Live |
| §22 §6.4: "Postgres stays" | Postgres stays, as Neon, on RustFS | Consistent. Neon is Postgres |
| Q-SC-9: which store backs Postgres writes | Neon | Answered by D149, if confirmed |

**If the owner declines D149,** the suite keeps plain Postgres 17 (§22 §6.4) and the TiKV front end stays the long-term plan. N1–N5 still have value for agent workspaces (D155) and bridges (D154), and D151 makes routing and bridges on plain Postgres a configuration, not a rewrite.

## 6. Loams's integration points

### 6.1 `loams-neon` (D150, N1)

A crate behind the `loams` feature `neon`, off by default. It has no Neon code dependency: it speaks HTTP and JSON with `reqwest` (rustls) and `serde`.

- **`NeonClient`** calls the pageserver API: `PUT /v1/tenant/{t}/location_config` (attach with a generation), `POST /v1/tenant/{t}/timeline/` (bootstrap or branch: `new_timeline_id`, `ancestor_timeline_id`, `ancestor_start_lsn`, `pg_version`, `read_only`), `GET …/timeline`, `DELETE …/timeline/{tl}` and `GET /v1/status`. Once the storage controller runs (Q48), the same calls go through the controller, which then owns generations. The request and response shapes are `neon/libs/pageserver_api/src/models.rs` (`TimelineCreateRequest`, `TimelineCreateRequestMode::Branch`); Loams keeps its own copies of the few structs it needs.
- **`ComputeSpec`** builds `compute_ctl`'s JSON spec from Neon's docker-compose template: roles, databases, settings, `wal_level = logical`, and `fsync` left on outside dev.
- **`ComputeRuntime`** is a trait with `start(spec) -> Endpoint`, `stop(id)` and `status(id)`. N1 ships a Podman/Compose runtime for dev and tests. Kubernetes follows in a later plan.
- **Errors** map onto Loams's error model. Neon returns JSON `{"msg": …}` with HTTP status codes.

### 6.2 Metastore records (D152, N2)

In `loams-meta-tikv`'s keyspace. The prefixes `x/`, `X/` and `b/` were checked as unused against `crates/loams-meta-tikv/src/keys.rs` at `e69ab38`:

| Record | Key | Value |
|---|---|---|
| External database | `x/<ns>/<db>` | `engine` (`neon` \| `postgres` \| `wesql`), Neon `tenant_id` and `main` timeline (or the WeSQL instance and prefix), `pg_version`, credentials reference (never the secret), `state` (`creating` \| `ready` \| `deleting`), version |
| Branch | `X/<ns>/<db>/<branch>` | `timeline_id`, `parent`, `ancestor_lsn`, compute `endpoint`, `owner` (workspace or session id), `created_at`, `expires_at`, `state`, version |
| Bridge checkpoint | `b/<ns>/<db>/<branch>/<bridge>` | slot or binlog position (`confirmed_flush_lsn`, or `file:pos` while WeSQL runs with `gtid_mode = OFF`), target collection, producer id, version |

Writes are compare-and-set on the version, like pointers (§20 §11.2). Secrets live in the credential store of the unified auth plan (Q30), not in the metastore.

### 6.3 Routing by database name (D153, N3 and N6)

> **Amended 2026-09-29** by D236 ([§28](28-loams-postgres.md) §8): for Postgres OLTP, PgDog (unmodified, AGPL-3.0, a separate service) routes by database name (`<db>`, `<db>__<branch>`), with config rendered by Loams's control plane. The pg listener keeps analytics (PG1). The MySQL half below is unchanged.

**Postgres.** Postgres OLTP connections go to **PgDog**, not to Loams's pg listener (D236, §28 §8). Loams does not peek or splice:

- **Config from records.** Loams's control plane renders `pgdog.toml` and `users.toml` from the `x/` and `X/` records: one `[[databases]]` entry per mapped database, named `<db>` for `main` and `<db>__<branch>` for each branch (Q49's syntax; the `options` form is unsupported), pointing at that branch's compute. It writes them to a ConfigMap and a Secret and sends `RELOAD` to PgDog's admin database after every record change, so a new or deleted branch takes effect without restarting PgDog.
- **Unmapped names** have no PgDog entry. Loams's analytics stays on the pg listener exactly as PG1 plans (Ruling 1: the database is the namespace).
- **Auth.** PgDog terminates SCRAM against `users.toml`; Loams provisions the same role into the compute spec, and PgDog uses it for server connections.
- **Authorization before routing.** Loams renders a role into `users.toml` only for the databases its grants allow (the branch record's `owner` and the namespace's grants), so PgDog refuses an unauthorized database name before it opens a compute connection.
- **TLS** on both hops, with peer verification (§28 §8): clients connect to PgDog over TLS, and PgDog connects to computes over TLS that verifies the compute's identity (or an equivalent protected channel, such as mesh mTLS). Neither hop carries plaintext on the cluster network.
- PgDog stays reachable only inside the cluster (or on loopback in dev) until the unified auth plan (D111, Q30).

> **Proposed 2026-10-01** ([§31](31-loams-router-and-verification.md), D320): Vitess's vtgate (Apache-2.0, unmodified, v24) fronts WeSQL instead of the Loams-built splice below, for unsharded and sharded keyspaces alike, with Loams rendering vtgate's static auth file and VSchema. If §31's RT3 compatibility gate fails, the splice stays the fallback for unsharded WeSQL only; sharded MySQL then waits for §31's D317 router. Vitess-fronted WeSQL runs with `gtid_mode = ON` (§31 §9.1).

**MySQL.** The server speaks first, and the database arrives in the client's `HandshakeResponse`, authenticated against a scramble from the server's greeting. Loams therefore cannot splice before authentication. It completes the handshake itself (`caching_sha2_password` or `mysql_native_password`, with credentials from the auth plan), opens its own connection to WeSQL with a stored credential and the requested schema, over TLS that verifies WeSQL's identity (the stored credential never crosses a plaintext connection), and then splices. `COM_CHANGE_USER` and `COM_INIT_DB` to another database are refused. This is N6. The simpler fallback is a dedicated port per WeSQL instance.

### 6.4 Change bridges (D154, N4 and N6)

**From Neon (pgoutput).** One logical slot and one publication per (database, branch, bridge). The spike showed `wal_level = logical` on the compute, pgoutput messages for insert, update and delete, and **a slot that survives replacing the compute**, because Neon keeps slot state in the pageserver **(spike)**.

- **Reading.** A replication connection (`START_REPLICATION SLOT … LOGICAL … (proto_version '1', publication_names …)`). Buy before building: `supabase/etl` (Apache-2.0, active, pushed 2026-09-28) implements the pgoutput protocol in Rust (verify its crate boundaries and dependency cost against `deny.toml` in N4). The fallback is a small decoder over a replication-capable Postgres client.
- **Mapping.** Each table maps to a collection by a declared mapping (primary key → `_id`, columns → fields, optional text and vector fields). Insert and update become upserts, delete becomes a delete by `_id`, and `TRUNCATE` becomes a delete by filter (D87).
- **Exactly once.** Operations are appended to the collection's implicit stream with **producer id = (database, branch, bridge)** and **sequence = (commit LSN, ordinal in the transaction)** through D72's idempotent producers. Loams sends `confirmed_flush_lsn` back only after the append is acknowledged. A crash re-sends from the confirmed LSN, and the producer sequence drops the duplicates. This is the same contract as D129.
- **Operations.** A bridge is a link-style task under worker leases (§09) and honours the unapplied-data budget (D86). A lagging bridge holds WAL on the safekeepers and pageserver, so Loams alarms on slot lag and can drop a slot past a limit, which then requires a resync (initial copy by `COPY` at a snapshot, then streaming, as in Postgres' own table sync).

**From WeSQL (binlog).** `binlog_format = ROW` and `binlog_row_image = FULL` by default, with `gtid_mode = OFF` **(spike)**. The bridge registers as a replica (`COM_BINLOG_DUMP` from `file:pos`) and maps row events the same way. The sequence is `(file index, position)`. `mysql_async` (MIT or Apache-2.0) has a binlog stream client (verify it against WeSQL's consensus-era binlog events, which WeSQL dropped on 2026-08-23).

### 6.5 A branch per agent workspace (D155, N5)

§15 §9.2's session workflow gains a step `ws_db = ctx.run(fork_database, task.db, id)` beside `fork_branch`. As a saga on Loams Durable (§21 §6.3, pattern c):

| Step | Action | Idempotency | Compensation |
|---|---|---|---|
| 1 | Write `X/<ns>/<db>/agent-<id>` with `state = creating` | CAS on absence; the id is deterministic | Delete the record |
| 2 | `POST …/timeline/` with the derived timeline id and `ancestor_timeline_id = main`, optionally at the LSN of the workspace's base commit | Same id; `409` or an existing timeline counts as done | `DELETE …/timeline/{id}` |
| 3 | `ComputeRuntime::start(spec)` | The compute id is derived from the branch | `stop` |
| 4 | Record the endpoint, set `state = ready`, return the DSN (routed through Loams, §6.3) | CAS on version | Revert the state |

At the end of a session, the branch is **kept** (for review, with a TTL), **discarded** (compensate steps 3 → 1), or **promoted**: the database's `x/` record is repointed to the branch's timeline, but only if `main` has had no writes since the branch point. Otherwise promotion is refused, and the result lands as migrations applied to `main`. Idle branches cost only their layers. GC deletes expired timelines through a durable schedule (§21 §6.1).

## 7. Licenses

| Component | License | Use in Loams |
|---|---|---|
| Neon storage, compute_ctl, proxy | Apache-2.0 (`neon/LICENSE`, `NOTICE`) | Separate, unmodified services. `deploy/neon/` adapts two files from `neon/docker-compose/`, with attribution |
| Neon's patched Postgres (compute) | PostgreSQL License | Inside the compute image |
| Compute extensions | Various. PostGIS is GPL-2.0-or-later; timescaledb is built `APACHE_ONLY` (`neon/compute/compute-node.Dockerfile`) | Unmodified inside the compute image; Loams distributes none of them |
| WeSQL | **GPL-2.0-only** | A separate, unmodified service only. Linking or modifying it would put Loams's code under GPL-2.0 (D148, D11) |
| SmartEngine (in WeSQL) | BSD (RocksDB's notice) inside a GPL-2.0 work | As WeSQL |
| RustFS | Apache-2.0 | Existing (D61) |
| `supabase/etl` (candidate, N4) | Apache-2.0 | A dependency, if its footprint passes `deny.toml` |
| `mysql_async` (candidate, N6) | MIT OR Apache-2.0 (verify) | A dependency |
| OpenFGA, GlitchTip (spike tests) | Apache-2.0, MIT | Suite apps (§22) |

Running separate services creates no obligation on Loams's code (as D60 does for Alternator and D-SC-6 for the suite). Images Loams redistributes must carry the upstream notices, and a patched WeSQL (which Loams does not plan) would need a GPL-2.0 source offer.

## 8. Deployment

- **Dev:** `deploy/neon/` and `deploy/wesql/` (added in this PR, both run from scratch in the spike): RustFS, the Neon storage services with one safekeeper, and computes started against explicit ids; WeSQL with the RustFS virtual-host settings. Loopback ports, default credentials.
- **Production (later plan):** three safekeepers on separate nodes with volumes, one or more pageservers, the storage controller with its own Postgres database (Q48), computes managed by Loams's Kubernetes `ComputeRuntime`, and Neon's proxy only if Q47 chooses it. Buckets or prefixes per engine, with lifecycle rules that never expire Neon layers (Neon's GC owns deletion).
- **Resources (spike):** storage services under 0.5 GB of RAM together; each idle compute tens of MB (verify under load); images of about 5 GB (`neon`) and 1.3 GB (`compute-node-v16`). WeSQL used 0.7 GB of RAM.

## 9. Spike results (2026-09-29)

Rootless Podman 5 with docker-compose v5.2.0, RustFS 1.0.0, `neon:latest` and `compute-node-v16:latest` (both from 2025-08-26), and `wesql-server:8.0.35-0.1.0_beta5.40`. No Loams code was written and nothing was compiled. The full commands and outputs are in the spike notes.

### 9.1 Neon on RustFS

| Check | Result |
|---|---|
| Neon's docker-compose with RustFS in place of MinIO | **Works**, after three changes: bucket creation adds `x-amz-content-sha256` (RustFS rejects SigV4 requests without it; curl 7.88's `--aws-sigv4` does not send it); the pageserver data directory becomes a named volume (a bind mount failed with `Permission denied` under rootless Podman); and the compute entrypoint is replaced (the `compute-node` image has no `curl`, `jq` or `nc`) |
| Tenant and timeline through the pageserver API | Created; layers, `index_part.json` and `initdb.tar.zst` appeared in the RustFS bucket |
| Compute and psql | Ready in about 3 s; `PostgreSQL 16.9`; `wal_level = logical` |
| Branch through the pageserver API (`ancestor_timeline_id`) | **36 ms**; a second compute attached to it in seconds |
| Branch isolation | **Confirmed both ways.** The branch inherited 1 000 rows. After divergent inserts, deletes, updates and a `CREATE TABLE` on each side, neither saw the other's changes |
| pgoutput logical replication | Slot and publication created; `Begin`, `Relation`, `Insert` ×2, `Update`, `Delete` and `Commit` messages decoded. **The slot and its pending changes survived replacing the compute with a fresh container** |
| OpenFGA v1.21.0 | `openfga migrate` on Neon: done in 0.7 s. `openfga run` on Neon: a store, a model and a tuple; `check` returned allowed for the member and denied for the non-member |
| GlitchTip (image from 2026-08-08) | `manage.py migrate`: **all 169 migrations OK** in 49 s (219 tables) |
| The bucket as source of truth | With the computes stopped and the pageserver's local tenant directory deleted, the tenant was re-attached with generation 2: both timelines came back from RustFS, and the main data, GlitchTip's schema and the branch's divergent rows were all intact. The safekeeper's disk was not wiped |
| Compute restart | `restart` fails (Postgres' stale lock file in the kept container filesystem); **replacing** the container works. Computes are cattle |
| Not run | The Neon **proxy** (its production auth backend needs the closed control-plane API; the `postgres` and `local` backends exist only with the `testing` feature), the **storage controller** (it needs its own Postgres; upstream's compose omits it too), OpenFGA's and GlitchTip's own test suites (Go and pytest builds were beyond the machine budget), and anything on Postgres 17 |

### 9.2 WeSQL on RustFS

| Check | Result |
|---|---|
| First start with `provider = minio` and endpoint `http://rustfs:9000` | **Failed** after about 5 minutes: `curlCode: 6, Couldn't resolve host name`. mysqld's S3 client (the AWS C++ SDK, `wesql/mysys/objstore/s3.cc`) always uses virtual-hosted-style URLs |
| With `RUSTFS_SERVER_DOMAINS=rustfs,rustfs:9000` and the network alias `wesql.rustfs` | **Works.** Initialization and startup take 75–90 s. The first write can fail for a few seconds with `Consensus Not Leader` |
| Basic SQL | `8.0.35`. DDL, DML, JSON, `SELECT … FOR UPDATE`, commit and rollback all work. Every table is SmartEngine, and `ENGINE=InnoDB` is **silently rewritten** |
| Binlog | `ROW`, `FULL` row image, `gtid_mode = OFF`. Row events are visible, and binlog slices are archived to the bucket about once a second |
| Replace the container after a graceful stop, without the local volume | Recovered from the object-store snapshot; data intact |
| Replace it after `SIGKILL`, without the local volume | **Committed rows written after the last snapshot were lost**, including one written 20 s before the kill, although their binlog slices were in the bucket. Recovery restored the snapshot and did not replay the archived binlog. This may be a configuration issue (`raft_replication_archive_recovery = OFF`), but with the image's defaults, recent commits live only on the local volume (Q50) |
| Forgejo 16.0.5 migrations | **Failed:** `Error 1235: SE currently doesn't support foreign key constraints`, at the first table with a foreign key (118 tables in). Forgejo also documents MySQL 8.4+, and WeSQL is 8.0.35. `make test-mysql` was therefore not attempted |

### 9.3 What the spike changes

- Neon is technically ready for the suite's apps and for workspace branches. The risk is upstream maintenance, not function (D151, §12).
- WeSQL cannot serve Forgejo. Its S3-only durability claim does not hold under the image's defaults, and upstream just removed HA. It stays a dev option (D156).
- Both compose files were promoted to `deploy/` and re-run from scratch: Neon (main 10 rows, branch 5 rows after a divergent delete) and WeSQL (rows kept across a restart with the volume).

## 10. Testing (per PR)

- **N1:** unit tests against recorded pageserver responses, and an ignored-by-default integration test against `deploy/neon/` (tenant, timeline, branch, compute, `SELECT 1`, teardown).
- **N2:** records through the metastore conformance suite's CAS and absence cases, plus a TiKV playground test.
- **N3 (§28's P3):** rendering tests from records to `pgdog.toml` and `users.toml` (golden files); then `tokio-postgres` through PgDog: `<db>` and `<db>__<branch>` reach the right compute (`SELECT current_setting('neon.timeline_id')`); a branch created or deleted after `RELOAD` appears or disappears without dropping other databases' connections; a role not granted a database is refused; plaintext is refused on both hops; an unmapped name stays on Loams's analytics listener.
- **N4:** exactly-once under fault injection (kill the bridge between append and confirm; replace the compute mid-stream; drop the connection), checking that the collection equals the table.
- **N5:** saga crash tests at every step boundary; compensation leaves no timeline or compute behind; a promotion is refused when `main` has diverged.
- **N6:** WeSQL through the MySQL listener, and the binlog bridge under the same fault set as N4. Blocked on Q50.
- **Suite gate (in the Commons project):** OpenFGA's Postgres datastore tests and GlitchTip's test suite against Neon through PgDog, then Keycloak and Plane.

## 11. Roadmap: track N (D157)

> **Amended 2026-09-29** ([§28](28-loams-postgres.md) §11): N1 and N2 become inputs to §28's P2b (the Loams control plane), and N3's Postgres routing becomes P3 (PgDog). N4 and N5 run on Loams Postgres; N4 also runs against CloudNativePG. N6 is unchanged.

Each PR is small, stacked and behind the `neon` feature, and changes no default code path. N3 no longer extends PG1's listener (D236), so it depends only on N1 and N2. N6's MySQL routing still extends the MySQL listener.

| PR | Scope | Depends on | Done when |
|---|---|---|---|
| **N1** | `loams-neon`: `NeonClient`, `ComputeSpec`, `ComputeRuntime` with the Podman/Compose runtime; `deploy/neon/` used by its integration test | — | Integration test green against `deploy/neon/`; `cargo deny` clean |
| **N2** | Metastore records `x/`, `X/`, `b/` in `loams-meta-tikv`, with a typed API | N1 (types only) | Conformance CAS tests; TiKV playground test |
| **N3** | PgDog routing by database name (§28 P3): render `pgdog.toml`/`users.toml` from the `x/` and `X/` records, `RELOAD` after each change, `<db>__<branch>` names (Q49) | N1, N2 | §10 N3 tests; in-cluster only |
| **N4** | The logical-replication bridge: slot and publication management, pgoutput decode (`supabase/etl` or in-tree), mapping to `DocOp`s, idempotent producer, confirm after append, lag alarms, resync | N2, D72 (idempotent producers), M1.5 Task 9a (filter deletes) | §10 N4 fault tests exact |
| **N5** | Branch-per-workspace saga on Loams Durable: fork, start, record, compensate, keep, discard or promote; TTL GC schedule | N1–N3, D1 (Loams Durable) | §10 N5 crash tests |
| **N6** | WeSQL: `deploy/wesql/` hardened, MySQL listener routing (terminating), binlog bridge | Q50 and Q51 answered; the MySQL listener merged | §10 N6 |

Production deployment (the storage controller, three safekeepers, the Kubernetes runtime, and the proxy if chosen) is a later plan, after Q47 and Q48.

## 12. Risks

| # | Risk | Mitigation |
|---|---|---|
| 1 | **Neon upstream is dormant** (§4.1): Postgres 16.9 and 17.5 from May 2025, and no security releases since. Rebasing Neon's patched Postgres onto newer minors is specialist work | D151: pin by digest; follow Postgres security advisories and decide per CVE; fork only when needed; keep every Loams path working on plain Postgres. Q46 asks the owner whether Loams accepts owning a Neon fork |
| 2 | **The storage controller is untested here** and needs its own Postgres | Q48. Dev runs without it (emergency mode, as upstream's compose does). Production needs it for failover |
| 3 | **Safekeeper durability**: WAL not yet offloaded lives on the safekeepers' disks | Three safekeepers on separate nodes with volumes in production; a quorum loss before offload loses the tail. The same trade-off Neon itself makes |
| 4 | **A slot that lags pins WAL** and grows storage | Lag alarms and a slot-drop limit with resync (§6.4); bridges on branches only by opt-in (Q49) |
| 5 | **Wire routing breaks a client** (cancel requests, `SSLRequest` negotiation, GSSAPI) | Splice after the startup packet only. Cancel keys map to the compute through a per-connection table. N3's tests include psql, psycopg 3, node-postgres and JDBC (Keycloak) |
| 6 | **WeSQL data loss or stall** | D156: no suite data on WeSQL until Q50; kept volumes; MariaDB stays for Matomo |
| 7 | **License creep** if someone patches WeSQL or links a GPL component | D148; the license-check CI job (D-SC-6) covers deploy images |
| 8 | **Owner declines D149** | Track N still serves workspaces and bridges; the suite keeps plain Postgres (§5) |

## 13. Open questions

| # | Question | Needed by |
|---|---|---|
| Q45 | **Answered 2026-09-29: no (D230, §28).** **Owner confirmation of D149**: Neon for the showcase apps' Postgres OLTP, with D-SC-12's TiKV front end narrowed to Loams Live's API and D-SC-16 amended | Founder, before the N3 plan |
| Q46 | **Answered 2026-09-29: yes (D231, §28 §10).** Given §4.1, does Loams accept owning a Neon fork (Postgres minor rebases, security fixes), or should the suite default to plain Postgres 17 and keep Neon for workspace branching only? | Founder, before the N1 plan |
| Q47 | **Superseded by Q113 (§28 §8).** Scale-to-zero and SNI: implement the proxy's control-plane API in Loams (`get_endpoint_access_control`, `wake_compute`, JWKS) and run Neon's proxy, or keep Loams's listener as the only router and start computes on first connect | Eng, production plan |
| Q48 | **Answered 2026-09-29: a small CNPG Cluster; TiKV later (§28 §5.2, §9).** The storage controller's own database: plain Postgres beside it, or a Neon tenant bootstrapped without the controller? Is it needed for a single-pageserver install? | Eng, production plan |
| Q49 | **Syntax answered 2026-09-29: `<db>__<branch>` in PgDog (D236).** Branch selection on the wire (`options=-c loams.branch=…` or `<db>@<branch>`), how the principal is presented so Loams can authorize the branch before splicing (§6.3), and whether bridges run on branches (an inherited slot must be dropped or kept) | Eng, N3 plan |
| Q50 | **Still open;** [§29](29-wesql-oltp.md) §6 (WS2) proposes the answer. WeSQL durability: does archive recovery replay binlog slices from the bucket when configured (`raft_replication_archive_recovery`, or a newer build), so that losing the local volume loses nothing committed? | Eng, before N6 |
| Q51 | Matomo on WeSQL: does its installer and schema (no foreign keys, verify) run on SmartEngine, and does its archiver's load fit a single node? | Eng, before N6 |

## 14. Contradictions with earlier decisions, and how they are resolved

| Earlier | Conflict | Resolution |
|---|---|---|
| D2: OLTP out of scope ("Neon needed a Paxos tier") | Loams deploys and routes to Neon | D2 is about the retrieval engine's own storage. Neon is a separate service with its own Paxos tier (the safekeepers), as D130 already allows for Live |
| D11: no copyleft dependencies | WeSQL is GPL-2.0-only | Not a dependency: a separate, unmodified process (D148), as D60 does for Alternator |
| D42: a narrow protocol footprint | Postgres and MySQL routing | Loams routes; it adds no protocol it does not already serve (PG1, the MySQL listener) |
| D-SC-12, D-SC-16 | D149 | Proposed amendments, pending Q45 (§5) |
| D-SC-14: Matomo on MariaDB | WeSQL as a candidate | MariaDB stays until Q50 and Q51 (D156) |
| D123: TiDB for MySQL | WeSQL for MySQL | D123 is already paused by D-SC-16 for the suite. WeSQL does not revive it, and neither is on the suite's path until Q50 |

## 15. Sources

Read on 2026-09-29.

- Neon: `github.com/neondatabase/neon` (`LICENSE`, `NOTICE`, `README.md`, `docker-compose/{docker-compose.yml,README.md,pageserver_config/,compute_wrapper/}`, `libs/pageserver_api/src/models.rs`, `libs/remote_storage/src/s3_bucket.rs` (path-style with a custom endpoint), `proxy/README.md`, `proxy/src/binary/proxy.rs` (`AuthBackendType`), `proxy/src/control_plane/client/cplane_proxy_v1.rs`, `storage_controller/src/main.rs` (`--database-url`), `compute/compute-node.Dockerfile`, `vendor/revisions.json`); commit and release history through the GitHub API; `ghcr.io/neondatabase/neon:latest` and `compute-node-v16:latest` image metadata.
- Neon and Databricks: https://neon.com/blog/neon-and-databricks, https://neon.com/docs/introduction/neon-and-lakebase, https://www.databricks.com/blog/announcing-lakebase-public-preview.
- WeSQL: `github.com/wesql/wesql` branch `8.0` (`README.md` "Licensing", `mysys/objstore/s3.cc`, `include/objstore.h`, `storage/smartengine/core/LICENSE`, `patches/`, `.github/workflows/cicd-pull-request.yml`); commit history; Docker Hub `apecloud/wesql-server` tags; the image's `/apecloud/mysql/scripts/{libacenv.sh,libutil.sh,libmysql.sh}`.
- RustFS: `rustfs/src/server/layer.rs` (`RUSTFS_SERVER_DOMAINS`), `docker-compose.yml`.
- Candidates: `github.com/supabase/etl` (Apache-2.0), `github.com/blackbeam/mysql_async`.
- Loams: §02 (D72), §09, §14, §15 §9, §20 §11–§12 (D129), §21 §6.3, §22 §6.4, §13a, §13b; `docs/plans/2026-09-28-pg1-postgres-wire.md`; D1, D2, D11, D42, D60, D61, D86, D87, D111, D123, D130, D-SC-6, D-SC-12, D-SC-14, D-SC-16; Q30, Q-SC-9.
