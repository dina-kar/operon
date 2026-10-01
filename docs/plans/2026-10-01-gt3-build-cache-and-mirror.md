# GT3 — The sccache Backend and the Crates Mirror Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Execute task by task, test first. Each task lists the interfaces it must produce and the tests that must exist and pass before it is done. Where this plan gives exact values (names, paths, variables, routes), use them verbatim. The code is not pre-written in this plan (M0.3 Ruling 1).

> **Status: Planned** (2026-10-01). Design: [§36](../design/36-loam-git.md) §8 and §9 (D398, D399), §11 (D393). It needs only GT1 Task 1 (`operon-git`'s `ids` module: `NamespaceId`, `RepoId`, `Principal`) and is otherwise independent of GT1 and GT2 (it shares the gateway role and `operon-store`), so it can run in parallel with them. Track GT; branches `gt3-t<N>`, stacked; PRs target `main`. GT3 adds two crates and routes behind the off-by-default features `buildcache` and `registry` on the `operon` binary.

**Goal:**
- **Build cache** (`operon-buildcache`): sccache's WebDAV backend served by the gateway, with trust classes, refresh-on-hit (approximate LRU), a TTL and quota sweeper and usage hooks; plus the **direct path** recipe (sccache's S3 backend on R2, RustFS or S3 with vended, prefix-scoped credentials) and CI templates.
- **Crates mirror** (`operon-registry`): a crates.io sparse-index read-through with content-addressed `.crate` storage in the public-packages namespace, §15 §6's policy (allow and deny lists, pins, quarantine) and audit records.
- The gates: cold and warm builds of a fixture workspace through each cache path with the warm hit rate reported; an untrusted build cannot write `trusted/`; `cargo fetch` of a fixture lockfile through the mirror with egress limited to Loam.

**Architecture:**
- **Two crates, both transport-light:** `crates/operon-buildcache` (key layout, trust, the WebDAV subset as axum handlers over `operon-store`, the sweeper) and `crates/operon-registry` (index proxy, crate store, policy). The `operon` binary mounts them in the `gateway` role, **loopback only** until the unified auth plan (D111), with the listener flags `--cache-listen` and `--registry-listen` (or one `--gateway-listen` if Task 0 finds the shared gateway listener as built).
- **Tokens before the unified auth plan:** a file `--cache-tokens <path>` (TOML) maps a token's SHA-256 to `{namespace, repo, class, principal}`; the registry uses `--registry-tokens` likewise. Both are replaced by the auth plan's credentials, so the mapping is behind a `TokenResolver` trait.
- **No new state service.** Cache entries, index files and crates are objects; refresh-on-hit uses an in-place copy; the sweeper lists by prefix.

**Tech Stack:** Rust 1.97.1. Reused: `axum` 0.8, `reqwest` 0.12 (workspace; upstream fetches), `operon-store`, `operon-stream-grpc` or the in-process stream API for audit records (as built), `sha2`, `tokio`, `serde`/`serde_json`, `toml`. Test tools run as processes: `sccache` v0.18.0 (Apache-2.0, pinned), `cargo` (the toolchain's), `rustfs/rustfs:1.0.x` (D61).

**Spec:**
- [`docs/design/36-loam-git.md`](../design/36-loam-git.md) §8 (tools, the two paths, the trust model), §9 (the mirror), §10, §11.
- [`docs/design/15-agent-workspaces.md`](../design/15-agent-workspaces.md) §2 principle 5 (the public-packages namespace), §6 (the registry proxy), §7 (caches), §8 (credential vending).
- [`docs/design/25-clever-cloud-stack.md`](../design/25-clever-cloud-stack.md) §5 (`ObjectStoreProvider::issue_credentials`); [§35](../design/35-cloudflare-target.md) D381 (the `r2` provider and R2 temporary credentials).
- sccache docs: `docs/Configuration.md`, `docs/S3.md`, `docs/Webdav.md`, `docs/MultiLevel.md` (v0.18.0). Cargo: https://doc.rust-lang.org/cargo/reference/registry-index.html, https://doc.rust-lang.org/cargo/reference/source-replacement.html, https://doc.rust-lang.org/cargo/reference/registry-authentication.html.

## Global Constraints

Same as the M1 overview §8, plus:
- **Loopback only** (D111).
- **Never a writable shared cache for untrusted builds** (§36 §8.3). Every write path checks the class from the token, never from the URL or a header.
- **Upstream access only from the mirror.** Tests that check egress run cargo with `CARGO_HTTP_PROXY` pointing at a refusing proxy, so only the mirror can reach upstream (a local stub of `index.crates.io` and `static.crates.io` in tests; real crates.io only in the manual run of Task 6).
- **Limits** (documented through the limits table, D88): cache entry ≤ 512 MiB; WebDAV request body ≤ 512 MiB; index file ≤ 16 MiB; crate ≤ 64 MiB (crates.io's own cap is lower).
- **The build machine.** sccache builds of the fixture workspace only; the Loam workspace build through the cache runs nightly in CI, never locally during other agents' builds.
- **Commit areas:** `cache`, `registry`, `api`, `ci`, `docs`.

## Rulings made while writing this plan

| # | Ruling | Why | Cost if wrong |
|---|---|---|---|
| 1 | **The gateway path is WebDAV, not an S3 facade** | sccache has a WebDAV backend; serving the S3 API's SigV4 and multipart for one client would be much more code; the direct path already covers S3 | If sccache's WebDAV backend needs more of WebDAV than Task 0 finds, the subset grows (Q394) |
| 2 | **Approximate LRU by refresh-on-hit**: a `GET` hit on an entry whose last-modified is older than `refresh_after` (7 days) triggers an asynchronous in-place copy | No per-read index; at most one copy per entry per week | Entries hit only on one day a week may be evicted early by the TTL; the sweeper's age is the TTL (30 days) |
| 3 | **Per-repository cache keys, no cross-repository dedup** (§36 §8.3) | Poisoning and side channels; §15 principle 5 | Lower hit rates across forks of one project; a fork reads its parent's `trusted/` only if the parent grants it (Task 2's `read_from` list) |
| 4 | **The mirror stores crates once, in `ns/_public/packages/crates/`**, and applies policy per namespace at serve time | §15 principle 5's designated public namespace; policy differs per tenant, bytes do not | A crate a tenant may not see is still stored; it is never served to that tenant |
| 5 | **`auth-required: true`** on the mirror's `config.json` | The mirror is per namespace and loopback-only until the auth plan; cargo sends `Authorization` on every request with `auth-required` | Cargo older than the `auth-required` support fails; Task 0 records the minimum version |

## Carried in

From §36: Q390 (eviction on the direct path) is measured in Task 4; Q394 (the WebDAV subset) is answered in Task 0.

## Review Focus

1. **Trust.** Tests: Task 1 (`untrusted_put_to_trusted_is_403`, `scratch_is_private_to_its_principal`, `class_comes_from_token_not_path`), Task 4 (`untrusted_ci_cannot_write_trusted`).
2. **Integrity.** Tests: Task 5 (`crate_with_wrong_cksum_is_refused_and_not_stored`, `index_lines_are_served_verbatim`).
3. **Quarantine and policy.** Tests: Task 5 (`quarantined_version_is_hidden`, `denied_crate_is_404`).
4. **Usage hooks.** Tests: Task 2 and Task 5 (`hits_misses_puts_are_counted`, `package_requests_are_counted`).

## File structure

```
crates/operon-buildcache/
  Cargo.toml
  src/{lib.rs,keys.rs,trust.rs,tokens.rs,webdav.rs,refresh.rs,sweep.rs,metrics.rs,direct.rs}
  tests/{webdav.rs,trust.rs,sweep.rs,sccache_e2e.rs}
crates/operon-registry/
  Cargo.toml
  src/{lib.rs,index.rs,crates.rs,policy.rs,upstream.rs,audit.rs,metrics.rs}
  tests/{index.rs,crates.rs,policy.rs,cargo_e2e.rs}
  tests/fixtures/{upstream/…,lockfile-workspace/…}
crates/operon/Cargo.toml                    # features buildcache, registry
crates/operon/src/api/{buildcache.rs,registry.rs}  crates/operon/src/{server.rs,main.rs}
bench/fixtures/sccache-ws/                  # a small workspace: 6 crates, ~40 dependencies, one proc-macro, one bin
docs/guides/{build-cache.md,crates-mirror.md}
.github/workflows/ci.yml                    # job gt3 (path-filtered); nightly: the Loam workspace through the cache
.github/workflow-templates/loam-sccache.yml # the CI template (§36 §8.2)
```

### Task 0: Reconcile and check the clients

**Files:** read `crates/operon/src/{server.rs,main.rs,api/}`, `crates/operon-store`, the stream API as built. Fill "Rulings made during execution".

**Checks** (record each with its command):
1. **Q394:** run sccache v0.18.0 with `SCCACHE_WEBDAV_ENDPOINT` against a logging WebDAV server (a 60-line axum logger in a scratch crate, or `rclone serve webdav` if installed) for a cold and a warm build of the fixture workspace; record every method, path pattern, header and status code it uses (`GET`, `PUT`, `HEAD`, `PROPFIND` with `Depth`, `MKCOL`?). Also record how `SCCACHE_WEBDAV_KEY_PREFIX`, `SCCACHE_WEBDAV_TOKEN` and `SCCACHE_WEBDAV_USERNAME`/`PASSWORD` appear on the wire.
2. `SCCACHE_MULTILEVEL_CHAIN=disk,webdav` works in v0.18.0 (and which release first shipped it), and its write-error policy default (`SCCACHE_MULTILEVEL_WRITE_ERROR_POLICY`).
3. sccache's S3 backend against RustFS and (with owner credentials) R2: `SCCACHE_ENDPOINT`, `SCCACHE_REGION=auto`, `SCCACHE_S3_KEY_PREFIX`, `SCCACHE_S3_RW_MODE=READ_ONLY`; whether a read-only credential makes sccache fail or degrade to read-only.
4. Cargo's minimum version for `auth-required` sparse registries with `cargo:token`, and the `[source]` replacement syntax for a sparse mirror (`replace-with` to a `[registries]` entry with `index = "sparse+…"`).
5. Whether crates.io's sparse index lines carry a publish time (`pubtime`) today; if not, the quarantine reads `https://crates.io/api/v1/crates/<name>/<version>` (`created_at`) once per new version and caches it.
6. kellnr (Apache-2.0, v6.9.0): its crates.io proxy feature and S3 storage, for the docs' "self-hosted private registry" pointer (§36 §9).
7. How R2 temporary credentials are issued (`POST /accounts/{account_id}/r2/temp-access-credentials` with `bucket`, `parentAccessKeyId`, `permission`, `prefixes`, `ttlSeconds`) and whether RustFS has an STS-style or access-key API that the `rustfs` provider (§25 §5) can use for prefix-scoped keys.

**Commit:** `docs: reconcile GT3 with main and record the sccache and cargo checks`.

### Task 1: Keys, trust classes and the WebDAV subset

**Files:** `crates/operon-buildcache/src/{lib.rs,keys.rs,trust.rs,tokens.rs,webdav.rs}`, `crates/operon-buildcache/tests/{webdav.rs,trust.rs}`, `crates/operon/src/api/buildcache.rs`, `crates/operon/src/{server.rs,main.rs}`, `crates/operon/Cargo.toml`.

**Produces:**

```rust
pub enum TrustClass { Trusted, Untrusted }
pub struct CacheGrant { pub namespace: NamespaceId, pub repo: RepoId, pub class: TrustClass,
                        pub principal: Principal, pub scratch: bool, pub read_from: Vec<RepoId> /* parents whose trusted/ it may read */ }
#[async_trait] pub trait TokenResolver: Send + Sync { async fn resolve(&self, bearer_or_basic: &str) -> Option<CacheGrant>; }
pub struct FileTokens;   // --cache-tokens <path>: [[token]] sha256 = "…", namespace, repo, class, principal, scratch, read_from
pub struct CacheKeys;    // ns/<ns>/cache/sccache/<repo>/trusted/<key>, …/scratch/<principal>/<key>
pub fn router(store: Store, tokens: Arc<dyn TokenResolver>, config: CacheConfig) -> axum::Router;
pub struct CacheConfig { pub max_entry_bytes: u64 /* 512 MiB */, pub refresh_after: Duration /* 7 d */,
                         pub ttl: Duration /* 30 d */, pub quota_bytes: u64 /* 50 GiB per repo */ }
```

**Semantics:** routes under `/cache/sccache/<ns>/<repo>/…` implementing exactly the subset Task 0 recorded. `GET`/`HEAD`: trusted classes read `trusted/`; untrusted read `scratch/<principal>/` first when scratch is on, then `trusted/` of the repository and of each `read_from` repository; `404` on a miss. `PUT`: trusted → `trusted/`; untrusted → `scratch/<principal>/` if scratch is on, else `403`; bodies over `max_entry_bytes` → `413`. `PROPFIND`/`MKCOL` answered as Task 0 requires (collections are implicit). The URL's `<ns>`/`<repo>` must equal the grant's (`404` otherwise: no existence oracle). No token → `401` with `WWW-Authenticate: Bearer`.

**Tests:** `put_then_get_round_trip`; `head_reports_length`; `miss_is_404`; `untrusted_put_to_trusted_is_403`; `scratch_is_private_to_its_principal`; `untrusted_reads_trusted`; `read_from_parent_repo`; `class_comes_from_token_not_path`; `other_namespace_in_url_is_404`; `oversized_put_is_413`; `non_loopback_is_refused`.

**Commit:** `cache: serve sccache's WebDAV backend with trust classes`.

### Task 2: Refresh-on-hit, the sweeper and the hooks

**Files:** `crates/operon-buildcache/src/{refresh.rs,sweep.rs,metrics.rs}`, `crates/operon-buildcache/tests/sweep.rs`.

**Semantics:** Ruling 2: a hit on an entry older than `refresh_after` spawns one in-place copy (`object_store` `copy` to the same path; a per-key in-memory dedup set prevents concurrent copies). The sweeper (a worker task, lease `task/buildcache-sweep/<ns>`, hourly) lists each repository prefix, deletes entries older than `ttl`, then, if the repository is over `quota_bytes`, deletes oldest-first until under 90% of it. Hooks (§36 §11): `loam_buildcache_requests_total{org,namespace,result}` with `hit`, `miss`, `put`, `denied`; `loam_buildcache_bytes_total{direction}`; `loam_buildcache_stored_bytes` per namespace from the sweeper's listing.

**Tests:** `hit_on_old_entry_refreshes_once`; `fresh_hit_does_not_copy`; `sweeper_deletes_expired`; `sweeper_enforces_quota_oldest_first`; `sweeper_lease_is_exclusive`; `hits_misses_puts_are_counted`.

**Commit:** `cache: add approximate LRU, the TTL and quota sweeper, and usage hooks`.

### Task 3: The direct path

**Files:** `crates/operon-buildcache/src/direct.rs`, `docs/guides/build-cache.md`.

**Produces:**

```rust
pub struct DirectRecipe { pub env: Vec<(String, String)> }    // the SCCACHE_* variables for one grant
#[async_trait] pub trait CredentialVendor: Send + Sync {
    /// Prefix-scoped, short-lived S3 credentials: read-only for Untrusted, read-write on trusted/ for Trusted.
    async fn vend(&self, grant: &CacheGrant, ttl: Duration) -> Result<S3Credentials, VendError>;
}
pub struct R2Vendor;      // POST /accounts/{account_id}/r2/temp-access-credentials (permission object-read-only|object-read-write, prefixes, ttlSeconds ≤ 604800)
pub struct StaticVendor;  // a configured key pair per (repository, class), for RustFS and S3 until the providers of §25 §5 exist
pub fn recipe(grant: &CacheGrant, creds: &S3Credentials, endpoint: &str, bucket: &str, region: &str) -> DirectRecipe;
```

**Semantics:** the recipe sets `SCCACHE_BUCKET`, `SCCACHE_ENDPOINT`, `SCCACHE_REGION` (`auto` for R2), `SCCACHE_S3_KEY_PREFIX=ns/<ns>/cache/sccache/<repo>/trusted/`, `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`/`AWS_SESSION_TOKEN`, and `SCCACHE_S3_RW_MODE=READ_ONLY` for untrusted grants. `R2Vendor` maps a grant to `prefixes = [<that prefix>]` and the permission; it never vends `admin-*` permissions. **Credentials are the boundary, not sccache's settings:** a build can call S3 directly with the raw `AWS_*` values, so `SCCACHE_S3_KEY_PREFIX` and `SCCACHE_S3_RW_MODE` restrict nothing. Every vended credential must be restricted on the server to the grant's repository prefix, and to read-only for untrusted grants: R2 by the temporary credential's `prefixes` and permission; RustFS and S3 by a per-(repository, class) key whose bucket policy allows only that prefix and those actions. `StaticVendor` refuses to start unless each configured key declares its prefix and permission, and Task 3 checks them against the store (a write and an out-of-prefix read with an untrusted key must fail). The guide documents both paths, the trust model and eviction (age since write only on the direct path, Q390).

**Tests:** `recipe_for_untrusted_is_read_only`; `r2_vendor_request_body_matches_api` (against a mock of the Cloudflare API); `r2_vendor_never_requests_admin_permissions`; `static_vendor_per_class`; `static_vendor_refuses_undeclared_scope`; `untrusted_key_cannot_write_or_read_outside_prefix` (against RustFS with a bucket policy).

**Commit:** `cache: add the direct path's credential vending and recipe`.

### Task 4: CI templates and the end-to-end builds

**Files:** `.github/workflow-templates/loam-sccache.yml`, `bench/fixtures/sccache-ws/`, `crates/operon-buildcache/tests/sccache_e2e.rs`, `.github/workflows/ci.yml`.

**Semantics:** the template sets `RUSTC_WRAPPER=sccache`, `CARGO_INCREMENTAL=0`, `SCCACHE_BASEDIRS=${{ github.workspace }}` and, by input, either the WebDAV variables or the direct recipe, and maps the job's class from the event (`push` to a protected branch → trusted; `pull_request` from a fork → untrusted), with the token from the vending step (a placeholder until the auth plan). The e2e test runs, with the binary sccache on `PATH`: a cold build of the fixture workspace (gateway path, trusted), a warm rebuild after `cargo clean` (records the hit rate; non-cacheable crates per sccache's README excluded from the denominator), an untrusted build (reads hits, writes nothing to `trusted/`), and the same three on the direct path against RustFS. Nightly: the Loam workspace through the gateway path on CI runners, never on the build machine.

**Tests:** `cold_then_warm_hit_rate` (≥ 90% of cacheable compilations on the warm run); `untrusted_ci_cannot_write_trusted`; `direct_path_on_rustfs`; `multilevel_disk_then_webdav` (if Task 0 confirms the chain).

**Commit:** `ci: add the sccache CI template and the build-cache end-to-end job`.

### Task 5: The crates mirror

**Files:** `crates/operon-registry/src/{lib.rs,index.rs,crates.rs,policy.rs,upstream.rs,audit.rs,metrics.rs}`, `crates/operon-registry/tests/{index.rs,crates.rs,policy.rs}`, `crates/operon/src/api/registry.rs`.

**Produces:**

```rust
pub struct RegistryConfig { pub upstream_index: Url /* https://index.crates.io/ */, pub upstream_dl: Url /* https://static.crates.io/crates/ */,
                            pub index_ttl: Duration /* 60 s */, pub public_ns: NamespaceId /* "_public" */ }
pub struct Policy { pub allow: Vec<Pattern>, pub deny: Vec<Pattern>, pub pins: BTreeMap<String, semver::VersionReq>,
                    pub quarantine: Duration /* 0 = off; default 0 */ }
pub fn router(store: Store, policies: Arc<dyn PolicySource>, tokens: Arc<dyn TokenResolver>, config: RegistryConfig) -> axum::Router;
```

**Semantics:** §36 §9 exactly. `config.json`: `{"dl":"<base>/registry/crates/<ns>/dl/{crate}/{version}/{sha256-checksum}","auth-required":true}`. Index requests: path validated against the sparse layout and the lowercase name; a cached copy younger than `index_ttl` is served; otherwise the upstream is asked with `If-None-Match` (the cached ETag); `304` refreshes the timestamp; `200` replaces the copy; `404` upstream → `404`. At serve time each index line (a JSON object) is kept or dropped by the namespace's policy (deny, then allow, then pins, then quarantine); kept lines are served byte for byte; the response gets its own ETag over the filtered bytes. Downloads: the `cksum` in the URL must belong to a line the namespace may see; the crate is read from `ns/_public/packages/crates/sha256/<cksum>` or fetched from upstream, its SHA-256 verified, and stored create-only. Every download appends an audit record (`io.loams.dev.packages.download.v1` CloudEvent: namespace, principal, crate, version, cksum, source) to the namespace stream `_packages`. Hooks: `loam_packages_requests_total{org,namespace,ecosystem="crates",source}` (`cache`, `upstream`, `denied`), `loam_packages_bytes_total`.

**Tests** (against a local upstream stub serving fixtures): `config_json_has_dl_template`; `index_is_cached_and_revalidated_with_etag`; `index_lines_are_served_verbatim`; `invalid_index_path_is_404`; `crate_is_fetched_once_and_content_addressed`; `crate_with_wrong_cksum_is_refused_and_not_stored`; `download_of_hidden_version_is_404`; `denied_crate_is_404`; `pinned_versions_only`; `quarantined_version_is_hidden`; `audit_record_per_download`; `package_requests_are_counted`.

**Commit:** `registry: add the crates.io sparse-index mirror with policy and audit`.

### Task 6: The mirror end to end

**Files:** `crates/operon-registry/tests/{cargo_e2e.rs,fixtures/lockfile-workspace/}`, `docs/guides/crates-mirror.md`.

**Semantics:** `cargo fetch --locked` of the fixture workspace with `.cargo/config.toml`:

```toml
[source.crates-io]
replace-with = "loam"
[registries.loam]
index = "sparse+http://127.0.0.1:<port>/registry/crates/default/index/"
[registry]
global-credential-providers = ["cargo:token"]
```

and `CARGO_REGISTRIES_LOAM_TOKEN`, with `CARGO_HTTP_PROXY` set to a refusing proxy so only the mirror reaches the upstream stub. A manual run against real crates.io is recorded in the guide.

**Tests:** `cargo_fetch_through_mirror`; `cargo_cannot_reach_upstream_directly`; `second_fetch_is_all_cache`.

**Commit:** `registry: run cargo end to end through the mirror`.

### Task 7: Cloudflare variants (only if CF1 says these workloads ship on Workers)

**Files:** `deploy/cloudflare/operon-buildcache-worker/`, `deploy/cloudflare/operon-registry-worker/` (§35's workspace).

**Semantics:** the same routes in thin-mode Workers over the R2 binding (`R2Fs` and an R2 `Store` adapter per CF1's Q399 answer), the Cache API for index files, and `loam-tail` for usage (§35 D385). The handler logic is shared with Tasks 1–5 through a `wasm32`-compatible core module in each crate (`--no-default-features --features core`); Task 0 of this task confirms the split builds for `wasm32-unknown-unknown`.

**Tests:** the Task 1 and Task 5 suites run against `wrangler dev`.

**Commit:** `cache, registry: deploy the cache and the mirror as Workers`.

### Task 8: Docs and close

**Files:** `docs/design/36-loam-git.md` ("As built (GT3)" notes in §8 and §9; the measured hit rates), `docs/guides/{build-cache.md,crates-mirror.md}`, the limits table, `CHANGELOG.md`, this plan's rulings.

**Commit:** `docs: record GT3 as built and close the plan`.

## Rulings made during execution

| # | Ruling | Why | Cost if wrong |
|---|---|---|---|
| — | (Task 0 fills this table) | | |
