# NET1 — Private Networking Implementation Plan (a pluggable tailnet: Tailscale BYOK or Headscale)

> **For agentic workers:** REQUIRED SUB-SKILL: use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Execute task by task, test first. Each task lists the interfaces it must produce and the tests that must exist and pass before it is done. Where this plan gives exact values (names, paths, ports, defaults), use them verbatim. The code is not pre-written in this plan (M0.3 Ruling 1).

> **Status: Planned** (2026-10-02). **Track MT** (design [§43](../design/43-private-networking.md), D580–D599, recorded in the [decision log](../design/13-decision-log.md); the owner's notes of 2026-10-02, "you can use Headscale if you want, for added security and unified handling", and later that day "for now make it **BYOK Tailscale**, make it configurable so I can add [Headscale] in the future"). **The owner chose Tailscale BYOK for now**: the default provider is `tailscale`, and every task below builds the provider seam first and Headscale as the alternative; switching is a configuration change only. Optional and independent of MT4 except Task 5, which needs MT4's control-plane crates (§41) and so waits for them. Identity comes from MT1 (Authentik). Branches `net1-t<N>`, stacked; PRs target `dev` (or `main` until `dev` exists). The hosted service's tailnet configuration (the Tailscale policy workflow and node, and the Headscale alternative at `headscale.loams.dev`) is **not** in this repository: it is `loam-platform` `deploy/tailnet/`, whose shared policy, config and checkers this plan's templates mirror.

**Goal:**
- **The provider seam:** `tailnet.provider = tailscale | headscale` and `tailnet.login_server` (empty for Tailscale), credentials per environment (never in Git), the same policy file and tags for both.
- A default-deny policy kept as code, **one file for both providers**, with tests that fail the build: an offline lint, Headscale's own checker (also the portability gate), and `tailscale/gitops-acl-action` `test`/`apply` for Tailscale (skipped without its OAuth secrets).
- A self-hosted template for either provider: a Tailscale node (compose service, k3s Deployment) and a Headscale stack (compose, k3s, config, an Authentik OIDC blueprint), pinned by version and digest.
- A policy renderer for BYOC tenants with generated positive and negative isolation tests, and an Authentik-roster sync that proposes pull requests.
- `loams-net`: the `NetProvider` trait with `HeadscaleClient` and `TailscaleClient`; BYOC join automation (single-use tagged keys; the agent chart's tailnet mode with a per-tenant provider knob, so a customer can bring their own Tailscale key or a Headscale).
- A nightly inventory diff that raises audit events.
- Docs for reaching an instance over a tailnet from the CLI, desktop and phones, and an e2e with real `tailscaled` containers (Headscale; Tailscale needs a live account and is a manual check).

**Architecture:**
- **Policy is the product.** Everything else is plumbing around one HuJSON file in the subset both providers share (`groups`, `tagOwners`, `grants`, `tests`), checked on every change by an offline lint, by Headscale's own `policy check` against a scratch database seeded with one node per tag and operator (the checker can only evaluate `tests` against nodes that exist), and, for Tailscale, by the API through `gitops-acl-action`.
- **The provider is a setting.** `--login-server` only when `tailnet.login_server` is set; `--advertise-tags` only when it is not (Headscale refuses a client-requested tag; verified). One rule in every node template.
- **Fail closed where Headscale does not.** Headscale starts even if its policy tests fail. The CI check and `NetProvider::apply_policy` therefore refuse a failing policy.
- **Hub and spoke for tenants.** A tenant tag reaches `tag:control:443` only; the control plane never dials in; the tenant section is generated between markers.
- **Nothing Loams-side names a provider** except `deploy/tailnet/`, `scripts/net/` and `crates/loams-net` (D599). Binaries speak to the OS's tailnet client by using a hostname.

**Tech Stack:** Tailscale client v1.102.5 (image `tailscale/tailscale` pinned by digest `sha256:c507f3a2a6ab1cabd8d809b98edeb41edbd5c3fb6ad9632ffd098b4c7d0b4065`), `tailscale/gitops-acl-action` v1.5.2, `tailscale/github-action` v4.2.0, Headscale v0.29.4 (image pinned by digest `sha256:8833f828b414c0907b7e5c71da76473216fe17cce0818a166b536ec552c0903f`), Caddy 2.11.4 (alpine, pinned), Alpine 3.24, rclone 1.75.1, Python 3.13 with `uv` and pytest, Rust 1.97.1 edition 2024 for `loams-net` (`reqwest`, `serde`, `tokio`, `wiremock`), `kubeconform`, `docker compose`. Task 0 re-checks every pin and every licence.

**Spec:**
- [§43](../design/43-private-networking.md) (all); §41 §5, §7, §11.2 (on the `byoc-control-plane` branch until merged); [§38](../design/38-knative-authentik-gitops.md) §4; [§37](../design/37-desktop-and-mobile-apps.md) §7.2; [§30](../design/30-loams-cli.md) §15.
- [MT1](2026-10-02-mt1-authentik-identity.md) Task 1 (the blueprint location and the guard).

## Global Constraints

Same as the M1 overview §8, plus:
- **No Headscale or Tailscale code in this repository.** Images are pulled; the REST APIs are spoken to; the policy format is HuJSON. No Tailscale client code is vendored.
- **Pinned by digest.** Every image in `deploy/tailnet/` has a tag and a digest; a CI check fails on an image without one.
- **No secret in Git.** Keys, OIDC secrets and S3 credentials are `.env` or Secret references; `gitleaks` runs on `deploy/tailnet/`. **Credentials are per environment** (an OAuth client or auth key in the environment's secret store).
- **Switching provider is configuration only.** A test (Task 4) renders every node template for both providers from the same variables and fails if anything but `TAILNET_PROVIDER`, `TAILNET_LOGIN_SERVER` and the key differs.
- **Default deny.** A policy file without a `grants` section, or with an `acls` section allowing `*:*`, fails the check (Task 1).
- **No public API.** The templates never publish `/api`, `/metrics`, `/debug` or `/swagger` on the public name.
- **No metering, billing, plan or price field** in any type, metric or API of `loams-net` (the open-core boundary; `scripts/ci/no-metering.sh` from MT4 Task 8 also scans `crates/loams-net`).
- **Python tools** run with `uv`, stdlib plus `pyyaml` and `pytest` only. **Rust** build: one cargo build at a time, the shared target, `-j 6`, lld; Headscale containers run only in the `net` CI job and the e2e job, never during a cargo build on the build machine.
- **Commit areas:** `net`, `deploy`, `ci`, `docs`.

## Rulings made while writing this plan

| # | Ruling | Why | Cost if wrong |
|---|---|---|---|
| 1 | **The checker seeds a scratch database.** `check.sh` starts Headscale once to migrate an empty SQLite file, `seed.py` inserts a user for every email and a tagged node for every `tag:*` in the policy (fake keys, addresses from 100.64.0.0/10), then `headscale policy check --bypass-grpc-and-access-database-directly` evaluates the `tests` | `policy check` resolves test sources against existing nodes; against an empty database every test fails with "resolved to no IP addresses". Verified on v0.29.4 | The `nodes` table layout can change between Headscale versions; Task 1's `seeder_matches_schema` and a bump-time run catch it |
| 2 | **Python for the renderer and sync** (as MT1's guard), **Rust only for `loams-net`** | Config rendering is text and tests; the provider is a library the control plane links | Two toolchains in one plan |
| 3 | **Caddy fronts Headscale.** TLS, no `/api` on the public name, a tailnet-only management listener on `:8081` | Keeps the REST API off the internet without splitting the DERP/TLS listener | One more component; Headscale's own ACME is the fallback for a one-container install |
| 4 | **The template's backup is an `age`-encrypted tar** (SQLite `.backup`, two private keys, the policy) | The Noise key is the server's identity and must be in the backup; encrypted so a bucket leak yields nothing | Key custody: the private age file is offline |
| 5 | **Authentik blueprint lives with MT1's** (`deploy/authentik/blueprints/loams-headscale.yaml`), reusing its `groups` scope mapping | One place for IdP configuration | Depends on MT1 Task 1's file layout |
| 6 | **Tailscale is the default provider; Headscale is the alternative** (owner, 2026-10-02) | The owner's BYOK choice; the seam is paid for once and the second provider is a configuration change | Free-plan limits and its non-commercial terms (D582, Q593); mitigated by the switch |
| 7 | **One policy file, the shared subset**, applied by the provider's tool: `gitops-acl-action` (`TS_OAUTH_CLIENT_ID`, `TS_OAUTH_SECRET`, skip if absent) for Tailscale, the reload hook for Headscale; the Headscale check runs in CI for both | The two share HuJSON but differ in `postures`, `ipsets`, autogroups and how tests are evaluated (§43 §3.1) | A Tailscale-only feature is wanted later; then the policy forks or the lint grows an exception |

## File structure

```
deploy/tailnet/
  policy/{example.hujson,policy_lint.py}        # shared by both providers
  tailscale/{compose/docker-compose.tailscale.yml,k8s/*,kustomization.yaml,.env.example}
  headscale/
    compose/{docker-compose.yml,Caddyfile,.env.example,backup/{Dockerfile,backup.sh,restore.sh}}
    k8s/{namespace.yaml,headscale.yaml,secrets.example.yaml}  kustomization.yaml
    config/config.yaml
    policy/{check.sh,seed.py}
    check-config.sh
  check-env.sh                                   # provider and login server must agree
deploy/authentik/blueprints/loams-headscale.yaml
scripts/net/
  render_policy.py, sync_groups.py, inventory_diff.py, tests/
crates/loams-net/            # NetProvider, HeadscaleClient, TailscaleClient, policy apply, key issue
charts/loams-byoc-agent/     # (MT4) values: net.mode, net.provider, net.loginServer, net.authKeySecret; tailscaled sidecar
docs/guides/private-networking.md
tests/e2e/net/               # docker compose: headscale + tailscaled containers
.github/workflows/net.yml     # lint, Headscale gate, gitops-acl-action (skipped without secrets)
```

## Tasks

### Task 0: Reconcile and check

**Produces:** a note in "Rulings made during execution": the pins re-checked (Headscale release and digest, Caddy, Alpine, rclone, Tailscale client) and their licences; whether §41 and MT4 have merged and where `loams-control` lives; MT1 Task 1's blueprint path; the result of a **client login test on all five platforms** against a Headscale with a mock OIDC provider (`headscale mockoidc`): Linux, macOS, Windows, iOS ("Use custom coordination server") and Android ("Use an alternate server"), with the client version of each; whether `tailscale-rs` can register against Headscale (a one-hour spike, for Q584 only); **on a throwaway Tailscale account: the free-plan limits as shown in the console (users, tagged resources, ACL groups), whether custom OIDC with Authentik signs up on Free (WebFinger on `loams.dev`; Q594), the OAuth-client scopes `gitops-acl-action` needs, and that `TS_CLIENT_ID`/`TS_CLIENT_SECRET` mint a key for the node template (Q593, Q594)**; that a peer is invisible to a node whose policy has no rule between them.

**Tests:** none (a reading and spike task). **Commit:** `docs: NET1 task 0 findings`.

### Task 1: The policy as code and its checker

**Files:** `deploy/tailnet/policy/{example.hujson,policy_lint.py}`, `deploy/tailnet/headscale/policy/{check.sh,seed.py}`, `scripts/net/tests/test_policy.py`, `.github/workflows/net.yml`.

**Produces:** the example policy (default-deny grants, tags owned by `group:ops`, a `tests` block with accepts and denies); `check.sh` (Ruling 1, container image or `$HEADSCALE_BIN`); the CI job that runs it **for both providers** (the Headscale check is the portability gate); a **lint** (`policy_lint.py`) that fails a policy with no `grants` or with an allow-all rule, a section outside the shared subset (`postures`, `ipsets`, ...), a tag without an owner, or a `tag:` used in a rule but not in `tagOwners`; and the workflow job that, when the repository variable `TAILNET_PROVIDER` is `tailscale` (the default) and `TS_OAUTH_CLIENT_ID`/`TS_OAUTH_SECRET` exist, runs `tailscale/gitops-acl-action` `test` on pull requests and `apply` on merge, and otherwise skips with a notice.

**Tests:** `example_policy_passes_check`; `failing_test_fails_check` (a fixture with `"deny"` turned into `"accept"` exits non-zero and names the failing pair); `empty_policy_is_rejected_by_lint` (`{}` is allow-all); `allow_all_grant_is_rejected_by_lint`; `unowned_tag_is_rejected_by_lint`; `seeder_matches_schema` (the `nodes` and `users` columns `seed.py` writes exist in a freshly migrated database); `tailscale_only_section_is_rejected_by_lint` (`postures` fails); `workflow_skips_without_secrets` (`actionlint` plus a dry parse of the `if` conditions); `reload_with_node_less_tags_is_refused` (documents the Headscale `SIGHUP` behaviour of §43 §3.1 against the seeded database); `ci_tag_cannot_reach_cluster` (a `tests` line asserting it, kept as a regression anchor).

**Commit:** `net: default-deny Headscale policy with an evaluated test suite`.

### Task 2: The tenant section renderer

**Files:** `scripts/net/render_policy.py`, `scripts/net/tests/{test_render.py,golden/*.hujson}`.

**Produces:** `render_policy(base_policy, tenants) -> policy`: between `// BEGIN GENERATED TENANT TAGS` and `// END GENERATED TENANT TAGS` (and the same markers around grants and tests) it emits, per tenant, `tag:byoc-<tenant>` owned by `group:ops`, a grant `tag:byoc-<tenant>` -> `tag:control` tcp 443, optional support grants (`group:ops` -> `tag:byoc-<tenant>` tcp 6443 with an expiry the caller supplies; expired entries are not emitted), and for every ordered pair of distinct tenants and for each operator tag a `deny` test. Tenant ids match `^[a-z][a-z0-9-]{1,30}[a-z0-9]$`. Output is deterministic (sorted).

**Tests:** `golden_two_tenants`; `golden_no_tenants` (markers present, empty body); `tenant_cannot_reach_other_tenant` (run through `check.sh` with 3 tenants); `tenant_cannot_reach_operator_tags`; `control_cannot_dial_tenant`; `expired_support_grant_is_dropped`; `bad_tenant_id_is_rejected`; `render_is_idempotent`.

**Commit:** `net: render the BYOC tenant section of the policy with isolation tests`.

### Task 3: Authentik roster sync

**Files:** `scripts/net/sync_groups.py`, `scripts/net/tests/test_sync.py`.

**Produces:** reads members of `loams-net-ops` and `loams-net-users` from Authentik's API (a service-account token with read permission only), edits `group:ops` and the operator `tests` lines in the policy, and opens a pull request (never pushes to the default branch, never reloads Headscale). A member of `loams-net-ops` who is not in `loams-net-users` is reported as a configuration error.

**Tests:** `adds_new_operator_and_a_test_line`; `removes_operator`; `no_change_no_pr`; `ops_not_in_users_is_error`; `never_writes_to_main` (the git client mock asserts a branch push only); `handles_pagination`.

**Commit:** `net: propose policy changes from Authentik's operator group`.

### Task 4: Node and server templates, and the OIDC blueprint

**Files:** `deploy/tailnet/tailscale/*`, `deploy/tailnet/check-env.sh`, `deploy/tailnet/headscale/{compose/*,k8s/*,kustomization.yaml,config/config.yaml,check-config.sh}`, `deploy/authentik/blueprints/loams-headscale.yaml`, `scripts/net/tests/test_templates.py`.

**Produces:** the **node template** (`tailscale/tailscale` pinned by digest; a compose service and a k3s Deployment in userspace networking; `TS_AUTHKEY` from the environment; the provider rule of §43 §6.5: `--login-server` only when `TAILNET_LOGIN_SERVER` is set, `--advertise-tags` only when it is not; `check-env.sh` rejects a provider and login server that disagree); the Headscale compose stack (Headscale, Caddy, backup sidecar), the k3s manifests (one pod, `Recreate`, one PVC, a `LoadBalancer` for 80/443/3478-udp, no published 8081), the config (OIDC optional, embedded DERP, `derp.urls: []`, `node.expiry: 90d`, `logtail` off, `taildrop` off, `only_start_if_oidc_is_available: false`), the blueprint (client `headscale`, groups `loams-net-users`, `loams-net-ops`, a policy binding), and `check-config.sh`. All images pinned.

**Tests:** `node_template_switches_provider_by_config_only` (render the node template for `tailscale` and `headscale` from the same variables; the only differences are the three variables and the resulting `TS_EXTRA_ARGS`); `node_template_joins_headscale` (a real `tailscale/tailscale` userspace node registers with a tagged pre-auth key and a granted port is reachable while another is dropped); `check_env_rejects_mismatch`; `no_key_in_git` (`gitleaks`); `config_loads` (`headscale configtest`); `images_are_pinned_by_digest`; `kustomize_renders_and_validates` (`kubectl kustomize | kubeconform`); `compose_config_is_valid` (`docker compose config`); `caddy_blocks_private_paths` (a request for `/api/v1/node`, `/metrics`, `/debug/pprof/`, `/swagger` through the public listener returns 404; the same API path on `:8081` reaches Headscale); `blueprint_passes_guard` (MT1's `guard.py`); `blueprint_applies_cleanly` (against the MT1 compose stack); `oidc_login_with_authentik` (real `tailscale up --login-server` driven headlessly through the flow executor, a user in `loams-net-users` joins, a user outside it is refused).

**Commit:** `deploy: Headscale compose and k3s templates with the Authentik client`.

### Task 5: `loams-net` and BYOC join automation

**Needs:** MT4 Task 1 (`loams-control` crates). **Files:** `crates/loams-net/src/{lib.rs,provider.rs,headscale.rs,policy.rs}`, `charts/loams-byoc-agent/values.yaml` and `templates/tailscaled.yaml`, `crates/loams-net/tests/*.rs`.

**Produces:** two implementations behind one trait, chosen by `tailnet.provider`: `HeadscaleClient` and **`TailscaleClient`** (Tailscale API v2 with an OAuth client holding only `auth_keys`, `devices` and `policy_file`: keys with tags and `reusable: false`, devices list/delete, ACL get/post with the ETag and `acl/validate` first). `trait NetProvider { async fn issue_join_key(&self, tenant: &TenantId, kind: KeyKind) -> Result<JoinKey>; async fn revoke_tenant(&self, tenant: &TenantId) -> Result<()>; async fn list_nodes(&self, tenant: &TenantId) -> Result<Vec<NodeInfo>>; async fn apply_policy(&self, rendered: &str) -> Result<()>; }`; `HeadscaleClient` over `/api/v1` with a bearer API key from the secret store (preauth keys create/expire/list, nodes list/delete/expire, policy get, and set in `database` mode); `apply_policy` runs the checker (Task 1) first and refuses on failure, then, by policy mode: `file` (default) commits the rendered file to the policy repository through the Git writer and relies on the host's reload hook (SIGHUP); `database` calls the API's policy `PUT`. Headscale v0.29.4 rejects API policy writes in `file` mode (verified in `hscontrol/grpcv1.go`); the agent chart's `net.mode: tailnet` with `net.provider`, `net.loginServer` and `net.authKeySecret` and a userspace `tailscaled` sidecar (no `NET_ADMIN`), off by default: a customer brings **their own Tailscale key** (`provider: tailscale`, empty `loginServer`) or joins a **Headscale** (`provider: headscale`, its URL) through the same knob (§43 §6.4); tenant offboarding deletes the tenant's nodes and removes its policy section; the support grant is a policy edit with an expiry that a reconciler removes.

**Tests:** `join_key_is_single_use_tagged_and_short` (wiremock asserts the request: `tags=["tag:byoc-acme"]`, `reusable=false`, expiration at most 1h); `ephemeral_kind_sets_ephemeral`; `reusable_requires_explicit_kind_and_max_24h`; `apply_policy_refuses_failing_policy` (nothing is committed and no PUT is sent); `apply_policy_file_mode_commits_to_policy_repo`; `apply_policy_database_mode_puts_policy`; `tailscale_join_key_is_tagged_nonreusable_short` (wiremock asserts the `POST .../keys` body); `tailscale_apply_validates_then_posts_with_etag`; `tailscale_oauth_scopes_are_minimal`; `tailscale_client_never_logs_secret`; `byoc_chart_tailscale_has_no_login_server_flag` and `byoc_chart_headscale_sets_login_server` (`helm template`); `file_mode_never_calls_policy_put`; `revoke_tenant_deletes_nodes_and_section`; `api_key_never_logged`; `support_grant_expires`; `agent_chart_renders_tailnet_mode` (`helm template` with and without `net.mode`); `no_billing_names` (the MT4 guard over the crate); integration `join_and_reach_control_only` against a real Headscale container.

**Commit:** `net: NetProvider, the Headscale client and BYOC tailnet mode`.

### Task 6: Inventory diff and audit events

**Files:** `scripts/net/inventory_diff.py` (or `loams-net`'s `inventory` module if MT4 has merged), tests.

**Produces:** `nodes list -o json` today vs the stored snapshot; findings: a new node, a new tag on an existing node, a server without tags, a tagged node unseen for 30 days, a node whose expiry is later than policy allows. Each becomes an audit event for the control plane (§41 §11.2; D221) or, standalone, a JSON line on stdout and a non-zero exit.

**Tests:** `new_untagged_server_alerts`; `new_tag_alerts`; `stale_tagged_node_alerts`; `no_change_is_silent`; `snapshot_is_atomic`; `audit_event_has_no_key_material`.

**Commit:** `net: nightly node inventory diff`.

### Task 7: Docs, the e2e and close

**Files:** `docs/guides/private-networking.md`, `tests/e2e/net/*`, the plan's status line, the decision-log paste in the canonical decision log, README rows.

**Produces:** the guide: choosing the provider (the setting, creating the tailnet and OAuth client, the free-plan limits and their non-commercial terms); running the template; joining each platform; MagicDNS names; the three TLS choices (D593); reaching an instance from `loams`, the desktop and the phones; the pairing `net` hint; what happens when the control server (Tailscale's or Headscale) or Authentik is down; backups and the restore drill; BYOC tailnet mode. The e2e (Headscale, Caddy, Authentik or `mockoidc`, and `tailscale/tailscale:v1.102.5` containers; Tailscale itself is not in CI because it needs a live account, so its steps are a documented manual check): (a) default deny, (b) tenant A cannot reach tenant B or an operator tag, (c) an ephemeral node is deleted after the timeout, (d) after restoring a backup into a new Headscale, existing nodes reconnect, (e) with Headscale stopped an established ping continues, (f) peers without a rule are absent from each other's network map. AP0's pairing fixtures gain a `net` case (a PR against the AP0 task, noted here).

**Tests:** the six e2e scenarios by those names: `e2e_default_deny`, `e2e_tenant_isolation`, `e2e_ephemeral_expiry`, `e2e_restore_reconnects`, `e2e_control_down_keeps_connections`, `e2e_peers_invisible_without_rule`; `docs_links_resolve`.

**Commit:** `docs: private networking guide and the Headscale e2e`.

## Exit

All tests above pass in CI; the e2e job is green; the guide's commands were run once on a fresh VM; the hosted service's runbooks (`loam-platform` `docs/deploy/tailnet.md` and `docs/deploy/headscale.md`) match the templates' pins; the decision-log paste is applied and §41 §7.4 is merged (recorded in the canonical decision log).
