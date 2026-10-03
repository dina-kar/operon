# 43 — Private Networking: a Pluggable Tailnet (Tailscale BYOK or Headscale)

Status: **Proposed** · 2026-10-02 (revised the same day). Sources: the owner's note of 2026-10-02, "you can use **Headscale** if you want, for added security and unified handling", and the owner's ruling later that day, "for now make it **BYOK Tailscale**, make it configurable so I can add [Headscale] in the future." This document decides how: the private network is a **pluggable tailnet provider**, `tailscale` (the owner's own hosted Tailscale account, bring-your-own key; the default now) or `headscale` (self-hosted, later), and switching is a configuration change only.

It adds decisions **D580–D599** and open questions **Q580–Q599** (Q580–Q597 are used). They are **proposals** until the owner rules on them, except D580, which records the owner's two rulings. (The first draft of this document was Headscale-only; D582 and D585 to D599 were rewritten, not renumbered, for the provider seam.) Plan: [NET1](../plans/2026-10-02-net1-private-networking.md). The canonical log is the [decision log](13-decision-log.md).

**Amends** §41 §7 (the optional connectivity mode for BYOC in §41 §7.4), [§30](30-loams-cli.md) §15 (reaching a self-hosted instance), [§37](37-desktop-and-mobile-apps.md) §7.2.4 (private instances and pairing) and [§10](10-operations.md) (private networking). **Builds on** [§38](38-knative-authentik-gitops.md) (Authentik is the identity provider) and [§19](19-console-identity-and-agents.md). The hosted service's side (the Tailscale account's policy workflow and runbook, and the Headscale alternative at `headscale.loams.dev` with its own runbook) lives in the private `loam-platform` repository under `deploy/tailnet/`; this repository depends on none of it.

Markers: **(verify)** means not checked against a primary source; the task that depends on it checks it first. Every version, licence, plan-limit and status claim with a date was read on 2026-10-02 from the source named in §15. Nothing here was run against a live Tailscale account (none exists yet); what was run is stated in §3.1.

**Numbering.** D580–D599 and Q580–Q599 are this document's reserved ranges.

## 1. Summary

Loams runs two networks with two jobs:

- **Cloudflare** (Tunnel, Workers, DNS) is the **public front door**: `loams.dev`, `auth.loams.dev`, `console.loams.dev`. People and browsers.
- **A tailnet** (a WireGuard mesh coordinated by a control server) is the **private network**: operator SSH and `kubectl` with no public ports, k3s, TiKV and WeSQL traffic between sites, backups to the object store over private paths, the `loam-bench` CI runner, the Authentik admin interface, and an optional way for a customer's BYOC cluster to reach the control plane without opening an inbound port.

The control server is **pluggable** (D582). The official Tailscale clients, unmodified, connect to either:

| `tailnet.provider` | Control server | `tailnet.login_server` | Credentials | Status |
|---|---|---|---|---|
| **`tailscale`** (default) | Tailscale's hosted service, on the **owner's own account** (bring your own key, BYOK) | empty | a tagged auth key per node, or an OAuth client that mints them; an OAuth client for policy CI. Per environment, in the secret store, **never in Git** | **Chosen now** (owner, 2026-10-02) |
| **`headscale`** | A self-hosted Headscale (BSD-3-Clause), signed in through Authentik | `https://headscale.<domain>` | a tagged pre-auth key per node; an API key for automation | Documented and templated; switch to it when wanted |

The same policy file, the same tags, the same Authentik wiring and the same clients apply to both. Loams **documents and templates** the network; it does **not** build or embed a tailnet client (D592). The policy is HuJSON kept in Git and tested on every change, applied by the provider's own tool (D585).

Facts that shaped the design, several of which correct assumptions the questions carried:

1. **The provider is selected by one client flag.** `tailscale up --login-server=<url>` (or `--login-server` in `TS_EXTRA_ARGS` of the `tailscale/tailscale` container, which has no `TS_LOGIN_SERVER` variable) picks the control server; unset means Tailscale's hosted service. Nothing else on the node changes.
2. **Headscale cannot sit behind Cloudflare's proxy or Tunnel.** The Tailscale control protocol upgrades a `POST` with `Upgrade: tailscale-control-protocol`; Headscale's documentation says Cloudflare "does not support WebSocket POSTs as required by the Tailscale protocol" and that this setup "is not supported and will not work". A Headscale host is therefore a DNS-only record with public 443/tcp and 3478/udp (D583). Hosted Tailscale has no host for us to run.
3. **Neither provider can use Authentik groups in policy.** Headscale cannot use OIDC groups (its documentation); Tailscale with a custom OIDC provider has no user or group provisioning (its documentation; SCIM is a paid feature). Authentik groups can decide who may sign in (Headscale `oidc.allowed_groups`), not what they may reach. The policy names operators by email, kept in Git (D584).
4. **Tagged nodes do not expire**, on either provider. Servers need tags to be owned by the organisation rather than a person. Lifecycle is enforced by short-lived keys, ephemeral nodes for CI, an inventory diff, and deleting nodes on retirement (D589).
5. **Tailscale's free plan is for non-commercial use** (its pricing FAQ) with 6 users, 3 ACL groups and 50 tagged resources to start (§3.1). That is fine for the owner's pre-revenue lab and not for a commercial service with customers on it; the choice of plan is Q593.

### 1.1 The picture

```
                   people, browsers                       operators, servers, tenants' agents
                          │                                              │
        loams.dev / auth.loams.dev / console.loams.dev     tailnet control server (provider knob)
                          │                                              │
                  ┌───────▼────────┐                          ┌──────────▼───────────┐
                  │   Cloudflare   │                          │ A: Tailscale hosted  │  login_server empty
                  │ Workers/Tunnel │                          │ B: Caddy ─► Headscale│  :443 tcp, :3478 udp (DNS only)
                  └───────┬────────┘                          └──────────┬───────────┘
                          │ outbound tunnel                              │ B only: OIDC (code + PKCE)
                  ┌───────▼────────┐  ◄────────────────────────────────── │
                  │   Authentik    │  groups loams-net-users / -ops (B)    │
                  │ (admin routes  │                                       │
                  │ tailnet-only)  │            WireGuard mesh (tailnet, 100.64.0.0/10; MagicDNS names)
                  └────────────────┘   ops laptops ── k3s ── TiKV ── WeSQL ── RustFS ── loam-bench ── tag:byoc-<tenant> agents
```

## 2. Goals and non-goals

### 2.1 Goals

- No public SSH, `kubectl` or admin port anywhere in the hosted service.
- One identity (Authentik), one policy file, one place to see who can reach what.
- A default-deny network: a new node can reach nothing until a rule says so.
- BYOC clusters that can be managed without a single inbound port, with tenant-to-tenant isolation that does not depend on getting a firewall right per customer.
- Self-hosters can adopt the same thing from a template, or ignore it.
- **Switching provider is configuration, not a migration project:** one setting, one URL and a new key; the policy file, tags and clients stay (D582).
- The failure of the control server (Tailscale's or Headscale) degrades administration, never the data path.

### 2.2 Non-goals

- Replacing Cloudflare for public traffic, or putting end-user (browser) traffic on a tailnet.
- Carrying Loams's data plane between customers or between a customer and the hosted service. The tailnet is a management and east-west network; data-plane paths keep their TLS and token authentication (§19, §41 §7.3).
- Building a Loams tailnet client, or a Headscale fork (D592).
- Tailscale Funnel or Serve in anything Loams depends on (Headscale does not implement them, and the provider must stay swappable; D599).
- Making hosted Tailscale a hidden dependency: it is the owner's chosen **default** provider, a documented and replaceable one (D582, D590).
- Metering. The tailnet carries no usage records and `loams-net` has no billing surface (the open-core boundary of §41).

## 3. What was verified (2026-10-02)

The first table is Headscale and the client libraries; §3.1 is Tailscale (hosted) and the provider seam.

| Item | Finding | Source |
|---|---|---|
| Headscale release | **v0.29.4**, 2026-09-23 (v0.29.0 2026-06-17). Minimum Tailscale client v1.80.0. Repository pushed daily, 44k stars | GitHub releases, `juanfont/headscale` |
| Headscale licence | **BSD-3-Clause** | GitHub licence field |
| Features in the docs (`about/features`) | Node registration (web auth and pre-auth keys), DNS (MagicDNS, split DNS, search domains, extra records), Taildrop and Taildrive, tags, routes (subnet routers, exit nodes, via filtering), dual stack, ephemeral nodes, **embedded DERP server**, peer relays, policy features (ACLs, **Grants**, autogroups, auto approvers, **Tailscale SSH**, node attributes, **tests**), **OIDC** registration | docs, `about/features.md` |
| Not supported | **OIDC groups in policy**, Funnel, Serve, network flow logs. Device posture and IP sets in policy are also unsupported | `about/features.md`, `ref/policy.md` |
| Policy format | HuJSON (`policy.path`, mode `file` or `database`); reload on SIGHUP; **`SetPolicy` through the API is refused unless `policy.mode` is `database`** (`ErrPolicyUpdateIsDisabled` in `hscontrol/grpcv1.go`, v0.29.4); **no policy file means allow-all**; `"grants": []` means deny-all. `headscale policy check -f` validates a file and evaluates its `tests` against the nodes that exist (`--bypass-grpc-and-access-database-directly` works without a server) | `ref/policy.md`, CLI of v0.29.4 |
| OIDC | `issuer`, `client_id`, `client_secret`, `scope`, `allowed_groups`, `allowed_users`, `allowed_domains`, `pkce`, `email_verified_required`, `use_expiry_from_token`, `only_start_if_oidc_is_available`. **Authentik is a documented, supported IdP**; do not set an encryption key (no JWE) | `ref/oidc.md`, `config-example.yaml` |
| Keys | Pre-auth keys: one-time by default, 1 hour default, `--reusable`, `--ephemeral`, `--expiration`, `--tags`. Ephemeral nodes are deleted after `node.ephemeral.inactivity_timeout` (30 m default). Tagged nodes are exempt from `node.expiry` | `ref/registration.md`, config |
| Database | SQLite (default, where all new work is done) or Postgres, which the project calls "highly discouraged" and "supported for legacy reasons". One server process; no multi-replica mode (HA exists only for subnet routers) | `config-example.yaml` |
| DERP | Embedded DERP shares the HTTPS listener; needs 443/tcp and 3478/udp; `derp.urls: []` uses only your own map; `verify_clients` on by default | `ref/derp.md` |
| Reverse proxy | Needs WebSocket-style upgrade on POST with `Upgrade: tailscale-control-protocol`; nginx, Caddy, Apache documented. **Cloudflare Proxy and Cloudflare Tunnel are not supported** | `ref/integration/reverse-proxy.md` |
| Images | `ghcr.io/juanfont/headscale:<v>` and `docker.io/headscale/headscale:<v>`; v0.29.4 digest `sha256:8833f828b414c0907b7e5c71da76473216fe17cce0818a166b536ec552c0903f` (both registries). Documented: Docker and Podman. **No official Helm chart**; community charts exist, unaffiliated | `setup/install/container.md`, registries |
| Tailscale client | **v1.102.5**, 2026-09-29. `tailscale/tailscale`: **BSD-3-Clause**; "The macOS, iOS, and Windows clients … GUI wrappers … are themselves not open source". The Android app is open (BSD-3-Clause) and the iOS and Android apps use the open `tailscale` code | `tailscale/tailscale` README |
| Custom control server, per platform | Linux, macOS, Windows: `tailscale login --login-server URL` (macOS GUI: Option-click -> Debug -> Custom Login Server). **iOS: "Use custom coordination server"** in the login menu. **Android: "Use an alternate server"** (or an auth key). Headscale serves `/apple` and `/windows` help pages. Verified from Headscale's documentation, **not yet tested on a device** | `usage/connect/*.md` **(verify on a device in NET1 Task 0)** |
| `tsnet` (Go) | In `tailscale/tailscale`; `Server.ControlURL` sets the coordination server. Mature; a Go library | `tsnet/tsnet.go` |
| `libtailscale` (C) | BSD-3-Clause; wraps `tsnet`; `tailscale_set_control_url`. Needs the Go runtime (cgo) | `tailscale.h` |
| `tsnet` crate (Rust) | v0.1.0, 2023-03-12, 2.3k downloads, a one-release wrapper of `libtailscale`. **Stale** | crates.io |
| `tailscale` crate (Rust) | Official (`tailscale/tailscale-rs`), **BSD-3-Clause**, **v0.6.1, 2026-09-18**; "a work-in-progress … no compatibility guarantees". Has a `control_server_url` (`TS_CONTROL_URL`); Headscale compatibility is **not documented (verify)**. Unsupported: **MagicDNS, private DERP relays**, split DNS, subnet routers, exit nodes, peer relays, **iOS, Android**. The 0.5.0 page on docs.rs carried the warning "unstable and insecure … unaudited cryptography" | crates.io, `tailscale-rs` README |

### 3.1 Tailscale (hosted), credentials, policy as code, and what was run

| Item | Finding | Source |
|---|---|---|
| Control-server selection | `tailscale up --login-server=<url>` selects it; empty is Tailscale's hosted service. The `tailscale/tailscale` container reads `TS_AUTHKEY`, `TS_HOSTNAME`, `TS_EXTRA_ARGS`, `TS_USERSPACE`, `TS_STATE_DIR`, `TS_KUBE_SECRET`, `TS_CLIENT_ID`/`TS_CLIENT_SECRET` and more; **there is no `TS_LOGIN_SERVER`**: use `TS_EXTRA_ARGS=--login-server=...` | `cmd/containerboot` at v1.102.5 |
| Versions and licences | Tailscale client **v1.102.5** (2026-09-29), BSD-3-Clause; the hosted control server is proprietary. `tailscale/gitops-acl-action` **v1.5.2** (2026-04-27), BSD-3-Clause. `tailscale/github-action` **v4.2.0** (2026-09-22), BSD-3-Clause. Headscale **v0.29.4** (2026-09-23), BSD-3-Clause | GitHub releases and licence fields |
| Credentials | **Auth key**: tagged, reusable or single-use, used at registration. **OAuth client** ("trust credential"): `gitops-acl-action` needs the `policy_file` scope; `github-action` and containerboot mint **ephemeral, tagged, pre-approved** keys from a client with the writable `auth_keys` scope (the node must be given tags). Workload-identity federation (GitHub OIDC, no stored secret) also exists. API keys expire in 90 days | action `README`/`action.yml`, containerboot source |
| **Personal (free) plan** | **Up to 6 users, unlimited user devices, up to 50 tagged resources to start, up to 3 ACL groups, 1,000 ephemeral-resource minutes per month**; basic Tailscale SSH (up to 5 hosts); MagicDNS, subnet routers, exit nodes, Serve and Funnel; "nearly all of Tailscale's features". Paid only: SCIM, device-posture integrations, MDM configuration, advanced roles (Standard, $8 per user per month); just-in-time access, advanced SSH, network flow logs, log streaming (Premium, $18). **The pricing FAQ describes Personal as for individuals using Tailscale non-commercially** | tailscale.com/pricing and /pricing/faq (page text, read through a summarising fetch: re-read on the page before relying on a number) |
| **Custom OIDC on Free** | The 2023 GA announcement: "Any custom OIDC provider can be used for up to three users on our Free plan"; free or self-hosted providers under Starter; advanced or enterprise providers under Premium and Enterprise. Today's Personal cap is six users and the current custom-OIDC page does not restate the plan matrix: **confirm in the sign-up flow**. Requirements: a custom email domain serving **WebFinger** at `https://<domain>/.well-known/webfinger`; `openid`, `profile`, `email` scopes; ES256 or RSA >= 2048 signatures; one IdP per domain; **no user or group provisioning**; chosen **at tailnet creation** (an existing `@gmail.com` account cannot migrate). Authentik is a standards-compliant OIDC provider but is **not on the documented provider list I could read: likely, not verified** | tailscale.com/blog/custom-oidc-ga, /docs/integrations/identity/custom-oidc |
| Policy as code | `gitops-acl-action` inputs: `tailnet`, `oauth-client-id` with `oauth-secret` (or `audience` for federation) or `api-key`, `policy-file` (default `policy.hujson`), `action: test` (runs the tests, changes nothing) or `apply` (tests, then updates). It is a composite GitHub action around Tailscale's `gitops-pusher` and calls the API: **it has no offline mode** | action `action.yml` and `README` |
| Policy differences that matter | See the table below | Headscale `ref/policy.md` and `about/features.md` (v0.29.4); Tailscale docs |
| Headscale behaviours found by running it (v0.29.4, with the §10 template's policy) | (1) a tagged pre-auth key fixes the node's tags, and `--advertise-tags` on top is refused ("requested tags ... are invalid or not permitted"); Tailscale accepts both. (2) `tests` entries naming a tag or user with no node fail, at boot (logged, server starts anyway) **and on a `SIGHUP` reload (refused)**; so the checker seeds nodes, and a live server needs a node per tag in the tests | run 2026-10-02, private repository `docs/deploy/headscale.md` |

**Policy differences** (the file is HuJSON for both; the shared subset is `groups`, `tagOwners`, `grants`, `tests`):

| | Tailscale | Headscale v0.29.4 |
|---|---|---|
| Apply | API (`gitops-acl-action apply`); the admin console can also edit it, so CI is the only writer by convention | file on the server, reload with `SIGHUP`; API writes only in `database` mode |
| Test | the API validates and evaluates `tests` and `sshTests` (no node needed, per Tailscale's docs; not exercised here) | `headscale policy check`; `tests` need a node for every tag or user named |
| Users | identity-provider email | `name@` or the OIDC user |
| Groups | `group:` lists, up to 3 on Personal | `group:` lists; OIDC groups never usable |
| Autogroups | all | `internet`, `member`, `tagged`, `self`, `nonroot`, `danger-all` |
| `ssh` | Tailscale SSH (Personal: basic, 5 hosts) | supported; Loams uses OpenSSH on the tailnet (grants for `tcp:22`), which works on both |
| `nodeAttrs` | full | supported subset; no Serve or Funnel |
| `postures`, `srcPosture`, `ipsets` | supported (posture is a paid feature) | **not supported** |

The shared file therefore uses only the common subset, and the lint (`policy_lint.py`) fails a Tailscale-only section.

**What was run on 2026-10-02:** Headscale v0.29.4 under Podman with the shared policy file: `policy check` with the seeded database; a `tailscale/tailscale` v1.102.5 container with `TS_USERSPACE=true` and the entrypoint logic of §6.5 joined with a tagged pre-auth key, and an operator device reached the node's tailnet address on port 9000 (delivered to the server's localhost in userspace mode) while a port not granted was dropped. **Not run:** anything against hosted Tailscale (no account yet): sign-up, custom OIDC with Authentik, OAuth-client scopes, `gitops-acl-action`, plan limits (read from pages, not from a console).

## 4. Two networks, clear roles (D581)

| Concern | Cloudflare | Tailnet (Tailscale or Headscale) |
|---|---|---|
| Public HTTP: site, Auth.js callbacks, console, OIDC endpoints, API gateway | Yes (Workers, Tunnel) | No |
| Operator SSH, `kubectl`, Argo CD UI | No | Yes, no public port |
| Authentik admin UI and management API | Denied on the public name | Yes (`tag:authentik` tcp 9000, operators only) |
| k3s, TiKV, WeSQL, Loams cluster traffic between sites | No | Yes (§5.4 of the policy) |
| Backups, snapshots, S3 to the private object store | R2 for the hosted beta | Yes for RustFS/private paths |
| CI self-hosted runner (`loam-bench`) | Outbound to GitHub as before | Operator SSH and private object store |
| BYOC agent to the control plane | Default: outbound mTLS over HTTPS (§41 D543) | Optional mode (D587) |
| Tailnet coordination and DERP | **Cannot** for Headscale (D583): its host is DNS-only. Not applicable to hosted Tailscale | Is the tailnet's control plane |

The rule of thumb: if a person with a browser needs it, it is public; if a machine or an operator needs it, it is on the tailnet.

## 5. Identity (D584, D585)

Who may sign in, and what they may reach, are two questions with different answers per provider. In both, **what they may reach is the policy file**.

| | `tailscale` (default now) | `headscale` |
|---|---|---|
| Who runs sign-in | Tailscale, with the identity provider chosen **when the tailnet is created**: a major provider (GitHub, Google, Microsoft, Apple) or a **custom OIDC** provider on a custom email domain (§3.1) | Headscale, with OIDC to Authentik (authorization code + PKCE S256, confidential client `headscale`, redirect `https://<headscale-host>/oidc/callback`) |
| Custom OIDC (Authentik) | Available to the free plan on the 2023 announcement (up to 3 users then, 6 now: confirm), needs a WebFinger route on the email domain (`loams.dev`, a Cloudflare Worker rule) and cannot be added to an existing `@gmail.com` account. **Likely, not verified, with Authentik.** Until it is tried, the owner signs in with one major-provider identity and `group:ops` lists that email | Documented and supported with Authentik |
| Who may sign in | The tailnet's users (invited, or members of the custom domain). No group gate: no SCIM on Personal | Members of the Authentik group **`loams-net-users`**: Headscale's `oidc.allowed_groups` **and** an Authentik policy binding on the application |
| What they may reach | The policy file; `group:ops` lists operator **emails** in Git; tags are owned by `group:ops` | The same |
| Roster sync | The same job (`scripts/net/sync-groups`, NET1 Task 3) proposes the `group:ops` edit; with Tailscale the source of truth is the operator list itself (Authentik is not the roster) unless custom OIDC is used | Reads `loams-net-ops` from Authentik and opens a pull request that edits `group:ops` and adds the `tests` lines. It never applies a change itself |
| Group claim | n/a | The `loams-` prefix filter of MT1 ruling 2 applies: `loams-net-users` and `loams-net-ops` reach Headscale; an operator's other Authentik groups do not |

**Tags** are owned by `group:ops` (`tag:k3s`, `tag:tikv`, `tag:wesql`, `tag:objstore`, `tag:authentik`, `tag:headscale`, `tag:bench`, `tag:ci`, `tag:control`, and one `tag:byoc-<tenant>` per tenant). A server joins with a **tagged auth key** (Tailscale: from the admin console or minted by an OAuth client; Headscale: a tagged pre-auth key created by an operator or by automation holding a Headscale API key); the key fixes the tags, so a compromised server cannot add itself to a different tag. (On Headscale the client must not also pass `--advertise-tags`: it is refused. On Tailscale it is needed only when the key comes from an OAuth client.)

**Join keys** (D588): single-use, as short-lived as the provider allows (Headscale: one hour for people-created ones, 15 minutes for CI; Tailscale: the shortest expiry the console offers, or an OAuth client that mints an ephemeral key per start), always tagged, ephemeral for CI and autoscaled nodes. A **reusable** key exists only for an autoscaling group whose boot code can fetch it from the secret store, expires as soon as practicable (24 hours at most on Headscale) and is rotated by the same job that created it. **No key is ever in Git**: they live in the secret store of each environment (GitHub Actions secrets, `.env`, Kubernetes Secrets).

## 6. BYOC over a tailnet (D586, D587)

§41 §7.1 gives BYOC an outbound-only agent. This document adds an optional **tailnet connectivity mode** alongside it, for customers who prefer a private path to the operator's control plane, or who need the operator's support staff to reach the cluster on request. It never replaces the outbound-agent mode and it opens no inbound port in either. The same provider knob applies per tenant (§6.4).

### 6.1 Per-tenant isolation: one operator tailnet, generated policy (D586)

The options were one operator tailnet with policy isolation per tenant, or one tailnet (or Headscale) per tenant. Decision: **one operator tailnet, hub-and-spoke, with a generated policy**, and a dedicated Headscale, or the tenant's own tailnet (§6.4), for tenants who ask or whose regulation requires it. The tables say "Headscale" for the self-hosted case; with hosted Tailscale the same generated tags, grants and tests apply and the API applies them.

| | Shared tailnet, policy per tenant (chosen default) | One tailnet or Headscale per tenant |
|---|---|---|
| What a tenant node can see | Nothing but `tag:control`: peers are visible only when a rule allows traffic between them, so other tenants' nodes are not in its network map | Only that tenant's nodes |
| Blast radius of a policy mistake | All tenants, mitigated by generated rules and tests (below) | One tenant |
| Blast radius of a Headscale compromise | All tenants' management paths (not their data: §41 §7.3) | One tenant |
| Operator cost | One process, one backup, one policy | A process, DNS name, certificate, backup and key per tenant: real work for the control plane to create and run at hundreds of tenants |
| Tenant control | None over the policy | Full, if they run it |
| Tenant user access | Not provided: this tailnet is the operator's management network | The tenant's own network |

Why shared by default: the operator tailnet carries **management traffic only**, spoke to hub (`tag:byoc-<tenant>` -> `tag:control:443`), never spoke to spoke, and the control plane never dials into a tenant. With that shape there is little for a tenant to be isolated from beyond the hub, and the policy is mechanical. The tenant's **own** users and east-west traffic belong on **their** network: the tenant runs their own Headscale (or any WireGuard tool) and the Loams chart does not care. The tenant can run one on the same cluster; nothing in Loams couples to the operator's tailnet.

How the shared design is kept safe:

- The tenant section of the policy is **generated** from tenant records (`scripts/net/render-policy`, NET1 Task 2), never edited by hand; it sits between marker comments. On hosted Tailscale the free plan's **50 tagged resources** (§3.1) and 3 ACL groups bound the number of tenants on one tailnet: a paid plan or a dedicated tailnet is the answer, not a workaround.
- The generator emits, for every tenant, positive rules and `tests` that a tenant tag cannot reach any other tenant tag or any operator tag. `policy check` runs them in CI and in the control plane before it applies a change (`policy set` is refused on failure).
- A tenant's pre-auth keys are single-use, tagged `tag:byoc-<tenant>`, and created by the control plane only.
- A dedicated Headscale for a tenant (Q582) is the same template instantiated with a different `server_url`, `base_domain` and OIDC client; the control plane's `NetProvider` (§6.3) addresses either.

### 6.2 What the tailnet mode changes

| | Outbound-agent mode (default, D543) | Tailnet mode (optional, D587) |
|---|---|---|
| Customer opens | No inbound port; allows outbound HTTPS | No inbound port; allows outbound 443/tcp to Headscale, 3478/udp or DERP over 443 |
| Agent to control plane | mTLS HTTP/2 to the control plane's public name | The same protocol, to the control plane's tailnet name (`tag:control:443`); the public name need not be reachable |
| Cluster Git pulls | Direct from Git (§41 §6) | Unchanged: from Git over the internet (the tailnet is not a Git mirror) |
| Operator support access | None by default | A **time-boxed grant** `group:ops` -> `tag:byoc-<tenant>:6443` that the **customer** enables (Q588); it expires and is removed by the control plane |
| Air-gapped (Q550) | Customer-run Git mirror | Unchanged; the tailnet is not used |
| Observe-only | Allowed | Allowed: no support grant is created |

The agent's certificate (24-hour, per cluster) remains its identity; the tailnet adds a network path, not an authentication factor. A stolen tenant node key can reach `tag:control:443` and nothing else, where the agent still requires its client certificate.

### 6.3 Automation (the `NetProvider` seam)

The control plane gets a small trait, `NetProvider`: `issue_join_key(tenant, kind)`, `revoke(tenant)`, `list_nodes(tenant)`, `apply_policy(rendered)`. There are two implementations, chosen by `tailnet.provider` (D582, D598):

| | `HeadscaleClient` (`loams-net`, NET1 Task 5) | `TailscaleClient` (same crate) |
|---|---|---|
| API | Headscale REST (`/api/v1`), bearer API key (no scopes: full admin, Q591) | Tailscale API v2 (`/api/v2/tailnet/{tailnet}/...`), an **OAuth client** with only the scopes needed (`auth_keys`, `devices`, `policy_file`) |
| Keys | preauth keys create/expire/list | `POST .../keys` with tags, `reusable: false`, short expiry |
| Nodes | list/delete/expire | list/delete devices |
| Policy | see below | `GET`/`POST .../acl` with ETag; `POST .../acl/validate` first |

**Applying a policy depends on the provider and mode (D585).** *Tailscale:* `apply_policy` validates, then applies through the API; Git is still the source of truth, so the normal path is the repository's workflow (`gitops-acl-action`) on merge, and the control plane's own writes (tenant sections) go through a commit to the policy repository, not around it. *Headscale* in `file` mode (Git is the source of truth) cannot take a policy through the API, so `apply_policy` runs the checker, **commits the rendered file to the policy repository** (Git is the write path, §41 §6) and a reload hook on the Headscale host pulls it and sends SIGHUP. In `database` mode (self-hosters who want a web UI, Q589) it calls the API's policy `PUT` instead. All refuse a policy that fails its tests. Headscale's API listens only on the tailnet (the hosted service) or on loopback (self-hosted default), never on the public name, and keys are 90-day and rotated.

### 6.4 Bring your own tailnet (D587)

The provider knob also applies **per tenant**. In tailnet mode the BYOC agent's chart values are the same two settings plus a key reference, whichever network the tenant brings:

```yaml
net:
  mode: tailnet                 # default: outbound (D543)
  provider: tailscale           # tailscale | headscale
  loginServer: ""               # empty for Tailscale; https://headscale.example.com for Headscale
  authKeySecret: loams-net-key  # a Secret holding the tagged auth key; never in values or Git
```

| Whose network | What the customer does | What the operator does |
|---|---|---|
| **The operator's tailnet** (default) | Applies the chart with the single-use key the control plane issued (`tag:byoc-<tenant>`) | Generated policy (§6.1) |
| **The customer's own Tailscale tailnet** (BYOK) | Creates a tagged auth key in their tailnet, puts it in the Secret (`provider: tailscale`) | Cannot see into their tailnet. To reach the control plane, the operator **shares** a control-plane gateway node with the customer's tailnet (Tailscale node sharing) or the customer's agent joins the operator's tailnet instead: the first keeps the customer's network the customer's, the second is the default above. Which one is Q596 |
| **The customer's own or a dedicated Headscale** | Sets `provider: headscale` and `loginServer`, creates a tagged pre-auth key there | The control plane's name must be reachable from that network (a route or a published name); the operator does not manage their policy |

The agent runs `tailscaled` in userspace networking (no `NET_ADMIN`, no `/dev/net/tun`) as a sidecar, with `--login-server` added **only when `loginServer` is set** and `--advertise-tags` only when no login server is set (§6.5). Neither the operator nor Loams ever holds a customer's key outside the customer's own Secret.

### 6.5 The node-side switch

The same rule is used by every Loams-shipped node (the Authentik host, the k3s templates, the agent sidecar), so switching provider changes configuration and nothing else:

```sh
if   [ -n "$TAILNET_LOGIN_SERVER" ]; then TS_EXTRA_ARGS="--login-server=$TAILNET_LOGIN_SERVER"   # headscale
elif [ -n "$TS_TAGS" ];               then TS_EXTRA_ARGS="--advertise-tags=$TS_TAGS"              # tailscale
fi
```

Headscale takes the tags from the tagged pre-auth key and refuses a client-requested tag (checked 2026-10-02, v0.29.4); Tailscale accepts a tagged key with or without `--advertise-tags`, and needs it with an OAuth-client key.

## 7. Apps and the CLI (D592, D593, D594)

**Document, do not build a client.** A person reaches a self-hosted Loams instance over their tailnet by running the official Tailscale client on the laptop or phone: signed in to their Tailscale account, or pointed at their Headscale; the instance's address is its MagicDNS name (`https://loams.net.example.com`, or a `*.ts.net` name on Tailscale). The `loams` CLI, the desktop app (§37 §18) and the phone apps need nothing new: their `endpoint` (§30 §7) and the pairing `issuer` (§37 §7.2) are ordinary HTTPS URLs that happen to resolve to `100.64.0.0/10` while the tailnet is up.

### 7.1 Embed or document: options considered

| Option | Maturity (2026-10-02) | Verdict |
|---|---|---|
| Official clients, documented | Production; BSD-3-Clause for CLI/daemon and Android; the macOS, iOS and Windows GUI wrappers are closed | **Chosen** |
| `tsnet` in a Go sidecar | Mature, supports `ControlURL`; adds a Go toolchain and 20+ MB per binary | Only for server-side agents that already ship Go; not for the CLI or apps |
| `libtailscale` (C) from Rust via FFI | BSD-3-Clause, `set_control_url`; cgo and a Go runtime in a Rust binary; no iOS/Android story in our apps | Rejected: two runtimes for a feature the OS client already provides |
| `tsnet` crate | 0.1.0 in 2023, one release | Rejected: stale |
| `tailscale` crate (`tailscale-rs`) | Official, 0.6.1, work in progress; no MagicDNS, no private DERP, no iOS/Android, no compatibility guarantees; Headscale interop not documented | Rejected now; revisit when it has MagicDNS, private DERP relays, an audit and a Headscale test (Q584) |

The custom-server step on the phone (iOS "Use custom coordination server", Android "Use an alternate server") applies to Headscale only; with Tailscale the user just signs in. An embedded client would also mean the CLI and apps own a second network identity, key storage, expiry and re-authentication UX. The OS client already has all of it, including the iOS VPN entitlement that a third-party app would need to share.

### 7.2 TLS on a tailnet (D593)

Headscale does not issue certificates for tailnet names (it has no equivalent of `tailscale cert`); hosted Tailscale can, for the tailnet's `*.ts.net` names, when its HTTPS-certificates feature is enabled in the admin console **(verify; not checked)**, which makes option 4 below available only on Tailscale. Credentials never travel over plain HTTP to a non-loopback origin (§37, the credential rule of the network bridge; D111), even inside WireGuard. A self-hosted instance on a tailnet chooses one:

1. **A name in the operator's own DNS zone** (`loams.example.com`) that resolves to the instance's tailnet address (public DNS pointing at a `100.x` address, or a Headscale `dns.extra_records` entry), with a certificate from the CA of choice by **DNS-01**. Works for every client with no pin. Recommended.
2. **A private CA or a self-signed certificate** with the **SPKI pin** in the pairing payload (§37 §7.2.3). Works for the phones through pairing; the CLI and the desktop take the CA through the existing trust settings.
3. The MagicDNS name `*.net.example.com` with an ACME **DNS-01** certificate for the base domain (a wildcard) if the operator controls the DNS for it.
4. **Tailscale only:** `tailscale cert` for the node's `*.ts.net` name (Q595).

### 7.3 Pairing (D594)

The pairing payload (§37 §7.2.1) gains one **optional** field and stays `v: 1`:

```json
"net": {"kind": "tailnet", "provider": "headscale", "login_server": "https://headscale.example.com"}
```

For hosted Tailscale the field is `"net": {"kind": "tailnet", "provider": "tailscale"}` with no `login_server`. `issuer` stays an `https` URL (the instance's MagicDNS or DNS name). A reader that sees `net` shows "Connect to your private network first" with a button that opens the Tailscale app and, when `login_server` is present, a link to the custom-server instructions; it never reads an auth key from the payload and the QR never carries one. A reader that does not know `net` ignores it (unknown optional fields are ignored; AP0's fixtures gain a case). The phone needs the Tailscale app on, which is why the app says so rather than failing with a timeout.

**iOS and Android caveats** (to confirm on devices in NET1 Task 0): one VPN profile at a time on both, so a user who also needs a work VPN must choose; on-demand activation on iOS is the Tailscale app's own setting; push notifications to the phone come from APNs/FCM over the internet and do not need the tailnet (§37), but fetching approvals and details does.

## 8. Security (D585, D589, D590, D591, D595, D596, D597)

### 8.1 The policy

- **Default deny.** The policy file has a `grants` section (never empty-omitted: no `grants` or `acls` section is allow-all). Every rule is a `src`/`dst`/`ip` triple naming a tag or a user, with ports.
- **Tags owned by `group:ops`.** No user owns a tag, so a person cannot make their laptop look like a server.
- **Tests are part of the policy** (Tailscale evaluates them through the API on every `gitops-acl-action test`; the checker below is Headscale's). The `tests` block asserts what is denied as much as what is allowed (CI cannot reach the cluster; a server has no shell on another; a tenant reaches only `tag:control`; the control plane does not dial a tenant). `headscale policy check` evaluates them, but only against nodes that exist, so the repository's checker seeds a scratch database with one node per tag and operator (`check.sh`, `seed.py`). Headscale **starts even if its tests fail** ("server starting anyway"), and a `SIGHUP` reload of a policy whose tests name a tag with no node is refused (checked 2026-10-02), so the check is a required CI status and the apply path (§6.3) refuses a failing policy. **Both checks run in CI whichever provider is active**, so the shared file stays valid for the other one: the Headscale check is the portability gate, and `gitops-acl-action` test/apply is skipped (not failed) when its two secrets are absent.
- **Grants, not legacy ACLs.** Headscale recommends grants.
- **SSH.** OpenSSH on the tailnet interface only, with a grant for tcp 22. Tailscale SSH is supported by Headscale and is an option (Q587); it is not required.

### 8.2 Keys, expiry and approval (D589)

| Node kind | Join | Expiry |
|---|---|---|
| Personal device | OIDC sign-in through Authentik | 90 days (`node.expiry`, Q581), then the user signs in again |
| Server, k3s node, runner | Tagged, single-use, short-lived key | None (Headscale exempts tagged nodes; Tailscale disables key expiry on tagged devices by default **(verify)**). Compensating controls: delete on retirement; a nightly inventory diff alerts on a new node, a new tag or an untagged server; keys rotate by re-joining |
| CI job, autoscaled node | Tagged, **ephemeral**, 15-minute single-use key | Deleted after 30 minutes without contact |
| Tenant agent | Tagged `tag:byoc-<tenant>`, single-use, 1-hour, issued by the control plane | As servers; deleted on tenant offboarding (§41 §6) |

**Node approval** is the issuance of the key (an operator or the control plane decides who gets one) or the OIDC sign-in (Authentik decides). Subnet routes and exit nodes are not used by the hosted service; if they are, an operator approves them (`nodes approve-routes`) and `autoApprovers` is limited to tags.

### 8.3 Audit

Headscale has no audit-event stream (flow logs are unsupported); Tailscale's network flow logs and log streaming are Premium features, not on Personal (§3.1), and its configuration audit log is not relied on here **(verify the plan)**. Evidence is assembled from: the Git history of the policy (every access change is a reviewed commit); Headscale's JSON logs (registrations, expiries, policy reloads) in the log pipeline; Authentik's event log for each sign-in; and a nightly inventory diff that raises an event the control plane records as an audit event (§41 §11.2; D221). The diff job is NET1 Task 6.

### 8.4 DERP (D590)

- **`headscale`: self-hosted DERP only.** The embedded server on the Headscale host (region 900), `derp.urls: []`, no Tailscale-run relay. Reasons: no third party in the path (traffic is end-to-end encrypted either way, but metadata such as peer IPs and timing reach the relay operator); no dependence on Tailscale Inc.'s uptime or its derp map; the fleet is mostly servers with public addresses that connect directly. The cost: if the one DERP host is unreachable, peers that cannot connect directly lose connectivity, and the Headscale host is also the DERP host, so both fail together. A second DERP region on another provider (Q586) is a documented, templated follow-up through `derp.paths`.
- **`tailscale`: Tailscale's relay network** (the default). Traffic stays end-to-end encrypted; the relay operator sees connection metadata. Accepted for the owner's choice of provider; a custom DERP map in the policy is the escape hatch if that changes (not used).

### 8.5 The control server's availability and recovery (D591)

- **`tailscale`:** the control plane is Tailscale's. If it is unreachable, **existing connections keep working** for the same reason as below; new logins, new nodes, key renewals, policy changes and discovery of new peers fail. Nothing of ours to back up beyond the policy (in Git), the OAuth client and the auth keys' provenance. Recovery from a lost tailnet is re-creating it, applying the policy file and re-joining nodes with new keys.
- **`headscale`: one instance, SQLite.** No Postgres (discouraged by the project), no multi-replica. The state is one file plus two private keys (Noise, DERP) and the policy. **Backup:** nightly, encrypted to an offline age key, to the object store; 30-day retention; a restore drill before first use and quarterly. Restoring the Noise key lets nodes reconnect without re-registering. **If it is down:** existing connections keep working; WireGuard sessions between peers do not need the control server, and MagicDNS answers from the client's cached map. New logins, new nodes, key renewal, policy and route changes and peer discovery for new nodes fail; relayed connections drop if DERP is on the same host. Target time to restore one hour. **If Authentik is down:** OIDC sign-ins fail, everything else works; Headscale starts anyway (`only_start_if_oidc_is_available: false`) and falls back to CLI registration; pre-auth keys still register nodes.
- In both cases the data path (Loams serving traffic) is unaffected because it does not depend on the tailnet.

### 8.6 Threats and the central point of control

The control server is the coordinator: whoever controls it can add nodes and rewrite the policy, which is the whole network. With hosted Tailscale that is Tailscale's service and the owner's account, whose compromise is guarded by the account's own sign-in, a minimum of OAuth-client scopes, secrets held per environment and a policy reviewed in Git (an out-of-band console edit is overwritten by the next apply and should be alerted on). With Headscale, mitigations: it holds no data and no other secrets; its host has no public ports besides 80, 443 and 3478/udp; its CLI is only reachable on the host's unix socket and its REST API only on the tailnet; the API key is 90-day and rotated; policy changes are reviewed commits checked by tests and cannot be applied by the control plane without them; the backup is encrypted to an offline key; recovery from a compromised host is replacement with a new Noise key and a re-registration of every node (documented in the runbook). Relays see encrypted traffic only. An ex-employee's device is expired and removed with the group membership. The tailnet's blast radius for tenants is bounded by §6.1; for the data plane, by not carrying it.

## 9. Placement (D595, D596)

| What | Where |
|---|---|
| The hosted service's tailnet configuration: the shared `policy.hujson`, the policy workflow (`tailscale/gitops-acl-action`, `TS_OAUTH_CLIENT_ID` and `TS_OAUTH_SECRET`, skipped when absent), the Tailscale node for Authentik (compose profile `tailnet`, k3s manifests), the runbook | `loam-platform` (private): `deploy/tailnet/` and `docs/deploy/tailnet.md` |
| The Headscale alternative: compose, k3s, config, DERP, backups, checker, runbook | `loam-platform`: `deploy/tailnet/headscale/`, beside `deploy/authentik/`, on a small host (preferably not the Authentik VM: a separate failure domain, Q580), DNS-only record, Authentik OIDC client from `loams-05-headscale.yaml` |
| Authentik admin UI on the tailnet only | The same repository; path rules deny `/if/admin`, `/if/user` and the management API on the public name once the tailnet is up (a verification checklist in the runbook, because the regular expression depends on Authentik's route layout). The Authentik host joins as `tag:authentik` with either provider |
| Self-hosted template, policy renderer, the `NetProvider` trait and both clients, the BYOC chart values | This repository (Apache-2.0): `deploy/tailnet/` (a `headscale/` compose and Kustomize template, a `tailscale/` node template, policy example and tests), `crates/loams-net`, `scripts/net/` |
| A tenant's own tailnet or Headscale | The tenant. The template is the same |

OSS never depends on the hosted instance, and nothing in a Loams binary names a provider: it speaks to the OS's tailnet client by using a hostname.

## 10. The self-hosted template

`deploy/tailnet/` in this repository holds: `policy/` (the shared `example.hujson`, its lint and the checker), `tailscale/` (a node template: compose service and k3s Deployment that join with a key from the environment, `--login-server` only when `TAILNET_LOGIN_SERVER` is set), and `headscale/` (`compose/`: Headscale, Caddy, an age-encrypted backup sidecar; `k8s/`: Kustomize for k3s, the same three containers in one pod, `Recreate`, one PVC; `config/config.yaml`: OIDC optional, embedded DERP, MagicDNS base domain), plus the Authentik blueprint `deploy/authentik/blueprints/loams-headscale.yaml` (MT1's location). Headscale uses the pinned image and Caddy and the ports 443/tcp, 80/tcp, 3478/udp. It works without OIDC (CLI registration and pre-auth keys), with any OIDC provider, and with Authentik by blueprint. It is optional in every sense: nothing in Loams requires it.

## 11. Tooling (D598)

| Tool | What | Language |
|---|---|---|
| `policy/check.sh` + `seed.py` | Run `headscale policy check` with tests against a seeded scratch database (the Headscale gate, and the portability gate for the shared file) | shell, Python 3 stdlib |
| `policy_lint.py` | Offline: only the shared subset (`groups`, `tagOwners`, `grants`, `tests`), default-deny, every tag owned | Python 3 stdlib |
| `tailscale/gitops-acl-action` workflow | Provider `tailscale`: `test` on pull requests, `apply` on merge; skipped without the OAuth secrets | GitHub Actions |
| `scripts/net/render-policy` | Render the tenant section of the policy from tenant records, with positive and negative tests per tenant | Python 3.13 (uv), pytest, golden files |
| `scripts/net/sync-groups` | Read Authentik's `loams-net-ops`, propose an edit of `group:ops` and its tests as a pull request | Python 3.13 |
| `crates/loams-net` | `NetProvider`, `HeadscaleClient` (preauth keys, nodes, policy get/set, API-key check) and `TailscaleClient` (API v2: keys, devices, ACL with ETag and validate) | Rust, `reqwest`, `serde` |
| Inventory diff | Nightly `nodes list -o json` against yesterday; emits audit events | Python or Rust (NET1 Task 6) |

## 12. Plan and exit

[NET1](../plans/2026-10-02-net1-private-networking.md): Task 0 reconciles with §41 and MT1 and tests the client login on the five platforms; Task 1 the policy as code and its tests (shared file, both gates); Task 2 the generator; Task 3 the sync job; Task 4 the node, compose and k3s templates and the OIDC blueprint; Task 5 `loams-net` (both providers) and the BYOC join automation; Task 6 the inventory diff and audit; Task 7 docs and the e2e (Headscale with real `tailscaled` containers: default-deny, tenant isolation, ephemeral expiry, restore; the same node image against Tailscale is not an automated test, since it needs a live account).

**Exit:** the e2e proves (a) a node with no grant reaches nothing (on Headscale in CI; on Tailscale by `gitops-acl-action test` and a one-time manual check), (b) tenant A's node cannot reach tenant B's or any operator tag, (c) an ephemeral key's node disappears after the timeout, (d) a restored backup lets existing nodes reconnect, and (e) every `policy check` test passes in CI.

## 13. Risks

| Risk | Mitigation |
|---|---|
| The control server is a single point of control (Headscale: also a single instance; Tailscale: a third party's service) | §8.5 and §8.6: no data, no public admin surfaces, a policy in Git, and for Headscale encrypted offline-keyed backups, a drill and a documented rebuild |
| Hosted Tailscale on the free plan is for non-commercial use, with 6 users, 3 ACL groups and 50 tagged resources | Q593: Personal while it is the owner's own lab; a paid plan, or Headscale, before customers depend on it. Switching is configuration (D582) |
| Custom OIDC with Authentik on Tailscale is unverified, and is chosen at tailnet creation | Start with one major-provider identity; try the custom-OIDC sign-up on a throwaway tailnet first (Q594) |
| A Tailscale-only feature creeps into the shared policy and breaks the switch | `policy_lint.py` and the Headscale portability gate fail the build |
| Headscale is an independent community project, not a company's product; its API and policy features still move | Pin by version and digest; the checker runs on every bump; the control server is replaceable in either direction without client changes (clients are unmodified) |
| Tagged nodes never expire | Single-use short keys, inventory diff, delete on retirement (D589) |
| A generated tenant policy has a bug that joins tenants | Generated negative tests, a refusal to apply on failure, golden files, a dedicated Headscale for sensitive tenants |
| Cloudflare path rules for Authentik's admin routes break a flow | Verification checklist before and after; the rule is reversible; the public name never has the tailnet as a dependency |
| iOS/Android custom-server sign-in changes or breaks in a client release | Per-release device test in NET1 Task 0 and the runbook; Headscale's minimum client is v1.80 |
| Postgres users want HA | Out of scope; SQLite plus restore is the supported path (Q580); revisit if the project's guidance changes |
| The `tailscale` Rust crate becomes mature | Revisit embedding (Q584) against a checklist: MagicDNS, private DERP, audit, a Headscale interop test, iOS and Android |

## 14. Amendments to other sections

These cross-references are integrated into §30, §37, §10 and §41. The decisions are in the [decision log](13-decision-log.md).

- **§41 §7.4:** BYOC connectivity modes: the outbound agent (D543) and the optional tailnet mode (D587; the provider knob of §6.4); per-tenant isolation (D586).
- **§30 §15:** a self-hosted instance on a tailnet (Tailscale or Headscale) is reached through its DNS or MagicDNS name; `loams login --endpoint https://...` needs no network option; the CLI never manages the tailnet (D592, D593).
- **§37 §7.2.4:** private instances use the user's tailnet client; pairing carries an optional `net` hint (D594); Q425 (a relay) is answered in the negative for tailnet users and stays open for others.
- **§10:** private networking for self-hosted clusters points here; the internal routes of §01 "must be on a private network" may be satisfied with the template.

## 15. Sources

All read 2026-10-02.

- Headscale: `juanfont/headscale` releases (v0.29.4, v0.29.0), licence, `docs/about/features.md`, `docs/ref/{oidc,policy,derp,registration,tags,tls}.md`, `docs/ref/integration/reverse-proxy.md`, `docs/setup/requirements.md`, `docs/setup/install/container.md`, `docs/usage/connect/{apple,android,windows}.md`, `config-example.yaml` and the v0.29.4 binary's `policy check`, `configtest` and `preauthkeys create` help.
- Tailscale: `tailscale/tailscale` (v1.102.5, README on which parts are open source, `tsnet/tsnet.go` `ControlURL`), `tailscale/libtailscale` (`tailscale.h`), `tailscale/tailscale-rs` (README, status, caveats, `src/config.rs`), crates.io entries for `tailscale` (0.6.1) and `tsnet` (0.1.0), docs.rs `tailscale` 0.5.0.
- Tailscale: tailscale.com/pricing and /pricing/faq (Personal plan), tailscale.com/blog/custom-oidc-ga and /docs/integrations/identity/custom-oidc (custom OIDC), `tailscale/gitops-acl-action` (`README`, `action.yml`, v1.5.2), `tailscale/github-action` (v4.2.0), `cmd/containerboot` (v1.102.5; environment variables, `TS_CLIENT_ID`, no `TS_LOGIN_SERVER`).
- Images: registry manifest digests for `ghcr.io/juanfont/headscale:v0.29.4` and `docker.io/tailscale/tailscale:v1.102.5` (`sha256:c507f3a2a6ab1cabd8d809b98edeb41edbd5c3fb6ad9632ffd098b4c7d0b4065`).
- Run locally 2026-10-02: Headscale v0.29.4 with the shared policy and a `tailscale/tailscale` v1.102.5 userspace node (§3.1).
- Loams: [§38](38-knative-authentik-gitops.md), §41 §5, §7, §11, [§37](37-desktop-and-mobile-apps.md) §7.2, [§30](30-loams-cli.md) §15, [MT1](../plans/2026-10-02-mt1-authentik-identity.md).
