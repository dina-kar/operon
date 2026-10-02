# 43 — Private Networking with Headscale

Status: **Proposed** · 2026-10-02. Source: the owner's note of 2026-10-02, "you can use **Headscale** if you want, for added security and unified handling." This document accepts the offer and decides how.

It adds decisions **D580–D599** and open questions **Q580–Q599** (Q580–Q592 are used). They are **proposals** until the owner rules on them, except D580, which is the owner's own. Plan: [NET1](../plans/2026-10-02-net1-headscale.md). The staged log is [`_pending/43-log.md`](_pending/43-log.md).

**Amends** §41 §7 (an optional connectivity mode for BYOC; staged in the pending log until §41 merges), [§30](30-loams-cli.md) §15 (reaching a self-hosted instance), [§37](37-desktop-and-mobile-apps.md) §7.2.4 (private instances and pairing) and [§10](10-operations.md) (private networking). **Builds on** [§38](38-knative-authentik-gitops.md) (Authentik is the identity provider) and [§19](19-console-identity-and-agents.md). The hosted service's side (the Headscale at `headscale.loams.dev`, its policy and runbook) lives in the private `loam-platform` repository; this repository depends on none of it.

Markers: **(verify)** means not checked against a primary source; the task that depends on it checks it first. Every version, licence and status claim with a date was read on 2026-10-02 from the source named in §15.

**Numbering.** D580–D599 and Q580–Q599 are this document's reserved ranges.

## 1. Summary

Loams runs two networks with two jobs:

- **Cloudflare** (Tunnel, Workers, DNS) is the **public front door**: `loams.dev`, `auth.loams.dev`, `console.loams.dev`. People and browsers.
- **A Headscale tailnet** is the **private network**: operator SSH and `kubectl` with no public ports, k3s, TiKV and WeSQL traffic between sites, backups to the object store over private paths, the `loam-bench` CI runner, the Authentik admin interface, and an optional way for a customer's BYOC cluster to reach the control plane without opening an inbound port.

Headscale is the open-source (BSD-3-Clause) self-hosted implementation of Tailscale's control server. The official Tailscale clients, unmodified, connect to it. Loams **documents and templates** it; it does **not** build or embed a tailnet client (D592). The hosted service runs one Headscale, signed in through Authentik (the same IdP as everything else), with a default-deny policy kept in Git and tested on every change.

Three facts shaped the design, and each corrects an assumption the question carried:

1. **Headscale cannot sit behind Cloudflare's proxy or Tunnel.** The Tailscale control protocol upgrades a `POST` with `Upgrade: tailscale-control-protocol`; Headscale's documentation says Cloudflare "does not support WebSocket POSTs as required by the Tailscale protocol" and that this setup "is not supported and will not work". `headscale.loams.dev` is therefore a DNS-only record on a host with public 443/tcp and 3478/udp (D583). The two networks meet only at the identity provider.
2. **Headscale cannot use OIDC groups in its policy.** Authentik groups can decide **who may sign in** (`oidc.allowed_groups`), not what a user may reach. The policy names operators by email, kept in Git, and a sync job proposes the change when Authentik's roster changes (D584).
3. **Tagged nodes do not expire.** Servers need tags to be owned by the organisation rather than a person, and Headscale exempts tagged nodes from key expiry. Lifecycle is therefore enforced by short-lived single-use join keys, ephemeral nodes for CI and autoscaling, an inventory diff, and deleting nodes on retirement (D589).

### 1.1 The picture

```
                   people, browsers                       operators, servers, tenants' agents
                          │                                              │
        loams.dev / auth.loams.dev / console.loams.dev          headscale.loams.dev (DNS only)
                          │                                              │
                  ┌───────▼────────┐                          ┌──────────▼───────────┐
                  │   Cloudflare   │                          │ Caddy ─► Headscale   │  :443 tcp, :3478 udp
                  │ Workers/Tunnel │                          │  + embedded DERP     │  SQLite, 1 replica
                  └───────┬────────┘                          └──────────┬───────────┘
                          │ outbound tunnel                              │ OIDC (code + PKCE)
                  ┌───────▼────────┐  ◄────────────────────────────────── │
                  │   Authentik    │  groups loams-net-users / -ops        │
                  │ (admin routes  │                                       │
                  │ tailnet-only)  │            WireGuard mesh (tailnet, 100.64.0.0/10, *.net.loams.dev)
                  └────────────────┘   ops laptops ── k3s ── TiKV ── WeSQL ── RustFS ── loam-bench ── tag:byoc-<tenant> agents
```

## 2. Goals and non-goals

### 2.1 Goals

- No public SSH, `kubectl` or admin port anywhere in the hosted service.
- One identity (Authentik), one policy file, one place to see who can reach what.
- A default-deny network: a new node can reach nothing until a rule says so.
- BYOC clusters that can be managed without a single inbound port, with tenant-to-tenant isolation that does not depend on getting a firewall right per customer.
- Self-hosters can adopt the same thing from a template, or ignore it.
- The failure of Headscale degrades administration, never the data path.

### 2.2 Non-goals

- Replacing Cloudflare for public traffic, or putting end-user (browser) traffic on a tailnet.
- Carrying Loams's data plane between customers or between a customer and the hosted service. The tailnet is a management and east-west network; data-plane paths keep their TLS and token authentication (§19, §41 §7.3).
- Building a Loams tailnet client, or a Headscale fork (D592).
- Tailscale Funnel or Serve (Headscale does not implement them), SaaS Tailscale, or any dependency on Tailscale Inc.'s services (D590).
- Metering. The tailnet carries no usage records and `loams-net` has no billing surface (the open-core boundary of §41).

## 3. What was verified (2026-10-02)

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

## 4. Two networks, clear roles (D581)

| Concern | Cloudflare | Headscale tailnet |
|---|---|---|
| Public HTTP: site, Auth.js callbacks, console, OIDC endpoints, API gateway | Yes (Workers, Tunnel) | No |
| Operator SSH, `kubectl`, Argo CD UI | No | Yes, no public port |
| Authentik admin UI and management API | Denied on the public name | Yes (`tag:authentik` tcp 9000, operators only) |
| k3s, TiKV, WeSQL, Loams cluster traffic between sites | No | Yes (§5.4 of the policy) |
| Backups, snapshots, S3 to the private object store | R2 for the hosted beta | Yes for RustFS/private paths |
| CI self-hosted runner (`loam-bench`) | Outbound to GitHub as before | Operator SSH and private object store |
| BYOC agent to the control plane | Default: outbound mTLS over HTTPS (§41 D543) | Optional mode (D587) |
| Tailnet coordination and DERP | **Cannot** (D583): `headscale.loams.dev` DNS-only | Is the tailnet's control plane |

The rule of thumb: if a person with a browser needs it, it is public; if a machine or an operator needs it, it is on the tailnet.

## 5. Identity (D584, D585)

Authentik is the IdP (§38). Headscale signs people in with OIDC (authorization code + PKCE S256, confidential client `headscale`, redirect `https://<headscale-host>/oidc/callback`). What Authentik can and cannot do here:

| Question | Answer |
|---|---|
| Who may sign in? | Members of the Authentik group **`loams-net-users`**: Headscale's `oidc.allowed_groups` **and** an Authentik policy binding on the application, so a non-member cannot even obtain a token |
| What may they reach? | Decided by the **policy file**, not by groups. `group:ops` lists operator **emails** in Git; tags are owned by `group:ops` |
| Why not map groups to tags? | Headscale cannot use OIDC groups in policy rules (its own documentation). A user's tags would come only from the key used to join a server, and tag ownership from the policy |
| How do the roster in Authentik and the list in Git stay equal? | A job (`scripts/net/sync-groups`, plan NET1 Task 3) reads `loams-net-ops` from Authentik and opens a pull request that edits `group:ops` and adds the `tests` lines. It never applies a change itself: access changes are reviewed commits |
| Group claim | The `loams-` prefix filter of MT1 ruling 2 applies: `loams-net-users` and `loams-net-ops` reach Headscale; an operator's other Authentik groups do not |

**Tags** are owned by `group:ops` (`tag:k3s`, `tag:tikv`, `tag:wesql`, `tag:objstore`, `tag:authentik`, `tag:headscale`, `tag:bench`, `tag:ci`, `tag:control`, and one `tag:byoc-<tenant>` per tenant). A server joins with a **tagged pre-auth key** created by an operator or by automation holding a Headscale API key; the key fixes the tags, so a compromised server cannot add itself to a different tag.

**Pre-auth keys** (D588): single-use, one hour for people-created ones, 15 minutes for CI, always tagged, `--ephemeral` for CI and autoscaled nodes. A **reusable** key exists only for an autoscaling group whose boot code can fetch it from the secret store, expires in 24 hours at most and is rotated by the same job that created it.

## 6. BYOC over a tailnet (D586, D587)

§41 §7.1 gives BYOC an outbound-only agent. This document adds an optional **tailnet connectivity mode** alongside it, for customers who prefer a private path to the operator's control plane, or who need the operator's support staff to reach the cluster on request. It never replaces the outbound-agent mode and it opens no inbound port in either.

### 6.1 Per-tenant isolation: shared Headscale, generated policy (D586)

The options were one Headscale for the operator with ACL isolation per tenant, or one Headscale per tenant. Decision: **one operator Headscale, hub-and-spoke, with a generated policy**, and a dedicated Headscale for tenants who ask or whose regulation requires it.

| | Shared Headscale, ACL per tenant (chosen default) | One Headscale per tenant |
|---|---|---|
| What a tenant node can see | Nothing but `tag:control`: peers are visible only when a rule allows traffic between them, so other tenants' nodes are not in its network map | Only that tenant's nodes |
| Blast radius of a policy mistake | All tenants, mitigated by generated rules and tests (below) | One tenant |
| Blast radius of a Headscale compromise | All tenants' management paths (not their data: §41 §7.3) | One tenant |
| Operator cost | One process, one backup, one policy | A process, DNS name, certificate, backup and key per tenant: real work for the control plane to create and run at hundreds of tenants |
| Tenant control | None over the policy | Full, if they run it |
| Tenant user access | Not provided: this tailnet is the operator's management network | The tenant's own network |

Why shared by default: the operator tailnet carries **management traffic only**, spoke to hub (`tag:byoc-<tenant>` -> `tag:control:443`), never spoke to spoke, and the control plane never dials into a tenant. With that shape there is little for a tenant to be isolated from beyond the hub, and the policy is mechanical. The tenant's **own** users and east-west traffic belong on **their** network: the tenant runs their own Headscale (or any WireGuard tool) and the Loams chart does not care. The tenant can run one on the same cluster; nothing in Loams couples to the operator's tailnet.

How the shared design is kept safe:

- The tenant section of the policy is **generated** from tenant records (`scripts/net/render-policy`, NET1 Task 2), never edited by hand; it sits between marker comments.
- The generator emits, for every tenant, positive rules and `tests` that a tenant tag cannot reach any other tenant tag or any operator tag. `policy check` runs them in CI and in the control plane before it applies a change (`policy set` is refused on failure).
- A tenant's pre-auth keys are single-use, tagged `tag:byoc-<tenant>`, and created by the control plane only.
- Dedicated Headscale for a tenant (Q582) is the same template instantiated with a different `server_url`, `base_domain` and OIDC client; the control plane's `NetProvider` (§6.3) addresses either.

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

The control plane gets a small trait, `NetProvider`: `issue_join_key(tenant, kind)`, `revoke(tenant)`, `list_nodes(tenant)`, `apply_policy(rendered)`. The Headscale implementation (`loams-net`, NET1 Task 5) speaks Headscale's REST API (`/api/v1`) with an API key held in the control plane's secret store, for keys and nodes.

**Applying a policy depends on the policy mode (D585).** In `file` mode (the default, and the hosted service's: Git is the source of truth) the API cannot write the policy, so `apply_policy` runs the checker, **commits the rendered file to the policy repository** (Git is the write path, §41 §6) and a reload hook on the Headscale host pulls it and sends SIGHUP. In `database` mode (self-hosters who want a web UI, Q589) it calls the API's policy `PUT` instead. Both refuse a policy that fails its tests. Headscale has **no scoped API keys**: an API key is full admin (Q591). The API therefore listens only on the tailnet (the hosted service) or on loopback (self-hosted default), never on the public name, and keys are 90-day and rotated. `join` renders as chart values: the `loams-byoc-agent` chart's BYOC profile gains `net.mode: tailnet`, `net.loginServer`, `net.authKeySecret` and runs `tailscaled` (userspace networking, no `NET_ADMIN`) as a sidecar of the agent.

## 7. Apps and the CLI (D592, D593, D594)

**Document, do not build a client.** A person reaches a self-hosted Loams instance over their tailnet by running the official Tailscale client on the laptop or phone, pointed at their Headscale; the instance's address is its MagicDNS name (`https://loams.net.example.com`). The `loams` CLI, the desktop app (§37 §18) and the phone apps need nothing new: their `endpoint` (§30 §7) and the pairing `issuer` (§37 §7.2) are ordinary HTTPS URLs that happen to resolve to `100.64.0.0/10` while the tailnet is up.

### 7.1 Embed or document: options considered

| Option | Maturity (2026-10-02) | Verdict |
|---|---|---|
| Official clients, documented | Production; BSD-3-Clause for CLI/daemon and Android; the macOS, iOS and Windows GUI wrappers are closed | **Chosen** |
| `tsnet` in a Go sidecar | Mature, supports `ControlURL`; adds a Go toolchain and 20+ MB per binary | Only for server-side agents that already ship Go; not for the CLI or apps |
| `libtailscale` (C) from Rust via FFI | BSD-3-Clause, `set_control_url`; cgo and a Go runtime in a Rust binary; no iOS/Android story in our apps | Rejected: two runtimes for a feature the OS client already provides |
| `tsnet` crate | 0.1.0 in 2023, one release | Rejected: stale |
| `tailscale` crate (`tailscale-rs`) | Official, 0.6.1, work in progress; no MagicDNS, no private DERP, no iOS/Android, no compatibility guarantees; Headscale interop not documented | Rejected now; revisit when it has MagicDNS, private DERP relays, an audit and a Headscale test (Q584) |

An embedded client would also mean the CLI and apps own a second network identity, key storage, expiry and re-authentication UX. The OS client already has all of it, including the iOS VPN entitlement that a third-party app would need to share.

### 7.2 TLS on a tailnet (D593)

Headscale does not issue certificates for tailnet names (it has no equivalent of `tailscale cert`). Credentials never travel over plain HTTP to a non-loopback origin (§37, the credential rule of the network bridge; D111), even inside WireGuard. A self-hosted instance on a tailnet chooses one:

1. **A name in the operator's own DNS zone** (`loams.example.com`) that resolves to the instance's tailnet address (public DNS pointing at a `100.x` address, or a Headscale `dns.extra_records` entry), with a certificate from the CA of choice by **DNS-01**. Works for every client with no pin. Recommended.
2. **A private CA or a self-signed certificate** with the **SPKI pin** in the pairing payload (§37 §7.2.3). Works for the phones through pairing; the CLI and the desktop take the CA through the existing trust settings.
3. The MagicDNS name `*.net.example.com` with an ACME **DNS-01** certificate for the base domain (a wildcard) if the operator controls the DNS for it.

### 7.3 Pairing (D594)

The pairing payload (§37 §7.2.1) gains one **optional** field and stays `v: 1`:

```json
"net": {"kind": "tailnet", "login_server": "https://headscale.example.com"}
```

`issuer` stays an `https` URL (the instance's MagicDNS or DNS name). A reader that sees `net` shows "Connect to your private network first" with a button that opens the Tailscale app and a link to the login-server instructions; it never reads an auth key from the payload and the QR never carries one. A reader that does not know `net` ignores it (unknown optional fields are ignored; AP0's fixtures gain a case). The phone needs the Tailscale app on, which is why the app says so rather than failing with a timeout.

**iOS and Android caveats** (to confirm on devices in NET1 Task 0): one VPN profile at a time on both, so a user who also needs a work VPN must choose; on-demand activation on iOS is the Tailscale app's own setting; push notifications to the phone come from APNs/FCM over the internet and do not need the tailnet (§37), but fetching approvals and details does.

## 8. Security (D585, D589, D590, D591, D595, D596, D597)

### 8.1 The policy

- **Default deny.** The policy file has a `grants` section (never empty-omitted: no `grants` or `acls` section is allow-all). Every rule is a `src`/`dst`/`ip` triple naming a tag or a user, with ports.
- **Tags owned by `group:ops`.** No user owns a tag, so a person cannot make their laptop look like a server.
- **Tests are part of the policy.** The `tests` block asserts what is denied as much as what is allowed (CI cannot reach the cluster; a server has no shell on another; a tenant reaches only `tag:control`; the control plane does not dial a tenant). `headscale policy check` evaluates them, but only against nodes that exist, so the repository's checker seeds a scratch database with one node per tag and operator (`check.sh`, `seed.py`). Headscale **starts even if its tests fail** ("server starting anyway"), so the check is a required CI status and the apply path (§6.3) refuses a failing policy.
- **Grants, not legacy ACLs.** Headscale recommends grants.
- **SSH.** OpenSSH on the tailnet interface only, with a grant for tcp 22. Tailscale SSH is supported by Headscale and is an option (Q587); it is not required.

### 8.2 Keys, expiry and approval (D589)

| Node kind | Join | Expiry |
|---|---|---|
| Personal device | OIDC sign-in through Authentik | 90 days (`node.expiry`, Q581), then the user signs in again |
| Server, k3s node, runner | Tagged, single-use, 1-hour pre-auth key | None (Headscale exempts tagged nodes). Compensating controls: delete on retirement; a nightly inventory diff alerts on a new node, a new tag or an untagged server; keys rotate by re-joining |
| CI job, autoscaled node | Tagged, **ephemeral**, 15-minute single-use key | Deleted after 30 minutes without contact |
| Tenant agent | Tagged `tag:byoc-<tenant>`, single-use, 1-hour, issued by the control plane | As servers; deleted on tenant offboarding (§41 §6) |

**Node approval** is the issuance of the key (an operator or the control plane decides who gets one) or the OIDC sign-in (Authentik decides). Subnet routes and exit nodes are not used by the hosted service; if they are, an operator approves them (`nodes approve-routes`) and `autoApprovers` is limited to tags.

### 8.3 Audit

Headscale has no audit-event stream (flow logs are unsupported). Evidence is assembled from: the Git history of the policy (every access change is a reviewed commit); Headscale's JSON logs (registrations, expiries, policy reloads) in the log pipeline; Authentik's event log for each sign-in; and a nightly inventory diff that raises an event the control plane records as an audit event (§41 §11.2; D221). The diff job is NET1 Task 6.

### 8.4 DERP (D590)

**Self-hosted DERP only**: the embedded server on the Headscale host (region 900), `derp.urls: []`, no Tailscale-run relay. Reasons: no third party in the path (traffic is end-to-end encrypted either way, but metadata such as peer IPs and timing reach the relay operator); no dependence on Tailscale Inc.'s uptime or its derp map; the fleet is mostly servers with public addresses that connect directly. The cost: if the one DERP host is unreachable, peers that cannot connect directly lose connectivity, and the Headscale host is also the DERP host, so both fail together. A second DERP region on another provider (Q586) is a documented, templated follow-up through `derp.paths`; self-hosters may choose differently and the template says how.

### 8.5 Headscale's own availability and recovery (D591)

- **One instance, SQLite.** No Postgres (discouraged by the project), no multi-replica. The state is one file plus two private keys (Noise, DERP) and the policy.
- **Backup:** nightly, encrypted to an offline age key, to the object store; 30-day retention; a restore drill before first use and quarterly. Restoring the Noise key lets nodes reconnect without re-registering.
- **If Headscale is down:** **existing connections keep working**. WireGuard sessions between peers do not need the control server; MagicDNS answers from the client's cached map. New logins, new nodes, key renewal, policy and route changes and peer discovery for new nodes fail; relayed connections drop if DERP is on the same host. Target time to restore one hour; the data path (Loams serving traffic) is unaffected because it does not depend on the tailnet.
- **If Authentik is down:** OIDC sign-ins fail, everything else works. Headscale is configured with `only_start_if_oidc_is_available: false`, so it starts anyway and falls back to CLI registration; pre-auth keys still register nodes.

### 8.6 Threats and the central point of control

Headscale is the coordinator: whoever controls it can add nodes and rewrite the policy, which is the whole network. Mitigations: it holds no data and no other secrets; its host has no public ports besides 80, 443 and 3478/udp; its CLI is only reachable on the host's unix socket and its REST API only on the tailnet; the API key is 90-day and rotated; policy changes are reviewed commits checked by tests and cannot be applied by the control plane without them; the backup is encrypted to an offline key; recovery from a compromised host is replacement with a new Noise key and a re-registration of every node (documented in the runbook). Relays see encrypted traffic only. An ex-employee's device is expired and removed with the group membership. The tailnet's blast radius for tenants is bounded by §6.1; for the data plane, by not carrying it.

## 9. Placement (D595, D596)

| What | Where |
|---|---|
| The hosted service's Headscale (`headscale.loams.dev`), its policy, DERP, backups, runbook | `loam-platform` (private): `deploy/headscale/` beside `deploy/authentik/`, on a small host (preferably not the Authentik VM: a separate failure domain, Q580), DNS-only record, Authentik OIDC client from `loams-05-headscale.yaml` |
| Authentik admin UI on the tailnet only | The same repository; path rules deny `/if/admin`, `/if/user` and the management API on the public name once the tailnet is up (a verification checklist in the runbook, because the regular expression depends on Authentik's route layout) |
| Self-hosted template, policy renderer, the `NetProvider` trait and Headscale client, the BYOC chart values | This repository (Apache-2.0): `deploy/headscale/` (compose, Kustomize for k3s, config, policy example and tests), `crates/loams-net`, `scripts/net/` |
| A tenant's own Headscale | The tenant. The template is the same |

OSS never depends on the hosted instance, and nothing in a Loams binary names Headscale: it speaks to the OS's tailnet client by using a hostname.

## 10. The self-hosted template

`deploy/headscale/` in this repository holds: `compose/` (Headscale, Caddy, an age-encrypted backup sidecar), `k8s/` (Kustomize for k3s: the same three containers in one pod, `Recreate`, one PVC), `config/config.yaml` (OIDC optional; embedded DERP; MagicDNS base domain), `policy/example.hujson` with tests and the checker, and the Authentik blueprint `deploy/authentik/blueprints/loams-headscale.yaml` (MT1's location). It uses the same pinned image and Caddy, and the same ports (443/tcp, 80/tcp, 3478/udp). It works without OIDC (CLI registration and pre-auth keys), with any OIDC provider, and with Authentik by blueprint. It is optional in every sense: nothing in Loams requires it.

## 11. Tooling (D598)

| Tool | What | Language |
|---|---|---|
| `policy/check.sh` + `seed.py` | Run `headscale policy check` with tests against a seeded scratch database | shell, Python 3 stdlib |
| `scripts/net/render-policy` | Render the tenant section of the policy from tenant records, with positive and negative tests per tenant | Python 3.13 (uv), pytest, golden files |
| `scripts/net/sync-groups` | Read Authentik's `loams-net-ops`, propose an edit of `group:ops` and its tests as a pull request | Python 3.13 |
| `crates/loams-net` | `NetProvider` and the Headscale REST client (preauth keys, nodes, policy get/set, API-key check) | Rust, `reqwest`, `serde` |
| Inventory diff | Nightly `nodes list -o json` against yesterday; emits audit events | Python or Rust (NET1 Task 6) |

## 12. Plan and exit

[NET1](../plans/2026-10-02-net1-headscale.md): Task 0 reconciles with §41 and MT1 and tests the client login on the five platforms; Task 1 the policy as code and its tests; Task 2 the generator; Task 3 the sync job; Task 4 the compose and k3s templates and the OIDC blueprint; Task 5 `loams-net` and the BYOC join automation; Task 6 the inventory diff and audit; Task 7 docs and the e2e (Headscale with real `tailscaled` containers: default-deny, tenant isolation, ephemeral expiry, restore).

**Exit:** the e2e proves (a) a node with no grant reaches nothing, (b) tenant A's node cannot reach tenant B's or any operator tag, (c) an ephemeral key's node disappears after the timeout, (d) a restored backup lets existing nodes reconnect, and (e) every `policy check` test passes in CI.

## 13. Risks

| Risk | Mitigation |
|---|---|
| Headscale is a single point of control and a single instance | §8.5 and §8.6: no data, no public admin surfaces, encrypted offline-keyed backups, a drill, a documented rebuild |
| Headscale is an independent community project, not a company's product; its API and policy features still move | Pin by version and digest; the checker runs on every bump; the tailnet is replaceable by Tailscale's own control plane without client changes (clients are unmodified) |
| Tagged nodes never expire | Single-use short keys, inventory diff, delete on retirement (D589) |
| A generated tenant policy has a bug that joins tenants | Generated negative tests, a refusal to apply on failure, golden files, a dedicated Headscale for sensitive tenants |
| Cloudflare path rules for Authentik's admin routes break a flow | Verification checklist before and after; the rule is reversible; the public name never has the tailnet as a dependency |
| iOS/Android custom-server sign-in changes or breaks in a client release | Per-release device test in NET1 Task 0 and the runbook; Headscale's minimum client is v1.80 |
| Postgres users want HA | Out of scope; SQLite plus restore is the supported path (Q580); revisit if the project's guidance changes |
| The `tailscale` Rust crate becomes mature | Revisit embedding (Q584) against a checklist: MagicDNS, private DERP, audit, a Headscale interop test, iOS and Android |

## 14. Amendments to other sections

These blocks are the cross-references. §30, §37 and §10 are edited in this change; §41 is not yet on `dev`, so its block is staged in [`_pending/43-log.md`](_pending/43-log.md) and pasted when §41 merges.

- **§41 §7.1 (new §7.4, staged):** BYOC connectivity modes: the outbound agent (D543) and the optional tailnet mode (D587); per-tenant isolation (D586).
- **§30 §15:** a self-hosted instance on a tailnet is reached through its DNS or MagicDNS name; `loams login --endpoint https://...` needs no network option; the CLI never manages the tailnet (D592, D593).
- **§37 §7.2.4:** private instances use the user's tailnet client; pairing carries an optional `net` hint (D594); Q425 (a relay) is answered in the negative for tailnet users and stays open for others.
- **§10:** private networking for self-hosted clusters points here; the internal routes of §01 "must be on a private network" may be satisfied with the template.

## 15. Sources

All read 2026-10-02.

- Headscale: `juanfont/headscale` releases (v0.29.4, v0.29.0), licence, `docs/about/features.md`, `docs/ref/{oidc,policy,derp,registration,tags,tls}.md`, `docs/ref/integration/reverse-proxy.md`, `docs/setup/requirements.md`, `docs/setup/install/container.md`, `docs/usage/connect/{apple,android,windows}.md`, `config-example.yaml` and the v0.29.4 binary's `policy check`, `configtest` and `preauthkeys create` help.
- Tailscale: `tailscale/tailscale` (v1.102.5, README on which parts are open source, `tsnet/tsnet.go` `ControlURL`), `tailscale/libtailscale` (`tailscale.h`), `tailscale/tailscale-rs` (README, status, caveats, `src/config.rs`), crates.io entries for `tailscale` (0.6.1) and `tsnet` (0.1.0), docs.rs `tailscale` 0.5.0.
- Images: registry manifest digests for `ghcr.io/juanfont/headscale:v0.29.4`.
- Loams: [§38](38-knative-authentik-gitops.md), §41 §5, §7, §11, [§37](37-desktop-and-mobile-apps.md) §7.2, [§30](30-loams-cli.md) §15, [MT1](../plans/2026-10-02-mt1-authentik-identity.md).
