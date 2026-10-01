# Pending log entries for §38 (Knative, Authentik and GitOps for self-hosted Loam)

For the integrator. Branch `open-multitenancy-design`. Source: [§38](../38-knative-authentik-gitops.md), [MT1](../../plans/2026-10-02-mt1-authentik-identity.md), [MT2](../../plans/2026-10-02-mt2-knative.md), [MT3](../../plans/2026-10-02-mt3-gitops-clever.md). Reserved ranges: **D440–D459** (all used) and **Q440–Q459** (Q440–Q453 used). The owner's rulings: 2026-10-01 ("loams multitenant also fully opensource using knative and for gitops clever cloud, for auth Authentik so no paid plan"; "all run on cloudflare using byoc"), narrowed on 2026-10-02 ("keep loam cloud and loams-cloud private; may add Knative in OSS but no metering; I want adoption and also to raise money from VCs; move Cloudflare, OpenRTB etc. commercial to private repos"). The hosted-cloud, Cloudflare, protocol-gateway and metering designs from the same rulings are in `loam-platform` and are not logged here.

## 1. Decision rows (append to `13-decision-log.md` → Decisions)

| ID | Date | Decision | Rationale | Status |
|---|---|---|---|---|
| D440 | 2026-10-02 | **The open-core boundary stands** (§38 §1, §7; D220 reconfirmed). Knative, Authentik and the GitOps layout are self-hosting features, so open; **no metering in this repository**, only §27's hooks. The protocol gateway (§34: D366–D371, D373, D377, D379, plans GW1–GW4) and the Cloudflare target (§35 on PR #179, plan CF1) move to `loam-platform`; §34 becomes a stub keeping D360–D365, D372, D374–D376, D378; RN1 loses its usage-event task | The owner's ruling of 2026-10-02: adoption from open source, the hosted cloud and commercial components private for fundraising | Proposed · owner ruling 2026-10-02 |
| D441 | 2026-10-02 | **Knative Serving runs the `http-port` contract** (§38 §3.1–§3.2): `KnativeRunner` implements D375 (one Knative `Service` per function, one revision per version, scale to zero, `runtimeClassName: gvisor`); T0 and T1 stay on the node supervisor. Refines §24 §11's F2 (Knative schedules T2 pods when enabled) | A server on `$PORT` is exactly a Knative Service; the supervisor's many-tenants-per-process model is not | Proposed |
| D442 | 2026-10-02 | **Kourier is Knative's ingress, internal only** (§38 §3.2): Envoy stays the edge (D184); the gateway authenticates and authorizes, then calls Kourier's internal service; every Knative `Service` is cluster-local | No function is reachable around the gateway | Proposed |
| D443 | 2026-10-02 | **One Kubernetes namespace per Loam namespace for Knative** (§38 §3.3): `loam-ns-<namespace>`, default-deny `NetworkPolicy`, `ResourceQuota` and `LimitRange` from the namespace's limits, service accounts with no API token; created by `loam-operator`. Quotas are enforced here; setting them per plan stays `loam-platform` (D220) | Kubernetes' own isolation primitives, driven by the limits D65 already stores | Proposed |
| D444 | 2026-10-02 | **No meter on Knative** (§38 §3.4): `KnativeRunner` returns `usage: None` and writes no host reports; Knative pods are T2 sandboxes under §27 §3.2's labels; queue-proxy and activator metrics and edge access logs are further open hooks. Adds §27 §3.5a | The owner's "no metering" (2026-10-02); D190, D202 | Proposed · owner ruling 2026-10-02 |
| D445 | 2026-10-02 | **Knative Eventing is an adapter** (§38 §3.5): Loam streams (D270) and the Event Fabric (§32 D331) stay the logs; `loam-knative-source` delivers stream records to any Knative sink as binary-mode CloudEvents, at least once; Loam's `POST …/streams/{stream}/events` is documented as a Knative sink; `InMemoryChannel` for development; the production broker is Q443 | Eventing is a delivery layer; a third log would split the event model | Proposed |
| D446 | 2026-10-02 | **Knative via the Knative Operator** (§38 §3.2, §9): `KnativeServing` and `KnativeEventing` 1.23 with `kubernetes.podspec-runtimeclassname`, `kubernetes.podspec-securitycontext` and `kubernetes.podspec-volumes-emptydir` enabled; `knative.enabled: false` by default | gVisor needs the runtime-class flag (disabled by default at 1.23); the Operator's CRs give Argo CD a health signal | Proposed |
| D447 | 2026-10-02 | **Authentik's open-source edition is the default IdP** of the Kubernetes distribution and the showcase (§38 §4): an unmodified separate service (2026.8.3, Postgres only), only code outside `authentik/enterprise/`, no licence key ever. **Supersedes D-SC-3** (Keycloak); **amends D221** (SAML brokered through Authentik) | The owner's "Authentik so no paid plan"; MIT core with blueprints, passkeys and outposts | Proposed · owner ruling 2026-10-01 |
| D448 | 2026-10-02 | **Authentik's usable features** (§38 §4.2): OAuth2/OIDC provider (code + PKCE, client credentials with JWT federation, device code, refresh, RFC 8693 token exchange since 2026.8.0, RFC 7591 registration); SAML, SCIM (static token), LDAP, RADIUS (PAP), Proxy, RAC providers; OAuth, SAML, LDAP, Kerberos, SCIM sources; flows and stages incl. TOTP, WebAuthn and passkeys; RBAC; brands; blueprints; outposts. **Excluded** (Enterprise, 2026-10-02): multi-tenancy, Google Workspace and Entra providers, SSF, WS-Federation, agent accounts, SCIM OAuth auth, RADIUS EAP-TLS, the source stage, mTLS stage, account lockdown, password history, enhanced audit, reports and CSV exports, lifecycle, PAM, device connectors | Checked against `authentik/enterprise/*` at 2026.8.3 and the Enterprise features page | Proposed |
| D449 | 2026-10-02 | **Loam's gateway stays the authority for Loam tokens** (§38 §4.3, §5): people sign in through Authentik (code + PKCE; device code for `loam login`); the gateway exchanges the Authentik token (RFC 8693) for a Loam access token; listeners verify only Loam tokens; agents stay Loam principals (§19 P5) with federation, delegation and vending unchanged; Biscuit (D188) and OpenFGA (D66, D67) unchanged; `groups` become `team#member` tuples at sign-in | One verifier on every listener; agent features do not depend on Enterprise | Proposed |
| D450 | 2026-10-02 | **The single binary keeps built-in sign-in** (§19 P7); Authentik is the default where Kubernetes is; any OIDC IdP works (§38 §4.5) | Air-gapped and laptop installs; IdP-agnostic gateway | Proposed |
| D451 | 2026-10-02 | **D111 narrowed** (§38 §1): identity, MFA, SSO and SAML brokering are Authentik's; the remaining unified-auth work (verification on every listener, API keys, agent tokens, TLS, leaving loopback) is MT1 for the native API, console API and MCP, and each other listener's plan adopts MT1's verifier | Splits a plan that blocked every listener into one plan and per-listener adoption | Proposed |
| D452 | 2026-10-02 | **Authentik configured by blueprints, installed from its upstream chart** (§38 §4.4): Loam's blueprints (Apache-2.0) under `deploy/authentik/blueprints/`; the chart `goauthentik/helm` is **GPL-3.0**, so it is referenced by an Argo CD `Application`, never vendored | Configuration as code; no copyleft files in this repository | Proposed |
| D453 | 2026-10-02 | **"GitOps from Clever Cloud" = Clever's open-source operator and infrastructure tooling** (§38 §6.1): the `clever-kubernetes-operator` fork (D185), `terraform-provider-clevercloud` and `karpenter-provider-clever-cloud` on CKE, `clever-tools` as CLI reference. Clever publishes no GitOps engine (checked 2026-10-02), so **Argo CD stays** (D186) | The owner's "for gitops clever cloud", made concrete against what Clever actually publishes | Proposed |
| D454 | 2026-10-02 | **A Flux layout with the same order** for the single-node profile (§38 §6.1); Argo CD stays the default and tested path | Argo CD's footprint on small clusters (Q-RT-11) | Proposed |
| D455 | 2026-10-02 | **New sync waves** (§38 §6.3; amends §25 §6.3): CNPG and Knative Operator CRDs at −2 and operators at −1; `authentik-db` at 1; Authentik at 2; `KnativeServing`/`KnativeEventing` at 3; `loam-knative-source` at 5; Lua health checks for the new kinds | Health-gated ordering, as D186 | Proposed |
| D456 | 2026-10-02 | **The reference small cluster is k3s or k3d with `--disable traefik`** (§38 §6.4) | Envoy and Kourier own ingress; matches CI | Proposed |
| D457 | 2026-10-02 | **Licences** (§38 §9): Knative (Serving, Eventing, Operator, Kourier, `func`) Apache-2.0; Authentik MIT outside `authentik/enterprise/`, unmodified; its chart GPL-3.0, referenced only; CNPG, Argo CD, Flux, k3s Apache-2.0; the operator fork MIT | D11; nothing enterprise or copyleft linked or vendored | Proposed |
| D458 | 2026-10-02 | **A CI guard keeps Authentik free of Enterprise** (§38 §1; MT1 Ruling 7): no licence in values, no `AUTHENTIK_TENANTS__ENABLED`, a blueprint lint against enterprise app labels derived from the image, and an e2e check that the licence summary is empty | An open-core dependency needs a mechanical check, not a promise | Proposed |
| D459 | 2026-10-02 | **Track MT** (§38 §10): MT1 Authentik identity, MT2 Knative, MT3 GitOps; MT1 and MT2 independent, MT3 wires both | Small stacked PRs on the one-build machine | Proposed |

## 2. Open-question rows (append to `13-decision-log.md` → Open questions)

| # | Question | Owner | Needed by |
|---|---|---|---|
| Q440 | Authentik's SCIM provider is free; keep SCIM provisioning into Loam in `loam-platform` (D221), or move it to OSS for adoption (§38 §4.3) | Founder | MT1 Task 5 |
| Q441 | Keep the single binary's built-in password and TOTP (§19 P7), or require an external OIDC IdP once MT1 lands | Founder | MT1 Task 0 |
| Q442 | Hosted Loams Cloud identity: Clerk (`loam-cloud` today) or Authentik, given "no paid plan" (decided in `loam-platform`; cross-reference only) | Founder | Before the hosted beta |
| Q443 | Knative Eventing's production broker: the Kafka broker over Loam's Kafka gateway (M5), a Loam broker class over streams, or none (§38 §3.5) | Eng | MT2 Task 6 |
| Q444 | gVisor mandatory for every `KnativeRunner` function, or optional for trusted single-org code (§38 §3.2) | Founder | MT2 Task 2 |
| Q445 | Should `KnativeRunner` emit request and wall-time reports (no CPU) for showback, or stay at `usage: None` (D444) | Founder | MT2 Task 4 |
| Q446 | Kourier, or `net-gateway-api` on Envoy Gateway so Envoy is the only proxy (§38 §3.2) | Eng | MT2 Task 3 |
| Q447 | Authentik's Postgres: CloudNativePG now (D230), Loam Postgres (§28) later | Eng | MT3 Task 3 |
| Q448 | Does Authentik send OIDC back-channel logout, so a removed user's session ends before its refresh (§38 §4.3) | Eng | MT1 Task 4 |
| Q449 | Flux as the default for the single-node profile if Argo CD measures too heavy (merges Q-RT-11) | Eng | MT3 Task 6 |
| Q450 | Give §34's retained vendor-neutral decisions their own document, and split GW1's vendor-neutral tasks (`buf breaking`, the CloudEvents profile, the event Arrow mapping) into an open plan | Founder | Before GW1 starts in `loam-platform` |
| Q451 | The `loam.dev/*` pod labels under the `loams` rename: keep, or `loams.dev/*` with the rename PR | Eng | Rename PR |
| Q452 | `loam-knative-source` for Iggy topics (§32): in MT2 or with FL1 | Eng | After FL1 |
| Q453 | Authentik upgrade cadence and security-patch policy for self-hosters | Eng | MT3 Task 2 |

Existing questions to annotate (edit in place):

- **Q30** (the unified auth plan): append "Narrowed 2026-10-02 (D451, §38): MT1 is the identity half; listeners adopt MT1's verifier in their own plans."
- **Q-RT-11** (Argo CD's footprint): append "See Q449 and MT3 Task 6 (2026-10-02)."
- **Q362** (§34, if integrated): "Answered 2026-10-02: no metering in OSS (D440, D444)."

## 3. README and roadmap rows, ready to paste

### 3.1 `docs/design/README.md`, reading-order table (after the last row present)

| 38 | [Knative, Authentik and GitOps](38-knative-authentik-gitops.md) | The 2026-10-02 boundary reconfirmation and what moved to `loam-platform`; Knative Serving for the `http-port` contract (`KnativeRunner`, per-namespace tenancy, Kourier behind the gateway, no meter) and Knative Eventing as an adapter to Loam streams; Authentik's open-source edition as the IdP (the checked feature list, the RFC 8693 exchange for Loam tokens, groups to OpenFGA, blueprints, the Enterprise guard), replacing Keycloak; GitOps with Clever's open-source operator and CKE tooling under Argo CD, new waves, a Flux layout and the k3s profile | **Proposed** |

Also change row 34's text (if §34's row is present) to: "Stub since 2026-10-02: the protocol gateway moved to `loam-platform`; keeps the standards charter, the narrow waist, the CloudEvents profile, the high-rate path and the events → Arrow mapping".

### 3.2 `docs/plans/README.md`, a new section (after the last track section)

```markdown
## Track MT: Knative, Authentik and GitOps for self-hosted Loam

Design reference: [38 Knative, Authentik and GitOps](../design/38-knative-authentik-gitops.md) (D440–D459), amending [19](../design/19-console-identity-and-agents.md), [22](../design/22-showcase-suite.md), [24](../design/24-cpu-time-runtime.md) §16, [25](../design/25-clever-cloud-stack.md) §6 and [27](../design/27-usage-hooks.md) §3.5a. No metering (D444).

| Plan | Scope | Depends on | Status |
|---|---|---|---|
| [MT1: Authentik as the identity provider](2026-10-02-mt1-authentik-identity.md) | Blueprints and the Enterprise guard; trusted issuers and a JWKS cache; the RFC 8693 exchange for Loam tokens; groups → teams → OpenFGA tuples; console sign-in (PKCE) and `loam login` (device code); the showcase moves from Keycloak; non-loopback listeners behind TLS and verification | §19's M2 identity work, D66's outbox | Planned |
| [MT2: Knative Serving and Eventing](2026-10-02-mt2-knative.md) | Knative through the Operator, off by default; per-namespace tenancy in `loam-operator`; `KnativeRunner`; hooks with no meter; gateway dispatch for `http-port`; `loam-knative-source` and Loam as a Knative sink | RN1 Tasks 1–3, `loam-operator` | Planned |
| [MT3: GitOps with Clever's open-source stack](2026-10-02-mt3-gitops-clever.md) | Waves and `Application`s for CNPG, Authentik and Knative; Lua health checks; Authentik on CNPG; the CKE profile with Clever's Terraform and Karpenter providers; a Flux layout; the measured single-node k3s profile | MT1 Task 1, MT2 Task 1, §25's layout | Planned |
```

Remove the GW1–GW4 rows from the Track GW section if they were integrated from `_pending/34-log.md` (that log now lists only RN1).

### 3.3 `docs/design/12-roadmap-testing-risks.md`

**§1 Milestones, a new row after the last parallel track:**

| **MT** | Knative, Authentik and GitOps for self-hosted Loam (§38), parallel track | MT1 Authentik identity (RFC 8693 exchange, groups to OpenFGA, the Enterprise guard, the showcase off Keycloak); MT2 Knative for `http-port` with per-namespace tenancy and no meter, Eventing as an adapter; MT3 waves, health checks, Flux layout, k3s and CKE profiles | k3d e2e: sign in through Authentik and call the API with an exchanged token; an `http-port` function scales from zero under gVisor; the waves reach Healthy from an empty cluster on Argo CD and on Flux |

**§3 Risk register, new row:**

| 36 | Authentik moves a feature Loam uses into its Enterprise tree, or changes the licence | Low–Medium | Medium | The guard (D458) on every bump; IdP-agnostic gateway, Keycloak as fallback; pin minors |

## 4. Conflicts with existing decisions (every superseded or amended D-number)

| Existing | What changes | Resolution |
|---|---|---|
| **D-SC-3** (§22): Keycloak is the suite's IdP | Authentik | **Superseded by D447** |
| **D221**: SAML brokered through Keycloak by self-hosters | Through Authentik | **Amended by D447**; the rest of D221 (SCIM, enforced SSO, hosted audit in `loam-platform`) stands |
| **D111**: one unified auth plan after M1 | MT1 plus per-listener adoption | **Narrowed by D451** |
| **§19 P7, §6**: Keycloak as the SAML broker | Authentik is the documented broker | **Amended** (D447, D450); built-in sign-in kept |
| **D220**: the boundary | Reconfirmed; Knative, Authentik and GitOps added as open | **Reconfirmed by D440**, not changed |
| **D366–D371, D373, D377, D379** (§34, merged in #177, not yet in the decision log) | Moved to `loam-platform` with the gateway | **Withdrawn from this repository by D440**; do not integrate them |
| **D379** (§34): adapters, negotiation and the ad-tech conformance suite are Apache-2.0 here | `loam-platform` | **Superseded by D440** |
| **D380, D383–D387** (§35, PR #179, unmerged) | Moved to `loam-platform` with the Cloudflare target | **Withdrawn by D440**; PR #179 is edited to drop §35 (D381 and D382 stay, re-homed in §36) |
| **D376 item 4** and RN1 Task 6: the usage CloudEvents form built here | Built in `loam-platform` | **Amended by D440, D444**; the record spec in §27 §3.6 stays |
| **D375**: a thin `WorkersRunner` in this repository after D111 | Commercial; plugs in as `RunnerKind::External` | **Amended by D440** |
| **§24 §11 F2**: Loam schedules T2 pods | Knative schedules them when enabled | **Refined by D441** |
| **D186**: Argo CD | "GitOps from Clever Cloud" | **No conflict** (D453): Clever publishes no GitOps engine |
| **§22 §4.4**: Authentik's enterprise split as a reason to prefer Keycloak | A CI guard | **Answered by D458** |
| **Q361**: event type prefix | The owner ruled `io.loams.dev.` on 2026-10-01 | Answered; §02's and §27's `dev.loam.*` types move with the rename PR |
| **Q362**: showback and billing in OSS | No metering in OSS | **Answered by D440, D444** |

## 5. Edits this branch makes to existing docs

| File | Edit |
|---|---|
| `docs/open-core.md` | A "Reconfirmed 2026-10-02" section (no change to the boundary) |
| `docs/design/34-protocol-gateway-and-standards.md` | Replaced by a stub keeping D360–D365, D372, D374, D378 and pointers for D375, D376 |
| `docs/plans/2026-10-01-gw{1,2,3,4}-*.md` | Deleted (moved to `loam-platform`) |
| `docs/design/_pending/34-log.md` | Gateway rows removed; Q361, Q362 marked answered; Track GW reduced to RN1 |
| `docs/plans/2026-10-01-rn1-runner-usage.md` | References retargeted to §24 §16 and §27 §3.6; Task 6 (usage events) removed; `RunnerKind` gains `Knative` and `External` |
| `docs/design/24-cpu-time-runtime.md` | Status note; §16 holds the `Runner` trait and runner table; a Knative paragraph |
| `docs/design/27-usage-hooks.md` | Status note; §3.5a (Knative pods, no meter); §3.6 marks the event form and the Workers path as built in `loam-platform` |
| `docs/design/19-console-identity-and-agents.md`, `22-showcase-suite.md`, `25-clever-cloud-stack.md` | One amendment note each under the status line |

## 6. Web checks (2026-10-02)

| Claim | Finding | Source |
|---|---|---|
| Authentik's licence split | MIT outside `authentik/enterprise/`; the EE licence inside requires a subscription for production use; dev and test use allowed | `goauthentik/authentik` `LICENSE`, `authentik/enterprise/LICENSE` at `version/2026.8.3` (2026-09-17) |
| Which features are Enterprise | As D448 | docs.goauthentik.io/enterprise/enterprise-features; the `authentik/enterprise/*` tree |
| Multi-tenancy | Enterprise and alpha; a licence per additional tenant; `AUTHENTIK_TENANTS__ENABLED` | docs.goauthentik.io/sys-mgmt/tenancy |
| Token exchange | RFC 8693, impersonation and delegation with `act`, since 2026.8.0, in the MIT tree | docs.goauthentik.io/add-secure-apps/providers/oauth2/token_exchange; `authentik/common/oauth/constants.py` |
| Redis | Removed in 2025.10; Postgres only, ~50% more connections | docs.goauthentik.io/releases/2025.10; goauthentik.io blog 2025-11-13 |
| Authentik chart licence | GPL-3.0 | `goauthentik/helm` |
| Knative | Serving and Eventing knative-v1.23.0 (2026-07-29/28); Operator knative-v1.23.1 (2026-09-01); Kafka broker knative-v1.23.1; CNCF graduated 2025-10-08; `kubernetes.podspec-runtimeclassname` disabled by default | GitHub releases; `config/core/configmaps/features.yaml`; CNCF announcement |
| Clever Cloud GitOps | No reconciler or deployer published; operator MIT v0.8.0; Terraform provider v2.3.0; Karpenter provider v0.13.0 (2026-10-01) | GitHub `CleverCloud` organisation |
| Argo CD, Flux, k3s, CNPG | v3.5.3 (2026-09-14), v2.9.6 (2026-10-01), v1.37.1+k3s1 (2026-09-30), v1.30.1 (2026-09-23), all Apache-2.0 | GitHub releases |
