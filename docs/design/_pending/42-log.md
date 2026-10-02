# Pending log for §42 (Cloudflare's Birthday Week 2026 betas)

Staged 2026-10-02 so the decision log is not edited from this branch. Paste each block into `docs/design/13-decision-log.md` after D559 and Q559, renumbering if D560–D579 or Q560–Q579 were taken meanwhile. All rows are **Proposed**; the direction is the owner's of 2026-10-02: "search the latest beta features in the Cloudflare blog and use them."

## Decisions

| ID | Date | Decision | Rationale | Status |
|---|---|---|---|---|
| D560 | 2026-10-02 | **Cloudflare betas enter Loams only as optional adapters behind traits; none is needed for self-hosting.** Hosted-service uses are private (loam-platform docs 03 to 07) | Open-core rule; betas change | Proposed |
| D561 | 2026-10-02 | **The bucket WAL stays the default and source of truth for Loams Git.** Artifacts (1 GB per repository, 32 MB per blob, Git-level API) cannot be a `WalStore`, `RefLog` or `BlobStore` | The limits and the missing fenced append | Proposed |
| D562 | 2026-10-02 | **`loams-git-artifacts` (Apache-2.0): a mirror sink and a `Materializer` and workspace host for Artifacts** | Gives agents Cloudflare-native repos without moving truth | Proposed |
| D563 | 2026-10-02 | Artifacts repo tokens are held by the credential broker, never in the WAL | §39 §6 | Proposed |
| D564 | 2026-10-02 | Hosted Artifacts use (namespaces, jurisdiction, quotas) is private | Open-core | Proposed |
| D565 | 2026-10-02 | **Q507 default: the client-tool relay for credentialed work; a `remote` provider for unattended public-web work**, one tool contract | Credentials stay on the person's machine | Proposed |
| D566 | 2026-10-02 | The remote provider targets Browser Run CDP and Playwright endpoints; Kitesurf is optional because it is closed source with no licence yet; Playwright MCP stays the self-host provider | Stable, documented surface | Proposed |
| D567 | 2026-10-02 | A remote browser is a third party: no user-credential `secret_ref` fills and no persistent profile by default; full audit | Integrity | Proposed |
| D568 | 2026-10-02 | WebMCP is an enhancement behind feature detection, never the only path (Community Group draft of 2026-09-30) | Experimental | Proposed |
| D569 | 2026-10-02 | The console and plugins register one WebMCP tool per plugin action from the same action registry as the MCP server, enforcing OpenFGA and approvals | One description per action | Proposed |
| D570 | 2026-10-02 | The web bridge's v2 adds `list_webmcp_tools` and `call_webmcp_tool` | Functions over UI driving | Proposed |
| D571 | 2026-10-02 | `loams-iceberg` accepts an external Iceberg REST catalog (Basin Catalog, formerly R2 Data Catalog; GA 2026-10-01) beside Lakekeeper | Open adapter | Proposed |
| D572 | 2026-10-02 | Read first; write only after a conformance spike; one writing catalog per table | Safety | Proposed |
| D573 | 2026-10-02 | The hosted analytics on Pipelines and Basin SQL is private | Open-core | Proposed |
| D574 | 2026-10-02 | SF4 gets a `WorkspaceSnapshot` seam: branch plus volume snapshot on Knative, Containers snapshots on Cloudflare (private) | Pause and resume | Proposed |
| D575 | 2026-10-02 | No open design uses the deprecated `Container` or `Sandbox` classes; private docs move to `ctx.container` | Deprecated after 2026-12-31 | Proposed |
| D576 | 2026-10-02 | CF1 treats Rust and Tokio on Workers through Emscripten as an experimental preview and pins the patch set | Experimental | Proposed |
| D577 | 2026-10-02 | The Monetization Gateway (closed beta) is private | Billing is not open | Proposed |
| D578 | 2026-10-02 | Dynamic Workers and Facets are the private Cloudflare-target tier for untrusted agent code; the open tier is workerd under gVisor | Open-core | Proposed |

## Open questions

| ID | Question | Owner | Needed by |
|---|---|---|---|
| Q560 | Does Artifacts expose atomic multi-ref updates or a push log that would allow a `RefLog`? Read the API reference | Eng | GT5 Task 0 |
| Q561 | Which Git protocol versions and push limits does Artifacts support? (Not stated) | Eng | GT5 Task 0 |
| Q562 | Billing starts 2026-10-14 (docs) or 2026-10-15 (blog)? | Eng | Before any live use |
| Q565 | Is Browser Run generally available or beta, and what are its rates? | Eng | AP1c Task 0 |
| Q566 | Kitesurf's licence and date when it is open-sourced | Founder | When announced |
| Q568 | Chrome and Edge WebMCP status on a primary page (origin trial range) | Eng | AP1d Task 0 |
| Q571 | The REST endpoint, auth and write support of the catalog (Basin Catalog, formerly R2 Data Catalog; GA 2026-10-01); is the rename real? | Eng | FL3 Task 0 |
| Q572 | Do commits from `iceberg-rust` to that catalog pass Loams's conformance tests? | Eng | FL3 |
