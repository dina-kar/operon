# Build on Loams

Loams is Apache-2.0. OSS projects and companies (cloud providers, SaaS builders, AI labs) are welcome to **fork, embed, host and build commercial products on it**. In return we ask for one thing: **upstream first**. If your product needs a change in Loams, send it here instead of carrying it in a private fork.

The reasons are practical. A change that lives upstream is maintained by everyone, is tested by our CI and works with the next release. A change that lives in a fork is yours to rebase forever.

> The examples below are **illustrations of how the process works**. They are not endorsements, and they do not announce partnerships with any company named.

## What stays open, and what is yours

The [open-core boundary](docs/open-core.md) applies to everyone. Everything a single organization needs to self-host Loams is in this repository. Your product can add whatever it likes on top (a console, billing, a vertical UI, a hosted offer) and keep it private, as long as you follow the Apache-2.0 license and the [trademark policy](TRADEMARKS.md).

Upstream the things that are **generic**: a new extension point, a bug fix, a new adapter that any user of that platform would want, a protocol compatibility fix, docs. Keep in your product the things that are **specific to you**: your brand, your UI, your pricing and billing, your hosted fleet's operations.

## How to propose a change upstream

1. **Start a [Discussion](https://github.com/ostrium-labs/loams/discussions)** in the Ecosystem & Partners category. Say what you are building and what you need from Loams. This is cheap and lets us point you at an existing extension point before you write code.
2. **Open an RFC issue** when the change touches a format, a protocol, a public API or a design decision. State the problem, the proposal and the alternatives. Maintainers aim to respond within a few working days; design-level changes get a comment window, as described in [GOVERNANCE.md](GOVERNANCE.md).
3. **Send small PRs to `dev`**, one concern per PR, each with tests and a DCO sign-off (`git commit -s`). See [CONTRIBUTING.md](CONTRIBUTING.md). Your first merged PR makes you a `committer`.

## Extension points

Prefer these over patching the core. If the one you need doesn't exist yet, that is exactly the kind of change to propose.

| Extension point | Use it to | Where to look |
|---|---|---|
| Plugins and providers | Add a model, embedding, auth, secrets or state provider | [`docs/design`](docs/design/README.md) |
| `Runner` adapters | Run functions and jobs on another compute substrate (process, Lambda, Knative, your own) | [§34 D375](docs/design/34-protocol-gateway-and-standards.md), [§24](docs/design/24-cpu-time-runtime.md) |
| Deploy targets | Package Loams for a platform (Helm, Argo CD, Terraform, a provider's template format) | [Deploy buttons](docs/ecosystem/deploy-buttons.md), [`deploy/`](deploy) |
| Wire protocols and SDKs | Add or fix a client compatibility surface, or a generated SDK | [`sdks/`](sdks), [`conformance/`](conformance) |
| Usage and audit hooks | Observe usage and audit events from your own platform (hooks only; no metering ledger here) | [§27](docs/design/27-usage-hooks.md) |

## Keeping your fork rebaseable

- Track `main` for releases and `dev` for what is coming. Rebase onto them regularly instead of merging forever.
- Put your product code in **separate crates, packages or directories**, not inside Loams files. Touch core files only for changes you intend to upstream.
- Keep each carried patch small, labelled and linked to its upstream issue. Drop it when the upstream PR merges.
- Don't rename Loams crates or protobuf packages in your fork; it makes every rebase a conflict. Use your own wrapper crates.
- Wire formats and on-disk formats are open and versioned; don't fork them. Propose changes upstream so your data stays readable by Loams.

## Worked examples (illustrative)

### A "T3 Code Cloud" product built on Loams

Suppose a team that builds an AI coding tool wants to ship a hosted cloud product, "T3 Code Cloud", on top of Loams for storage, retrieval, durable agent runs and sandboxed functions. They could fork Loams or embed its crates and packages.

| Upstream it (to Loams) | Keep it in the product |
|---|---|
| A missing extension point, for example a hook in the agent runner for their sandbox | Their IDE and web UI, brand and onboarding |
| Fixes and performance work found while scaling their workload | Their account, plan and billing system |
| A `Runner` adapter or provider that other products would also use | Their prompt library, agents and product logic |
| Compatibility tests and docs for flows they rely on | Their hosted operations and support tooling |

They would say "built on Loams" (see [TRADEMARKS.md](TRADEMARKS.md), a provisional policy pending legal review), run Loams from a tagged release plus a short, documented patch queue, and use [Discussions](https://github.com/ostrium-labs/loams/discussions) to agree each upstream change before sending it.

### A "Deploy to Cloudflare" button

Suppose a cloud provider, say Cloudflare, wants users to run Loams on the provider's own infrastructure with one click. That is a **deploy-target adapter** plus a **button**:

1. The provider (or a community member) contributes a template under `deploy/<provider>/` that maps Loams's components onto the provider's services (compute, object storage, key-value or SQL metadata, queues).
2. The deployment is **BYOC**: it runs in the user's account on the provider, and the user owns the bucket and the data.
3. Any provider-specific compute substrate is added as a `Runner` adapter upstream when it is generic.
4. The README of `deploy/<provider>/` carries the button, built to the [button specification](docs/ecosystem/deploy-buttons.md).

The provider keeps its own account flow, console and billing. Loams only needs the template and adapter, which are Apache-2.0 and reviewed like any other PR.

## Becoming a marketplace listing

If you want your project to be installable from the Loams Cloud marketplace, read [Publish your project to the Loams Cloud Marketplace](docs/marketplace/publishing.md).

## Talk to us

Start in [Discussions](https://github.com/ostrium-labs/loams/discussions). See [MAINTAINERS.md](MAINTAINERS.md) for who decides.
