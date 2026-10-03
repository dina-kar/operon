# Publishing Loams: registries, secrets and the release path

Status: **infrastructure only, nothing published**. This page is the checklist a
maintainer works through before the first real publish. It is written for
`ostrium-labs/loams`; the decisions it follows are in
[`docs/design/13-decision-log.md`](../design/13-decision-log.md).

## What exists today, and what does not

Issue #254 asks for six ecosystems. Only one of them is a release path this
repository can run now, and the rest are recorded here as deliberate omissions
rather than as empty workflows.

| Registry | State | Why |
|---|---|---|
| **crates.io** | **Workflow present, publishing off.** [`.github/workflows/release-crates.yml`](../../.github/workflows/release-crates.yml) verifies every publishable crate on a published release and uploads only when a maintainer dispatches it inside the `crates-io` environment. | The namespace is `loams` on crates.io (D400), the crates are `loams-*`, and 18 of them are publishable as they stand. |
| PyPI | Not implemented | There is no Python SDK yet (#285). `sdks/` holds `typescript/` only, and D400 keeps the PyPI name `loams` reserved for it. A workflow that builds nothing would be a file that only looks finished. |
| npm | Not implemented | The packages are publish-shaped but nothing publishes them yet. `@loams/client` (SDK2 Task 0) and `@loams/live` ship from `sdks/typescript/packages/`, with `exports`, types, a build script and a conformance suite; `@loams/proto` in `web/` is generated from the same protos and is consumed from the workspace. Publishing them needs the version and provenance decision in D400, and the thirteen-language release cadence the design describes, so the `@loams` scope (D400) stays reserved. |
| Maven Central | Not applicable | D400 defers Java SDKs: there is no Loam-written Java. The Kotlin template in `buf.gen.kotlin.yaml` generates code for `ostrium-labs/loams-mobile`; a client library is that repository's decision, and publishing a Java artifact from here would put a second, unowned release in the pipeline. |
| Go modules | Not applicable | No Go module and no `go.mod` anywhere in the tree. D101 places a Go SDK in M2, and D400 gives it the `loams.dev/...` path, which needs a `go-import` responder on `loams.dev` — that route belongs to the site repository, not here. |
| GitHub Packages (`ghcr.io`) | Not implemented | There is no image to publish: the only Dockerfile in the tree is `deploy/dapr/edge/Dockerfile`, a sidecar for one deployment. D292 chooses cargo-dist for binaries and GitHub Releases, and image publishing (attestations, cosign) has no artifact to run against yet. It should arrive with the server image, not before it. |

A rule for later: a registry gets a workflow when something in this repository
is ready to be installed from it, not when the issue lists it.

## What crates.io actually publishes

The crate list is declared in the workflow's `CRATES` and checked by
[`scripts/ci/publishable-crates.py`](../../scripts/ci/publishable-crates.py),
which derives the same list from the manifests and fails the run when the two
disagree. A crate is publishable when its manifest does not say
`publish = false` and none of its normal dependencies is itself unpublishable;
dev-dependencies do not count, because cargo strips them from a published
manifest.

Two crates are excluded by a `publish = false` this change adds, and both are
structural, not a judgement about their API:

- `loams`: its optional `loams-tikv`, `loams-durable` and `loams-meta-tikv`
  dependencies appear in the published manifest, and all three are
  unpublishable. `loams-tikv` and `loams-durable` depend on git revisions of
  private forks, which no registry can resolve. Uploading the binary today
  would produce a crate nobody can build.
- `loams-safekeeper`: same shape through its `tikv` feature and `loams-tikv`.

Both flags are one line each, and both come off in the change that publishes
the binary (D294's `cargo install loams`) or the first release after the forks
are vendored. This page is where that promise is written down; the crate
comments say it in place.

## Before the first publish

1. **Claim the namespace.** crates.io rejects a crate whose name is taken. The
   `loams` and `loams-*` names are free as of 2026-10-03 (the sparse index
   answers 404 for `loams`, `loams-common`, `loams-collection` and
   `loams-store`), but crates.io does not reserve them: whoever uploads
   `loams-store` first owns it, permanently.
2. **Reserve every name, by hand, with a token.** crates.io's trusted publishing
   only issues a token for a crate that already exists, so the first upload of
   each of the 18 crates is a manual `cargo publish` from a workstation with a
   freshly generated token (`cargo login`, scope `publish-write`, then revoke).
   Do this once per crate, lowest dependency first. No token is stored in this
   repository at any point.
3. **Configure the trusted publisher** on crates.io, for each crate: repository
   `ostrium-labs/loams`, workflow `release-crates.yml`, environment `crates-io`.
   From then on CI mints its own short-lived token through
   `rust-lang/crates-io-auth-action` and no credential is stored in GitHub.
4. **Create the GitHub environment** `crates-io` with required reviewers, so an
   upload waits for a second maintainer, and restrict it to the protected tags
   (`v*`). The environment name is part of the trusted-publisher configuration,
   so a token cannot be minted outside it.
5. **Add the missing package metadata.** Every crate carries a description and
   the Apache-2.0 licence, but none declares `repository`, `homepage`,
   `documentation` or `keywords`; `cargo publish --dry-run` warns about it and
   the verify job prints a warning per missing field. Add them in
   `[workspace.package]` and inherit them per crate, with at most five
   keywords.
6. **Set the release branch variable** if releases are not tagged on the
   repository's default branch: a repository variable `RELEASE_BRANCH`. Without
   it the guard requires the tag to be on the default branch, which is the
   integration branch here.
7. **Bump the workspace version in a PR, merge it, then tag.** The guard
   compares the tag against `[workspace.package] version` and refuses a tag that
   does not match, so a mis-tagged release cannot reach the registry.

Secrets, environments and variables this path uses, in full:

| Name | Kind | Needed by | Value |
|---|---|---|---|
| `crates-io` | environment (required reviewers, protected tags `v*`) | the publish job | none; it is a gate, not a secret |
| `RELEASE_BRANCH` | repository variable (optional) | the guard, verify and publish jobs | the branch releases are tagged on; defaults to the default branch |
| `RUNNER_MAIN` | repository variable (optional, pre-existing) | verify and publish | a runner label for the long build |

There is no other secret. In particular there is no `CARGO_REGISTRY_TOKEN`
secret, no npm token, no PyPI token and no Maven GPG key: the only credential
is the OIDC token GitHub mints for this workflow and crates.io revokes when the
job ends.

## How a release runs

1. Merge the version bump. `dev` is the integration branch.
2. Tag `vX.Y.Z` and publish a GitHub Release for it. That fires the workflow's
   `release` trigger, which runs the guard and `verify`: the tag is a real
   release, it is on the release branch at the workspace version, the crate list
   matches the manifests, and every crate packages and compiles the way crates.io
   will compile it. **Nothing is uploaded by this step.**
3. Dispatch `Release (crates.io)` with `tag: vX.Y.Z` and `publish: true`. The
   same guard and verify run again, the `crates-io` environment asks for a
   reviewer's approval, and the crates upload in dependency order.
4. A failure part way through is safe to repeat: cargo refuses a version that is
   already published, and the next dispatch continues with the crates that are
   not.

## Why a fork cannot reach the registry

- The only triggers are a published GitHub Release and a manual dispatch.
  Neither can be produced by a pull request, and nothing here uses
  `pull_request`, `pull_request_target` or `workflow_run`.
- A manual dispatch needs write access to the repository, which a fork does not
  have, and its `publish` input defaults to `false` anyway.
- The code that runs is checked out from the release branch, never from the tag
  and never from a pull request head, and the checkout keeps no credentials in
  `.git/config`.
- Every step of the guard reads the tag, the branch head and the release, so a
  tag pushed to a branch that is not the release branch is refused.
- The upload token is not a stored secret: it is minted from this workflow's
  OIDC identity, and crates.io only honours it for this repository, this
  workflow and this environment.

## Related

- [`docs/marketplace/publishing.md`](../marketplace/publishing.md) is a
  different thing: listing a third-party project in the Loams Cloud
  marketplace, not publishing Loams itself.
- D292 (cargo-dist for binaries and GitHub Releases), D294 (`cargo install
  loams`), D298 (the CLI track) and D400 (the `loams` package namespace).
- D220 and D552 (the open-core boundary and the `scripts/ci/no-metering.sh`
  guard). A publish path may not add a billing-grade name or a private package
  to anything it uploads; the guard runs over the workflows themselves, because
  they are tracked files.
