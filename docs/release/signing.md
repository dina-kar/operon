# Signing Loams releases

Status: **the pipeline is complete; the credentials are not**. Everything in this
repository that does not need an organisation owner's browser is merged. Four
items need one, and they are listed as [the handoff](#the-handoff) below with the
exact values each one needs.

This page supersedes nothing else: the crates.io half of the release path is
[`docs/release/publishing.md`](publishing.md) and is unchanged by this document.

## What is signed, and what is not

The owner ruled on 2026-10-03, and it is not the split issue #253 assumed:

| Platform | State | What that means for a user |
|---|---|---|
| **Linux** | **Signed**, through SignPath, once the handoff is done. Until then: unsigned, and every run says so. | Nothing changes for a Linux user. Linux package managers verify their own repository signatures; a detached signature on the Loams binary is provenance, not install trust. |
| **Windows** | **Unsigned**, by decision. Build it yourself: [`docs/build-from-source/windows.md`](../build-from-source/windows.md). | SmartScreen shows *Windows protected your PC* on first run because the publisher is unknown. It is an unknown-publisher warning, not a detected-malware warning, and it clears per-file once the user unblocks it. |
| **macOS** | **Unsigned**, by decision. Build it yourself: [`docs/build-from-source/macos.md`](../build-from-source/macos.md). | Gatekeeper blocks a quarantined download — either *the developer cannot be verified* or *Apple cannot check it for malicious software*, depending on how the file arrived. Right-click → Open, or `xattr -dr com.apple.quarantine`, runs it. There is no Developer ID, no notarisation ticket and no `spctl` acceptance. |

Windows and macOS are unsigned because the maintainers hold no Apple Developer
account and no Windows code-signing certificate, and buying either is a
deliberate future decision ([Q615](#decision-log-rows)). Shipping an unsigned
build with a documented local build path is better than shipping a build signed by
a certificate in someone else's name.

## What SignPath's free programme can and cannot sign

Verified 2026-10-03 against <https://docs.signpath.io/artifact-configuration/reference>,
<https://docs.signpath.io/trusted-build-systems/github>, <https://signpath.org/>
and <https://signpath.org/terms>. SignPath publishes an availability list per
signing directive, and the edition matters:

| Signing method | Code Signing Basic | Semantic | **Open Source Code Signing** (free) |
|---|---|---|---|
| `<rpm-sign>` (Linux `.rpm`) | no | yes | **yes** |
| `<debsigs-sign>` (Debian `.deb`) | no | yes | **no** |
| `<create-gpg-signature>` (detached GPG on any file) | no | yes | **no** |
| `<dsse-sign>`, `<smime-sign>`, `<create-cms-signature>`, `<create-raw-signature>` | no | yes | **no** |
| `<xml-sign>` | no | yes | **no** |
| `<apk-sign>` (Android `.apk`) | no | yes | **yes** |
| `<jar-sign>` (`.jar`, `.zip`) | yes | yes | **yes** |
| `<jsf-sign>` (`.json`) | no | yes | **yes** |
| `<authenticode-sign>` (Windows `.exe`, `.msi`, `.msix`, `.dll`, …) | yes | yes | **yes** |
| `<notation-sign>`, `<cosign-sign>` (containers) | — | — | — |

The first eight rows are quoted from the "Available for" line each directive
carries on the same page. The last two are weaker, and the difference matters:

- `<authenticode-sign>` states only that it is *"Not available for Code Signing
  Starter"* for `<windows-script-file>`; it carries no per-edition list. The
  `yes` in the OSS column is the Foundation's own premise — the free programme
  exists to issue Windows Authenticode certificates — rather than a quotation.
  **Unverified** against an edition table.
- `<notation-sign>` and `<cosign-sign>` state no edition at all, so the row is
  left blank rather than guessed. Nothing in this repository ships a container
  image, so neither is in scope.

Two consequences, and the first one is the important one:

1. **The only Linux artifact the free programme can sign is an `.rpm`.** Not a
   `.deb` (debsigs is Semantic-only), and not a detached GPG signature on a bare
   ELF binary or a `.tar.gz` (`<create-gpg-signature>` is Semantic-only). So the
   Linux signing path in this repository is built around RPM packaging, and
   [`signpath/artifact-configuration.rpm.xml`](../../signpath/artifact-configuration.rpm.xml)
   is the configuration that carries it.
2. **The free programme's headline capability is Windows Authenticode**, which
   this project is deliberately not using ([Q615](#decision-log-rows)). That is
   an unusual position to be in, and it is why the pipeline below is complete but
   idle: the certificate exists to sign Windows, and Windows is out of scope.

There is a third consequence that is not about formats. The Foundation also
requires that *"All signed binaries must have metadata attributes set and
enforced"* — product name set to the project's name, product version identical
across a build. SignPath's metadata-restriction attributes exist for `<pe-file>`,
`<msi-file>` and `<xml-file>` only, so for an RPM that obligation lands on the
packaging's own `Name`/`Version`/`Summary`. It is part of the RPM work and is
recorded as [Q618](#decision-log-rows).

`scripts/ci/signpath-artifacts.py` enforces the platform scope mechanically. It
classifies every file before a request is built and refuses anything that is not
an `.rpm` or a Linux x86-64/aarch64 ELF, so a Windows or macOS artifact cannot
reach SignPath even if someone adds it to the release by accident.

## How the flow works

[`.github/workflows/release-sign.yml`](../../.github/workflows/release-sign.yml)
runs on a published release and on a manual dispatch. Three jobs:

```
guard      the tag is a published release on RELEASE_BRANCH at the workspace
           version -- the same guard release-crates.yml runs, so the two release
           workflows cannot disagree about what a release is
preflight  decides whether this run signs, out loud, from the four configuration
           values below. Nothing else decides.
sign       self-test -> fetch -> classify -> refuse unless an .rpm is in the
           set -> store as a GitHub Actions artifact -> submit -> wait for the
           approver -> report the signing request URL
```

Two properties of the `sign` job are requirements, not choices:

- **It runs on `ubuntu-latest`, never on `vars.RUNNER_MAIN`.** SignPath's GitHub
  connector checks, for every OSS request, that all jobs of the workflow leading
  up to the request ran on GitHub-hosted agents, and `RUNNER_MAIN` is this
  organisation's self-hosted pool. A workflow that signed from there would fail
  the connector's own check.
- **It holds no write permission on the repository.** It submits the request and
  stops. Attaching the signed file to the release belongs to the release workflow
  (D292's cargo-dist pipeline), and a signing job that can also publish is a
  signing job whose output nobody re-reads.

The classification step is not advisory. It rejects every Windows and macOS
artifact by name and by content, and then a second step **refuses to submit
unless an `.rpm` is in the accepted set** — because the free programme cannot
sign a bare ELF (D621), and submitting one it provably cannot sign would fail
inside SignPath with a message about a configuration rather than about the real
problem. If a Semantic Code Signing policy is ever bought, that one check is the
line to revisit.

SignPath's Foundation terms require **a human approver on every release**, so the
`sign` job waits up to 30 minutes rather than polling. An approver is one of the
three roles below.

## The gate: what happens without configuration

Nothing is configured yet (see [the handoff](#the-handoff)), so this is what a
release does today. The behaviour is the point of this section.

| Dispatch | Result |
|---|---|
| Published release, nothing configured | The `preflight` job emits two `::warning::` annotations, including *"No signature was produced — the release is unsigned"*, sets `mode=skip`, and the `sign` job is skipped. The run is **green** and says twice that it signed nothing. |
| Manual dispatch with `require_signing: true`, nothing configured | The run **fails**, naming each missing value and linking this page. |
| Manual dispatch with `require_signing: true`, configured | Signs, and a failure inside SignPath fails the run. |

So the failure mode is a visible one. There is no step that reports success
without having signed, which is the specific thing to avoid: a release pipeline
that looks signed and is not teaches everyone to ignore the signature check.

Once the configuration exists, set `require_signing: true` on the dispatch that
matters and the run stops being able to pass unsigned at all.

## Verifying a signature

### RPM (Linux)

RPM signatures are GPG, so the project's public key is what a user needs. It is
published in the release's notes when signing is live.

```sh
rpm --import loams-signing-key.asc
rpm --verbose --checksig loams-0.0.1-1.x86_64.rpm
```

`rpm --checksig` reports `digests signatures OK` for a good signature and names
the key ID for the publisher otherwise.

### A bare ELF binary

Only if a policy that can carry a detached signature exists. As of 2026-10-03
the OSS edition cannot, so this section is here for the day a paid Semantic
policy does:

```sh
gpg --import loams-signing-key.asc
gpg --verify loams.asc loams
```

## The handoff

Four items. Each is one value, and each names where it comes from. **None of them
can be created from this repository, and the available automation token cannot
create any of them** — see [why](#why-this-is-a-human-step) below.

1. **A SignPath account, an organisation and a project for `ostrium-labs`.**
   The Foundation's application form is at <https://signpath.org/apply>. It is
   reviewed by a human, and it is the gate for everything else.
2. **The SignPath GitHub App installed on the `ostrium-labs` organisation**, with
   read access to this repository's Actions. Install it from
   <https://github.com/apps/signpath>. SignPath's documentation lists this as a
   prerequisite for the GitHub connector. **Organisation owner.**
3. **The signing policy**, carrying a GPG certificate (an `.rpm` signature is a
   GPG signature, so a GPG key, not an Authenticode certificate) and the
   pipeline policies from [`signpath/pipeline-policy.md`](../../signpath/pipeline-policy.md).
   The artifact configuration is
   [`signpath/artifact-configuration.rpm.xml`](../../signpath/artifact-configuration.rpm.xml).
4. **The API token and the four configuration values**, then:

   | Kind | Name | Value |
   |---|---|---|
   | secret (org or repo) | `SIGNPATH_API_TOKEN` | A SignPath API token for a user with **submitter** permission on the project and policy. Generated in SignPath under the user's API tokens. |
   | variable (org or repo) | `SIGNPATH_ORGANIZATION_ID` | The SignPath organisation's id, from the signing policy's details page (it appears in the copyable PowerShell snippet). |
   | variable (org or repo) | `SIGNPATH_PROJECT_SLUG` | The project's slug, e.g. `loams`. |
   | variable (org or repo) | `SIGNPATH_SIGNING_POLICY_SLUG` | The signing policy's slug, e.g. `release-signing`. |
   | variable (org or repo) | `SIGNPATH_ARTIFACT_CONFIGURATION_SLUG` | Optional. Unset means the project's default artifact configuration. |

   Repository scope is enough for all five, which matters because it removes the
   organisation-owner requirement from the last item:

   ```sh
   gh secret set SIGNPATH_API_TOKEN --repo ostrium-labs/loams
   gh variable set SIGNPATH_ORGANIZATION_ID --repo ostrium-labs/loams --body '<uuid>'
   gh variable set SIGNPATH_PROJECT_SLUG --repo ostrium-labs/loams --body 'loams'
   gh variable set SIGNPATH_SIGNING_POLICY_SLUG --repo ostrium-labs/loams --body 'release-signing'
   gh variable set SIGNPATH_ARTIFACT_CONFIGURATION_SLUG --repo ostrium-labs/loams --body 'rpm'
   ```

### Then, and only then

Run the workflow by hand with `require_signing: true` against a published
release. Expect it to fail until item 4 is done, then pass. Do not turn the
`require_signing` default on until that run is green.

### The three roles the Foundation requires

[signpath.org/terms](https://signpath.org/terms) requires the project to define
three roles, and to name the people in them on the project's home page under the
heading **Code signing policy**, including the literal sentence *"Free code
signing provided by [SignPath.io](https://about.signpath.io), certificate by
[SignPath Foundation](https://signpath.org)"*. Until the roles are filled, this
page cannot satisfy that condition and the application should not be expected to
be approved.

| Role | What it does | Suggested GitHub team |
|---|---|---|
| **Authors** | May push to the repository without further review. | Maintainers with write access. |
| **Reviewers** | Must review every change proposed by someone who is not a committer. | Maintainers with write access. |
| **Approvers** | Approve each signing request, after deciding the release should be signed. | Repository owners, or a `release-signers` team. |

The Foundation also requires multi-factor authentication on both SignPath and
the source host for everyone in these roles.

## Why this is a human step

Re-attempted on 2026-10-03, and confirmed rather than assumed, with the
automation token this work had (`gho_`, scopes
`admin:public_key, gist, read:org, repo, workflow`):

```console
$ gh secret set SIGNPATH_PROBE --org ostrium-labs --body probe
failed to fetch public key: HTTP 403: You must be an org admin or have the
actions secrets fine-grained permission.
(https://api.github.com/orgs/ostrium-labs/actions/secrets/public-key)
This API operation needs the "admin:org" scope. To request it, run:
  gh auth refresh -h github.com -s admin:org

$ gh secret list --org ostrium-labs
failed to get secrets: HTTP 403: You must be an org admin or have the actions
secrets fine-grained permission.
(https://api.github.com/orgs/ostrium-labs/actions/secrets?per_page=100)

$ gh variable set SIGNPATH_PROBE --org ostrium-labs --body probe
failed to set variable "SIGNPATH_PROBE": HTTP 403: You must be an org admin or
have the actions variables fine-grained permission.
(https://api.github.com/orgs/ostrium-labs/actions/variables)
```

The precise shape of the blocker, because it decides who has to do what:

- **The account is already an organisation owner.**
  `gh api orgs/ostrium-labs/memberships/dina-kar` returns
  `{"role":"admin","state":"active"}`. The 403 is *not* a permissions problem
  within the organisation.
- **It is purely a token-scope problem.** The token carries `read:org` but not
  `admin:org`, and organisation secrets, organisation variables and
  organisation *rulesets* all need the latter.

So a single `gh auth refresh -h github.com -s admin:org` unblocks items 2, 3 and
part of 4 **for a maintainer who can grant themselves the scope**. It does not
unblock item 1, and it does not unblock installing a GitHub App by any API:
GitHub has no endpoint by which an app installs itself, only an install URL an
organisation owner follows in a browser.

What *was* established, and it shortens the handoff:

- **The `ostrium-labs` organisation has no Actions secrets and no Actions
  variables** (both lists refuse, and nothing is half-configured to conflict
  with).
- **`ostrium-labs/loams` has no Actions secrets, no Actions variables and zero
  GitHub environments.** `gh api repos/ostrium-labs/loams/environments` reports
  `total_count: 0`, which is also why
  [`release-crates.yml`](../../.github/workflows/release-crates.yml) cannot
  publish ([Q619](#decision-log-rows)).

So the irreducible human step is items 1–3: the Foundation application, the
GitHub App installation, and the signing policy with its GPG certificate. Item 4
is five `gh` commands that any token with repository write access can run.

Two related gaps worth knowing about, both verified 2026-10-03 and neither in
this change's scope:

- **There is no RPM packaging in this repository**, and no binary release
  pipeline at all (D292's cargo-dist pipeline is still planned). The Foundation
  also requires that a project *"must already be released in the form that
  should be signed"*, so this is the binding constraint: the Linux signing path
  has nothing to sign yet. It signs an `.rpm` once one exists, and today it warns
  and stops. Building the RPM is the next piece of work, and it belongs to the
  release pipeline rather than to this page ([Q618](#decision-log-rows)).

## Decision-log rows

In [`docs/design/13-decision-log.md`](../design/13-decision-log.md):

| Row | What it records |
|---|---|
| **D620** | The platform split: Linux signed, Windows and macOS unsigned. |
| **D621** | Linux signing is RPM-only under the free programme. |
| **D622** | The pipeline warns or fails; it never reports success without signing. |
| **D623** | The platform scope is enforced by a script, and the `sign` job stays on GitHub-hosted runners. |
| **D624** | Build-from-source documentation replaces #264's Apple-ID signing path. |

**Q615** (confirm Windows stays unsigned), **Q616** (confirm RPM-only for Linux),
**Q617** (who fills the Foundation's three signing roles), **Q618** (build the RPM)
and **Q619** (create the `crates-io` environment) are the open items that follow.

## Verifying this page

`python3 scripts/ci/signpath-artifacts.py --self-test` is what keeps the platform
scope honest. `.github/workflows/ci.yml`'s `signing` job runs it, and
`.github/workflows/release-sign.yml` runs it again before it signs. A rule in
this document that disagrees with the script is a bug in one of the two, and the
script is what decides.