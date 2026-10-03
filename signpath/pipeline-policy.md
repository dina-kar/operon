# Loams SignPath pipeline policies

The signing policy SignPath evaluates for every request from
[`ostrium-labs/loams`](https://github.com/ostrium-labs/loams). Paste this into
the policy's **Pipeline Policies** tab after the project exists, then read
`docs/release/signing.md`, which is the setup as a whole.

Verified 2026-10-03 against <https://docs.signpath.io/trusted-build-systems/github>
for the policy keys and against <https://signpath.org/terms> for what the
Foundation requires of the project.

## `github-build-policies`

GitHub-hosted runners are not a preference here. The GitHub connector runs this
check for **every** request, before any policy is consulted:

> For OSS projects: All jobs of the GitHub workflow leading up to the signing
> request were executed on GitHub-hosted agents

`.github/workflows/release-sign.yml` is on `ubuntu-latest` for that reason, and
it is deliberately **not** on `vars.RUNNER_MAIN`, which is the self-hosted pool
most of this repository's other workflows use. Setting the policy below makes
that a SignPath-side invariant as well, so a future edit that moves the job
fails policy rather than failing confusingly.

```yaml
github-build-policies:
  version: '1.0'
  # SignPath allows policy evaluation for at most 3 re-runs of a build, so a
  # rerun past the third fails policy. Left false because a release rerun after
  # a transient signing failure is normal and must not need a policy waiver.
  disallow_reruns: false
  runners:
    required_github_hosted: true
```

> **One caveat, because it will cost an hour otherwise.** SignPath's own
> documentation is inconsistent about this one key. The prose table in the
> `runners` section spells it `required_github_hosted`; the worked example
> earlier on the same page spells it `require_github_hosted` (no `d`). The key
> above follows the prose table, which is the one that documents what the value
> *means*. **Unverified:** which spelling the API actually accepts. If SignPath
> rejects the pasted policy, try the other spelling — this is the first thing to
> change, and nothing else in this file depends on it.

## `github-scm-policies`

SignPath evaluates these against GitHub's own **branch rulesets**, and every rule
in a constraint must be covered by a rule in at least one active ruleset. So the
constraints below are only satisfied once the matching rulesets exist in
`ostrium-labs`. As of 2026-10-03 this repository has no rulesets, and creating
them is an organisation-level action (see `docs/release/signing.md`).

```yaml
github-scm-policies:
  version: '1.0'
  ruleset_constraints:
    - # Evaluated at signing time only. Setting `enforced_from` to a date or
      # `EARLIEST` would make the constraints have to have held continuously
      # since then, and this repository has had no rulesets at all until now, so
      # that would be unsatisfiable on the day it is created.
      enforced_from: CURRENT_BUILD
      # true is the *weaker* setting: it permits the GitHub ruleset to list
      # bypass actors. Set this to false only once the ruleset itself has no
      # bypass actors, because a bypass actor who can force-push defeats the
      # `non_fast_forward` constraint.
      allow_bypass_actors: true
      rules:
        - type: non_fast_forward
        - type: pull_request
          parameters:
            required_approving_review_count: 1
```

| Constraint | The GitHub ruleset rule that satisfies it | Why |
|---|---|---|
| `non_fast_forward` | Ruleset: *restrict updates*, denying force pushes | A signature attests that the binary came from this source tree. If the tree can be rewritten, the attestation is worthless. |
| `pull_request` with `required_approving_review_count: 1` | Ruleset: *require a pull request* with at least one approving review | SignPath Foundation counts **reviewers** as one of the three roles it requires the project to define (authors, reviewers, approvers). |
| `required_linear_history` is **not** set | — | The repository merges with merge commits, never squash. Setting it would make the constraints unsatisfiable. |
| `required_signatures` is **not** set | — | The repository signs off with DCO (`git commit -s`) rather than signing commits with GPG. |

The same three roles come up in the Foundation's terms, so `docs/release/signing.md`
names the people and the GitHub teams that fill them.

## What this policy does not decide

Two things, and both are the signing policy's own settings rather than anything
this file can carry:

- **The certificate.** It comes from SignPath Foundation. For an RPM it must be
  a **GPG key** certificate, because an RPM signature *is* a GPG signature — not
  an Authenticode certificate.
- **The file metadata restrictions**, which the Foundation requires: "All signed
  binaries must have metadata attributes set and enforced … set all *product
  name* attributes to your project's name, *product version* attributes to the
  same value in each build." SignPath's
  [metadata-restrictions reference](https://docs.signpath.io/artifact-configuration/reference#file-metadata-restrictions)
  lists restriction attributes for `<pe-file>`, `<msi-file>` and `<xml-file>`
  only, so there is **no** metadata-restriction attribute to set on an
  `<rpm-file>`. For RPM the equivalent fields are the package's own `Name`,
  `Version`, `Release` and `Summary`, and they have to be right in the packaging
  itself. That is part of the RPM packaging work, not part of this policy.