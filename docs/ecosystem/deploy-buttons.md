# "Deploy to <provider>" buttons

A one-click deploy button lets a user run Loams on a cloud provider's infrastructure from a link. This page is the convention every provider template follows, so buttons look and behave the same.

Status: **draft convention**, open to change through an RFC issue. Examples of providers are illustrative, not endorsements.

## Layout

```
deploy/<provider>/
  README.md        # what it deploys, prerequisites, the button, limits
  loams.<ext>      # the provider's template (Helm values, Terraform, wrangler config, ...)
  variables.md     # every input the user is asked for, with defaults
  CHANGELOG.md     # template changes, tied to the Loams versions it supports
```

`<provider>` is lowercase, for example `cloudflare`, `aws`, `gcp`, `hetzner`. Shared pieces (the Helm umbrella chart, the operator) stay where they are and are referenced, not copied.

## Rules for a template

1. **BYOC.** It deploys into the user's own account. The user owns the bucket, the data and the credentials.
2. **Pinned.** It pins a released Loams version and the digests of its images. `latest` is not allowed.
3. **Least privilege.** It lists every permission it requests in `README.md` and asks for no more.
4. **No secrets in the repo.** Inputs that are secrets are prompted for or generated; none is committed.
5. **No phone-home.** It adds no telemetry or billing hooks. Usage hooks stay opt-in ([§27](../design/27-usage-hooks.md)).
6. **Open formats.** Data stays in the user's bucket in Loams's open formats.
7. **Tested.** CI (or the provider's own check, linked from the README) deploys it on every release candidate.
8. **Owned.** `deploy/<provider>/` names at least one maintainer of the template in `CODEOWNERS`. Orphaned templates are marked deprecated.

## The button

Use the provider's own button image and URL scheme, wrapped like this in the template README:

```markdown
[![Deploy to <Provider>](https://<provider-button-image-url>)](https://<provider-deploy-url>?<repo-or-template-param>=https://github.com/ostrium-labs/loams/tree/main/deploy/<provider>)
```

Rules:

- The link target is a **tagged release path** in releases and `main` only in development docs.
- The alt text is exactly `Deploy to <Provider>`.
- Next to the button, state in one line what gets created and in which account ("Creates a Loams cluster in your <Provider> account using your bucket").
- Show a badge for the Loams version it deploys: `![Loams v0.x](https://img.shields.io/badge/loams-v0.x-blue)`.

## Adding a provider

1. Open a Discussion (Ecosystem & Partners), then an RFC issue if the target needs a new `Runner` or provider ([ECOSYSTEM.md](../../ECOSYSTEM.md)).
2. Send PRs to `dev`: the template, its docs and its CI.
3. Maintainers review it like any other change. A merged template is listed in the wiki's "Deploy buttons" page.
