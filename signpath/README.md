# `signpath/`

The two files SignPath needs from this repository, and where each one goes.
Neither is read by any build; they are inputs to a web form. The page that
explains the whole flow is [`docs/release/signing.md`](../docs/release/signing.md).

| File | SignPath screen | What it is |
|---|---|---|
| [`artifact-configuration.rpm.xml`](artifact-configuration.rpm.xml) | Project → Artifact Configurations | How to sign a Linux artifact: `<rpm-sign>`. |
| [`pipeline-policy.md`](pipeline-policy.md) | Signing policy → Pipeline Policies | How a build may run: GitHub-hosted runners only, no force pushes, a reviewed pull request. |

There is deliberately no artifact configuration for a Windows executable, a
macOS `.dmg`, an Android `.apk` or a detached GPG signature on a bare ELF binary.
The first two are unsigned by the owner's ruling of 2026-10-03; the third
outside this repository; and the fourth is not something SignPath's Open Source
Code Signing edition offers (verified 2026-10-03 — see the comment in
`artifact-configuration.rpm.xml`). A file that is absent because the platform is
out of scope is a fact. A file that is present but cannot succeed is a trap.