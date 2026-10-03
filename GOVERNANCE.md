# Loams governance

Loams has two maintainers with equal roles: [Dinakaran V (@dina-kar)](https://github.com/dina-kar) and [Keshav (@Kesh3805)](https://github.com/Kesh3805). The current roster is in [MAINTAINERS.md](MAINTAINERS.md). They make project decisions by consensus and can add trusted people to the maintainer teams. Changes to formats, protocols, licensing or the [decision log](docs/design/13-decision-log.md) need an issue or design PR and a 72-hour comment window.

## Contribution ladder

- Anyone can open an issue or a pull request against `dev`. A first merged PR makes the contributor eligible for the `committers` team, and the [auto-promote workflow](.github/workflows/auto-promote.yml) adds them on the merge. If the repository has no organisation-members credential configured, the workflow says so on the PR instead, and a team maintainer adds them by hand. Adding someone is reversible: ask a maintainer and they will remove them from the team.
- `committers` may review and merge PRs into `dev` once required checks and review are complete. The team has write permission.
- `maintainers` set release direction and merge `dev` into `main`. The team has maintain permission. A PR to `main` requires one approval from this team.
- `main` is the release branch; `dev` is the default integration branch. Neither branch accepts force pushes or deletion. Use merge commits, not squash merges.

Both branches require PRs and passing CI and DCO checks. The branch rules, rather than team membership alone, enforce who can update them.

## Project proposals and ecosystem work

Start partner or ecosystem proposals in [Discussions](https://github.com/ostrium-labs/loams/discussions), then write an RFC issue before implementation PRs to `dev`. Contribute broadly useful changes upstream first.

Forks, embedded uses, hosted services and products built on Loams are welcome. See the [ecosystem policy](https://github.com/ostrium-labs/loams/blob/e443ed77a57d59eb1f0b589ff27671dd30b5ab2c/ECOSYSTEM.md) and [trademark guidance](https://github.com/ostrium-labs/loams/blob/e443ed77a57d59eb1f0b589ff27671dd30b5ab2c/TRADEMARKS.md) from [PR #262](https://github.com/ostrium-labs/loams/pull/262). “Built on Loams” is fine; do not name a separate product “Loams”.

## Principles and longer-term governance

The self-hosted engine, open control plane and APIs are Apache-2.0; commercial metering, billing and hosted services live in private repositories, as described in [open-core.md](docs/open-core.md). Data at rest stays readable without Loams. No single company should control the roadmap long term.

Once at least three organizations contribute regularly, the maintainers intend to propose Loams to a neutral foundation and adopt that foundation's governance.
