# Governance

## Current model

Operon is maintainer-led during its early phase. Maintainers are listed in [MAINTAINERS.md](MAINTAINERS.md).

- **Decisions:** made by lazy consensus on issues and pull requests. Changes to formats, protocols, licensing or the [decision log](docs/design/13-decision-log.md) need approval from at least one maintainer, and a 72-hour comment window for design-level changes.
- **Disagreements:** resolved by maintainer vote (simple majority). The project lead breaks ties while there are fewer than three maintainers.
- **Becoming a maintainer:** sustained, high-quality contributions and reviews; nominated by an existing maintainer and approved by a majority of maintainers.

## Principles

1. **Everything is open.** The engine, all protocol gateways, the reliability features (such as the `quorum` and `express` WAL classes), the performance features (such as the hot tiers) and the Kubernetes operator are Apache-2.0. They won't be moved behind a commercial license.
2. **Open formats.** Data at rest stays readable without Operon.
3. **Vendor neutrality.** No single company should control the roadmap long term.

## Path to a foundation

Once the project has at least three organizations contributing regularly, the maintainers intend to propose it to a neutral foundation (for example LF AI & Data or the CNCF) and adopt that foundation's governance.
