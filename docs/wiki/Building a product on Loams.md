# Building a product on Loams

Fork or embed, then keep the fork rebaseable: put product code in separate crates and packages, keep a short documented patch queue, and upstream generic changes.

**Upstream:** extension points, bug fixes, generic adapters, tests and docs. **Keep:** brand, UI, billing, prompts and hosted operations.

Extension points (plugins, providers, `Runner` adapters, deploy targets) and the worked examples are in [ECOSYSTEM.md](https://github.com/ostrium-labs/loams/blob/main/ECOSYSTEM.md). The open-core boundary is in [docs/open-core.md](https://github.com/ostrium-labs/loams/blob/main/docs/open-core.md).
