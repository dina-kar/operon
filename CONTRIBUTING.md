# Contributing to Operon

Thanks for your interest in Operon. The project is in its design and early implementation phase, which makes this the best time to shape it.

## Ways to contribute

- **Design review.** Read [`docs/design`](docs/design/README.md) and open an issue (label `design`) for anything unclear, wrong or missing.
- **Implementation.** Pick an open task from an implementation plan in [`docs/plans`](docs/plans/), or an issue labelled `good first issue`.
- **Compatibility testing.** Run your Kafka, Elasticsearch, Qdrant, Neo4j or ClickHouse client or framework against Operon and report gaps.

## Ground rules

1. **Discuss before large changes.** Anything that changes a format, a protocol, or a design decision in [`docs/design/13-decision-log.md`](docs/design/13-decision-log.md) needs an issue or design PR first.
2. **Tests first.** New behavior comes with tests. Storage, log and metadata code also needs deterministic-simulation or fault-injection coverage (see the testing strategy in [`12-roadmap-testing-risks.md`](docs/design/12-roadmap-testing-risks.md)).
3. **Dependency licenses.** Only Apache-2.0, MIT, BSD, ISC, Zlib, MPL-2.0 (unmodified, as a separate crate) or compatible licenses. **No AGPL, GPL, BSL, SSPL, ELv2 or proprietary code.** CI enforces this with `cargo-deny`.
4. **Code derived from other projects** (for example Quickwit, Qdrant or lance-graph) must keep its original copyright header, add an attribution line to [NOTICE](NOTICE), and say so in the PR description.
5. **Small, focused PRs** with a clear description of what changed and why.

## Developer Certificate of Origin (DCO)

Operon uses the [DCO](https://developercertificate.org/) instead of a CLA. Sign off every commit:

```bash
git commit -s -m "log: add WAL object encoder"
```

This adds a `Signed-off-by: Your Name <you@example.com>` trailer, certifying that you have the right to submit the contribution under the project license.

## Development setup

The Rust workspace is being bootstrapped (milestone M0). Once it lands:

```bash
rustup show            # installs the toolchain pinned in rust-toolchain.toml
cargo build
cargo test
cargo clippy --all-targets -- -D warnings
cargo fmt --check
```

## Commit messages

Use `<area>: <summary>` in the imperative mood, for example `meta: add sequencer snapshot`, `docs: clarify express WAL quorum`. Areas match crate names or `docs`/`ci`.

## Code of Conduct

Participation is governed by our [Code of Conduct](CODE_OF_CONDUCT.md).
