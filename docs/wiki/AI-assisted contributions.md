# AI-assisted contributions

Codex and Claude agents work on Loams through issues labelled `codex-ready`. The rules are the same as for people:

- One plan task per PR, tests first, read the plan and design sections before coding.
- Local gates: fmt, clippy and tests on touched crates, `cargo deny` when dependencies change.
- CI green, every CodeRabbit comment fixed or answered, DCO sign-off on every commit.
- Hard or risky changes get the `needs-opus-review` label for a deeper review before merge.
- A human maintainer owns the outcome; agents don't approve their own work.

See [CONTRIBUTING.md](https://github.com/ostrium-labs/loams/blob/main/CONTRIBUTING.md) and the open `codex-ready` issues.
