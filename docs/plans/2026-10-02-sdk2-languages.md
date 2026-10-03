# SDK2 — Per-Language SDKs: Thirteen Languages, One Task Each Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Execute task by task, test first. Each task lists the interfaces it must produce and the tests that must exist and pass before it is done. Where this plan gives exact values (names, paths, versions), use them verbatim. The code is not pre-written in this plan; the tests are the specification.

> **Status: In progress** (2026-10-03; Task 0 delivered, #284). **Slot: track SDK, third plan** (proposed; D606, D612–D615, D619). Branches `sdk2-<lang>`, one per language; PRs target `dev`. Depends on SDK1 (Tasks 1–4 at least) and on the API1 services each fixture touches. Every task below also has its own issue.

**Goal:** A published, tested client for every language gRPC officially supports (design [§44](../design/44-unified-api-and-sdks.md) §9): the same `Loams` object and modules, the same runtime contract (SDK1 Task 2), passing 100% of the required fixtures, with a README, an example and a reference page.

**Architecture:** Per language the work is the same shape:
1. `sdks/<lang>/` with the pinned `buf.gen.yaml` (SDK1), generated stubs, the facade output of `protoc-gen-loams-facade`, and the hand-written **runtime** (transport, `TokenSource`s, retry, errors, token store, pagination, streams).
2. Typed builder for the hybrid query IR where §44 §7.6 says so.
3. Conformance tests over the public facade against `sdks/conformance/run.sh`.
4. Release wiring (SDK1 Task 6): registry package, trusted publishing where the registry has it, mirror repo where required.
5. README quickstart, example app, snippets from the fixture corpus.

**Tech Stack:** per the language table below; versions are recorded in `docs/sdk/toolchain-2026-10.md` (SDK1 Task 0) and pinned.

**Spec:** [§44](../design/44-unified-api-and-sdks.md) §7, §9–§11; SDK1 plan; issue #254.

## Global Constraints

- **The same behaviour in every language** (D617): a language cannot skip a required fixture; `transport:grpc-only` fixtures skip only on the Connect-unary fallback.
- **Idiomatic naming and streaming** per §44 §7.1; the proto names are the source.
- **No secrets in examples**; examples read `LOAMS_ENDPOINT` and `LOAMS_API_KEY`.
- **Do not publish** until the owner confirmed the registry account (§44 §13). Until then the release job is a dry run.
- **Licences:** Apache-2.0 package metadata; dependencies checked against the repo's policy (no AGPL).
- **A Connect library fails Task 0's check** (unmaintained 12 months, or fails the Connect conformance suite): the language switches to gRPC and the row in §44 §9 is amended in the same PR.

## Language matrix

| Wave | Language | Protocol and library | Package | Registry |
|---|---|---|---|---|
| 1 | TypeScript / JavaScript | Connect (`@connectrpc/connect` 2.x, `connect-es`) | `@loams/client`, `@loams/proto`, `@loams/live` (re-export) | npm (trusted publishing, provenance) |
| 1 | Python | Connect (`connect-python`, beta) | `loams` (extras `flight`, `arrow`, `polars`) | PyPI (trusted publishers) |
| 1 | Go | Connect (`connect-go`) | module `loams.dev/go` (mirror `ostrium-labs/loams-go`) | Go module proxy (git tag on the mirror) |
| 1 | Rust | Connect (`connectrpc` / connect-rust, buffa) | crate `loams` | crates.io (trusted publishing) |
| 2 | Swift | Connect (`connect-swift`, stable) | SwiftPM package `Loams` (mirror `ostrium-labs/loams-swift`) | SwiftPM (git tag) |
| 2 | Kotlin (JVM, Android) | Connect (`connect-kotlin`, beta) | `dev.loams:loams-kotlin` | Maven Central (Portal token + GPG) |
| 2 | Java | gRPC (`grpc-java`, `protobuf-java`) | `dev.loams:loams` | Maven Central (Portal token + GPG) |
| 2 | C# / .NET | gRPC (`Grpc.Net.Client`, `Google.Protobuf`) | `Loams` (reserve `Loams.*`) | NuGet (trusted publishing) |
| 3 | Dart / Flutter | Connect if `connect-dart` passes Task 0, else gRPC (`grpc` package) | `loams` | pub.dev (automated publishing) |
| 3 | Ruby | gRPC (`grpc`, `google-protobuf`) plus Connect-unary transport (`Net::HTTP`) | gem `loams` | RubyGems (trusted publishing) |
| 3 | PHP | gRPC (`grpc/grpc` + `google/protobuf`) plus Connect-unary transport (PSR-18) | `loams/loams` | Packagist (webhook; mirror `ostrium-labs/loams-php`) |
| 3 | C++ | gRPC (`grpc++`, protobuf) | `loams` (vcpkg overlay, Conan remote, source tarball) | vcpkg / Conan (own registries first, Q610) |
| 3 | Objective-C | gRPC (`gRPC-ProtoRPC`, gRPC-ObjC) | `Loams` generated sources + SwiftPM target | Source and SwiftPM only (CocoaPods trunk read-only 2026-12-02) |

## Tasks (one PR or a short stack per language; each ticks in its own issue)

Every language task has the same sub-steps: (a) Task 0 check of library and plugin versions; (b) generated stubs and facade; (c) runtime (R1–R9 of the contract); (d) builder where required; (e) conformance suite green; (f) README, example, snippets; (g) release dry run, then the real publish after the owner action.

- [x] **Task 0: TypeScript / JavaScript SDK (wave 1).** Node >= 22, browsers, Deno, Bun; ESM with types; fold `sdks/live-typescript` in; typed hybrid-query builder; no Flight (chunked writes, optional `QueryArrow`); `loams.live.watch` as `AsyncIterable`; the console (AP1a) consumes `@loams/proto`. *Tests:* `typescript_conformance_all_required_fixtures`, `typescript_retry_reuses_idempotency_key`, `typescript_error_reason_mapping`, `typescript_stream_resume_with_cursor`, `typescript_token_source_refresh`, `typescript_pagination_iterator`.

  **Delivered** (#284, branch `sdk2-ts`, 2026-10-03). `@loams/client` in `sdks/typescript/packages/client`, `@loams/live` folded in from `sdks/live-typescript`. All six tests exist and pass, against both the recorded corpus and a live `loams dev`.

  What landed **short of the task's wording**, and why:
  - *The typed hybrid-query builder is not there.* No hybrid query RPC exists: `loams.collection.v1` and `loams.query.v1` arrive with API1 Tasks 2 and 4, and Q604 (whether to generate one builder or thirteen) is not answered. A builder written now would be a hand-written API with no proto behind it, which is exactly what §7.3 says not to do.
  - *The per-call `listAll` alias is not there.* The pagination **iterator** is, and is tested; a generated alias needs a generated signature to hang on, which arrives with `ListCollections` (API1 Task 2). `loams.paginate(module, call, request)` is the same function under its binding names.
  - *`bulk` / `QueryArrow` are not there*, for the same reason: the write RPCs land with the write paths (API1 Tasks 3 and 4), and the API has no bidi (D420), so there is nothing to stream a request over.
  - *No release dry run.* Publishing is D400's decision and needs the thirteen-language cadence; `docs/release/publishing.md` records the npm row honestly.

  **Taken from SDK1's scope** (SDK1 Task 2, SDK1 Task 3 and part of SDK1 Task 4), because TypeScript could not be generated without them:
  - SDK1 Task 1, TypeScript slice: `sdks/typescript/buf.gen.yaml` and `buf.gen.live.yaml` (the root `buf.gen.yaml` moved), `scripts/sdk/gen.sh typescript` and `typescript-live`, with buf 1.73.0 and protoc-gen-es 2.16.0 pinned as devDependencies of `@loams/client`. The other languages' templates, `sdks/templates/`, the drift runner and the pin-checking script are **not** done.
  - SDK1 Task 2, TypeScript slice: `docs/sdk/runtime-contract.md` clauses R1–R10, each naming the test that pins it and what it does not pin.
  - SDK1 Task 3, TypeScript slice: `crates/loams-facade-gen` (`protoc-gen-loams-facade`), the descriptor reader, the `Reason` registry parser and the TypeScript renderer, with `golden_typescript` and the model tests. Python and Go templates are **not** done, and Q604 is not answered.
  - SDK1 Task 4, minimum: `sdks/fixtures/` (13 cases recorded from a real `loams dev`, 3 RPCs × 4 encodings plus the streaming refusal), `sdks/conformance/record-fixtures.mjs`, `fixture-server.mjs` and `run.sh`. The `status`/`error`/`state` fixture files, the `loams-apps-mock` harness and the per-test runner are **not** done.
  - A new reason, `token_expired`, in `docs/api/reasons.md`, because R1's refresh-on-`token_expired` cannot be written without it. Added, not renamed: the registry's rule is that a reason may be added but never renamed or removed within a major version.
- [ ] **Task 1: Python SDK (wave 1).** Python >= 3.10, sync and async clients, typed builder, `bulk` through `adbc-driver-flightsql` (extra), `to_arrow()`/`to_polars()` from M1.6 Tasks reused where they exist, `mypy --strict`; replaces `loams-client`. *Tests:* `python_conformance_all_required_fixtures`, `python_retry_reuses_idempotency_key`, `python_error_reason_mapping`, `python_stream_resume_with_cursor`, `python_token_source_refresh`, `python_pagination_iterator`.
- [ ] **Task 2: Go SDK (wave 1).** Go >= 1.24, context-first API, `*Stream` helpers, builder, `bulk` over Arrow Flight (`arrow-go`), `go-import` meta for `loams.dev/go` (Q607). *Tests:* `go_conformance_all_required_fixtures`, `go_retry_reuses_idempotency_key`, `go_error_reason_mapping`, `go_stream_resume_with_cursor`, `go_token_source_refresh`, `go_pagination_iterator`.
- [ ] **Task 3: Rust SDK (wave 1).** Same stack as the server (D128); async `Stream` for server streams, builder, `bulk` over `arrow-flight`; `tonic` is not used. Split `loams-proto` if the generated crate is large. *Tests:* `rust_conformance_all_required_fixtures`, `rust_retry_reuses_idempotency_key`, `rust_error_reason_mapping`, `rust_stream_resume_with_cursor`, `rust_token_source_refresh`, `rust_pagination_iterator`.
- [ ] **Task 4: Swift SDK (wave 2).** Swift 6, `AsyncSequence` streams, `GenerateAsyncMethods`, builder; shares generation with AP3 (`loams-mobile` pins the same proto ref); iOS 15+/macOS 12+/Linux. *Tests:* `swift_conformance_all_required_fixtures`, `swift_retry_reuses_idempotency_key`, `swift_error_reason_mapping`, `swift_stream_resume_with_cursor`, `swift_token_source_refresh`, `swift_pagination_iterator`.
- [ ] **Task 5: Kotlin (JVM, Android) SDK (wave 2).** Coroutines and `Flow`, OkHttp engine, Android and JVM targets, builder DSL; shares generation with AP2; namespace `dev.loams` verified by DNS TXT. *Tests:* `kotlin_conformance_all_required_fixtures`, `kotlin_retry_reuses_idempotency_key`, `kotlin_error_reason_mapping`, `kotlin_stream_resume_with_cursor`, `kotlin_token_source_refresh`, `kotlin_pagination_iterator`.
- [ ] **Task 6: Java SDK (wave 2).** Java 17+, blocking and async stubs, `Iterator` for server streams, builder, `bulk` over Arrow Flight (`flight-core`); supersedes "Java deferred". Repackage `grpc-netty-shaded` as an optional artifact. *Tests:* `java_conformance_all_required_fixtures`, `java_retry_reuses_idempotency_key`, `java_error_reason_mapping`, `java_stream_resume_with_cursor`, `java_token_source_refresh`, `java_pagination_iterator`.
- [ ] **Task 7: C# / .NET SDK (wave 2).** net8.0+ and netstandard2.1 where gRPC supports it, `IAsyncEnumerable` streams, DI extension (`AddLoams`), builder, `bulk` over Apache.Arrow.Flight; gRPC-Web for Blazor WASM. *Tests:* `csharp_conformance_all_required_fixtures`, `csharp_retry_reuses_idempotency_key`, `csharp_error_reason_mapping`, `csharp_stream_resume_with_cursor`, `csharp_token_source_refresh`, `csharp_pagination_iterator`.
- [ ] **Task 8: Dart / Flutter SDK (wave 3).** Dart 3, `Stream`, Flutter mobile/desktop/web; reconcile `connectrpc` and `connect_kit` on pub.dev with the connectrpc-org repo before choosing. *Tests:* `dart_conformance_all_required_fixtures`, `dart_retry_reuses_idempotency_key`, `dart_error_reason_mapping`, `dart_stream_resume_with_cursor`, `dart_token_source_refresh`, `dart_pagination_iterator`.
- [ ] **Task 9: Ruby SDK (wave 3).** Ruby >= 3.2, `Enumerator` streams, keyword arguments, both transports share the facade; precompiled gem platforms noted; fixtures marked `transport:grpc-only` skip on the unary transport. *Tests:* `ruby_conformance_all_required_fixtures`, `ruby_retry_reuses_idempotency_key`, `ruby_error_reason_mapping`, `ruby_stream_resume_with_cursor`, `ruby_token_source_refresh`, `ruby_pagination_iterator`.
- [ ] **Task 10: PHP SDK (wave 3).** PHP >= 8.2, `Generator` streams, PSR-18/17 for the fallback transport, Composer autoload, works without ext-grpc for unary calls; pecl notes. *Tests:* `php_conformance_all_required_fixtures`, `php_retry_reuses_idempotency_key`, `php_error_reason_mapping`, `php_stream_resume_with_cursor`, `php_token_source_refresh`, `php_pagination_iterator`.
- [ ] **Task 11: C++ SDK (wave 3).** C++17, CMake config package, sync and callback APIs, `bulk` over Arrow Flight (`arrow-flight`), builder; build matrix Linux/macOS/Windows. *Tests:* `cpp_conformance_all_required_fixtures`, `cpp_retry_reuses_idempotency_key`, `cpp_error_reason_mapping`, `cpp_stream_resume_with_cursor`, `cpp_token_source_refresh`, `cpp_pagination_iterator`.
- [ ] **Task 12: Objective-C SDK (wave 3).** Spike first (Q611): does gRPC-ObjC build under SwiftPM; if not, ship generated sources and docs only and mark the language community-supported. *Tests:* `objc_conformance_all_required_fixtures`, `objc_retry_reuses_idempotency_key`, `objc_error_reason_mapping`, `objc_stream_resume_with_cursor`, `objc_token_source_refresh`, `objc_pagination_iterator`.
- [ ] **Task 13: Cross-language docs.** The language matrix page, the install matrix, a "one call in 13 languages" page generated from the fixtures, status badges, and the community-SDK guide. *Tests:* `docs_snippets_compile` for the languages that can check a snippet without a server.
