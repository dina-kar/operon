# The SDK runtime contract

Design §44 §7.4 states the behaviour in a paragraph; this page states it as
clauses a conformance suite can check, which is what makes the thirteen SDKs
comparable. SDK1 Task 2 owns the whole document. This is the TypeScript slice:
the clauses SDK2 Task 0 implemented and tests, with the rest marked as what is
still owed.

The conformance corpus is `sdks/fixtures` (design §44 §10.4), and each SDK's
suite replays it through its own public surface. A clause below is only true of
an SDK once its suite pins it.

## R1 — Credentials

A client's bearer comes from a token source and travels in
`Authorization: Bearer`, never in the query string and never in a URL. A
`401` carrying `ErrorInfo.reason = token_expired` triggers **exactly one**
refresh and **one** retry; a second expiry is reported. A source that cannot
refresh — an API key, which does not expire — makes the refresh a no-op.

*Pinned by* `typescript_token_source_refresh`. The token exchange
(`oidcExchange`) is written to RFC 8693 and is **not** exercised: the instance
serves no OAuth endpoint yet (MT, API1 Task 7).

## R2 — Retry classes and backoff

A call's retry class comes from the generated bindings, not from a guess: reads
(`NO_SIDE_EFFECTS`) and idempotent RPCs retry on their own; a mutation does not,
unless it carries an idempotency key. A retryable code is `unavailable`,
`deadline_exceeded` or `resource_exhausted`. The numbers are M1.6 Ruling 5's and
are the same in every SDK: base 100 ms, doubling, capped at 2 s, 3 retries,
**full** jitter. A server-sent `RetryInfo.retry_delay` replaces the computed
backoff, up to 30 s.

*Not yet implemented anywhere:* reading `RetryInfo`. No proto carries it, so
the jittered backoff is all an SDK can do today.

*Pinned by* `typescript_retry_reuses_idempotency_key`.

## R3 — Idempotency keys

A mutating call that carries an `idempotency_key` field is given one **per
logical call**, before the first attempt, and **the same key goes out on every
retry**. A key regenerated per attempt turns one write into two, which is the
failure the key exists to prevent. An SDK mints a UUIDv7 unless the caller
supplies a key. A request with no such field is left alone.

*Pinned by* `typescript_retry_reuses_idempotency_key`.

## R4 — Consistency tokens

A write answers with a `consistency_token`; a read accepts one, so a caller that
just wrote can read its own write. Threading those by hand is the caller's job
today. A session store is the alternative: **off by default**, and when a call
opts in, every response's token is folded into the session and attached to later
reads.

**The token's encoding is not in the protos yet.** §05 §5 defines the semantics
and API1's write paths carry an opaque `v1:` string; §44 §7.4 says it merges by
"max offset per stream and partition", which needs the encoding parsed. Until
that lands, a store keeps the token it was given and reports two different
tokens meeting as an error rather than merging them into a wrong one — a
silently-wrong consistency token reads stale data, which is worse than a
failure.

*Not pinned by a test yet,* because no RPC carries a token.

## R5 — Unavailable services

Design §44 §4, D600. Two halves, and an SDK needs both.

1. **Without calling.** `GetInstance.services[]` says which packages this binary
   carries. One call, no auth, cheap. `available()`, `served()`,
   `unavailable()` and `guard()` wrap it; the catalogue is cached for the life
   of the process and concurrent readers share one in-flight fetch.
2. **When the caller calls anyway.** Every RPC of an absent package answers
   `unimplemented` with `reason = feature_not_in_variant` and the variant in
   `metadata.variant`. An SDK turns that into a dedicated error type, so the
   branch is `instanceof FeatureNotInVariantError` or
   `err.reason === 'feature_not_in_variant'` — never the package name, which is
   a proto detail, and never the message.

`guard()` raises the *same* error type, so one `catch` covers "the guard said
no" and "the server refused", and the guard costs no RPC once the catalogue is
cached.

*Pinned by* `typescript_conformance_all_required_fixtures`, in all three shapes:
the guard, the unary refusal, and the refusal on a stream.

## R6 — Pagination

A paged call is `page_size` in, `next_page_token` out, and the facade exposes
both the raw page call and an iterator that follows the tokens to the end and
yields **items**, not pages. One function serves every paged RPC, because the
generated binding names the two fields; a per-call alias (`listAll`) appears
when there is a generated signature to hang it on.

*Pinned by* `typescript_pagination_iterator` against a stub, because **no RPC
is paged yet** — `ListCollections` arrives with API1 Task 2. The end-to-end half
of that test is a deliberate skip, not an omission: a fixture for an RPC the
server does not serve would test the stub rather than the SDK.

## R7 — Streams

A server stream is an `AsyncIterable`. There is no client streaming and no bidi
(D420): a browser cannot do it over `fetch`, and half-duplex works through
every proxy.

A stream is the one call where "retry it" is not enough. The server hands out
cursors; a reconnect resumes from the last one the client applied, or the client
silently misses everything that changed in between — worse than an error, because
a sync UI that is quietly stale looks like one that works. So an SDK tracks the
cursor of every message, re-opens from it on a retryable failure, and does not
re-yield what it already yielded. A failure the retry class does not cover —
notably an `unimplemented` stream — is reported rather than spun on.

*Pinned by* `typescript_stream_resume_with_cursor` against a stub, since the only
server stream (`LiveService/Watch`) is never served in any variant. The
end-to-end half is covered once a watchable service is served.

## R8 — Errors

Design §44 §7.4, D611. A failed RPC carries a Connect code and one
`loams.errors.v1.ErrorInfo`. The **code** gives the class (a taxonomy that
does not change within a major version) and the **`reason`** is the stable
branch. `reason` is a type in the SDK, generated from the registry page, so a
caller switching on it is exhaustive and a reason the registry has lost stops
compiling. `metadata` is structured context; `hint` is a next step in the
caller's locale.

Three cases are distinct and must not be conflated:

- a reason from a **newer** server, which this SDK's registry does not have: it
  is surfaced as text and flagged, not dropped, because losing it would leave a
  caller unable to tell "not supported here" from "not supported at all";
- a failure from **below the API** — a socket, a CORS rejection, an abort —
  which carries no `reason` at all;
- a `LoamsError`, which is a mapped one and is returned unchanged if mapped
  twice.

*Pinned by* `typescript_error_reason_mapping`, over all twenty-five reasons in
the registry.

## R9 — Version reporting

An SDK declares the proto revision it was generated from (`PROTO_REV`), and
reports the server's `GetInstance.api_versions` beside it. A package the SDK
speaks that the server does not serve is a **warning, not an exception**: the
SDK still works for the modules that are there, and the caller decides what a
missing one means.

*Pinned by* `typescript_conformance_all_required_fixtures`.

## R10 — The browser is a first-class target

One port serves the Connect protocol, gRPC and gRPC-Web (D600), and a browser can
only do gRPC-Web. So the same client object runs in a browser with no proxy and
no sidecar. An SDK's default entry point must therefore reach for no Node
built-in, and an HTTP/2 transport must live behind a subpath a bundler will not
follow.

*Pinned by* `typescript_grpc_web_transport_in_a_browser*, which runs the same
assertions over the Connect and gRPC-Web transports and checks the entry points
statically. **What it cannot pin** is a CORS preflight or a browser-specific
header rule: that needs an origin and a server that answers preflights, and no
test in this repository has one.
