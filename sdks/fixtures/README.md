# SDK wire fixtures

The exact JSON the SDKs send to the native REST API, and what the server must
answer. Three test suites read these files, so the server and both SDKs stay
in step:

- `crates/operon/tests/it/sdk_wire.rs` runs every step against an in-process
  server (`cargo test -p operon --test it sdk_wire`);
- the Python SDK's `tests/test_fixtures.py` and the TypeScript SDK's
  `test/fixtures.test.ts` check that their public API produces exactly these
  requests (method, path, query string, token header and parsed body).

A server change that breaks a fixture fails `cargo test`; an SDK change that
sends something else fails that SDK's suite. The wire contract itself (the
W-table and the shapes) is in `docs/plans/2026-09-24-m1.6-sdks-mcp.md`.

## `scenario.json`

```text
{"version": 1, "steps": [Step]}
Step {"name": str,                 // unique
      "method": "GET"|"POST"|"DELETE",
      "path": str,                 // may carry a query string
      "headers": {str: str},
      "body": JSON | null,
      "status": [u16],             // accepted statuses
      "keys": [str],               // top-level keys the response object must have
      "header_token": bool,        // the response carries Operon-Consistency-Token, a v1 token
      "expect": [{"pointer": str, "equals"?: JSON, "one_of"?: [JSON], "absent"?: true}]}
```

The steps run in order against one server, and later steps depend on earlier
ones (step 11 reads what step 10 wrote). `pointer` is an RFC 6901 JSON pointer
into the response body. Numbers compare exactly: id 18446744073709551615 must
come back as that integer.

Placeholders:

- `{ns}` in `path` and in any string inside `body` is the test's namespace
  (`wire` in the server test);
- `{token:<step>}` in a header value is the `Operon-Consistency-Token` header
  that step `<step>`'s response carried.

One check is not expressed in the file: step `create_collection_again` must
answer the same `id` as step `create_collection` (an identical re-create is
201 with the same collection).

Request bodies follow the encoder rules of the plan: every key of the type,
with its default (`"distance": null` in vector params, `"consistency":
"strong"` in search requests, `"sparse_vectors": {}` in documents and
patches), except where the rules omit one (W11 and W13 carry `consistency`
only for eventual and pinned reads).

## `queries.json`

```text
{"version": 1, "queries": [{"name": str, "query": Query}]}
```

One entry per `Query` variant of the IR (named after it), plus
`match_fuzzy_auto` and `range_dates` (a date range, `{"date": "<RFC 3339>"}`
values). They run over a collection `qv` with fields `title` (text), `tag`
(keyword), `n` (i64), `ts` (date), `flag` (bool) and `meta` (json). The
server test posts each one as a search filter and as a text retriever and
expects 200; each SDK's builder must encode to exactly this JSON.
