# @operon/client

The TypeScript client for Operon's native REST API: namespaces, streams,
collections, documents (dense and sparse vectors), hybrid search through a typed
query builder, SQL, scan plans, consistency tokens, typed errors and retries. ESM
with type declarations, zero runtime dependencies.

`@operon/client` is a working name and is not published yet (it is renamed with
the product after M1).

## Install

Until it is published, install it from a checkout:

```sh
pnpm add ../path/to/sdks/typescript
```

Node 22 or newer, or a browser. Browsers need the server to send CORS headers,
which M1 does not: serve the page and the API from one origin (a same-origin
proxy in front of Operon).

## Quickstart

Start a server with `operon dev` (native REST on `http://127.0.0.1:8080`), then:

```ts
import { OperonClient, q, s } from "@operon/client";

const client = new OperonClient({ baseUrl: "http://127.0.0.1:8080" });
await client.createNamespace("docs", { existOk: true });
const ns = client.namespace("docs");

await ns.createCollection("kb", {
  fields: [s.text("body"), s.keyword("tenant"), s.i64("n")],
  vectors: [s.vector("embedding", 3)],
});
const kb = ns.collection("kb");
const written = await kb.upsert([
  { id: 1, source: { body: "refund policy", tenant: "a", n: 1 }, vectors: { embedding: [1, 0, 0] } },
  { id: 2, source: { body: "shipping times", tenant: "a", n: 2 }, vectors: { embedding: [0.9, 0.1, 0] } },
]);

// Hybrid search: two retrievers fuse with RRF (k = 60) unless you call .fuse().
const response = await kb
  .search()
  .retrieve(
    q.vector("embedding", [1, 0, 0], { k: 50 }),
    q.text("refund", { k: 50, fields: ["body"] }),
  )
  .filter(q.term("tenant", "a"))
  .limit(10)
  .execute();
for (const hit of response.hits) console.log(hit.id, hit.score, hit.source);

console.log(await kb.get([1, 2], { consistency: written.token }));
const counts = await ns.sql("SELECT tenant, count(*) AS n FROM kb GROUP BY tenant");
console.log(counts.toObjects());
```

Every builder method returns a new builder, so a base query can be reused. `q`
has one constructor per query of the IR (`q.match`, `q.term`, `q.range`,
`q.bool`, …), the retrievers (`q.vector`, `q.text`, `q.sparse`, `q.fused`,
`q.rescore`), the fusions (`q.rrf`, `q.dbsf`, `q.weightedSum`) and the sort keys
(`q.scoreSort`, `q.fieldSort`, `q.pkSort`). `s` builds schema fields. Queries,
retrievers and sort keys are plain tagged objects (`{ term: { field, value } }`),
so they can also be written by hand. Sparse vectors go in `sparseVectors` as
`{ indices, values }`.

Vectors are arrays of numbers, `Float32Array`s or `Float64Array`s. A NaN or
infinity is refused before anything is sent, as is a malformed sparse vector
(unequal lengths, a repeated or out-of-range index). Dates in queries are `Date`s,
sent in UTC.

## Ids

A document id is a `number` (a non-negative safe integer), a `bigint` (a u64:
ids above 2^53 − 1 must be `bigint`), a `string`, or `{ uuid: "…" }`. Ids come
back the same way: a u64 above 2^53 − 1 is a `bigint`, never a rounded number.
A string that looks like a UUID stays a string id; use `{ uuid }` for a UUID id.
SQL results and hit `sortValues` keep integers above 2^53 − 1 exact as `bigint`
too.

## Consistency

Reads are strong by default. Pass `consistency` to `get`, `query` and `sql`, or
call `search().consistency(...)`:

- `"strong"` (the default): sees every acknowledged write;
- `"eventual"`: may lag, answers faster;
- a `ConsistencyToken` (or its text, `"v1:s7/p3@918274"`): sees at least the
  writes that token covers. Every write returns one (`result.token`); merge tokens
  with `a.merge(b)`;
- a `Pin` (a scan plan's `plan.pin`): exactly one state, see "Scan plans".

## Retries

A request that fails with 503, a network error or a per-attempt timeout is
retried with capped, jittered exponential backoff (100 ms base, ×2, 2 s cap, 3
retries); a `Retry-After` header is honoured up to 30 s. Collection writes are
keyed upserts, patches and deletes, so they are retried too: applying one twice
gives the same state. Every method takes `{ signal }` to abort the request and
any retry wait.

**Stream produce is never retried** once the request may have reached the
server: a 503 on produce may already be committed, and a blind retry would
duplicate records. If you retry produce yourself, dedupe by key.

## Errors

Every error is an `OperonError` with `code`, `status`, `message` and the JSON
`body`. Subclasses: `InvalidArgumentError` (and its `SchemaViolationError`),
`NotFoundError`, `AlreadyExistsError` (`id` on namespace and stream conflicts),
`ConflictError`, `OffsetOutOfRangeError`, `ResourceExhaustedError`
(`retryAfterMs`), `UnavailableError`, `OperonTimeoutError`, `InternalError`, and
`TransportError` for failures before a response (the fetch error is `cause`).
Arguments the SDK refuses before sending throw `TypeError` or `RangeError`.

## Scan plans

A scan plan (D53) resolves a collection into what an external reader needs to
read one state of it directly: the Lance dataset URI and version, its fragments
with their data and deletion files, and the columns.

```ts
const plan = await kb.scanPlan(); // or { at: <manifest version> } or { at: <token> }
if (plan.tail) {
  // The Lance version lacks recent writes; read them through the pin instead.
  console.log(plan.tailRecords, "records not yet in Lance");
}
const rows = await ns.sql("SELECT count(*) FROM kb", { consistency: plan.pin }); // the same state
const timeLeftMs = (plan.expiresAtMs ?? 0) - plan.plannedAtMs; // how long the plan is retained
```

`tail` says the requested state holds writes the Lance version does not yet
have; `consistency: plan.pin` reads exactly the planned state, tail included.
`expiresAtMs - plannedAtMs` is how long the plan's manifest stays retained (both
on the server's metastore clock). A plan carries no credentials: in M1 a reader
brings its own access to the object store. Manifest versions and
`plan.lance.version` are u64s: `bigint` above 2^53 − 1 (a detached Lance version
always is), and passed back exactly.

The SDK does not open Lance itself. Readers built on scan plans arrive in M2.

## Arrow

The SDK carries no Arrow implementation (so it keeps zero dependencies): results
have no `toArrow()` in M1. Use `SqlResult.toObjects()` for rows as objects, and
for Arrow results or bulk loading use an ADBC Flight SQL driver against the
server's Flight SQL listener (`operon dev`: `grpc://127.0.0.1:8082`), where
`adbc_ingest` loads Arrow data into a collection (see the native Flight SQL
docs). The SDK has no Flight client in M1.
