// The SDK sends exactly the requests of `sdks/fixtures/` (plan M1.6 Task 6, rows T1-2 and T1-3).
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";

import { OperonClient, type Query, q, s } from "../dist/index.js";
import { decode, encode } from "../dist/json.js";
import { encodeQuery } from "../dist/wire.js";
import { json } from "./operon.ts";

const FIXTURES = new URL("../../fixtures/", import.meta.url);
const NS = "wire";
const UUID = { uuid: "0190f5c4-6c1e-7b3a-9d2e-4f5a6b7c8d9e" };
const U64_MAX = 2n ** 64n - 1n;

type Json = Record<string, unknown>;

const INFO = {
  id: 11,
  name: "kb",
  schema: {
    fields: [],
    vectors: [],
    sparse_vectors: [],
    dynamic: "ignore",
    max_fields: 1000,
    version: 1,
  },
  partitions: 2,
  live_doc_count: 0,
};
const SEARCH = { hits: [], total: null, aggregations: null, groups: null };
const SCAN = {
  namespace: NS,
  collection: "sc",
  collection_id: 13,
  manifest_version: 0,
  schema_version: 1,
  lance: null,
  fragments: [],
  live_rows: 0,
  columns: [],
  pk_encoding: "operon_canonical_v1",
  tail: false,
  tail_records: 0,
  offsets: [{ partition: 0, applied: 0, target: 0 }],
  durable_token: "v1:s9/p0@0",
  pin: { manifest_version: 0, token: "v1:s9/p0@0" },
  planned_at_ms: 0,
  expires_at_ms: null,
};

function written(n: number): Json {
  return { results: Array(n).fill("accepted"), positions: Array(n).fill(null) };
}

// Per SDK-producible step: status and body of the canned answer. Every answer
// also carries a distinct token (header, and `token`/`read_token` where the
// real server has one), so `{token:<step>}` can be checked.
const CANNED: Record<string, [number, Json]> = {
  create_namespace: [201, { id: 1 }],
  create_stream: [201, { id: 7 }],
  produce: [200, { base_offset: 0, last_offset: 0, token: [] }],
  fetch: [200, { records: [], next_offset: 0, high_watermark: 0, log_start_offset: 0 }],
  create_collection: [201, INFO],
  list_collections: [200, { collections: [INFO] }],
  get_collection: [200, INFO],
  write_docs: [200, written(6)],
  get_docs: [200, { documents: Array(5).fill(null) }],
  query_hybrid: [200, SEARCH],
  query_filter_only: [200, SEARCH],
  patch: [200, written(1)],
  delete: [200, written(1)],
  get_after_changes: [200, { documents: [null, null] }],
  sql_count: [200, { columns: [{ name: "n", type: "Int64" }], rows: [[5]] }],
  drop_collection: [200, { dropped: true }],
  create_sparse_collection: [201, INFO],
  write_sparse_docs: [200, written(3)],
  query_sparse: [200, SEARCH],
  query_sparse_hybrid: [200, SEARCH],
  get_sparse_docs: [200, { documents: [null] }],
  drop_sparse_collection: [200, { dropped: true }],
  create_scan_collection: [201, INFO],
  scan_plan_fresh: [200, SCAN],
  drop_scan_collection: [200, { dropped: true }],
};
const TOKEN_KEY = new Set(["write_docs", "patch", "delete", "write_sparse_docs"]);
const READ_TOKEN_KEY = new Set([
  "get_docs",
  "get_after_changes",
  "query_hybrid",
  "query_filter_only",
  "query_sparse",
  "query_sparse_hybrid",
  "get_sparse_docs",
]);

function token(index: number): string {
  return `v1:s1/p0@${index + 100}`;
}

function readFixture(name: string): Json {
  return decode(readFileSync(new URL(name, FIXTURES), "utf8")) as Json;
}

function replaceNs(value: unknown): unknown {
  if (typeof value === "string") return value.replaceAll("{ns}", NS);
  if (Array.isArray(value)) return value.map(replaceNs);
  if (typeof value === "object" && value !== null) {
    return Object.fromEntries(Object.entries(value).map(([k, v]) => [k, replaceNs(v)]));
  }
  return value;
}

interface Recorded {
  method: string;
  url: URL;
  token: string | null;
  body: unknown;
}

/** Answers each request with the next step's canned reply, and records it. */
function recorder(names: string[]): { fetch: typeof globalThis.fetch; requests: Recorded[] } {
  const requests: Recorded[] = [];
  const fetch = async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    const request = new Request(input, init);
    const text = await request.text();
    const index = requests.length;
    requests.push({
      method: request.method,
      url: new URL(request.url),
      token: request.headers.get("operon-consistency-token"),
      body: text === "" ? null : decode(text),
    });
    const name = names[index];
    if (name === undefined) throw new Error(`unexpected request ${request.method} ${request.url}`);
    const [status, canned] = CANNED[name] as [number, Json];
    const body: Json = { ...canned };
    if (TOKEN_KEY.has(name)) body.token = token(index);
    if (READ_TOKEN_KEY.has(name)) body.read_token = token(index);
    return json(status, body, { "operon-consistency-token": token(index) });
  };
  return { fetch, requests };
}

/** Every SDK-producible step, in order, through the public API. */
async function runScenario(client: OperonClient): Promise<void> {
  await client.createNamespace(NS);
  const ns = client.namespace(NS);
  await ns.createStream("events", 2);
  await ns.produce("events", 0, [
    { key: "k", value: "v", headers: [["h", "x"]], timestampMs: 1_700_000_000_000 },
  ]);
  await ns.fetch("events", 0, 0, { maxBytes: 1_048_576, maxWaitMs: 0 });
  const kbSchema = {
    fields: [s.text("body"), s.keyword("tenant"), s.i64("n")],
    vectors: [s.vector("embedding", 3)],
    dynamic: "ignore" as const,
  };
  await ns.createCollection("kb", kbSchema, { partitions: 2 });
  await ns.listCollections();
  await ns.getCollection("kb");
  const kb = ns.collection("kb");
  const docs = await kb.upsert([
    {
      id: 1,
      source: { body: "refund policy", tenant: "a", n: 1 },
      vectors: { embedding: [1, 0, 0] },
    },
    {
      id: 2,
      source: { body: "shipping times", tenant: "a", n: 2 },
      vectors: { embedding: [0.9, 0.1, 0] },
    },
    {
      id: 3,
      source: { body: "refund window", tenant: "b", n: 3 },
      vectors: { embedding: [0, 0, 1] },
    },
    { id: U64_MAX, source: { tenant: "c" } },
    { id: "k-str", source: { tenant: "c" } },
    { id: UUID, source: { tenant: "c" } },
  ]);
  await kb.get([1, U64_MAX, "k-str", UUID, 999], { consistency: docs.token });
  await ns
    .search("kb")
    .retrieve(q.vector("embedding", [1, 0, 0], { k: 10 }))
    .retrieve(q.text(q.match("body", "refund"), { k: 10 }))
    .limit(3)
    .execute();
  await kb.search().filter(q.term("tenant", "b")).execute();
  await kb.patch(1, { source: { meta: { x: 1 } }, deleteKeys: ["tenant"] });
  const deleted = await kb.delete([2]);
  await kb.get([1, 2], { consistency: deleted.token });
  await ns.sql("SELECT count(*) AS n FROM kb");
  await ns.dropCollection("kb");

  await ns.createCollection("sp", {
    vectors: [s.vector("e", 2)],
    sparseVectors: [s.sparseVector("s")],
    dynamic: "ignore",
  });
  const sp = ns.collection("sp");
  const sparseWritten = await sp.upsert([
    { id: 1, vectors: { e: [1, 0] }, sparseVectors: { s: { indices: [5, 1], values: [2, 1] } } },
    { id: 2, vectors: { e: [0, 1] }, sparseVectors: { s: { indices: [5], values: [0.5] } } },
    { id: 3, vectors: { e: [0.8, 0.6] }, sparseVectors: { s: { indices: [7], values: [3] } } },
  ]);
  const sparse = q.sparse("s", { indices: [5], values: [1] }, { k: 10 });
  await sp.search().retrieve(sparse).execute();
  await sp
    .search()
    .retrieve(sparse, q.vector("e", [1, 0], { k: 10 }))
    .limit(3)
    .execute();
  await sp.get([1], {
    select: { source: "none", vectors: ["s"] },
    consistency: sparseWritten.token,
  });
  await ns.dropCollection("sp");

  await ns.createCollection(
    "sc",
    { vectors: [s.vector("e", 2)], dynamic: "ignore" },
    { partitions: 1 },
  );
  await ns.collection("sc").scanPlan();
  await ns.dropCollection("sc");
}

test("SDK requests equal the wire fixtures", async () => {
  const steps = new Map(
    (readFixture("scenario.json").steps as Json[]).map((step) => [step.name as string, step]),
  );
  const names = Object.keys(CANNED);
  const { fetch, requests } = recorder(names);
  await runScenario(new OperonClient({ baseUrl: "http://operon.test", fetch }));
  assert.equal(requests.length, names.length);
  const indexOf = new Map(names.map((name, i) => [name, i]));
  names.forEach((name, i) => {
    const step = steps.get(name);
    const request = requests[i];
    assert.ok(step !== undefined && request !== undefined, name);
    assert.equal(request.method, step.method, name);
    assert.equal(request.url.pathname + request.url.search, replaceNs(step.path), name);
    let expectedToken: string | null = null;
    for (const [key, value] of Object.entries(step.headers as Record<string, string>)) {
      assert.equal(key.toLowerCase(), "operon-consistency-token", name);
      const source = value.replace(/^\{token:/, "").replace(/\}$/, "");
      expectedToken = token(indexOf.get(source) as number);
    }
    assert.equal(request.token, expectedToken, name);
    assert.deepEqual(request.body, replaceNs(step.body), name);
  });
});

// One builder expression per entry of queries.json, in the file's order.
const BUILDERS: Record<string, () => Query> = {
  match_all: q.matchAll,
  match_none: q.matchNone,
  match: () => q.match("title", "hello world"),
  match_phrase: () => q.matchPhrase("title", "hello world"),
  multi_match: () =>
    q.multiMatch(
      [
        ["title", 2],
        ["tag", 1],
      ],
      "hello",
    ),
  term: () => q.term("tag", "a"),
  terms: () => q.terms("n", [1, 2]),
  range: () => q.range("n", { gte: 1, lt: 10 }),
  exists: () => q.exists("meta"),
  is_null: () => q.isNull("meta.k"),
  is_empty: () => q.isEmpty("tag"),
  values_count: () => q.valuesCount("meta.k", { gte: 1 }),
  prefix: () => q.prefix("tag", "a"),
  wildcard: () => q.wildcard("tag", "a*"),
  fuzzy: () => q.fuzzy("title", "helo", { fuzziness: 1 }),
  ids: () => q.ids(1, "k-str", UUID, U64_MAX),
  query_string: () => q.queryString("title:hello AND tag:a"),
  bool: () =>
    q.bool({
      must: [q.match("title", "hello")],
      should: [q.term("flag", true)],
      mustNot: [q.term("tag", "z")],
      filter: [q.range("n", { gte: 1 })],
    }),
  boost: () => q.boost(q.term("tag", "a"), 2),
  constant_score: () => q.constantScore(q.term("tag", "a"), 1.5),
  match_fuzzy_auto: () => q.match("title", "helo", { fuzziness: "auto" }),
  range_dates: () =>
    q.range("ts", {
      gte: new Date(Date.UTC(2026, 0, 1)),
      lt: new Date("2027-01-01T00:00:00Z"),
    }),
};

test("every fixture query is produced by q", () => {
  const entries = readFixture("queries.json").queries as Array<{ name: string; query: unknown }>;
  assert.deepEqual(
    entries.map((e) => e.name),
    Object.keys(BUILDERS),
  );
  for (const { name, query } of entries) {
    const build = BUILDERS[name] as () => Query;
    // Through the encoder's JSON, as the SDK sends it.
    assert.deepEqual(decode(encode(encodeQuery(build()))), query, name);
  }
});
