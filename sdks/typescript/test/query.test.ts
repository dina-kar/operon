// The query builder and the encoder's checks (plan M1.6 Task 6; Task 3 rules 2–5).
import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { ConsistencyToken, type Namespace, OperonClient, q } from "../dist/index.js";
import {
  encodeDate,
  encodeFieldValue,
  encodeRetriever,
  encodeSortKey,
  encodeVector,
} from "../dist/wire.js";
import { json, Script } from "./operon.ts";

const SEARCH = { hits: [], total: null, aggregations: null, groups: null, read_token: "v1:" };
const WRITTEN = { token: "v1:s1/p0@1", results: ["accepted"], positions: [null] };

/** A namespace over a fake fetch that answers every search and write. */
function recorded(): { ns: Namespace; script: Script; bodies: () => Record<string, unknown>[] } {
  const script = new Script();
  const answer = (request: Request): Response =>
    json(200, request.url.endsWith("/query") ? SEARCH : WRITTEN);
  for (let i = 0; i < 20; i += 1) script.steps.push(answer);
  const ns = new OperonClient({ fetch: script.fetch }).namespace("n");
  return { ns, script, bodies: () => script.bodies.map((b) => JSON.parse(b)) };
}

test("two retrievers default to rrf 60", () => {
  const { ns } = recorded();
  const builder = ns
    .search("kb")
    .retrieve(q.vector("e", [1], { k: 5 }), q.text("refund", { k: 5 }));
  assert.deepEqual(builder.toRequest().fusion, { rrf: { k: 60 } });
  assert.equal(
    ns
      .search("kb")
      .retrieve(q.vector("e", [1], { k: 5 }))
      .toRequest().fusion,
    undefined,
  );
  assert.equal(ns.search("kb").toRequest().fusion, undefined);
  assert.equal(builder.fuse(q.dbsf()).toRequest().fusion, "dbsf");
});

test("builder methods do not mutate", async () => {
  const { ns, bodies } = recorded();
  const base = ns.search("kb").filter(q.term("tenant", "a"));
  const three = base.limit(3);
  const seven = base.limit(7);
  await three.execute();
  await seven.execute();
  assert.deepEqual(
    bodies().map((b) => b.limit),
    [3, 7],
  );
  assert.equal(base.toRequest().limit, undefined);
  assert.notEqual(base.retrieve(q.text("x", { k: 1 })), base);
  assert.equal(base.toRequest().retrievers, undefined);
  const withToken = base.consistency("v1:s1/p0@1");
  assert.notEqual(withToken, base);
  await base.execute();
  assert.equal(bodies()[2]?.consistency, "strong");
});

test("text with a string picks match, multi_match or query_string", () => {
  assert.deepEqual(q.text("refund", { k: 3 }), {
    text: { query: { queryString: { query: "refund" } }, k: 3 },
  });
  assert.deepEqual(q.text("refund", { k: 3, fields: ["body"] }), {
    text: { query: { match: { field: "body", text: "refund" } }, k: 3 },
  });
  assert.deepEqual(q.text("refund", { k: 3, fields: ["body", "title"] }), {
    text: {
      query: {
        multiMatch: {
          fields: [
            ["body", 1],
            ["title", 1],
          ],
          text: "refund",
        },
      },
      k: 3,
    },
  });
  assert.deepEqual(q.text(q.match("body", "x"), { k: 2 }), {
    text: { query: { match: { field: "body", text: "x" } }, k: 2 },
  });
  assert.throws(() => q.text(q.match("body", "x"), { k: 2, fields: ["a"] }), TypeError);
  assert.deepEqual(encodeRetriever(q.text("refund", { k: 3 })), {
    text: {
      query: { query_string: { query: "refund", default_fields: [], default_operator: "or" } },
      k: 3,
    },
  });
});

describe("NaN and Infinity in a vector are rejected before sending", () => {
  for (const bad of [Number.NaN, Number.POSITIVE_INFINITY, Number.NEGATIVE_INFINITY]) {
    test(String(bad), async () => {
      const { ns, script } = recorded();
      await assert.rejects(
        ns
          .search("kb")
          .retrieve(q.vector("e", [1, bad], { k: 3 }))
          .execute(),
        /vector "e"/,
      );
      await assert.rejects(
        ns.collection("kb").upsert([{ id: 1, vectors: { e: [bad, 0] } }]),
        /vector "e"/,
      );
      await assert.rejects(
        ns.collection("kb").patch(1, { vectors: { e: new Float32Array([bad]) } }),
        /vector "e"/,
      );
      assert.equal(script.requests.length, 0);
    });
  }
});

test("non-numbers in a vector are rejected before sending", async () => {
  const { ns, script } = recorded();
  await assert.rejects(
    ns.collection("kb").upsert([{ id: 1, vectors: { e: [true as never, 0] } }]),
    TypeError,
  );
  await assert.rejects(
    ns
      .search("kb")
      .retrieve(q.vector("e", ["1.0" as never], { k: 3 }))
      .execute(),
    TypeError,
  );
  assert.equal(script.requests.length, 0);
});

test("Float32Array vectors are accepted", async () => {
  const { ns, bodies } = recorded();
  await ns.collection("kb").upsert([{ id: 1, vectors: { e: new Float32Array([1, 0.5]) } }]);
  await ns
    .search("kb")
    .retrieve(q.vector("e", new Float64Array([1, 0.25]), { k: 3 }))
    .execute();
  const [upsert, search] = bodies();
  assert.deepEqual(upsert?.ops, [
    { upsert: { id: 1, source: {}, vectors: { e: [1, 0.5] }, sparse_vectors: {} } },
  ]);
  const retrievers = search?.retrievers as Array<{ vector: { query: number[] } }>;
  assert.deepEqual(retrievers[0]?.vector.query, [1, 0.25]);
  // A float32 that is not exact in binary keeps its float32 value.
  assert.deepEqual(encodeVector("e", new Float32Array([0.1])), [Math.fround(0.1)]);
});

test("limit below one is rejected", async () => {
  const { ns, script } = recorded();
  assert.throws(() => ns.search("kb").limit(0).toRequest(), /limit/);
  assert.throws(() => ns.search("kb").limit(1.5).toRequest(), /limit/);
  assert.throws(() => ns.search("kb").offset(-1).toRequest(), /offset/);
  assert.throws(
    () =>
      ns
        .search("kb")
        .retrieve(q.text("x", { k: 0 }))
        .toRequest(),
    /k/,
  );
  assert.throws(
    () =>
      ns
        .search("kb")
        .retrieve(q.fused([q.text("x", { k: 0 })], q.rrf(), 3))
        .toRequest(),
    /k/,
  );
  await assert.rejects(ns.query({ collection: "kb", limit: 0 }), RangeError);
  await assert.rejects(ns.search("kb").limit(0).execute(), RangeError);
  assert.equal(script.requests.length, 0);
});

describe("sparse vectors are validated before sending", () => {
  const cases: Array<[string, number[], number[]]> = [
    ["a repeated index", [1, 1], [1, 2]],
    ["unequal lengths", [1, 2], [1]],
    ["NaN", [1], [Number.NaN]],
    ["a negative index", [-1], [1]],
    ["an index of 2^32", [2 ** 32], [1]],
    ["a fractional index", [1.5], [1]],
  ];
  for (const [name, indices, values] of cases) {
    test(name, async () => {
      const { ns, script } = recorded();
      assert.throws(() => q.sparse("s", { indices, values }, { k: 10 }), /sparse vector/);
      await assert.rejects(
        ns.collection("sp").upsert([{ id: 1, sparseVectors: { s: { indices, values } } }]),
        /sparse vector/,
      );
      await assert.rejects(
        ns.collection("sp").patch(1, { sparseVectors: { s: { indices, values } } }),
        /sparse vector/,
      );
      assert.equal(script.requests.length, 0);
    });
  }
});

test("q.sparse encodes the wire form", () => {
  assert.deepEqual(encodeRetriever(q.sparse("s", { indices: [5], values: [1] }, { k: 10 })), {
    sparse: {
      field: "s",
      query: { indices: [5], values: [1] },
      k: 10,
      filter: null,
      params: { idf_corpus: null },
    },
  });
  const withCorpus = q.sparse(
    "s",
    { indices: [5, 1], values: [1, 2] },
    { k: 4, filter: q.term("t", "a"), idfCorpus: q.term("t", "b") },
  );
  assert.deepEqual(encodeRetriever(withCorpus), {
    sparse: {
      field: "s",
      query: { indices: [5, 1], values: [1, 2] },
      k: 4,
      filter: { term: { field: "t", value: "a" } },
      params: { idf_corpus: { term: { field: "t", value: "b" } } },
    },
  });
});

test("dates encode as RFC 3339 in UTC", () => {
  assert.deepEqual(encodeFieldValue(new Date(Date.UTC(2026, 0, 2, 3, 4, 5))), {
    date: "2026-01-02T03:04:05Z",
  });
  assert.deepEqual(encodeFieldValue(new Date(Date.UTC(2026, 0, 2, 3, 4, 5, 6))), {
    date: "2026-01-02T03:04:05.006Z",
  });
  assert.equal(encodeDate(new Date("2026-06-01T12:00:00+02:00")), "2026-06-01T10:00:00Z");
  assert.throws(() => encodeFieldValue(new Date(Number.NaN)), RangeError);
  assert.equal(encodeFieldValue(2n ** 63n), 2n ** 63n);
  assert.equal(encodeFieldValue(-(2n ** 63n)), -(2n ** 63n));
  assert.throws(() => encodeFieldValue(2n ** 64n), RangeError);
  assert.throws(() => encodeFieldValue(2 ** 60), /bigint/);
  assert.throws(() => encodeFieldValue(Number.NaN), RangeError);
  assert.equal(encodeFieldValue(1.5), 1.5);
});

test("sort keys encode the wire form", () => {
  assert.deepEqual(encodeSortKey(q.scoreSort()), { score: { order: "desc" } });
  assert.deepEqual(encodeSortKey(q.fieldSort("n", "desc")), {
    field: { field: "n", order: "desc", missing: "last" },
  });
  assert.deepEqual(encodeSortKey({ field: { field: "n", order: "asc" } }), {
    field: { field: "n", order: "asc", missing: "last" },
  });
  assert.deepEqual(encodeSortKey(q.pkSort()), { pk: { order: "asc" } });
});

test("every search request key is sent", async () => {
  const { ns, script, bodies } = recorded();
  await ns
    .search("kb")
    .retrieve(
      q.fused([q.vector("e", [1], { k: 4 }), q.text("x", { k: 4 })], q.weightedSum(0.7, 0.3), 5),
      q.rescore(q.vector("e", [1], { k: 4, exact: true, ef: 16 }), "e", [0.5], 3),
    )
    .fuse(q.rrf(10))
    .sort(q.scoreSort(), q.pkSort("desc"))
    .offset(2)
    .limit(4)
    .searchAfter([1.5, { uuid: "0190f5c4-6c1e-7b3a-9d2e-4f5a6b7c8d9e" }, 2n ** 64n - 1n])
    .scoreThreshold(0.25)
    .select({ source: { include: ["a"] }, vectors: ["e"], fields: ["n"] })
    .aggregations({ t: { terms: { field: "tenant" } } })
    .highlight({ fields: [{ field: "body" }] })
    .groupBy({ field: "tenant", group_size: 1, limit: 2 })
    .trackTotalHits({ upTo: 100 })
    .consistency("eventual")
    .execute();
  const body = bodies()[0] as Record<string, unknown>;
  assert.deepEqual(Object.keys(body), [
    "collection",
    "consistency",
    "retrievers",
    "fusion",
    "filter",
    "sort",
    "offset",
    "limit",
    "search_after",
    "score_threshold",
    "select",
    "aggregations",
    "highlight",
    "group_by",
    "track_total_hits",
  ]);
  assert.equal(body.consistency, "eventual");
  assert.deepEqual(body.fusion, { rrf: { k: 10 } });
  assert.deepEqual(body.sort, [{ score: { order: "desc" } }, { pk: { order: "desc" } }]);
  assert.deepEqual(body.select, {
    source: { include: ["a"], exclude: [] },
    vectors: ["e"],
    fields: ["n"],
  });
  assert.deepEqual(body.track_total_hits, { up_to: 100 });
  assert.ok(
    script.bodies[0]?.includes(
      '"search_after":[1.5,{"uuid":"0190f5c4-6c1e-7b3a-9d2e-4f5a6b7c8d9e"},18446744073709551615]',
    ),
  );
  const retrievers = body.retrievers as Array<Record<string, Record<string, unknown>>>;
  assert.deepEqual(retrievers[0]?.fused?.fusion, { weighted_sum: { weights: [0.7, 0.3] } });
  const rescored = retrievers[1]?.rescore?.input as { vector: { params: unknown } };
  assert.deepEqual(rescored.vector.params, {
    exact: true,
    nprobes: null,
    refine_factor: null,
    ef: 16,
    oversampling: null,
    distance: null,
  });
  assert.equal(script.requests[0]?.headers.get("operon-consistency-token"), null);
});

test("a token is sent in the header and as at_least; a pin in the body", async () => {
  const { ns, script, bodies } = recorded();
  await ns.search("kb").consistency("v1:s1/p0@3").execute();
  await ns.query({ collection: "kb" }, { consistency: ConsistencyToken.parse("v1:s1/p0@4") });
  const pin = { manifestVersion: 5, token: ConsistencyToken.parse("v1:s1/p0@6") };
  await ns.search("kb").consistency(pin).execute();
  const [first, second, third] = script.requests;
  assert.equal(first?.headers.get("operon-consistency-token"), "v1:s1/p0@3");
  assert.deepEqual(bodies()[0]?.consistency, { at_least: "v1:s1/p0@3" });
  assert.equal(second?.headers.get("operon-consistency-token"), "v1:s1/p0@4");
  assert.deepEqual(bodies()[2]?.consistency, {
    pinned: { manifest_version: 5, token: "v1:s1/p0@6" },
  });
  assert.equal(third?.headers.get("operon-consistency-token"), null);
  await assert.rejects(ns.search("kb").consistency("v2:nope").execute(), /consistency token/);
  await assert.rejects(
    ns.search("kb").consistency({ manifestVersion: -1, token: pin.token }).execute(),
    RangeError,
  );
  assert.equal(script.requests.length, 3);
});

test("an empty write sends nothing", async () => {
  const { ns, script } = recorded();
  await assert.rejects(ns.collection("kb").upsert([]), /empty/);
  await assert.rejects(ns.collection("kb").delete([]), /empty/);
  await assert.rejects(ns.collection("kb").write([]), /empty/);
  assert.equal(script.requests.length, 0);
});

test("write ops encode the wire form", async () => {
  const { ns, bodies } = recorded();
  await ns.collection("kb").write(
    [
      { delete: "a" },
      {
        patch: {
          id: { uuid: "0190F5C4-6C1E-7B3A-9D2E-4F5A6B7C8D9E" },
          mode: "merge_top",
          vectors: { e: null, f: [1] },
          sparseVectors: { s: null },
          upsert: { id: 9, source: { x: 1 } },
        },
      },
    ],
    { reportExistence: true },
  );
  assert.deepEqual(bodies()[0], {
    ops: [
      { delete: { id: "a" } },
      {
        patch: {
          id: { uuid: "0190f5c4-6c1e-7b3a-9d2e-4f5a6b7c8d9e" },
          mode: "merge_top",
          source: {},
          delete_keys: [],
          vectors: { e: null, f: [1] },
          sparse_vectors: { s: null },
          upsert: { id: 9, source: { x: 1 }, vectors: {}, sparse_vectors: {} },
        },
      },
    ],
    report_existence: true,
  });
});
