// Collections and documents against a spawned `operon dev` (plan M1.6 Task 6).
import assert from "node:assert/strict";
import { after, before, describe, test } from "node:test";

import {
  AlreadyExistsError,
  type Collection,
  ConsistencyToken,
  InvalidArgumentError,
  type Namespace,
  NotFoundError,
  OperonClient,
  q,
  s,
} from "../dist/index.js";
import {
  freshName,
  json,
  kbDocs,
  kbSchema,
  type Operon,
  Script,
  startOperon,
  unavailable,
} from "./operon.ts";

describe("against operon dev", () => {
  let server: Operon;
  let client: OperonClient;

  before(async () => {
    server = await startOperon();
    client = new OperonClient({ baseUrl: server.baseUrl });
  });
  after(async () => {
    await server?.stop();
  });

  async function freshNamespace(): Promise<Namespace> {
    const name = freshName();
    await client.createNamespace(name);
    return client.namespace(name);
  }

  /** `kb` with its three documents written. */
  async function kb(): Promise<Collection> {
    const ns = await freshNamespace();
    await ns.createCollection("kb", kbSchema(), { partitions: 2 });
    const collection = ns.collection("kb");
    await collection.upsert(kbDocs());
    return collection;
  }

  test("collection lifecycle", async () => {
    const ns = await freshNamespace();
    const info = await ns.createCollection("kb", kbSchema(), { partitions: 2 });
    assert.equal(info.name, "kb");
    assert.equal(info.partitions, 2);
    assert.equal(info.liveDocCount, 0);
    assert.equal(info.schema.dynamic, "ignore");
    assert.equal(info.schema.version, 1);
    assert.deepEqual(
      info.schema.fields?.map((f) => f.name),
      ["body", "tenant", "n"],
    );
    assert.deepEqual(info.schema.fields?.[0]?.kind, {
      text: { analyzer: "standard", positions: true },
    });
    assert.deepEqual(info.schema.vectors?.[0], { name: "embedding", dim: 3, distance: "cosine" });
    assert.equal(info.raw.name, "kb");
    const got = await ns.getCollection("kb");
    assert.equal(got.id, info.id);
    assert.deepEqual(got.schema.fields, info.schema.fields);
    assert.ok((await ns.listCollections()).map((c) => c.name).includes("kb"));
    assert.equal(await ns.dropCollection("kb"), true);
    assert.equal(await ns.dropCollection("kb"), false);
    await assert.rejects(ns.getCollection("kb"), NotFoundError);
  });

  test("createCollection is retry-safe", async () => {
    const ns = await freshNamespace();
    const first = await ns.createCollection("kb", kbSchema());
    const again = await ns.createCollection("kb", kbSchema());
    assert.equal(again.id, first.id);
    await assert.rejects(
      ns.createCollection("kb", { fields: [s.keyword("tenant")], dynamic: "ignore" }),
      (error: unknown) => {
        assert.ok(error instanceof AlreadyExistsError);
        assert.equal(error.id, undefined);
        return true;
      },
    );
  });

  test("upsert then get reads its own write", async () => {
    const ns = await freshNamespace();
    await ns.createCollection("kb", kbSchema());
    const collection = ns.collection("kb");
    const result = await collection.upsert(kbDocs());
    assert.deepEqual(result.results, ["accepted", "accepted", "accepted"]);
    assert.ok(result.token.items.length > 0);
    const docs = await collection.get([1, 2, 3]);
    assert.deepEqual(
      docs.map((d) => d?.source.body),
      ["refund policy", "shipping times", "refund window"],
    );
    const again = await collection.get([1], {
      consistency: result.token,
      select: { vectors: ["embedding"] },
    });
    assert.deepEqual(again[0]?.vectors, { embedding: [1, 0, 0] });
    assert.equal((await collection.get([1], { consistency: "eventual" })).length, 1);
  });

  test("patch merge_deep and deleteKeys", async () => {
    const collection = await kb();
    const result = await collection.patch(1, {
      source: { meta: { x: 1 } },
      deleteKeys: ["tenant"],
    });
    const [doc] = await collection.get([1], { consistency: result.token });
    assert.deepEqual(doc?.source.meta, { x: 1 });
    assert.equal("tenant" in (doc?.source ?? {}), false);
    assert.equal(doc?.source.body, "refund policy");
  });

  test("delete then get returns null", async () => {
    const collection = await kb();
    const result = await collection.delete([2]);
    const docs = await collection.get([2, 1], { consistency: result.token });
    assert.equal(docs[0], null);
    assert.equal(docs[1]?.id, 1);
  });

  test("hybrid search fuses with rrf", async () => {
    const collection = await kb();
    const response = await collection
      .search()
      .retrieve(
        q.vector("embedding", [1, 0, 0], { k: 10 }),
        q.text(q.match("body", "refund"), { k: 10 }),
      )
      .limit(3)
      .execute();
    assert.deepEqual(
      response.hits.map((h) => h.id),
      [1, 3, 2],
    );
    const scores = response.hits.map((h) => h.score);
    assert.deepEqual(
      scores,
      [...scores].sort((a, b) => b - a),
    );
    assert.equal(response.hits[0]?.source?.body, "refund policy");
    assert.ok(response.readToken instanceof ConsistencyToken);
  });

  test("filter applies to every retriever", async () => {
    const collection = await kb();
    const response = await collection
      .search()
      .retrieve(
        q.vector("embedding", [1, 0, 0], { k: 10 }),
        q.text(q.match("body", "refund"), { k: 10 }),
      )
      .filter(q.term("tenant", "a"))
      .execute();
    assert.deepEqual(
      response.hits.map((h) => h.id),
      [1, 2],
    );
  });

  test("u64 ids above 2^53 come back as bigint", async () => {
    const collection = await kb();
    const big = 9007199254740993n;
    const result = await collection.upsert([{ id: big, source: { tenant: "c" } }]);
    const [doc] = await collection.get([big], { consistency: result.token });
    assert.equal(doc?.id, big);
    const hits = (await collection.search().filter(q.ids(big)).execute()).hits;
    assert.deepEqual(
      hits.map((h) => h.id),
      [big],
    );
    assert.equal(hits[0]?.sortValues.at(-1), big);
  });

  test("uuid and string ids round trip", async () => {
    const collection = await kb();
    const key = { uuid: "0190F5C4-6C1E-7B3A-9D2E-4F5A6B7C8D9E" };
    const result = await collection.upsert([
      { id: key, source: { tenant: "c" } },
      { id: "k-str", source: { tenant: "c" } },
      { id: "0190f5c4-6c1e-7b3a-9d2e-4f5a6b7c8d9e", source: { tenant: "d" } },
    ]);
    const got = await collection.get([key, "k-str", "0190f5c4-6c1e-7b3a-9d2e-4f5a6b7c8d9e"], {
      consistency: result.token,
    });
    assert.deepEqual(
      got.map((d) => d?.id),
      [
        { uuid: "0190f5c4-6c1e-7b3a-9d2e-4f5a6b7c8d9e" },
        "k-str",
        "0190f5c4-6c1e-7b3a-9d2e-4f5a6b7c8d9e",
      ],
    );
    assert.equal(got[0]?.source.tenant, "c");
    assert.equal(got[2]?.source.tenant, "d");
  });

  test("wrong vector dimension throws InvalidArgumentError", async () => {
    const collection = await kb();
    await assert.rejects(
      collection.upsert([{ id: 7, vectors: { embedding: [1, 2] } }]),
      InvalidArgumentError,
    );
  });

  test("sparse and hybrid search", async () => {
    const ns = await freshNamespace();
    const info = await ns.createCollection("sp", {
      vectors: [s.vector("e", 2)],
      sparseVectors: [s.sparseVector("s")],
      dynamic: "ignore",
    });
    assert.deepEqual(info.schema.sparseVectors, [{ name: "s", modifier: "none" }]);
    const sp = ns.collection("sp");
    await sp.upsert([
      { id: 1, vectors: { e: [1, 0] }, sparseVectors: { s: { indices: [5, 1], values: [2, 1] } } },
      { id: 2, vectors: { e: [0, 1] }, sparseVectors: { s: { indices: [5], values: [0.5] } } },
      { id: 3, vectors: { e: [0.8, 0.6] }, sparseVectors: { s: { indices: [7], values: [3] } } },
    ]);
    const sparse = q.sparse("s", { indices: [5], values: [1] }, { k: 10 });
    const only = await sp.search().retrieve(sparse).execute();
    assert.deepEqual(
      only.hits.map((h) => h.id),
      [1, 2],
    );
    assert.deepEqual(
      only.hits.map((h) => h.score),
      [2, 0.5],
    );
    const hybrid = await sp
      .search()
      .retrieve(sparse, q.vector("e", [1, 0], { k: 10 }))
      .limit(3)
      .execute();
    assert.deepEqual(
      hybrid.hits.map((h) => h.id),
      [1, 2, 3],
    );
    const [doc] = await sp.get([1], { select: { source: "none", vectors: ["s"] } });
    assert.deepEqual(doc?.source, {});
    assert.deepEqual(doc?.sparseVectors.s, { indices: [1, 5], values: [1, 2] });
  });
});

describe("with a fake fetch", () => {
  test("a 503 on a collection write is retried", async () => {
    const written = { token: "v1:s1/p0@1", results: ["accepted"], positions: [null] };
    const script = new Script(
      unavailable(),
      json(200, written, { "operon-consistency-token": "v1:s1/p0@1" }),
    );
    const client = new OperonClient({ fetch: script.fetch, sleep: async () => {} });
    const result = await client
      .namespace("n")
      .collection("c")
      .upsert([{ id: 1, source: { a: 1 } }]);
    assert.deepEqual(result.results, ["accepted"]);
    assert.equal(result.token.toString(), "v1:s1/p0@1");
    assert.equal(script.requests.length, 2);
    assert.equal(script.bodies[0], script.bodies[1]);
  });

  test("the body token is read when the header is absent", async () => {
    const script = new Script(
      json(200, { token: "v1:s2/p1@4", results: ["deleted"], positions: [null] }),
    );
    const result = await new OperonClient({ fetch: script.fetch })
      .namespace("n")
      .collection("c")
      .delete([1]);
    assert.equal(result.token.toString(), "v1:s2/p1@4");
  });
});
