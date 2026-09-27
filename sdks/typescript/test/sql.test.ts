// SQL over the native API (W13).
import assert from "node:assert/strict";
import { after, before, describe, test } from "node:test";

import {
  ConsistencyToken,
  InvalidArgumentError,
  type Namespace,
  OperonClient,
} from "../dist/index.js";
import { freshName, json, kbDocs, kbSchema, type Operon, Script, startOperon } from "./operon.ts";

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

  test("sql select returns rows", async () => {
    const ns = await freshNamespace();
    await ns.createCollection("kb", kbSchema());
    await ns.collection("kb").upsert(kbDocs());
    const result = await ns.sql("SELECT count(*) AS n FROM kb");
    assert.equal(result.columns[0]?.name, "n");
    assert.equal(result.columns[0]?.type, "Int64");
    assert.deepEqual(result.rows, [[3]]);
    assert.deepEqual(result.toObjects(), [{ n: 3 }]);
    assert.equal(result.truncated, false);
  });

  test("sql keeps u64 values above 2^53 exact", async () => {
    const ns = await freshNamespace();
    const result = await ns.sql("SELECT arrow_cast(18446744073709551615, 'UInt64') AS big");
    assert.deepEqual(result.rows, [[18446744073709551615n]]);
  });

  test("sql error throws InvalidArgumentError", async () => {
    const ns = await freshNamespace();
    await assert.rejects(ns.sql("SELEC 1"), InvalidArgumentError);
  });
});

test("eventual is sent in the body and tokens in the header", async () => {
  const script = new Script();
  for (let i = 0; i < 5; i += 1) {
    script.steps.push(json(200, { columns: [], rows: [], truncated: false }));
  }
  const token = "v1:s1/p0@9";
  const pin = { manifestVersion: 3, token: ConsistencyToken.parse("v1:s1/p0@8") };
  const ns = new OperonClient({ fetch: script.fetch }).namespace("n");
  await ns.sql("SELECT 1");
  await ns.sql("SELECT 1", { consistency: "eventual" });
  await ns.sql("SELECT 1", { consistency: token });
  await ns.sql("SELECT 1", { consistency: ConsistencyToken.parse(token) });
  await ns.sql("SELECT 1", { consistency: pin });
  assert.deepEqual(
    script.bodies.map((b) => JSON.parse(b)),
    [
      { query: "SELECT 1" },
      { query: "SELECT 1", consistency: "eventual" },
      { query: "SELECT 1" },
      { query: "SELECT 1" },
      { query: "SELECT 1", consistency: { pinned: { manifest_version: 3, token: "v1:s1/p0@8" } } },
    ],
  );
  assert.deepEqual(
    script.requests.map((r) => r.headers.get("operon-consistency-token")),
    [null, null, token, token, null],
  );
  assert.ok(script.requests.every((r) => new URL(r.url).pathname === "/v1/namespaces/n/sql"));
  await assert.rejects(ns.sql("SELECT 1", { consistency: "v2:x" }), RangeError);
  assert.equal(script.requests.length, 5);
});
