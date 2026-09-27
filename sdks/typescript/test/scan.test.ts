// Scan plans (W15, Ruling 18).
import assert from "node:assert/strict";
import { after, before, describe, test } from "node:test";

import {
  type Collection,
  ConsistencyToken,
  type Namespace,
  OperonClient,
  type ScanPlan,
  type SchemaInput,
  s,
  type WriteResult,
} from "../dist/index.js";
import { freshName, json, type Operon, Script, startOperon } from "./operon.ts";

const FRESH = {
  namespace: "n",
  collection: "sc",
  collection_id: 4,
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

function scSchema(): SchemaInput {
  return { vectors: [s.vector("e", 2)], dynamic: "ignore" };
}

/** A scan plan answer as raw JSON text: u64 literals above 2^53 must not pass through a JS number. */
function rawPlan(manifestVersion: string, lance: string): Response {
  const text = JSON.stringify({ ...FRESH, lance: "@LANCE@", manifest_version: "@MV@" })
    .replace('"@LANCE@"', lance)
    .replace('"@MV@"', manifestVersion)
    .replace('"pin":{"manifest_version":0', `"pin":{"manifest_version":${manifestVersion}`);
  return new Response(text, { headers: { "content-type": "application/json" } });
}

describe("with a fake fetch", () => {
  test("scan points encode the wire form", async () => {
    const script = new Script();
    for (let i = 0; i < 5; i += 1) script.steps.push(json(200, FRESH));
    const sc = new OperonClient({ fetch: script.fetch }).namespace("n").collection("sc");
    await sc.scanPlan();
    await sc.scanPlan({ at: 5 });
    await sc.scanPlan({ at: ConsistencyToken.parse("v1:s9/p0@3") });
    await sc.scanPlan({ at: "v1:s9/p0@4" });
    await sc.scanPlan({ at: "current" });
    await assert.rejects(sc.scanPlan({ at: true as never }), TypeError);
    await assert.rejects(sc.scanPlan({ at: "v2:x" }), /consistency token/);
    await assert.rejects(sc.scanPlan({ at: -1 }), /manifest version/);
    await assert.rejects(sc.scanPlan({ at: 1.5 }), /manifest version/);
    await assert.rejects(sc.scanPlan({ at: 2n ** 64n }), /manifest version/);
    assert.deepEqual(
      script.bodies.map((b) => JSON.parse(b)),
      [
        { at: "current" },
        { at: { manifest_version: 5 } },
        { at: { token: "v1:s9/p0@3" } },
        { at: { token: "v1:s9/p0@4" } },
        { at: "current" },
      ],
    );
    assert.ok(
      script.requests.every(
        (r) =>
          new URL(r.url).pathname === "/v1/namespaces/n/collections/sc/scan" &&
          r.headers.get("operon-consistency-token") === null,
      ),
    );
  });

  test("a detached lance version comes back as bigint", async () => {
    const lance =
      '{"uri":"file:///d/kb.lance","version":9223372036854775809,"manifest_path":"_versions/x.manifest"}';
    const script = new Script(rawPlan("7", lance));
    const plan = await new OperonClient({ fetch: script.fetch })
      .namespace("n")
      .collection("sc")
      .scanPlan();
    assert.deepEqual(plan.lance, {
      uri: "file:///d/kb.lance",
      version: 9223372036854775809n,
      manifestPath: "_versions/x.manifest",
    });
    assert.equal(plan.manifestVersion, 7);
  });

  test("a manifest version above 2^53 round trips", async () => {
    const search = { hits: [], total: null, aggregations: null, groups: null, read_token: "v1:" };
    const script = new Script(
      rawPlan("9007199254740993", "null"),
      json(200, search),
      json(200, FRESH),
    );
    const sc = new OperonClient({ fetch: script.fetch }).namespace("n").collection("sc");
    const plan = await sc.scanPlan();
    assert.equal(plan.manifestVersion, 9007199254740993n);
    assert.equal(plan.pin.manifestVersion, 9007199254740993n);
    await sc.search().consistency(plan.pin).execute();
    await sc.scanPlan({ at: 9007199254740993n });
    assert.ok(
      script.bodies[1]?.includes(
        '"consistency":{"pinned":{"manifest_version":9007199254740993,"token":"v1:s9/p0@0"}}',
      ),
    );
    assert.equal(script.bodies[2], '{"at":{"manifest_version":9007199254740993}}');
    assert.equal(script.requests[1]?.headers.get("operon-consistency-token"), null);
  });

  test("a plan decodes fragments, files and columns", async () => {
    const answer = {
      ...FRESH,
      manifest_version: 3,
      lance: { uri: null, version: 2, manifest_path: "_versions/2.manifest" },
      fragments: [
        {
          id: 0,
          physical_rows: 3,
          deleted_rows: 1,
          live_rows: 2,
          files: [{ path: "data/a.lance", size_bytes: null }],
          deletion_file: { path: "_deletions/0.arrow" },
          lance: { id: 0 },
        },
      ],
      live_rows: 2,
      columns: [
        { name: "_pk", data_type: "Binary", role: "pk" },
        {
          name: "_vector_0",
          data_type: "FixedSizeList(2 x Float32)",
          role: "vector",
          vector: "e",
          dim: 2,
        },
      ],
      tail: true,
      tail_records: 4,
      planned_at_ms: 10,
      expires_at_ms: 70,
    };
    const script = new Script(json(200, answer));
    const plan: ScanPlan = await new OperonClient({ fetch: script.fetch })
      .namespace("n")
      .collection("sc")
      .scanPlan();
    assert.deepEqual(plan.fragments, [
      {
        id: 0,
        physicalRows: 3,
        deletedRows: 1,
        liveRows: 2,
        files: ["data/a.lance"],
        deletionFile: "_deletions/0.arrow",
        lance: { id: 0 },
      },
    ]);
    assert.deepEqual(plan.columns, [
      { name: "_pk", dataType: "Binary", role: "pk" },
      {
        name: "_vector_0",
        dataType: "FixedSizeList(2 x Float32)",
        role: "vector",
        vector: "e",
        dim: 2,
      },
    ]);
    assert.equal(plan.lance?.uri, null);
    assert.equal(plan.tail, true);
    assert.equal(plan.tailRecords, 4);
    assert.equal(plan.expiresAtMs, 70);
    assert.equal(plan.raw.pk_encoding, "operon_canonical_v1");
  });
});

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

  async function settled(collection: Collection): Promise<ScanPlan> {
    const deadline = performance.now() + 30_000;
    for (;;) {
      const plan = await collection.scanPlan();
      if (!plan.tail && plan.lance !== null) return plan;
      if (performance.now() > deadline) {
        assert.fail(
          `the scan plan still has a tail after 30 s: ${JSON.stringify(plan.raw, (_k, v) => (typeof v === "bigint" ? v.toString() : v))}`,
        );
      }
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
  }

  async function written(ns: Namespace): Promise<[Collection, WriteResult]> {
    await ns.createCollection("kb", scSchema(), { partitions: 1 });
    const kb = ns.collection("kb");
    await kb.upsert([1, 2, 3].map((i) => ({ id: i, source: { i }, vectors: { e: [1, i] } })));
    await kb.upsert([{ id: 1, source: { i: 10 }, vectors: { e: [1, 0] } }]);
    return [kb, await kb.delete([2])];
  }

  test("a scan plan of a fresh collection", async () => {
    const ns = await freshNamespace();
    await ns.createCollection("sc", scSchema(), { partitions: 1 });
    const plan = await ns.collection("sc").scanPlan();
    assert.equal(plan.collection, "sc");
    assert.equal(plan.manifestVersion, 0);
    assert.equal(plan.lance, null);
    assert.deepEqual(plan.fragments, []);
    assert.equal(plan.liveRows, 0);
    assert.equal(plan.tail, false);
    assert.deepEqual(plan.raw.offsets, [{ partition: 0, applied: 0, target: 0 }]);
    assert.equal(plan.pin.manifestVersion, 0);
    assert.equal(plan.expiresAtMs, null);
    assert.equal(plan.durableToken.toString(), plan.pin.token.toString());
  });

  test("a scan plan after writes reports lance and fragments", async () => {
    const ns = await freshNamespace();
    const [kb, last] = await written(ns);
    const plan = await settled(kb);
    assert.ok(plan.lance?.uri?.startsWith("file://"));
    assert.equal(plan.liveRows, 2);
    assert.equal(plan.durableToken.toString(), plan.pin.token.toString());
    assert.ok(plan.fragments.length > 0);
    assert.ok(plan.fragments.every((f) => f.files.length > 0));
    const vector = plan.columns.find((c) => c.name === "_vector_0");
    assert.equal(vector?.vector, "e");
    assert.equal(vector?.dim, 2);
    const atToken = await kb.scanPlan({ at: last.token });
    assert.ok(BigInt(atToken.manifestVersion) >= BigInt(plan.manifestVersion));
    const atVersion = await kb.scanPlan({ at: plan.manifestVersion });
    assert.deepEqual(atVersion.fragments, plan.fragments);
  });

  test("a pin reads the same state", async () => {
    const ns = await freshNamespace();
    const [kb] = await written(ns);
    const plan = await settled(kb);
    const count = async (consistency: Parameters<Namespace["sql"]>[1]): Promise<unknown> =>
      (await ns.sql("SELECT count(*) AS n FROM kb", consistency)).rows;
    assert.deepEqual(await count({ consistency: plan.pin }), [[2]]);
    const later = await kb.upsert([{ id: 4, source: { i: 4 }, vectors: { e: [0, 1] } }]);
    assert.deepEqual(await count({ consistency: later.token }), [[3]]);
    assert.deepEqual(await count({ consistency: plan.pin }), [[2]]);
    assert.deepEqual(await kb.get([4], { consistency: plan.pin }), [null]);
    const hits = (await kb.search().consistency(plan.pin).execute()).hits;
    assert.deepEqual(new Set(hits.map((h) => h.id)), new Set([1, 3]));
  });
});
