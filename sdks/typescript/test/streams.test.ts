import assert from "node:assert/strict";
import { after, before, describe, test } from "node:test";

import {
  AlreadyExistsError,
  ConsistencyToken,
  NotFoundError,
  OffsetOutOfRangeError,
  OperonClient,
} from "../dist/index.js";
import { decodeId, encodeId } from "../dist/wire.js";
import { freshName, json, type Operon, Script, startOperon } from "./operon.ts";

const utf8 = new TextEncoder();

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

  async function freshNamespace() {
    const name = freshName();
    await client.createNamespace(name);
    return client.namespace(name);
  }

  test("creates a namespace and a stream, then produces and fetches", async () => {
    const ns = await freshNamespace();
    const streamId = await ns.createStream("events", 2);
    const key = new Uint8Array([0, 1, 255]);
    const produced = await ns.produce("events", 1, [
      { key, value: "hello", headers: [["h", "v"]] },
      { value: new Uint8Array([7]) },
      { key: "k", timestampMs: 1_700_000_000_000 },
    ]);
    assert.equal(produced.baseOffset, 0);
    assert.equal(produced.lastOffset, 2);
    assert.deepEqual(produced.token.items, [[BigInt(streamId), 1, 3n]]);

    const fetched = await ns.fetch("events", 1, 0);
    assert.equal(fetched.records.length, 3);
    assert.equal(fetched.nextOffset, 3);
    assert.equal(fetched.highWatermark, 3);
    assert.equal(fetched.logStartOffset, 0);
    const [first, second, third] = fetched.records;
    assert.deepEqual(first?.key, key);
    assert.deepEqual(first?.value, utf8.encode("hello"));
    assert.deepEqual(first?.headers, [["h", utf8.encode("v")]]);
    assert.equal(first?.offset, 0);
    assert.equal(second?.key, null);
    assert.deepEqual(second?.value, new Uint8Array([7]));
    assert.deepEqual(third?.key, utf8.encode("k"));
    assert.equal(third?.value, null);
    assert.equal(third?.timestampMs, 1_700_000_000_000);
  });

  test("creating a namespace twice throws AlreadyExistsError unless existOk", async () => {
    const name = freshName();
    const id = await client.createNamespace(name);
    await assert.rejects(client.createNamespace(name), (error: unknown) => {
      assert.ok(error instanceof AlreadyExistsError);
      assert.equal(error.id, id);
      return true;
    });
    assert.equal(await client.createNamespace(name, { existOk: true }), id);
  });

  test("fetching above the high watermark throws OffsetOutOfRangeError", async () => {
    const ns = await freshNamespace();
    await ns.createStream("s", 1);
    await assert.rejects(ns.fetch("s", 0, 100), (error: unknown) => {
      assert.ok(error instanceof OffsetOutOfRangeError);
      assert.equal(error.highWatermark, 0);
      assert.equal(error.offset, 100);
      return true;
    });
  });

  test("a long poll longer than timeoutMs does not time out", async () => {
    const ns = await freshNamespace();
    await ns.createStream("s", 1);
    const slow = new OperonClient({ baseUrl: server.baseUrl, timeoutMs: 1000 });
    const started = performance.now();
    const fetched = await slow.namespace(ns.name).fetch("s", 0, 0, { maxWaitMs: 2000 });
    assert.deepEqual(fetched.records, []);
    assert.ok(performance.now() - started >= 1900);
  });

  test("getStream describes partitions and retention", async () => {
    const ns = await freshNamespace();
    const id = await ns.createStream("s", 3, { maxAgeMs: 60_000 });
    await ns.produce("s", 2, [{ value: "x" }]);
    const info = await ns.getStream("s");
    assert.equal(info.id, id);
    assert.deepEqual(
      info.partitions.map((p) => [p.partition, p.logStartOffset, p.highWatermark]),
      [
        [0, 0, 0],
        [1, 0, 0],
        [2, 0, 1],
      ],
    );
    assert.equal(info.maxAgeMs, 60_000);
  });

  test("a missing stream throws NotFoundError", async () => {
    const ns = await freshNamespace();
    await assert.rejects(ns.getStream("nope"), NotFoundError);
  });
});

describe("wire shapes", () => {
  test("produce sends the W4 body to a percent-encoded path", async () => {
    const script = new Script(
      json(
        200,
        { base_offset: 4, last_offset: 5, token: [] },
        { "operon-consistency-token": "v1:s1/p0@6" },
      ),
    );
    const client = new OperonClient({
      baseUrl: "http://operon.test/",
      fetch: script.fetch,
      headers: { "x-extra": "1" },
    });
    const result = await client.namespace("a b/c").produce("s!", 0, [
      {
        key: new Uint8Array([1, 2]),
        value: "é",
        headers: [
          ["h", null],
          ["i", new Uint8Array([3])],
        ],
        timestampMs: 5,
      },
      {},
    ]);
    const request = script.requests[0];
    assert.equal(request?.method, "POST");
    assert.equal(
      request?.url,
      "http://operon.test/v1/namespaces/a%20b%2Fc/streams/s%21/partitions/0/records",
    );
    assert.equal(request?.headers.get("content-type"), "application/json");
    assert.equal(request?.headers.get("x-extra"), "1");
    assert.equal(
      script.bodies[0],
      '{"records":[{"key":"AQI=","value":"w6k=","headers":[{"key":"h"},{"key":"i","value":"Aw=="}],"timestamp_ms":5},{}]}',
    );
    assert.equal(result.baseOffset, 4);
    assert.equal(result.token.toString(), "v1:s1/p0@6");
  });

  test("the header token wins over the body", async () => {
    const script = new Script(
      json(
        200,
        { base_offset: 0, last_offset: 0, token: [{ stream: 9, partition: 0, offset: 0 }] },
        { "operon-consistency-token": "v1:s1/p0@6" },
      ),
    );
    const result = await new OperonClient({ fetch: script.fetch })
      .namespace("n")
      .produce("s", 0, [{ value: "v" }]);
    assert.equal(result.token.toString(), "v1:s1/p0@6");
  });

  test("without the header the token is built from the body, next offset", async () => {
    // Raw text: a JS number literal this large would already have lost precision.
    const text =
      '{"base_offset":0,"last_offset":0,"token":[{"stream":18446744073709551615,"partition":2,"offset":18446744073709551614}]}';
    const script = new Script(
      new Response(text, { headers: { "content-type": "application/json" } }),
    );
    const result = await new OperonClient({ fetch: script.fetch })
      .namespace("n")
      .produce("s", 2, [{ value: "v" }]);
    assert.deepEqual(result.token.items, [[18446744073709551615n, 2, 18446744073709551615n]]);
    assert.ok(result.token instanceof ConsistencyToken);
  });

  test("create requests carry their W1 and W2 bodies", async () => {
    const script = new Script(json(201, { id: 3 }), json(201, { id: 4 }), json(201, { id: 5 }));
    const client = new OperonClient({ fetch: script.fetch });
    assert.equal(await client.createNamespace("n"), 3);
    assert.equal(await client.namespace("n").createStream("s", 2), 4);
    assert.equal(
      await client.namespace("n").createStream("t", 1, { maxAgeMs: 10, maxBytes: 20 }),
      5,
    );
    assert.equal(script.requests[0]?.url, "http://127.0.0.1:8080/v1/namespaces");
    assert.deepEqual(script.bodies, [
      '{"name":"n"}',
      '{"name":"s","partitions":2}',
      '{"name":"t","partitions":1,"retention":{"max_age_ms":10,"max_bytes":20}}',
    ]);
  });

  test("existOk re-throws a conflict without an id", async () => {
    const script = new Script(json(409, { error: "already_exists", message: "m" }));
    await assert.rejects(
      new OperonClient({ fetch: script.fetch }).createNamespace("n", { existOk: true }),
      AlreadyExistsError,
    );
  });

  test("fetch sends its query and extends the timeout by maxWaitMs", async () => {
    const script = new Script(async (request) => {
      await new Promise((resolve) => setTimeout(resolve, 100));
      assert.equal(request.signal.aborted, false);
      return json(200, { records: [], next_offset: 7, high_watermark: 7, log_start_offset: 0 });
    });
    const client = new OperonClient({ fetch: script.fetch, timeoutMs: 20 });
    const result = await client.namespace("n").fetch("s", 1, 7, { maxBytes: 1024, maxWaitMs: 500 });
    assert.equal(result.nextOffset, 7);
    const url = new URL(script.requests[0]?.url ?? "");
    assert.equal(url.pathname, "/v1/namespaces/n/streams/s/partitions/1/records");
    assert.equal(url.search, "?offset=7&max_bytes=1024&max_wait_ms=500");
    assert.equal(script.requests[0]?.headers.get("content-type"), null);
  });

  test("a non-finite timestamp is refused before sending", async () => {
    const script = new Script();
    await assert.rejects(
      new OperonClient({ fetch: script.fetch })
        .namespace("n")
        .produce("s", 0, [{ timestampMs: Number.NaN }]),
      RangeError,
    );
    assert.equal(script.requests.length, 0);
  });

  test("partitions and offsets must be non-negative integers", async () => {
    const script = new Script();
    const ns = new OperonClient({ fetch: script.fetch }).namespace("n");
    await assert.rejects(ns.produce("s", -1, [{ value: "v" }]), RangeError);
    await assert.rejects(ns.fetch("s", 0, 1.5), RangeError);
    assert.equal(script.requests.length, 0);
  });
});

describe("ids", () => {
  test("a bool is not an id", () => {
    assert.throws(() => encodeId(true as never), TypeError);
    assert.throws(() => encodeId(null as never), TypeError);
    assert.throws(() => encodeId(2n ** 64n), RangeError);
    assert.throws(() => encodeId(-1n), RangeError);
    assert.throws(() => encodeId(2 ** 53), RangeError);
    assert.throws(() => encodeId(1.5), RangeError);
    assert.throws(() => encodeId(-1), RangeError);
    assert.throws(() => encodeId({ uuid: "nope" }), RangeError);
  });

  test("ids encode and decode", () => {
    assert.equal(encodeId(7), 7);
    assert.equal(encodeId(2n ** 64n - 1n), 18446744073709551615n);
    assert.equal(encodeId("7"), "7");
    const uuid = "0189F7E2-3C4D-7A8B-9C0D-1E2F3A4B5C6D";
    assert.deepEqual(encodeId({ uuid }), { uuid: uuid.toLowerCase() });
    assert.equal(decodeId(7), 7);
    assert.equal(decodeId(18446744073709551615n), 18446744073709551615n);
    assert.equal(decodeId(5n), 5);
    assert.equal(decodeId("x"), "x");
    assert.deepEqual(decodeId({ uuid: "0189f7e2-3c4d-7a8b-9c0d-1e2f3a4b5c6d" }), {
      uuid: "0189f7e2-3c4d-7a8b-9c0d-1e2f3a4b5c6d",
    });
    assert.throws(() => decodeId(true), TypeError);
    assert.throws(() => decodeId(1.5), TypeError);
  });
});
