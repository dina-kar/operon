// Ruling 5: which failures are retried, and how long the client waits.
import assert from "node:assert/strict";
import { test } from "node:test";

import {
  type ClientOptions,
  NotFoundError,
  OperonClient,
  TransportError,
  UnavailableError,
} from "../dist/index.js";
import { Transport } from "../dist/transport.js";
import { json, Script, unavailable } from "./operon.ts";

const STREAM = {
  id: 1,
  partitions: [{ partition: 0, log_start_offset: 0, high_watermark: 0 }],
  retention: { max_age_ms: null, max_bytes: null },
};
const PRODUCED = {
  base_offset: 0,
  last_offset: 0,
  token: [{ stream: 1, partition: 0, offset: 0 }],
};

function options(script: Script, delays: number[], extra: ClientOptions = {}): ClientOptions {
  return {
    baseUrl: "http://operon.test",
    fetch: script.fetch,
    sleep: async (ms) => {
      delays.push(ms);
    },
    random: () => 1,
    ...extra,
  };
}

function client(script: Script, delays: number[], extra: ClientOptions = {}): OperonClient {
  return new OperonClient(options(script, delays, extra));
}

test("a 503 is retried then succeeds", async () => {
  const script = new Script(unavailable(), unavailable(), json(200, STREAM));
  const delays: number[] = [];
  const info = await client(script, delays).namespace("n").getStream("s");
  assert.equal(info.id, 1);
  assert.equal(script.requests.length, 3);
  assert.deepEqual(delays, [100, 200]);
});

test("backoff is capped and jittered", async () => {
  const script = new Script(...Array.from({ length: 6 }, () => unavailable()), json(200, STREAM));
  const delays: number[] = [];
  await client(script, delays, { maxRetries: 6, random: () => 0 })
    .namespace("n")
    .getStream("s");
  assert.deepEqual(delays, [50, 100, 200, 400, 800, 1000]);
});

test("retries stop after maxRetries", async () => {
  const script = new Script(...Array.from({ length: 4 }, () => unavailable()));
  const delays: number[] = [];
  await assert.rejects(
    client(script, delays, { maxRetries: 3 }).namespace("n").getStream("s"),
    UnavailableError,
  );
  assert.equal(script.requests.length, 4);
  assert.equal(delays.length, 3);
});

test("maxRetries 0 sends once", async () => {
  const script = new Script(unavailable());
  const delays: number[] = [];
  await assert.rejects(
    client(script, delays, { maxRetries: 0 }).namespace("n").getStream("s"),
    UnavailableError,
  );
  assert.equal(script.requests.length, 1);
});

test("invalid retry options are refused", () => {
  const script = new Script();
  assert.throws(() => client(script, [], { maxRetries: -1 }), RangeError);
  assert.throws(() => client(script, [], { maxRetries: 1.5 }), RangeError);
  assert.throws(() => client(script, [], { timeoutMs: 0 }), RangeError);
});

test("Retry-After is honoured", async () => {
  const script = new Script(unavailable("2"), json(200, STREAM));
  const delays: number[] = [];
  await client(script, delays).namespace("n").getStream("s");
  assert.deepEqual(delays, [2000]);
});

test("a Retry-After over 30 seconds is not waited for", async () => {
  const script = new Script(unavailable("31"), json(200, STREAM));
  const delays: number[] = [];
  await assert.rejects(client(script, delays).namespace("n").getStream("s"), UnavailableError);
  assert.equal(script.requests.length, 1);
  assert.deepEqual(delays, []);
});

test("a non-integer Retry-After falls back to the backoff", async () => {
  const script = new Script(unavailable("Wed, 21 Oct 2026 07:28:00 GMT"), json(200, STREAM));
  const delays: number[] = [];
  await client(script, delays).namespace("n").getStream("s");
  assert.deepEqual(delays, [100]);
});

test("produce is never retried", async () => {
  const script = new Script(unavailable(), json(200, PRODUCED));
  const delays: number[] = [];
  await assert.rejects(
    client(script, delays)
      .namespace("n")
      .produce("s", 0, [{ value: "v" }]),
    UnavailableError,
  );
  assert.equal(script.requests.length, 1);
  assert.deepEqual(delays, []);
});

test("a 503 on a collection write is retried", async () => {
  const written = { token: "v1:s1/p0@1", results: ["accepted"], positions: [null] };
  const script = new Script(unavailable(), json(200, written));
  const delays: number[] = [];
  const transport = new Transport(options(script, delays));
  const response = await transport.send({
    method: "POST",
    path: "/v1/namespaces/n/collections/c/documents",
    body: { ops: [{ delete: { id: 1 } }], report_existence: false },
    idempotent: true,
  });
  assert.equal(response.status, 200);
  assert.equal(script.requests.length, 2);
  assert.equal(
    new URL(script.requests[1]?.url ?? "").pathname,
    "/v1/namespaces/n/collections/c/documents",
  );
  assert.deepEqual(JSON.parse(script.bodies[1] ?? ""), {
    ops: [{ delete: { id: 1 } }],
    report_existence: false,
  });
});

test("a network error on produce is not retried", async () => {
  const cause = new TypeError("fetch failed");
  const script = new Script(cause, json(200, PRODUCED));
  const delays: number[] = [];
  await assert.rejects(
    client(script, delays)
      .namespace("n")
      .produce("s", 0, [{ value: "v" }]),
    (error: unknown) => {
      assert.ok(error instanceof TransportError);
      assert.equal(error.code, "transport");
      assert.equal(error.status, 0);
      assert.equal(error.cause, cause);
      return true;
    },
  );
  assert.equal(script.requests.length, 1);
});

test("a network error on a read is retried", async () => {
  const script = new Script(new TypeError("fetch failed"), json(200, STREAM));
  const delays: number[] = [];
  const info = await client(script, delays).namespace("n").getStream("s");
  assert.equal(info.id, 1);
  assert.equal(script.requests.length, 2);
  assert.deepEqual(delays, [100]);
});

test("the last network error is a TransportError", async () => {
  const script = new Script(new TypeError("a"), new TypeError("b"));
  const delays: number[] = [];
  await assert.rejects(
    client(script, delays, { maxRetries: 1 }).namespace("n").getStream("s"),
    (error: unknown) => error instanceof TransportError && (error.cause as Error).message === "b",
  );
  assert.equal(script.requests.length, 2);
});

test("an error that is not a network failure is not retried", async () => {
  const script = new Script(new SyntaxError("odd"), json(200, STREAM));
  await assert.rejects(client(script, []).namespace("n").getStream("s"), TransportError);
  assert.equal(script.requests.length, 1);
});

test("a 404 is not retried", async () => {
  const script = new Script(json(404, { error: "not_found", message: "no stream" }));
  await assert.rejects(client(script, []).namespace("n").getStream("s"), NotFoundError);
  assert.equal(script.requests.length, 1);
});

// A fetch that never answers until its signal aborts. The interval keeps the event loop
// alive, as a real socket would: AbortSignal.timeout timers are unref'd (Node 22 exits early).
function hang(request: Request): Promise<Response> {
  return new Promise((_, reject) => {
    const alive = setInterval(() => {}, 1000);
    const onAbort = (): void => {
      clearInterval(alive);
      reject(request.signal.reason);
    };
    if (request.signal.aborted) onAbort();
    else request.signal.addEventListener("abort", onAbort, { once: true });
  });
}

test("a timeout on produce is a TransportError and not retried", async () => {
  const script = new Script(hang, json(200, PRODUCED));
  await assert.rejects(
    client(script, [], { timeoutMs: 20 })
      .namespace("n")
      .produce("s", 0, [{ value: "v" }]),
    TransportError,
  );
  assert.equal(script.requests.length, 1);
});

test("a timeout on a read is retried", async () => {
  const script = new Script(hang, json(200, STREAM));
  const delays: number[] = [];
  const info = await client(script, delays, { timeoutMs: 20 }).namespace("n").getStream("s");
  assert.equal(info.id, 1);
  assert.equal(script.requests.length, 2);
});

test("an aborted request is not retried", async () => {
  const controller = new AbortController();
  const reason = new Error("caller gave up");
  const script = new Script(unavailable(), json(200, STREAM));
  const c = new OperonClient({
    baseUrl: "http://operon.test",
    fetch: script.fetch,
    random: () => 1,
    sleep: async () => {
      controller.abort(reason);
    },
  });
  await assert.rejects(
    c.namespace("n").getStream("s", { signal: controller.signal }),
    (error) => error === reason,
  );
  assert.equal(script.requests.length, 1);
});

test("an abort during a request rejects with the reason", async () => {
  const controller = new AbortController();
  const reason = new Error("stop");
  const script = new Script(
    (request) => {
      const pending = hang(request);
      controller.abort(reason);
      return pending;
    },
    json(200, STREAM),
  );
  await assert.rejects(
    client(script, []).namespace("n").getStream("s", { signal: controller.signal }),
    (error) => error === reason,
  );
  assert.equal(script.requests.length, 1);
});

test("an already aborted signal sends nothing", async () => {
  const controller = new AbortController();
  controller.abort(new Error("before"));
  const script = new Script(json(200, STREAM));
  await assert.rejects(
    client(script, []).namespace("n").getStream("s", { signal: controller.signal }),
    {
      message: "before",
    },
  );
  assert.equal(script.requests.length, 0);
});

test("the default sleep stops on abort", async () => {
  const controller = new AbortController();
  const script = new Script(unavailable("30"), json(200, STREAM));
  const c = new OperonClient({ baseUrl: "http://operon.test", fetch: script.fetch });
  const started = Date.now();
  setTimeout(() => controller.abort(new Error("late")), 50);
  await assert.rejects(c.namespace("n").getStream("s", { signal: controller.signal }), {
    message: "late",
  });
  assert.ok(Date.now() - started < 5000);
  assert.equal(script.requests.length, 1);
});
