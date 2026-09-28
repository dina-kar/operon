import assert from "node:assert/strict";
import { test } from "node:test";

import {
  AlreadyExistsError,
  ConflictError,
  InternalError,
  InvalidArgumentError,
  NotFoundError,
  OffsetOutOfRangeError,
  OperonClient,
  OperonError,
  OperonTimeoutError,
  ResourceExhaustedError,
  SchemaViolationError,
  UnavailableError,
} from "../dist/index.js";
import { json, Script } from "./operon.ts";

function client(script: Script): OperonClient {
  return new OperonClient({
    baseUrl: "http://operon.test",
    fetch: script.fetch,
    maxRetries: 0,
    sleep: async () => {},
  });
}

async function errorFor(response: Response): Promise<unknown> {
  const script = new Script(response);
  try {
    await client(script).namespace("n").getStream("s");
  } catch (error) {
    return error;
  }
  assert.fail("expected an error");
}

const CODES: Array<[string, number, new (...args: never[]) => OperonError, object]> = [
  ["invalid_argument", 400, InvalidArgumentError, {}],
  ["schema_violation", 400, SchemaViolationError, { field: "n" }],
  ["not_found", 404, NotFoundError, { kind: "stream", name: "s" }],
  ["already_exists", 409, AlreadyExistsError, { id: 7 }],
  ["conflict", 409, ConflictError, {}],
  [
    "offset_out_of_range",
    416,
    OffsetOutOfRangeError,
    { offset: 9, log_start_offset: 0, high_watermark: 3 },
  ],
  ["resource_exhausted", 429, ResourceExhaustedError, { retry_after_ms: 250 }],
  ["internal", 500, InternalError, {}],
  ["unavailable", 503, UnavailableError, {}],
  ["timeout", 504, OperonTimeoutError, {}],
];

test("error codes map to typed errors", async () => {
  for (const [code, status, cls, extras] of CODES) {
    const error = await errorFor(json(status, { error: code, message: `m-${code}`, ...extras }));
    assert.ok(error instanceof cls, code);
    assert.ok(error instanceof OperonError, code);
    assert.equal(error.code, code);
    assert.equal(error.status, status);
    assert.equal(error.message, `m-${code}`);
    assert.equal(error.name, cls.name);
    assert.equal(error.retryable, status === 503);
    assert.deepEqual(error.body, { error: code, message: `m-${code}`, ...extras });
  }
});

test("error extras are attributes", async () => {
  const range = await errorFor(
    json(416, {
      error: "offset_out_of_range",
      message: "m",
      offset: 9,
      log_start_offset: 1,
      high_watermark: 3,
    }),
  );
  assert.ok(range instanceof OffsetOutOfRangeError);
  assert.equal(range.offset, 9);
  assert.equal(range.logStartOffset, 1);
  assert.equal(range.highWatermark, 3);

  const schema = await errorFor(json(400, { error: "schema_violation", message: "m", field: "n" }));
  assert.ok(schema instanceof SchemaViolationError);
  assert.ok(schema instanceof InvalidArgumentError);
  assert.equal(schema.field, "n");

  const exists = await errorFor(json(409, { error: "already_exists", message: "m", id: 7 }));
  assert.ok(exists instanceof AlreadyExistsError);
  assert.equal(exists.id, 7);

  const noId = await errorFor(json(409, { error: "already_exists", message: "m" }));
  assert.ok(noId instanceof AlreadyExistsError);
  assert.equal(noId.id, undefined);

  const busy = await errorFor(
    json(429, { error: "resource_exhausted", message: "m", retry_after_ms: 250 }),
  );
  assert.ok(busy instanceof ResourceExhaustedError);
  assert.equal(busy.retryAfterMs, 250);
});

test("unknown error code keeps its code and maps by status", async () => {
  const cases: Array<[number, new (...args: never[]) => OperonError]> = [
    [400, InvalidArgumentError],
    [404, NotFoundError],
    [409, ConflictError],
    [429, ResourceExhaustedError],
    [503, UnavailableError],
    [504, OperonTimeoutError],
    [500, InternalError],
    [502, InternalError],
    [418, OperonError],
  ];
  for (const [status, cls] of cases) {
    const error = await errorFor(json(status, { error: "busy", message: "m" }));
    assert.ok(error instanceof cls, String(status));
    assert.equal((error as OperonError).code, "busy");
    assert.equal((error as OperonError).status, status);
  }
  const plain = await errorFor(json(418, { error: "busy", message: "m" }));
  assert.equal((plain as OperonError).constructor, OperonError);
});

test("offset_out_of_range without its extras is a generic error", async () => {
  for (const code of ["offset_out_of_range", "weird"]) {
    const error = await errorFor(json(416, { error: code, message: "m", offset: 9 }));
    assert.equal((error as OperonError).constructor, OperonError, code);
    assert.equal((error as OperonError).code, code);
  }
});

test("non-JSON error body gives a generic error", async () => {
  const html = `<html>${"x".repeat(500)}</html>`;
  const error = await errorFor(
    new Response(html, { status: 502, headers: { "content-type": "text/html" } }),
  );
  assert.equal((error as OperonError).constructor, OperonError);
  assert.equal((error as OperonError).code, "http_502");
  assert.equal((error as OperonError).status, 502);
  assert.equal((error as OperonError).message, html.slice(0, 200));
});

test("a JSON body without a string error is treated as non-JSON", async () => {
  const error = await errorFor(json(500, { error: 5, message: "m" }));
  assert.equal((error as OperonError).constructor, OperonError);
  assert.equal((error as OperonError).code, "http_500");
});

test("a missing message falls back to the code", async () => {
  const error = await errorFor(json(404, { error: "not_found" }));
  assert.ok(error instanceof NotFoundError);
  assert.equal(error.message, "not_found");
});
