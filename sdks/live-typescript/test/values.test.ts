// Values: int64 ↔ bigint losslessly, over binary and JSON (R1 plan Task 14).
import assert from "node:assert/strict";
import { test } from "node:test";

import { create, fromBinary, fromJson, toBinary, toJson } from "@bufbuild/protobuf";
import { Code, ConnectError } from "@connectrpc/connect";
import { ValueSchema } from "../dist/gen/loam/live/v1/value_pb.js";
import { canonical, fromValue, type LiveValue, pb, toLiveError, toValue } from "../dist/index.js";

const EXTREMES = [-(2n ** 63n), 2n ** 63n - 1n, 2n ** 53n + 1n, -(2n ** 53n) - 1n, 0n];

test("int64_roundtrip_is_lossless", () => {
  for (const n of EXTREMES) {
    const wire = toValue(n);
    assert.equal(wire.kind.case, "int64Value");
    assert.equal(fromValue(fromBinary(ValueSchema, toBinary(ValueSchema, wire))), n);
    const json = toJson(ValueSchema, wire);
    assert.deepEqual(json, { int64Value: n.toString() }, "int64 is a JSON string");
    assert.equal(fromValue(fromJson(ValueSchema, json)), n);
  }
  // A number stays a double, even when it is whole.
  assert.equal(toValue(5).kind.case, "doubleValue");
  assert.equal(fromValue(toValue(5)), 5);
  assert.throws(() => toValue(2n ** 63n), RangeError);
  assert.throws(() => toValue(-(2n ** 63n) - 1n), RangeError);
});

test("nested_values_roundtrip", () => {
  const v: LiveValue = {
    id: 2n ** 60n,
    score: -0.5,
    ok: true,
    name: "ü",
    blob: new Uint8Array([0, 255]),
    tags: ["a", null, 1n],
    nested: { deep: [{ x: 1 }] },
  };
  const back = fromValue(fromBinary(ValueSchema, toBinary(ValueSchema, toValue(v))));
  assert.deepEqual(back, v);
  // `undefined` properties are left out, as on the server.
  assert.deepEqual(fromValue(toValue({ a: 1n, b: undefined } as unknown as LiveValue)), { a: 1n });
  assert.throws(() => toValue((() => 1) as unknown as LiveValue), TypeError);
  assert.throws(() => toValue(new Date() as unknown as LiveValue), TypeError);
});

// Review of #97: a key named __proto__ is an own property on both sides of
// this layer and never changes a prototype. (protobuf-es's own `fromBinary`
// and `toJson` drop such a map key; that is upstream, row T16 carries it.)
test("proto_keys_stay_own_properties", () => {
  const v = JSON.parse('{"__proto__": {"x": 1}, "a": 2}') as LiveValue;
  const wire = toValue(v);
  assert.equal(wire.kind.case, "objectValue");
  const fields = (wire.kind.value as { fields: Record<string, unknown> }).fields;
  assert.deepEqual(Object.keys(fields).sort(), ["__proto__", "a"]);
  assert.ok(
    toBinary(ValueSchema, wire).length > toBinary(ValueSchema, toValue({ a: 2 })).length,
    "the key is on the wire",
  );
  const back = fromValue(wire) as Record<string, LiveValue>;
  assert.equal(Object.getPrototypeOf(back), Object.prototype);
  assert.ok(Object.hasOwn(back, "__proto__"));
  assert.deepEqual(Object.keys(back).sort(), ["__proto__", "a"]);
  assert.deepEqual(Object.getOwnPropertyDescriptor(back, "__proto__")?.value, { x: 1 });
  assert.equal(canonical(back), canonical(v));
});

test("canonical_keys_ignore_key_order_and_keep_types", () => {
  assert.equal(canonical({ a: 1n, b: [true] }), canonical({ b: [true], a: 1n }));
  assert.notEqual(canonical(1n), canonical(1));
  assert.notEqual(canonical("1"), canonical(1));
  assert.notEqual(canonical(null), canonical("n"));
});

test("errors_carry_wire_codes", () => {
  assert.equal(pb.ErrorCode.UNAVAILABLE, 8);
});

// Owner ruling on row T14-12: the server attaches the LiveError as a typed
// Connect detail, and the client reads its code instead of the message.
test("errors_read_the_live_error_detail", () => {
  const detail = (code: pb.ErrorCode, message: string) => ({
    desc: pb.LiveErrorSchema,
    value: { code, message },
  });
  // The detail tells FUNCTION_OUT_OF_MEMORY from a limit, whatever the message says.
  const oom = new ConnectError("the call failed", Code.ResourceExhausted, undefined, [
    detail(pb.ErrorCode.FUNCTION_OUT_OF_MEMORY, "function out of memory: 64 MiB"),
  ]);
  assert.equal(toLiveError(oom).code, "FUNCTION_OUT_OF_MEMORY");
  const limit = new ConnectError("out of memory? no, a limit", Code.ResourceExhausted, undefined, [
    detail(pb.ErrorCode.RESOURCE_EXHAUSTED, "limit max_written_docs exceeded"),
  ]);
  assert.equal(toLiveError(limit).code, "RESOURCE_EXHAUSTED");
  // Across the binary wire form too (as a server sends it).
  const wire = new ConnectError("x", Code.Unknown);
  wire.details.push({
    type: pb.LiveErrorSchema.typeName,
    value: toBinary(
      pb.LiveErrorSchema,
      create(pb.LiveErrorSchema, { code: pb.ErrorCode.FUNCTION_TIMEOUT, message: "1 s" }),
    ),
  });
  assert.equal(toLiveError(wire).code, "FUNCTION_TIMEOUT");
  // An unusable first detail does not hide a usable later one (review of #101).
  const second = new ConnectError("x", Code.Internal, undefined, [
    detail(pb.ErrorCode.UNSPECIFIED, "unspecified"),
    detail(pb.ErrorCode.UNAVAILABLE, "busy"),
  ]);
  assert.equal(toLiveError(second).code, "UNAVAILABLE");
  assert.equal(toLiveError(second).retryable, true);
  // Without a detail (an older server) the Connect code is mapped as before.
  assert.equal(toLiveError(new ConnectError("busy", Code.Unavailable)).code, "UNAVAILABLE");
  assert.equal(
    toLiveError(new ConnectError("function out of memory: x", Code.ResourceExhausted)).code,
    "FUNCTION_OUT_OF_MEMORY",
  );
});
