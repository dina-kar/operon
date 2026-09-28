// Values: int64 ↔ bigint losslessly, over binary and JSON (R1 plan Task 14).
import assert from "node:assert/strict";
import { test } from "node:test";

import { fromBinary, fromJson, toBinary, toJson } from "@bufbuild/protobuf";
import { ValueSchema } from "../dist/gen/loam/live/v1/value_pb.js";
import { canonical, fromValue, type LiveValue, pb, toValue } from "../dist/index.js";

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

test("canonical_keys_ignore_key_order_and_keep_types", () => {
  assert.equal(canonical({ a: 1n, b: [true] }), canonical({ b: [true], a: 1n }));
  assert.notEqual(canonical(1n), canonical(1));
  assert.notEqual(canonical("1"), canonical(1));
  assert.notEqual(canonical(null), canonical("n"));
});

test("errors_carry_wire_codes", () => {
  assert.equal(pb.ErrorCode.UNAVAILABLE, 8);
});
