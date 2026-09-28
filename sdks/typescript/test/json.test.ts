import assert from "node:assert/strict";
import { test } from "node:test";

import { decode, encode } from "../dist/json.js";

test("u64 integers above 2^53 decode as bigint", () => {
  const value = decode('{"id":18446744073709551615,"b":9007199254740993,"n":-9223372036854775808}');
  assert.deepEqual(value, {
    id: 18446744073709551615n,
    b: 9007199254740993n,
    n: -9223372036854775808n,
  });
});

test("safe integers stay numbers", () => {
  assert.deepEqual(decode('{"a":9007199254740991,"b":0,"c":-3,"d":[1,2]}'), {
    a: 9007199254740991,
    b: 0,
    c: -3,
    d: [1, 2],
  });
});

test("fractions and exponents stay numbers", () => {
  assert.deepEqual(decode('{"a":1.5,"b":1e300,"c":12345678901234567890.5}'), {
    a: 1.5,
    b: 1e300,
    c: Number("12345678901234567890.5"),
  });
});

test("bigint encodes as a raw JSON integer", () => {
  assert.equal(encode({ id: 18446744073709551615n }), '{"id":18446744073709551615}');
  assert.equal(encode([1n, 2]), "[1,2]");
});

test("NaN and Infinity are rejected", () => {
  assert.throws(() => encode({ v: [1, Number.NaN] }), RangeError);
  assert.throws(() => encode({ v: Number.POSITIVE_INFINITY }), RangeError);
  assert.throws(() => encode(Number.NEGATIVE_INFINITY), RangeError);
});

test("the error names the key path", () => {
  assert.throws(() => encode({ ops: [{ upsert: { vectors: { e: [0, Number.NaN] } } }] }), {
    name: "RangeError",
    message: /\$\.ops\[0\]\.upsert\.vectors\.e\[1\]/,
  });
});

test("encode is compact and keeps key order", () => {
  assert.equal(encode({ b: 1, a: [true, null, "x"] }), '{"b":1,"a":[true,null,"x"]}');
});

test("a round trip keeps u64 values exactly", () => {
  const text = '{"sort_values":[0.5,18446744073709551614]}';
  assert.equal(encode(decode(text)), text);
});
