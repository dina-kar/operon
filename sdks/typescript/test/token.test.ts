import assert from "node:assert/strict";
import { test } from "node:test";

import { ConsistencyToken } from "../dist/index.js";

test("round trips its text form", () => {
  const token = ConsistencyToken.parse("v1:s7/p3@918274,s7/p4@1");
  assert.deepEqual(token.items, [
    [7n, 3, 918274n],
    [7n, 4, 1n],
  ]);
  assert.equal(token.toString(), "v1:s7/p3@918274,s7/p4@1");
  assert.equal(String(token), "v1:s7/p3@918274,s7/p4@1");
});

test("an empty token has no items", () => {
  const token = ConsistencyToken.parse("v1:");
  assert.deepEqual(token.items, []);
  assert.equal(token.toString(), "v1:");
});

test("rejects malformed text", () => {
  for (const text of [
    "v2:s1/p0@1",
    "s1/p0@1",
    "v1:s7p3@1",
    "v1:s-1/p0@1",
    "v1:s7/p0@",
    "v1:s07/p0@1",
    "v1:s7/p4294967296@1",
    "v1:s7/p0@18446744073709551616",
    "v1:s1/p0@1,s1/p0@2",
    "v1:s1/p0@1,",
  ]) {
    assert.throws(() => ConsistencyToken.parse(text), RangeError, text);
  }
});

test("a non-string is a TypeError", () => {
  assert.throws(() => ConsistencyToken.parse(7 as unknown as string), TypeError);
});

test("items are sorted by stream then partition", () => {
  const token = ConsistencyToken.parse("v1:s10/p1@1,s2/p5@2,s10/p0@3");
  assert.equal(token.toString(), "v1:s2/p5@2,s10/p0@3,s10/p1@1");
});

test("merge keeps the highest offset per partition", () => {
  const a = ConsistencyToken.parse("v1:s1/p0@5,s1/p1@2");
  const b = ConsistencyToken.parse("v1:s1/p0@3,s2/p0@9");
  assert.equal(a.merge(b).toString(), "v1:s1/p0@5,s1/p1@2,s2/p0@9");
  assert.equal(a.merge().toString(), a.toString());
});

test("offsets above 2^53 are kept exactly", () => {
  const text = "v1:s18446744073709551615/p4294967295@18446744073709551615";
  const token = ConsistencyToken.parse(text);
  assert.deepEqual(token.items, [[18446744073709551615n, 4294967295, 18446744073709551615n]]);
  assert.equal(token.toString(), text);
});

test("items cannot be changed", () => {
  const token = ConsistencyToken.parse("v1:s1/p0@1");
  assert.ok(Object.isFrozen(token.items));
  assert.ok(Object.isFrozen(token.items[0]));
});
