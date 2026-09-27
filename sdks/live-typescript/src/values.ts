// Values on the wire (loam.live.v1.Value) and in TypeScript (R1 plan Task 14).
//
// `bigint` is int64 and `number` is double, so ids and counters stay lossless;
// `Uint8Array` is bytes. `undefined` is null at the top level and an object
// property holding it is left out, as the server's JavaScript does (row T13-14).
import { create } from "@bufbuild/protobuf";

import { type Value, ValueSchema } from "./gen/loam/live/v1/value_pb.js";

/** A Loam Live value. */
export type LiveValue =
  | null
  | bigint
  | number
  | boolean
  | string
  | Uint8Array
  | LiveValue[]
  | { [key: string]: LiveValue };

const INT64_MIN = -(2n ** 63n);
const INT64_MAX = 2n ** 63n - 1n;
/** Nesting deeper than this is refused, as on the server (row T13-14). */
export const MAX_DEPTH = 64;

/** `v` as a wire value; throws `TypeError` or `RangeError` for what has no wire form. */
export function toValue(v: LiveValue | undefined): Value {
  return encode(v, 0, "value");
}

function encode(v: unknown, depth: number, path: string): Value {
  if (depth > MAX_DEPTH) throw new RangeError(`${path}: nested deeper than ${MAX_DEPTH}`);
  if (v === null || v === undefined) {
    return create(ValueSchema, { kind: { case: "nullValue", value: {} } });
  }
  switch (typeof v) {
    case "bigint":
      if (v < INT64_MIN || v > INT64_MAX) throw new RangeError(`${path}: ${v} is outside int64`);
      return create(ValueSchema, { kind: { case: "int64Value", value: v } });
    case "number":
      return create(ValueSchema, { kind: { case: "doubleValue", value: v } });
    case "boolean":
      return create(ValueSchema, { kind: { case: "boolValue", value: v } });
    case "string":
      return create(ValueSchema, { kind: { case: "stringValue", value: v } });
    case "object":
      break;
    default:
      throw new TypeError(`${path}: a ${typeof v} has no Loam value`);
  }
  if (v instanceof Uint8Array) {
    return create(ValueSchema, { kind: { case: "bytesValue", value: v } });
  }
  if (Array.isArray(v)) {
    const values = v.map((item, i) => encode(item, depth + 1, `${path}[${i}]`));
    return create(ValueSchema, { kind: { case: "arrayValue", value: { values } } });
  }
  const proto = Object.getPrototypeOf(v);
  if (proto !== Object.prototype && proto !== null) {
    throw new TypeError(`${path}: only plain objects are Loam values`);
  }
  // The fields are set on the created message: `create` copies a map by
  // assignment, which would drop a `__proto__` key.
  const out = create(ValueSchema, { kind: { case: "objectValue", value: {} } });
  const fields = (out.kind.value as { fields: { [key: string]: Value } }).fields;
  for (const [key, item] of Object.entries(v)) {
    if (item === undefined) continue;
    setOwn(fields, key, encode(item, depth + 1, `${path}.${key}`));
  }
  return out;
}

/**
 * Sets `key` as an own enumerable property: plain assignment of `__proto__`
 * would call the prototype setter instead (review of #97).
 */
function setOwn<T>(target: { [key: string]: T }, key: string, value: T): void {
  Object.defineProperty(target, key, {
    value,
    enumerable: true,
    writable: true,
    configurable: true,
  });
}

/** The TypeScript form of a wire value; an absent value is `null`. */
export function fromValue(v: Value | undefined): LiveValue {
  if (v === undefined) return null;
  const kind = v.kind;
  switch (kind.case) {
    case undefined:
    case "nullValue":
      return null;
    case "int64Value":
    case "doubleValue":
    case "boolValue":
    case "stringValue":
    case "bytesValue":
      return kind.value;
    case "arrayValue":
      return kind.value.values.map(fromValue);
    case "objectValue": {
      const out: { [key: string]: LiveValue } = {};
      for (const [key, item] of Object.entries(kind.value.fields))
        setOwn(out, key, fromValue(item));
      return out;
    }
  }
}

/**
 * A string that is equal for equal values and different otherwise: object keys
 * are sorted, and each scalar carries its type, so `1n` and `1` differ. The
 * client uses it to share one query between identical `watch` calls.
 */
export function canonical(v: LiveValue | undefined): string {
  if (v === null || v === undefined) return "n";
  switch (typeof v) {
    case "bigint":
      return `i${v}`;
    case "number":
      return Object.is(v, -0) ? "d-0" : `d${v}`;
    case "boolean":
      return v ? "t" : "f";
    case "string":
      return `s${JSON.stringify(v)}`;
  }
  if (v instanceof Uint8Array)
    return `b${Array.from(v, (b) => b.toString(16).padStart(2, "0")).join("")}`;
  if (Array.isArray(v)) return `[${v.map(canonical).join(",")}]`;
  const keys = Object.keys(v)
    .filter((k) => v[k] !== undefined)
    .sort();
  return `{${keys.map((k) => `${JSON.stringify(k)}:${canonical(v[k])}`).join(",")}}`;
}
