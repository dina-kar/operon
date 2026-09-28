/**
 * JSON with lossless u64 integers (plan M1.6 Task 5 rule 2).
 *
 * `decode` turns an integer literal outside the safe range into a `bigint`,
 * using the reviver's source-text access; `encode` writes a `bigint` as a raw
 * JSON integer and refuses non-finite numbers, which `JSON.stringify` would
 * silently write as `null`.
 */

/** The third reviver argument (JSON.parse source text access). */
interface ReviverContext {
  source?: string;
}

type RawJson = (text: string) => unknown;

const INTEGER = /^-?\d+$/;

function rawJson(): RawJson {
  const raw = (JSON as unknown as { rawJSON?: RawJson }).rawJSON;
  if (typeof raw !== "function") {
    throw new TypeError("this runtime has no JSON.rawJSON: bigint values cannot be encoded");
  }
  return raw;
}

function childPath(parent: string, holder: unknown, key: string): string {
  return Array.isArray(holder) ? `${parent}[${key}]` : `${parent}.${key}`;
}

/** Compact JSON text; a non-finite number throws `RangeError` naming its key path. */
export function encode(value: unknown): string {
  const paths = new WeakMap<object, string>();
  const text = JSON.stringify(value, function (this: unknown, key: string, v: unknown): unknown {
    const parent = typeof this === "object" && this !== null ? paths.get(this) : undefined;
    const path = parent === undefined ? "$" : childPath(parent, this, key);
    if (typeof v === "number" && !Number.isFinite(v)) {
      throw new RangeError(`JSON has no ${v}: a non-finite number at ${path}`);
    }
    if (typeof v === "bigint") {
      return rawJson()(v.toString());
    }
    if (typeof v === "object" && v !== null) {
      paths.set(v, path);
    }
    return v;
  });
  if (text === undefined) {
    throw new TypeError("value has no JSON form");
  }
  return text;
}

/** Parses JSON; integers outside `Number.isSafeInteger` become `bigint`. */
export function decode(text: string): unknown {
  return JSON.parse(text, (_key: string, value: unknown, context?: ReviverContext): unknown => {
    if (
      typeof value === "number" &&
      !Number.isSafeInteger(value) &&
      context?.source !== undefined &&
      INTEGER.test(context.source)
    ) {
      return BigInt(context.source);
    }
    return value;
  });
}
