/**
 * The one module that knows the native REST API's wire JSON (plan M1.6, rows W1–W5 here).
 *
 * Everything here is pure: request builders return a `WireRequest`, parsers take
 * a `WireResponse`. The transport sends them and maps errors.
 */
import { decodeBase64, encodeBase64 } from "./base64.js";
import { errorFromBody, OperonError } from "./errors.js";
import { decode } from "./json.js";
import { ConsistencyToken, type TokenItem } from "./token.js";
import type {
  FetchedRecord,
  FetchResult,
  Id,
  ProduceRecord,
  ProduceResult,
  StreamInfo,
} from "./types.js";

/** The response and read-request header that carries a consistency token. */
export const TOKEN_HEADER = "operon-consistency-token";

const U64_MAX = 2n ** 64n - 1n;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

/** One API call, before it is sent. */
export interface WireRequest {
  method: "GET" | "POST" | "DELETE";
  /** Starts with `/`; names are already escaped with `seg`. */
  path: string;
  query?: Record<string, string | number>;
  headers?: Record<string, string>;
  /** Encoded with `json.encode`; absent means no body. */
  body?: unknown;
  /** Whether resending after an unknown outcome is safe (Ruling 5): false only for produce. */
  idempotent: boolean;
  /** Milliseconds added to the client's timeout for this request (a long poll's wait). */
  extraTimeoutMs?: number;
}

/** A successful answer: status, headers and the raw body text. */
export interface WireResponse {
  status: number;
  headers: Headers;
  text: string;
}

type JsonObject = Record<string, unknown>;

/** A path segment: every reserved character percent-encoded (as Python's `quote(safe="")`). */
export function seg(name: string): string {
  if (typeof name !== "string") throw new TypeError(`a name is a string, not ${typeof name}`);
  return encodeURIComponent(name).replace(
    /[!'()*]/g,
    (c) => `%${c.charCodeAt(0).toString(16).toUpperCase()}`,
  );
}

function object(value: unknown, what: string): JsonObject {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new TypeError(`expected a JSON object for ${what}`);
  }
  return value as JsonObject;
}

function array(value: unknown, what: string): unknown[] {
  if (value === undefined || value === null) return [];
  if (!Array.isArray(value)) throw new TypeError(`expected a JSON array for ${what}`);
  return value;
}

/** A JSON integer as a `number`; a value above 2^53 − 1 cannot be one and throws. */
export function int(value: unknown, what: string): number {
  if (typeof value === "number" && Number.isInteger(value)) return value;
  if (typeof value === "bigint") {
    if (value >= BigInt(Number.MIN_SAFE_INTEGER) && value <= BigInt(Number.MAX_SAFE_INTEGER))
      return Number(value);
    throw new RangeError(`${what} ${value} is outside the safe integer range`);
  }
  throw new TypeError(`expected an integer for ${what}, got ${String(value)}`);
}

function intOrNull(value: unknown, what: string): number | null {
  return value === undefined || value === null ? null : int(value, what);
}

function bigint(value: unknown, what: string): bigint {
  if (typeof value === "bigint") return value;
  if (typeof value === "number" && Number.isInteger(value)) return BigInt(value);
  throw new TypeError(`expected an integer for ${what}, got ${String(value)}`);
}

/** A caller's integer argument: a non-negative safe integer. */
export function index(value: number, what: string): number {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new RangeError(`${what} must be a non-negative integer, got ${String(value)}`);
  }
  return value;
}

export function parseJson(response: WireResponse): unknown {
  return decode(response.text);
}

// ---------------------------------------------------------------- ids

/** A document id's JSON form: a u64, a string, or `{"uuid": …}` (lower-cased). */
export function encodeId(value: Id): number | bigint | string | { uuid: string } {
  if (typeof value === "number") {
    if (!Number.isSafeInteger(value) || value < 0) {
      throw new RangeError(
        `a number id must be a non-negative safe integer (use a bigint for ids above 2^53 − 1), got ${value}`,
      );
    }
    return value;
  }
  if (typeof value === "bigint") {
    if (value < 0n || value > U64_MAX)
      throw new RangeError(`a bigint id must be in 0..2^64 − 1, got ${value}`);
    return value;
  }
  if (typeof value === "string") return value;
  if (typeof value === "object" && value !== null && typeof value.uuid === "string") {
    const uuid = value.uuid.toLowerCase();
    if (!UUID.test(uuid))
      throw new RangeError(`not a hyphenated UUID: ${JSON.stringify(value.uuid)}`);
    return { uuid };
  }
  throw new TypeError(
    `a document id is a number, bigint, string or {uuid}, not ${value === null ? "null" : typeof value}`,
  );
}

/** The inverse of `encodeId`: a `number` when safe, a `bigint` above 2^53 − 1. */
export function decodeId(value: unknown): Id {
  if (typeof value === "number" && Number.isSafeInteger(value) && value >= 0) return value;
  if (typeof value === "bigint" && value >= 0n && value <= U64_MAX) {
    return value <= BigInt(Number.MAX_SAFE_INTEGER) ? Number(value) : value;
  }
  if (typeof value === "string") return value;
  if (typeof value === "object" && value !== null && !Array.isArray(value)) {
    const keys = Object.keys(value);
    const uuid = (value as JsonObject).uuid;
    if (keys.length === 1 && typeof uuid === "string") return { uuid };
  }
  throw new TypeError(`not a document id: ${String(value)}`);
}

// ---------------------------------------------------------------- errors

/** `Retry-After` as an integer number of seconds, if it is one. */
export function retryAfterSeconds(headers: Headers): number | null {
  const raw = headers.get("retry-after")?.trim();
  if (raw === undefined || !/^\d+$/.test(raw)) return null;
  return Number(raw);
}

/** Maps a non-2xx answer to its typed error. */
export function errorFromResponse(status: number, text: string): OperonError {
  let body: unknown;
  try {
    body = decode(text);
  } catch {
    body = undefined;
  }
  if (
    typeof body === "object" &&
    body !== null &&
    !Array.isArray(body) &&
    typeof (body as JsonObject).error === "string"
  ) {
    return errorFromBody(status, body as JsonObject & { error: string });
  }
  return new OperonError(text.slice(0, 200), { code: `http_${status}`, status, body: {} });
}

// ---------------------------------------------------------------- namespaces (W1)

export function createNamespace(name: string): WireRequest {
  return { method: "POST", path: "/v1/namespaces", body: { name }, idempotent: true };
}

export function parseCreatedId(response: WireResponse): number {
  return int(object(parseJson(response), "a create answer").id, "id");
}

// ---------------------------------------------------------------- streams (W2–W5)

function streamPath(ns: string, stream: string): string {
  return `/v1/namespaces/${seg(ns)}/streams/${seg(stream)}`;
}

export function createStream(
  ns: string,
  name: string,
  partitions: number,
  maxAgeMs: number | undefined,
  maxBytes: number | undefined,
): WireRequest {
  const body: JsonObject = { name, partitions: index(partitions, "partitions") };
  if (maxAgeMs !== undefined || maxBytes !== undefined) {
    const retention: JsonObject = {};
    if (maxAgeMs !== undefined) retention.max_age_ms = index(maxAgeMs, "maxAgeMs");
    if (maxBytes !== undefined) retention.max_bytes = index(maxBytes, "maxBytes");
    body.retention = retention;
  }
  return { method: "POST", path: `/v1/namespaces/${seg(ns)}/streams`, body, idempotent: true };
}

export function getStream(ns: string, stream: string): WireRequest {
  return { method: "GET", path: streamPath(ns, stream), idempotent: true };
}

export function parseStreamInfo(response: WireResponse): StreamInfo {
  const body = object(parseJson(response), "a stream");
  const retention =
    body.retention === undefined || body.retention === null
      ? {}
      : object(body.retention, "retention");
  return {
    id: int(body.id, "id"),
    partitions: array(body.partitions, "partitions").map((raw) => {
      const p = object(raw, "a partition");
      return {
        partition: int(p.partition, "partition"),
        logStartOffset: int(p.log_start_offset, "log_start_offset"),
        highWatermark: int(p.high_watermark, "high_watermark"),
      };
    }),
    maxAgeMs: intOrNull(retention.max_age_ms, "max_age_ms"),
    maxBytes: intOrNull(retention.max_bytes, "max_bytes"),
  };
}

function encodeRecord(record: ProduceRecord): JsonObject {
  if (typeof record !== "object" || record === null) {
    throw new TypeError("a record is an object {key?, value?, headers?, timestampMs?}");
  }
  const out: JsonObject = {};
  if (record.key !== undefined) out.key = encodeBase64(record.key);
  if (record.value !== undefined) out.value = encodeBase64(record.value);
  if (record.headers !== undefined && record.headers.length > 0) {
    out.headers = record.headers.map(([key, value]) => {
      if (typeof key !== "string") throw new TypeError("a record header key is a string");
      return value === null || value === undefined ? { key } : { key, value: encodeBase64(value) };
    });
  }
  if (record.timestampMs !== undefined) out.timestamp_ms = record.timestampMs;
  return out;
}

/** W4. Never retried automatically: a 503 may already have been committed (Ruling 5). */
export function produce(
  ns: string,
  stream: string,
  partition: number,
  records: ProduceRecord[],
): WireRequest {
  return {
    method: "POST",
    path: `${streamPath(ns, stream)}/partitions/${index(partition, "partition")}/records`,
    body: { records: records.map(encodeRecord) },
    idempotent: false,
  };
}

export function tokenFromHeader(response: WireResponse): ConsistencyToken | null {
  const raw = response.headers.get(TOKEN_HEADER);
  return raw === null ? null : ConsistencyToken.parse(raw);
}

export function parseProduce(response: WireResponse): ProduceResult {
  const body = object(parseJson(response), "a produce answer");
  let token = tokenFromHeader(response);
  if (token === null) {
    // The body's offsets are the last written; a token names the next one.
    token = new ConsistencyToken(
      array(body.token, "token").map((raw): TokenItem => {
        const t = object(raw, "a token item");
        return [
          bigint(t.stream, "stream"),
          int(t.partition, "partition"),
          bigint(t.offset, "offset") + 1n,
        ];
      }),
    );
  }
  return {
    baseOffset: int(body.base_offset, "base_offset"),
    lastOffset: int(body.last_offset, "last_offset"),
    token,
  };
}

export function fetchRecords(
  ns: string,
  stream: string,
  partition: number,
  offset: number,
  maxBytes: number | undefined,
  maxWaitMs: number | undefined,
): WireRequest {
  const query: Record<string, number> = { offset: index(offset, "offset") };
  if (maxBytes !== undefined) query.max_bytes = index(maxBytes, "maxBytes");
  if (maxWaitMs !== undefined) query.max_wait_ms = index(maxWaitMs, "maxWaitMs");
  return {
    method: "GET",
    path: `${streamPath(ns, stream)}/partitions/${index(partition, "partition")}/records`,
    query,
    idempotent: true,
    extraTimeoutMs: maxWaitMs ?? 0,
  };
}

function bytesOrNull(value: unknown, what: string): Uint8Array | null {
  if (value === undefined || value === null) return null;
  if (typeof value !== "string") throw new TypeError(`expected a base64 string for ${what}`);
  return decodeBase64(value);
}

export function parseFetch(response: WireResponse): FetchResult {
  const body = object(parseJson(response), "a fetch answer");
  return {
    records: array(body.records, "records").map((raw): FetchedRecord => {
      const r = object(raw, "a record");
      return {
        offset: int(r.offset, "offset"),
        key: bytesOrNull(r.key, "key"),
        value: bytesOrNull(r.value, "value"),
        headers: array(r.headers, "headers").map((rawHeader): [string, Uint8Array | null] => {
          const h = object(rawHeader, "a header");
          return [String(h.key), bytesOrNull(h.value, "header value")];
        }),
        timestampMs: int(r.timestamp_ms, "timestamp_ms"),
      };
    }),
    nextOffset: int(body.next_offset, "next_offset"),
    highWatermark: int(body.high_watermark, "high_watermark"),
    logStartOffset: int(body.log_start_offset, "log_start_offset"),
  };
}
