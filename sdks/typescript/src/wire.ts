/**
 * The one module that knows the native REST API's wire JSON (plan M1.6, rows W1–W13 and W15).
 *
 * It alone maps the SDK's camelCase shapes to the wire's snake_case keys and
 * tagged forms, and applies the encoder rules.
 *
 * Everything here is pure: request builders return a `WireRequest`, parsers take
 * a `WireResponse`. The transport sends them and maps errors.
 */
import { decodeBase64, encodeBase64 } from "./base64.js";
import { errorFromBody, OperonError } from "./errors.js";
import { decode } from "./json.js";
import {
  checkRequest,
  type FieldValue,
  type Fusion,
  type Projection,
  type Query,
  type Retriever,
  type SearchRequestInput,
  type SortKey,
  type TrackTotalHits,
} from "./query.js";
import type {
  Distance,
  Dynamic,
  FieldInput,
  Kind,
  SchemaInput,
  SparseModifier,
  SparseVectorFieldInput,
  VectorInput,
} from "./schema.js";
import { ConsistencyToken, type TokenItem } from "./token.js";
import type {
  CollectionInfo,
  Consistency,
  DocumentInput,
  FetchedRecord,
  FetchResult,
  Hit,
  Id,
  Op,
  Pin,
  ProduceRecord,
  ProduceResult,
  ScanAt,
  ScanColumn,
  ScanFragment,
  ScanPlan,
  SearchResponse,
  SparseVector,
  SqlResult,
  StoredDoc,
  StreamInfo,
  VectorLike,
  WriteResult,
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
  if (typeof value === "number" && Number.isInteger(value)) {
    // A literal such as `1e20` decodes as a number, already rounded.
    if (Number.isSafeInteger(value)) return value;
    throw new RangeError(`${what} ${value} is outside the safe integer range`);
  }
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

// ---------------------------------------------------------------- u64 versions

/** A caller's u64 (a manifest version): a safe non-negative `number` or a `bigint` in 0..2^64 − 1. */
export function encodeU64(value: number | bigint, what: string): number | bigint {
  if (typeof value === "number") {
    if (!Number.isSafeInteger(value) || value < 0) {
      throw new RangeError(
        `${what} must be a non-negative safe integer (use a bigint above 2^53 − 1), got ${value}`,
      );
    }
    return value;
  }
  if (typeof value === "bigint") {
    if (value < 0n || value > U64_MAX)
      throw new RangeError(`${what} must be in 0..2^64 − 1, got ${value}`);
    return value;
  }
  throw new TypeError(`${what} is a number or a bigint, not ${typeof value}`);
}

/** A u64 from the server: a `number` when safe, a `bigint` above 2^53 − 1. */
export function decodeU64(value: unknown, what: string): number | bigint {
  if (typeof value === "number" && Number.isSafeInteger(value) && value >= 0) return value;
  if (typeof value === "bigint" && value >= 0n && value <= U64_MAX) {
    return value <= BigInt(Number.MAX_SAFE_INTEGER) ? Number(value) : value;
  }
  throw new TypeError(`expected a u64 for ${what}, got ${String(value)}`);
}

// ---------------------------------------------------------------- vectors and field values

const U32_LIMIT = 2 ** 32;

function numbers(value: unknown, what: string): unknown[] {
  if (Array.isArray(value)) return value;
  if (value instanceof Float32Array || value instanceof Float64Array) return Array.from(value);
  throw new TypeError(`${what} must be an array of numbers or a Float32Array/Float64Array`);
}

/** A dense vector: finite numbers only (a `Float32Array` sends its float32 values). */
export function encodeVector(name: string, value: VectorLike): number[] {
  const what = `vector ${JSON.stringify(name)}`;
  return numbers(value, what).map((element, i) => {
    if (typeof element !== "number") {
      throw new TypeError(`${what} holds a non-number at [${i}]: ${String(element)}`);
    }
    if (!Number.isFinite(element)) {
      throw new RangeError(`${what} holds a non-finite value at [${i}]: ${element}`);
    }
    return element;
  });
}

/** Checks a sparse vector (Task 3 rule 4) and returns plain copies of its arrays. */
export function checkSparse(value: SparseVector): SparseVector {
  if (typeof value !== "object" || value === null) {
    throw new TypeError("a sparse vector is {indices, values}");
  }
  const indices = numbers(value.indices, "a sparse vector's indices");
  const values = numbers(value.values, "a sparse vector's values");
  if (indices.length !== values.length) {
    throw new RangeError(
      `a sparse vector has ${indices.length} indices but ${values.length} values`,
    );
  }
  const seen = new Set<number>();
  for (const index of indices) {
    if (typeof index !== "number" || !Number.isInteger(index) || index < 0 || index >= U32_LIMIT) {
      throw new RangeError(
        `a sparse vector index must be an integer in 0..2^32 − 1, got ${String(index)}`,
      );
    }
    if (seen.has(index)) {
      throw new RangeError(`a sparse vector's indices must be unique: ${index} repeats`);
    }
    seen.add(index);
  }
  for (const v of values) {
    if (typeof v !== "number" || !Number.isFinite(v)) {
      throw new RangeError(`a sparse vector value must be a finite number, got ${String(v)}`);
    }
  }
  return { indices: indices as number[], values: values as number[] };
}

function decodeSparse(value: unknown, what: string): SparseVector {
  const v = object(value, what);
  return {
    indices: array(v.indices, `${what}.indices`).map((i) => int(i, `${what} index`)),
    values: array(v.values, `${what}.values`).map(Number),
  };
}

const I64_MIN = -(2n ** 63n);

/** RFC 3339 in UTC; whole seconds drop the `.000` (so `queries.json`'s `range_dates` matches). */
export function encodeDate(value: Date): string {
  const ms = value.getTime();
  if (Number.isNaN(ms)) throw new RangeError("an invalid Date is not a field value");
  const year = value.getUTCFullYear();
  if (year < 0 || year > 9999)
    throw new RangeError(`a date's year must be in 0..9999, got ${year}`);
  const text = value.toISOString();
  return text.endsWith(".000Z") ? `${text.slice(0, -5)}Z` : text;
}

/** A `FieldValue` (row E10): dates as `{"date": RFC 3339}`, integers in −2^63..2^64 − 1. */
export function encodeFieldValue(value: FieldValue): unknown {
  if (typeof value === "string" || typeof value === "boolean") return value;
  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw new RangeError(`a field value must be finite, got ${value}`);
    if (Number.isInteger(value) && !Number.isSafeInteger(value)) {
      throw new RangeError(`the integer ${value} is not exact as a number: pass a bigint`);
    }
    return value;
  }
  if (typeof value === "bigint") {
    if (value < I64_MIN || value > U64_MAX) {
      throw new RangeError(`an integer field value must be in -2^63..2^64 − 1, got ${value}`);
    }
    return value;
  }
  if (value instanceof Date) return { date: encodeDate(value) };
  throw new TypeError(`not a field value: ${String(value)}`);
}

function optionalValue(value: FieldValue | undefined): unknown {
  return value === undefined ? null : encodeFieldValue(value);
}

// ---------------------------------------------------------------- queries

function queries(values: Query[] | undefined): unknown[] {
  return (values ?? []).map(encodeQuery);
}

/** A `Query` in the wire form (the plan's wire contract). */
export function encodeQuery(query: Query): unknown {
  if (typeof query !== "object" || query === null)
    throw new TypeError(`not a query: ${String(query)}`);
  if ("matchAll" in query) return "match_all";
  if ("matchNone" in query) return "match_none";
  if ("match" in query) {
    const m = query.match;
    return {
      match: {
        field: m.field,
        text: m.text,
        operator: m.operator ?? "or",
        minimum_should_match: m.minimumShouldMatch ?? null,
        fuzziness: m.fuzziness ?? null,
        analyzer: m.analyzer ?? null,
      },
    };
  }
  if ("matchPhrase" in query) {
    const m = query.matchPhrase;
    return { match_phrase: { field: m.field, text: m.text, slop: m.slop ?? 0 } };
  }
  if ("multiMatch" in query) {
    const m = query.multiMatch;
    return {
      multi_match: {
        fields: m.fields.map(([name, weight]) => [name, weight]),
        text: m.text,
        kind: m.kind ?? "best_fields",
        operator: m.operator ?? "or",
        tie_breaker: m.tieBreaker ?? null,
      },
    };
  }
  if ("term" in query) {
    return { term: { field: query.term.field, value: encodeFieldValue(query.term.value) } };
  }
  if ("terms" in query) {
    return {
      terms: { field: query.terms.field, values: query.terms.values.map(encodeFieldValue) },
    };
  }
  if ("range" in query) {
    const r = query.range;
    return {
      range: {
        field: r.field,
        gt: optionalValue(r.gt),
        gte: optionalValue(r.gte),
        lt: optionalValue(r.lt),
        lte: optionalValue(r.lte),
      },
    };
  }
  if ("exists" in query) return { exists: { field: query.exists.field } };
  if ("isNull" in query) return { is_null: { field: query.isNull.field } };
  if ("isEmpty" in query) return { is_empty: { field: query.isEmpty.field } };
  if ("valuesCount" in query) {
    const v = query.valuesCount;
    return {
      values_count: {
        field: v.field,
        gt: v.gt ?? null,
        gte: v.gte ?? null,
        lt: v.lt ?? null,
        lte: v.lte ?? null,
      },
    };
  }
  if ("prefix" in query)
    return { prefix: { field: query.prefix.field, value: query.prefix.value } };
  if ("wildcard" in query) {
    return { wildcard: { field: query.wildcard.field, pattern: query.wildcard.pattern } };
  }
  if ("fuzzy" in query) {
    const f = query.fuzzy;
    return { fuzzy: { field: f.field, value: f.value, fuzziness: f.fuzziness ?? "auto" } };
  }
  if ("ids" in query) return { ids: query.ids.map(encodeId) };
  if ("queryString" in query) {
    const s = query.queryString;
    return {
      query_string: {
        query: s.query,
        default_fields: [...(s.defaultFields ?? [])],
        default_operator: s.defaultOperator ?? "or",
      },
    };
  }
  if ("bool" in query) {
    const b = query.bool;
    return {
      bool: {
        must: queries(b.must),
        should: queries(b.should),
        must_not: queries(b.mustNot),
        filter: queries(b.filter),
        minimum_should_match: b.minimumShouldMatch ?? null,
      },
    };
  }
  if ("boost" in query) {
    return { boost: { query: encodeQuery(query.boost.query), boost: query.boost.boost } };
  }
  if ("constantScore" in query) {
    const c = query.constantScore;
    return { constant_score: { query: encodeQuery(c.query), score: c.score } };
  }
  throw new TypeError(`not a query: ${JSON.stringify(Object.keys(query))}`);
}

function optionalQuery(query: Query | undefined): unknown {
  return query === undefined ? null : encodeQuery(query);
}

export function encodeFusion(fusion: Fusion): unknown {
  if (fusion === "dbsf") return "dbsf";
  if (typeof fusion === "object" && fusion !== null) {
    if ("rrf" in fusion) return { rrf: { k: fusion.rrf.k } };
    if ("weightedSum" in fusion)
      return { weighted_sum: { weights: [...fusion.weightedSum.weights] } };
  }
  throw new TypeError(`not a fusion: ${String(fusion)}`);
}

export function encodeRetriever(retriever: Retriever): unknown {
  if (typeof retriever !== "object" || retriever === null) {
    throw new TypeError(`not a retriever: ${String(retriever)}`);
  }
  if ("vector" in retriever) {
    const v = retriever.vector;
    const p = v.params ?? {};
    return {
      vector: {
        field: v.field,
        query: encodeVector(v.field, v.query),
        k: v.k,
        params: {
          exact: p.exact ?? false,
          nprobes: p.nprobes ?? null,
          refine_factor: p.refineFactor ?? null,
          ef: p.ef ?? null,
          oversampling: p.oversampling ?? null,
          distance: null,
        },
        filter: optionalQuery(v.filter),
      },
    };
  }
  if ("text" in retriever) {
    return { text: { query: encodeQuery(retriever.text.query), k: retriever.text.k } };
  }
  if ("fused" in retriever) {
    const f = retriever.fused;
    return {
      fused: { inputs: f.inputs.map(encodeRetriever), fusion: encodeFusion(f.fusion), k: f.k },
    };
  }
  if ("rescore" in retriever) {
    const r = retriever.rescore;
    return {
      rescore: {
        input: encodeRetriever(r.input),
        field: r.field,
        query: encodeVector(r.field, r.query),
        k: r.k,
      },
    };
  }
  if ("sparse" in retriever) {
    const s = retriever.sparse;
    return {
      sparse: {
        field: s.field,
        query: checkSparse(s.query),
        k: s.k,
        filter: optionalQuery(s.filter),
        params: { idf_corpus: optionalQuery(s.idfCorpus) },
      },
    };
  }
  throw new TypeError(`not a retriever: ${JSON.stringify(Object.keys(retriever))}`);
}

export function encodeSortKey(key: SortKey): unknown {
  if (typeof key === "object" && key !== null) {
    if ("score" in key) return { score: { order: key.score.order } };
    if ("field" in key) {
      const f = key.field;
      return { field: { field: f.field, order: f.order, missing: f.missing ?? "last" } };
    }
    if ("pk" in key) return { pk: { order: key.pk.order } };
  }
  throw new TypeError(`not a sort key: ${String(key)}`);
}

export function encodeProjection(projection: Projection = {}): JsonObject {
  const source = projection.source ?? "all";
  let encoded: unknown;
  if (source === "all" || source === "none") encoded = source;
  else if (typeof source === "object" && source !== null) {
    encoded = { include: [...(source.include ?? [])], exclude: [...(source.exclude ?? [])] };
  } else {
    throw new TypeError(
      `a projection's source is "all", "none" or {include, exclude}: ${String(source)}`,
    );
  }
  return {
    source: encoded,
    vectors: [...(projection.vectors ?? [])],
    fields: [...(projection.fields ?? [])],
  };
}

function encodeTrackTotalHits(value: TrackTotalHits | undefined): unknown {
  if (value === undefined) return "none";
  if (value === "none" || value === "exact") return value;
  if (typeof value === "object" && value !== null && "upTo" in value) {
    return { up_to: index(value.upTo, "trackTotalHits.upTo") };
  }
  throw new TypeError(`trackTotalHits is "none", "exact" or {upTo}: ${String(value)}`);
}

// ---------------------------------------------------------------- consistency (Ruling 6)

/** How one read sends its consistency: a body value and/or the token header. */
export interface ReadConsistency {
  /** `"eventual"` or a pinned object, for W11 and W13 bodies; undefined sends no key. */
  body: unknown;
  /** The SearchRequest's `consistency` (always sent). */
  searchBody: unknown;
  header: string | undefined;
}

function isPin(value: unknown): value is Pin {
  return (
    typeof value === "object" &&
    value !== null &&
    "manifestVersion" in value &&
    "token" in value &&
    (value as Pin).token instanceof ConsistencyToken
  );
}

export function encodePin(pin: Pin): JsonObject {
  return {
    pinned: {
      manifest_version: encodeU64(pin.manifestVersion, "a pin's manifestVersion"),
      token: pin.token.toString(),
    },
  };
}

export function readConsistency(value: Consistency | undefined): ReadConsistency {
  if (value === undefined || value === "strong") {
    return { body: undefined, searchBody: "strong", header: undefined };
  }
  if (value === "eventual") return { body: "eventual", searchBody: "eventual", header: undefined };
  if (isPin(value)) {
    const pinned = encodePin(value);
    return { body: pinned, searchBody: pinned, header: undefined };
  }
  const token =
    typeof value === "string"
      ? ConsistencyToken.parse(value)
      : value instanceof ConsistencyToken
        ? value
        : null;
  if (token === null) throw new TypeError(`not a consistency: ${String(value)}`);
  const text = token.toString();
  return { body: undefined, searchBody: { at_least: text }, header: text };
}

function tokenHeaders(read: ReadConsistency): Record<string, string> {
  return read.header === undefined ? {} : { [TOKEN_HEADER]: read.header };
}

// ---------------------------------------------------------------- schemas

function encodeKind(kind: Kind): unknown {
  if (typeof kind === "string") return kind;
  if (typeof kind === "object" && kind !== null && "text" in kind) {
    return { text: { analyzer: kind.text.analyzer, positions: kind.text.positions } };
  }
  throw new TypeError(`not a field kind: ${String(kind)}`);
}

export function encodeSchema(schema: SchemaInput): JsonObject {
  if (typeof schema !== "object" || schema === null) throw new TypeError("a schema is an object");
  return {
    fields: (schema.fields ?? []).map((f) => ({
      name: f.name,
      source_path: f.sourcePath ?? f.name,
      kind: encodeKind(f.kind),
      indexed: f.indexed ?? true,
      fast: f.fast ?? false,
    })),
    vectors: (schema.vectors ?? []).map((v) => ({
      name: v.name,
      dim: v.dim,
      distance: v.distance ?? "cosine",
    })),
    sparse_vectors: (schema.sparseVectors ?? []).map((s) => ({
      name: s.name,
      modifier: s.modifier ?? "none",
    })),
    dynamic: schema.dynamic ?? "strict",
    max_fields: schema.maxFields ?? 1000,
  };
}

function decodeKind(value: unknown): Kind {
  if (value === "text") return { text: { analyzer: "standard", positions: true } };
  if (typeof value === "string") return value as Kind;
  if (typeof value === "object" && value !== null && "text" in value) {
    const spec = (value as JsonObject).text;
    const t = spec === null || spec === undefined ? {} : object(spec, "a text kind");
    return {
      text: {
        analyzer: typeof t.analyzer === "string" ? t.analyzer : "standard",
        positions: typeof t.positions === "boolean" ? t.positions : true,
      },
    };
  }
  throw new TypeError(`unknown field kind: ${String(value)}`);
}

export function decodeSchema(value: unknown): SchemaInput & { version?: number } {
  const s = object(value, "a schema");
  const out: SchemaInput & { version?: number } = {
    fields: array(s.fields, "fields").map((raw): FieldInput => {
      const f = object(raw, "a field");
      const field: FieldInput = {
        name: String(f.name),
        kind: decodeKind(f.kind),
        indexed: f.indexed === undefined ? true : Boolean(f.indexed),
        fast: Boolean(f.fast),
      };
      if (typeof f.source_path === "string") field.sourcePath = f.source_path;
      return field;
    }),
    vectors: array(s.vectors, "vectors").map((raw): VectorInput => {
      const v = object(raw, "a vector");
      return {
        name: String(v.name),
        dim: int(v.dim, "dim"),
        distance: (typeof v.distance === "string" ? v.distance : "cosine") as Distance,
      };
    }),
    sparseVectors: array(s.sparse_vectors, "sparse_vectors").map((raw): SparseVectorFieldInput => {
      const sv = object(raw, "a sparse vector");
      return {
        name: String(sv.name),
        modifier: (typeof sv.modifier === "string" ? sv.modifier : "none") as SparseModifier,
      };
    }),
    dynamic: (typeof s.dynamic === "string" ? s.dynamic : "strict") as Dynamic,
    maxFields:
      s.max_fields === undefined || s.max_fields === null ? 1000 : int(s.max_fields, "max_fields"),
  };
  if (s.version !== undefined && s.version !== null) out.version = int(s.version, "version");
  return out;
}

// ---------------------------------------------------------------- collections (W6–W9)

function collectionsPath(ns: string): string {
  return `/v1/namespaces/${seg(ns)}/collections`;
}

function collectionPath(ns: string, collection: string): string {
  return `${collectionsPath(ns)}/${seg(collection)}`;
}

export function createCollection(
  ns: string,
  name: string,
  schema: SchemaInput,
  partitions: number | undefined,
): WireRequest {
  const body: JsonObject = { name, schema: encodeSchema(schema) };
  if (partitions !== undefined) body.partitions = index(partitions, "partitions");
  return { method: "POST", path: collectionsPath(ns), body, idempotent: true };
}

export function getCollection(ns: string, name: string): WireRequest {
  return { method: "GET", path: collectionPath(ns, name), idempotent: true };
}

export function listCollections(ns: string): WireRequest {
  return { method: "GET", path: collectionsPath(ns), idempotent: true };
}

export function dropCollection(ns: string, name: string): WireRequest {
  return { method: "DELETE", path: collectionPath(ns, name), idempotent: true };
}

function decodeCollectionInfo(value: unknown): CollectionInfo {
  const c = object(value, "a collection");
  return {
    id: int(c.id, "id"),
    name: String(c.name),
    schema: decodeSchema(c.schema),
    partitions: intOrNull(c.partitions, "partitions"),
    liveDocCount: intOrNull(c.live_doc_count, "live_doc_count"),
    raw: c,
  };
}

export function parseCollectionInfo(response: WireResponse): CollectionInfo {
  return decodeCollectionInfo(parseJson(response));
}

export function parseCollectionList(response: WireResponse): CollectionInfo[] {
  const body = object(parseJson(response), "a collection list");
  return array(body.collections, "collections").map(decodeCollectionInfo);
}

export function parseDropped(response: WireResponse): boolean {
  return object(parseJson(response), "a drop answer").dropped === true;
}

// ---------------------------------------------------------------- documents (W10, W11)

function source(value: Record<string, unknown> | undefined, what: string): JsonObject {
  if (value === undefined) return {};
  return object(value, what);
}

function encodeVectors(vectors: Record<string, VectorLike> | undefined): JsonObject {
  const out: JsonObject = {};
  for (const [name, v] of Object.entries(vectors ?? {})) out[name] = encodeVector(name, v);
  return out;
}

function encodeSparseVectors(vectors: Record<string, SparseVector> | undefined): JsonObject {
  const out: JsonObject = {};
  for (const [name, v] of Object.entries(vectors ?? {})) out[name] = checkSparse(v);
  return out;
}

export function encodeDocument(doc: DocumentInput): JsonObject {
  if (typeof doc !== "object" || doc === null) {
    throw new TypeError("a document is {id, source?, vectors?, sparseVectors?}");
  }
  return {
    id: encodeId(doc.id),
    source: source(doc.source, "a document's source"),
    vectors: encodeVectors(doc.vectors),
    sparse_vectors: encodeSparseVectors(doc.sparseVectors),
  };
}

export function encodeOp(op: Op): JsonObject {
  if (typeof op === "object" && op !== null) {
    if ("upsert" in op) return { upsert: encodeDocument(op.upsert) };
    if ("delete" in op) return { delete: { id: encodeId(op.delete) } };
    if ("patch" in op) {
      const p = op.patch;
      const vectors: JsonObject = {};
      for (const [name, v] of Object.entries(p.vectors ?? {})) {
        vectors[name] = v === null ? null : encodeVector(name, v);
      }
      const sparse: JsonObject = {};
      for (const [name, v] of Object.entries(p.sparseVectors ?? {})) {
        sparse[name] = v === null ? null : checkSparse(v);
      }
      return {
        patch: {
          id: encodeId(p.id),
          mode: p.mode ?? "merge_deep",
          source: source(p.source, "a patch's source"),
          delete_keys: [...(p.deleteKeys ?? [])],
          vectors,
          sparse_vectors: sparse,
          upsert: p.upsert === undefined ? null : encodeDocument(p.upsert),
        },
      };
    }
  }
  throw new TypeError(`not a write op: ${String(op)}`);
}

/** W10: one request, atomic across partitions; a keyed write, so retried (Ruling 5). */
export function write(
  ns: string,
  collection: string,
  ops: Op[],
  reportExistence: boolean,
): WireRequest {
  if (!Array.isArray(ops)) throw new TypeError("ops is an array");
  if (ops.length === 0) throw new RangeError("an empty write: give at least one op");
  return {
    method: "POST",
    path: `${collectionPath(ns, collection)}/documents`,
    body: { ops: ops.map(encodeOp), report_existence: reportExistence },
    idempotent: true,
  };
}

function readToken(response: WireResponse, body: JsonObject, key: string): ConsistencyToken {
  const token = tokenFromHeader(response);
  if (token !== null) return token;
  const raw = body[key];
  if (typeof raw !== "string") throw new TypeError(`expected a token string in ${key}`);
  return ConsistencyToken.parse(raw);
}

export function parseWrite(response: WireResponse): WriteResult {
  const body = object(parseJson(response), "a write answer");
  return {
    token: readToken(response, body, "token"),
    results: array(body.results, "results").map(String),
  };
}

export function getDocuments(
  ns: string,
  collection: string,
  ids: Id[],
  select: Projection | undefined,
  consistency: Consistency | undefined,
): WireRequest {
  if (!Array.isArray(ids)) throw new TypeError("ids is an array");
  const read = readConsistency(consistency);
  const body: JsonObject = { ids: ids.map(encodeId), select: encodeProjection(select) };
  if (read.body !== undefined) body.consistency = read.body;
  return {
    method: "POST",
    path: `${collectionPath(ns, collection)}/documents/get`,
    headers: tokenHeaders(read),
    body,
    idempotent: true,
  };
}

function decodeVectors(value: unknown): Record<string, number[]> {
  const out: Record<string, number[]> = {};
  if (value === undefined || value === null) return out;
  for (const [name, v] of Object.entries(object(value, "vectors"))) {
    out[name] = array(v, `vector ${name}`).map(Number);
  }
  return out;
}

function decodeSparseVectors(value: unknown): Record<string, SparseVector> {
  const out: Record<string, SparseVector> = {};
  if (value === undefined || value === null) return out;
  for (const [name, v] of Object.entries(object(value, "sparse_vectors"))) {
    out[name] = decodeSparse(v, `sparse vector ${name}`);
  }
  return out;
}

function decodeStoredDoc(value: unknown): StoredDoc | null {
  if (value === null || value === undefined) return null;
  const d = object(value, "a document");
  return {
    id: decodeId(d.id),
    source: d.source === null || d.source === undefined ? {} : object(d.source, "source"),
    vectors: decodeVectors(d.vectors),
    sparseVectors: decodeSparseVectors(d.sparse_vectors),
  };
}

export function parseDocuments(response: WireResponse): Array<StoredDoc | null> {
  const body = object(parseJson(response), "a get answer");
  return array(body.documents, "documents").map(decodeStoredDoc);
}

// ---------------------------------------------------------------- search (W12)

export function encodeSearchRequest(
  request: SearchRequestInput,
  consistency: Consistency | undefined,
): JsonObject {
  if (typeof request !== "object" || request === null)
    throw new TypeError("a search request is an object");
  checkRequest(request);
  const read = readConsistency(consistency);
  const optional = (value: Record<string, unknown> | undefined, what: string): unknown =>
    value === undefined ? null : object(value, what);
  return {
    collection: request.collection,
    consistency: read.searchBody,
    retrievers: (request.retrievers ?? []).map(encodeRetriever),
    fusion: request.fusion === undefined ? null : encodeFusion(request.fusion),
    filter: optionalQuery(request.filter),
    sort: (request.sort ?? []).map(encodeSortKey),
    offset: request.offset ?? 0,
    limit: request.limit ?? 10,
    search_after: request.searchAfter === undefined ? null : [...request.searchAfter],
    score_threshold: request.scoreThreshold ?? null,
    select: encodeProjection(request.select),
    aggregations: optional(request.aggregations, "aggregations"),
    highlight: optional(request.highlight, "highlight"),
    group_by: optional(request.groupBy, "groupBy"),
    track_total_hits: encodeTrackTotalHits(request.trackTotalHits),
  };
}

export function query(
  ns: string,
  request: SearchRequestInput,
  consistency: Consistency | undefined,
): WireRequest {
  return {
    method: "POST",
    path: `/v1/namespaces/${seg(ns)}/query`,
    headers: tokenHeaders(readConsistency(consistency)),
    body: encodeSearchRequest(request, consistency),
    idempotent: true,
  };
}

function decodeHit(value: unknown): Hit {
  const h = object(value, "a hit");
  const highlight: Record<string, string[]> = {};
  if (h.highlight !== undefined && h.highlight !== null) {
    for (const [k, v] of Object.entries(object(h.highlight, "highlight"))) {
      highlight[k] = array(v, "highlight").map(String);
    }
  }
  return {
    id: decodeId(h.pk),
    score: Number(h.score),
    sortValues: [...array(h.sort_values, "sort_values")],
    source: h.source === undefined || h.source === null ? null : object(h.source, "source"),
    vectors: decodeVectors(h.vectors),
    sparseVectors: decodeSparseVectors(h.sparse_vectors),
    highlight,
  };
}

export function parseSearch(response: WireResponse): SearchResponse {
  const body = object(parseJson(response), "a search answer");
  let total: SearchResponse["total"] = null;
  if (body.total !== undefined && body.total !== null) {
    const t = object(body.total, "total");
    total = { value: int(t.value, "total.value"), relation: t.relation === "gte" ? "gte" : "eq" };
  }
  return {
    hits: array(body.hits, "hits").map(decodeHit),
    total,
    aggregations:
      body.aggregations === undefined || body.aggregations === null
        ? null
        : object(body.aggregations, "aggregations"),
    groups:
      body.groups === undefined || body.groups === null
        ? null
        : array(body.groups, "groups").map((g) => object(g, "a group")),
    readToken: readToken(response, body, "read_token"),
  };
}

// ---------------------------------------------------------------- SQL (W13)

export function sql(ns: string, text: string, consistency: Consistency | undefined): WireRequest {
  if (typeof text !== "string") throw new TypeError("a SQL query is a string");
  const read = readConsistency(consistency);
  const body: JsonObject = { query: text };
  if (read.body !== undefined) body.consistency = read.body;
  return {
    method: "POST",
    path: `/v1/namespaces/${seg(ns)}/sql`,
    headers: tokenHeaders(read),
    body,
    idempotent: true,
  };
}

export function parseSql(response: WireResponse): SqlResult {
  const body = object(parseJson(response), "a SQL answer");
  const columns = array(body.columns, "columns").map((raw) => {
    const c = object(raw, "a column");
    return { name: String(c.name), type: String(c.type) };
  });
  const rows = array(body.rows, "rows").map((r) => [...array(r, "a row")]);
  return {
    columns,
    rows,
    truncated: body.truncated === true,
    toObjects(): Record<string, unknown>[] {
      return rows.map((row) => {
        const out: Record<string, unknown> = {};
        columns.forEach((column, i) => {
          out[column.name] = row[i];
        });
        return out;
      });
    },
  };
}

// ---------------------------------------------------------------- scan plans (W15)

export function encodeScanAt(at: ScanAt): unknown {
  if (at === "current") return "current";
  if (typeof at === "number" || typeof at === "bigint") {
    return { manifest_version: encodeU64(at, "a manifest version") };
  }
  if (typeof at === "string") return { token: ConsistencyToken.parse(at).toString() };
  if (at instanceof ConsistencyToken) return { token: at.toString() };
  throw new TypeError(
    `a scan point is "current", a manifest version or a token, not ${String(at)}`,
  );
}

export function scanPlan(ns: string, collection: string, at: ScanAt): WireRequest {
  return {
    method: "POST",
    path: `${collectionPath(ns, collection)}/scan`,
    body: { at: encodeScanAt(at) },
    idempotent: true,
  };
}

function decodePin(value: unknown): Pin {
  const p = object(value, "a pin");
  if (typeof p.token !== "string") throw new TypeError("expected a token string in pin.token");
  return {
    manifestVersion: decodeU64(p.manifest_version, "pin.manifest_version"),
    token: ConsistencyToken.parse(p.token),
  };
}

function path(value: unknown, what: string): string {
  return String(object(value, what).path);
}

export function decodeScanPlan(value: unknown): ScanPlan {
  const v = object(value, "a scan plan");
  let lance: ScanPlan["lance"] = null;
  if (v.lance !== undefined && v.lance !== null) {
    const l = object(v.lance, "lance");
    lance = {
      uri: typeof l.uri === "string" ? l.uri : null,
      version: decodeU64(l.version, "lance.version"),
      manifestPath: String(l.manifest_path),
    };
  }
  if (typeof v.durable_token !== "string") throw new TypeError("expected a durable_token string");
  return {
    collection: String(v.collection),
    collectionId: int(v.collection_id, "collection_id"),
    manifestVersion: decodeU64(v.manifest_version, "manifest_version"),
    lance,
    fragments: array(v.fragments, "fragments").map((raw): ScanFragment => {
      const f = object(raw, "a fragment");
      return {
        id: int(f.id, "fragment id"),
        physicalRows: int(f.physical_rows, "physical_rows"),
        deletedRows: int(f.deleted_rows, "deleted_rows"),
        liveRows: int(f.live_rows, "live_rows"),
        files: array(f.files, "files").map((file) => path(file, "a file")),
        deletionFile:
          f.deletion_file === undefined || f.deletion_file === null
            ? null
            : path(f.deletion_file, "deletion_file"),
        lance: f.lance === undefined || f.lance === null ? {} : object(f.lance, "fragment lance"),
      };
    }),
    liveRows: int(v.live_rows, "live_rows"),
    columns: array(v.columns, "columns").map((raw): ScanColumn => {
      const c = object(raw, "a column");
      const column: ScanColumn = {
        name: String(c.name),
        dataType: String(c.data_type),
        role: String(c.role),
      };
      if (typeof c.vector === "string") column.vector = c.vector;
      if (c.dim !== undefined && c.dim !== null) column.dim = int(c.dim, "dim");
      return column;
    }),
    tail: v.tail === true,
    tailRecords: v.tail_records === undefined ? 0 : int(v.tail_records, "tail_records"),
    durableToken: ConsistencyToken.parse(v.durable_token),
    pin: decodePin(v.pin),
    plannedAtMs: v.planned_at_ms === undefined ? 0 : int(v.planned_at_ms, "planned_at_ms"),
    expiresAtMs: intOrNull(v.expires_at_ms, "expires_at_ms"),
    raw: v,
  };
}

export function parseScanPlan(response: WireResponse): ScanPlan {
  return decodeScanPlan(parseJson(response));
}
