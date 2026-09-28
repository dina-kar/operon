/**
 * The hybrid query IR (overview §6.6) as plain tagged objects, built by `q`.
 *
 * ```ts
 * import { q } from "@operon/client";
 * ns.search("kb").retrieve(q.vector("embedding", emb, { k: 50 }), q.text("refund", { k: 50 })).limit(10);
 * ```
 *
 * These types know nothing about JSON: `wire.ts` maps them to the wire's
 * snake_case keys and tagged forms.
 */
import type { Id, SparseVector, VectorLike } from "./types.js";
import { checkSparse } from "./wire.js";

/** A field value: strings, numbers, u64/i64 `bigint`s, booleans and dates. */
export type FieldValue = string | number | bigint | boolean | Date;
export type Operator = "or" | "and";
export type Fuzziness = "auto" | 0 | 1 | 2;
export type Order = "asc" | "desc";
export type MultiMatchKind =
  | "best_fields"
  | "most_fields"
  | "cross_fields"
  | "phrase"
  | "phrase_prefix";
export type Bounds<T> = { gt?: T; gte?: T; lt?: T; lte?: T };

export interface MatchOptions {
  operator?: Operator;
  minimumShouldMatch?: string;
  fuzziness?: Fuzziness;
  analyzer?: string;
}

export interface MultiMatchOptions {
  kind?: MultiMatchKind;
  operator?: Operator;
  tieBreaker?: number;
}

export interface BoolSpec {
  must?: Query[];
  should?: Query[];
  mustNot?: Query[];
  filter?: Query[];
  minimumShouldMatch?: string;
}

export type Query =
  | { matchAll: true }
  | { matchNone: true }
  | { match: { field: string; text: string } & MatchOptions }
  | { matchPhrase: { field: string; text: string; slop?: number } }
  | { multiMatch: { fields: Array<[string, number]>; text: string } & MultiMatchOptions }
  | { term: { field: string; value: FieldValue } }
  | { terms: { field: string; values: FieldValue[] } }
  | { range: { field: string } & Bounds<FieldValue> }
  | { exists: { field: string } }
  | { isNull: { field: string } }
  | { isEmpty: { field: string } }
  | { valuesCount: { field: string } & Bounds<number> }
  | { prefix: { field: string; value: string } }
  | { wildcard: { field: string; pattern: string } }
  | { fuzzy: { field: string; value: string; fuzziness?: Fuzziness } }
  | { ids: Id[] }
  | { queryString: { query: string; defaultFields?: string[]; defaultOperator?: Operator } }
  | { bool: BoolSpec }
  | { boost: { query: Query; boost: number } }
  | { constantScore: { query: Query; score: number } };

/** ANN parameters; an absent one is left to the server. */
export interface AnnParams {
  exact?: boolean;
  nprobes?: number;
  refineFactor?: number;
  ef?: number;
  oversampling?: number;
}

export type Fusion = { rrf: { k: number } } | "dbsf" | { weightedSum: { weights: number[] } };

export type Retriever =
  | { vector: { field: string; query: VectorLike; k: number; params?: AnnParams; filter?: Query } }
  | { text: { query: Query; k: number } }
  | { fused: { inputs: Retriever[]; fusion: Fusion; k: number } }
  | { rescore: { input: Retriever; field: string; query: VectorLike; k: number } }
  | {
      sparse: { field: string; query: SparseVector; k: number; filter?: Query; idfCorpus?: Query };
    };

/** `missing` (field sorts) defaults to `"last"`; `pk` sorts by document id. */
export type SortKey =
  | { score: { order: Order } }
  | { field: { field: string; order: Order; missing?: "first" | "last" } }
  | { pk: { order: Order } };

/** What a read returns: the source (all, none or paths), vectors by name, typed fields. */
export interface Projection {
  source?: "all" | "none" | { include?: string[]; exclude?: string[] };
  vectors?: string[];
  fields?: string[];
}

export type TrackTotalHits = "none" | "exact" | { upTo: number };

/** A search request; `offset` defaults to 0 and `limit` to 10. */
export interface SearchRequestInput {
  collection: string;
  retrievers?: Retriever[];
  fusion?: Fusion;
  filter?: Query;
  sort?: SortKey[];
  offset?: number;
  limit?: number;
  searchAfter?: unknown[];
  scoreThreshold?: number;
  select?: Projection;
  aggregations?: Record<string, unknown>;
  highlight?: Record<string, unknown>;
  groupBy?: Record<string, unknown>;
  trackTotalHits?: TrackTotalHits;
}

function checkPositive(value: unknown, what: string): void {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 1) {
    throw new RangeError(`${what} must be an integer >= 1, got ${String(value)}`);
  }
}

function checkK(retriever: Retriever): void {
  if (typeof retriever !== "object" || retriever === null) {
    throw new TypeError(`not a retriever: ${String(retriever)}`);
  }
  const [inner] = Object.values(retriever) as Array<{ k?: unknown } | undefined>;
  checkPositive(inner?.k, "a retriever's k");
  if ("fused" in retriever) for (const input of retriever.fused.inputs) checkK(input);
  else if ("rescore" in retriever) checkK(retriever.rescore.input);
}

/** `RangeError` unless `limit >= 1`, `offset >= 0` and every `k >= 1` (Task 3 rule 3). */
export function checkRequest(request: SearchRequestInput): void {
  if (request.limit !== undefined) checkPositive(request.limit, "limit");
  const offset = request.offset;
  if (offset !== undefined && (!Number.isSafeInteger(offset) || offset < 0)) {
    throw new RangeError(`offset must be an integer >= 0, got ${String(offset)}`);
  }
  for (const retriever of request.retrievers ?? []) checkK(retriever);
}

function defined<T extends object>(value: T): T {
  const out: Record<string, unknown> = {};
  for (const [key, v] of Object.entries(value)) if (v !== undefined) out[key] = v;
  return out as T;
}

/** One constructor per query variant, retriever, fusion and sort key. */
export const q = {
  matchAll(): Query {
    return { matchAll: true };
  },
  matchNone(): Query {
    return { matchNone: true };
  },
  match(field: string, text: string, opts: MatchOptions = {}): Query {
    return { match: defined({ field, text, ...opts }) };
  },
  matchPhrase(field: string, text: string, opts: { slop?: number } = {}): Query {
    return { matchPhrase: defined({ field, text, ...opts }) };
  },
  multiMatch(fields: Array<[string, number]>, text: string, opts: MultiMatchOptions = {}): Query {
    return {
      multiMatch: defined({
        fields: fields.map(([f, w]): [string, number] => [f, w]),
        text,
        ...opts,
      }),
    };
  },
  term(field: string, value: FieldValue): Query {
    return { term: { field, value } };
  },
  terms(field: string, values: FieldValue[]): Query {
    return { terms: { field, values: [...values] } };
  },
  range(field: string, bounds: Bounds<FieldValue>): Query {
    return { range: defined({ field, ...bounds }) };
  },
  exists(field: string): Query {
    return { exists: { field } };
  },
  isNull(field: string): Query {
    return { isNull: { field } };
  },
  isEmpty(field: string): Query {
    return { isEmpty: { field } };
  },
  valuesCount(field: string, bounds: Bounds<number>): Query {
    return { valuesCount: defined({ field, ...bounds }) };
  },
  prefix(field: string, value: string): Query {
    return { prefix: { field, value } };
  },
  wildcard(field: string, pattern: string): Query {
    return { wildcard: { field, pattern } };
  },
  fuzzy(field: string, value: string, opts: { fuzziness?: Fuzziness } = {}): Query {
    return { fuzzy: defined({ field, value, ...opts }) };
  },
  ids(...ids: Id[]): Query {
    return { ids };
  },
  queryString(
    query: string,
    opts: { defaultFields?: string[]; defaultOperator?: Operator } = {},
  ): Query {
    return { queryString: defined({ query, ...opts }) };
  },
  bool(spec: BoolSpec): Query {
    return { bool: defined({ ...spec }) };
  },
  boost(query: Query, boost: number): Query {
    return { boost: { query, boost } };
  },
  constantScore(query: Query, score: number): Query {
    return { constantScore: { query, score } };
  },

  /** Nearest neighbours of `query` in the dense vector field `field`. */
  vector(
    field: string,
    query: VectorLike,
    opts: AnnParams & { k: number; filter?: Query },
  ): Retriever {
    const { k, filter, ...params } = opts;
    const spec: { field: string; query: VectorLike; k: number; params: AnnParams; filter?: Query } =
      { field, query, k, params: defined(params) };
    if (filter !== undefined) spec.filter = filter;
    return { vector: spec };
  },
  /**
   * Full-text search. A string becomes `queryString` (no field), `match` (one
   * field) or `multiMatch` (several, each boost 1).
   */
  text(query: Query | string, opts: { k: number; fields?: string[] }): Retriever {
    const fields = opts.fields ?? [];
    if (typeof query !== "string") {
      if (fields.length > 0)
        throw new TypeError("fields apply only to a text query given as a string");
      return { text: { query, k: opts.k } };
    }
    if (fields.length === 0) return { text: { query: q.queryString(query), k: opts.k } };
    if (fields.length === 1)
      return { text: { query: q.match(fields[0] as string, query), k: opts.k } };
    return {
      text: {
        query: q.multiMatch(
          fields.map((f): [string, number] => [f, 1]),
          query,
        ),
        k: opts.k,
      },
    };
  },
  fused(inputs: Retriever[], fusion: Fusion, k: number): Retriever {
    return { fused: { inputs: [...inputs], fusion, k } };
  },
  rescore(input: Retriever, field: string, query: VectorLike, k: number): Retriever {
    return { rescore: { input, field, query, k } };
  },
  /** Exact sparse-vector search; the vector is checked here and again when sent. */
  sparse(
    field: string,
    query: SparseVector,
    opts: { k: number; filter?: Query; idfCorpus?: Query },
  ): Retriever {
    checkSparse(query);
    return { sparse: defined({ field, query, ...opts }) };
  },
  rrf(k = 60): Fusion {
    return { rrf: { k } };
  },
  dbsf(): Fusion {
    return "dbsf";
  },
  weightedSum(...weights: number[]): Fusion {
    return { weightedSum: { weights } };
  },

  scoreSort(order: Order = "desc"): SortKey {
    return { score: { order } };
  },
  fieldSort(
    field: string,
    order: Order = "asc",
    opts: { missing?: "first" | "last" } = {},
  ): SortKey {
    return { field: { field, order, missing: opts.missing ?? "last" } };
  },
  pkSort(order: Order = "asc"): SortKey {
    return { pk: { order } };
  },
};
