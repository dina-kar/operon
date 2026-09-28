/** The client, its namespace handle, collections and the search builder (plan M1.6 Tasks 5, 6). */
import { AlreadyExistsError } from "./errors.js";
import {
  checkRequest,
  type Fusion,
  type Projection,
  type Query,
  type Retriever,
  type SearchRequestInput,
  type SortKey,
  type TrackTotalHits,
} from "./query.js";
import type { SchemaInput } from "./schema.js";
import { Transport } from "./transport.js";
import type {
  ClientOptions,
  CollectionInfo,
  Consistency,
  DocumentInput,
  FetchResult,
  Id,
  Op,
  PatchInput,
  ProduceRecord,
  ProduceResult,
  ReadOptions,
  RequestOptions,
  ScanAt,
  ScanPlan,
  SearchResponse,
  SqlResult,
  StoredDoc,
  StreamInfo,
  WriteResult,
} from "./types.js";
import * as wire from "./wire.js";

/**
 * A client of the native REST API.
 *
 * Idempotent requests that fail with 503, a network error or a per-attempt
 * timeout are retried with capped exponential backoff; stream produce is never
 * retried, because a failed produce may already have been committed.
 */
export class OperonClient {
  readonly #transport: Transport;

  constructor(options: ClientOptions = {}) {
    this.#transport = new Transport(options);
  }

  /** Creates a namespace and returns its id; with `existOk`, returns an existing one's. */
  async createNamespace(
    name: string,
    opts: RequestOptions & { existOk?: boolean } = {},
  ): Promise<number> {
    try {
      return wire.parseCreatedId(
        await this.#transport.send(wire.createNamespace(name), opts.signal),
      );
    } catch (error) {
      if (opts.existOk === true && error instanceof AlreadyExistsError && error.id !== undefined) {
        return error.id;
      }
      throw error;
    }
  }

  /** A handle on a namespace (no request is made). */
  namespace(name: string): Namespace {
    return new Namespace(this.#transport, name);
  }
}

/** A namespace: its streams, collections, search and SQL. */
export class Namespace {
  readonly name: string;
  readonly #transport: Transport;

  /** @internal Use `OperonClient.namespace`. */
  constructor(transport: Transport, name: string) {
    if (typeof name !== "string") throw new TypeError("a namespace name is a string");
    this.#transport = transport;
    this.name = name;
  }

  /** Creates a stream and returns its id. */
  async createStream(
    name: string,
    partitions: number,
    opts: RequestOptions & { maxAgeMs?: number; maxBytes?: number } = {},
  ): Promise<number> {
    const request = wire.createStream(this.name, name, partitions, opts.maxAgeMs, opts.maxBytes);
    return wire.parseCreatedId(await this.#transport.send(request, opts.signal));
  }

  /** A stream's partitions and retention. */
  async getStream(name: string, opts: RequestOptions = {}): Promise<StreamInfo> {
    return wire.parseStreamInfo(
      await this.#transport.send(wire.getStream(this.name, name), opts.signal),
    );
  }

  /** Appends records to one partition. Never retried: a failed produce may have been committed. */
  async produce(
    stream: string,
    partition: number,
    records: ProduceRecord[],
    opts: RequestOptions = {},
  ): Promise<ProduceResult> {
    const request = wire.produce(this.name, stream, partition, records);
    return wire.parseProduce(await this.#transport.send(request, opts.signal));
  }

  /** Reads records from `offset`; with `maxWaitMs`, waits up to that long for new ones. */
  async fetch(
    stream: string,
    partition: number,
    offset: number,
    opts: RequestOptions & { maxBytes?: number; maxWaitMs?: number } = {},
  ): Promise<FetchResult> {
    const request = wire.fetchRecords(
      this.name,
      stream,
      partition,
      offset,
      opts.maxBytes,
      opts.maxWaitMs,
    );
    return wire.parseFetch(await this.#transport.send(request, opts.signal));
  }

  /** Creates a collection; re-creating it with the same schema returns the same collection. */
  async createCollection(
    name: string,
    schema: SchemaInput,
    opts: RequestOptions & { partitions?: number } = {},
  ): Promise<CollectionInfo> {
    const request = wire.createCollection(this.name, name, schema, opts.partitions);
    return wire.parseCollectionInfo(await this.#transport.send(request, opts.signal));
  }

  async getCollection(name: string, opts: RequestOptions = {}): Promise<CollectionInfo> {
    const request = wire.getCollection(this.name, name);
    return wire.parseCollectionInfo(await this.#transport.send(request, opts.signal));
  }

  async listCollections(opts: RequestOptions = {}): Promise<CollectionInfo[]> {
    const request = wire.listCollections(this.name);
    return wire.parseCollectionList(await this.#transport.send(request, opts.signal));
  }

  /** Drops a collection; false when it did not exist. */
  async dropCollection(name: string, opts: RequestOptions = {}): Promise<boolean> {
    const request = wire.dropCollection(this.name, name);
    return wire.parseDropped(await this.#transport.send(request, opts.signal));
  }

  /** A handle on a collection (no request is made). */
  collection(name: string): Collection {
    return new Collection(this.#transport, this.name, name);
  }

  /** A search builder over `collection`. */
  search(collection: string): SearchBuilder {
    return new SearchBuilder(this.#transport, this.name, { collection });
  }

  /** Runs a search request as given (no fusion default: see `SearchBuilder`). */
  async query(request: SearchRequestInput, opts: ReadOptions = {}): Promise<SearchResponse> {
    const wireRequest = wire.query(this.name, request, opts.consistency);
    return wire.parseSearch(await this.#transport.send(wireRequest, opts.signal));
  }

  /** Read-only SQL over the namespace's collections. */
  async sql(query: string, opts: ReadOptions = {}): Promise<SqlResult> {
    const request = wire.sql(this.name, query, opts.consistency);
    return wire.parseSql(await this.#transport.send(request, opts.signal));
  }
}

/** A collection: writes, reads by id, search and scan plans. */
export class Collection {
  readonly name: string;
  readonly namespace: string;
  readonly #transport: Transport;

  /** @internal Use `Namespace.collection`. */
  constructor(transport: Transport, namespace: string, name: string) {
    if (typeof name !== "string") throw new TypeError("a collection name is a string");
    this.#transport = transport;
    this.namespace = namespace;
    this.name = name;
  }

  /** Writes whole documents (one upsert op each). */
  async upsert(docs: DocumentInput[], opts: RequestOptions = {}): Promise<WriteResult> {
    return this.write(
      docs.map((upsert): Op => ({ upsert })),
      opts,
    );
  }

  /** Updates part of one document. */
  async patch(
    id: Id,
    patch: Omit<PatchInput, "id">,
    opts: RequestOptions = {},
  ): Promise<WriteResult> {
    return this.write([{ patch: { ...patch, id } }], opts);
  }

  /** Deletes documents by id. */
  async delete(ids: Id[], opts: RequestOptions = {}): Promise<WriteResult> {
    return this.write(
      ids.map((id): Op => ({ delete: id })),
      opts,
    );
  }

  /** Sends `ops` in one request, atomic across partitions; an empty list throws before sending. */
  async write(
    ops: Op[],
    opts: RequestOptions & { reportExistence?: boolean } = {},
  ): Promise<WriteResult> {
    const request = wire.write(this.namespace, this.name, ops, opts.reportExistence ?? false);
    return wire.parseWrite(await this.#transport.send(request, opts.signal));
  }

  /** Reads documents by id, in order; `null` for a missing one. */
  async get(
    ids: Id[],
    opts: ReadOptions & { select?: Projection } = {},
  ): Promise<Array<StoredDoc | null>> {
    const request = wire.getDocuments(
      this.namespace,
      this.name,
      ids,
      opts.select,
      opts.consistency,
    );
    return wire.parseDocuments(await this.#transport.send(request, opts.signal));
  }

  /** A search builder over this collection. */
  search(): SearchBuilder {
    return new SearchBuilder(this.#transport, this.namespace, { collection: this.name });
  }

  /** Resolves the collection into what an external reader needs to read one state of it (D53). */
  async scanPlan(opts: RequestOptions & { at?: ScanAt } = {}): Promise<ScanPlan> {
    const request = wire.scanPlan(this.namespace, this.name, opts.at ?? "current");
    return wire.parseScanPlan(await this.#transport.send(request, opts.signal));
  }
}

/**
 * An immutable search builder: every method returns a new builder, so a base
 * query can be reused. Two or more retrievers without `fuse()` fuse with RRF (k = 60).
 */
export class SearchBuilder {
  readonly #transport: Transport;
  readonly #namespace: string;
  readonly #request: Readonly<SearchRequestInput>;
  readonly #consistency: Consistency | undefined;

  /** @internal Use `Namespace.search` or `Collection.search`. */
  constructor(
    transport: Transport,
    namespace: string,
    request: SearchRequestInput,
    consistency?: Consistency,
  ) {
    this.#transport = transport;
    this.#namespace = namespace;
    this.#request = Object.freeze({ ...request });
    this.#consistency = consistency;
  }

  #with(changes: Partial<SearchRequestInput>): SearchBuilder {
    return new SearchBuilder(
      this.#transport,
      this.#namespace,
      { ...this.#request, ...changes },
      this.#consistency,
    );
  }

  /** Appends retrievers. */
  retrieve(...retrievers: Retriever[]): SearchBuilder {
    return this.#with({ retrievers: [...(this.#request.retrievers ?? []), ...retrievers] });
  }

  fuse(fusion: Fusion): SearchBuilder {
    return this.#with({ fusion });
  }

  filter(query: Query): SearchBuilder {
    return this.#with({ filter: query });
  }

  sort(...keys: SortKey[]): SearchBuilder {
    return this.#with({ sort: keys });
  }

  offset(n: number): SearchBuilder {
    return this.#with({ offset: n });
  }

  limit(n: number): SearchBuilder {
    return this.#with({ limit: n });
  }

  searchAfter(values: unknown[]): SearchBuilder {
    return this.#with({ searchAfter: [...values] });
  }

  scoreThreshold(value: number): SearchBuilder {
    return this.#with({ scoreThreshold: value });
  }

  select(projection: Projection): SearchBuilder {
    return this.#with({ select: projection });
  }

  aggregations(aggs: Record<string, unknown>): SearchBuilder {
    return this.#with({ aggregations: aggs });
  }

  highlight(spec: Record<string, unknown>): SearchBuilder {
    return this.#with({ highlight: spec });
  }

  groupBy(spec: Record<string, unknown>): SearchBuilder {
    return this.#with({ groupBy: spec });
  }

  trackTotalHits(value: TrackTotalHits): SearchBuilder {
    return this.#with({ trackTotalHits: value });
  }

  /** How fresh the read must be; a scan plan's `Pin` reads exactly its state. */
  consistency(value: Consistency): SearchBuilder {
    return new SearchBuilder(this.#transport, this.#namespace, this.#request, value);
  }

  /** The request, with RRF (k = 60) when two or more retrievers have no fusion; checks limits. */
  toRequest(): SearchRequestInput {
    const request: SearchRequestInput = { ...this.#request };
    if (request.fusion === undefined && (request.retrievers?.length ?? 0) >= 2) {
      request.fusion = { rrf: { k: 60 } };
    }
    checkRequest(request);
    return request;
  }

  async execute(opts: RequestOptions = {}): Promise<SearchResponse> {
    const request = wire.query(this.#namespace, this.toRequest(), this.#consistency);
    return wire.parseSearch(await this.#transport.send(request, opts.signal));
  }
}
