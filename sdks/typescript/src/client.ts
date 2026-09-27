/** The client and its namespace handle (plan M1.6 Task 5). */
import { AlreadyExistsError } from "./errors.js";
import { Transport } from "./transport.js";
import type {
  ClientOptions,
  FetchResult,
  ProduceRecord,
  ProduceResult,
  RequestOptions,
  StreamInfo,
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

/** A namespace: its streams (collections, search and SQL arrive in Task 6). */
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
}
