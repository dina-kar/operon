// The session layer (R1 plan Task 14; design §20 §7.1). `SessionState` is the
// port of the server's reference client, `operon_live::session::ClientState`
// (row T12-15): a Transition applies only from the client's current version,
// chunks (`more`) apply together, and a gap changes nothing and asks for a
// resume. `QuerySetState` tracks the query set the client wants against the
// one the server holds, so `ModifyQuerySet` carries only the difference and a
// reconnect sends the whole wanted set.

import { fromWireError, LiveError } from "./errors.js";
import type { QueryUpdate, StateVersion, Transition } from "./gen/loam/live/v1/live_pb.js";
import { canonical, fromValue, type LiveValue } from "./values.js";

/** A session's state version: (query-set version, identity version, ts). */
export interface Version {
  readonly querySet: bigint;
  readonly identity: bigint;
  /** The TSO timestamp every result of the session is valid at. */
  readonly ts: bigint;
}

/** The state of a client with no results. */
export const ZERO: Version = Object.freeze({ querySet: 0n, identity: 0n, ts: 0n });

export function versionOf(v: StateVersion | undefined): Version {
  return v === undefined ? ZERO : { querySet: v.querySet, identity: v.identity, ts: v.ts };
}

export function sameVersion(a: Version, b: Version): boolean {
  return a.querySet === b.querySet && a.identity === b.identity && a.ts === b.ts;
}

function show(v: Version): string {
  return `(${v.querySet}, ${v.identity}, ${v.ts})`;
}

/** One query's result: a value or a per-query error. */
export type QueryResult =
  | { readonly kind: "value"; readonly value: LiveValue }
  | { readonly kind: "error"; readonly error: LiveError };

/** Thrown by `SessionState.apply` for a Transition that does not apply: resume. */
export class VersionGap extends LiveError {
  constructor(message: string) {
    super("FAILED_PRECONDITION", message);
  }
}

/** What applying one Transition message did. */
export type Applied =
  /** A chunk with more to come; nothing changed yet. */
  | { readonly complete: false }
  /** The Transition applied; `changed` are the query ids it set or removed. */
  | { readonly complete: true; readonly changed: ReadonlySet<number> };

/** The client's copy of a session: its version and its results by query id. */
export class SessionState {
  version: Version;
  readonly results = new Map<number, QueryResult>();
  #partial: { start: Version; end: Version; updates: QueryUpdate[] } | undefined;

  constructor(version: Version = ZERO) {
    this.version = version;
  }

  /** Whether a chunked Transition is half received. */
  get partial(): boolean {
    return this.#partial !== undefined;
  }

  /**
   * Applies `t` if it starts at the client's version (for a chunk, if it
   * continues the chunks received so far). A gap throws `VersionGap` and
   * changes nothing.
   */
  apply(t: Transition): Applied {
    const start = versionOf(t.start);
    const end = versionOf(t.end);
    let updates: QueryUpdate[];
    const partial = this.#partial;
    if (partial !== undefined) {
      if (!sameVersion(partial.start, start) || !sameVersion(partial.end, end)) {
        throw new VersionGap(
          `a chunk of ${show(start)} → ${show(end)} arrived inside ${show(partial.start)} → ${show(partial.end)}`,
        );
      }
      updates = partial.updates;
    } else if (sameVersion(start, this.version)) {
      updates = [];
    } else {
      throw new VersionGap(
        `a Transition from ${show(start)} does not apply at ${show(this.version)}`,
      );
    }
    updates.push(...t.updates);
    if (t.more) {
      this.#partial = { start, end, updates };
      return { complete: false };
    }
    this.#partial = undefined;
    const next = new Map<number, QueryResult | undefined>();
    for (const u of updates) {
      switch (u.update.case) {
        case "value":
          next.set(u.queryId, { kind: "value", value: fromValue(u.update.value) });
          break;
        case "error":
          next.set(u.queryId, { kind: "error", error: fromWireError(u.update.value) });
          break;
        case "removed":
          next.set(u.queryId, undefined);
          break;
        default:
          throw new LiveError("INTERNAL", `query ${u.queryId} has an update with nothing set`);
      }
    }
    for (const [id, result] of next) {
      if (result === undefined) this.results.delete(id);
      else this.results.set(id, result);
    }
    this.version = end;
    return { complete: true, changed: new Set(next.keys()) };
  }

  /**
   * Starts over at `version` for a new stream: a resume keeps the results
   * (the first Transition replaces them), an initial start passes `ZERO`.
   */
  restart(version: Version): void {
    this.version = version;
    this.#partial = undefined;
  }

  /** Drops the results of queries not in `ids`; returns the dropped ids. */
  retain(ids: ReadonlySet<number>): number[] {
    const dropped: number[] = [];
    for (const id of this.results.keys()) {
      if (!ids.has(id)) dropped.push(id);
    }
    for (const id of dropped) this.results.delete(id);
    return dropped;
  }
}

/** One query of a query set. */
export interface QueryDef {
  readonly id: number;
  readonly fn: string;
  readonly args: LiveValue;
  /** `canonical` of (fn, args): equal queries share one id. */
  readonly key: string;
}

export function queryKey(fn: string, args: LiveValue): string {
  return `${fn}\u0000${canonical(args)}`;
}

/** A change to bring the server's query set to the wanted one. */
export interface QuerySetDiff {
  readonly baseVersion: bigint;
  readonly newVersion: bigint;
  readonly add: readonly QueryDef[];
  readonly remove: readonly number[];
}

/**
 * The query set the client wants (`wanted`) and the one the server holds
 * (`held`, at `heldVersion`). Versions come from one counter, so every
 * set a client ever sends has a new version.
 */
export class QuerySetState {
  readonly wanted = new Map<number, QueryDef>();
  #byKey = new Map<string, QueryDef>();
  #held = new Map<number, QueryDef>();
  #heldVersion = 0n;
  #counter = 0n;
  #nextId = 1;

  /** The query with this (fn, args), added if new; `created` says which. */
  add(fn: string, args: LiveValue): { def: QueryDef; created: boolean } {
    const key = queryKey(fn, args);
    const existing = this.#byKey.get(key);
    if (existing !== undefined) return { def: existing, created: false };
    const def: QueryDef = { id: this.#nextId++, fn, args, key };
    this.wanted.set(def.id, def);
    this.#byKey.set(key, def);
    return { def, created: true };
  }

  remove(id: number): void {
    const def = this.wanted.get(id);
    if (def === undefined) return;
    this.wanted.delete(id);
    this.#byKey.delete(def.key);
  }

  byKey(key: string): QueryDef | undefined {
    return this.#byKey.get(key);
  }

  get heldVersion(): bigint {
    return this.#heldVersion;
  }

  /**
   * The wanted set as a new stream sends it; the server holds it once the
   * stream opens, so it becomes `held`.
   */
  start(): { version: bigint; queries: QueryDef[] } {
    const version = ++this.#counter;
    this.#held = new Map(this.wanted);
    this.#heldVersion = version;
    return { version, queries: [...this.wanted.values()] };
  }

  /** The ids the server holds. */
  heldIds(): Set<number> {
    return new Set(this.#held.keys());
  }

  /** The next `ModifyQuerySet`, or `undefined` when the sets agree. */
  diff(): QuerySetDiff | undefined {
    const add = [...this.wanted.values()].filter((d) => !this.#held.has(d.id));
    const remove = [...this.#held.keys()].filter((id) => !this.wanted.has(id));
    if (add.length === 0 && remove.length === 0) return undefined;
    return { baseVersion: this.#heldVersion, newVersion: ++this.#counter, add, remove };
  }

  /** The server accepted `d`. */
  accepted(d: QuerySetDiff): void {
    for (const def of d.add) this.#held.set(def.id, def);
    for (const id of d.remove) this.#held.delete(id);
    this.#heldVersion = d.newVersion;
  }
}
