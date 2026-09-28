// Optimistic updates (R1 plan Task 14; design §20 §7.1). A mutation may carry
// an update that edits the client's query results at once. Pending updates are
// layered, in the order the mutations were made, over the server's results;
// one is dropped when its mutation fails, or once the session's `ts` reaches
// the mutation's `commitTs`, when the server's results include its write.
import { queryKey } from "./session.js";
import type { LiveValue } from "./values.js";

/** The query results an optimistic update reads and edits. */
export interface LocalStore {
  /** The query's current result, or `undefined` while it has none. */
  getQuery<T = LiveValue>(fn: string, args?: LiveValue): T | undefined;
  /** Sets the query's result; `undefined` shows it as loading. */
  setQuery<T = LiveValue>(fn: string, args: LiveValue | undefined, value: T | undefined): void;
}

export type OptimisticUpdate = (local: LocalStore) => void;

interface Pending {
  readonly id: number;
  readonly update: OptimisticUpdate;
  commitTs: bigint | undefined;
}

/** The pending optimistic updates of one client. */
export class OptimisticLayer {
  #pending: Pending[] = [];
  #nextId = 1;

  get size(): number {
    return this.#pending.length;
  }

  /** Adds an update; returns its handle. */
  push(update: OptimisticUpdate): number {
    const id = this.#nextId++;
    this.#pending.push({ id, update, commitTs: undefined });
    return id;
  }

  /** The update's mutation committed at `commitTs`. */
  committed(id: number, commitTs: bigint): void {
    const p = this.#pending.find((x) => x.id === id);
    if (p !== undefined) p.commitTs = commitTs;
  }

  /** Drops one update (its mutation failed, or nothing will show its commit). */
  drop(id: number): boolean {
    const before = this.#pending.length;
    this.#pending = this.#pending.filter((x) => x.id !== id);
    return this.#pending.length !== before;
  }

  /** Drops every update whose commit is visible at `ts`; returns whether any was. */
  prune(ts: bigint): boolean {
    const before = this.#pending.length;
    this.#pending = this.#pending.filter((x) => x.commitTs === undefined || x.commitTs > ts);
    return this.#pending.length !== before;
  }

  clear(): void {
    this.#pending = [];
  }

  /**
   * Runs the pending updates over the server's results (`server`, by query
   * key) and returns the results they set, by query key. An update that
   * throws is skipped.
   */
  view(server: (key: string) => LiveValue | undefined): Map<string, LiveValue | undefined> {
    const local = new Map<string, LiveValue | undefined>();
    const store: LocalStore = {
      getQuery<T = LiveValue>(fn: string, args?: LiveValue): T | undefined {
        const key = queryKey(fn, args ?? null);
        const v = local.has(key) ? local.get(key) : server(key);
        // A copy, so an update that edits in place cannot touch server results.
        return (v === undefined ? undefined : structuredClone(v)) as T | undefined;
      },
      setQuery<T = LiveValue>(fn: string, args: LiveValue | undefined, value: T | undefined): void {
        local.set(queryKey(fn, args ?? null), value as LiveValue | undefined);
      },
    };
    for (const p of this.#pending) {
      try {
        p.update(store);
      } catch {
        // An optimistic update is a guess; the server's result replaces it.
      }
    }
    return local;
  }
}
