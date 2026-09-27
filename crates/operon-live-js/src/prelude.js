// `loam:server`, the server-function API of Loam Live (design §20 §6; R1
// plan Task 13). The engine evaluates this module in every fresh context
// before the bundle: it takes the host object the engine left in
// `globalThis.__loam`, makes the context deterministic (§6.2) and freezes
// the built-in globals, then the bundle imports `query` and `mutation`.

const host = globalThis.__loam;
delete globalThis.__loam;

const KIND = Symbol("loam.kind");
const ERROR_ID = Symbol("loam.error");
const CRYPTO =
  "crypto randomness is not available in queries and mutations; use an action";

// Thrown by `crypto.getRandomValues` and `crypto.randomUUID`, which are never
// seeded (§6.2): a deterministic value must never pass for a random one.
export class DeterminismError extends Error {
  constructor(message) {
    super(message);
    Object.defineProperty(this, "name", { value: "DeterminismError", writable: true, configurable: true });
  }
}

function define(kind, def) {
  if (typeof def === "function") {
    def = { handler: def };
  }
  if (def === null || typeof def !== "object" || typeof def.handler !== "function") {
    throw new TypeError(`${kind}() takes { args?, handler }`);
  }
  const f = { args: def.args, handler: def.handler };
  Object.defineProperty(f, KIND, { value: kind });
  return Object.freeze(f);
}

/** A query: reads one snapshot and is rerun when what it read changes. */
export function query(def) {
  return define("query", def);
}

/** A mutation: one transaction, rerun from scratch on a conflict. */
export function mutation(def) {
  return define("mutation", def);
}

function kindOf(value) {
  return (value !== null && typeof value === "object" && value[KIND]) || null;
}

// --- The database API (`ctx.db`); every call is a host call.

function call(op, args) {
  return host.call(op, args);
}

class IndexRange {
  constructor() {
    this.eqFields = [];
    this.eqValues = [];
    this.rangeField = undefined;
    this.lower = undefined;
    this.upper = undefined;
  }

  eq(field, value) {
    if (this.rangeField !== undefined) {
      throw new TypeError("withIndex: eq() comes before gt, gte, lt and lte");
    }
    this.eqFields.push(field);
    this.eqValues.push(value);
    return this;
  }

  gt(field, value) {
    return this.bound("lower", field, value, false);
  }

  gte(field, value) {
    return this.bound("lower", field, value, true);
  }

  lt(field, value) {
    return this.bound("upper", field, value, false);
  }

  lte(field, value) {
    return this.bound("upper", field, value, true);
  }

  bound(side, field, value, inclusive) {
    if (this.rangeField !== undefined && this.rangeField !== field) {
      throw new TypeError(
        `withIndex: the range is on '${this.rangeField}', not '${field}'`,
      );
    }
    if (this[side] !== undefined) {
      throw new TypeError(`withIndex: the ${side} bound is set twice`);
    }
    this.rangeField = field;
    this[side] = { value, inclusive };
    return this;
  }
}

class Query {
  #table;
  #index = "by_creation_time";
  #range = new IndexRange();
  #order = "asc";

  constructor(table) {
    this.#table = table;
  }

  withIndex(name, build) {
    this.#index = name;
    if (build !== undefined) {
      const range = new IndexRange();
      const built = build(range);
      this.#range = built instanceof IndexRange ? built : range;
    }
    return this;
  }

  order(order) {
    if (order !== "asc" && order !== "desc") {
      throw new TypeError(`order() takes "asc" or "desc", not ${String(order)}`);
    }
    this.#order = order;
    return this;
  }

  take(n) {
    if (!Number.isInteger(n) || n < 0) {
      throw new TypeError(`take() takes a non-negative integer, not ${String(n)}`);
    }
    return this.#run(n);
  }

  collect() {
    return this.#run(undefined);
  }

  async first() {
    const docs = await this.#run(1);
    return docs.length > 0 ? docs[0] : null;
  }

  #run(limit) {
    const r = this.#range;
    return call("query", {
      table: this.#table,
      index: this.#index,
      eqFields: r.eqFields,
      eq: r.eqValues,
      rangeField: r.rangeField,
      lower: r.lower,
      upper: r.upper,
      order: this.#order,
      limit: limit === undefined ? undefined : BigInt(limit),
    });
  }
}

const db = Object.freeze({
  get: (id) => call("get", { id }),
  query: (table) => new Query(table),
  insert: (table, doc) => call("insert", { table, fields: doc }),
  patch: (id, fields) => call("patch", { id, fields }),
  replace: (id, doc) => call("replace", { id, fields: doc }),
  delete: (id) => call("delete", { id }),
});

// --- What the engine calls.

let defs = new Map();

// Indexes the bundle's exports: an object export `m` maps each of its
// function properties `f` to "m:f"; an export whose name holds a ':' is a
// path as is. Returns [path, kind] pairs.
host.index = (ns) => {
  defs = new Map();
  for (const name of Object.keys(ns)) {
    const value = ns[name];
    const kind = kindOf(value);
    if (kind !== null) {
      if (!name.includes(":")) {
        throw new TypeError(
          `export '${name}' is a ${kind}: functions are addressed as module:export, ` +
            `so export it inside an object (export const messages = { ${name} })`,
        );
      }
      defs.set(name, value);
    } else if (value !== null && typeof value === "object" && !Array.isArray(value)) {
      for (const key of Object.keys(value)) {
        if (kindOf(value[key]) !== null) {
          defs.set(`${name}:${key}`, value[key]);
        }
      }
    }
  }
  return [...defs].map(([path, def]) => [path, def[KIND]]).sort();
};

host.invoke = (path, kind, args) => {
  const def = defs.get(path);
  if (def === undefined || def[KIND] !== kind) {
    throw new TypeError(`no ${kind} ${path} in this bundle`);
  }
  return (async () => def.handler({ db }, args))();
};

host.error = (id, code, message) => {
  const e = new Error(message);
  Object.defineProperty(e, "name", { value: "LoamError", writable: true, configurable: true });
  e.code = code;
  Object.defineProperty(e, ERROR_ID, { value: id });
  return e;
};

host.errorId = (e) =>
  e !== null && typeof e === "object" && typeof e[ERROR_ID] === "number" ? e[ERROR_ID] : -1;

host.describe = (e) => {
  if (e instanceof Error) {
    const head = `${e.name}: ${e.message}`;
    return typeof e.stack === "string" && e.stack !== "" ? `${head}\n${e.stack}` : head;
  }
  try {
    return String(e);
  } catch {
    return "a value that cannot be printed";
  }
};

host.bytesOf = (v) => {
  if (v instanceof ArrayBuffer) {
    return Array.from(new Uint8Array(v));
  }
  if (v instanceof Uint8Array) {
    return Array.from(v);
  }
  return null;
};

host.fitsI64 = (v) => BigInt.asIntN(64, v) === v;

// --- Determinism (§6.2): the clock is the start timestamp, `Math.random` is
// seeded from it and the request id, crypto randomness throws, and there
// are no timers, `fetch` or WebAssembly.

const RealDate = Date;
const now = () => host.now();
function LoamDate(...args) {
  if (new.target === undefined) {
    return new RealDate(now()).toString();
  }
  return args.length === 0 ? new RealDate(now()) : new RealDate(...args);
}
Object.defineProperty(LoamDate, "prototype", { value: RealDate.prototype });
Object.defineProperty(LoamDate, "now", { value: now });
Object.defineProperty(LoamDate, "parse", { value: RealDate.parse });
Object.defineProperty(LoamDate, "UTC", { value: RealDate.UTC });
Object.defineProperty(RealDate.prototype, "constructor", { value: LoamDate });
globalThis.Date = LoamDate;

Math.random = () => host.random();

globalThis.crypto = {
  getRandomValues() {
    throw new DeterminismError(CRYPTO);
  },
  randomUUID() {
    throw new DeterminismError(CRYPTO);
  },
};
globalThis.DeterminismError = DeterminismError;

const quiet = () => undefined;
globalThis.console = { log: quiet, info: quiet, warn: quiet, error: quiet, debug: quiet };

for (const name of [
  "setTimeout",
  "setInterval",
  "setImmediate",
  "clearTimeout",
  "clearInterval",
  "fetch",
  "XMLHttpRequest",
  "WebAssembly",
]) {
  delete globalThis[name];
}

// --- Freeze the built-ins, then the global object, before the bundle runs.
//
// Frozen prototypes break plain assignments that shadow them (`this.name =
// "MyError"` in an Error subclass throws in strict code), so the data
// properties such code assigns become accessors whose setter defines an own
// property on the instance instead.

function tame(proto, key) {
  const d = Object.getOwnPropertyDescriptor(proto, key);
  if (d === undefined || !("value" in d)) {
    return;
  }
  const value = d.value;
  Object.defineProperty(proto, key, {
    get() {
      return value;
    },
    set(v) {
      if (this === proto) {
        throw new TypeError(`Cannot assign to read only property '${String(key)}'`);
      }
      Object.defineProperty(this, key, { value: v, writable: true, enumerable: true, configurable: true });
    },
    enumerable: d.enumerable,
    configurable: false,
  });
}
for (const key of [
  "constructor",
  "toString",
  "toLocaleString",
  "valueOf",
  "hasOwnProperty",
  "isPrototypeOf",
  "propertyIsEnumerable",
]) {
  tame(Object.prototype, key);
}
for (const E of [
  Error,
  EvalError,
  RangeError,
  ReferenceError,
  SyntaxError,
  TypeError,
  URIError,
  AggregateError,
  DeterminismError,
]) {
  tame(E.prototype, "name");
  tame(E.prototype, "message");
  tame(E.prototype, "constructor");
}
tame(Function.prototype, "toString");
tame(Function.prototype, "constructor");
tame(Array.prototype, "constructor");
tame(Array.prototype, "toString");
tame(Promise.prototype, "constructor");

const seen = new WeakSet();
function freeze(o) {
  if (o === null || (typeof o !== "object" && typeof o !== "function") || seen.has(o)) {
    return;
  }
  seen.add(o);
  for (const key of Reflect.ownKeys(o)) {
    const d = Object.getOwnPropertyDescriptor(o, key);
    if (d === undefined) {
      continue;
    }
    if ("value" in d) {
      freeze(d.value);
    } else {
      freeze(d.get);
      freeze(d.set);
    }
  }
  freeze(Object.getPrototypeOf(o));
  Object.freeze(o);
}
freeze(Object.getPrototypeOf(async () => {}));
freeze(Object.getPrototypeOf(function* () {}));
freeze(Object.getPrototypeOf(async function* () {}));
freeze(Object.getPrototypeOf([][Symbol.iterator]()));
freeze(Object.getPrototypeOf(new Map()[Symbol.iterator]()));
freeze(Object.getPrototypeOf(new Set()[Symbol.iterator]()));
freeze(Object.getPrototypeOf(""[Symbol.iterator]()));
freeze(globalThis);
