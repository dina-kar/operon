/**
 * Collection schemas: typed fields, dense vectors and sparse vectors (plan M1.6 Task 6).
 *
 * Plain objects; the wire form is `wire.encodeSchema` / `wire.decodeSchema`.
 */

/** A field's kind; `text` carries its analyzer and whether positions are indexed. */
export type Kind =
  | { text: { analyzer: string; positions: boolean } }
  | "keyword"
  | "i64"
  | "f64"
  | "bool"
  | "date"
  | "uuid"
  | "json";

export type Distance = "cosine" | "dot" | "euclid" | "manhattan";
export type SparseModifier = "none" | "idf";
export type Dynamic = "strict" | "ignore" | "map";

/** A typed field; `sourcePath` defaults to `name`, `indexed` to true, `fast` to false. */
export interface FieldInput {
  name: string;
  kind: Kind;
  sourcePath?: string;
  indexed?: boolean;
  fast?: boolean;
}

/** A dense vector field; `distance` defaults to `"cosine"`. */
export interface VectorInput {
  name: string;
  dim: number;
  distance?: Distance;
}

/** A sparse vector field (overview A26); `modifier` defaults to `"none"`. */
export interface SparseVectorFieldInput {
  name: string;
  modifier?: SparseModifier;
}

/** A collection's schema; `dynamic` defaults to `"strict"`, `maxFields` to 1000. */
export interface SchemaInput {
  fields?: FieldInput[];
  vectors?: VectorInput[];
  sparseVectors?: SparseVectorFieldInput[];
  dynamic?: Dynamic;
  maxFields?: number;
}

const DISTANCES: readonly string[] = ["cosine", "dot", "euclid", "manhattan"];
const MODIFIERS: readonly string[] = ["none", "idf"];
const MAX_DIM = 65535;

function field(
  name: string,
  kind: Kind,
  opts: { fast?: boolean | undefined; sourcePath?: string | undefined },
  fast: boolean,
): FieldInput {
  const out: FieldInput = { name, kind, fast: opts.fast ?? fast };
  if (opts.sourcePath !== undefined) out.sourcePath = opts.sourcePath;
  return out;
}

/** Field constructors, as Python's `operon.schema`: keyword, numeric, bool and date fields are fast. */
export const s = {
  /** A full-text field (not fast). */
  text(
    name: string,
    opts: { analyzer?: string; positions?: boolean; sourcePath?: string } = {},
  ): FieldInput {
    const kind: Kind = {
      text: { analyzer: opts.analyzer ?? "standard", positions: opts.positions ?? true },
    };
    return field(name, kind, { sourcePath: opts.sourcePath }, false);
  },
  keyword(name: string, opts: { fast?: boolean; sourcePath?: string } = {}): FieldInput {
    return field(name, "keyword", opts, true);
  },
  i64(name: string, opts: { fast?: boolean; sourcePath?: string } = {}): FieldInput {
    return field(name, "i64", opts, true);
  },
  f64(name: string, opts: { fast?: boolean; sourcePath?: string } = {}): FieldInput {
    return field(name, "f64", opts, true);
  },
  bool(name: string, opts: { fast?: boolean; sourcePath?: string } = {}): FieldInput {
    return field(name, "bool", opts, true);
  },
  date(name: string, opts: { fast?: boolean; sourcePath?: string } = {}): FieldInput {
    return field(name, "date", opts, true);
  },
  uuid(name: string, opts: { sourcePath?: string } = {}): FieldInput {
    return field(name, "uuid", opts, false);
  },
  json(name: string, opts: { sourcePath?: string } = {}): FieldInput {
    return field(name, "json", opts, false);
  },
  /** A dense vector field; `RangeError` unless `1 <= dim <= 65535` and the distance is known. */
  vector(name: string, dim: number, distance: Distance = "cosine"): VectorInput {
    if (!Number.isInteger(dim) || dim < 1 || dim > MAX_DIM) {
      throw new RangeError(
        `vector ${JSON.stringify(name)}: dim must be an integer in 1..${MAX_DIM}, got ${dim}`,
      );
    }
    if (!DISTANCES.includes(distance)) {
      throw new RangeError(
        `vector ${JSON.stringify(name)}: distance must be one of ${DISTANCES.join(", ")}, got ${String(distance)}`,
      );
    }
    return { name, dim, distance };
  },
  /** A sparse vector field; `RangeError` for an empty name or an unknown modifier. */
  sparseVector(name: string, opts: { modifier?: SparseModifier } = {}): SparseVectorFieldInput {
    if (typeof name !== "string" || name === "")
      throw new RangeError("a sparse vector needs a name");
    const modifier = opts.modifier ?? "none";
    if (!MODIFIERS.includes(modifier)) {
      throw new RangeError(
        `sparse vector ${JSON.stringify(name)}: modifier must be "none" or "idf", got ${String(modifier)}`,
      );
    }
    return { name, modifier };
  },
};
