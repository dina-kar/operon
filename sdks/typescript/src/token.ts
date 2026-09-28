/** Consistency tokens (overview §6.5): the text form `v1:s<stream>/p<partition>@<offset>,…`. */

const U64_MAX = 2n ** 64n - 1n;
const U32_MAX = 2 ** 32 - 1;
const ITEM = /^s(0|[1-9]\d*)\/p(0|[1-9]\d*)@(0|[1-9]\d*)$/;
const PREFIX = "v1:";

export type TokenItem = readonly [stream: bigint, partition: number, nextOffset: bigint];

function compare(a: TokenItem, b: TokenItem): number {
  if (a[0] !== b[0]) return a[0] < b[0] ? -1 : 1;
  return a[1] - b[1];
}

function freeze(items: Iterable<TokenItem>): ReadonlyArray<TokenItem> {
  return Object.freeze(
    Array.from(items, (item) => Object.freeze([item[0], item[1], item[2]] as const)).sort(compare),
  );
}

/**
 * A read-your-writes token: per (stream, partition), the next offset a read must see.
 * `items` are sorted, one per (stream, partition).
 */
export class ConsistencyToken {
  readonly items: ReadonlyArray<TokenItem>;

  /** Items are sorted; use `parse` for checked input. */
  constructor(items: Iterable<TokenItem> = []) {
    this.items = freeze(items);
  }

  /** Parses the text form; throws `RangeError` on anything outside the grammar. */
  static parse(text: string): ConsistencyToken {
    if (typeof text !== "string") {
      throw new TypeError(`a consistency token is a string, not ${typeof text}`);
    }
    if (!text.startsWith(PREFIX)) {
      throw new RangeError(`not a v1 consistency token: ${JSON.stringify(text)}`);
    }
    const rest = text.slice(PREFIX.length);
    if (rest === "") return new ConsistencyToken();
    const seen = new Set<string>();
    const items: TokenItem[] = [];
    for (const part of rest.split(",")) {
      const match = ITEM.exec(part);
      if (match === null) {
        throw new RangeError(
          `malformed consistency token item ${JSON.stringify(part)} in ${JSON.stringify(text)}`,
        );
      }
      const stream = BigInt(match[1] as string);
      const partition = BigInt(match[2] as string);
      const offset = BigInt(match[3] as string);
      if (stream > U64_MAX || offset > U64_MAX || partition > BigInt(U32_MAX)) {
        throw new RangeError(`consistency token item out of range: ${JSON.stringify(part)}`);
      }
      const key = `${stream}/${partition}`;
      if (seen.has(key)) {
        throw new RangeError(
          `duplicate stream/partition in consistency token: ${JSON.stringify(part)}`,
        );
      }
      seen.add(key);
      items.push([stream, Number(partition), offset]);
    }
    return new ConsistencyToken(items);
  }

  toString(): string {
    return PREFIX + this.items.map(([s, p, o]) => `s${s}/p${p}@${o}`).join(",");
  }

  toJSON(): string {
    return this.toString();
  }

  /** The token at least as new as every input: the highest offset per (stream, partition). */
  merge(...others: ConsistencyToken[]): ConsistencyToken {
    const best = new Map<string, TokenItem>();
    for (const token of [this, ...others]) {
      for (const item of token.items) {
        const key = `${item[0]}/${item[1]}`;
        const current = best.get(key);
        if (current === undefined || item[2] > current[2]) best.set(key, item);
      }
    }
    return new ConsistencyToken(best.values());
  }
}
