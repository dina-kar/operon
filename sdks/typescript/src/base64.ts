/** Standard base64 over bytes, with `btoa`/`atob` (plan M1.6 Task 5 rule 6). */

const CHUNK = 32_768;
const utf8 = new TextEncoder();

/** Bytes as they are, a string as its UTF-8 encoding. */
export function toBytes(value: Uint8Array | string): Uint8Array {
  if (typeof value === "string") return utf8.encode(value);
  if (value instanceof Uint8Array) return value;
  throw new TypeError(`expected a Uint8Array or string, not ${typeof value}`);
}

export function encodeBase64(value: Uint8Array | string): string {
  const bytes = toBytes(value);
  const parts: string[] = [];
  for (let i = 0; i < bytes.length; i += CHUNK) {
    parts.push(String.fromCharCode(...bytes.subarray(i, i + CHUNK)));
  }
  return btoa(parts.join(""));
}

export function decodeBase64(text: string): Uint8Array {
  const binary = atob(text);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i += 1) {
    bytes[i] = binary.charCodeAt(i);
  }
  return bytes;
}
