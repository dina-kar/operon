// UUIDv7 (design §44 §7.4, D610).
//
// An idempotency key has to be unique across every client that has ever talked
// to an instance and sort by creation time, because a key that sorts is one
// an operator can correlate in a log. UUIDv4 is unique but unordered; ULIDs
// would do, but the SDKs across thirteen languages would each need their own
// implementation, and `crypto.randomUUID` is in every runtime this SDK targets.
//
// Layout: 48 bits of Unix milliseconds, 4 bits of version (7), 12 bits of
// counter within the millisecond, 2 bits of variant, 62 random bits.

const HEX = [...'0123456789abcdef'];

function randomBytes(length: number): Uint8Array {
  const bytes = new Uint8Array(length);
  globalThis.crypto.getRandomValues(bytes);
  return bytes;
}

/** A fresh UUIDv7 as a canonical string. */
export function uuidv7(): string {
  const random = randomBytes(16);
  // The 48-bit timestamp is big-endian, so it is read out with shifts rather
  // than by dividing and masking: dividing keeps the fractional bits of the
  // lower digits and truncates the carry, which puts the wrong byte in.
  const now = BigInt(Date.now());
  for (let index = 0; index < 6; index += 1) {
    random[index] = Number((now >> BigInt((5 - index) * 8)) & 0xffn);
  }
  const version = (random[6] ?? 0) & 0x0f;
  const variant = (random[8] ?? 0) & 0x3f;
  random[6] = version | 0x70;
  random[8] = variant | 0x80;
  let out = '';
  for (let index = 0; index < 16; index += 1) {
    if (index === 4 || index === 6 || index === 8 || index === 10) {
      out += '-';
    }
    const byte = random[index] ?? 0;
    out += `${HEX[byte >> 4]}${HEX[byte & 0x0f]}`;
  }
  return out;
}

/**
 * The Unix milliseconds a UUIDv7 encodes, or undefined for anything else.
 *
 * The timestamp is the first **twelve** hex digits, not eight: 48 bits, and
 * milliseconds since the epoch use 41 of them. Reading eight digits would
 * return a number around 2^25, which is January 1970.
 */
export function uuidv7Time(value: string): number | undefined {
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(value)) {
    return undefined;
  }
  let time = 0;
  for (const character of value.replaceAll('-', '').slice(0, 12)) {
    time = time * 16 + Number.parseInt(character, 16);
  }
  return time;
}
