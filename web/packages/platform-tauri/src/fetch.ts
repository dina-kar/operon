// `tauriFetch`: the standard fetch signature over the Rust bridge
// (§37 §6.4, D430). Rust performs the request (origin allowlist, bearer
// injection, header stripping, redirect rules) and streams the response
// back over a Tauri Channel; this turns the events into a Response whose
// body is a ReadableStream, so connect-es server streams work unchanged.

import { Channel, invoke as tauriInvoke } from '@tauri-apps/api/core';

/** The events `net_fetch` sends (net.rs `FetchEvent`). */
export type FetchEvent =
  | { type: 'head'; status: number; headers: [string, string][] }
  | { type: 'chunk'; data: string }
  | { type: 'end' }
  | { type: 'error'; message: string };

export type Invoke = <T>(cmd: string, args?: Record<string, unknown>) => Promise<T>;

/** What `Channel` needs to be, so tests can pass a fake. */
export interface ChannelLike<T> {
  onmessage: (message: T) => void;
}

export type ChannelFactory = () => ChannelLike<FetchEvent>;

const NULL_BODY = new Set([101, 103, 204, 205, 304]);

function toBase64(bytes: Uint8Array): string {
  let binary = '';
  for (let i = 0; i < bytes.length; i += 0x8000) {
    binary += String.fromCharCode(...bytes.subarray(i, i + 0x8000));
  }
  return btoa(binary);
}

function fromBase64(data: string): Uint8Array {
  const binary = atob(data);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
  return bytes;
}

export function createTauriFetch(
  invoke: Invoke = tauriInvoke,
  channel: ChannelFactory = () => new Channel<FetchEvent>(),
): typeof globalThis.fetch {
  return async (input, init) => {
    const request = new Request(input, init);
    const bytes = request.body ? new Uint8Array(await request.arrayBuffer()) : undefined;
    const signal = init?.signal ?? request.signal;
    if (signal?.aborted) throw new DOMException('The request was aborted', 'AbortError');
    return new Promise<Response>((resolve, reject) => {
      let id: number | undefined;
      let head = false;
      let controller: ReadableStreamDefaultController<Uint8Array> | undefined;
      const abort = () => {
        if (id !== undefined) invoke('net_abort', { id }).catch(() => {});
        const error = new DOMException('The request was aborted', 'AbortError');
        if (head) controller?.error(error);
        else reject(error);
      };
      signal?.addEventListener('abort', abort, { once: true });
      const body = new ReadableStream<Uint8Array>({
        start(c) {
          controller = c;
        },
        cancel() {
          if (id !== undefined) invoke('net_abort', { id }).catch(() => {});
        },
      });
      const events = channel();
      events.onmessage = (event) => {
        switch (event.type) {
          case 'head':
            head = true;
            resolve(
              new Response(NULL_BODY.has(event.status) ? null : body, {
                status: event.status,
                headers: event.headers,
              }),
            );
            break;
          case 'chunk':
            controller?.enqueue(fromBase64(event.data));
            break;
          case 'end':
            signal?.removeEventListener('abort', abort);
            controller?.close();
            break;
          case 'error':
            signal?.removeEventListener('abort', abort);
            if (head) controller?.error(new TypeError(event.message));
            else reject(new TypeError(event.message));
            break;
        }
      };
      invoke<number>('net_fetch', {
        request: {
          url: request.url,
          method: request.method,
          headers: [...request.headers.entries()],
          body: bytes && bytes.length > 0 ? toBase64(bytes) : undefined,
        },
        onEvent: events,
      }).then(
        (requestId) => {
          id = requestId;
        },
        (error: unknown) => reject(new TypeError(String(error))),
      );
    });
  };
}
