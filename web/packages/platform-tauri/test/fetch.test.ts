import { describe, expect, it } from 'vitest';
import { createTauriFetch, type FetchEvent } from '../src/fetch.js';

/** A fake bridge: answers net_fetch by replaying `events` on the channel. */
function bridge(
  events: FetchEvent[] | ((req: { url: string; headers: [string, string][] }) => FetchEvent[]),
) {
  const calls: { cmd: string; args?: Record<string, unknown> }[] = [];
  const invoke = async <T>(cmd: string, args?: Record<string, unknown>): Promise<T> => {
    calls.push({ cmd, args });
    if (cmd === 'net_fetch') {
      const channel = args?.onEvent as { onmessage: (e: FetchEvent) => void };
      const request = args?.request as { url: string; headers: [string, string][] };
      const list = typeof events === 'function' ? events(request) : events;
      queueMicrotask(() => {
        for (const e of list) channel.onmessage(e);
      });
      return 7 as T;
    }
    return undefined as T;
  };
  return { invoke, calls, channel: () => ({ onmessage: (_: FetchEvent) => {} }) };
}

const b64 = (s: string) => btoa(s);

describe('tauriFetch', () => {
  it('turns head, chunks and end into a streaming Response', async () => {
    const fake = bridge([
      { type: 'head', status: 200, headers: [['content-type', 'application/json']] },
      { type: 'chunk', data: b64('{"a":') },
      { type: 'chunk', data: b64('1}') },
      { type: 'end' },
    ]);
    const fetch = createTauriFetch(fake.invoke, fake.channel);
    const response = await fetch('http://127.0.0.1:8084/x', {
      method: 'POST',
      body: '{}',
      headers: { 'content-type': 'application/json' },
    });
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({ a: 1 });
    const request = fake.calls[0]?.args?.request as { method: string; body: string };
    expect(request.method).toBe('POST');
    expect(atob(request.body)).toBe('{}');
  });

  it('rejects like fetch when the bridge refuses before the head', async () => {
    const fake = bridge([
      { type: 'error', message: "refused: https://evil.example is not this environment's origin" },
    ]);
    const fetch = createTauriFetch(fake.invoke, fake.channel);
    await expect(fetch('https://evil.example/')).rejects.toThrow(TypeError);
  });

  it('errors the body stream when the bridge fails mid-body', async () => {
    const fake = bridge([
      { type: 'head', status: 200, headers: [] },
      { type: 'chunk', data: b64('par') },
      { type: 'error', message: 'connection reset' },
    ]);
    const response = await createTauriFetch(fake.invoke, fake.channel)('http://127.0.0.1:1/');
    await expect(response.text()).rejects.toThrow('connection reset');
  });

  it('aborts through net_abort', async () => {
    const fake = bridge([{ type: 'head', status: 200, headers: [] }]);
    const controller = new AbortController();
    const response = await createTauriFetch(fake.invoke, fake.channel)('http://127.0.0.1:1/', {
      signal: controller.signal,
    });
    expect(response.ok).toBe(true);
    controller.abort();
    await new Promise((r) => setTimeout(r, 0));
    expect(fake.calls.map((c) => c.cmd)).toEqual(['net_fetch', 'net_abort']);
    expect(fake.calls[1]?.args).toEqual({ id: 7 });
  });

  it('a 204 has no body', async () => {
    const fake = bridge([{ type: 'head', status: 204, headers: [] }, { type: 'end' }]);
    const response = await createTauriFetch(fake.invoke, fake.channel)('http://127.0.0.1:1/');
    expect(response.status).toBe(204);
    expect(response.body).toBeNull();
  });
});
