// Starts the conformance fixture server (sdks/conformance/fixture-server.mjs)
// and hands back its URL.
//
// Every SDK's conformance suite needs one of these, and each should be able to
// run against a live `loams dev` instead: `LOAMS_TEST_ENDPOINT` short-circuits
// the fixture server entirely, which is how the same suite runs in CI against a
// recording and on a developer machine against the real thing.

import { spawn, type ChildProcess } from 'node:child_process';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));

/** Where the server script and the recorded corpus are, from this file. */
export const FIXTURE_SERVER = resolve(here, '../../../../../conformance/fixture-server.mjs');
export const FIXTURES = resolve(here, '../../../../../fixtures');

/** A running server, and how to stop it. */
export interface FixtureServer {
  readonly endpoint: string;
  /** Whether this is a recording replay or a live `loams dev`. */
  readonly live: boolean;
  stop(): Promise<void>;
}

/** The endpoint a suite should talk to: a live server if one was named, a
 * fixture server otherwise. */
export async function startFixtureServer(): Promise<FixtureServer> {
  const live = process.env.LOAMS_TEST_ENDPOINT;
  if (live !== undefined && live !== '') {
    return {
      endpoint: live.replace(/\/$/, ''),
      live: true,
      stop: () => Promise.resolve(),
    };
  }
  const child = spawn(
    process.execPath,
    [FIXTURE_SERVER, '--fixtures', FIXTURES, '--port', '0'],
    { stdio: ['ignore', 'pipe', 'inherit'] },
  );
  const endpoint = await new Promise<string>((resolvePromise, rejectPromise) => {
    const timer = setTimeout(() => {
      rejectPromise(new Error('the fixture server did not report a port in 10 s'));
    }, 10_000);
    let buffered = '';
    child.stdout?.on('data', (chunk: Buffer) => {
      buffered += chunk.toString('utf8');
      const newline = buffered.indexOf('\n');
      if (newline < 0) {
        return;
      }
      clearTimeout(timer);
      resolvePromise(JSON.parse(buffered.slice(0, newline)).url as string);
    });
    child.on('exit', (code) => {
      clearTimeout(timer);
      rejectPromise(new Error(`the fixture server exited with ${code}`));
    });
  });
  return {
    endpoint,
    live: false,
    stop: () => stopChild(child),
  };
}

function stopChild(child: ChildProcess): Promise<void> {
  return new Promise((resolvePromise) => {
    if (child.exitCode !== null) {
      resolvePromise();
      return;
    }
    child.on('exit', () => resolvePromise());
    child.kill('SIGTERM');
  });
}
