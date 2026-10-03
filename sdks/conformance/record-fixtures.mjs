// Records the conformance fixture corpus against a real `loams` server.
//
// This is SDK1 Task 4's recorder in its smallest useful form: it speaks raw
// HTTP to whatever `LOAMS_TEST_ENDPOINT` names, saves the exact status,
// headers and body bytes of each case under `sdks/fixtures/recorded/`, and
// writes the corpus index the per-SDK conformance suites replay.
//
// Why a recording at all (design §44 §10.4): the per-SDK suite has to run in
// CI on every PR, and CI cannot afford to boot a Rust server for each of the
// thirteen SDKs on every change. So the corpus is captured once from a real
// server, committed, and replayed by `fixture-server.mjs`. `run.sh` still
// offers the live path (`LOAMS_TEST_ENDPOINT`) for the job that can afford it.
//
// Every RPC is recorded in both unary encodings (proto3 JSON and protobuf),
// because an SDK's transport picks one and the corpus cannot know which. The
// browser's gRPC-Web path and the streaming refusal get a case each: they are
// not interchangeable with the unary ones, and the unavailable-service path in
// particular arrives differently on a server stream.
//
// Usage:
//   LOAMS_TEST_ENDPOINT=http://127.0.0.1:8080 node sdks/conformance/record-fixtures.mjs
//
// A body is stored base64 when it is not UTF-8 JSON, so a gRPC-Web frame or a
// Connect streaming envelope replays byte for byte.

import { mkdir, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const outDir = join(here, '..', 'fixtures', 'recorded');
const endpoint = process.env.LOAMS_TEST_ENDPOINT;
if (!endpoint) {
  console.error('LOAMS_TEST_ENDPOINT is not set; nothing recorded');
  process.exit(2);
}
const base = endpoint.replace(/\/$/, '');

/** The RPCs the corpus covers, and what each one is. */
const RPCS = [
  {
    name: 'instance_get_instance',
    path: '/loams.instance.v1.InstanceService/GetInstance',
    about:
      'A successful unary call, no auth: what this instance is. This is the ' +
      'case a client makes before anything else, on a cold start.',
    reason: null,
  },
  {
    name: 'instance_who_am_i',
    path: '/loams.instance.v1.InstanceService/WhoAmI',
    about:
      'A structured-reason error: `unimplemented` carrying an ErrorInfo whose ' +
      'reason is `not_implemented`, because this build has no authentication ' +
      'yet. An SDK branches on the reason, never on the message.',
    reason: 'not_implemented',
  },
  {
    name: 'live_query',
    path: '/loams.live.v1.LiveService/Query',
    about:
      'The unavailable-service path on a unary RPC: `loams.live.v1` is a ' +
      '`full`-variant engine, so every one of its RPCs answers `unimplemented` ' +
      'with reason `feature_not_in_variant` and the variant in `metadata`.',
    reason: 'feature_not_in_variant',
  },
];

/** The encodings a case is recorded in. */
const ENCODINGS = [
  {
    name: 'json',
    about: 'The proto3 JSON mapping, which is what `curl` sends (design §44 §4).',
    contentType: 'application/json',
    body: '{}',
  },
  {
    name: 'proto',
    about: 'The binary encoding, which is what an SDK sends by default.',
    contentType: 'application/proto',
    bodyBase64: '',
  },
  {
    name: 'grpc_web',
    about:
      'gRPC-Web, the framing a browser sends. One length-prefixed frame: flag 0 ' +
      'and a zero-length message.',
    contentType: 'application/grpc-web+proto',
    // Five bytes: one flag byte and a four-byte big-endian length, both zero.
    bodyBase64: 'AAAAAAA=',
  },
  {
    name: 'grpc_web_json',
    about: 'gRPC-Web carrying the proto3 JSON mapping, which a browser SDK can ask for.',
    contentType: 'application/grpc-web+json',
    // The same frame, with `{}` as its payload: flag 0, length 2.
    bodyBase64: 'AAAAAAJ7fQ==',
  },
];

/**
 * The refusal on a server stream. It is its own case because the answer is not
 * an HTTP status: the refusal arrives inside the Connect streaming envelope, so
 * a client that only reads status codes sees a 200 and no error.
 */
const STREAM_CASE = {
  name: 'live_watch',
  path: '/loams.live.v1.LiveService/Watch',
  about:
    'The unavailable-service path on a server stream: the refusal arrives ' +
    'inside the Connect streaming envelope, not as an HTTP status. An SDK that ' +
    'reports it has to read the envelope.',
  reason: 'feature_not_in_variant',
  contentType: 'application/connect+proto',
  // flags 0, length 0, then a zero-length request message.
  bodyBase64: 'AAAAAAA=',
};

/** Headers worth recording; `date` and friends change on every call. */
const KEEP = ['content-type', 'grpc-status', 'grpc-message', 'loams-consistency-token'];

/** Builds the case list: each RPC in each encoding, plus the stream. */
function cases() {
  const out = [];
  for (const rpc of RPCS) {
    for (const encoding of ENCODINGS) {
      out.push({
        name: `${rpc.name}_${encoding.name}`,
        about: `${rpc.about} (${encoding.about})`,
        request: {
          method: 'POST',
          path: rpc.path,
          headers: { 'content-type': encoding.contentType },
          ...(encoding.bodyBase64 === undefined
            ? { body: encoding.body }
            : { bodyBase64: encoding.bodyBase64 }),
        },
        // gRPC-Web always answers 200 and puts the code in trailers, so the
        // HTTP status only says something on the Connect paths.
        expect: {
          status: encoding.name.startsWith('grpc_web') ? 200 : rpc.reason ? 501 : 200,
          reason: rpc.reason ?? undefined,
        },
        framed: encoding.name.startsWith('grpc_web'),
      });
    }
  }
  out.push({
    name: STREAM_CASE.name,
    about: STREAM_CASE.about,
    request: {
      method: 'POST',
      path: STREAM_CASE.path,
      headers: { 'content-type': STREAM_CASE.contentType },
      bodyBase64: STREAM_CASE.bodyBase64,
    },
    expect: { status: 200, reason: STREAM_CASE.reason },
    framed: true,
  });
  return out;
}

async function record(testCase) {
  const body = testCase.request.bodyBase64
    ? Buffer.from(testCase.request.bodyBase64, 'base64')
    : Buffer.from(testCase.request.body ?? '', 'utf8');
  const response = await fetch(`${base}${testCase.request.path}`, {
    method: testCase.request.method,
    headers: testCase.request.headers,
    body,
    duplex: 'half',
  });
  const bytes = Buffer.from(await response.arrayBuffer());
  const headers = {};
  for (const name of KEEP) {
    const value = response.headers.get(name);
    if (value !== null) {
      headers[name] = value;
    }
  }
  if (response.status !== testCase.expect.status) {
    throw new Error(
      `${testCase.name}: status ${response.status}, expected ${testCase.expect.status}: ${bytes.toString('utf8')}`,
    );
  }
  const text = bytes.toString('utf8');
  const isJson = !testCase.framed && (text.startsWith('{') || text.startsWith('['));
  if (testCase.expect.reason && !carriesReason(bytes, testCase.expect.reason)) {
    throw new Error(`${testCase.name}: the body does not carry ${testCase.expect.reason}: ${text}`);
  }
  // A gRPC-Web or streaming case reports failure in `grpc-status`, not in the
  // HTTP status, so a case meant to succeed has to check it.
  const grpcStatus = headers['grpc-status'];
  if (!testCase.expect.reason && grpcStatus !== undefined && grpcStatus !== '0') {
    throw new Error(`${testCase.name}: grpc-status ${grpcStatus}: ${text}`);
  }
  const recorded = {
    name: testCase.name,
    about: testCase.about,
    request: testCase.request,
    response: {
      status: response.status,
      headers,
      ...(isJson ? { body: JSON.parse(text) } : { bodyBase64: bytes.toString('base64') }),
    },
    expect: testCase.expect,
  };
  await mkdir(outDir, { recursive: true });
  await writeFile(
    join(outDir, `${testCase.name}.json`),
    `${JSON.stringify(recorded, null, 2)}\n`,
    'utf8',
  );
  console.log(`recorded ${testCase.name} (${response.status}, ${bytes.length} bytes)`);
  return recorded;
}

/**
 * Whether the reason is anywhere in the response: a Connect JSON error carries
 * it base64-encoded inside the `ErrorInfo` detail, so the detail bytes are
 * decoded before the check. On a stream the refusal is in the body itself.
 */
function carriesReason(bytes, reason) {
  const text = bytes.toString('utf8');
  if (text.includes(reason)) {
    return true;
  }
  // The Connect JSON error body puts the ErrorInfo in a `value`, and gRPC-Web
  // puts a google.rpc.Status in a `grpc-status-details-bin` trailer. Both are
  // base64, so any long base64 run in the response is decoded and searched.
  for (const match of text.matchAll(/[A-Za-z0-9+/]{16,}={0,2}/g)) {
    if (Buffer.from(match[0], 'base64').toString('utf8').includes(reason)) {
      return true;
    }
  }
  return false;
}

const list = cases();
const index = [];
for (const testCase of list) {
  index.push(await record(testCase));
}
await mkdir(join(here, '..', 'fixtures'), { recursive: true });
await writeFile(
  join(here, '..', 'fixtures', 'index.json'),
  `${JSON.stringify(
    {
      about:
        'The conformance corpus (design §44 §10.4, SDK1 Task 4). Recorded from ' +
        'a real `loams dev` by sdks/conformance/record-fixtures.mjs and ' +
        'replayed by sdks/conformance/fixture-server.mjs.',
      cases: index.map((entry) => ({
        name: entry.name,
        about: entry.about,
        path: entry.request.path,
        contentType: entry.request.headers['content-type'],
        status: entry.response.status,
        reason: entry.expect.reason ?? null,
      })),
    },
    null,
    2,
  )}\n`,
  'utf8',
);
console.log(`wrote ${list.length} cases`);
