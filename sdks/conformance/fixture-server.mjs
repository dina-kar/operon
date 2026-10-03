// The conformance fixture server (design §44 §10.4, SDK1 Task 4).
//
// Serves the recorded corpus over real HTTP so an SDK's conformance suite runs
// against something that speaks the wire, with nothing to install but Node. The
// thirteen SDKs all run their suite against this, which is what makes the suite
// comparable between them: same bytes, same status codes, same trailers.
//
//   node sdks/conformance/fixture-server.mjs [--port 0] [--fixtures <dir>]
//
// It prints `{"url":"http://127.0.0.1:PORT"}` on stdout once it is listening,
// then serves until it is killed.
//
// A request that matches no recorded case is a **404 with a body naming the
// gap**, never a silent success. A conformance suite that passed because the
// fixture server answered everything with 200 would be worse than no suite, so
// an unrecorded path fails loudly and the corpus has to grow.
//
// Requests are matched on path and content type. The corpus records each RPC in
// both unary encodings, plus gRPC-Web and the streaming envelope, because a
// client's transport picks one and the response body is not interchangeable
// between them.

import { createServer } from 'node:http';
import { readFile, readdir } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));

function arg(name, fallback) {
  const at = process.argv.indexOf(`--${name}`);
  return at >= 0 && at + 1 < process.argv.length ? process.argv[at + 1] : fallback;
}

/**
 * The encoding of a content type, which is what a recorded case is keyed on:
 * `json`, `proto`, `grpc_web`, `grpc_web_json` and `connect` each have their own
 * response bytes. The gRPC-Web variants are kept apart because a client that
 * asks for gRPC-Web with protobuf must not be handed gRPC-Web with JSON, which
 * is the kind of mismatch that shows up as a parse error inside somebody's SDK
 * rather than as a failure here.
 */
function family(contentType) {
  const type = (contentType ?? '').split(';')[0].trim();
  if (type === 'application/grpc-web+json') {
    return 'grpc_web_json';
  }
  if (type.startsWith('application/grpc-web')) {
    return 'grpc_web';
  }
  if (type.startsWith('application/connect')) {
    return 'connect';
  }
  if (type === 'application/json') {
    return 'json';
  }
  if (type === 'application/proto') {
    return 'proto';
  }
  return type === '' ? 'none' : type;
}

async function load(fixturesDir) {
  const dir = join(fixturesDir, 'recorded');
  const files = (await readdir(dir)).filter((name) => name.endsWith('.json'));
  const byKey = new Map();
  for (const file of files.sort()) {
    const case_ = JSON.parse(await readFile(join(dir, file), 'utf8'));
    const key = `${case_.request.method} ${case_.request.path} ${family(
      case_.request.headers['content-type'],
    )}`;
    byKey.set(key, case_);
  }
  return byKey;
}

const fixturesDir = arg('fixtures', join(here, '..', 'fixtures'));
const cases = await load(fixturesDir);
if (cases.size === 0) {
  console.error(`no recorded cases under ${fixturesDir}/recorded`);
  process.exit(2);
}

const server = createServer((request, response) => {
  const chunks = [];
  request.on('data', (chunk) => chunks.push(chunk));
  request.on('end', () => {
    const key = `${request.method} ${request.url} ${family(request.headers['content-type'])}`;
    const found = cases.get(key);
    if (found === undefined) {
      const body = JSON.stringify({
        error: `no recorded fixture for ${key}`,
        recorded: [...cases.keys()],
      });
      response.writeHead(404, { 'content-type': 'application/json' });
      response.end(body);
      return;
    }
    // The recorded case also carries the request that produced it, and it is
    // checked. A replay that ignores what the client sent would pass an SDK
    // that frames a gRPC-Web message wrongly or posts the wrong payload, which
    // is the class of bug a recorded corpus exists to catch.
    const sent = Buffer.concat(chunks);
    const expected =
      found.request.body === undefined
        ? Buffer.from(found.request.bodyBase64 ?? '', 'base64')
        : Buffer.from(found.request.body, 'utf8');
    if (!sent.equals(expected)) {
      const body = JSON.stringify({
        error: `the request does not match the recorded one for ${key}`,
        expected: expected.toString('base64'),
        sent: sent.toString('base64'),
      });
      response.writeHead(400, { 'content-type': 'application/json' });
      response.end(body);
      return;
    }
    const headers = { ...found.response.headers };
    const body =
      found.response.body === undefined
        ? Buffer.from(found.response.bodyBase64 ?? '', 'base64')
        : Buffer.from(JSON.stringify(found.response.body), 'utf8');
    response.writeHead(found.response.status, headers);
    response.end(body);
  });
});

server.listen(Number(arg('port', '0')), '127.0.0.1', () => {
  const address = server.address();
  process.stdout.write(`${JSON.stringify({ url: `http://127.0.0.1:${address.port}` })}\n`);
});

for (const signal of ['SIGINT', 'SIGTERM']) {
  process.on(signal, () => {
    server.close(() => process.exit(0));
  });
}
