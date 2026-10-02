// node --test apps/console/deploy/ (CI's web job runs it).
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { after, describe, test } from 'node:test';
import { fileURLToPath } from 'node:url';
import { loadRuntimeConfig, parseRuntimeConfig } from '../src/runtime-config.ts';
import { assetUrls, isJavaScript, smoke } from './smoke.mjs';
import {
  checkBase,
  contentSecurityPolicy,
  headersFile,
  injectCsp,
  inlineScriptHashes,
  redirectsFile,
  stage,
} from './stage.mjs';

const here = dirname(fileURLToPath(import.meta.url));
const console_ = join(here, '..');

const THEME = "\n      try { document.documentElement.classList.add('dark'); } catch {}\n    ";
const INDEX = `<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <link rel="icon" href="/ui/favicon.svg" type="image/svg+xml" />
    <script>${THEME}</script>
    <script type="module" crossorigin src="/ui/assets/index-abc.js"></script>
    <link rel="stylesheet" crossorigin href="/ui/assets/index-abc.css">
  </head>
  <body><div id="root"></div></body>
</html>
`;

describe('wrangler.jsonc', () => {
  const text = readFileSync(join(console_, 'wrangler.jsonc'), 'utf8');
  const config = JSON.parse(text.replace(/^\s*\/\/.*$/gm, ''));

  test('is the assets-only loams-console Worker on console.loams.dev', () => {
    assert.equal(config.name, 'loams-console');
    assert.equal(config.main, undefined, 'no Worker code: free tier, no CPU budget used');
    assert.deepEqual(config.routes, [{ pattern: 'console.loams.dev', custom_domain: true }]);
  });

  test('serves the staged directory with an SPA fallback', () => {
    assert.equal(config.assets.directory, './dist-cloudflare');
    assert.equal(config.assets.not_found_handling, 'single-page-application');
  });
});

describe('stage', () => {
  test('hashes inline scripts and skips external ones', () => {
    const want = `'sha256-${createHash('sha256').update(THEME).digest('base64')}'`;
    assert.deepEqual(inlineScriptHashes(INDEX), [want]);
  });

  test('the CSP allows the inline hash and the configured server, and nothing remote otherwise', () => {
    const csp = contentSecurityPolicy({
      scriptHashes: ["'sha256-x'"],
      server: 'https://loams.example.com/path',
    });
    assert.match(csp, /script-src 'self' 'sha256-x'(;|$)/);
    assert.match(csp, /connect-src 'self' https:\/\/loams\.example\.com(;|$)/);
    assert.doesNotMatch(csp, /unsafe-eval/);
    assert.match(contentSecurityPolicy(), /connect-src 'self'(;|$)/);
  });

  test('injects the CSP once, after the charset', () => {
    const once = injectCsp(INDEX, "default-src 'self'");
    assert.match(once, /<meta charset="utf-8" \/>\n {4}<meta http-equiv="Content-Security-Policy"/);
    assert.equal(injectCsp(once, 'other'), once);
  });

  test('rejects a build made for another base path', () => {
    assert.throws(
      () => checkBase(INDEX.replaceAll('/ui/assets/', '/assets/')),
      /expected \/ui\/assets/,
    );
    checkBase(INDEX);
  });

  test('headers cache hashed assets forever and config.json never', () => {
    const h = headersFile();
    assert.match(h, /\/ui\/assets\/\*\n {2}Cache-Control: public, max-age=31536000, immutable/);
    assert.match(h, /\/ui\/config\.json\n {2}Cache-Control: no-store/);
    assert.match(h, /X-Content-Type-Options: nosniff/);
    assert.equal(redirectsFile(), '/ /ui/ 302\n');
  });

  const tmp = join(here, '.test-tmp');
  after(() => rmSync(tmp, { recursive: true, force: true }));

  test('lays the build out under /ui with a root fallback page and runtime config', () => {
    const dist = join(tmp, 'dist');
    const out = join(tmp, 'out');
    mkdirSync(join(dist, 'assets'), { recursive: true });
    writeFileSync(join(dist, 'index.html'), INDEX);
    writeFileSync(join(dist, 'config.json'), '{}\n');
    writeFileSync(join(dist, 'assets', 'index-abc.js'), 'export {};\n');
    writeFileSync(join(dist, 'assets', 'index-abc.js.map'), '{}');

    stage({ dist, out, server: 'https://loams.example.com/' });

    assert.ok(existsSync(join(out, 'ui', 'assets', 'index-abc.js')));
    const root = readFileSync(join(out, 'index.html'), 'utf8');
    assert.equal(root, readFileSync(join(out, 'ui', 'index.html'), 'utf8'));
    assert.match(root, /http-equiv="Content-Security-Policy"/);
    assert.match(root, /connect-src 'self' https:\/\/loams\.example\.com/);
    assert.deepEqual(JSON.parse(readFileSync(join(out, 'ui', 'config.json'), 'utf8')), {
      server: 'https://loams.example.com',
    });
    assert.equal(readFileSync(join(out, '.assetsignore'), 'utf8'), '*.map\n');
    assert.ok(existsSync(join(out, '_headers')));
    assert.ok(existsSync(join(out, '_redirects')));

    // Without a server the engine's empty config stays: same origin.
    stage({ dist, out });
    assert.equal(readFileSync(join(out, 'ui', 'config.json'), 'utf8'), '{}\n');
  });
});

describe('runtime config', () => {
  test('keeps an http(s) server origin and drops everything else', () => {
    assert.deepEqual(parseRuntimeConfig({ server: 'https://loams.example.com/x?y' }), {
      server: 'https://loams.example.com',
    });
    assert.deepEqual(parseRuntimeConfig({}), {});
    assert.deepEqual(parseRuntimeConfig({ server: '' }), {});
    assert.deepEqual(parseRuntimeConfig({ server: 'javascript:alert(1)' }), {});
    assert.deepEqual(parseRuntimeConfig({ server: 'not a url' }), {});
    assert.deepEqual(parseRuntimeConfig([]), {});
    assert.deepEqual(parseRuntimeConfig(null), {});
  });

  test('loads <base>config.json and ignores an HTML fallback or a failure', async () => {
    const seen = [];
    const json = async (url) => {
      seen.push(url);
      return Response.json({ server: 'https://a.example' });
    };
    assert.deepEqual(await loadRuntimeConfig('/ui/', json), { server: 'https://a.example' });
    assert.deepEqual(seen, ['/ui/config.json']);
    const html = async () =>
      new Response('<!doctype html>', { headers: { 'content-type': 'text/html' } });
    assert.deepEqual(await loadRuntimeConfig('/ui/', html), {});
    const down = async () => {
      throw new TypeError('offline');
    };
    assert.deepEqual(await loadRuntimeConfig('/ui/', down), {});
  });
});

describe('smoke', () => {
  test('finds the scripts and stylesheets a page loads', () => {
    assert.deepEqual(assetUrls(INDEX, 'https://console.loams.dev/ui/'), [
      'https://console.loams.dev/ui/assets/index-abc.js',
      'https://console.loams.dev/ui/assets/index-abc.css',
    ]);
    assert.ok(isJavaScript('text/javascript; charset=utf-8'));
    assert.ok(isJavaScript('application/javascript'));
    assert.ok(!isJavaScript('text/html'));
  });

  /** A fake deploy: `fallbackAssets` reproduces the 2026-10-02 bug (assets served as the HTML fallback). */
  const site = (fallbackAssets) => async (url) => {
    const { pathname } = new URL(url);
    const html = () => {
      const res = new Response(INDEX, { headers: { 'content-type': 'text/html; charset=utf-8' } });
      Object.defineProperty(res, 'url', { value: 'https://console.loams.dev/ui/' });
      return res;
    };
    if (pathname.endsWith('.js') && !fallbackAssets)
      return new Response('export {}', { headers: { 'content-type': 'text/javascript' } });
    if (pathname.endsWith('.css') && !fallbackAssets)
      return new Response('', { headers: { 'content-type': 'text/css' } });
    if (pathname === '/ui/config.json') return Response.json({});
    return html();
  };

  test('passes a good deploy', async () => {
    assert.deepEqual(await smoke('https://console.loams.dev', site(false)), []);
  });

  test('fails when scripts come back as the HTML fallback', async () => {
    const failures = await smoke('https://console.loams.dev', site(true));
    assert.ok(
      failures.some((f) => /index-abc\.js is text\/html/.test(f)),
      failures.join('\n'),
    );
  });
});
