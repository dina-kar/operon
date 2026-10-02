// Checks a deployed console (issue #269):
//
//   node deploy/smoke.mjs https://console.loams.dev
//
// The page loads, deep links fall back to the app, and every script the page
// loads comes back as JavaScript rather than as the HTML fallback.

/** The script and stylesheet URLs a page loads, resolved against `base`. */
export function assetUrls(html, base) {
  const urls = [];
  for (const m of html.matchAll(/<(?:script|link)\b[^>]*\b(?:src|href)="([^"]+\.(?:js|css))"/gi)) {
    urls.push(new URL(m[1] ?? '', base).href);
  }
  return urls;
}

/** True when a content type is JavaScript. */
export function isJavaScript(type) {
  return /^(text|application)\/javascript\b/i.test(type ?? '');
}

export async function smoke(origin, fetcher = fetch) {
  const failures = [];
  const get = async (path) => {
    const res = await fetcher(new URL(path, origin), { redirect: 'follow' });
    return { res, type: res.headers.get('content-type') ?? '', body: await res.text() };
  };
  const expect = (ok, what) => {
    if (!ok) failures.push(what);
  };

  const page = await get('/');
  expect(page.res.status === 200, `/ returned ${page.res.status}`);
  expect(page.type.startsWith('text/html'), `/ is ${page.type}, not text/html`);

  const deep = await get('/ui/projects/smoke-check');
  expect(deep.res.status === 200, `a deep link returned ${deep.res.status}`);
  expect(deep.type.startsWith('text/html'), `a deep link is ${deep.type}, not text/html`);

  const assets = assetUrls(page.body, page.res.url || origin);
  expect(
    assets.some((u) => u.endsWith('.js')),
    'the page loads no script',
  );
  for (const url of assets) {
    const asset = await get(url);
    expect(asset.res.status === 200, `${url} returned ${asset.res.status}`);
    if (url.endsWith('.js'))
      expect(isJavaScript(asset.type), `${url} is ${asset.type}, not JavaScript`);
    if (url.endsWith('.css'))
      expect(asset.type.startsWith('text/css'), `${url} is ${asset.type}, not CSS`);
  }

  const config = await get('/ui/config.json');
  expect(config.res.status === 200, `/ui/config.json returned ${config.res.status}`);
  expect(config.type.includes('json'), `/ui/config.json is ${config.type}, not JSON`);
  return failures;
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const origin = process.argv[2] ?? 'https://console.loams.dev';
  const failures = await smoke(origin);
  for (const f of failures) console.error(`FAIL ${f}`);
  if (failures.length) process.exit(1);
  console.log(`${origin}: ok`);
}
