// The in-frame runtime for third-party console plugins (design §37 §5.6).
//
// A classic script, so it needs no CORS from inside an opaque origin. It
// waits for the host's `loams/init` message carrying a MessagePort, exposes
// `globalThis.loams` (call a bridged service method, get the plugin's root
// element), then loads the plugin script named in the fragment
// (`#script=/ui/plugins/<id>/client.js`), which must be a path on this
// origin. Everything the plugin does outside its frame goes through
// `loams.call`, which the host checks against the plugin's permissions.
(() => {
  const params = new URLSearchParams(location.hash.slice(1));
  const script = params.get('script') || '';
  const local =
    script.startsWith('/') &&
    !script.startsWith('//') &&
    !script.includes('..') &&
    !script.includes(':');

  let port = null;
  let nextId = 1;
  const waiting = new Map();
  const queued = [];
  let initResolve;
  const ready = new Promise((resolve) => {
    initResolve = resolve;
  });

  function send(message) {
    if (port) port.postMessage(message);
    else queued.push(message);
  }

  const loams = Object.freeze({
    /** Calls `<service>.<method>(input)` through the host bridge. */
    call(service, method, input) {
      return new Promise((resolve, reject) => {
        const id = nextId++;
        waiting.set(id, { resolve, reject });
        send({ t: 'call', id, service, method, input });
      });
    },
    /** Resolves with {plugin, version} once the host has connected. */
    ready: () => ready,
    root: document.getElementById('root'),
  });

  window.addEventListener('message', (event) => {
    // Only the embedding console, only once, only with a port.
    if (event.source !== window.parent || port) return;
    const data = event.data;
    if (data?.t !== 'loams/init' || !event.ports?.[0]) return;
    port = event.ports[0];
    port.onmessage = (reply) => {
      const message = reply.data;
      if (message?.t !== 'result') return;
      const pending = waiting.get(message.id);
      if (!pending) return;
      waiting.delete(message.id);
      if (message.ok) pending.resolve(message.value);
      else {
        const error = new Error(message.error);
        error.refused = Boolean(message.refused);
        pending.reject(error);
      }
    };
    for (const message of queued.splice(0)) port.postMessage(message);
    initResolve({ plugin: data.plugin, version: data.version });
  });

  Object.defineProperty(globalThis, 'loams', { value: loams, writable: false });

  if (local) {
    const element = document.createElement('script');
    element.src = script;
    document.head.append(element);
  } else if (script) {
    loams.root.textContent = 'Refused a plugin script that is not on this origin.';
  }
})();
