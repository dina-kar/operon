// The in-frame runtime for third-party console plugins (design §37 §5.6).
//
// A classic script, so it needs no CORS from inside an opaque origin. It
// waits for the host's `loams/init` message carrying a MessagePort, exposes
// `globalThis.loams` (call a bridged service method, get the plugin's root
// element), then loads the plugin named in the fragment (`#plugin=<id>`)
// from `../plugins/<id>/client.js` on this origin, but only after the host
// has handed over the port: no plugin code runs before the frame holds its
// port, so a plugin cannot navigate the frame first and have the port posted
// to the next document. Everything the plugin does outside its frame goes
// through `loams.call`, which the host checks against the plugin's
// permissions.
//
// The runtime refuses to load a plugin unless its own origin is opaque
// (`self.origin === 'null'`) and it is framed: opened directly, or framed
// without `sandbox`, frame.html would run plugin code with the console's
// origin (its storage, its cookies).
(() => {
  const params = new URLSearchParams(location.hash.slice(1));
  const requested = params.get('plugin') || '';
  // Only an id, never a URL: [a-z0-9-], as the host's sandboxScriptId.
  const pluginId = /^[a-z0-9][a-z0-9-]{0,63}$/.test(requested) ? requested : '';
  const isolated = self.origin === 'null' && window.parent !== window;

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
    if (pluginId && isolated) {
      const element = document.createElement('script');
      element.src = new URL(`../plugins/${pluginId}/client.js`, location.href).pathname;
      document.head.append(element);
    }
  });

  Object.defineProperty(globalThis, 'loams', { value: loams, writable: false });

  if (requested && !pluginId) {
    loams.root.textContent = 'Refused a plugin name that is not an id.';
  } else if (pluginId && !isolated) {
    loams.root.textContent = 'Refused to run a plugin outside a sandboxed frame.';
  }
})();
