// A sample third-party Loams console plugin (design §37 §5.6).
//
// It runs inside the sandbox frame, so it has no network (connect-src
// 'none'), no cookies and no access to the console. Its only way out is
// `loams.call(service, method, input)`, which the console checks against the
// permissions in package.json: it declared `approvals:read`, so listing
// approvals works and creating a device pairing (`devices:manage`) is refused.
(async () => {
  const { loams } = globalThis;
  const root = loams.root;
  const line = (text, className) => {
    const p = document.createElement('p');
    p.textContent = text;
    if (className) p.className = className;
    root.append(p);
    return p;
  };

  const heading = document.createElement('h2');
  heading.textContent = 'Hello from a sandboxed plugin';
  root.append(heading);

  const { plugin, version } = await loams.ready();
  line(`I am ${plugin}@${version}, running in an opaque-origin frame.`);

  try {
    const { approvals } = await loams.call('rpc.approvals', 'listApprovals', {});
    line(`approvals:read works: ${approvals.length} pending approval(s).`);
    for (const a of approvals) line(`• ${a.summary}`);
  } catch (error) {
    line(`listApprovals failed: ${error.message}`);
  }

  try {
    await loams.call('rpc.devices', 'createPairing', {});
    line('createPairing worked (it should not have).');
  } catch (error) {
    line(`createPairing was refused: ${error.message}`);
  }

  try {
    await fetch('https://example.com/');
    line('fetch worked (it should not have).');
  } catch (error) {
    line(`fetch is blocked by the frame's CSP: ${error.message}`);
  }
})();
