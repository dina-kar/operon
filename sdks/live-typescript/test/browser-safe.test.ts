// Plan Task 14: nothing under src/ may need Node, so the build runs in browsers;
// the runtime dependencies are the Apache-2.0 protobuf and Connect packages only.
import assert from "node:assert/strict";
import { readdirSync, readFileSync } from "node:fs";
import { test } from "node:test";
import { fileURLToPath } from "node:url";

const DIST = fileURLToPath(new URL("../dist/", import.meta.url));
const FORBIDDEN = ['from "node:', "require(", "process.", "Buffer"];

function files(dir: string): string[] {
  return readdirSync(dir, { withFileTypes: true, recursive: true })
    .filter((e) => e.isFile() && e.name.endsWith(".js"))
    .map((e) => `${e.parentPath}/${e.name}`);
}

test("dist has no Node built-ins", () => {
  const all = files(DIST);
  assert.ok(
    all.some((f) => f.endsWith("/index.js")),
    "build first: pnpm run build",
  );
  for (const path of all) {
    const text = readFileSync(path, "utf8");
    for (const needle of FORBIDDEN) {
      assert.ok(!text.includes(needle), `${path} contains ${needle}`);
    }
  }
});

test("the runtime dependencies are protobuf-es and Connect", () => {
  const pkg = JSON.parse(readFileSync(new URL("../package.json", import.meta.url), "utf8"));
  assert.deepEqual(Object.keys(pkg.dependencies).sort(), [
    "@bufbuild/protobuf",
    "@connectrpc/connect",
    "@connectrpc/connect-web",
  ]);
  assert.equal(pkg.peerDependencies, undefined);
});
