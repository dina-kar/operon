// Global Constraints: nothing under src/ may need Node, so the build runs in browsers.
import assert from "node:assert/strict";
import { readdirSync, readFileSync } from "node:fs";
import { test } from "node:test";
import { fileURLToPath } from "node:url";

const DIST = fileURLToPath(new URL("../dist/", import.meta.url));
const FORBIDDEN = ['from "node:', "require(", "process.", "Buffer"];

test("dist has no Node built-ins", () => {
  const files = readdirSync(DIST).filter((name) => name.endsWith(".js"));
  assert.ok(files.includes("index.js"), "build first: pnpm run build");
  for (const name of files) {
    const text = readFileSync(`${DIST}${name}`, "utf8");
    for (const needle of FORBIDDEN) {
      assert.ok(!text.includes(needle), `${name} contains ${needle}`);
    }
  }
});

test("the package has no runtime dependencies", () => {
  const pkg = JSON.parse(readFileSync(new URL("../package.json", import.meta.url), "utf8"));
  assert.equal(pkg.dependencies, undefined);
  assert.equal(pkg.peerDependencies, undefined);
  assert.equal(pkg.optionalDependencies, undefined);
});
