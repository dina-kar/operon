// A spawned `operon dev` serving Loam Live (R1 plan Task 14). It needs a
// binary built with `cargo build -p operon --features live` and a TiKV
// playground: `OPERON_TEST_PD` names its PD, as for the Rust cluster tests.
// Each test run takes a random root in the `loam_test_live` keyspace (R1
// Ruling 1), so runs never see each other's documents.
import { type ChildProcess, spawn } from "node:child_process";
import { existsSync } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createInterface } from "node:readline";
import { fileURLToPath } from "node:url";

const REPO = fileURLToPath(new URL("../../../", import.meta.url));
const LIVE = /operon live listening on (http:\/\/\S+)/;
const LISTENING = /operon listening on (http:\/\/\S+)/;
// biome-ignore lint/suspicious/noControlCharactersInRegex: ANSI escapes are control characters
const ANSI = /\x1b\[[0-9;?]*[ -/]*[@-~]/g;
const STARTUP_TIMEOUT_MS = 90_000;
const STOP_GRACE_MS = 15_000;

/** The PD of the test cluster, or `undefined` (the live tests then skip). */
export const PD = process.env.OPERON_TEST_PD || undefined;

export function binary(): string {
  const path = process.env.OPERON_BIN ?? join(REPO, "target", "debug", "operon");
  if (!existsSync(path)) {
    throw new Error(
      `no operon binary at ${path}; build it with: cargo build -p operon --features live`,
    );
  }
  return path;
}

function exited(child: ChildProcess): Promise<void> {
  if (child.exitCode !== null || child.signalCode !== null) return Promise.resolve();
  return new Promise((resolve) => child.once("exit", () => resolve()));
}

/** A free loopback port (the Live listener must keep its port across a restart). */
export function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const server = createServer();
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      const port = typeof address === "object" && address !== null ? address.port : 0;
      server.close(() => resolve(port));
    });
  });
}

function hex(bytes: number): string {
  const b = crypto.getRandomValues(new Uint8Array(bytes));
  return Array.from(b, (x) => x.toString(16).padStart(2, "0")).join("");
}

/** Where one Live app lives: kept across restarts of its server. */
export interface LiveSite {
  readonly dataDir: string;
  readonly livePort: number;
  readonly root: string;
  readonly app: string;
}

export async function newSite(): Promise<LiveSite> {
  return {
    dataDir: await mkdtemp(join(tmpdir(), "operon-live-ts-")),
    livePort: await freePort(),
    root: hex(8),
    app: `t14${hex(3)}`,
  };
}

export async function removeSite(site: LiveSite): Promise<void> {
  await rm(site.dataDir, { recursive: true, force: true });
}

export interface LiveServer {
  readonly liveUrl: string;
  stop(): Promise<void>;
}

/** Spawns `operon dev` for `site` and waits for its Live and HTTP listeners. */
export async function startOperon(site: LiveSite): Promise<LiveServer> {
  if (PD === undefined) throw new Error("OPERON_TEST_PD is not set");
  const args = [
    "dev",
    "--listen",
    "127.0.0.1:0",
    "--data-dir",
    site.dataDir,
    "--no-qdrant",
    "--no-flight-sql",
    "--live-listen",
    `127.0.0.1:${site.livePort}`,
    "--live-pd",
    PD,
    "--live-keyspace",
    "loam_test_live",
    "--live-root",
    site.root,
    "--live-app",
    site.app,
  ];
  const child = spawn(binary(), args, {
    env: { ...process.env, RUST_LOG: process.env.RUST_LOG ?? "warn" },
    stdio: ["ignore", "pipe", "pipe"],
  });
  const stop = async (): Promise<void> => {
    const done = exited(child);
    child.kill("SIGTERM");
    const timer = setTimeout(() => child.kill("SIGKILL"), STOP_GRACE_MS);
    await done;
    clearTimeout(timer);
  };
  const seen: string[] = [];
  let liveUrl: string | null = null;
  let http = false;
  try {
    await new Promise<void>((resolve, reject) => {
      const timer = setTimeout(
        () => reject(new Error(`operon dev did not start: ${seen.slice(-20).join("\n")}`)),
        STARTUP_TIMEOUT_MS,
      );
      const finish = (error?: Error): void => {
        clearTimeout(timer);
        if (error) reject(error);
        else resolve();
      };
      child.once("error", finish);
      child.once("exit", (code) =>
        finish(new Error(`operon dev exited (${code}): ${seen.slice(-20).join("\n")}`)),
      );
      for (const stream of [child.stdout, child.stderr]) {
        if (stream === null) continue;
        createInterface({ input: stream }).on("line", (raw) => {
          const line = raw.replace(ANSI, "");
          seen.push(line);
          if (process.env.OPERON_TS_LOG) console.error(`[operon] ${line}`);
          liveUrl ??= LIVE.exec(line)?.[1] ?? null;
          http ||= LISTENING.test(line);
          if (liveUrl !== null && http) finish();
        });
      }
    });
  } catch (error) {
    await stop();
    throw error;
  }
  child.removeAllListeners("exit");
  if (liveUrl === null) throw new Error("unreachable: no Live URL");
  return { liveUrl, stop };
}
