// Test helpers: a spawned `operon dev` (M1.6 Task 5 rule 7, rows E13, E21) and a scripted fake fetch.
import { type ChildProcess, spawn } from "node:child_process";
import { existsSync } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createInterface } from "node:readline";
import { fileURLToPath } from "node:url";

import { type DocumentInput, type SchemaInput, s } from "../dist/index.js";

const REPO = fileURLToPath(new URL("../../../", import.meta.url));
const LISTENING = /operon listening on (http:\/\/\S+)/;
const FLIGHT = /operon flight sql listening on (grpc:\/\/\S+)/;
// biome-ignore lint/suspicious/noControlCharactersInRegex: ANSI escapes are control characters
const ANSI = /\x1b\[[0-9;?]*[ -/]*[@-~]/g;
const STARTUP_TIMEOUT_MS = 60_000;
const STOP_GRACE_MS = 10_000;

export interface Operon {
  baseUrl: string;
  flightUri: string | null;
  stop(): Promise<void>;
}

function binary(): string {
  const path = process.env.OPERON_BIN ?? join(REPO, "target", "debug", "operon");
  if (!existsSync(path)) {
    throw new Error(`no operon binary at ${path}; build it with: cargo build -p operon`);
  }
  return path;
}

function exited(child: ChildProcess): Promise<void> {
  if (child.exitCode !== null || child.signalCode !== null) return Promise.resolve();
  return new Promise((resolve) => child.once("exit", () => resolve()));
}

/** Spawns `operon dev` on ephemeral ports and waits for its listening line(s). */
export async function startOperon(opts: { flight?: boolean } = {}): Promise<Operon> {
  const flight = opts.flight ?? false;
  const data = await mkdtemp(join(tmpdir(), "operon-ts-"));
  const args = [
    "dev",
    "--listen",
    "127.0.0.1:0",
    "--flush-interval-ms",
    "20",
    "--data-dir",
    data,
    "--no-qdrant",
    "--no-es",
    ...(flight ? ["--flight-sql-listen", "127.0.0.1:0"] : ["--no-flight-sql"]),
  ];
  const child = spawn(binary(), args, {
    env: { ...process.env, RUST_LOG: "warn" },
    stdio: ["ignore", "pipe", "pipe"],
  });
  const stop = async (): Promise<void> => {
    const done = exited(child);
    child.kill("SIGTERM");
    const timer = setTimeout(() => child.kill("SIGKILL"), STOP_GRACE_MS);
    await done;
    clearTimeout(timer);
    await rm(data, { recursive: true, force: true });
  };

  const seen: string[] = [];
  let baseUrl: string | null = null;
  let flightUri: string | null = null;
  try {
    await new Promise<void>((resolve, reject) => {
      const timer = setTimeout(
        () =>
          reject(new Error(`operon dev did not start within 60 s: ${seen.slice(-20).join("\n")}`)),
        STARTUP_TIMEOUT_MS,
      );
      const finish = (error?: Error): void => {
        clearTimeout(timer);
        if (error) reject(error);
        else resolve();
      };
      child.once("error", finish);
      child.once("exit", (code) =>
        finish(
          new Error(`operon dev exited (${code}) before listening: ${seen.slice(-20).join("\n")}`),
        ),
      );
      // Tracing logs share the output streams and are coloured (E13): read both, strip ANSI.
      for (const stream of [child.stdout, child.stderr]) {
        if (stream === null) continue;
        createInterface({ input: stream }).on("line", (raw) => {
          const line = raw.replace(ANSI, "");
          seen.push(line);
          baseUrl ??= LISTENING.exec(line)?.[1] ?? null;
          flightUri ??= FLIGHT.exec(line)?.[1] ?? null;
          if (baseUrl !== null && (!flight || flightUri !== null)) finish();
        });
      }
    });
  } catch (error) {
    await stop();
    throw error;
  }
  child.removeAllListeners("exit");
  if (baseUrl === null) throw new Error("unreachable: no base URL");
  return { baseUrl, flightUri, stop };
}

/** A fresh namespace name, `t-<random hex>`. */
export function freshName(): string {
  const bytes = crypto.getRandomValues(new Uint8Array(6));
  return `t-${Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("")}`;
}

export type Step = Response | Error | ((request: Request) => Response | Promise<Response>);

/** A fake `fetch` answering from a script; it records every request. */
export class Script {
  readonly steps: Step[];
  readonly requests: Request[] = [];
  readonly bodies: string[] = [];

  constructor(...steps: Step[]) {
    this.steps = steps;
  }

  readonly fetch = async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    const request = new Request(input, init);
    this.requests.push(request);
    this.bodies.push(await request.clone().text());
    const step = this.steps.shift();
    if (step === undefined) throw new Error(`unexpected request ${request.method} ${request.url}`);
    if (step instanceof Error) throw step;
    if (typeof step === "function") return step(request);
    return step;
  };
}

export function json(
  status: number,
  body: unknown,
  headers: Record<string, string> = {},
): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json", ...headers },
  });
}

export function unavailable(retryAfter?: string): Response {
  return json(
    503,
    { error: "unavailable", message: "try again" },
    retryAfter === undefined ? {} : { "retry-after": retryAfter },
  );
}

/** The fixture's `kb` collection (scenario.json step 6). */
export function kbSchema(): SchemaInput {
  return {
    fields: [s.text("body"), s.keyword("tenant"), s.i64("n")],
    vectors: [s.vector("embedding", 3)],
    dynamic: "ignore",
  };
}

/** The fixture's first three documents (scenario.json step 10). */
export function kbDocs(): DocumentInput[] {
  return [
    {
      id: 1,
      source: { body: "refund policy", tenant: "a", n: 1 },
      vectors: { embedding: [1, 0, 0] },
    },
    {
      id: 2,
      source: { body: "shipping times", tenant: "a", n: 2 },
      vectors: { embedding: [0.9, 0.1, 0] },
    },
    {
      id: 3,
      source: { body: "refund window", tenant: "b", n: 3 },
      vectors: { embedding: [0, 0, 1] },
    },
  ];
}
