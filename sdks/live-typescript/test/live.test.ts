// @operon/live against a spawned `operon dev --features live` on the TiKV
// playground (R1 plan Task 14): deploy a bundle, subscribe, mutate, see the
// update, restart the server, resume. Skips without OPERON_TEST_PD.
import assert from "node:assert/strict";
import { after, afterEach, before, describe, test } from "node:test";

import { LiveClient, type LocalStore, type Subscription } from "../dist/index.js";
import { type LiveServer, type LiveSite, newSite, PD, removeSite, startOperon } from "./operon.ts";

const CHAT = `
import { query, mutation } from "loam:server";

export const messages = {
  send: mutation(async (ctx, { channel, body }) => ctx.db.insert("messages", { channel, body })),
  list: query(async (ctx, { channel }) =>
    ctx.db.query("messages").withIndex("by_channel", (q) => q.eq("channel", channel)).collect()),
};
`;

const SCHEMA = {
  tables: [{ name: "messages", indexes: [{ name: "by_channel", fields: ["channel"] }] }],
};

interface Message {
  _id: string;
  channel: string;
  body: string;
}

const bodies = (docs: Message[] | undefined): string[] => (docs ?? []).map((d) => d.body).sort();

/** Resolves once `predicate` holds; fails after `ms`. */
async function until(predicate: () => boolean, what: string, ms = 20_000): Promise<void> {
  const deadline = Date.now() + ms;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`);
    await new Promise((r) => setTimeout(r, 20));
  }
}

const skip =
  PD === undefined ? "needs OPERON_TEST_PD and operon built with --features live" : false;
if (skip) console.log(`skipped: live.test.ts ${skip}`);

describe("against operon dev with Loam Live", { skip, timeout: 240_000 }, () => {
  let site: LiveSite;
  let server: LiveServer;
  const clients: LiveClient[] = [];
  const client = (transport: "connect" | "grpc-web" = "connect"): LiveClient => {
    const c = new LiveClient({ baseUrl: server.liveUrl, transport });
    clients.push(c);
    return c;
  };

  before(async () => {
    site = await newSite();
    server = await startOperon(site);
    const deployed = await client().deploy(CHAT, SCHEMA);
    assert.match(deployed.deploymentId, /^[0-9a-f]{32}$/);
  });

  afterEach(() => {
    for (const c of clients.splice(0)) c.close();
  });

  after(async () => {
    await server?.stop();
    if (site !== undefined) await removeSite(site);
  });

  test("watch_sees_insert_from_another_client", async () => {
    const reader = client();
    const writer = client("grpc-web");
    const list: Subscription<Message[]> = reader.watch("messages:list", { channel: "a" });
    await until(() => list.value !== undefined, "the first result");
    assert.deepEqual(list.value, []);

    const sent = await writer.mutate<string>("messages:send", { channel: "a", body: "hello" });
    assert.equal(typeof sent.result, "string");
    await until(() => bodies(list.value).includes("hello"), "the insert");
    assert.ok(reader.version.ts >= sent.commitTs, "the session is at or past the commit");
    // Another channel's insert is not in this query's result.
    await writer.mutate("messages:send", { channel: "b", body: "elsewhere" });
    await writer.mutate("messages:send", { channel: "a", body: "again" });
    await until(() => bodies(list.value).includes("again"), "the second insert");
    assert.deepEqual(bodies(list.value), ["again", "hello"]);
  });

  test("mutate_returns_commit_ts_and_watch_catches_up", async () => {
    const c = client();
    const list = c.watch<Message[]>("messages:list", { channel: "c" });
    await until(() => list.value !== undefined, "the first result");
    await until(() => c.sessionId !== undefined, "the session");

    const shown: string[][] = [];
    list.onUpdate((docs) => shown.push(bodies(docs)));
    const optimistic = (local: LocalStore): void => {
      const docs = local.getQuery<Message[]>("messages:list", { channel: "c" }) ?? [];
      local.setQuery("messages:list", { channel: "c" }, [
        ...docs,
        { _id: "pending", channel: "c", body: "mine" },
      ]);
    };
    const pending = c.mutate<string>(
      "messages:send",
      { channel: "c", body: "mine" },
      { optimistic },
    );
    assert.deepEqual(bodies(list.value), ["mine"], "the optimistic update shows at once");
    const { commitTs, result: id } = await pending;
    assert.ok(commitTs > 0n);

    // The session's ts reaches the commit (ts-only Transitions carry it), and
    // the server's result replaces the optimistic one.
    await until(() => c.version.ts >= commitTs, "the session at the commit");
    await until(() => list.value?.some((d) => d._id === id) ?? false, "the server's document");
    assert.equal(
      list.value?.some((d) => d._id === "pending"),
      false,
      "the optimistic row is gone",
    );
    assert.deepEqual(shown[0], ["mine"]);

    // A one-shot query at the commit timestamp sees the write.
    const at = await c.query<Message[]>("messages:list", { channel: "c" }, { ts: commitTs });
    assert.deepEqual(bodies(at), ["mine"]);
    const before = await c.query<Message[]>(
      "messages:list",
      { channel: "c" },
      { ts: commitTs - 1n },
    );
    assert.deepEqual(bodies(before), []);
  });

  test("reconnect_after_server_restart_converges", async () => {
    const reader = client();
    const list = reader.watch<Message[]>("messages:list", { channel: "r" });
    await client().mutate("messages:send", { channel: "r", body: "before" });
    await until(() => bodies(list.value).includes("before"), "the first insert");
    const firstSession = reader.sessionId;
    const lastTs = reader.version.ts;

    await server.stop();
    await until(() => reader.sessionId === undefined, "the stream to drop", 15_000);
    assert.deepEqual(bodies(list.value), ["before"], "results stay while disconnected");
    server = await startOperon(site);

    // The deployment survived the restart; a write after it reaches the resumed session.
    const writer = client();
    await writer.mutate("messages:send", { channel: "r", body: "after" });
    await until(() => bodies(list.value).includes("after"), "the insert after the restart", 60_000);
    assert.notEqual(reader.sessionId, firstSession, "a new session");
    assert.ok(reader.version.ts >= lastTs, "the resumed session is not behind");
    assert.deepEqual(bodies(list.value), ["after", "before"]);
    const fresh = await writer.query<Message[]>(
      "messages:list",
      { channel: "r" },
      { ts: reader.version.ts },
    );
    assert.deepEqual(bodies(fresh), bodies(list.value), "converged with a fresh query at its ts");
  });
});
