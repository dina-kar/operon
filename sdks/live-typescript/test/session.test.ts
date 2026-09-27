// The session and query-set layer (R1 plan Task 14), against the pure state
// and a fake server behind connect's in-memory transport.
import assert from "node:assert/strict";
import { afterEach, describe, test } from "node:test";

import { type LiveValue, QuerySetState, SessionState, VersionGap, ZERO } from "../dist/index.js";
import { closeClients, FakeServer, transition, until, v } from "./fake.ts";

describe("SessionState", () => {
  test("applies_transitions_in_version_order", () => {
    const s = new SessionState();
    const a = s.apply(transition(ZERO, v(1n, 10n), { 1: "a", 2: "b" }));
    assert.deepEqual(a, { complete: true, changed: new Set([1, 2]) });
    s.apply(transition(v(1n, 10n), v(1n, 20n), { 1: "a2" }));
    assert.deepEqual(s.version, v(1n, 20n));
    assert.deepEqual(s.results.get(1), { kind: "value", value: "a2" });

    // Out of order: a Transition from a version the client is not at changes nothing.
    assert.throws(() => s.apply(transition(v(1n, 30n), v(1n, 40n), { 1: "x" })), VersionGap);
    // A replay of an applied one is a gap too.
    assert.throws(() => s.apply(transition(v(1n, 10n), v(1n, 20n), { 1: "x" })), VersionGap);
    assert.deepEqual(s.version, v(1n, 20n));
    assert.deepEqual(s.results.get(1), { kind: "value", value: "a2" });

    // A heartbeat (start == end) applies and changes nothing.
    assert.deepEqual(s.apply(transition(v(1n, 20n), v(1n, 20n))), {
      complete: true,
      changed: new Set(),
    });

    // Removed and per-query errors.
    s.apply(transition(v(1n, 20n), v(2n, 25n), {}, { removed: [2], errors: { 1: "boom" } }));
    assert.equal(s.results.has(2), false);
    const r = s.results.get(1);
    assert.equal(r?.kind, "error");
    assert.equal(r?.kind === "error" && r.error.code, "FUNCTION_ERROR");
  });

  test("chunks_apply_together", () => {
    const s = new SessionState();
    const end = v(1n, 10n);
    assert.deepEqual(s.apply(transition(ZERO, end, { 1: "a" }, { more: true })), {
      complete: false,
    });
    assert.equal(s.results.size, 0, "nothing shows before the last chunk");
    assert.deepEqual(s.version, ZERO);
    // A chunk of another Transition inside is a gap and keeps the partial one.
    assert.throws(() => s.apply(transition(ZERO, v(1n, 11n), { 2: "x" })), VersionGap);
    s.apply(transition(ZERO, end, { 2: "b" }));
    assert.deepEqual([...s.results.keys()], [1, 2]);
    assert.deepEqual(s.version, end);
  });

  test("retain_drops_queries_outside_the_set", () => {
    const s = new SessionState();
    s.apply(transition(ZERO, v(1n, 10n), { 1: "a", 2: "b", 3: "c" }));
    assert.deepEqual(s.retain(new Set([1, 3])), [2]);
    assert.deepEqual([...s.results.keys()], [1, 3]);
  });
});

describe("QuerySetState", () => {
  test("equal_queries_share_an_id_and_diffs_carry_versions", () => {
    const q = new QuerySetState();
    const a = q.add("messages:list", { channel: "general", limit: 10n });
    const b = q.add("messages:list", { limit: 10n, channel: "general" });
    assert.equal(a.created, true);
    assert.equal(b.created, false);
    assert.equal(a.def.id, b.def.id);
    assert.notEqual(q.add("messages:list", { channel: "general", limit: 10 }).def.id, a.def.id);

    const started = q.start();
    assert.equal(started.version, 1n);
    assert.equal(started.queries.length, 2);
    assert.equal(q.diff(), undefined);

    const c = q.add("counters:get", null).def;
    q.remove(a.def.id);
    const d = q.diff();
    assert.ok(d);
    assert.equal(d.baseVersion, 1n);
    assert.equal(d.newVersion, 2n);
    assert.deepEqual(
      d.add.map((x) => x.id),
      [c.id],
    );
    assert.deepEqual(d.remove, [a.def.id]);
    q.accepted(d);
    assert.equal(q.heldVersion, 2n);
    assert.equal(q.diff(), undefined);
  });
});

describe("LiveClient over a fake server", { timeout: 5000 }, () => {
  afterEach(closeClients);

  test("resumes_on_version_gap", async () => {
    const server = new FakeServer();
    const client = server.client();
    const sub = client.watch<LiveValue>("messages:list", { channel: "general" });
    const first = await server.watch(0);
    assert.equal(first.request.start.case, "initial");
    const set = first.request.start.case === "initial" ? first.request.start.value : undefined;
    assert.equal(set?.queries.length, 1);
    const id = set?.queries[0]?.queryId ?? -1;

    first.stream.push(transition(ZERO, v(1n, 10n), { [id]: ["m1"] }));
    await until(() => sub.value !== undefined, "the first result");
    assert.deepEqual(sub.value, ["m1"]);

    // A Transition that skips a version: the client drops the stream and resumes.
    first.stream.push(transition(v(1n, 30n), v(1n, 40n), { [id]: ["m1", "m2", "m3"] }));
    const second = await server.watch(1);
    assert.equal(second.request.start.case, "resume");
    const resume = second.request.start.case === "resume" ? second.request.start.value : undefined;
    assert.deepEqual(
      {
        querySet: resume?.lastVersion?.querySet,
        identity: resume?.lastVersion?.identity,
        ts: resume?.lastVersion?.ts,
      },
      v(1n, 10n),
    );
    assert.deepEqual(
      resume?.querySet?.queries.map((q) => [q.queryId, q.function]),
      [[id, "messages:list"]],
    );
    assert.deepEqual(sub.value, ["m1"], "the skipped Transition was not applied");

    const qs = resume?.querySet?.version ?? 0n;
    second.stream.push(transition(v(1n, 10n), v(qs, 45n), { [id]: ["m1", "m2", "m3"] }));
    await until(() => Array.isArray(sub.value) && sub.value.length === 3, "the resumed result");
    assert.deepEqual(client.version, v(qs, 45n));
    client.close();
  });

  test("reconnects_after_a_stream_error_and_resumes", async () => {
    const server = new FakeServer();
    const client = server.client();
    const sub = client.watch<LiveValue>("counters:get");
    const first = await server.watch(0);
    first.stream.push(transition(ZERO, v(1n, 10n), { 1: 1n }));
    await until(() => sub.value === 1n, "the first result");
    first.stream.fail(new Error("connection reset"));
    const second = await server.watch(1);
    assert.equal(second.request.start.case, "resume");
    second.stream.push(transition(v(1n, 10n), v(2n, 12n), { 1: 2n }));
    await until(() => sub.value === 2n, "the resumed result");
    client.close();
  });

  test("modifies_the_query_set_with_versions", async () => {
    const server = new FakeServer();
    const client = server.client();
    const a = client.watch("counters:get", { name: "a" });
    const first = await server.watch(0);
    first.stream.push(transition(ZERO, v(1n, 10n), { 1: 0n }));
    await until(() => a.value !== undefined, "the first result");

    const b = client.watch("counters:get", { name: "b" });
    await until(() => server.modifies.length === 1, "an add");
    const add = server.modifies[0];
    assert.equal(add?.sessionId, "s-1");
    assert.equal(add?.baseVersion, 1n);
    assert.equal(add?.newVersion, 2n);
    assert.deepEqual(
      add?.changes.map((c) => c.change.case),
      ["add"],
    );
    first.stream.push(transition(v(1n, 10n), v(2n, 10n), { 2: 5n }));
    await until(() => b.value === 5n, "the added query's result");

    a.unsubscribe();
    await until(() => server.modifies.length === 2, "a remove");
    const remove = server.modifies[1];
    assert.equal(remove?.baseVersion, 2n);
    assert.equal(remove?.newVersion, 3n);
    assert.deepEqual(
      remove?.changes.map((c) => [c.change.case, c.change.value]),
      [["remove", 1]],
    );
    client.close();
  });

  test("drops_optimistic_update_at_commit_ts", async () => {
    const server = new FakeServer();
    const client = server.client();
    const list = client.watch<string[]>("messages:list");
    const first = await server.watch(0);
    first.stream.push(transition(ZERO, v(1n, 50n), { 1: ["a"] }));
    await until(() => list.value !== undefined, "the first result");

    const seen: string[][] = [];
    list.onUpdate((value) => seen.push(value));
    server.mutateAnswer = () => ({ commitTs: 100n, result: "id-b" });
    const optimistic = (local: import("../dist/index.js").LocalStore): void => {
      const current = local.getQuery<string[]>("messages:list") ?? [];
      local.setQuery("messages:list", null, [...current, "b (sending)"]);
    };
    const done = await client.mutate("messages:send", { body: "b" }, { optimistic });
    assert.equal(done.commitTs, 100n);
    assert.equal(done.result, "id-b");
    assert.equal(server.mutates[0]?.session, "s-1", "the mutation names the session");
    assert.deepEqual(list.value, ["a", "b (sending)"]);

    // A Transition before the commit keeps the optimistic update over the new server result.
    first.stream.push(transition(v(1n, 50n), v(1n, 80n), { 1: ["z", "a"] }));
    await until(() => client.version.ts === 80n, "ts 80");
    assert.deepEqual(list.value, ["z", "a", "b (sending)"]);

    // At ts ≥ commitTs the server's result shows the write: the update is dropped.
    first.stream.push(transition(v(1n, 80n), v(1n, 100n), { 1: ["z", "a", "b"] }));
    await until(() => client.version.ts === 100n, "ts 100");
    assert.deepEqual(list.value, ["z", "a", "b"]);
    assert.deepEqual(seen, [
      ["a", "b (sending)"],
      ["z", "a", "b (sending)"],
      ["z", "a", "b"],
    ]);

    // A failed mutation drops its update at once.
    server.mutateAnswer = () => {
      throw new Error("refused");
    };
    await assert.rejects(client.mutate("messages:send", { body: "c" }, { optimistic }));
    assert.deepEqual(list.value, ["z", "a", "b"]);
    client.close();
  });

  test("deploy_retries_a_busy_answer", async () => {
    const { Code, ConnectError } = await import("@connectrpc/connect");
    const server = new FakeServer();
    const client = server.client();
    server.deployAnswers = [
      () => {
        throw new ConnectError("busy", Code.Unavailable);
      },
      () => "d-2",
    ];
    const deployed = await client.deploy("export const m = {};", {
      tables: [{ name: "messages", indexes: [{ name: "by_channel", fields: ["channel"] }] }],
    });
    assert.equal(deployed.deploymentId, "d-2");
    assert.equal(server.deploys.length, 2);
    assert.equal(server.deploys[1]?.schema?.tables[0]?.indexes[0]?.name, "by_channel");

    server.deployAnswers = [
      () => {
        throw new ConnectError("bad bundle", Code.InvalidArgument);
      },
    ];
    await assert.rejects(client.deploy("nope"), (e: { code?: string }) => {
      assert.equal(e.code, "INVALID_ARGUMENT");
      return true;
    });
    client.close();
  });
});
