import { afterEach, describe, expect, it } from "vitest";
import { schema as s } from "../../src/";
import { Db, type QueryBuilder } from "../../src/runtime/db.js";
import { createInspectorLocalQueryOptions as inspectorLocalQueryOptions } from "../../src/internal/inspector-query.js";
import { ReadTier } from "../../src/runtime/client.js";
import { generateAuthSecret } from "../../src/runtime/auth-secret-store.js";
import { deploy } from "../../src/dev/catalogue.js";
import {
  TestCleanup,
  createBrowserTestDb as createDb,
  sleep,
  uniqueDbName,
  waitForCondition,
  waitForQuery,
  withTimeout,
} from "./support.js";
import { getJazzServerInfo, type JazzServerInfo } from "./testing-server.js";

const schema = {
  todos: s.table({
    title: s.string(),
    done: s.boolean(),
  }),
};

type AppSchema = s.Schema<typeof schema>;
const app: s.App<AppSchema> = s.defineApp(schema);
const { todos } = app;
type Todo = s.RowOf<typeof todos>;

const allowAllPermissions = s.definePermissions(app, ({ policy }) => [
  policy.todos.allowRead.always(),
  policy.todos.allowInsert.always(),
  policy.todos.allowUpdate.always(),
  policy.todos.allowDelete.always(),
]);

const PENDING_ASSERTION_MS = 750;
const LOCAL_OPERATION_TIMEOUT_MS = 2_000;
// Persistent browser operations cross the SharedWorker boundary even when the
// public operation is local-only. Keep the direct-runtime responsiveness
// target above as a soft diagnostic, but allow for worker scheduling under the
// full browser suite before treating the operation as stuck.
const WORKER_OPERATION_TIMEOUT_MS = 5_000;
const SYNC_OPERATION_TIMEOUT_MS = 10_000;

type DbFactory = (
  ctx: TestCleanup,
  label: string,
  secret: string,
  server: JazzServerInfo,
) => Promise<Db>;

interface ConnectedPair {
  readonly db: Db;
  readonly peer: Db;
}

describe("Db disconnect/reconnect", () => {
  const ctx = new TestCleanup();

  afterEach(async () => {
    await ctx.cleanup();
  });

  it.each(["reconnect", "reopen"] as const)(
    "repairs a default local query after an offline predicate exit on worker %s",
    async (mode) => {
      const label = uniqueDbName("local-query-offline-exit");
      const server = await publishSyncServerSchemaAndPermissions(label);
      const secret = generateAuthSecret();
      const dbName = uniqueDbName(label);
      const open = async () =>
        ctx.track(
          await createDb({
            appId: server.appId,
            driver: { type: "persistent", dbName },
            serverUrl: server.serverUrl,
            secret,
          }),
        );
      let db = await open();
      const peer = await createDirectDb(ctx, `${label}-peer`, secret, server);
      const pending = todos.where({ done: { eq: false } });
      let visible: Todo[] = [];
      let stop = ctx.trackSubscription(
        db.subscribe(pending, (rows) => {
          visible = rows;
        }),
      );
      const row = await db.insert(todos, { title: label, done: false }).wait({ tier: "global" });
      await waitForCondition(
        async () => visible.some((item) => item.id === row.id),
        SYNC_OPERATION_TIMEOUT_MS,
        "local subscription did not show its own write",
      );
      await db.all(pending, { tier: "global" });
      await db.disconnect();
      if (mode === "reopen") {
        stop();
        await db.shutdown();
        ctx.untrack(db);
      }
      await peer.all(todoByTitle(label), { tier: "global" });
      await peer.update(todos, row.id, { done: true }).wait({ tier: "global" });
      if (mode === "reopen") {
        db = await open();
        visible = [];
        stop = ctx.trackSubscription(
          db.subscribe(pending, (rows) => {
            visible = rows;
          }),
        );
      }
      await db.reconnect();
      expect(await db.all(pending, { tier: "global" })).toEqual([]);
      // No broader remote query may fetch the missing newer version for us.
      await waitForCondition(
        async () => {
          const cached = await db.all(todoByTitle(label), inspectorLocalQueryOptions());
          return cached.some((item) => item.done) && !visible.some((item) => item.id === row.id);
        },
        SYNC_OPERATION_TIMEOUT_MS,
        "current-row repair did not reach the foreground local cache",
      );
      stop();
    },
    60_000,
  );

  it("removes a remotely deleted owned row from the current default local worker snapshot", async () => {
    const label = uniqueDbName("local-worker-live-delete");
    const server = await getJazzServerInfo(label);
    await deploy({
      ...server,
      schema: app,
      permissions: s.definePermissions(app, ({ policy, session }) => {
        policy.todos.allowRead.where({ "$createdBy.account": session.user.account });
        policy.todos.allowInsert.always();
        policy.todos.allowUpdate.where({ "$createdBy.account": session.user.account });
        policy.todos.allowDelete.where({ "$createdBy.account": session.user.account });
      }),
    });
    const secret = generateAuthSecret();
    const db = await createWorkerDb(ctx, `${label}-reader`, secret, server);
    const peer = await createWorkerDb(ctx, `${label}-writer`, secret, server);
    let current: Todo[] = [];
    let preserveSurvivor = false;
    const subsequentSnapshots: Todo[][] = [];
    ctx.trackSubscription(
      db.subscribe(todos, (rows) => {
        current = rows;
        if (preserveSurvivor) subsequentSnapshots.push(rows);
      }),
    );
    const row = await db.insert(todos, { title: label, done: false }).wait({ tier: "global" });
    const survivor = await db
      .insert(todos, { title: `${label}-survivor`, done: false })
      .wait({ tier: "global" });
    await waitForCondition(
      async () => current.some((item) => item.id === row.id),
      SYNC_OPERATION_TIMEOUT_MS,
      "reader must first show the row",
    );
    let peerCurrent: Todo[] = [];
    ctx.trackSubscription(
      peer.subscribe(todos, (rows) => {
        peerCurrent = rows;
      }),
    );
    await waitForCondition(
      async () =>
        peerCurrent.some((item) => item.id === row.id) &&
        peerCurrent.some((item) => item.id === survivor.id),
      SYNC_OPERATION_TIMEOUT_MS,
      "writer must hydrate both rows through default local query",
    );
    await peer.update(todos, row.id, { done: true }).wait({ tier: "global" });
    await waitForCondition(
      async () =>
        current.some((item) => item.id === row.id && item.title === label && item.done) &&
        current.some(
          (item) => item.id === survivor.id && item.title === `${label}-survivor` && !item.done,
        ),
      SYNC_OPERATION_TIMEOUT_MS,
      "reader must see the remote update before deletion",
    );
    preserveSurvivor = true;
    await peer.delete(todos, row.id).wait({ tier: "global" });
    await waitForCondition(
      async () =>
        !current.some((item) => item.id === row.id) &&
        current.some(
          (item) => item.id === survivor.id && item.title === `${label}-survivor` && !item.done,
        ),
      SYNC_OPERATION_TIMEOUT_MS,
      "current local subscription must remove target and preserve survivor",
    );
    expect(subsequentSnapshots.length).toBeGreaterThan(0);
    for (const rows of subsequentSnapshots) {
      expect(rows.find((item) => item.id === survivor.id)).toMatchObject({
        title: `${label}-survivor`,
        done: false,
      });
    }
  }, 60_000);

  describe("server-backed subscriptions", () => {
    it.each(["edge", "global"] as const)(
      "keeps a disconnected %s subscription pending, then hydrates its local write",
      async (tier) => {
        const { db, peer } = await createDbPair(ctx, createWorkerDb, createDirectDb);
        const serverTitle = "existing server row";
        await withTimeout(
          peer.insert(todos, { title: serverTitle, done: true }).wait({ tier: "edge" }),
          SYNC_OPERATION_TIMEOUT_MS,
          "server row did not reach edge",
        );
        await waitForTodos(
          db,
          (rows) => rows.some((row) => row.title === serverTitle),
          "db did not receive the existing server row",
          SYNC_OPERATION_TIMEOUT_MS,
          "edge",
        );
        await db.disconnect();

        const title = "pending optimistic write";
        const snapshots: Array<{
          rows: Todo[];
          edgeSettled: boolean;
          afterReconnect: boolean;
        }> = [];
        let edgeSettled = false;
        let reconnectRequested = false;
        const unsubscribe = ctx.trackSubscription(
          db.subscribe(
            app.todos,
            (rows) => {
              snapshots.push({
                rows,
                edgeSettled,
                afterReconnect: reconnectRequested,
              });
            },
            { tier },
          ),
        );

        const write = db.insert(todos, { title, done: false });
        const edgeWait = write.wait({ tier }).then(() => {
          edgeSettled = true;
        });
        await withTimeout(
          write.wait({ tier: "local" }),
          LOCAL_OPERATION_TIMEOUT_MS,
          "local write did not become visible",
        );
        await expectStillPending(
          write.wait({ tier }),
          PENDING_ASSERTION_MS,
          `${tier} write settled before the delayed server snapshot was allowed to arrive`,
        );
        expect(snapshots).toEqual([]);
        reconnectRequested = true;
        await db.reconnect();
        await waitForCondition(
          async () =>
            snapshots.some(
              ({ rows, afterReconnect }) =>
                afterReconnect && rows.some((row) => row.title === title),
            ),
          SYNC_OPERATION_TIMEOUT_MS,
          `${tier} subscription did not publish its authoritative snapshot after reconnect`,
        );
        const beforeAcceptance = snapshots.at(-1)!.rows;
        await withTimeout(
          edgeWait,
          SYNC_OPERATION_TIMEOUT_MS,
          `local write did not settle at ${tier} after reconnect`,
        );

        expect(snapshots.at(-1)!.rows).toEqual(beforeAcceptance);
        expect(snapshots[0]!.afterReconnect).toBe(true);
        unsubscribe();
      },
      60_000,
    );

    it("keeps an edge subscription pending while disconnected, then hydrates its local update", async () => {
      const { db, peer } = await createDbPair(ctx, createWorkerDb, createDirectDb);
      const title = "pending optimistic update";
      const serverRow = await withTimeout(
        peer.insert(todos, { title, done: false }).wait({ tier: "edge" }),
        SYNC_OPERATION_TIMEOUT_MS,
        "server row did not reach edge",
      );
      await waitForTodos(
        db,
        (rows) => rows.some((row) => row.id === serverRow.id),
        "db did not receive the row to update",
        SYNC_OPERATION_TIMEOUT_MS,
        "edge",
      );
      await db.disconnect();

      const snapshots: Array<{
        rows: Todo[];
        edgeSettled: boolean;
        afterReconnect: boolean;
      }> = [];
      let edgeSettled = false;
      let reconnectRequested = false;
      const unsubscribe = ctx.trackSubscription(
        db.subscribe(
          todoByTitle(title),
          (rows) => {
            snapshots.push({
              rows,
              edgeSettled,
              afterReconnect: reconnectRequested,
            });
          },
          { tier: "edge" },
        ),
      );

      const update = db.update(todos, serverRow.id, { done: true });
      const edgeWait = update.wait({ tier: "edge" }).then(() => {
        edgeSettled = true;
      });
      await withTimeout(
        update.wait({ tier: "local" }),
        LOCAL_OPERATION_TIMEOUT_MS,
        "local update did not become visible",
      );
      await expectStillPending(
        update.wait({ tier: "edge" }),
        PENDING_ASSERTION_MS,
        "update settled before the delayed server snapshot was allowed to arrive",
      );
      expect(snapshots).toEqual([]);
      reconnectRequested = true;
      await db.reconnect();
      await waitForCondition(
        async () =>
          snapshots.some(
            ({ rows, afterReconnect }) =>
              afterReconnect && rows.some((row) => row.id === serverRow.id && row.done),
          ),
        SYNC_OPERATION_TIMEOUT_MS,
        "edge subscription did not publish its authoritative snapshot after reconnect",
      );
      const beforeAcceptance = snapshots.at(-1)!.rows;
      await withTimeout(
        edgeWait,
        SYNC_OPERATION_TIMEOUT_MS,
        "local update did not settle at edge after reconnect",
      );

      expect(snapshots.at(-1)!.rows).toEqual(beforeAcceptance);
      expect(snapshots[0]!.rows.some((row) => row.id === serverRow.id && row.done)).toBe(true);
      unsubscribe();
    }, 60_000);

    it("keeps an edge subscription pending while disconnected, then hydrates its local delete", async () => {
      const { db, peer } = await createDbPair(ctx, createWorkerDb, createDirectDb);
      const title = "pending optimistic delete";
      const serverRow = await withTimeout(
        peer.insert(todos, { title, done: false }).wait({ tier: "edge" }),
        SYNC_OPERATION_TIMEOUT_MS,
        "server row did not reach edge",
      );
      await waitForTodos(
        db,
        (rows) => rows.some((row) => row.id === serverRow.id),
        "db did not receive the row to delete",
        SYNC_OPERATION_TIMEOUT_MS,
        "edge",
      );
      await db.disconnect();

      const deletion = db.delete(todos, serverRow.id);
      await withTimeout(
        deletion.wait({ tier: "local" }),
        LOCAL_OPERATION_TIMEOUT_MS,
        "local delete did not become visible",
      );
      const localRows = await withTimeout(
        db.all(todoByTitle(title), inspectorLocalQueryOptions()),
        LOCAL_OPERATION_TIMEOUT_MS,
        "local read did not reflect the delete",
      );
      expect(localRows).toEqual([]);

      const snapshots: Array<{
        rows: Todo[];
        edgeSettled: boolean;
        afterReconnect: boolean;
      }> = [];
      let edgeSettled = false;
      let reconnectRequested = false;
      const edgeWait = deletion.wait({ tier: "edge" }).then(() => {
        edgeSettled = true;
      });
      const unsubscribe = ctx.trackSubscription(
        db.subscribe(
          todoByTitle(title),
          (rows) => {
            snapshots.push({
              rows,
              edgeSettled,
              afterReconnect: reconnectRequested,
            });
          },
          { tier: "edge" },
        ),
      );
      await expectStillPending(
        deletion.wait({ tier: "edge" }),
        PENDING_ASSERTION_MS,
        "delete settled before the delayed server snapshot was allowed to arrive",
      );
      expect(snapshots).toEqual([]);
      reconnectRequested = true;
      await db.reconnect();
      await waitForCondition(
        async () =>
          snapshots.some(({ rows, afterReconnect }) => afterReconnect && rows.length === 0),
        SYNC_OPERATION_TIMEOUT_MS,
        "edge subscription did not publish its authoritative snapshot after reconnect",
      );
      const beforeAcceptance = snapshots.at(-1)!.rows;
      await withTimeout(
        edgeWait,
        SYNC_OPERATION_TIMEOUT_MS,
        "local delete did not settle at edge after reconnect",
      );

      expect(snapshots.at(-1)!.rows).toEqual(beforeAcceptance);
      expect(snapshots[0]!.rows).toEqual([]);
      unsubscribe();
    }, 60_000);

    it("publishes server deletions through an edge subscription", async () => {
      const { db, peer } = await createDbPair(ctx, createDirectDb);
      const deletedTitle = "server row deleted live";
      const serverRow = await withTimeout(
        peer.insert(todos, { title: deletedTitle, done: false }).wait({ tier: "edge" }),
        SYNC_OPERATION_TIMEOUT_MS,
        "server row did not reach edge",
      );
      const snapshots: Todo[][] = [];
      const unsubscribe = ctx.trackSubscription(
        db.subscribe(
          todoByTitle(deletedTitle),
          (rows) => {
            snapshots.push(rows);
          },
          { tier: "edge" },
        ),
      );

      await waitForCondition(
        async () => snapshots.some((rows) => rows.some((row) => row.title === deletedTitle)),
        SYNC_OPERATION_TIMEOUT_MS,
        "edge subscription did not show the server row",
      );
      await withTimeout(
        peer.delete(todos, serverRow.id).wait({ tier: "edge" }),
        SYNC_OPERATION_TIMEOUT_MS,
        "server deletion did not reach edge",
      );
      await waitForCondition(
        async () => snapshots.some((rows) => rows.length === 0),
        SYNC_OPERATION_TIMEOUT_MS,
        "edge subscription did not publish the server deletion",
      );
      unsubscribe();
    }, 60_000);

    it("syncs writes made while disconnected after reconnect", async () => {
      const { db, peer } = await createDbPair(ctx, createDirectDb);

      await db.disconnect();

      const offlineTitle = "offline write";
      db.insert(todos, { title: offlineTitle, done: true });

      const localRowsWhileOffline = await withTimeout(
        db.all(todoByTitle(offlineTitle), {
          tier: "local",
        }),
        LOCAL_OPERATION_TIMEOUT_MS,
        "direct server connection: local-tier read for disconnected write did not resolve",
      );
      expect(localRowsWhileOffline.some((row) => row.title === offlineTitle)).toBe(true);

      const peerRowsBeforeReconnect = await withTimeout(
        peer.all(todoByTitle(offlineTitle), inspectorLocalQueryOptions()),
        LOCAL_OPERATION_TIMEOUT_MS,
        "direct server connection: peer local read before reconnect did not resolve",
      );
      expect(peerRowsBeforeReconnect).toEqual([]);

      await db.reconnect();

      await waitForTodos(
        peer,
        (rows) => rows.some((row) => row.title === offlineTitle),
        "direct server connection: peer sees disconnected write after reconnect",
        SYNC_OPERATION_TIMEOUT_MS,
        "edge",
      );
    }, 60_000);

    it("receives server updates missed while disconnected after reconnect", async () => {
      const { db, peer } = await createDbPair(ctx, createDirectDb);

      await db.disconnect();

      const serverOnlyTitle = "server only";
      await withTimeout(
        peer.insert(todos, { title: serverOnlyTitle, done: true }).wait({ tier: "edge" }),
        SYNC_OPERATION_TIMEOUT_MS,
        "direct server connection: peer write did not reach edge while db was disconnected",
      );

      const localRowsWhileOffline = await withTimeout(
        db.all(todoByTitle(serverOnlyTitle), {
          tier: "local",
        }),
        LOCAL_OPERATION_TIMEOUT_MS,
        "direct server connection: local-tier read while disconnected did not resolve",
      );
      expect(localRowsWhileOffline).toEqual([]);

      await db.reconnect();

      await waitForTodos(
        db,
        (rows) => rows.some((row) => row.title === serverOnlyTitle),
        "direct server connection: disconnected client receives server update after reconnect",
        SYNC_OPERATION_TIMEOUT_MS,
        "edge",
      );
    }, 60_000);
  });

  describe("worker mode", () => {
    it("propagates explicit offline state across tabs in one worker namespace", async () => {
      const label = uniqueDbName("worker-namespace-disconnect");
      const server = await publishSyncServerSchemaAndPermissions(label);
      const secret = generateAuthSecret();
      const dbName = uniqueDbName(label);
      const owner = ctx.track(
        await createDb({
          appId: server.appId,
          driver: { type: "persistent", dbName },
          serverUrl: server.serverUrl,
          secret,
        }),
      );
      await owner.all(app.todos, { tier: "edge" });
      await owner.disconnect();

      const title = "namespace-wide offline write";
      const write = owner.insert(todos, { title, done: true });
      await withWorkerOperationTimeout(
        write.wait({ tier: "local" }),
        "worker namespace: owner local write did not resolve while disconnected",
      );

      // Attach only after the namespace is already offline. This is the
      // important race: the late tab must await its init-state handshake
      // before classifying RemoteIfPossible as Local rather than Edge.
      const editor = ctx.track(
        await createDb({
          appId: server.appId,
          driver: { type: "persistent", dbName },
          serverUrl: server.serverUrl,
          secret,
        }),
      );

      // The editor did not issue disconnect(), but it shares the durable
      // worker and must therefore make the same explicit-offline read choice.
      // An Edge read would exclude this not-yet-settled row.
      const localFallback = await withWorkerOperationTimeout(
        editor.all(todoByTitle(title), { tier: ReadTier.RemoteIfPossible }),
        "worker namespace: editor did not use local fallback after owner disconnect",
      );
      expect(localFallback).toHaveLength(1);
      expect(localFallback[0]?.title).toBe(title);

      const snapshots: Todo[][] = [];
      const unsubscribe = ctx.trackSubscription(
        editor.subscribe(todoByTitle(title), (rows) => snapshots.push(rows), {
          tier: ReadTier.RemoteIfPossible,
        }),
      );
      await waitForCondition(
        () => Promise.resolve(snapshots.some((rows) => rows.some((row) => row.title === title))),
        WORKER_OPERATION_TIMEOUT_MS,
        "worker namespace: late editor subscription did not use local fallback",
      );

      const edgeWait = write.wait({ tier: "edge" });
      await expectStillPending(
        edgeWait,
        PENDING_ASSERTION_MS,
        "worker namespace: editor edge wait settled while the shared worker was offline",
      );

      // Any attached tab can reconnect the namespace. The queued editor write
      // must retain its ordinary fate route and settle after that reconnect.
      await editor.reconnect();
      await withTimeout(
        edgeWait,
        SYNC_OPERATION_TIMEOUT_MS,
        "worker namespace: editor write did not settle after reconnect",
      );
      unsubscribe();
    }, 60_000);

    it("syncs writes made while disconnected after reconnect", async () => {
      const { db, peer } = await createDbPair(ctx, createWorkerDb);

      await db.disconnect();

      const offlineTitle = "offline write";
      db.insert(todos, { title: offlineTitle, done: true });

      const localRows = await withWorkerOperationTimeout(
        db.all(todoByTitle(offlineTitle), {
          tier: "local",
        }),
        "worker mode: local-tier read for disconnected write did not resolve",
      );
      expect(localRows.some((row) => row.title === offlineTitle)).toBe(true);

      const peerRowsBeforeReconnect = await withWorkerOperationTimeout(
        peer.all(todoByTitle(offlineTitle), inspectorLocalQueryOptions()),
        "worker mode: peer local read before reconnect did not resolve",
      );
      expect(peerRowsBeforeReconnect).toEqual([]);

      await db.reconnect();

      await waitForTodos(
        peer,
        (rows) => rows.some((row) => row.title === offlineTitle),
        "worker mode: peer sees disconnected write after reconnect",
        SYNC_OPERATION_TIMEOUT_MS,
        "edge",
      );
    }, 60_000);

    it("receives server updates missed while disconnected after reconnect", async () => {
      const { db, peer } = await createDbPair(ctx, createWorkerDb);

      await db.disconnect();

      const serverOnlyTitle = "server only";
      await withTimeout(
        peer.insert(todos, { title: serverOnlyTitle, done: true }).wait({ tier: "edge" }),
        SYNC_OPERATION_TIMEOUT_MS,
        "worker mode: peer write did not reach edge while db was disconnected",
      );

      const disconnectedLocalRows = await withWorkerOperationTimeout(
        db.all(todoByTitle(serverOnlyTitle), {
          tier: "local",
        }),
        "worker mode: local-tier read while disconnected did not resolve",
      );
      expect(disconnectedLocalRows).toEqual([]);

      await db.reconnect();

      await waitForTodos(
        db,
        (rows) => rows.some((row) => row.title === serverOnlyTitle),
        "worker mode: disconnected client receives server update after reconnect",
        SYNC_OPERATION_TIMEOUT_MS,
        "edge",
      );
    }, 60_000);

    it("resolves local waits and keeps edge/global waits pending while disconnected", async () => {
      const { db } = await createDbPair(ctx, createWorkerDb);

      await db.disconnect();

      const write = db.insert(todos, { title: "local wait", done: false });
      const localWait = write.wait({ tier: "local" });
      // #2639: distinguish submission from the worker's Local acknowledgement
      // if this intermittently stalls, without logging identifiers or rows.
      const phase = { transactionId: "pending", localWait: "pending" };
      void write.txId.then(
        () => {
          phase.transactionId = "fulfilled";
        },
        () => {
          phase.transactionId = "rejected";
        },
      );
      void localWait.then(
        () => {
          phase.localWait = "fulfilled";
        },
        () => {
          phase.localWait = "rejected";
        },
      );
      await withWorkerOperationTimeout(
        localWait,
        "worker mode: local wait should resolve while disconnected",
        () => phase,
      );

      const edgeWait = db.insert(todos, { title: "edge wait", done: false }).wait({ tier: "edge" });
      await expectStillPending(
        edgeWait,
        PENDING_ASSERTION_MS,
        "worker mode: edge wait while disconnected",
      );

      const globalWait = db
        .insert(todos, { title: "global wait", done: false })
        .wait({ tier: "global" });
      await expectStillPending(
        globalWait,
        PENDING_ASSERTION_MS,
        "worker mode: global wait while disconnected",
      );

      await db.reconnect();

      await withTimeout(
        edgeWait,
        SYNC_OPERATION_TIMEOUT_MS,
        "worker mode: edge wait did not settle after reconnect",
      );
      await withTimeout(
        globalWait,
        SYNC_OPERATION_TIMEOUT_MS,
        "worker mode: global wait did not settle after reconnect",
      );
    }, 60_000);

    it("keeps local writes responsive while a disconnected edge query is pending", async () => {
      const { db } = await createDbPair(ctx, createWorkerDb);
      await db.disconnect();

      const title = "strict query FIFO";
      const edgeRead = db.all(todoByTitle(title), { tier: ReadTier.Remote });
      const laterWrite = db.insert(todos, { title, done: false });
      await withWorkerOperationTimeout(
        laterWrite.wait({ tier: "local" }),
        "worker mode: local write did not resolve independently of a parked edge query",
      );

      await db.reconnect();
      const rows = await edgeRead;
      expect(rows).toHaveLength(1);
      expect(rows[0]?.title).toBe(title);
      await withTimeout(
        laterWrite.wait({ tier: "local" }),
        SYNC_OPERATION_TIMEOUT_MS,
        "worker mode: queued write did not run after edge query",
      );
    }, 60_000);

    it("keeps local writes responsive while a disconnected edge wait is pending", async () => {
      const { db } = await createDbPair(ctx, createWorkerDb);
      const priorWrite = db.insert(todos, { title: "strict wait FIFO", done: false });
      await priorWrite.wait({ tier: "local" });
      await db.disconnect();

      const edgeWait = priorWrite.wait({ tier: "edge" });
      const laterWrite = db.insert(todos, { title: "after strict wait", done: false });
      await withWorkerOperationTimeout(
        laterWrite.wait({ tier: "local" }),
        "worker mode: local write did not resolve independently of a parked edge wait",
      );

      await db.reconnect();
      await withTimeout(edgeWait, SYNC_OPERATION_TIMEOUT_MS, "worker mode: edge wait did not run");
      await withTimeout(
        laterWrite.wait({ tier: "local" }),
        SYNC_OPERATION_TIMEOUT_MS,
        "worker mode: queued write did not run after edge wait",
      );
    }, 60_000);

    it("reconnects an already-pending public durability wait", async () => {
      const { db } = await createDbPair(ctx, createWorkerDb);
      const write = db.insert(todos, { title: "blocked durability wait", done: false });
      await write.wait({ tier: "local" });
      await db.disconnect();
      const edgeWait = write.wait({ tier: "edge" });
      await expectStillPending(
        edgeWait,
        PENDING_ASSERTION_MS,
        "worker mode: executing edge wait while disconnected",
      );

      await db.reconnect();
      await withTimeout(
        edgeWait,
        SYNC_OPERATION_TIMEOUT_MS,
        "worker mode: durability wait did not settle after reconnect",
      );
    }, 60_000);

    it("resolves local reads and defers edge reads while disconnected", async () => {
      const { db } = await createDbPair(ctx, createWorkerDb);

      await db.disconnect();

      const title = "read mode";
      db.insert(todos, { title, done: true });

      const localRows = await withWorkerOperationTimeout(
        db.all(todoByTitle(title), inspectorLocalQueryOptions()),
        "worker mode: immediate local read while disconnected did not resolve",
      );
      expect(localRows.some((row) => row.title === title)).toBe(true);

      const deferredRead = db.all(todoByTitle(title), {
        tier: ReadTier.Remote,
      });
      await expectStillPending(
        deferredRead,
        PENDING_ASSERTION_MS,
        "worker mode: deferred read while disconnected",
      );

      await db.reconnect();

      await withTimeout(
        deferredRead,
        SYNC_OPERATION_TIMEOUT_MS,
        "worker mode: deferred read did not resolve after reconnect",
      );
    }, 60_000);

    it("does not register a deferred subscription readiness waiter after immediate shutdown", async () => {
      const { db } = await createDbPair(ctx, createWorkerDb);
      await db.disconnect();

      let callbacks = 0;
      const unsubscribe = db.subscribe(
        app.todos,
        () => {
          callbacks += 1;
        },
        { tier: "edge" },
      );
      const connection = (db as unknown as { connection: { reconnectWaiters: Set<unknown> } })
        .connection;

      await db.shutdown();
      ctx.untrack(db);
      await Promise.resolve();
      await Promise.resolve();
      unsubscribe();
      expect(connection.reconnectWaiters.size).toBe(0);
      expect(callbacks).toBe(0);
    }, 60_000);
  });
});

async function createDirectDb(
  ctx: TestCleanup,
  _label: string,
  secret: string,
  server: JazzServerInfo,
): Promise<Db> {
  return ctx.track(
    await createDb({
      appId: server.appId,
      driver: { type: "memory" },
      serverUrl: server.serverUrl,
      secret,
    }),
  );
}

async function createWorkerDb(
  ctx: TestCleanup,
  label: string,
  secret: string,
  server: JazzServerInfo,
): Promise<Db> {
  return ctx.track(
    await createDb({
      appId: server.appId,
      driver: { type: "persistent", dbName: uniqueDbName(label) },
      serverUrl: server.serverUrl,
      secret,
    }),
  );
}

async function createDbPair(
  ctx: TestCleanup,
  createDbForMode: DbFactory,
  createPeerDb: DbFactory = createDbForMode,
): Promise<ConnectedPair> {
  const label = uniqueDbName("db-disconnect-pair");
  const server = await publishSyncServerSchemaAndPermissions(label);
  const sharedSecret = generateAuthSecret();
  const db = await createDbForMode(ctx, `${label}-a`, sharedSecret, server);
  const peer = await createPeerDb(ctx, `${label}-peer`, sharedSecret, server);

  return { db, peer };
}

function todoByTitle(title: string): QueryBuilder<Todo> {
  return app.todos.where({ title: { eq: title } });
}

async function expectStillPending<T>(
  promise: Promise<T>,
  timeoutMs: number,
  label: string,
): Promise<void> {
  const result = await Promise.race([
    promise.then(
      () => ({ state: "fulfilled" as const }),
      (error) => ({ state: "rejected" as const, error }),
    ),
    sleep(timeoutMs).then(() => ({ state: "pending" as const })),
  ]);

  if (result.state === "pending") return;

  const reason =
    result.state === "rejected" && result.error instanceof Error ? `: ${result.error.message}` : "";
  throw new Error(`${label} ${result.state}${reason}`);
}

async function withWorkerOperationTimeout<T>(
  promise: Promise<T>,
  label: string,
  failurePhase?: () => { transactionId: string; localWait: string },
): Promise<T> {
  const startedAt = performance.now();
  let softDeadlineExceeded = false;
  let softDeadlineDelayMs: number | null = null;
  const softDeadline = setTimeout(() => {
    softDeadlineExceeded = true;
    softDeadlineDelayMs = Math.max(
      0,
      Math.round(performance.now() - startedAt - LOCAL_OPERATION_TIMEOUT_MS),
    );
  }, LOCAL_OPERATION_TIMEOUT_MS);

  try {
    const result = await withTimeout(promise, WORKER_OPERATION_TIMEOUT_MS, label);
    if (softDeadlineExceeded) {
      console.warn(
        `${label}: exceeded ${LOCAL_OPERATION_TIMEOUT_MS}ms soft target; resolved after ${Math.round(performance.now() - startedAt)}ms`,
      );
    }
    return result;
  } catch (error) {
    if (failurePhase) {
      console.warn("JAZZ_LOCAL_WAIT_FAILURE", {
        ...failurePhase(),
        elapsedMs: Math.round(performance.now() - startedAt),
        softDeadlineDelayMs,
      });
    }
    throw error;
  } finally {
    clearTimeout(softDeadline);
  }
}

async function waitForTodos(
  db: Db,
  predicate: (rows: Todo[]) => boolean,
  label: string,
  timeoutMs = SYNC_OPERATION_TIMEOUT_MS,
  tier?: "local" | "edge",
): Promise<Todo[]> {
  return waitForQuery(db, app.todos, predicate, label, timeoutMs, tier);
}

async function publishSyncServerSchemaAndPermissions(
  requestedAppId: string,
): Promise<JazzServerInfo> {
  const testingServer = await getJazzServerInfo(requestedAppId);
  const { appId, serverUrl, adminSecret } = testingServer;
  await deploy({
    appId,
    serverUrl,
    adminSecret,
    schema: app,
    permissions: allowAllPermissions,
  });
  return testingServer;
}
