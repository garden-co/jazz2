import { randomUUID } from "node:crypto";
import { describe, expect, it } from "vitest";
import { schema as s } from "../index.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { createJazzSession } from "./index.js";

describe("nullable JSON through public native sessions", () => {
  for (const inMemory of [true, false]) {
    it(`inserts and clears JSON with ${inMemory ? "memory" : "persistent"} storage`, async () => {
      const app = s.defineApp({
        documents: s.table({ name: s.string(), metadata: s.json().optional() }),
      });
      const server = await startLocalJazzServer({ appId: randomUUID(), inMemory });
      let owner: Awaited<ReturnType<typeof createJazzSession>> | undefined;
      try {
        await deploy({
          serverUrl: server.url,
          appId: server.appId,
          adminSecret: server.adminSecret,
          schema: app,
          permissions: {},
        });
        owner = await createJazzSession({
          appId: server.appId,
          serverUrl: server.url,
          app,
          permissions: {},
          driver: { type: "memory" },
          initial: { backendSecret: server.backendSecret },
        });
        const db = owner.getSnapshot().client!.db;
        const write = await db.transaction((tx) =>
          tx.insert(app.documents, { name: "Alice", metadata: null }),
        );
        const row = await write.wait({ tier: "global" });
        const read = () => db.one(app.documents.where({ id: row.id }), { tier: "remote" });
        expect(await read()).toMatchObject({ id: row.id, metadata: null });
        await db
          .update(app.documents, row.id, { metadata: { answer: 42 } })
          .wait({ tier: "global" });
        expect(await read()).toMatchObject({ metadata: { answer: 42 } });
        const cleared = await db.exclusiveTransaction((tx) => {
          tx.update(app.documents, row.id, { metadata: null });
        });
        await cleared.wait();
        expect(await read()).toMatchObject({ metadata: null });
        expect(
          await db.all(app.documents.where({ metadata: null }), { tier: "remote" }),
        ).toMatchObject([{ id: row.id, metadata: null }]);
      } finally {
        await owner?.close();
        await server.stop();
      }
    }, 30_000);
  }
});
