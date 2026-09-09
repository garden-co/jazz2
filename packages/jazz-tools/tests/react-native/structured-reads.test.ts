import { describe, expect, it } from "vitest";
import { schema } from "../../src/index.js";
import { withNativeRelayFixture } from "./fixture.js";

const app = schema.defineApp({
  groups: schema.table({ name: schema.string() }),
  tasks: schema.table({ title: schema.string(), group_id: schema.ref("groups") }),
  notes: schema.table({ body: schema.string(), task_id: schema.ref("tasks") }),
});
const query = app.groups
  .include({
    tasksViaGroup: app.tasks.select("title").include({ notesViaTask: true }),
  })
  .requireIncludes();

// Browser donor: db.include-subscriptions.server.test.ts selected nested
// includes. This fixture replaces browser transport with the real native owner.
describe("React Native structured reads", () => {
  it("returns empty local hop results before inserts and after removing their last match", async () => {
    await withNativeRelayFixture(app, async (fixture) => {
      const db = await fixture.createDb();
      const related = app.tasks.where({ title: "included" }).hopTo("group").orderBy("name");
      expect(await db.all(related, { tier: "local" })).toEqual([]);
      const group = db.insert(app.groups, { name: "group" }).value;
      const task = db.insert(app.tasks, { title: "included", group_id: group.id }).value;
      expect(await db.all(related, { tier: "local" })).toEqual([group]);
      db.delete(app.tasks, task.id);
      expect(await db.all(related, { tier: "local" })).toEqual([]);
    });
  });

  it("executes a public hop through canonical async relation preparation", async () => {
    await withNativeRelayFixture(app, async (fixture) => {
      const db = await fixture.createDb();
      const first = db.insert(app.groups, { name: "first" }).value;
      const second = db.insert(app.groups, { name: "second" }).value;
      const excluded = db.insert(app.groups, { name: "excluded" }).value;
      const firstTask = db.insert(app.tasks, { title: "included", group_id: first.id }).value;
      db.insert(app.tasks, { title: "included", group_id: second.id });
      db.insert(app.tasks, { title: "excluded", group_id: excluded.id });
      const related = app.tasks.where({ title: "included" }).hopTo("group").orderBy("name");
      expect(await db.all(related)).toEqual([first, second]);
      db.update(app.tasks, firstTask.id, { title: "now excluded" });
      expect(await db.all(related)).toEqual([second]);
    });
  });

  it("maintains filtered relation hops through the canonical subscription", async () => {
    await withNativeRelayFixture(app, async (fixture) => {
      const db = await fixture.createDb();
      const first = db.insert(app.groups, { name: "first" }).value;
      const second = db.insert(app.groups, { name: "second" }).value;
      const task = db.insert(app.tasks, { title: "included", group_id: first.id }).value;
      const related = app.tasks.where({ title: "included" }).hopTo("group").orderBy("name");
      const snapshots: unknown[] = [];
      const stop = db.subscribe(related, (rows) => snapshots.push(rows));
      try {
        await expect.poll(() => snapshots.at(-1)).toEqual([first]);
        db.update(app.tasks, task.id, { group_id: second.id });
        await expect.poll(() => snapshots.at(-1)).toEqual([second]);
        db.update(app.tasks, task.id, { title: "excluded" });
        await expect.poll(() => snapshots.at(-1)).toEqual([]);
      } finally {
        stop();
      }
    });
  });

  it("hydrates selected nested includes and isolates transaction overlays", async () => {
    await withNativeRelayFixture(app, async (fixture) => {
      const db = await fixture.createDb();
      const group = db.insert(app.groups, { name: "group" }).value;
      const task = db.insert(app.tasks, { title: "task", group_id: group.id }).value;
      const note = db.insert(app.notes, { body: "base note", task_id: task.id }).value;
      expect(await db.all(query)).toMatchObject([
        {
          ...group,
          tasksViaGroup: [
            {
              id: task.id,
              title: "task",
              notesViaTask: [note],
            },
          ],
        },
      ]);
      const tx = db.beginExclusiveTransaction();
      tx.update(app.notes, note.id, { body: "draft note" });
      expect(await tx.all(query)).toMatchObject([
        { tasksViaGroup: [{ notesViaTask: [{ body: "draft note" }] }] },
      ]);
      expect(await db.all(query)).toMatchObject([
        { tasksViaGroup: [{ notesViaTask: [{ body: "base note" }] }] },
      ]);
      await tx.rollback();
      expect(await db.all(query)).toMatchObject([
        { tasksViaGroup: [{ notesViaTask: [{ body: "base note" }] }] },
      ]);
    });
  });

  it("delivers nested child updates and removals through shared subscription decoding", async () => {
    await withNativeRelayFixture(app, async (fixture) => {
      const db = await fixture.createDb();
      const group = db.insert(app.groups, { name: "group" }).value;
      const task = db.insert(app.tasks, { title: "task", group_id: group.id }).value;
      const note = db.insert(app.notes, { body: "base", task_id: task.id }).value;
      const snapshots: unknown[] = [];
      const unsubscribe = db.subscribe(query, (rows) => snapshots.push(rows));
      try {
        await expect
          .poll(() => snapshots.at(-1))
          .toMatchObject([{ tasksViaGroup: [{ notesViaTask: [{ body: "base" }] }] }]);
        db.update(app.notes, note.id, { body: "updated" });
        await expect
          .poll(() => snapshots.at(-1))
          .toMatchObject([{ tasksViaGroup: [{ notesViaTask: [{ body: "updated" }] }] }]);
        db.delete(app.notes, note.id);
        await expect
          .poll(() => snapshots.at(-1))
          .toMatchObject([{ tasksViaGroup: [{ notesViaTask: [] }] }]);
      } finally {
        unsubscribe();
      }
    });
  });
});
