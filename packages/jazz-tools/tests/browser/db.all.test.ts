import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { createBrowserTestDb as createDb } from "./support.js";
import type { Db } from "../../src/runtime/db.js";
import {
  orgs,
  teams,
  users,
  todos,
  bundleItems,
  bundles,
  conditionCases,
  seedTodosForConditions,
  assertConditionQuery,
  assertByteaQuery,
  assertUuidOrderQuery,
  assertWindowQuery,
} from "../shared/local-query-scenarios.js";

function uniqueDbName(label: string): string {
  return `db-all-${label}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

describe("db.all browser integration", () => {
  const dbs: Db[] = [];
  let conditionsDb: Db;

  function track(db: Db): Db {
    dbs.push(db);
    return db;
  }

  afterEach(async () => {
    for (const db of dbs.splice(0).reverse()) {
      await db.shutdown();
    }
  });

  beforeAll(async () => {
    conditionsDb = await createDb({
      appId: "db-all-test",
      driver: { type: "persistent", dbName: uniqueDbName("ops") },
    });
    await seedTodosForConditions(conditionsDb);
  });

  afterAll(async () => {
    await conditionsDb.shutdown();
  });

  for (const testCase of conditionCases) {
    it(`supports condition operator ${testCase.name}`, async () => {
      await assertConditionQuery(conditionsDb, testCase);
    });
  }

  it("returns BYTEA columns as Uint8Array", async () => {
    const db = track(
      await createDb({
        appId: "db-all-test",
        driver: { type: "persistent", dbName: uniqueDbName("assertByteaQuery") },
      }),
    );
    await assertByteaQuery(db);
  });

  it("generates clock-backed UUIDv7 row ids and returns them in insertion order by default", async () => {
    const db = track(
      await createDb({
        appId: "db-all-test",
        driver: { type: "persistent", dbName: uniqueDbName("assertUuidOrderQuery") },
      }),
    );
    await assertUuidOrderQuery(db);
  });

  it("supports orderBy + limit + offset", async () => {
    const db = track(
      await createDb({
        appId: "db-all-test",
        driver: { type: "persistent", dbName: uniqueDbName("assertWindowQuery") },
      }),
    );
    await assertWindowQuery(db);
  });

  it("supports include relations", async () => {
    const db = track(
      await createDb({
        appId: "db-all-test",
        driver: { type: "persistent", dbName: uniqueDbName("include") },
      }),
    );

    const {
      value: { id: orgId },
    } = db.insert(orgs, { name: "Acme" });
    const {
      value: { id: teamId },
    } = db.insert(teams, {
      name: "Core",
      org_id: orgId,
      parent_id: undefined,
    });
    const {
      value: { id: ownerId },
    } = db.insert(users, { name: "Owner", team_id: teamId });
    db.insert(todos, {
      title: "with-owner-1",
      done: false,
      priority: 1,
      owner_id: ownerId,
      tags: ["x"],
    });
    db.insert(todos, {
      title: "with-owner-2",
      done: true,
      priority: 2,
      owner_id: ownerId,
      tags: ["y"],
    });

    const rows = await db.all(users.where({ id: ownerId }).include({ todosViaOwner: true }));

    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({
      id: ownerId,
      name: "Owner",
    });
    expect(rows[0].todosViaOwner).toHaveLength(2);
    expect(rows[0].todosViaOwner).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ title: "with-owner-1", owner_id: ownerId }),
        expect.objectContaining({ title: "with-owner-2", owner_id: ownerId }),
      ]),
    );
  });

  it("supports multi-hop queries", async () => {
    const db = track(
      await createDb({
        appId: "db-all-test",
        driver: { type: "persistent", dbName: uniqueDbName("hops") },
      }),
    );

    const {
      value: { id: orgId },
    } = db.insert(orgs, { name: "Org A" });
    const {
      value: { id: teamId },
    } = db.insert(teams, {
      name: "Team A",
      org_id: orgId,
      parent_id: undefined,
    });
    const {
      value: { id: userId },
    } = db.insert(users, { name: "User A", team_id: teamId });

    const rows = await db.all(users.where({ id: userId }).hopTo("team").hopTo("org"));

    expect(rows).toHaveLength(1);
    expect(rows[0]).toEqual({ id: orgId, name: "Org A" });
  });

  it("supports one-off all queries across scalar and UUID[] foreign-key hops", async () => {
    const db = track(
      await createDb({
        appId: "db-all-test",
        driver: { type: "persistent", dbName: uniqueDbName("fk-hops") },
      }),
    );

    const {
      value: { id: orgId },
    } = db.insert(orgs, { name: "FK Org" });
    const {
      value: { id: teamId },
    } = db.insert(teams, {
      name: "FK Team",
      org_id: orgId,
      parent_id: undefined,
    });
    const {
      value: { id: userId },
    } = db.insert(users, { name: "FK User", team_id: teamId });

    const {
      value: { id: partAId },
    } = db.insert(bundleItems, { label: "A" });
    const {
      value: { id: partBId },
    } = db.insert(bundleItems, { label: "B" });
    const {
      value: { id: bundleId },
    } = db.insert(bundles, { name: "Bundle 1", items: [partBId, partAId] });

    const teamRows = await db.all(users.where({ id: userId }).hopTo("team"));
    expect(teamRows).toHaveLength(1);
    expect(teamRows[0]).toMatchObject({ id: teamId, name: "FK Team" });

    const itemRows = await db.all(bundles.where({ id: bundleId }).hopTo("items"));
    expect(itemRows).toHaveLength(2);
    expect(itemRows.map((row) => row.label).sort()).toEqual(["A", "B"]);
  });

  it("supports gather queries", async () => {
    const db = track(
      await createDb({
        appId: "db-all-test",
        driver: { type: "persistent", dbName: uniqueDbName("gather") },
      }),
    );

    const {
      value: { id: rootId },
    } = db.insert(teams, {
      name: "root",
      org_id: undefined,
      parent_id: undefined,
    });
    const {
      value: { id: midId },
    } = db.insert(teams, {
      name: "mid",
      org_id: undefined,
      parent_id: rootId,
    });
    const {
      value: { id: leafId },
    } = db.insert(teams, {
      name: "leaf",
      org_id: undefined,
      parent_id: midId,
    });

    const rows = await db.all(
      teams.where({ id: leafId }).gather({
        maxDepth: 10,
        step: ({ current }) => teams.where({ id: current }).hopTo("parent"),
      }),
    );

    const ids = rows.map((row) => row.id).sort();
    expect(ids).toEqual([leafId, midId, rootId].sort());
  });
});
