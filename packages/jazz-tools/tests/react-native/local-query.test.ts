import { afterAll, beforeAll, describe, it } from "vitest";
import {
  app,
  conditionCases,
  seedTodosForConditions,
  assertConditionQuery,
  assertByteaQuery,
  assertUuidOrderQuery,
  assertWindowQuery,
} from "../shared/local-query-scenarios.js";
import { createNativeRelayFixture, withNativeRelayFixture } from "./fixture.js";
import type { Db } from "../../src/runtime/db.js";

describe("real RN local queries (shared browser scenarios)", () => {
  let fixture: Awaited<ReturnType<typeof createNativeRelayFixture>>;
  let db: Db;
  beforeAll(async () => {
    fixture = await createNativeRelayFixture(app);
    db = await fixture.createDb();
    await seedTodosForConditions(db);
  });
  afterAll(async () => {
    await fixture?.close();
  });
  for (const testCase of conditionCases) {
    it(`supports condition operator ${testCase.name}`, async () => {
      await assertConditionQuery(db, testCase);
    });
  }
  for (const [name, scenario] of [
    ["returns BYTEA columns as Uint8Array", assertByteaQuery],
    ["generates clock-backed UUIDv7 ids in insertion order", assertUuidOrderQuery],
    ["supports orderBy + limit + offset", assertWindowQuery],
  ] as const) {
    it(name, async () => {
      await withNativeRelayFixture(app, async (fixture) => {
        await scenario(await fixture.createDb());
      });
    });
  }
});
