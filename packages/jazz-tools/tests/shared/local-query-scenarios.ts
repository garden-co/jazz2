import { expect } from "vitest";
import { schema } from "../../src/index.js";
import type { Db, QueryBuilder } from "../../src/runtime/db.js";

export const app = schema.defineApp({
  orgs: schema.table({ name: schema.string() }),
  teams: schema.table({
    name: schema.string(),
    org_id: schema.ref("orgs").optional(),
    parent_id: schema.ref("teams").optional(),
  }),
  users: schema.table({
    name: schema.string(),
    team_id: schema.ref("teams").optional(),
  }),
  todos: schema.table({
    title: schema.string(),
    done: schema.boolean(),
    priority: schema.int().optional(),
    owner_id: schema.ref("users").optional(),
    tags: schema.array(schema.string()),
    payload: schema.bytes().optional(),
  }),
  bundle_items: schema.table({ label: schema.string() }),
  bundles: schema.table({
    name: schema.string(),
    items: schema.array(schema.ref("bundle_items")),
  }),
});

export const { orgs, teams, users, todos, bundle_items: bundleItems, bundles } = app;
export type Todo = schema.RowOf<typeof todos>;

export const CONDITION_OWNER_ID = "00000000-0000-0000-0000-000000000101";
export const CONDITION_ALPHA_ID = "00000000-0000-0000-0000-000000000102";
export const CONDITION_BETA_ID = "00000000-0000-0000-0000-000000000103";
export const CONDITION_GAMMA_ID = "00000000-0000-0000-0000-000000000104";

export const conditionCases: Array<{
  name: string;
  query: QueryBuilder<Todo>;
  expectedTitles: string[];
}> = [
  {
    name: "eq",
    query: todos.where({ title: "alpha" }),
    expectedTitles: ["alpha"],
  },
  {
    name: "ne",
    query: todos.where({ title: { ne: "alpha" } }),
    expectedTitles: ["beta", "gamma"],
  },
  {
    name: "gt",
    query: todos.where({ priority: { gt: 1 } }),
    expectedTitles: ["beta"],
  },
  {
    name: "gte",
    query: todos.where({ priority: { gte: 2 } }),
    expectedTitles: ["beta"],
  },
  {
    name: "lt",
    query: todos.where({ priority: { lt: 2 } }),
    expectedTitles: ["alpha"],
  },
  {
    name: "lte",
    query: todos.where({ priority: { lte: 1 } }),
    expectedTitles: ["alpha"],
  },
  {
    name: "isNull",
    query: todos.where({ priority: null }),
    expectedTitles: ["gamma"],
  },
  {
    name: "contains-array",
    query: todos.where({ tags: { contains: "work" } }),
    expectedTitles: ["alpha", "gamma"],
  },
  {
    name: "contains-text",
    query: todos.where({ title: { contains: "alp" } }),
    expectedTitles: ["alpha"],
  },
  {
    name: "contains-text-empty",
    query: todos.where({ title: { contains: "" } }),
    expectedTitles: ["alpha", "beta", "gamma"],
  },
  {
    name: "in-id",
    query: todos.where({
      id: { in: [CONDITION_ALPHA_ID, "00000000-0000-0000-0000-000000000199"] },
    }),
    expectedTitles: ["alpha"],
  },
  {
    name: "in-text",
    query: todos.where({ title: { in: ["alpha", "gamma"] } }),
    expectedTitles: ["alpha", "gamma"],
  },
  {
    name: "in-boolean",
    query: todos.where({ done: { in: [false] } }),
    expectedTitles: ["alpha"],
  },
  {
    name: "in-number",
    query: todos.where({ priority: { in: [1, 999] } }),
    expectedTitles: ["alpha"],
  },
  {
    name: "in-reference",
    query: todos.where({ owner_id: { in: [CONDITION_OWNER_ID] } }),
    expectedTitles: ["alpha", "beta", "gamma"],
  },
  {
    name: "in-array-whole-value",
    query: todos.where({ tags: { in: [["work", "backend"]] } }),
    expectedTitles: ["alpha"],
  },
  {
    name: "in-bytea",
    query: todos.where({ payload: { in: [new Uint8Array([1, 2, 3])] } }),
    expectedTitles: ["alpha"],
  },
  {
    name: "in-empty",
    query: todos.where({ title: { in: [] } }),
    expectedTitles: [],
  },
  {
    name: "eq-bytea",
    query: todos.where({ payload: { eq: new Uint8Array([1, 2, 3]) } }),
    expectedTitles: ["alpha"],
  },
];
export async function seedTodosForConditions(db: Db): Promise<void> {
  const {
    value: { id: orgId },
  } = await db.insert(orgs, { name: "Acme" });
  const {
    value: { id: teamId },
  } = await db.insert(teams, {
    name: "Core",
    org_id: orgId,
    parent_id: undefined,
  });
  const {
    value: { id: userId },
  } = await db.insert(users, { name: "Alice", team_id: teamId }, { id: CONDITION_OWNER_ID });

  await db.insert(
    todos,
    {
      title: "alpha",
      done: false,
      priority: 1,
      owner_id: userId,
      tags: ["work", "backend"],
      payload: new Uint8Array([1, 2, 3]),
    },
    { id: CONDITION_ALPHA_ID },
  );
  await db.insert(
    todos,
    {
      title: "beta",
      done: true,
      priority: 2,
      owner_id: userId,
      tags: ["home"],
      payload: new Uint8Array([4, 5, 6]),
    },
    { id: CONDITION_BETA_ID },
  );
  await db.insert(
    todos,
    {
      title: "gamma",
      done: true,
      priority: undefined,
      owner_id: userId,
      tags: ["work", "urgent"],
      payload: undefined,
    },
    { id: CONDITION_GAMMA_ID },
  );
}
export async function assertByteaQuery(db: Db): Promise<void> {
  const {
    value: { id },
  } = await db.insert(todos, {
    title: "has-bytes",
    done: false,
    priority: 1,
    owner_id: undefined,
    tags: ["x"],
    payload: new Uint8Array([0, 1, 2, 255]),
  });

  const rows = await db.all(todos.where({ id }));

  expect(rows).toHaveLength(1);
  expect(rows[0]?.payload).toBeInstanceOf(Uint8Array);
  expect(Array.from(rows[0]?.payload ?? [])).toEqual([0, 1, 2, 255]);
}

export async function assertUuidOrderQuery(db: Db): Promise<void> {
  const ids: string[] = [];
  for (const title of ["first", "second", "third"]) {
    const result = await db.insert(todos, {
      title,
      done: false,
      priority: undefined,
      owner_id: undefined,
      tags: [],
      payload: undefined,
    });
    ids.push(result.value.id);
  }

  for (const id of ids) {
    expect(id).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
  }
  const generatedAtMs = Number.parseInt(ids[0]!.replaceAll("-", "").slice(0, 12), 16);
  expect(Math.abs(Date.now() - generatedAtMs)).toBeLessThan(60_000);
  expect(ids).toEqual([...ids].sort());

  const rows = await db.all(todos);
  expect(rows.map((row) => row.id)).toEqual(ids);
}

export async function assertWindowQuery(db: Db): Promise<void> {
  await db.insert(todos, {
    title: "p1",
    done: false,
    priority: 1,
    owner_id: undefined,
    tags: ["x"],
  });
  await db.insert(todos, {
    title: "p2",
    done: false,
    priority: 2,
    owner_id: undefined,
    tags: ["x"],
  });
  await db.insert(todos, {
    title: "p3",
    done: false,
    priority: 3,
    owner_id: undefined,
    tags: ["x"],
  });

  const rows = await db.all(todos.orderBy("priority", "desc").offset(1).limit(1));

  expect(rows).toHaveLength(1);
  expect(rows[0].priority).toBe(2);
  expect(rows[0].title).toBe("p2");
}

export async function assertConditionQuery(
  db: Db,
  testCase: (typeof conditionCases)[number],
): Promise<void> {
  const rows = await db.all(testCase.query);
  const actual = rows.map((row) => row.title).sort();
  const expected = [...testCase.expectedTitles].sort();
  expect(actual).toEqual(expected);
}
