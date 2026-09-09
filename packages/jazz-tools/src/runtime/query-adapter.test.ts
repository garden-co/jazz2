import { describe, expect, it } from "vitest";
import { schema as s } from "../index.js";
import { translateQuery } from "./query-adapter.js";

const app = s.defineApp({
  users: s.table({
    name: s.string(),
  }),
  projects: s.table({
    name: s.string(),
  }),
  todos: s.table({
    title: s.string(),
    body: s.string(),
    rank: s.bigint(),
    attachment: s.bytes(),
    metadata: s.json(),
    done: s.boolean(),
    projectId: s.ref("projects"),
    ownerId: s.ref("users").optional(),
  }),
  events: s.table({
    event: s.enum({
      message: { text: s.string(), level: s.int() },
      closed: { code: s.int() },
    }),
  }),
});

describe("translateQuery", () => {
  it("uses total structured author equality in both flat and relation predicates", () => {
    const author = {
      account: "00000000-0000-4000-8000-000000000001",
      identity: { issuer: "issuer", subject: "subject" },
    };
    const eq = app.todos.where({ $createdBy: author });
    const ne = app.todos.where({ $createdBy: { ne: author } });
    const flatEq = JSON.parse(translateQuery(eq._build(), app.wasmSchema)).conditions[0];
    const flatNe = JSON.parse(translateQuery(ne._build(), app.wasmSchema)).conditions[0];
    expect(flatEq.And[0]).toEqual({
      Cmp: {
        left: { column: "$createdBy.account" },
        op: "Eq",
        right: { Literal: { type: "Uuid", value: author.account } },
      },
    });
    expect(flatNe).toEqual({ Not: flatEq });
    const relation = JSON.parse(
      translateQuery(app.union([eq, ne])._build(), app.wasmSchema),
    ).relation_ir;
    const text = JSON.stringify(relation);
    for (const path of [
      "$createdBy.account",
      "$createdBy.identity.issuer",
      "$createdBy.identity.subject",
    ])
      expect(text).toContain(path);
    expect(text).not.toContain('"IsNotNull"');
    expect(text).toContain('"Not"');
  });

  it.each([
    {},
    { account: null },
    { account: null, identity: { issuer: "issuer", subject: "subject" } },
    { account: "not-a-uuid", identity: { issuer: "issuer", subject: "subject" } },
    { account: null, identity: { issuer: "issuer", subject: " " } },
    { account: null, identity: { issuer: "issuer", subject: "\ud800" } },
    { account: null, identity: { issuer: "issuer", subject: "subject", extra: true } },
    { account: null, identity: { issuer: "issuer", subject: "subject" }, extra: true },
  ])("rejects malformed whole-author filters without dropping constraints: %j", (author) => {
    expect(() =>
      translateQuery(
        app.todos.where({ $createdBy: { eq: author } } as never)._build(),
        app.wasmSchema,
      ),
    ).toThrow();
  });

  // https://github.com/garden-co/jazz/issues/2571
  it("preserves public union membership in the shared runtime query", () => {
    const union = app.union([
      app.users.where({ name: "first" }),
      app.users.where({ name: "second" }),
    ]);
    const translated = JSON.parse(translateQuery(union._build(), app.wasmSchema));
    expect(translated).toHaveProperty("relation_ir");
    expect(JSON.stringify(translated.relation_ir)).toContain('"Union"');
  });

  it("preserves signed bigint union literals through relation JSON", () => {
    const query = app.union([
      app.todos.where({ rank: 9_007_199_254_740_993n }),
      app.todos.where({ rank: -9_007_199_254_740_993n }),
    ]);
    const translated = JSON.parse(translateQuery(query._build(), app.wasmSchema));
    const relationJson = JSON.stringify(translated.relation_ir);
    expect(relationJson).toContain('"9007199254740993"');
    expect(relationJson).toContain('"-9007199254740993"');
  });

  it("derives bounded, typed-distinct union labels", () => {
    const first = app.todos.where({
      metadata: { eq: new Date("2026-01-01T00:00:00.000Z") },
    } as any);
    const second = app.todos.where({
      metadata: { eq: new Date("2026-01-02T00:00:00.000Z") },
    } as any);
    const bigint = app.todos.where({ metadata: { eq: 9_007_199_254_740_993n } } as any);
    const bytes = app.todos.where({ attachment: { eq: new Uint8Array([1, 2, 3]) } } as any);
    const wide = app.todos.where({ metadata: { eq: "x".repeat(10_000) } } as any);
    const union = app.union([first, second, bigint, bytes, wide]) as any;
    const labels = union._unionVal.union.inputs.map((arm: any) => arm.label);
    expect(labels).toHaveLength(5);
    expect(new Set(labels).size).toBe(5);
    expect(
      labels.every((label: string) => new TextEncoder().encode(label).byteLength <= 4096),
    ).toBe(true);

    const reordered = app.union([wide, bytes, bigint, second, first]) as any;
    expect(new Set(reordered._unionVal.union.inputs.map((arm: any) => arm.label))).toEqual(
      new Set(labels),
    );
    const inserted = app.union([first, app.todos.where({ title: "inserted" }), second]) as any;
    expect(inserted._unionVal.union.inputs.map((arm: any) => arm.label)).toEqual([
      labels[0],
      expect.any(String),
      labels[1],
    ]);

    const named = app.union({ duplicate: first, second });
    expect((named as any)._unionVal.union.inputs.map((arm: any) => arm.label)).toEqual([
      "duplicate",
      "second",
    ]);
    expect(() => app.union({ ["é".repeat(2049)]: first })).toThrow(/1..=4096 bytes/);

    const nestedForward = app.union({
      outer: app.union({ z: first, ä: second }),
    }) as any;
    const nestedReverse = app.union({
      outer: app.union({ ä: second, z: first }),
    }) as any;
    expect(nestedForward._unionVal.union.inputs[0].input.union.inputs).not.toEqual(
      nestedReverse._unionVal.union.inputs[0].input.union.inputs,
    );
    expect(nestedForward._unionVal.union.inputs[0].label).toEqual(
      nestedReverse._unionVal.union.inputs[0].label,
    );
  });

  it("rejects public union modifiers that retained relation lowering cannot compose", () => {
    const union = app.union([app.todos.where({ done: false }), app.todos.where({ title: "a" })]);
    expect(() => union.select("title")).toThrow(/select\(\.\.\.\) is not supported/);
    expect(() => union.include({})).toThrow(/include\(\.\.\.\) is not supported/);
    expect(() => union.requireIncludes()).toThrow(/requireIncludes\(\) is not supported/);
    expect(() => union.includeDeleted()).toThrow(/includeDeleted\(\) is not supported/);
  });

  it("rejects colliding externally supplied relation schemas during query lowering", () => {
    const ambiguousRelationsSchema = {
      users: {
        columns: [{ name: "name", column_type: { type: "Text" as const }, nullable: false }],
      },
      todos: {
        columns: [
          {
            name: "ownerId",
            column_type: { type: "Uuid" as const },
            nullable: false,
            references: "users",
          },
          {
            name: "owner_id",
            column_type: { type: "Uuid" as const },
            nullable: false,
            references: "users",
          },
        ],
      },
    };

    expect(() => translateQuery(app.todos._build(), ambiguousRelationsSchema)).toThrow(
      /Generated relation name "owner" is ambiguous on table "todos".*"todos.ownerId".*"todos.owner_id"/,
    );
  });

  it("rejects duplicate external descriptors before allowing a reference-name alias", () => {
    const duplicateDescriptorSchema = {
      users: {
        columns: [{ name: "name", column_type: { type: "Text" as const }, nullable: false }],
      },
      todos: {
        columns: [
          { name: "owner", column_type: { type: "Text" as const }, nullable: false },
          {
            name: "owner",
            column_type: { type: "Uuid" as const },
            nullable: false,
            references: "users",
          },
        ],
      },
    };

    expect(() => translateQuery(app.todos._build(), duplicateDescriptorSchema)).toThrow(
      /Table "todos" has duplicate column descriptor "owner": descriptor #1 \(Text\) conflicts with descriptor #2 \(Uuid referencing "users"\)/,
    );
  });

  it("rejects a forward relation that would shadow a stored output column", () => {
    expect(() =>
      s.defineApp({
        users: s.table({ name: s.string() }),
        todos: s.table({
          owner: s.string(),
          ownerId: s.ref("users"),
        }),
      }),
    ).toThrow(
      /Generated relation name "owner" on table "todos".*forward relation generated from reference column "todos.ownerId".*stored\/public output column "todos.owner"/,
    );
  });

  it("rejects a generated relation that would shadow the implicit public id", () => {
    expect(() =>
      s.defineApp({
        users: s.table({ name: s.string() }),
        todos: s.table({
          idId: s.ref("users"),
        }),
      }),
    ).toThrow(
      /Generated relation name "id" on table "todos".*forward relation generated from reference column "todos.idId".*stored\/public output column "todos.id"/,
    );
  });

  it("rejects a nested reverse relation that would shadow a stored output column", () => {
    expect(() =>
      s.defineApp({
        users: s.table({
          todosViaOwner: s.string(),
        }),
        todos: s.table({
          ownerId: s.ref("users"),
        }),
      }),
    ).toThrow(
      /Generated relation name "todosViaOwner" on table "users".*reverse relation generated from reference column "todos.ownerId".*stored\/public output column "users.todosViaOwner"/,
    );
  });

  it("preserves the established reference-column relation alias", () => {
    expect(() =>
      s.defineApp({
        users: s.table({ name: s.string() }),
        todos: s.table({
          owner: s.ref("users"),
        }),
      }),
    ).not.toThrow();
  });

  it("emits ordinary table queries on the flat Query path", () => {
    const query = app.todos
      .includeDeleted()
      .where({ done: false, ownerId: { isNull: true } })
      .include({ owner: true })
      .select("title")
      .orderBy("title", "desc")
      .limit(5)
      .offset(2);

    const translated = JSON.parse(translateQuery(query._build(), app.wasmSchema));

    expect(translated).toMatchObject({
      table: "todos",
      include_deleted: true,
      conditions: [
        {
          Cmp: {
            left: { column: "done" },
            op: "Eq",
            right: { Literal: { type: "Boolean", value: false } },
          },
        },
        { IsNull: { column: { column: "ownerId" } } },
      ],
      select_columns: [{ kind: "full", column: "title" }],
      order_by: [{ column: "title", direction: "Desc" }],
      limit: 5,
      offset: 2,
    });
    expect(translated.relation_ir).toBeUndefined();
    expect(translated.array_subqueries).toHaveLength(1);
  });

  it("uses the same canonical predicate IR for root and included membership filters", () => {
    const translated = JSON.parse(
      translateQuery(
        app.projects
          .where({ id: { notIn: ["00000000-0000-0000-0000-000000000001"] } })
          .include({ todosViaProject: app.todos.where({ title: { notIn: ["hidden"] } }) })
          ._build(),
        app.wasmSchema,
      ),
    );

    const rootPredicate = translated.conditions[0];
    const includePredicate = translated.array_subqueries[0].filters[0];
    expect(rootPredicate).toEqual({
      Not: {
        In: {
          left: { column: "id" },
          values: [{ Literal: { type: "Uuid", value: "00000000-0000-0000-0000-000000000001" } }],
        },
      },
    });
    expect(includePredicate).toEqual({
      Not: {
        In: {
          left: { column: "title" },
          values: [{ Literal: { type: "Text", value: "hidden" } }],
        },
      },
    });
  });

  it("lowers partial select descriptors to the native projection contract", () => {
    const translated = JSON.parse(
      translateQuery(
        app.todos
          .select({
            attachment: { from: 1_000_000, to: 2_000_000 },
            body: { from: 4, to: 124 },
            title: { fromUtf8: 4, toUtf8: 67 },
            metadata: { at: "/someKey/11/otherKey" },
          })
          ._build(),
        app.wasmSchema,
      ),
    );

    expect(translated.select_columns).toEqual([
      { kind: "bytes", column: "attachment", from: 1_000_000, to: 2_000_000 },
      { kind: "text_utf16", column: "body", from: 4, to: 124 },
      { kind: "text_utf8", column: "title", from: 4, to: 67 },
      { kind: "json_pointer", column: "metadata", at: "/someKey/11/otherKey" },
    ]);
  });

  it("rejects partial large-value selections in include builders without rejecting named columns", () => {
    const namedColumnInclude = JSON.parse(
      translateQuery(
        app.projects.include({ todosViaProject: app.todos.select("body") })._build(),
        app.wasmSchema,
      ),
    );
    expect(namedColumnInclude.array_subqueries).toMatchObject([
      { column_name: "todosViaProject", select_columns: ["body"] },
    ]);

    expect(() =>
      translateQuery(
        app.projects
          .include({
            todosViaProject: app.todos.select({ body: { from: 4, to: 124 } }),
          })
          ._build(),
        app.wasmSchema,
      ),
    ).toThrow(
      'Include builder for relation "todosViaProject" does not support partial large-value selections.',
    );

    expect(() =>
      translateQuery(
        app.projects
          .include({
            todosViaProject: app.todos.include({
              project: app.projects.select({ name: { from: 0, to: 1 } }),
            }),
          })
          ._build(),
        app.wasmSchema,
      ),
    ).toThrow(
      'Include builder for relation "project" does not support partial large-value selections.',
    );
  });
  it("orders relation hops by the projected output scope", () => {
    const translated = JSON.parse(
      translateQuery(
        app.todos.where({ done: false }).hopTo("owner").orderBy("name")._build(),
        app.wasmSchema,
      ),
    );
    const { input, terms } = translated.relation_ir.OrderBy;
    const projectedName = input.Project.columns.find(
      (column: { alias: string }) => column.alias === "name",
    );
    expect(terms).toEqual([{ column: projectedName.expr.Column, direction: "Asc" }]);
    expect(terms[0].column).toEqual({ scope: "__hop_0", column: "name" });
  });

  it("keeps native relation IR for relation traversal queries", () => {
    const translated = JSON.parse(
      translateQuery(app.todos.where({ done: false }).hopTo("owner")._build(), app.wasmSchema),
    );

    expect(translated.table).toBe("users");
    expect(translated.relation_ir).toBeDefined();
    expect(translated.conditions).toBeUndefined();
  });

  it("lowers a payload enum match to the first-class relation predicate", () => {
    const translated = JSON.parse(
      translateQuery(
        app.events.where({ event: { match: { type: "message", where: { level: 2 } } } })._build(),
        app.wasmSchema,
      ),
    );

    expect(translated.relation_ir).toEqual({
      Project: {
        input: {
          Filter: {
            input: { TableScan: { table: "events" } },
            predicate: {
              EnumMatch: {
                column: { column: "event", scope: "events" },
                case: "message",
                payload: {
                  Cmp: {
                    left: { column: "level" },
                    op: "Eq",
                    right: { Literal: { type: "Integer", value: 2 } },
                  },
                },
              },
            },
          },
        },
        columns: [{ alias: "event", expr: { Column: { column: "event", scope: "events" } } }],
      },
    });
  });

  it("rejects a payload enum match against an absent case field", () => {
    const built = JSON.parse(app.events._build());
    built.conditions = [
      { column: "event", op: "match", value: { type: "closed", where: { level: 2 } } },
    ];
    expect(() => translateQuery(JSON.stringify(built), app.wasmSchema)).toThrow(
      'unknown payload enum field "level" for case "closed"',
    );
  });

  it("treats an omitted include limit as unbounded", () => {
    const translated = JSON.parse(
      translateQuery(app.users.include({ todosViaOwner: app.todos })._build(), app.wasmSchema),
    );

    expect(translated.array_subqueries).toMatchObject([
      { column_name: "todosViaOwner", limit: null },
    ]);
  });

  it("preserves an omitted limit across subsequent query-builder clones", () => {
    const translated = JSON.parse(
      translateQuery(
        app.users
          .include({
            todosViaOwner: app.todos.select("title").orderBy("title"),
          })
          ._build(),
        app.wasmSchema,
      ),
    );

    expect(translated.array_subqueries).toMatchObject([
      { column_name: "todosViaOwner", limit: null },
    ]);
  });

  it("treats an omitted forward-relation limit as unbounded", () => {
    const translated = JSON.parse(
      translateQuery(app.todos.include({ owner: app.users })._build(), app.wasmSchema),
    );

    expect(translated.array_subqueries).toMatchObject([{ column_name: "owner", limit: null }]);
  });

  it("treats include shorthand as an explicit whole-relation request", () => {
    const translated = JSON.parse(
      translateQuery(app.users.include({ todosViaOwner: true })._build(), app.wasmSchema),
    );

    expect(translated.array_subqueries).toMatchObject([
      { column_name: "todosViaOwner", limit: null },
    ]);
  });

  it("keeps projected include fields in their public terminal namespace", () => {
    const translated = JSON.parse(
      translateQuery(app.todos.select("title").include({ owner: true })._build(), app.wasmSchema),
    );

    // Query bytes are ShapeAst v0-compatible: do not add a positional codec
    // field just to recover this name later. The collector descriptor carries
    // the public relation field directly.
    expect(translated.array_subqueries).toMatchObject([{ column_name: "owner" }]);
    expect(translated.array_subqueries[0]).not.toHaveProperty("public_name");
  });

  it("leaves required-include pagination at the core query boundary", () => {
    const translated = JSON.parse(
      translateQuery(
        app.todos.include({ project: true }).requireIncludes().offset(2).limit(1)._build(),
        app.wasmSchema,
      ),
    );

    expect(translated).toMatchObject({ limit: 1, offset: 2 });
    expect(translated.array_subqueries).toMatchObject([{ requirement: "AtLeastOne" }]);
    expect(translated).not.toHaveProperty("__jazz_client_limit");
    expect(translated).not.toHaveProperty("__jazz_client_offset");
  });
});
