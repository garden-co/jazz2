import { describe, expect, it, vi } from "vitest";
import { JazzClient, type Row, type Runtime } from "./client.js";
import type { AppContext } from "./context.js";

const schemaWithTodos = {
  todos: {
    columns: [
      {
        name: "done",
        column_type: { type: "Boolean" as const },
        nullable: false,
      },
    ],
  },
} as AppContext["schema"];

function toBase64Url(value: unknown): string {
  const encoded = Buffer.from(JSON.stringify(value), "utf8").toString("base64");
  return encoded.replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
}

function makeJwt(payload: Record<string, unknown>): string {
  const header = { alg: "HS256", typ: "JWT" };
  return `${toBase64Url(header)}.${toBase64Url(payload)}.signature`;
}

async function flushMicrotasks(): Promise<void> {
  await Promise.resolve();
}

function mockRow(id = "todo-1"): Row {
  return { id, values: [] };
}

function makeClient() {
  const queryCalls: Array<[string, string | undefined, string | undefined, string | undefined]> =
    [];
  const createSubscriptionCalls: Array<
    [string, string | undefined, string | undefined, string | undefined]
  > = [];
  const executeSubscriptionCalls: Array<[number, Function]> = [];
  const unsubscribeCalls: number[] = [];
  let nextHandle = 0;

  const runtime: Runtime = {
    insert: () => ({ id: "00000000-0000-0000-0000-000000000001", values: [] }),
    update: () => {},
    delete: () => {},
    query: async (
      queryJson: string,
      sessionJson?: string | null,
      tier?: string | null,
      optionsJson?: string | null,
    ) => {
      queryCalls.push([
        queryJson,
        sessionJson ?? undefined,
        tier ?? undefined,
        optionsJson ?? undefined,
      ]);
      return [];
    },
    subscribe: () => nextHandle++,
    createSubscription: (
      queryJson: string,
      sessionJson?: string | null,
      tier?: string | null,
      optionsJson?: string | null,
    ) => {
      createSubscriptionCalls.push([
        queryJson,
        sessionJson ?? undefined,
        tier ?? undefined,
        optionsJson ?? undefined,
      ]);
      return nextHandle++;
    },
    executeSubscription: (handle: number, onUpdate: Function) => {
      executeSubscriptionCalls.push([handle, onUpdate]);
    },
    unsubscribe: (handle: number) => {
      unsubscribeCalls.push(handle);
    },
    insertDurable: async () => ({
      id: "00000000-0000-0000-0000-000000000001",
      values: [],
    }),
    updateDurable: async () => {},
    deleteDurable: async () => {},
    onSyncMessageReceived: () => {},
    addServer: () => {},
    removeServer: () => {},
    addClient: () => "00000000-0000-0000-0000-000000000001",
    getSchema: () => ({}),
    getSchemaHash: () => "schema-hash",
  };

  const context: AppContext = {
    appId: "test-app",
    schema: {},
    serverUrl: "http://localhost:1625",
    backendSecret: "test-backend-secret",
  };

  const JazzClientCtor = JazzClient as unknown as {
    new (
      runtime: Runtime,
      context: AppContext,
      defaultDurabilityTier: "local" | "edge" | "global",
    ): JazzClient;
  };
  return {
    client: new JazzClientCtor(runtime, context, "edge"),
    queryCalls,
    createSubscriptionCalls,
    executeSubscriptionCalls,
    unsubscribeCalls,
  };
}

function makeClientWithContext(context: AppContext): JazzClient {
  let nextHandle = 0;
  const runtime: Runtime = {
    insert: () => ({ id: "00000000-0000-0000-0000-000000000001", values: [] }),
    update: () => {},
    delete: () => {},
    query: async () => [],
    subscribe: () => nextHandle++,
    createSubscription: () => nextHandle++,
    executeSubscription: () => {},
    unsubscribe: () => {},
    insertDurable: async () => ({
      id: "00000000-0000-0000-0000-000000000001",
      values: [],
    }),
    updateDurable: async () => {},
    deleteDurable: async () => {},
    onSyncMessageReceived: () => {},
    addServer: () => {},
    removeServer: () => {},
    addClient: () => "00000000-0000-0000-0000-000000000001",
    getSchema: () => ({}),
    getSchemaHash: () => "schema-hash",
  };

  const JazzClientCtor = JazzClient as unknown as {
    new (
      runtime: Runtime,
      context: AppContext,
      defaultDurabilityTier: "local" | "edge" | "global",
    ): JazzClient;
  };
  return new JazzClientCtor(runtime, context, "edge");
}

describe("JazzClient.forRequest", () => {
  it("enables backend mode when backend secret + server URL are configured", () => {
    const { client } = makeClient();
    expect(client.asBackend()).toBe(client);
  });

  it("throws when backend mode is requested without backend secret", () => {
    const client = makeClientWithContext({
      appId: "test-app",
      schema: {},
      serverUrl: "http://localhost:1625",
    });
    expect(() => client.asBackend()).toThrow("backendSecret required for backend mode");
  });

  it("throws when backend mode is requested without server URL", () => {
    const client = makeClientWithContext({
      appId: "test-app",
      schema: {},
      backendSecret: "test-backend-secret",
    });
    expect(() => client.asBackend()).toThrow("serverUrl required for backend mode");
  });

  it("extracts sub + claims from a bearer JWT", async () => {
    const { client, queryCalls } = makeClient();
    const token = makeJwt({
      sub: "user-123",
      claims: { role: "admin" },
    });

    const scopedClient = client.forRequest({
      header(name: string) {
        return name.toLowerCase() === "authorization" ? `Bearer ${token}` : undefined;
      },
    });

    await scopedClient.query('{"table":"todos"}');

    expect(queryCalls.length).toBe(1);
    expect(queryCalls[0]![1]).toBe(
      JSON.stringify({
        user_id: "user-123",
        claims: { role: "admin" },
      }),
    );
  });

  it("supports Node-style headers object", async () => {
    const { client, queryCalls } = makeClient();
    const token = makeJwt({ sub: "user-456" });

    const scopedClient = client.forRequest({
      headers: {
        authorization: [`Bearer ${token}`],
      },
    });

    await scopedClient.query('{"table":"todos"}');

    expect(queryCalls[0]![1]).toBe(
      JSON.stringify({
        user_id: "user-456",
        claims: {},
      }),
    );
  });

  it("throws when Authorization header is missing", () => {
    const { client } = makeClient();

    expect(() => client.forRequest({ headers: {} })).toThrow(
      "Missing or invalid Authorization header",
    );
  });

  it("throws when JWT sub is missing", () => {
    const { client } = makeClient();
    const token = makeJwt({ claims: { role: "admin" } });

    expect(() =>
      client.forRequest({
        headers: {
          authorization: `Bearer ${token}`,
        },
      }),
    ).toThrow("JWT payload missing sub");
  });

  it("accepts query builders for session-scoped query calls", async () => {
    const { client, queryCalls } = makeClient();
    const token = makeJwt({ sub: "user-789" });

    const scopedClient = client.forRequest({
      headers: {
        authorization: `Bearer ${token}`,
      },
    });

    const builder = {
      _build() {
        return '{"table":"todos","conditions":[{"column":"done","op":"eq","value":true}]}';
      },
    };

    await scopedClient.query(builder);

    expect(queryCalls[0]![0]).toBe(builder._build());
  });

  it("translates schema-aware query builders for session-scoped query calls", async () => {
    const { client, queryCalls } = makeClient();
    const token = makeJwt({ sub: "user-901" });

    const scopedClient = client.forRequest({
      headers: {
        authorization: `Bearer ${token}`,
      },
    });

    const builder = {
      _schema: schemaWithTodos,
      _build() {
        return JSON.stringify({
          table: "todos",
          conditions: [{ column: "done", op: "eq", value: true }],
          includes: {},
          orderBy: [],
        });
      },
    };

    await scopedClient.query(builder);

    const parsed = JSON.parse(queryCalls[0]![0]) as Record<string, unknown>;
    expect(parsed.table).toBe("todos");
    expect(parsed).toHaveProperty("relation_ir");
  });

  it("accepts query builders for subscribe calls", async () => {
    const { client, createSubscriptionCalls, executeSubscriptionCalls } = makeClient();

    const builder = {
      _build() {
        return '{"table":"todos"}';
      },
    };

    client.subscribe(builder, () => {});

    expect(createSubscriptionCalls).toHaveLength(1);
    expect(createSubscriptionCalls[0]![0]).toBe(builder._build());
    expect(executeSubscriptionCalls).toHaveLength(0);
    await flushMicrotasks();
    expect(executeSubscriptionCalls).toHaveLength(1);
  });

  it("translates schema-aware query builders for subscribe calls", async () => {
    const { client, createSubscriptionCalls } = makeClient();

    const builder = {
      _schema: schemaWithTodos,
      _build() {
        return JSON.stringify({
          table: "todos",
          conditions: [],
          includes: {},
          orderBy: [],
        });
      },
    };

    client.subscribe(builder, () => {});

    expect(createSubscriptionCalls).toHaveLength(1);
    const parsed = JSON.parse(createSubscriptionCalls[0]![0]) as Record<string, unknown>;
    expect(parsed.table).toBe("todos");
    expect(parsed).toHaveProperty("relation_ir");
  });

  it("forwards structured RN delta payloads to subscription callbacks", async () => {
    const { client, executeSubscriptionCalls } = makeClient();
    const callback = vi.fn();
    client.subscribe('{"table":"todos"}', callback);
    await flushMicrotasks();

    const onUpdate = executeSubscriptionCalls[0]![1];
    onUpdate(
      JSON.stringify({
        added: [{ row: { id: "row-a", values: [] }, index: 0 }],
        removed: [{ row: { id: "row-r", values: [] }, index: 1 }],
        updated: [
          {
            old_row: { id: "row-u", values: [] },
            new_row: { id: "row-u", values: [] },
            old_index: 0,
            new_index: 0,
          },
        ],
        pending: false,
      }),
    );

    expect(callback).toHaveBeenCalledTimes(1);
    expect(callback).toHaveBeenCalledWith({
      added: [{ row: { id: "row-a", values: [] }, index: 0 }],
      removed: [{ row: { id: "row-r", values: [] }, index: 1 }],
      updated: [
        {
          old_row: { id: "row-u", values: [] },
          new_row: { id: "row-u", values: [] },
          old_index: 0,
          new_index: 0,
        },
      ],
      pending: false,
    });
  });

  it("forwards NAPI error-first delta payloads to subscription callbacks", async () => {
    const { client, executeSubscriptionCalls } = makeClient();
    const callback = vi.fn();
    client.subscribe('{"table":"todos"}', callback);
    await flushMicrotasks();

    const onUpdate = executeSubscriptionCalls[0]![1];
    onUpdate(null, [
      {
        kind: 0,
        id: "row-a",
        index: 0,
        row: { id: "row-a", values: [] },
      },
    ]);

    expect(callback).toHaveBeenCalledTimes(1);
    expect(callback).toHaveBeenCalledWith([
      {
        kind: 0,
        id: "row-a",
        index: 0,
        row: { id: "row-a", values: [] },
      },
    ]);
  });

  it("forwards partial structured deltas without throwing", async () => {
    const { client, executeSubscriptionCalls } = makeClient();
    const callback = vi.fn();
    client.subscribe('{"table":"todos"}', callback);
    await flushMicrotasks();

    const onUpdate = executeSubscriptionCalls[0]![1];
    expect(() =>
      onUpdate(
        JSON.stringify({
          pending: true,
        }),
      ),
    ).not.toThrow();

    expect(callback).toHaveBeenCalledWith({
      pending: true,
    });
  });

  it("passes query propagation options to runtime query", async () => {
    const { client, queryCalls } = makeClient();
    await client.query('{"table":"todos"}', { propagation: "local-only" });
    expect(queryCalls[0]![3]).toBe(JSON.stringify({ propagation: "local-only" }));
  });

  it("passes strict transaction visibility options to runtime query", async () => {
    const { client, queryCalls } = makeClient();
    await client.query('{"table":"todos"}', { strictTransactions: true });
    expect(queryCalls[0]![3]).toBe(JSON.stringify({ strict_transactions: true }));
  });

  it("passes query propagation options to runtime createSubscription", () => {
    const { client, createSubscriptionCalls } = makeClient();
    client.subscribe('{"table":"todos"}', () => {}, {
      propagation: "local-only",
    });
    expect(createSubscriptionCalls[0]![3]).toBe(JSON.stringify({ propagation: "local-only" }));
  });

  it("passes strict transaction visibility options to runtime createSubscription", () => {
    const { client, createSubscriptionCalls } = makeClient();
    client.subscribe('{"table":"todos"}', () => {}, {
      strictTransactions: true,
    });
    expect(createSubscriptionCalls[0]![3]).toBe(JSON.stringify({ strict_transactions: true }));
  });

  // =========================================================================
  // 2-phase subscribe lifecycle
  // =========================================================================

  it("createSubscription is called synchronously, executeSubscription is deferred", async () => {
    const { client, createSubscriptionCalls, executeSubscriptionCalls } = makeClient();
    client.subscribe('{"table":"todos"}', () => {});

    expect(createSubscriptionCalls).toHaveLength(1);
    expect(executeSubscriptionCalls).toHaveLength(0);

    await flushMicrotasks();
    expect(executeSubscriptionCalls).toHaveLength(1);
  });

  it("returns the handle from runtime.createSubscription", () => {
    const { client } = makeClient();
    const subId = client.subscribe('{"table":"todos"}', () => {});
    expect(subId).toBe(0);
    const subId2 = client.subscribe('{"table":"todos"}', () => {});
    expect(subId2).toBe(1);
  });

  it("unsubscribe before execute calls runtime.unsubscribe with the handle", async () => {
    const { client, executeSubscriptionCalls, unsubscribeCalls } = makeClient();
    const subId = client.subscribe('{"table":"todos"}', () => {});
    client.unsubscribe(subId);

    expect(unsubscribeCalls).toEqual([0]);

    await flushMicrotasks();
    // executeSubscription still fires (the runtime no-ops since handle was already unsubscribed)
    expect(executeSubscriptionCalls).toHaveLength(1);
  });

  it("unsubscribe after execute calls runtime.unsubscribe", async () => {
    const { client, unsubscribeCalls } = makeClient();
    const subId = client.subscribe('{"table":"todos"}', () => {});
    await flushMicrotasks();
    client.unsubscribe(subId);
    expect(unsubscribeCalls).toEqual([0]);
  });

  it("unsubscribe unknown handle is a no-op", () => {
    const { client } = makeClient();
    expect(() => client.unsubscribe(123_456)).not.toThrow();
  });
});

describe("JazzClient schema order", () => {
  it("passes create values through in the declared schema order", async () => {
    const insert = vi.fn(() => mockRow());
    const insertDurable = vi.fn(async () => mockRow());
    const runtime: Runtime = {
      insert,
      update: () => {},
      delete: () => {},
      query: async () => [],
      subscribe: () => 0,
      createSubscription: () => 0,
      executeSubscription: () => {},
      unsubscribe: () => {},
      insertDurable,
      updateDurable: async () => {},
      deleteDurable: async () => {},
      onSyncMessageReceived: () => {},
      addServer: () => {},
      removeServer: () => {},
      addClient: () => "client-1",
      getSchema: () =>
        new Map([
          [
            "todos",
            {
              columns: [
                {
                  name: "done",
                  column_type: { type: "Boolean" as const },
                  nullable: false,
                },
                {
                  name: "title",
                  column_type: { type: "Text" as const },
                  nullable: false,
                },
              ],
            },
          ],
        ]),
      getSchemaHash: () => "schema-hash",
    };
    const client = JazzClient.connectWithRuntime(runtime, {
      appId: "test-app",
      schema: {
        todos: {
          columns: [
            {
              name: "title",
              column_type: { type: "Text" as const },
              nullable: false,
            },
            {
              name: "done",
              column_type: { type: "Boolean" as const },
              nullable: false,
            },
          ],
        },
      },
    });

    await client.create("todos", {
      title: { type: "Text", value: "Buy milk" },
      done: { type: "Boolean", value: false },
    });

    expect(insert).toHaveBeenCalledWith("todos", {
      title: { type: "Text", value: "Buy milk" },
      done: { type: "Boolean", value: false },
    });
    expect(insertDurable).not.toHaveBeenCalled();
  });

  it("reorders query rows back to the declared schema order", async () => {
    const runtime: Runtime = {
      insert: () => mockRow(),
      update: () => {},
      delete: () => {},
      query: async () => [
        {
          id: "todo-1",
          values: [
            { type: "Boolean", value: false },
            { type: "Text", value: "Buy milk" },
          ],
        },
      ],
      subscribe: () => 0,
      createSubscription: () => 0,
      executeSubscription: () => {},
      unsubscribe: () => {},
      insertDurable: async () => mockRow(),
      updateDurable: async () => {},
      deleteDurable: async () => {},
      onSyncMessageReceived: () => {},
      addServer: () => {},
      removeServer: () => {},
      addClient: () => "client-1",
      getSchema: () =>
        new Map([
          [
            "todos",
            {
              columns: [
                {
                  name: "done",
                  column_type: { type: "Boolean" as const },
                  nullable: false,
                },
                {
                  name: "title",
                  column_type: { type: "Text" as const },
                  nullable: false,
                },
              ],
            },
          ],
        ]),
      getSchemaHash: () => "schema-hash",
    };
    const client = JazzClient.connectWithRuntime(runtime, {
      appId: "test-app",
      schema: {
        todos: {
          columns: [
            {
              name: "title",
              column_type: { type: "Text" as const },
              nullable: false,
            },
            {
              name: "done",
              column_type: { type: "Boolean" as const },
              nullable: false,
            },
          ],
        },
      },
    });

    const rows = await client.query(
      JSON.stringify({ relation_ir: { TableScan: { table: "todos" } } }),
    );

    expect(rows).toEqual([
      {
        id: "todo-1",
        values: [
          { type: "Text", value: "Buy milk" },
          { type: "Boolean", value: false },
        ],
      },
    ]);
  });

  it("reorders query row columns while preserving included relation values", async () => {
    const runtime: Runtime = {
      insert: () => mockRow(),
      update: () => {},
      delete: () => {},
      query: async () => [
        {
          id: "todo-1",
          values: [
            { type: "Boolean", value: false },
            { type: "Text", value: "Buy milk" },
            {
              type: "Array",
              value: [
                {
                  type: "Row",
                  value: {
                    id: "project-1",
                    values: [{ type: "Text", value: "Inbox" }],
                  },
                },
              ],
            },
          ],
        },
      ],
      subscribe: () => 0,
      createSubscription: () => 0,
      executeSubscription: () => {},
      unsubscribe: () => {},
      insertDurable: async () => mockRow(),
      updateDurable: async () => {},
      deleteDurable: async () => {},
      onSyncMessageReceived: () => {},
      addServer: () => {},
      removeServer: () => {},
      addClient: () => "client-1",
      getSchema: () =>
        new Map([
          [
            "todos",
            {
              columns: [
                {
                  name: "done",
                  column_type: { type: "Boolean" as const },
                  nullable: false,
                },
                {
                  name: "title",
                  column_type: { type: "Text" as const },
                  nullable: false,
                },
              ],
            },
          ],
        ]),
      getSchemaHash: () => "schema-hash",
    };
    const client = JazzClient.connectWithRuntime(runtime, {
      appId: "test-app",
      schema: {
        todos: {
          columns: [
            {
              name: "title",
              column_type: { type: "Text" as const },
              nullable: false,
            },
            {
              name: "done",
              column_type: { type: "Boolean" as const },
              nullable: false,
            },
          ],
        },
      },
    });

    const rows = await client.query(
      JSON.stringify({ relation_ir: { TableScan: { table: "todos" } } }),
    );

    expect(rows).toEqual([
      {
        id: "todo-1",
        values: [
          { type: "Text", value: "Buy milk" },
          { type: "Boolean", value: false },
          {
            type: "Array",
            value: [
              {
                type: "Row",
                value: {
                  id: "project-1",
                  values: [{ type: "Text", value: "Inbox" }],
                },
              },
            ],
          },
        ],
      },
    ]);
  });

  it("reorders included relation row values to the declared schema order", async () => {
    const runtime: Runtime = {
      insert: () => mockRow(),
      update: () => {},
      delete: () => {},
      query: async () => [
        {
          id: "todo-1",
          values: [
            { type: "Boolean", value: false },
            { type: "Text", value: "Buy milk" },
            {
              type: "Array",
              value: [
                {
                  type: "Row",
                  value: {
                    id: "project-1",
                    values: [
                      { type: "Text", value: "inbox" },
                      { type: "Text", value: "Inbox" },
                    ],
                  },
                },
              ],
            },
          ],
        },
      ],
      subscribe: () => 0,
      createSubscription: () => 0,
      executeSubscription: () => {},
      unsubscribe: () => {},
      insertDurable: async () => mockRow(),
      updateDurable: async () => {},
      deleteDurable: async () => {},
      onSyncMessageReceived: () => {},
      addServer: () => {},
      removeServer: () => {},
      addClient: () => "client-1",
      getSchema: () =>
        new Map([
          [
            "todos",
            {
              columns: [
                {
                  name: "done",
                  column_type: { type: "Boolean" as const },
                  nullable: false,
                },
                {
                  name: "title",
                  column_type: { type: "Text" as const },
                  nullable: false,
                },
              ],
            },
          ],
          [
            "projects",
            {
              columns: [
                {
                  name: "slug",
                  column_type: { type: "Text" as const },
                  nullable: false,
                },
                {
                  name: "name",
                  column_type: { type: "Text" as const },
                  nullable: false,
                },
              ],
            },
          ],
        ]),
      getSchemaHash: () => "schema-hash",
    };
    const client = JazzClient.connectWithRuntime(runtime, {
      appId: "test-app",
      schema: {
        todos: {
          columns: [
            {
              name: "title",
              column_type: { type: "Text" as const },
              nullable: false,
            },
            {
              name: "done",
              column_type: { type: "Boolean" as const },
              nullable: false,
            },
          ],
        },
        projects: {
          columns: [
            {
              name: "name",
              column_type: { type: "Text" as const },
              nullable: false,
            },
            {
              name: "slug",
              column_type: { type: "Text" as const },
              nullable: false,
            },
          ],
        },
      },
    });

    const rows = await client.query(
      JSON.stringify({
        relation_ir: { TableScan: { table: "todos" } },
        array_subqueries: [{ table: "projects", nested_arrays: [] }],
      }),
    );

    expect(rows).toEqual([
      {
        id: "todo-1",
        values: [
          { type: "Text", value: "Buy milk" },
          { type: "Boolean", value: false },
          {
            type: "Array",
            value: [
              {
                type: "Row",
                value: {
                  id: "project-1",
                  values: [
                    { type: "Text", value: "Inbox" },
                    { type: "Text", value: "inbox" },
                  ],
                },
              },
            ],
          },
        ],
      },
    ]);
  });

  it("keeps magic projection values ahead of included rows during schema alignment", async () => {
    const runtime: Runtime = {
      insert: () => mockRow(),
      update: () => {},
      delete: () => {},
      query: async () => [
        {
          id: "todo-1",
          values: [
            { type: "Text", value: "Buy milk" },
            { type: "Boolean", value: true },
            {
              type: "Array",
              value: [
                {
                  type: "Row",
                  value: {
                    id: "project-1",
                    values: [
                      { type: "Text", value: "inbox" },
                      { type: "Text", value: "Inbox" },
                    ],
                  },
                },
              ],
            },
          ],
        },
      ],
      subscribe: () => 0,
      createSubscription: () => 0,
      executeSubscription: () => {},
      unsubscribe: () => {},
      insertDurable: async () => mockRow(),
      updateDurable: async () => {},
      deleteDurable: async () => {},
      onSyncMessageReceived: () => {},
      addServer: () => {},
      removeServer: () => {},
      addClient: () => "client-1",
      getSchema: () =>
        new Map([
          [
            "todos",
            {
              columns: [
                {
                  name: "done",
                  column_type: { type: "Boolean" as const },
                  nullable: false,
                },
                {
                  name: "title",
                  column_type: { type: "Text" as const },
                  nullable: false,
                },
              ],
            },
          ],
          [
            "projects",
            {
              columns: [
                {
                  name: "slug",
                  column_type: { type: "Text" as const },
                  nullable: false,
                },
                {
                  name: "name",
                  column_type: { type: "Text" as const },
                  nullable: false,
                },
              ],
            },
          ],
        ]),
      getSchemaHash: () => "schema-hash",
    };
    const client = JazzClient.connectWithRuntime(runtime, {
      appId: "test-app",
      schema: {
        todos: {
          columns: [
            {
              name: "title",
              column_type: { type: "Text" as const },
              nullable: false,
            },
            {
              name: "done",
              column_type: { type: "Boolean" as const },
              nullable: false,
            },
          ],
        },
        projects: {
          columns: [
            {
              name: "name",
              column_type: { type: "Text" as const },
              nullable: false,
            },
            {
              name: "slug",
              column_type: { type: "Text" as const },
              nullable: false,
            },
          ],
        },
      },
    });

    const rows = await client.query(
      JSON.stringify({
        table: "todos",
        relation_ir: { TableScan: { table: "todos" } },
        select_columns: ["title", "$canDelete", "__jazz_include_project"],
        array_subqueries: [{ table: "projects", nested_arrays: [] }],
      }),
    );

    expect(rows).toEqual([
      {
        id: "todo-1",
        values: [
          { type: "Text", value: "Buy milk" },
          { type: "Boolean", value: true },
          {
            type: "Array",
            value: [
              {
                type: "Row",
                value: {
                  id: "project-1",
                  values: [
                    { type: "Text", value: "Inbox" },
                    { type: "Text", value: "inbox" },
                  ],
                },
              },
            ],
          },
        ],
      },
    ]);
  });

  it("reorders subscription deltas back to the declared schema order", async () => {
    let onUpdate: ((delta: unknown) => void) | undefined;
    const runtime: Runtime = {
      insert: () => mockRow(),
      update: () => {},
      delete: () => {},
      query: async () => [],
      subscribe: () => 0,
      createSubscription: () => 1,
      executeSubscription: (_handle, callback) => {
        onUpdate = callback as (delta: unknown) => void;
      },
      unsubscribe: () => {},
      insertDurable: async () => mockRow(),
      updateDurable: async () => {},
      deleteDurable: async () => {},
      onSyncMessageReceived: () => {},
      addServer: () => {},
      removeServer: () => {},
      addClient: () => "client-1",
      getSchema: () =>
        new Map([
          [
            "todos",
            {
              columns: [
                {
                  name: "done",
                  column_type: { type: "Boolean" as const },
                  nullable: false,
                },
                {
                  name: "title",
                  column_type: { type: "Text" as const },
                  nullable: false,
                },
              ],
            },
          ],
        ]),
      getSchemaHash: () => "schema-hash",
    };
    const client = JazzClient.connectWithRuntime(runtime, {
      appId: "test-app",
      schema: {
        todos: {
          columns: [
            {
              name: "title",
              column_type: { type: "Text" as const },
              nullable: false,
            },
            {
              name: "done",
              column_type: { type: "Boolean" as const },
              nullable: false,
            },
          ],
        },
      },
    });
    const callback = vi.fn();

    client.subscribe(JSON.stringify({ relation_ir: { TableScan: { table: "todos" } } }), callback);
    await flushMicrotasks();
    onUpdate?.([
      {
        kind: 0,
        id: "todo-1",
        index: 0,
        row: {
          id: "todo-1",
          values: [
            { type: "Boolean", value: false },
            { type: "Text", value: "Buy milk" },
          ],
        },
      },
    ]);

    expect(callback).toHaveBeenCalledWith([
      {
        kind: 0,
        id: "todo-1",
        index: 0,
        row: {
          id: "todo-1",
          values: [
            { type: "Text", value: "Buy milk" },
            { type: "Boolean", value: false },
          ],
        },
      },
    ]);
  });
});
