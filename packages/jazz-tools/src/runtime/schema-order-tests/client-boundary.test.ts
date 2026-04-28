import { describe, expect, it, vi } from "vitest";
import {
  JazzClient,
  flushMicrotasks,
  mockRow,
  runtimeBatchRecordStubs,
  type Runtime,
} from "../client-tests/support.js";

describe("JazzClient schema order", () => {
  it("passes create values through in the declared schema order", async () => {
    const insert = vi.fn(() => mockRow());
    const runtime: Runtime = {
      ...runtimeBatchRecordStubs,
      insert,
      update: () => ({
        batchId: "batch-id",
      }),
      delete: () => ({
        batchId: "batch-id",
      }),
      query: async () => [],
      subscribe: () => 0,
      createSubscription: () => 0,
      executeSubscription: () => {},
      unsubscribe: () => {},
      onSyncMessageReceived: () => {},
      onSyncMessageToSend: () => {},
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
  });

  it("reorders query rows back to the declared schema order", async () => {
    const runtime: Runtime = {
      ...runtimeBatchRecordStubs,
      insert: () => mockRow(),
      update: () => ({
        batchId: "batch-id",
      }),
      delete: () => ({
        batchId: "batch-id",
      }),
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
      onSyncMessageReceived: () => {},
      onSyncMessageToSend: () => {},
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
      ...runtimeBatchRecordStubs,
      insert: () => mockRow(),
      update: () => ({
        batchId: "batch-id",
      }),
      delete: () => ({
        batchId: "batch-id",
      }),
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
      onSyncMessageReceived: () => {},
      onSyncMessageToSend: () => {},
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
      ...runtimeBatchRecordStubs,
      insert: () => mockRow(),
      update: () => ({
        batchId: "batch-id",
      }),
      delete: () => ({
        batchId: "batch-id",
      }),
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
      onSyncMessageReceived: () => {},
      onSyncMessageToSend: () => {},
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
      ...runtimeBatchRecordStubs,
      insert: () => mockRow(),
      update: () => ({
        batchId: "batch-id",
      }),
      delete: () => ({
        batchId: "batch-id",
      }),
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
      onSyncMessageReceived: () => {},
      onSyncMessageToSend: () => {},
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
      ...runtimeBatchRecordStubs,
      insert: () => mockRow(),
      update: () => ({
        batchId: "batch-id",
      }),
      delete: () => ({
        batchId: "batch-id",
      }),
      query: async () => [],
      subscribe: () => 0,
      createSubscription: () => 1,
      executeSubscription: (_handle, callback) => {
        onUpdate = callback as (delta: unknown) => void;
      },
      unsubscribe: () => {},
      onSyncMessageReceived: () => {},
      onSyncMessageToSend: () => {},
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
