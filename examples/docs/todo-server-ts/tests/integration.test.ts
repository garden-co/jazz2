/**
 * Integration tests for the todo server.
 *
 * These tests start the server programmatically with Fjall-backed storage,
 * exercise the full HTTP API, and clean up afterwards.
 */

import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { tmpdir } from "node:os";
import { mkdtempSync } from "node:fs";
import { join } from "node:path";
import { pushSchemaCatalogue, TestingServer } from "jazz-tools/testing";
import {
  createServer,
  startServer,
  stopServer,
  type RunningServer,
  type Todo,
} from "../src/main.ts";

describe("Todo Server Integration", () => {
  let server: RunningServer;
  let baseUrl: string;
  let upstream: Awaited<ReturnType<typeof TestingServer.start>> | undefined;

  beforeAll(async () => {
    upstream = await TestingServer.start();

    await pushSchemaCatalogue({
      serverUrl: upstream.url,
      appId: upstream.appId,
      adminSecret: upstream.adminSecret,
      schemaDir: join(import.meta.dirname ?? __dirname, ".."),
    });

    // Create server with temp persistent storage plus an ephemeral upstream server.
    const todoServer = await createServer({
      appId: upstream.appId,
      serverUrl: upstream.url,
      backendSecret: upstream.backendSecret,
      adminSecret: upstream.adminSecret,
    });

    // Start on random available port
    server = await startServer(todoServer, 0);
    baseUrl = server.baseUrl;
  });

  afterAll(async () => {
    if (server) {
      await stopServer(server);
    }

    if (upstream) {
      await upstream.stop();
    }
  });

  describe("Health Check", () => {
    it("returns healthy status", async () => {
      const res = await fetch(`${baseUrl}/health`);
      expect(res.status).toBe(200);
      const data = await res.json();
      expect(data.status).toBe("healthy");
    });
  });

  describe("CRUD Operations", () => {
    let createdTodoId: string;

    it("creates a todo", async () => {
      const res = await fetch(`${baseUrl}/todos`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          title: "Test Todo",
        }),
      });

      expect(res.status).toBe(201);
      const todo: Todo = await res.json();
      expect(todo.title).toBe("Test Todo");
      expect(todo.done).toBe(false);
      expect(todo.id).toBeDefined();

      createdTodoId = todo.id;
    });

    it("lists todos", async () => {
      const res = await fetch(`${baseUrl}/todos`);
      expect(res.status).toBe(200);
      const todos: Todo[] = await res.json();
      expect(Array.isArray(todos)).toBe(true);

      // Should include our created todo
      const found = todos.find((t) => t.id === createdTodoId);
      expect(found).toBeDefined();
      expect(found?.title).toBe("Test Todo");
    });

    it("gets a single todo", async () => {
      const res = await fetch(`${baseUrl}/todos/${createdTodoId}`);
      expect(res.status).toBe(200);
      const todo: Todo = await res.json();
      expect(todo.id).toBe(createdTodoId);
      expect(todo.title).toBe("Test Todo");
    });

    it("updates a todo", async () => {
      const res = await fetch(`${baseUrl}/todos/${createdTodoId}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          done: true,
          title: "Updated Todo",
        }),
      });

      expect(res.status).toBe(200);
      const todo: Todo = await res.json();
      expect(todo.done).toBe(true);
      expect(todo.title).toBe("Updated Todo");
    });

    it("deletes a todo", async () => {
      const res = await fetch(`${baseUrl}/todos/${createdTodoId}`, {
        method: "DELETE",
      });
      expect(res.status).toBe(204);

      // Verify it's gone
      const getRes = await fetch(`${baseUrl}/todos/${createdTodoId}`);
      expect(getRes.status).toBe(404);
    });
  });

  describe("Error Handling", () => {
    it("returns 404 for non-existent todo", async () => {
      const res = await fetch(`${baseUrl}/todos/00000000-0000-0000-0000-000000000000`);
      expect(res.status).toBe(404);
    });

    it("returns 400 for missing title", async () => {
      const res = await fetch(`${baseUrl}/todos`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({}),
      });
      expect(res.status).toBe(400);
    });
  });

  describe("Policy-Aware Session Reads", () => {
    it("filters rows by owner_id when querying with session context", async () => {
      const aliceTitle = `Alice private ${Date.now()}`;
      const bobTitle = `Bob private ${Date.now()}`;

      const createAlice = await fetch(`${baseUrl}/todos`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ title: aliceTitle, owner_id: "alice" }),
      });
      expect(createAlice.status).toBe(201);
      const aliceTodo: Todo = await createAlice.json();

      const createBob = await fetch(`${baseUrl}/todos`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ title: bobTitle, owner_id: "bob" }),
      });
      expect(createBob.status).toBe(201);
      const bobTodo: Todo = await createBob.json();

      const aliceViewRes = await fetch(`${baseUrl}/todos/as/alice`);
      expect(aliceViewRes.status).toBe(200);
      const aliceView: Todo[] = await aliceViewRes.json();
      const aliceTitles = new Set(aliceView.map((todo) => todo.title));
      expect(aliceTitles.has(aliceTitle)).toBe(true);
      expect(aliceTitles.has(bobTitle)).toBe(false);

      const bobViewRes = await fetch(`${baseUrl}/todos/as/bob`);
      expect(bobViewRes.status).toBe(200);
      const bobView: Todo[] = await bobViewRes.json();
      const bobTitles = new Set(bobView.map((todo) => todo.title));
      expect(bobTitles.has(bobTitle)).toBe(true);
      expect(bobTitles.has(aliceTitle)).toBe(false);

      const deleteAlice = await fetch(`${baseUrl}/todos/${aliceTodo.id}`, { method: "DELETE" });
      expect(deleteAlice.status).toBe(204);
      const deleteBob = await fetch(`${baseUrl}/todos/${bobTodo.id}`, { method: "DELETE" });
      expect(deleteBob.status).toBe(204);
    });
  });

  describe("Persistence / Cold Start", () => {
    it("survives a server restart", async () => {
      // Use an isolated upstream so CRUD test data doesn't leak into the count assertion.
      const coldStartUpstream = await TestingServer.start();
      await pushSchemaCatalogue({
        serverUrl: coldStartUpstream.url,
        appId: coldStartUpstream.appId,
        adminSecret: coldStartUpstream.adminSecret,
        schemaDir: join(import.meta.dirname ?? __dirname, ".."),
      });

      // Use a shared data path so both server instances see the same Fjall file
      const dataDir = mkdtempSync(join(tmpdir(), "jazz-cold-start-"));
      const dbPath = join(dataDir, "jazz.db");

      try {
        // --- First boot: create some todos ---
        const server1 = await startServer(
          await createServer({
            dataPath: dbPath,
            appId: coldStartUpstream.appId,
            serverUrl: coldStartUpstream.url,
            backendSecret: coldStartUpstream.backendSecret,
            adminSecret: coldStartUpstream.adminSecret,
          }),
          0,
        );

        let todo1: Todo;
        let todo2: Todo;
        try {
          const createRes1 = await fetch(`${server1.baseUrl}/todos`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ title: "Survive restart" }),
          });
          expect(createRes1.status).toBe(201);
          todo1 = await createRes1.json();

          const createRes2 = await fetch(`${server1.baseUrl}/todos`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ title: "Also persist" }),
          });
          expect(createRes2.status).toBe(201);
          todo2 = await createRes2.json();

          // Flush to disk before the restart.
          server1.flush();
        } finally {
          await stopServer(server1);
        }

        // --- Second boot: same data path, fresh server ---
        const server2 = await startServer(
          await createServer({
            dataPath: dbPath,
            appId: coldStartUpstream.appId,
            serverUrl: coldStartUpstream.url,
            backendSecret: coldStartUpstream.backendSecret,
            adminSecret: coldStartUpstream.adminSecret,
          }),
          0,
        );

        try {
          const listRes = await fetch(`${server2.baseUrl}/todos`);
          expect(listRes.status).toBe(200);
          const todos: Todo[] = await listRes.json();

          // Both todos should be present.
          expect(todos.length).toBe(2);

          const found1 = todos.find((t) => t.id === todo1.id);
          expect(found1).toBeDefined();
          expect(found1!.title).toBe("Survive restart");
          expect(found1!.done).toBe(false);

          const found2 = todos.find((t) => t.id === todo2.id);
          expect(found2).toBeDefined();
          expect(found2!.title).toBe("Also persist");
        } finally {
          await stopServer(server2);
        }
      } finally {
        await coldStartUpstream.stop();
      }
    });
  });

  describe("SSE Live Endpoint", () => {
    it("streams all todos and updates on changes", async () => {
      // Use an isolated upstream and local server so this test starts from a clean global state.
      const sseUpstream = await TestingServer.start();
      await pushSchemaCatalogue({
        serverUrl: sseUpstream.url,
        appId: sseUpstream.appId,
        adminSecret: sseUpstream.adminSecret,
        schemaDir: join(import.meta.dirname ?? __dirname, ".."),
      });
      const sseServer = await startServer(
        await createServer({
          appId: sseUpstream.appId,
          serverUrl: sseUpstream.url,
          backendSecret: sseUpstream.backendSecret,
          adminSecret: sseUpstream.adminSecret,
        }),
        0,
      );
      const sseBaseUrl = sseServer.baseUrl;
      let reader: ReadableStreamDefaultReader<Uint8Array> | undefined;
      try {
        // Connect to SSE endpoint
        const res = await fetch(`${sseBaseUrl}/todos/live`);
        expect(res.status).toBe(200);
        expect(res.headers.get("content-type")).toBe("text/event-stream");

        reader = res.body!.getReader();
        const decoder = new TextDecoder();

        // Helper to read next SSE event
        async function readEvent(): Promise<Todo[]> {
          let buffer = "";
          while (true) {
            const { value, done } = await reader.read();
            if (done) throw new Error("Stream ended unexpectedly");
            buffer += decoder.decode(value, { stream: true });

            // Parse SSE format: "data: {...}\n\n"
            const eventEnd = buffer.indexOf("\n\n");
            if (eventEnd !== -1) {
              const eventData = buffer.slice(0, eventEnd);
              buffer = buffer.slice(eventEnd + 2);

              const dataLine = eventData.split("\n").find((line) => line.startsWith("data: "));
              if (dataLine) {
                return JSON.parse(dataLine.slice(6));
              }
            }
          }
        }

        // 1. Initial event should be empty list
        const initial = await readEvent();
        expect(initial).toEqual([]);

        // 2. Create a todo - should see it in next event
        const createRes = await fetch(`${sseBaseUrl}/todos`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ title: "SSE Test Todo" }),
        });
        expect(createRes.status).toBe(201);
        const createdTodo: Todo = await createRes.json();

        const afterCreate = await readEvent();
        expect(afterCreate.length).toBe(1);
        expect(afterCreate[0].id).toBe(createdTodo.id);
        expect(afterCreate[0].title).toBe("SSE Test Todo");

        // 3. Update the todo - should see updated state
        await fetch(`${sseBaseUrl}/todos/${createdTodo.id}`, {
          method: "PUT",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ done: true }),
        });

        const afterUpdate = await readEvent();
        expect(afterUpdate.length).toBe(1);
        expect(afterUpdate[0].done).toBe(true);

        // 4. Delete the todo - should see empty list again
        await fetch(`${sseBaseUrl}/todos/${createdTodo.id}`, {
          method: "DELETE",
        });

        const afterDelete = await readEvent();
        expect(afterDelete).toEqual([]);
      } finally {
        if (reader) {
          await reader.cancel().catch(() => undefined);
        }
        await stopServer(sseServer);
        await sseUpstream.stop();
      }
    });
  });
});
