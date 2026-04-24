import {
  createDb,
  type Db,
  generateAuthSecret,
} from "../../../packages/jazz-tools/src/runtime/index.js";
import jazzWasmModule from "jazz-wasm/pkg/jazz_wasm_bg.wasm";
import { app } from "./schema.js";

const APP_ID = "cloudflare-worker-runtime-ts";

let dbPromise: Promise<Db> | null = null;

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body, null, 2), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
    },
  });
}

function getDb(): Promise<Db> {
  if (dbPromise) {
    return dbPromise;
  }

  dbPromise = createDb({
    appId: APP_ID,
    env: "dev",
    userBranch: "main",
    auth: { localFirstSecret: generateAuthSecret() },
    runtimeSources: {
      wasmModule: jazzWasmModule,
    },
  });

  return dbPromise;
}

async function listTodos(db: Db) {
  return db.all(app.todos);
}

async function handleSmoke(db: Db): Promise<Response> {
  const title = `workerd-${crypto.randomUUID().slice(0, 8)}`;
  const { value: inserted } = db.insert(app.todos, {
    title,
    done: false,
  });
  const todos = await listTodos(db);

  return json({
    ok: true,
    runtime: "cloudflare-workers",
    wasmInit: "runtimeSources.wasmModule",
    insertedId: inserted.id,
    todoCount: todos.length,
    todos,
  });
}

export default {
  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const db = await getDb();

    if (url.pathname === "/") {
      return json({
        ok: true,
        example: "cloudflare-worker-runtime-ts",
        verify: {
          smoke: "GET /smoke",
          todos: "GET /todos",
        },
      });
    }

    if (url.pathname === "/smoke") {
      return handleSmoke(db);
    }

    if (url.pathname === "/todos") {
      const todos = await listTodos(db);
      return json({
        ok: true,
        todoCount: todos.length,
        todos,
      });
    }

    return json(
      {
        ok: false,
        error: "Not found",
      },
      404,
    );
  },
};
