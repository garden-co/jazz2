import { describe, expect, it } from "vitest";
import { schema as s } from "../../index.js";
import { createOpenTransactionId } from "../client.js";
import { testAuthorBytes } from "../testing/account-fixtures.js";
import { loadWasmModuleForTest } from "../testing/wasm-runtime-test-utils.js";
import { openConfig, queryFromTable, queryWithPredicates } from "./native-codec.js";
import { encodeSchema } from "./schema-codec.js";

const app = s.defineApp({
  folders: s.table({ title: s.string() }),
  notes: s.table({ text: s.string(), folderId: s.ref("folders") }),
});

describe("WASM backend read capability parity", () => {
  for (const backend of [false, true]) {
    it(`uses the consolidated read surface after ${backend ? "backend" : "ordinary"} open`, async () => {
      const { WasmDb } = await loadWasmModuleForTest();
      const open = backend ? WasmDb.openMemoryAsBackend : WasmDb.openMemory;
      const db = open(
        encodeSchema(app.wasmSchema),
        openConfig(
          new Uint8Array(16).fill(backend ? 1 : 2),
          testAuthorBytes(`wasm-backend-read-contract:${backend ? "backend" : "ordinary"}`),
          1,
          true,
        ),
      );
      const query = queryFromTable("notes");
      const relation = queryWithPredicates("notes", [], {
        relation: {
          Project: {
            input: { TableScan: { table: "notes" } },
            columns: [{ alias: "text", expr: { Column: { scope: "notes", column: "text" } } }],
          },
        },
      });
      const opts = { tier: "local" };
      const txId = createOpenTransactionId();
      db.beginTransaction(txId, "mergeable");
      const reads = [
        () => db.all(query, opts),
        () => db.all(query, opts, txId),
        () => db.all(relation, opts),
      ];
      try {
        for (const read of reads) expect(await resolveRead(read())).toBeInstanceOf(Uint8Array);
        await db.subscribe(query, opts).cancel();
        await db.subscribe(relation, opts).cancel();
      } finally {
        db.rollbackTransaction(txId);
        db.close();
      }
    });
  }
});

async function resolveRead(read: Uint8Array | { poll(): Uint8Array | null }): Promise<Uint8Array> {
  if (read instanceof Uint8Array) return read;
  for (;;) {
    const result = read.poll();
    if (result !== null) return result;
    await new Promise((resolve) => setTimeout(resolve, 0));
  }
}
