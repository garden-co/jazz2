import { describe, expect, it } from "vitest";
import { runQuerySettlementBench } from "./testing/query-settlement-bench.js";
import { hasJazzNapiBuild } from "./testing/napi-runtime-test-utils.js";

const RUN_ABSTRACT_BENCH = process.env.JAZZ_ABSTRACT_BENCH === "1";

describe.skipIf(!RUN_ABSTRACT_BENCH)("query settlement abstract bench (node)", () => {
  it("measures burst-write settlement cost on the first full query", async () => {
    if (!hasJazzNapiBuild()) {
      throw new Error(
        "Node query-settlement bench requires jazz-napi build artifacts. Run `pnpm --filter jazz-napi build:debug` first.",
      );
    }

    const result = await runQuerySettlementBench({
      label: "node-napi",
    });

    expect(result.immediateFull.rowCount).toBeGreaterThan(0);
    expect(result.immediateFull.rowCount).toBe(result.localOnly.rowCount);
    expect(result.immediateFull.rowCount).toBe(result.warmedFull.rowCount);

    console.info(JSON.stringify(result, null, 2));
  }, 180_000);
});
