import assert from "node:assert/strict";
import test from "node:test";
import {
  proveForegroundByteAbi,
  proveForegroundRevoked,
  proveForegroundScopeIsolation,
  proveSameJsiRuntimeWriteSubscription,
  type ForegroundByteCodec,
} from "./foreground-byte-abi.ts";
import { todosQuery } from "./scope-fixture.ts";
import { NATIVE_RELAY_ABI_V1 } from "jazz-rn/native-relay-abi";
import {
  createRecord,
  PostcardWriter,
  writeDescriptor,
} from "jazz-tools/_dev/native-binding-codec";

const capability = Uint8Array.from({ length: 32 }, (_, index) => index + 1);
const subscriptionRowId = Uint8Array.from({ length: 16 }, (_, index) => index + 17);
const codec = {
  encode(command: "probe" | "tick" | "close") {
    return Uint8Array.of(command === "probe" ? 0 : command === "tick" ? 1 : 2);
  },
  decode(bytes: Uint8Array) {
    if (bytes[0] === 0 && bytes[1] === NATIVE_RELAY_ABI_V1)
      return { type: "probe" as const, abiVersion: NATIVE_RELAY_ABI_V1 };
    if (bytes[0] === 1) return { type: "ticked" as const };
    if (bytes[0] === 2) return { type: "closed" as const, closed: bytes[1] === 1 };
    throw new Error("malformed response");
  },
};

test("foreground receipt sends the v1 Probe, Tick, and Close byte commands", () => {
  let closed = false;
  const commands: number[] = [];
  const stages: string[] = [];
  const foreground = {
    execute(command: Uint8Array) {
      commands.push(command[0]!);
      if (closed && command[0] !== 2) throw new Error("foreground closed");
      if (command[0] === 0) return Uint8Array.of(0, NATIVE_RELAY_ABI_V1);
      if (command[0] === 1) return Uint8Array.of(1);
      if (command[0] === 2) {
        const first = !closed;
        closed = true;
        return Uint8Array.of(2, first ? 1 : 0);
      }
      throw new Error("unexpected command");
    },
    tick() {},
    close: () => false,
  };
  proveForegroundByteAbi(
    {
      abiVersion: NATIVE_RELAY_ABI_V1,
      openAttached: (received) => {
        assert.deepEqual(received, capability);
        return foreground;
      },
    },
    capability,
    codec,
    (stage) => stages.push(stage),
  );
  assert.deepEqual(commands, [0, 1, 2, 0]);
  assert.deepEqual(stages, [
    "foreground-abi-version-failed",
    "foreground-open-failed",
    "foreground-probe-failed",
    "foreground-tick-failed",
    "foreground-close-failed",
  ]);
});

test("foreground receipt treats a native revocation as an execution error", () => {
  const revokedForeground = {
    execute() {
      throw new Error("native relay rejected revoked foreground");
    },
    tick() {},
    close: () => false,
  };
  proveForegroundRevoked(revokedForeground, codec.encode);
});

test("foreground receipt rejects a live alias after claimed revocation", () => {
  const liveForeground = {
    execute: () => Uint8Array.of(0, NATIVE_RELAY_ABI_V1),
    tick() {},
    close: () => false,
  };
  assert.throws(
    () => proveForegroundRevoked(liveForeground, codec.encode),
    /revoked foreground accepted Probe/,
  );
});

test("foreground ABI mismatch records its boundary before any open", () => {
  const stages: string[] = [];
  assert.throws(
    () =>
      proveForegroundByteAbi(
        {
          abiVersion: NATIVE_RELAY_ABI_V1 + 1,
          openAttached() {
            throw new Error("must not open");
          },
        },
        capability,
        codec,
        (stage) => stages.push(stage),
      ),
    /unexpected ABI/,
  );
  assert.deepEqual(stages, ["foreground-abi-version-failed"]);
});

test("scope-isolation receipt keeps both native-selected scope stores disjoint", async () => {
  const scopeA = Uint8Array.from({ length: 32 }, () => 1);
  const scopeB = Uint8Array.from({ length: 32 }, () => 2);
  const scopeCodec: ForegroundByteCodec = {
    encode(command) {
      if (command === "probe") return Uint8Array.of(0);
      if (command === "tick") return Uint8Array.of(1);
      if (command === "close") return Uint8Array.of(6);
      const tags = {
        all: 2,
        subscribe: 3,
        drainSubscription: 4,
        unsubscribe: 5,
        poll: 7,
        cancel: 8,
        beginTransaction: 9,
        insert: 10,
        update: 11,
        upsert: 12,
        delete: 13,
        commitTransaction: 14,
        rollbackTransaction: 15,
      } as const;
      if (command.type === "poll" || command.type === "cancel")
        return Uint8Array.of(tags[command.type], command.operation);
      return Uint8Array.of(tags[command.type]);
    },
    decode(bytes) {
      switch (bytes[0]) {
        case 2:
          return { type: "rows", rows: bytes.subarray(2) };
        case 3:
          return { type: "subscribed", subscription: 1 };
        case 4:
          return {
            type: "subscriptionEvents",
            events:
              bytes[1] === 2
                ? [{ type: "rejected", message: "fixed fixture rejection" }]
                : bytes[1] === 3
                  ? [{ type: "closed" }]
                  : bytes[1]
                    ? [
                        {
                          type: "delta",
                          reset: true,
                          settled: true,
                          tier: "local",
                          delta: new Uint8Array(),
                        },
                      ]
                    : [],
          };
        case 5:
          return { type: "unsubscribed", closed: true };
        case 9:
          return { type: "cancelled", cancelled: true };
        case 6:
          return { type: "closed", closed: bytes[1] === 1 };
        case 10:
          return { type: "transactionOpened", transaction: bytes[1]! };
        case 12:
          return { type: "mutationStaged" };
        case 13:
          return { type: "transactionCommitted", txId: bytes.subarray(1) };
        case 7:
          return { type: "pending", operation: bytes[1]! };
        default:
          throw new Error(`unexpected mock response ${bytes[0]}`);
      }
    },
  };
  let aWasWritten = false;
  let bWasWritten = false;
  const rows = (containsA: boolean, containsB: boolean) =>
    Uint8Array.of(
      2,
      0,
      ...(containsA ? utf8("scope-a-private-row") : []),
      ...(containsB ? utf8("scope-b-private-row") : []),
    );
  const scopeFactory = (
    bLeaksA: boolean,
    aLeaksB: boolean,
    delayedAReads = 0,
    writerMustProgress = false,
    writerMustYield = false,
    writerTerminal?: "rejected" | "closed",
  ) => ({
    abiVersion: NATIVE_RELAY_ABI_V1,
    openAttached(capability: Uint8Array) {
      const isA = capability[0] === 1;
      let stagedWrite = false;
      let subscribed = false;
      let published = false;
      let scheduler: ((urgency: string) => void) | undefined;
      return {
        execute(command: Uint8Array) {
          switch (command[0]) {
            case 9:
              return Uint8Array.of(10, 1); // TransactionOpened { 1 }
            case 12:
              if (writerMustProgress) stagedWrite = true;
              else if (isA) aWasWritten = true;
              else bWasWritten = true;
              return Uint8Array.of(12); // MutationStaged
            case 14:
              return Uint8Array.of(13, ...new Uint8Array(16).fill(1));
            case 2:
              // A separate foreground has no local rows until its retained
              // subscription actually receives a relay publication.
              if (!subscribed || !published) return rows(false, false);
              if (isA && delayedAReads > 0) {
                delayedAReads -= 1;
                return rows(false, false);
              }
              return rows(isA ? aWasWritten : bLeaksA, isA ? aLeaksB : bWasWritten);
            case 3:
              subscribed = true;
              return Uint8Array.of(3);
            case 4:
              return Uint8Array.of(
                4,
                writerTerminal === "rejected"
                  ? 2
                  : writerTerminal === "closed"
                    ? 3
                    : published
                      ? 1
                      : 0,
              );
            case 5:
              assert.equal(subscribed, true);
              subscribed = false;
              return Uint8Array.of(5);
            case 6:
              return Uint8Array.of(6, 1);
            default:
              throw new Error(`unexpected foreground command ${command[0]}`);
          }
        },
        tick() {
          if (subscribed) published = true;
          if (!stagedWrite) return;
          if (writerMustYield) {
            stagedWrite = false;
            setTimeout(() => {
              if (!scheduler) return;
              scheduler("immediate");
              if (isA) aWasWritten = true;
              else bWasWritten = true;
            }, 0);
            return;
          }
          if (isA) aWasWritten = true;
          else bWasWritten = true;
          stagedWrite = false;
        },
        setTickScheduler(callback: (urgency: string) => void) {
          scheduler = callback;
        },
        close: () => true,
      };
    },
  });

  const passing = scopeFactory(false, false);
  const stages: string[] = [];
  await proveForegroundScopeIsolation(
    passing,
    scopeA,
    scopeCodec,
    {
      write: "a",
      contains: ["a"],
      excludes: ["b"],
    },
    (stage) => stages.push(stage),
  );
  assert.deepEqual(stages, [
    "scope-isolation-open-failed",
    "scope-isolation-write-failed",
    "scope-isolation-writer-read-failed",
    "scope-isolation-open-failed",
    "scope-isolation-read-failed",
    "scope-isolation-assert-failed",
  ]);
  await proveForegroundScopeIsolation(passing, scopeB, scopeCodec, {
    write: "b",
    contains: ["b"],
    excludes: ["a"],
  });
  await proveForegroundScopeIsolation(passing, scopeA, scopeCodec, {
    contains: ["a"],
    excludes: ["b"],
  });

  const eventuallyVisible = scopeFactory(false, false, 2);
  await proveForegroundScopeIsolation(eventuallyVisible, scopeA, scopeCodec, {
    write: "a",
    contains: ["a"],
    excludes: ["b"],
  });

  // A hosted Android emulator can need far more than 96 zero-delay JS turns
  // to publish a committed row when host scheduling is contended immediately
  // after the release build. The receipt is bounded by elapsed time, not
  // scheduler turn count, so that load does not manufacture a false failure.
  const slowAndroidPublication = scopeFactory(false, false, 192);
  let slowClock = 0;
  await proveForegroundScopeIsolation(
    slowAndroidPublication,
    scopeA,
    scopeCodec,
    {
      write: "a",
      contains: ["a"],
      excludes: ["b"],
    },
    undefined,
    {
      timeoutMs: 1_000,
      now: () => slowClock,
      yieldTurn: async () => {
        slowClock += 1;
      },
    },
  );

  const stalledWriter = scopeFactory(false, false, 1_000);
  const writerDetails: string[] = [];
  let stalledWriterClock = 0;
  await assert.rejects(
    proveForegroundScopeIsolation(
      stalledWriter,
      scopeA,
      scopeCodec,
      { write: "a", contains: ["a"], excludes: ["b"] },
      undefined,
      {
        timeoutMs: 3,
        now: () => stalledWriterClock,
        yieldTurn: async () => {
          stalledWriterClock += 1;
        },
      },
      (detail) => writerDetails.push(detail),
    ),
    /did not settle before its bounded deadline/,
  );
  assert.deepEqual(writerDetails, [
    "scope-isolation-writer-read-detail:last-rows-wakes-0-polls-0-row-responses-2-ready-no",
  ]);

  const rejectedWriter = scopeFactory(false, false, 0, false, false, "rejected");
  const rejectedWriterDetails: string[] = [];
  await assert.rejects(
    proveForegroundScopeIsolation(
      rejectedWriter,
      scopeA,
      scopeCodec,
      { write: "a", contains: ["a"], excludes: ["b"] },
      undefined,
      undefined,
      (detail) => rejectedWriterDetails.push(detail),
    ),
    /subscription ended before its read/,
  );
  assert.deepEqual(rejectedWriterDetails, [
    "scope-isolation-writer-read-detail:last-rejected-wakes-0-polls-0-row-responses-0-ready-no",
  ]);

  await assert.rejects(
    proveForegroundScopeIsolation(
      scopeFactory(false, false, 0, false, false, "rejected"),
      scopeA,
      scopeCodec,
      { write: "a", contains: ["a"], excludes: ["b"] },
      undefined,
      undefined,
      () => Promise.reject(new Error("diagnostic sink rejected")),
    ),
    /subscription ended before its read/,
  );

  const terminal = await Promise.race([
    proveForegroundScopeIsolation(
      scopeFactory(false, false, 0, false, false, "rejected"),
      scopeA,
      scopeCodec,
      { write: "a", contains: ["a"], excludes: ["b"] },
      undefined,
      undefined,
      () => new Promise<void>(() => {}),
    ).then(
      () => "unexpected success",
      (error: Error) => error.message,
    ),
    new Promise<string>((resolve) => setTimeout(() => resolve("diagnostic blocked failure"), 25)),
  ]);
  assert.match(terminal, /subscription ended before its read/);

  aWasWritten = false;
  bWasWritten = false;
  const writerProgressRequired = scopeFactory(false, false, 0, true);
  await proveForegroundScopeIsolation(writerProgressRequired, scopeA, scopeCodec, {
    write: "a",
    contains: ["a"],
    excludes: ["b"],
  });

  aWasWritten = false;
  bWasWritten = false;
  const nativeWakeRequiresEventLoop = scopeFactory(false, false, 0, true, true);
  await proveForegroundScopeIsolation(nativeWakeRequiresEventLoop, scopeA, scopeCodec, {
    write: "a",
    contains: ["a"],
    excludes: ["b"],
  });

  const lifecycleReceipt = (failure?: "reader-open" | "reader-read" | "reader-close") => {
    let opens = 0;
    const closes: string[] = [];
    const factory = {
      abiVersion: NATIVE_RELAY_ABI_V1,
      openAttached() {
        opens += 1;
        if (opens === 2 && failure === "reader-open")
          throw new Error("planted reader open failure");
        const role = opens === 1 ? "writer" : "reader";
        return {
          execute(command: Uint8Array) {
            if (command[0] === 3) return Uint8Array.of(3);
            if (command[0] === 4) return Uint8Array.of(4, 1);
            if (command[0] === 5) return Uint8Array.of(5, 1);
            if (command[0] === 6) return Uint8Array.of(6, 1);
            if (role === "writer") {
              if (command[0] === 9) return Uint8Array.of(10, 1);
              if (command[0] === 12) return Uint8Array.of(12);
              if (command[0] === 14) return Uint8Array.of(13, ...new Uint8Array(16).fill(1));
              if (command[0] === 2) return rows(true, false);
            } else {
              if (command[0] === 2) {
                if (failure === "reader-read") throw new Error("planted reader read failure");
                return rows(true, false);
              }
            }
            throw new Error(`unexpected ${role} lifecycle command ${command[0]}`);
          },
          tick() {},
          setTickScheduler() {},
          close() {
            closes.push(role);
            if (role === "reader" && failure === "reader-close")
              throw new Error("planted reader close failure");
            return true;
          },
        };
      },
    };
    return { factory, closes };
  };

  const cleanLifecycle = lifecycleReceipt();
  await proveForegroundScopeIsolation(cleanLifecycle.factory, scopeA, scopeCodec, {
    write: "a",
    contains: ["a"],
    excludes: ["b"],
  });
  assert.deepEqual(cleanLifecycle.closes, ["reader", "writer"]);

  for (const failure of ["reader-open", "reader-read", "reader-close"] as const) {
    const failedLifecycle = lifecycleReceipt(failure);
    await assert.rejects(
      async () =>
        proveForegroundScopeIsolation(failedLifecycle.factory, scopeA, scopeCodec, {
          write: "a",
          contains: ["a"],
          excludes: ["b"],
        }),
      new RegExp(`planted ${failure.replace("-", " ")} failure`),
    );
    assert.deepEqual(
      failedLifecycle.closes,
      failure === "reader-open" ? ["writer"] : ["reader", "writer"],
    );
  }

  const pendingReceipt = (
    settles: boolean,
    emitsWake = true,
    settlesImmediately = false,
    pendingDrain = false,
  ) => {
    const cleanup: string[] = [];
    const metrics = { all: 0, poll: 0, tick: 0, close: 0 };
    let scheduler: ((urgency: string) => void) | undefined;
    const scheduleWake = () => {
      if (!emitsWake) return;
      setTimeout(() => {
        const ticksBeforeCallback = metrics.tick;
        scheduler?.("immediate");
        assert.equal(
          metrics.tick,
          ticksBeforeCallback,
          "native wake callback must not re-enter foreground.tick",
        );
      }, 0);
    };
    const factory = {
      abiVersion: NATIVE_RELAY_ABI_V1,
      openAttached() {
        return {
          execute(command: Uint8Array) {
            switch (command[0]) {
              case 3:
                return Uint8Array.of(3);
              case 4:
                if (pendingDrain) {
                  scheduleWake();
                  return Uint8Array.of(7, 42);
                }
                return Uint8Array.of(4, 1);
              case 5:
                cleanup.push("unsubscribe");
                return Uint8Array.of(5);
              case 8:
                assert.equal(command[1], 42);
                cleanup.push("cancel");
                return Uint8Array.of(9);
              case 2:
                metrics.all += 1;
                if (metrics.all > 1)
                  throw new Error("scope receipt reissued all instead of polling");
                if (settlesImmediately) return rows(true, false);
                scheduleWake();
                return Uint8Array.of(7, 42);
              case 7:
                metrics.poll += 1;
                assert.equal(command[1], 42);
                if (settles && metrics.poll === 2) return rows(true, false);
                scheduleWake();
                return Uint8Array.of(7, 42);
              case 6:
                return Uint8Array.of(6, 1);
              default:
                throw new Error(`unexpected pending foreground command ${command[0]}`);
            }
          },
          tick() {
            metrics.tick += 1;
          },
          setTickScheduler(callback: (urgency: string) => void) {
            scheduler = callback;
          },
          close() {
            metrics.close += 1;
            return true;
          },
        };
      },
    };
    return { factory, metrics, cleanup };
  };

  const pendingThenVisible = pendingReceipt(true);
  await proveForegroundScopeIsolation(pendingThenVisible.factory, scopeA, scopeCodec, {
    contains: ["a"],
    excludes: ["b"],
  });
  assert.deepEqual(pendingThenVisible.metrics, { all: 1, poll: 2, tick: 3, close: 1 });
  assert.deepEqual(pendingThenVisible.cleanup, ["unsubscribe"]);

  const pendingForever = pendingReceipt(false);
  let pendingClock = 0;
  await assert.rejects(
    async () =>
      proveForegroundScopeIsolation(
        pendingForever.factory,
        scopeA,
        scopeCodec,
        {
          contains: ["a"],
          excludes: ["b"],
        },
        undefined,
        {
          timeoutMs: 96,
          now: () => pendingClock,
          yieldTurn: async () => {
            await new Promise<void>((resolve) => setTimeout(resolve, 0));
            pendingClock += 1;
          },
        },
      ),
    /did not settle before its bounded deadline/,
  );
  assert.deepEqual(pendingForever.metrics, { all: 1, poll: 94, tick: 96, close: 1 });
  assert.deepEqual(pendingForever.cleanup, ["cancel", "unsubscribe"]);

  const pendingPublication = pendingReceipt(false, true, false, true);
  let publicationClock = 0;
  await assert.rejects(
    proveForegroundScopeIsolation(
      pendingPublication.factory,
      scopeA,
      scopeCodec,
      { contains: ["a"], excludes: ["b"] },
      undefined,
      {
        timeoutMs: 3,
        now: () => publicationClock,
        yieldTurn: async () => {
          await new Promise<void>((resolve) => setTimeout(resolve, 0));
          publicationClock += 1;
        },
      },
    ),
    /did not settle before its bounded deadline/,
  );
  assert.equal(pendingPublication.metrics.all, 0);
  assert.deepEqual(pendingPublication.cleanup, ["cancel", "unsubscribe"]);

  const missingNativeWake = pendingReceipt(true, false);
  let missingWakeClock = 0;
  await assert.rejects(
    async () =>
      proveForegroundScopeIsolation(
        missingNativeWake.factory,
        scopeA,
        scopeCodec,
        {
          contains: ["a"],
          excludes: ["b"],
        },
        undefined,
        {
          timeoutMs: 96,
          now: () => missingWakeClock,
          yieldTurn: async () => {
            await new Promise<void>((resolve) => setTimeout(resolve, 0));
            missingWakeClock += 1;
          },
        },
      ),
    /did not settle before its bounded deadline/,
  );
  assert.deepEqual(missingNativeWake.metrics, { all: 1, poll: 0, tick: 96, close: 1 });

  const expiredDuringYield = pendingReceipt(false);
  let yieldClock = 0;
  await assert.rejects(
    async () =>
      proveForegroundScopeIsolation(
        expiredDuringYield.factory,
        scopeA,
        scopeCodec,
        { contains: ["a"], excludes: ["b"] },
        undefined,
        {
          timeoutMs: 1,
          now: () => yieldClock,
          yieldTurn: async () => {
            yieldClock = 1;
          },
        },
      ),
    /did not settle before its bounded deadline/,
  );
  assert.deepEqual(expiredDuringYield.metrics, { all: 0, poll: 0, tick: 1, close: 1 });

  const expiredWhileExecuting = pendingReceipt(false, true, true);
  let clockReads = 0;
  await assert.rejects(
    async () =>
      proveForegroundScopeIsolation(
        expiredWhileExecuting.factory,
        scopeA,
        scopeCodec,
        { contains: ["a"], excludes: ["b"] },
        undefined,
        {
          timeoutMs: 1,
          now: () => (clockReads++ < 4 ? 0 : 1),
          yieldTurn: async () => {},
        },
      ),
    /did not settle before its bounded deadline/,
  );
  assert.deepEqual(expiredWhileExecuting.metrics, { all: 1, poll: 0, tick: 1, close: 1 });

  aWasWritten = false;
  bWasWritten = false;
  const leaking = scopeFactory(true, false);
  await assert.rejects(
    async () =>
      proveForegroundScopeIsolation(leaking, scopeB, scopeCodec, {
        contains: [],
        excludes: ["a"],
      }),
    /observed scope A's persisted fixture row/,
  );

  aWasWritten = false;
  bWasWritten = false;
  const reverseLeaking = scopeFactory(false, true);
  await assert.rejects(
    async () =>
      proveForegroundScopeIsolation(reverseLeaking, scopeA, scopeCodec, {
        contains: [],
        excludes: ["b"],
      }),
    /observed scope B's persisted fixture row/,
  );
});

test("two aliases in one installed JSI runtime require B to observe A's committed subscription delta", async () => {
  let insertedRowId: Uint8Array | undefined;
  let insertedCells: Uint8Array | undefined;
  let subscribedQuery: Uint8Array | undefined;
  const command = {
    encode(value: unknown) {
      if (
        typeof value === "object" &&
        value !== null &&
        "type" in value &&
        value.type === "insert" &&
        "rowId" in value &&
        value.rowId instanceof Uint8Array
      ) {
        insertedRowId = value.rowId;
        if ("cells" in value && value.cells instanceof Uint8Array) insertedCells = value.cells;
      }
      if (
        typeof value === "object" &&
        value !== null &&
        "type" in value &&
        value.type === "subscribe" &&
        "query" in value &&
        value.query instanceof Uint8Array
      ) {
        subscribedQuery = value.query;
      }
      return new TextEncoder().encode(JSON.stringify(value));
    },
    decode(bytes: Uint8Array) {
      const decoded = JSON.parse(new TextDecoder().decode(bytes)) as Record<string, unknown>;
      if (decoded.type === "subscriptionEvents" && Array.isArray(decoded.events)) {
        for (const event of decoded.events) {
          if (event?.type === "delta" && Array.isArray(event.delta))
            event.delta = Uint8Array.from(event.delta);
        }
      }
      if (decoded.type === "inserted" && Array.isArray(decoded.rowId)) {
        decoded.rowId = Uint8Array.from(decoded.rowId);
      }
      return decoded;
    },
  } as unknown as ForegroundByteCodec;
  let committed = false;
  let opened = 0;
  let emitCommitWake = true;
  let postCommitWakeAfterBTicks = 0;
  let pendingPostCommitWake = false;
  const schedulers: Array<((urgency: string) => void) | undefined> = [];
  const wakeTraceToggles: boolean[][] = [[], []];
  const ticks = [0, 0];
  const factory = {
    abiVersion: NATIVE_RELAY_ABI_V1,
    openAttached(received: Uint8Array) {
      assert.deepEqual(received, capability);
      const peer = opened++;
      let initialResetDrained = false;
      return {
        execute(bytes: Uint8Array) {
          const request = command.decode(bytes) as { type?: string };
          const response =
            request.type === "subscribe"
              ? { type: "subscribed", subscription: 2 }
              : request.type === "beginTransaction"
                ? { type: "transactionOpened", transaction: 3 }
                : request.type === "insert"
                  ? { type: "inserted", rowId: Array.from(subscriptionRowId) }
                  : request.type === "commitTransaction"
                    ? (setTimeout(() => {
                        committed = true;
                        const bTicksBeforeWake = ticks[1];
                        if (emitCommitWake) {
                          if (postCommitWakeAfterBTicks === 0) schedulers[1]?.("immediate");
                          else pendingPostCommitWake = true;
                        }
                        assert.equal(
                          ticks[1],
                          bTicksBeforeWake,
                          "subscription wake must not re-enter foreground.tick",
                        );
                      }, 0),
                      {
                        type: "transactionCommitted",
                        txId: new Uint8Array(16).fill(1),
                      })
                    : request.type === "drainSubscription"
                      ? {
                          type: "subscriptionEvents",
                          events:
                            peer === 1 && !initialResetDrained
                              ? ((initialResetDrained = true),
                                [
                                  {
                                    type: "delta",
                                    reset: true,
                                    settled: true,
                                    tier: "local",
                                    delta: [],
                                  },
                                ])
                              : peer === 1 && committed
                                ? [
                                    {
                                      type: "delta",
                                      reset: false,
                                      settled: true,
                                      tier: "local",
                                      delta: Array.from(
                                        encodeSubscriptionDelta({
                                          added: [
                                            {
                                              rowId: subscriptionRowId,
                                              raw: Uint8Array.from([
                                                2,
                                                ...new TextEncoder().encode(
                                                  "subscription from foreground A",
                                                ),
                                              ]),
                                            },
                                          ],
                                        }),
                                      ),
                                    },
                                  ]
                                : [],
                        }
                      : request.type === "unsubscribe"
                        ? { type: "unsubscribed", closed: true }
                        : { type: "closed", closed: true };
          return command.encode(response);
        },
        tick() {
          ticks[peer] += 1;
          if (peer === 1 && pendingPostCommitWake && ticks[peer] >= postCommitWakeAfterBTicks) {
            pendingPostCommitWake = false;
            schedulers[peer]?.("immediate");
          }
        },
        setTickScheduler(callback: (urgency: string) => void) {
          schedulers[peer] = callback;
        },
        setWakeTrace(enabled: boolean) {
          wakeTraceToggles[peer]!.push(enabled);
        },
        close: () => true,
      };
    },
  };
  const subscriptionStages: string[] = [];
  await proveSameJsiRuntimeWriteSubscription(
    factory,
    capability,
    command,
    subscriptionRowId,
    (stage) => subscriptionStages.push(stage),
  );
  assert.deepEqual(
    insertedRowId,
    subscriptionRowId,
    "the receipt inserts the host-run row instead of upserting a retained fixed id",
  );
  assert.deepEqual(subscribedQuery, todosQuery, "the receipt uses the Rust-generated todos query");
  assert.equal(
    insertedCells?.[9],
    insertedCells ? insertedCells.byteLength - 10 : undefined,
    "the fixed Rust record envelope includes its primitive-string variant byte",
  );
  assert.deepEqual(subscriptionStages, [
    "same-runtime-open-failed",
    "same-runtime-subscribe-failed",
    "same-runtime-initial-reset-failed",
    "same-runtime-write-failed",
    "same-runtime-transaction-open-failed",
    "same-runtime-mutation-stage-failed",
    "same-runtime-commit-failed",
    "same-runtime-delta-failed",
    "same-runtime-postcommit-wake-failed",
    "same-runtime-delta-drain-failed",
    "same-runtime-delta-decode-failed",
    "same-runtime-delta-content-failed",
    "same-runtime-delta-row-id-failed",
    "same-runtime-unsubscribe-failed",
  ]);
  assert.deepEqual(
    wakeTraceToggles,
    [[], [true, false]],
    "only B enables the default-off bridge trace and disables it during cleanup",
  );

  committed = false;
  opened = 0;
  schedulers.length = 0;
  ticks.fill(0);
  const unavailableTraceToggles: boolean[] = [];
  const unavailableTraceCloses: number[] = [];
  let unavailableTraceOpened = 0;
  const unavailableTrace = {
    ...factory,
    openAttached(received: Uint8Array) {
      const foreground = factory.openAttached(received);
      const peer = unavailableTraceOpened++;
      return {
        ...foreground,
        setWakeTrace(enabled: boolean) {
          unavailableTraceToggles.push(enabled);
          throw new Error("private wake trace unavailable");
        },
        close() {
          unavailableTraceCloses.push(peer);
          return foreground.close();
        },
      };
    },
  };
  await proveSameJsiRuntimeWriteSubscription(
    unavailableTrace,
    capability,
    command,
    subscriptionRowId,
  );
  assert.deepEqual(
    unavailableTraceToggles,
    [true, false],
    "a failing B-only trace toggle cannot replace the successful subscription receipt",
  );
  assert.deepEqual(
    unavailableTraceCloses,
    [0, 1],
    "a failing trace disable still closes both foreground aliases",
  );

  // Android's CallInvoker may need more than the former 96 zero-delay turns
  // after a release build. The receipt still waits for B's actual scheduler
  // callback before draining; a late callback may not be replaced with a read.
  committed = false;
  opened = 0;
  schedulers.length = 0;
  ticks.fill(0);
  postCommitWakeAfterBTicks = 192;
  pendingPostCommitWake = false;
  const lateWake: Array<{ elapsedMs: number; turns: number }> = [];
  await proveSameJsiRuntimeWriteSubscription(
    factory,
    capability,
    command,
    subscriptionRowId,
    undefined,
    {
      timeoutMs: 1_000,
      now: () => performance.now(),
      yieldTurn: () => new Promise<void>((resolve) => setTimeout(resolve, 0)),
      onWake: (details) => lateWake.push(details),
    },
  );
  assert.equal(lateWake.length, 1, "the delayed scheduler callback is still required");
  assert.ok(lateWake[0]!.turns > 96, "the receipt tolerates an Android-late native callback");
  postCommitWakeAfterBTicks = 0;

  committed = false;
  opened = 0;
  schedulers.length = 0;
  ticks.fill(0);
  let splitOpened = 0;
  let unsettledResetDrains = 0;
  const unsettledInitialReset = {
    ...factory,
    openAttached(received: Uint8Array) {
      const foreground = factory.openAttached(received);
      const peer = splitOpened++;
      return {
        ...foreground,
        execute(bytes: Uint8Array) {
          const request = command.decode(bytes) as { type?: string };
          if (peer === 1 && request.type === "drainSubscription") {
            unsettledResetDrains += 1;
            if (unsettledResetDrains === 1) {
              // Consume the underlying reset but preserve only its reset
              // semantics; durability settlement is orthogonal to proving
              // that the later content delta was not the opening snapshot.
              foreground.execute(bytes);
              return command.encode({
                type: "subscriptionEvents",
                events: [{ type: "delta", reset: true, settled: false, tier: "local", delta: [] }],
              });
            }
          }
          return foreground.execute(bytes);
        },
      };
    },
  };
  await proveSameJsiRuntimeWriteSubscription(
    unsettledInitialReset,
    capability,
    command,
    subscriptionRowId,
  );
  assert.equal(unsettledResetDrains, 2, "unsettled reset and committed delta drain separately");

  committed = false;
  opened = 0;
  schedulers.length = 0;
  ticks.fill(0);
  let prematureOpened = 0;
  const settlementBeforeReset = {
    ...factory,
    openAttached(received: Uint8Array) {
      const foreground = factory.openAttached(received);
      const peer = prematureOpened++;
      let drained = false;
      return {
        ...foreground,
        execute(bytes: Uint8Array) {
          const request = command.decode(bytes) as { type?: string };
          if (peer === 1 && request.type === "drainSubscription") {
            if (!drained) {
              drained = true;
              foreground.execute(bytes);
              return command.encode({
                type: "subscriptionEvents",
                events: [{ type: "delta", reset: false, settled: true, tier: "local", delta: [] }],
              });
            }
            return command.encode({ type: "subscriptionEvents", events: [] });
          }
          return foreground.execute(bytes);
        },
      };
    },
  };
  await assert.rejects(
    async () =>
      proveSameJsiRuntimeWriteSubscription(
        settlementBeforeReset,
        capability,
        command,
        subscriptionRowId,
      ),
    /initial subscription reset did not materialize/,
  );

  committed = false;
  opened = 0;
  schedulers.length = 0;
  ticks.fill(0);
  let delayedOpened = 0;
  let delayedEmptyDrains = 0;
  let delayedTicks = 0;
  let delayedTurns = 0;
  let allowDelayedProgress = true;
  const delayedInitialReset = {
    ...factory,
    openAttached(received: Uint8Array) {
      const foreground = factory.openAttached(received);
      const peer = delayedOpened++;
      let resetReady = false;
      let progressionCycles = 0;
      return {
        ...foreground,
        execute(bytes: Uint8Array) {
          const request = command.decode(bytes) as { type?: string };
          if (peer === 1 && request.type === "drainSubscription" && !resetReady) {
            delayedEmptyDrains += 1;
            return command.encode({ type: "subscriptionEvents", events: [] });
          }
          return foreground.execute(bytes);
        },
        tick() {
          foreground.tick();
          if (peer !== 1) return;
          delayedTicks += 1;
          if (allowDelayedProgress)
            setTimeout(() => {
              progressionCycles += 1;
              resetReady = progressionCycles >= 2;
              delayedTurns += 1;
            }, 0);
        },
      };
    },
  };
  await proveSameJsiRuntimeWriteSubscription(
    delayedInitialReset,
    capability,
    command,
    subscriptionRowId,
  );
  assert.equal(delayedEmptyDrains, 2, "two successive reset drains are ready but empty");
  assert.ok(delayedTicks >= 2, "each empty drain receives an ordinary B tick");
  assert.ok(delayedTurns >= 2, "two yielded turns are required before the reset is ready");

  committed = false;
  opened = 0;
  schedulers.length = 0;
  ticks.fill(0);
  delayedOpened = 0;
  delayedEmptyDrains = 0;
  delayedTicks = 0;
  delayedTurns = 0;
  allowDelayedProgress = false;
  await assert.rejects(
    async () =>
      proveSameJsiRuntimeWriteSubscription(
        delayedInitialReset,
        capability,
        command,
        subscriptionRowId,
      ),
    /initial subscription reset did not materialize/,
  );
  assert.equal(delayedEmptyDrains, 96);
  assert.equal(delayedTurns, 0);
  allowDelayedProgress = true;

  committed = false;
  opened = 0;
  schedulers.length = 0;
  ticks.fill(0);
  let pendingPolls = 0;
  let pendingDrains = 0;
  let emitPendingWake = true;
  const pendingHydration = {
    ...factory,
    openAttached(received: Uint8Array) {
      const foreground = factory.openAttached(received);
      let scheduler: ((urgency: string) => void) | undefined;
      let pendingResponse: Uint8Array | undefined;
      let pollsForDrain = 0;
      return {
        ...foreground,
        execute(bytes: Uint8Array) {
          const request = command.decode(bytes) as { type?: string; operation?: number };
          if (request.type === "drainSubscription") {
            assert.equal(pendingResponse, undefined);
            pendingDrains += 1;
            pendingResponse = foreground.execute(bytes);
            pollsForDrain = 0;
            if (emitPendingWake) setTimeout(() => scheduler?.("immediate"), 0);
            return command.encode({ type: "pending", operation: 57 });
          }
          if (request.type === "poll") {
            assert.equal(request.operation, 57);
            assert.ok(pendingResponse);
            pendingPolls += 1;
            pollsForDrain += 1;
            if (pollsForDrain === 1) {
              if (emitPendingWake) setTimeout(() => scheduler?.("immediate"), 0);
              return command.encode({ type: "pending", operation: 57 });
            }
            const response = pendingResponse;
            pendingResponse = undefined;
            return response;
          }
          return foreground.execute(bytes);
        },
        setTickScheduler(callback: (urgency: string) => void) {
          scheduler = callback;
          foreground.setTickScheduler?.(callback);
        },
      };
    },
  };
  await proveSameJsiRuntimeWriteSubscription(
    pendingHydration,
    capability,
    command,
    subscriptionRowId,
  );
  assert.equal(pendingDrains, 2, "a retained operation must not reissue DrainSubscription");
  assert.equal(pendingPolls, 4, "both drains preserve one operation across repeated Polls");

  committed = false;
  opened = 0;
  schedulers.length = 0;
  ticks.fill(0);
  pendingDrains = 0;
  pendingPolls = 0;
  emitPendingWake = false;
  await assert.rejects(
    async () =>
      proveSameJsiRuntimeWriteSubscription(
        pendingHydration,
        capability,
        command,
        subscriptionRowId,
      ),
    /subscription drain did not settle after bounded ticks/,
  );
  assert.equal(pendingDrains, 1);
  assert.equal(pendingPolls, 0, "a retained operation cannot Poll without a fresh native wake");
  emitPendingWake = true;

  committed = false;
  opened = 0;
  schedulers.length = 0;
  ticks.fill(0);
  const terminalCloses: number[] = [];
  let terminalOpened = 0;
  const terminalOperation = {
    ...factory,
    openAttached(received: Uint8Array) {
      const foreground = factory.openAttached(received);
      const peer = terminalOpened++;
      return {
        ...foreground,
        execute(bytes: Uint8Array) {
          const request = command.decode(bytes) as { type?: string };
          if (request.type === "drainSubscription")
            return command.encode({ type: "operationError", reason: "cancelled" });
          return foreground.execute(bytes);
        },
        close() {
          terminalCloses.push(peer);
          return foreground.close();
        },
      };
    },
  };
  await assert.rejects(
    async () =>
      proveSameJsiRuntimeWriteSubscription(
        terminalOperation,
        capability,
        command,
        subscriptionRowId,
      ),
    /unexpected response/,
  );
  assert.deepEqual(terminalCloses, [0, 1], "both foregrounds close after terminal operation error");

  committed = false;
  opened = 0;
  schedulers.length = 0;
  ticks.fill(0);
  wakeTraceToggles[0]!.length = 0;
  wakeTraceToggles[1]!.length = 0;
  emitCommitWake = false;
  await assert.rejects(
    async () =>
      proveSameJsiRuntimeWriteSubscription(
        factory,
        capability,
        command,
        subscriptionRowId,
        undefined,
        shortPostCommitWakeTiming(),
      ),
    /did not observe foreground A's committed row after \d+ turns and \d+ms without a post-commit native wake/,
  );

  // The diagnostic acknowledgement precedes foreground creation and trace
  // enable. It cannot reach B's scheduler before A commits when the real
  // commit wake is suppressed.
  committed = false;
  opened = 0;
  schedulers.length = 0;
  ticks.fill(0);
  wakeTraceToggles[0]!.length = 0;
  wakeTraceToggles[1]!.length = 0;
  emitCommitWake = false;
  await assert.rejects(
    async () =>
      proveSameJsiRuntimeWriteSubscription(
        factory,
        capability,
        command,
        subscriptionRowId,
        undefined,
        {
          ...shortPostCommitWakeTiming(),
          onPostCommitWakeArmed: () =>
            new Promise<void>((resolve) => {
              setTimeout(() => {
                assert.deepEqual(
                  wakeTraceToggles,
                  [[], []],
                  "the acknowledged diagnostic boundary precedes B trace enable",
                );
                assert.equal(
                  opened,
                  0,
                  "the acknowledged diagnostic boundary precedes foreground creation",
                );
                assert.equal(
                  schedulers[1],
                  undefined,
                  "the acknowledged diagnostic boundary precedes foreground creation",
                );
                resolve();
              }, 0);
            }),
        },
      ),
    /without a post-commit native wake/,
  );

  committed = false;
  opened = 0;
  schedulers.length = 0;
  ticks.fill(0);
  emitCommitWake = true;
  const missingNativeWake = {
    ...factory,
    openAttached(received: Uint8Array) {
      const foreground = factory.openAttached(received);
      return { ...foreground, setTickScheduler() {} };
    },
  };
  await assert.rejects(
    async () =>
      proveSameJsiRuntimeWriteSubscription(
        missingNativeWake,
        capability,
        command,
        subscriptionRowId,
        undefined,
        shortPostCommitWakeTiming(),
      ),
    /did not observe foreground A's committed row/,
  );

  committed = false;
  opened = 0;
  schedulers.length = 0;
  ticks.fill(0);
  const noObservation = {
    ...factory,
    openAttached(received: Uint8Array) {
      const foreground = factory.openAttached(received);
      let initialResetDrained = false;
      return {
        ...foreground,
        execute(bytes: Uint8Array) {
          const request = command.decode(bytes) as { type?: string };
          if (request.type === "drainSubscription" && initialResetDrained)
            return command.encode({
              type: "subscriptionEvents",
              events: [
                {
                  type: "delta",
                  reset: false,
                  settled: true,
                  tier: "local",
                  delta: Array.from(encodeSubscriptionDelta({})),
                },
              ],
            });
          if (request.type === "drainSubscription") initialResetDrained = true;
          return foreground.execute(bytes);
        },
      };
    },
  };
  const noObservationStages: string[] = [];
  await assert.rejects(
    async () =>
      proveSameJsiRuntimeWriteSubscription(
        noObservation,
        capability,
        command,
        subscriptionRowId,
        (stage) => noObservationStages.push(stage),
        shortPostCommitWakeTiming(),
      ),
    /did not observe foreground A's committed row/,
  );
  assert.ok(noObservationStages.includes("same-runtime-delta-drain-failed"));
  assert.ok(
    noObservationStages.includes("same-runtime-delta-content-failed") &&
      !noObservationStages.includes("same-runtime-delta-row-id-failed"),
    "an empty settlement decodes but stops before row-identity classification",
  );

  committed = false;
  opened = 0;
  schedulers.length = 0;
  ticks.fill(0);
  let wrongDeltaKind: "incremental" | "reset" | "mixed" = "incremental";
  let wrongMatchesWrittenContent = false;
  const wrongObservation = {
    ...factory,
    openAttached(received: Uint8Array) {
      const foreground = factory.openAttached(received);
      let initialResetDrained = false;
      return {
        ...foreground,
        execute(bytes: Uint8Array) {
          const request = command.decode(bytes) as { type?: string };
          if (request.type === "drainSubscription" && initialResetDrained)
            return command.encode({
              type: "subscriptionEvents",
              events: (wrongDeltaKind === "mixed"
                ? [true, false]
                : [wrongDeltaKind === "reset"]
              ).map((reset) => ({
                type: "delta",
                reset,
                settled: true,
                tier: "local",
                // The title alone is not authoritative: binding envelopes
                // may carry unrelated text, while the inserted run-bound
                // RowUuid is the identity this receipt must observe.
                delta: Array.from(
                  encodeSubscriptionDelta({
                    added: [
                      {
                        rowId: new Uint8Array(16).fill(0xee),
                        raw: Uint8Array.from([
                          2,
                          ...new TextEncoder().encode(
                            wrongMatchesWrittenContent
                              ? "subscription from foreground A"
                              : "unrelated pre-existing row",
                          ),
                        ]),
                      },
                    ],
                  }),
                ),
              })),
            });
          if (request.type === "drainSubscription") initialResetDrained = true;
          return foreground.execute(bytes);
        },
      };
    },
  };
  const wrongObservationStages: string[] = [];
  await assert.rejects(
    async () =>
      proveSameJsiRuntimeWriteSubscription(
        wrongObservation,
        capability,
        command,
        subscriptionRowId,
        (stage) => wrongObservationStages.push(stage),
        shortPostCommitWakeTiming(),
      ),
    /did not observe foreground A's committed row/,
  );
  assert.ok(wrongObservationStages.includes("same-runtime-delta-drain-failed"));
  assert.ok(
    wrongObservationStages.includes("same-runtime-delta-content-failed") &&
      wrongObservationStages.includes("same-runtime-delta-row-id-failed") &&
      wrongObservationStages.includes("same-runtime-delta-incremental-row-id-failed") &&
      !wrongObservationStages.includes("same-runtime-delta-reset-row-id-failed"),
    "a title-only payload reaches content diagnostics without satisfying row-id observation",
  );

  committed = false;
  opened = 0;
  schedulers.length = 0;
  ticks.fill(0);
  wrongDeltaKind = "reset";
  const wrongResetStages: string[] = [];
  await assert.rejects(
    async () =>
      proveSameJsiRuntimeWriteSubscription(
        wrongObservation,
        capability,
        command,
        subscriptionRowId,
        (stage) => wrongResetStages.push(stage),
        shortPostCommitWakeTiming(),
      ),
    /did not observe foreground A's committed row/,
  );
  assert.ok(
    wrongResetStages.includes("same-runtime-delta-reset-row-id-failed") &&
      !wrongResetStages.includes("same-runtime-delta-incremental-row-id-failed"),
    "a reset carrying only other rows remains distinct from an incremental wrong-row delta",
  );

  committed = false;
  opened = 0;
  schedulers.length = 0;
  ticks.fill(0);
  wrongDeltaKind = "mixed";
  const wrongMixedStages: string[] = [];
  await assert.rejects(
    async () =>
      proveSameJsiRuntimeWriteSubscription(
        wrongObservation,
        capability,
        command,
        subscriptionRowId,
        (stage) => wrongMixedStages.push(stage),
        shortPostCommitWakeTiming(),
      ),
    /did not observe foreground A's committed row/,
  );
  assert.ok(
    wrongMixedStages.includes("same-runtime-delta-mixed-row-id-failed") &&
      !wrongMixedStages.includes("same-runtime-delta-reset-row-id-failed") &&
      !wrongMixedStages.includes("same-runtime-delta-incremental-row-id-failed"),
    "a mixed reset/incremental drain cannot be mislabeled as either single event kind",
  );

  committed = false;
  opened = 0;
  schedulers.length = 0;
  ticks.fill(0);
  wrongDeltaKind = "incremental";
  wrongMatchesWrittenContent = true;
  const wrongWrittenContentStages: string[] = [];
  await assert.rejects(
    async () =>
      proveSameJsiRuntimeWriteSubscription(
        wrongObservation,
        capability,
        command,
        subscriptionRowId,
        (stage) => wrongWrittenContentStages.push(stage),
        shortPostCommitWakeTiming(),
      ),
    /did not observe foreground A's committed row/,
  );
  assert.ok(
    wrongWrittenContentStages.includes("same-runtime-delta-written-content-row-id-failed") &&
      !wrongWrittenContentStages.includes("same-runtime-delta-incremental-row-id-failed"),
    "the written fixture content with another row id is an identity mismatch, not unrelated data",
  );
});

function shortPostCommitWakeTiming() {
  return {
    timeoutMs: 96,
    now: () => performance.now(),
    yieldTurn: () => new Promise<void>((resolve) => setTimeout(resolve, 0)),
  };
}

function utf8(value: string): number[] {
  return Array.from(new TextEncoder().encode(value));
}

function encodeSubscriptionDelta({
  added = [],
  updated = [],
}: {
  added?: Array<{ rowId: Uint8Array; raw: Uint8Array }>;
  updated?: Array<{ rowId: Uint8Array; raw: Uint8Array }>;
}): Uint8Array {
  const writer = new PostcardWriter();
  const descriptor = [{ name: "title", valueType: { tag: 8 } }];
  const writeBatches = (
    target: PostcardWriter,
    rows: Array<{ rowId: Uint8Array; raw: Uint8Array }>,
  ) => {
    target.vec(
      (batch) => {
        batch.string("todos");
        writeDescriptor(batch, descriptor);
        batch.vec((row, index) => {
          row.bytes(rows[index]!.rowId);
          row.bool(false);
          row.bytes(createRecord(descriptor, [rows[index]!.raw]));
        }, rows.length);
      },
      rows.length === 0 ? 0 : 1,
    );
  };
  writeBatches(writer, added);
  writeBatches(writer, updated);
  writer.vec(() => undefined, 0);
  for (const rows of [added, updated]) {
    writer.vec((key, index) => {
      key.bytes(Uint8Array.from([1, ...rows[index]!.rowId, 0, 0, 0, 0, 0, 0, 0, 0]));
    }, rows.length);
  }
  writer.vec(() => undefined, 0);
  writer.vec((indexWriter, index) => indexWriter.u64(index), added.length);
  writer.vec((indexWriter, index) => indexWriter.u64(index), updated.length);
  writer.vec((indexWriter, index) => indexWriter.u64(index), updated.length);
  writer.vec(() => undefined, 0);
  return writer.finish();
}

test("queries and owner cells match the Rust-generated fixture", async () => {
  const { readFileSync } = await import("node:fs");
  const fixture = JSON.parse(
    readFileSync(new URL("../native/device-fixture.json", import.meta.url), "utf8"),
  );
  const {
    scopeQuery,
    scopeCells,
    todosQuery: generatedTodosQuery,
  } = await import("./scope-fixture.ts");
  assert.deepEqual([...scopeQuery], fixture.scopeQuery);
  assert.deepEqual([...generatedTodosQuery], fixture.todosQuery);
  assert.deepEqual(scopeCells, fixture.scopeCells);
});
