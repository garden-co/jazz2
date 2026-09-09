import { scopeQuery, scopeCells, todosQuery } from "./scope-fixture.ts";
import type {
  NativeForegroundCommand,
  NativeForegroundResponse,
  NativeForegroundRuntime,
  NativeForegroundRuntimeFactory,
} from "jazz-rn";
import { NATIVE_RELAY_ABI_V1 } from "jazz-rn/native-relay-abi";
import type { DeviceDiagnosticCode } from "./device-diagnostics";
import {
  nativeSubscriptionDeltaHasFieldBytes,
  nativeSubscriptionDeltaHasRowId,
  nativeSubscriptionDeltaRowIds,
} from "./native-subscription-observation.ts";

export type ForegroundByteCodec = {
  encode(command: NativeForegroundCommand): Uint8Array;
  decode(bytes: Uint8Array): NativeForegroundResponse;
};

type PostCommitWakeTiming = {
  readonly timeoutMs: number;
  now(): number;
  yieldTurn(): Promise<void>;
  onWake?(details: { elapsedMs: number; turns: number }): void;
  onPostCommitWakeArmed?(): void | Promise<void>;
};

const DEVICE_POST_COMMIT_WAKE_TIMING: PostCommitWakeTiming = {
  timeoutMs: 5_000,
  now: () => performance.now(),
  yieldTurn: () => new Promise<void>((resolve) => setTimeout(resolve, 0)),
};

/**
 * Exercise the installed JSI HostObject through the first v1 byte vocabulary.
 * This is intentionally not a React-Native-shaped database API: it proves the
 * compiled C++ bridge copies postcard bytes to the actual foreground owner.
 */
export function proveForegroundByteAbi(
  factory: NativeForegroundRuntimeFactory,
  capability: Uint8Array,
  codec: ForegroundByteCodec,
  markFailure?: (
    stage:
      | "foreground-abi-version-failed"
      | "foreground-open-failed"
      | "foreground-probe-failed"
      | "foreground-tick-failed"
      | "foreground-close-failed",
  ) => void,
): NativeForegroundRuntime {
  markFailure?.("foreground-abi-version-failed");
  if (factory.abiVersion !== NATIVE_RELAY_ABI_V1)
    throw new Error(`installed foreground factory has unexpected ABI ${factory.abiVersion}`);
  markFailure?.("foreground-open-failed");
  const foreground = factory.openAttached(capability);
  markFailure?.("foreground-probe-failed");
  const probe = codec.decode(foreground.execute(codec.encode("probe")));
  if (probe.type !== "probe" || probe.abiVersion !== NATIVE_RELAY_ABI_V1)
    throw new Error("installed foreground returned an unexpected Probe response");
  markFailure?.("foreground-tick-failed");
  const tick = codec.decode(foreground.execute(codec.encode("tick")));
  if (tick.type !== "ticked") throw new Error("installed foreground did not acknowledge Tick");
  markFailure?.("foreground-close-failed");
  const close = codec.decode(foreground.execute(codec.encode("close")));
  if (close.type !== "closed" || !close.closed)
    throw new Error("installed foreground did not acknowledge its first Close");
  assertRejected(
    () => foreground.execute(codec.encode("probe")),
    "foreground accepted Probe after Close",
  );
  return foreground;
}

/** Deterministically exercise the JSI allocation/finalizer reentry boundary. */
export function proveForegroundJsReentry(
  factory: NativeForegroundRuntimeFactory,
  capability: Uint8Array,
  codec: ForegroundByteCodec,
): void {
  const foreground = factory.openAttached(capability);
  try {
    // Native response construction can run JS (and Hermes GC). Re-enter the
    // shared lease from Uint8Array construction to prove no lifecycle mutex is
    // held across that boundary, without depending on GC timing.
    const sibling = factory.openAttached(capability);
    const command = codec.encode("probe");
    const OriginalUint8Array = globalThis.Uint8Array;
    let reentered = false;
    let response: Uint8Array;
    try {
      globalThis.Uint8Array = new Proxy(OriginalUint8Array, {
        construct(target, args) {
          if (!reentered) {
            reentered = true;
            if (!sibling.close()) throw new Error("reentrant foreground close failed");
          }
          return Reflect.construct(target, args);
        },
      });
      // The bridge validates the command's constructor against the global one.
      Object.defineProperty(command, "constructor", { value: globalThis.Uint8Array });
      response = foreground.execute(command);
    } finally {
      globalThis.Uint8Array = OriginalUint8Array;
      sibling.close();
    }
    if (!reentered) throw new Error("foreground response did not exercise JS reentry");
    const probe = codec.decode(response);
    if (probe.type !== "probe" || probe.abiVersion !== NATIVE_RELAY_ABI_V1)
      throw new Error("installed foreground returned an unexpected Probe response");
  } finally {
    foreground.close();
  }
}

/** A foreground alias left open before native revoke must no longer execute. */
export function proveForegroundRevoked(
  foreground: NativeForegroundRuntime,
  encode: ForegroundByteCodec["encode"],
): void {
  assertRejected(() => foreground.execute(encode("probe")), "revoked foreground accepted Probe");
}

/**
 * Drive the mutable half of the installed foreground ABI through the JSI
 * HostObject.  The cell payloads are fixed canonical Rust fixture bytes, not
 * a JavaScript row codec: the device host's schema is the matching `todos`
 * text schema and Rust remains the only decoder of the record envelope.
 */
export function proveForegroundWriteAbi(
  factory: NativeForegroundRuntimeFactory,
  capability: Uint8Array,
  codec: ForegroundByteCodec,
): void {
  const foreground = factory.openAttached(capability);
  const execute = (command: NativeForegroundCommand): NativeForegroundResponse =>
    codec.decode(foreground.execute(codec.encode(command)));
  const requireTransaction = (kind: "mergeable" | "exclusive") => {
    const response = execute({ type: "beginTransaction", kind });
    if (response.type !== "transactionOpened")
      throw new Error(`${kind} foreground transaction did not open`);
    return response.transaction;
  };
  const rowId = Uint8Array.from({ length: 16 }, () => 0x71);
  const mergeable = requireTransaction("mergeable");
  const inserted = execute({
    type: "insert",
    transaction: mergeable,
    table: "todos",
    rowId,
    cells: fixtureCells("mergeable"),
  });
  if (inserted.type !== "inserted" || !sameBytes(inserted.rowId, rowId))
    throw new Error("foreground Insert did not return its supplied row id");
  for (const command of [
    { type: "update" as const, patch: fixtureCells("updated") },
    { type: "upsert" as const, cells: fixtureCells("upserted") },
    { type: "delete" as const },
  ]) {
    const response = execute({
      ...command,
      transaction: mergeable,
      table: "todos",
      rowId,
    });
    if (response.type !== "mutationStaged")
      throw new Error(`foreground ${command.type} was not staged`);
  }
  const committed = execute({
    type: "commitTransaction",
    transaction: mergeable,
  });
  if (
    committed.type !== "transactionCommitted" ||
    committed.txId.byteLength !== 16 ||
    committed.txId.every((byte) => byte === 0)
  )
    throw new Error("foreground Commit did not return a non-zero public txId");
  const retired = execute({
    type: "rollbackTransaction",
    transaction: mergeable,
  });
  if (retired.type !== "operationError")
    throw new Error("foreground accepted a terminal transaction handle");

  const exclusive = requireTransaction("exclusive");
  const rollbackRowId = Uint8Array.from({ length: 16 }, () => 0x72);
  const rollbackInsert = execute({
    type: "insert",
    transaction: exclusive,
    table: "todos",
    rowId: rollbackRowId,
    cells: fixtureCells("rolled back"),
  });
  if (rollbackInsert.type !== "inserted" || !sameBytes(rollbackInsert.rowId, rollbackRowId))
    throw new Error("exclusive foreground Insert did not return its supplied row id");
  const rolledBack = execute({
    type: "rollbackTransaction",
    transaction: exclusive,
  });
  if (rolledBack.type !== "transactionRolledBack" || !rolledBack.rolledBack)
    throw new Error("exclusive foreground transaction did not roll back");

  // Handles are local to their foreground alias. A sibling cannot commit an
  // otherwise well-formed handle from this terminal transaction.
  const sibling = factory.openAttached(capability);
  const siblingResponse = codec.decode(
    sibling.execute(
      codec.encode({
        type: "commitTransaction",
        transaction: exclusive,
      }),
    ),
  );
  if (siblingResponse.type !== "operationError")
    throw new Error("foreground accepted a transaction handle from another alias");
  sibling.close();
  foreground.close();
}

/**
 * Actual two-foreground receipt for one installed JSI runtime. A and B are
 * separate foreground aliases in that same JSI runtime, both attached to the
 * same native relay admitted by the capability. B starts a local subscription before A writes;
 * bounded ordinary ticks must then deliver A's committed row as B's binding
 * delta.  This deliberately stays at the byte ABI boundary: JS only checks a
 * fixed fixture title in Rust-produced binding bytes.
 */
export async function proveSameJsiRuntimeWriteSubscription(
  factory: NativeForegroundRuntimeFactory,
  capability: Uint8Array,
  codec: ForegroundByteCodec,
  rowId: Uint8Array,
  markFailure: (code: DeviceDiagnosticCode) => void = () => {},
  wakeTiming: PostCommitWakeTiming = DEVICE_POST_COMMIT_WAKE_TIMING,
): Promise<void> {
  if (rowId.byteLength !== 16) throw new Error("subscription fixture row id must be 16 bytes");
  // Acknowledging the opt-in diagnostic bridge may resume through the JS event
  // loop. Do it before creating either foreground, while tracing is disabled,
  // so no B subscription callback can enter the post-commit epoch through the
  // acknowledgement.
  await wakeTiming.onPostCommitWakeArmed?.();
  markFailure("same-runtime-open-failed");
  const openedA = openScopeForeground(factory, capability);
  const openedB = openScopeForeground(factory, capability);
  const a = openedA.runtime;
  const b = openedB.runtime;
  const execute = (foreground: NativeForegroundRuntime, command: NativeForegroundCommand) =>
    codec.decode(foreground.execute(codec.encode(command)));
  try {
    markFailure("same-runtime-subscribe-failed");
    const subscribed = execute(b, {
      type: "subscribe",
      query: todosQuery,
      optionsJson: "{}",
    });
    if (subscribed.type !== "subscribed")
      throw new Error("foreground B could not subscribe to the todos query");

    // Settle and acknowledge B's initial reset before A writes. Subscribe may
    // make the reset immediately ready without scheduling a wake, so Drain must
    // be attempted first. If hydration makes Drain pending, the helper below
    // requires a fresh native wake before every Poll.
    markFailure("same-runtime-initial-reset-failed");
    let initialResetSeen = false;
    for (let attempt = 0; attempt < 96; attempt += 1) {
      const initialEvents = await drainSubscription(
        b,
        subscribed.subscription,
        codec,
        openedB.consumeWake,
      );
      for (const event of initialEvents) {
        if (event.type !== "delta") continue;
        if (event.reset) initialResetSeen = true;
      }
      if (initialResetSeen) break;
      // Ready-but-empty means the subscription exists but its initial IVM
      // reset has not reached the stream yet. It owns no retained operation,
      // so a later turn may issue a new Drain after advancing the foreground.
      b.tick();
      await new Promise<void>((resolve) => setTimeout(resolve, 0));
    }
    if (!initialResetSeen)
      throw new Error("foreground B initial subscription reset did not materialize");
    while (openedB.consumeWake()) {
      // Retire already-delivered initial-settlement notifications before A's
      // write establishes the post-commit wake epoch.
    }
    // B alone traces the post-commit bridge path. The native flag defaults to
    // off so all other foreground aliases and production callbacks are quiet.
    setWakeTraceBestEffort(openedB, true);

    markFailure("same-runtime-write-failed");
    markFailure("same-runtime-transaction-open-failed");
    const transaction = execute(a, {
      type: "beginTransaction",
      kind: "mergeable",
    });
    if (transaction.type !== "transactionOpened")
      throw new Error("foreground A write transaction did not open");
    markFailure("same-runtime-mutation-stage-failed");
    const staged = execute(a, {
      type: "insert",
      transaction: transaction.transaction,
      table: "todos",
      rowId,
      cells: fixtureCells("subscription from foreground A"),
    });
    if (staged.type !== "inserted" || !sameBytes(staged.rowId, rowId))
      throw new Error("foreground A insert was not staged with its run-bound row id");
    markFailure("same-runtime-commit-failed");
    const committed = execute(a, {
      type: "commitTransaction",
      transaction: transaction.transaction,
    });
    if (committed.type !== "transactionCommitted")
      throw new Error("foreground A write did not commit");

    markFailure("same-runtime-delta-failed");
    markFailure("same-runtime-postcommit-wake-failed");
    const wakeStartedAt = wakeTiming.now();
    const wakeDeadline = wakeStartedAt + wakeTiming.timeoutMs;
    let wakeTurns = 0;
    do {
      // Both aliases get fair ordinary relay turns.  This is the same polling
      // progression used by the first native subscription slice, not a test
      // side channel into the persistent SQLite store.
      a.tick();
      b.tick();
      // SQLite/IVM completion reaches this runtime through React Native's
      // CallInvoker. A synchronous drain loop starves that callback even
      // though both aliases are ticked fairly.
      await wakeTiming.yieldTurn();
      wakeTurns += 1;
      if (wakeTiming.now() >= wakeDeadline) break;
      if (!openedB.consumeWake()) continue;
      wakeTiming.onWake?.({ elapsedMs: wakeTiming.now() - wakeStartedAt, turns: wakeTurns });
      markFailure("same-runtime-delta-drain-failed");
      const events = await drainSubscription(
        b,
        subscribed.subscription,
        codec,
        openedB.consumeWake,
      );
      markFailure("same-runtime-delta-decode-failed");
      const deltas = events.filter((event) => event.type === "delta");
      const visibleRowIds = deltas.flatMap((event) => nativeSubscriptionDeltaRowIds(event.delta));
      markFailure("same-runtime-delta-content-failed");
      if (visibleRowIds.length === 0) continue;
      markFailure("same-runtime-delta-row-id-failed");
      if (deltas.some((event) => nativeSubscriptionDeltaHasRowId(event.delta, rowId))) {
        markFailure("same-runtime-unsubscribe-failed");
        const closed = execute(b, {
          type: "unsubscribe",
          subscription: subscribed.subscription,
        });
        if (closed.type !== "unsubscribed" || !closed.closed)
          throw new Error("foreground B subscription did not close");
        return;
      }
      if (
        deltas.some((event) =>
          nativeSubscriptionDeltaHasFieldBytes(
            event.delta,
            "title",
            Uint8Array.from([2, ...new TextEncoder().encode("subscription from foreground A")]),
          ),
        )
      ) {
        markFailure("same-runtime-delta-written-content-row-id-failed");
        continue;
      }
      const hasReset = deltas.some((event) => event.reset);
      const hasIncremental = deltas.some((event) => !event.reset);
      markFailure(
        hasReset && hasIncremental
          ? "same-runtime-delta-mixed-row-id-failed"
          : hasReset
            ? "same-runtime-delta-reset-row-id-failed"
            : "same-runtime-delta-incremental-row-id-failed",
      );
    } while (wakeTiming.now() < wakeDeadline);
    throw new Error(
      `foreground B did not observe foreground A's committed row after ${wakeTurns} turns and ${Math.round(wakeTiming.now() - wakeStartedAt)}ms without a post-commit native wake`,
    );
  } finally {
    try {
      setWakeTraceBestEffort(openedB, false);
    } finally {
      try {
        a.close();
      } finally {
        b.close();
      }
    }
  }
}

/**
 * Prove the device fixture's trusted A -> B path selection is data-plane
 * isolation, not just control-plane capability revocation. The foreground
 * command surface stays byte-only: the fixed query bytes below are the
 * canonical postcard encoding of Rust's `Query::from("todos")`, and the
 * receipt only searches returned binding bytes for the fixed fixture title.
 * It deliberately does not grow a React-Native-shaped row/query API.
 *
 * The caller performs the native A -> B replacement between the two phases,
 * then later revokes B and re-admits A. That final A read proves that closing
 * a scope and its SQLite owner does not discard its data, while B never sees
 * A's row even though both scopes use the same application fixture.
 */
export type ScopeIsolationReceipt = {
  /** A fixed fixture row written through this admitted foreground, if any. */
  write?: "a" | "b";
  /** Fixed fixture rows that this scope must materialize. */
  contains: readonly ("a" | "b")[];
  /** Fixed fixture rows that this scope must never materialize. */
  excludes: readonly ("a" | "b")[];
};

type ScopeReadTiming = {
  readonly timeoutMs: number;
  now(): number;
  yieldTurn(): Promise<void>;
};

const DEVICE_SCOPE_READ_TIMING: ScopeReadTiming = {
  timeoutMs: 5_000,
  now: () => performance.now(),
  yieldTurn: () => new Promise<void>((resolve) => setTimeout(resolve, 0)),
};

/**
 * This deliberately accepts only the two compile-time fixture row names, not
 * a caller-selected query, path, or payload.  Native platform code remains
 * the sole selector of the app/storage/auth scope behind `capability`.
 */
export async function proveForegroundScopeIsolation(
  factory: NativeForegroundRuntimeFactory,
  capability: Uint8Array,
  codec: ForegroundByteCodec,
  receipt: ScopeIsolationReceipt,
  markFailure: (code: DeviceDiagnosticCode) => void = () => {},
  readTiming: ScopeReadTiming = DEVICE_SCOPE_READ_TIMING,
  reportWriterReadDiagnostic: (detail: string) => void | Promise<void> = () => {},
): Promise<void> {
  let writer: ScopeForeground | undefined;
  try {
    if (receipt.write) {
      // The writer and reader are deliberately separate foreground handles. A
      // row must travel through the admitted relay/store rather than appearing
      // only in the memory of the handle that staged it. Keep the writer alive
      // and progressing until the reader observes the committed row: closing a
      // foreground is cancellation, not a flush primitive.
      markFailure("scope-isolation-open-failed");
      const openedWriter = openScopeForeground(factory, capability);
      writer = openedWriter;
      const execute = (command: NativeForegroundCommand): NativeForegroundResponse =>
        codec.decode(openedWriter.runtime.execute(codec.encode(command)));
      markFailure("scope-isolation-write-failed");
      const transaction = execute({
        type: "beginTransaction",
        kind: "mergeable",
      });
      if (transaction.type !== "transactionOpened")
        throw new Error("scope fixture foreground transaction did not open");
      const rowId = Uint8Array.from({ length: 16 }, () => (receipt.write === "a" ? 0x73 : 0x75));
      const staged = execute({
        type: "upsert",
        transaction: transaction.transaction,
        table: "scope_rows",
        rowId,
        cells: Uint8Array.from(scopeCells[receipt.write]),
      });
      if (staged.type !== "mutationStaged")
        throw new Error("scope fixture foreground upsert was not staged");
      const committed = execute({
        type: "commitTransaction",
        transaction: transaction.transaction,
      });
      if (committed.type !== "transactionCommitted")
        throw new Error("scope fixture foreground transaction did not commit");

      // First prove that the commit entered the writer's own materialized
      // view. This separates write/admission failures from propagation to the
      // independently attached reader below without exposing runtime details.
      markFailure("scope-isolation-writer-read-failed");
      const observation: ScopeReadObservation = {
        last: "none",
        wakes: 0,
        polls: 0,
        rowResponses: 0,
        ready: false,
      };
      try {
        await readScopeRows(
          openedWriter.runtime,
          codec,
          (candidate) => containsUtf8(candidate, scopeFixtureTitle(receipt.write!)),
          undefined,
          openedWriter.consumeWake,
          readTiming,
          observation,
        );
      } catch (error) {
        try {
          void Promise.resolve(
            reportWriterReadDiagnostic(
              `scope-isolation-writer-read-detail:last-${observation.last}-wakes-${observation.wakes}-polls-${observation.polls}-row-responses-${observation.rowResponses}-ready-${observation.ready ? "yes" : "no"}`,
            ),
          ).catch(() => {});
        } catch {
          // Diagnostics must not replace or delay the native read failure.
        }
        throw error;
      }
    }

    markFailure("scope-isolation-open-failed");
    const foreground = openScopeForeground(factory, capability);
    try {
      markFailure("scope-isolation-read-failed");
      const rows = await readScopeRows(
        foreground.runtime,
        codec,
        (candidate) =>
          receipt.contains.every((scope) => containsUtf8(candidate, scopeFixtureTitle(scope))),
        () => writer?.runtime.tick(),
        foreground.consumeWake,
        readTiming,
      );
      markFailure("scope-isolation-assert-failed");
      for (const scope of receipt.contains) {
        if (!containsUtf8(rows, scopeFixtureTitle(scope)))
          throw new Error(
            `scope ${scope.toUpperCase()} did not materialize its persisted fixture row`,
          );
      }
      for (const scope of receipt.excludes) {
        if (containsUtf8(rows, scopeFixtureTitle(scope)))
          throw new Error(
            `scope ${receipt.write?.toUpperCase() ?? "read"} observed scope ${scope.toUpperCase()}'s persisted fixture row`,
          );
      }
    } finally {
      foreground.runtime.close();
    }
  } finally {
    writer?.runtime.close();
  }
}

type ScopeForeground = {
  runtime: NativeForegroundRuntime;
  consumeWake: () => boolean;
  setWakeTrace?: (enabled: boolean) => void;
};

// An unavailable or already-torn-down private diagnostic hook must never
// replace the receipt's native behavior or its primary failure.
function setWakeTraceBestEffort(foreground: ScopeForeground, enabled: boolean): void {
  try {
    foreground.setWakeTrace?.(enabled);
  } catch {
    // The receipt still owns normal foreground progress and cleanup.
  }
}

type ScopeReadObservation = {
  last: "none" | "pending" | "subscription" | "rejected" | "closed" | "rows";
  wakes: number;
  polls: number;
  rowResponses: number;
  ready: boolean;
};

function openScopeForeground(
  factory: NativeForegroundRuntimeFactory,
  capability: Uint8Array,
): ScopeForeground {
  const foreground = factory.openAttached(capability);
  if (typeof foreground.setTickScheduler !== "function") {
    foreground.close();
    throw new Error("scope isolation foreground cannot install its native wake scheduler");
  }
  // Register the actual ForegroundWakeRegistration/CallInvoker path. The
  // callback only records delivery; the bounded read loop consumes that wake
  // before polling and performs the tick on a later event-loop turn.
  let pendingWakes = 0;
  foreground.setTickScheduler(() => {
    pendingWakes += 1;
  });
  return {
    runtime: foreground,
    setWakeTrace: foreground.setWakeTrace,
    consumeWake() {
      if (pendingWakes === 0) return false;
      pendingWakes -= 1;
      return true;
    },
  };
}

function scopeFixtureTitle(scope: "a" | "b") {
  return scope === "a" ? "scope-a-private-row" : "scope-b-private-row";
}

async function readScopeRows(
  foreground: NativeForegroundRuntime,
  codec: ForegroundByteCodec,
  ready: (rows: Uint8Array) => boolean = () => true,
  progressWriter: () => void = () => {},
  consumeWake: () => boolean = () => true,
  timing: ScopeReadTiming = DEVICE_SCOPE_READ_TIMING,
  observation?: ScopeReadObservation,
): Promise<Uint8Array> {
  const execute = (command: NativeForegroundCommand): NativeForegroundResponse =>
    codec.decode(foreground.execute(codec.encode(command)));
  const observeResponse = (response: NativeForegroundResponse) => {
    if (!observation) return;
    observation.last =
      response.type === "pending"
        ? "pending"
        : response.type === "subscriptionEvents"
          ? "subscription"
          : response.type === "rows"
            ? "rows"
            : "none";
  };
  const subscribed = execute({ type: "subscribe", query: scopeQuery, optionsJson: "{}" });
  if (subscribed.type !== "subscribed")
    throw new Error(
      "scope isolation fixture could not subscribe to the owner-protected scope query",
    );
  let pendingOperation: number | undefined;
  let published = false;
  let failed = true;
  const finishRead = () => {
    let cleanupError: unknown;
    try {
      if (pendingOperation !== undefined) execute({ type: "cancel", operation: pendingOperation });
    } catch (error) {
      cleanupError = error;
    }
    try {
      const closed = execute({ type: "unsubscribe", subscription: subscribed.subscription });
      if (closed.type !== "unsubscribed" || !closed.closed)
        throw new Error("scope isolation fixture subscription did not close");
    } catch (error) {
      cleanupError ??= error;
    }
    if (!failed && cleanupError) throw cleanupError;
  };
  const deadline = timing.now() + timing.timeoutMs;
  try {
    do {
      progressWriter();
      foreground.tick();
      // The subscription retains relay coverage for this fresh foreground.
      // LocalFirst all() alone may legitimately remain empty. Wait for actual
      // publication and keep the coverage alive until the local read finishes.
      await timing.yieldTurn();
      if (timing.now() >= deadline) break;
      if (pendingOperation !== undefined) {
        const woke = consumeWake();
        if (woke && observation) observation.wakes += 1;
        if (!woke) continue;
      }
      if (timing.now() >= deadline) break;
      let response: NativeForegroundResponse;
      if (pendingOperation !== undefined) {
        if (observation) observation.polls += 1;
        response = execute({ type: "poll", operation: pendingOperation });
      } else if (published) {
        response = execute({ type: "all", query: scopeQuery, optionsJson: "{}" });
      } else {
        response = execute({ type: "drainSubscription", subscription: subscribed.subscription });
      }
      // Retain a newly admitted operation even if execution crossed the deadline,
      // so timeout cleanup can cancel it rather than abandoning its future.
      pendingOperation = response.type === "pending" ? response.operation : undefined;
      observeResponse(response);
      if (timing.now() >= deadline) break;
      if (response.type === "subscriptionEvents") {
        if (response.events.some((event) => event.type === "rejected")) {
          if (observation) observation.last = "rejected";
          throw new Error("scope isolation fixture subscription ended before its read");
        }
        if (response.events.some((event) => event.type === "closed")) {
          if (observation) observation.last = "closed";
          throw new Error("scope isolation fixture subscription ended before its read");
        }
        if (!response.events.some((event) => event.type === "delta")) continue;
        published = true;
        response = execute({ type: "all", query: scopeQuery, optionsJson: "{}" });
        pendingOperation = response.type === "pending" ? response.operation : undefined;
        observeResponse(response);
        if (timing.now() >= deadline) break;
      }
      if (response.type === "rows") {
        if (observation) observation.rowResponses += 1;
        if (ready(response.rows)) {
          if (observation) observation.ready = true;
          failed = false;
          return response.rows;
        }
        continue;
      }
      if (response.type === "pending") continue;
      throw new Error("scope isolation fixture read returned an unexpected response");
    } while (timing.now() < deadline);
    throw new Error("scope isolation fixture read did not settle before its bounded deadline");
  } finally {
    finishRead();
  }
}

async function drainSubscription(
  foreground: NativeForegroundRuntime,
  subscription: number,
  codec: ForegroundByteCodec,
  consumeWake: () => boolean,
): Promise<Extract<NativeForegroundResponse, { type: "subscriptionEvents" }>["events"]> {
  const execute = (command: NativeForegroundCommand): NativeForegroundResponse =>
    codec.decode(foreground.execute(codec.encode(command)));
  let response = execute({ type: "drainSubscription", subscription });
  for (let attempt = 0; attempt < 96; attempt += 1) {
    if (response.type === "subscriptionEvents") return response.events;
    if (response.type !== "pending")
      throw new Error("foreground subscription drain returned an unexpected response");
    foreground.tick();
    await new Promise<void>((resolve) => setTimeout(resolve, 0));
    if (!consumeWake()) continue;
    response = execute({ type: "poll", operation: response.operation });
  }
  throw new Error("foreground subscription drain did not settle after bounded ticks");
}

function containsUtf8(bytes: Uint8Array, value: string): boolean {
  const needle = utf8(value);
  return bytes.some(
    (_, offset) =>
      offset + needle.byteLength <= bytes.byteLength &&
      needle.every((byte, index) => bytes[offset + index] === byte),
  );
}

function utf8(value: string): Uint8Array {
  const encoded = encodeURIComponent(value);
  const bytes: number[] = [];
  for (let index = 0; index < encoded.length; index += 1) {
    if (encoded[index] === "%") {
      bytes.push(Number.parseInt(encoded.slice(index + 1, index + 3), 16));
      index += 2;
    } else {
      bytes.push(encoded.charCodeAt(index));
    }
  }
  return Uint8Array.from(bytes);
}

function fixtureCells(
  title: "mergeable" | "updated" | "upserted" | "rolled back" | "subscription from foreground A",
): Uint8Array {
  const bytes = {
    mergeable: [
      1, 1, 5, 116, 105, 116, 108, 101, 8, 10, 2, 109, 101, 114, 103, 101, 97, 98, 108, 101,
    ],
    updated: [1, 1, 5, 116, 105, 116, 108, 101, 8, 8, 2, 117, 112, 100, 97, 116, 101, 100],
    upserted: [1, 1, 5, 116, 105, 116, 108, 101, 8, 9, 2, 117, 112, 115, 101, 114, 116, 101, 100],
    "rolled back": [
      1, 1, 5, 116, 105, 116, 108, 101, 8, 12, 2, 114, 111, 108, 108, 101, 100, 32, 98, 97, 99, 107,
    ],
    "subscription from foreground A": [
      1, 1, 5, 116, 105, 116, 108, 101, 8, 30, 2, 102, 111, 114, 101, 103, 114, 111, 117, 110, 100,
      45, 97, 45, 115, 117, 98, 115, 99, 114, 105, 112, 116, 105, 111, 110, 45, 114, 111, 119,
    ],
  } as const;
  const encoded = Uint8Array.from(bytes[title]);
  // This is a fixed Rust-generated one-column record fixture, not a JS row
  // codec. Its final length prefix includes the primitive-string variant byte;
  // fail locally if a hand-updated payload no longer matches that envelope.
  if (encoded[9] !== encoded.byteLength - 10) {
    throw new Error("foreground text fixture has a stale record-envelope length");
  }
  return encoded;
}

function sameBytes(left: Uint8Array, right: Uint8Array): boolean {
  return left.byteLength === right.byteLength && left.every((byte, index) => byte === right[index]);
}

function assertRejected(action: () => unknown, message: string): void {
  try {
    action();
  } catch {
    return;
  }
  throw new Error(message);
}
