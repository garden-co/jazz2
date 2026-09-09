import nativeRelay from "./NativeJazzRelay";
import { NATIVE_RELAY_ABI, NATIVE_RELAY_ABI_V1 } from "./native-relay-abi";

/**
 * Versioned private global installed by the native JSI bridge.
 *
 * This is an implementation detail of `jazz-rn`: applications keep using
 * `jazz-tools/react-native`; they neither construct nor retain a foreground
 * engine directly. A string key lets native C++ install a JSI HostObject in the
 * current JavaScript runtime without a second JavaScript/WASM loader.
 */
const NATIVE_FOREGROUND_RUNTIME_GLOBAL = "__jazzNativeForegroundRuntimeV1";

export interface NativeRelayAbiRange {
  minimum: number;
  maximum: number;
}

export { NATIVE_RELAY_ABI, NATIVE_RELAY_ABI_V1 };

function requireNativeRelay() {
  if (nativeRelay == null) {
    throw new Error(
      "Jazz native relay is unavailable: install a matching native development or release build containing the Jazz relay artifact. Expo Go never includes it.",
    );
  }
  return nativeRelay;
}

export type NativeForegroundRuntimeFactory = {
  /** Must match the enclosing native relay ABI before a runtime is opened. */
  readonly abiVersion: number;
  /** OS entropy and shared Rust signing; neither opens a database. */
  /** @internal Logical account setup, used only by the opaque-handle adapter. */
  beginAccountSession?(requestJson: string): Uint8Array;
  attachAccountSchema?(capability: Uint8Array, schemaJson: string): Uint8Array;
  releaseAccountSession?(capability: Uint8Array): void;
  refreshAccountSession?(capability: Uint8Array, requestJson: string): void;
  /** @internal Synchronous OS-protected account preference transaction. */
  withAccountStoreLock?(callback: () => void): void;
  accountSecret?(): Uint8Array;
  mintLocalFirstToken?(
    seed: Uint8Array,
    audience: string,
    ttlSeconds: number,
    nowSeconds: number,
  ): string;
  /**
   * Create one memory-only foreground runtime for an already admitted scope.
   * The returned JSI HostObject is consumed only by Jazz's internal native
   * adapter; its database method contract is intentionally not duplicated in
   * this package.
   */
  openAttached(capability: Uint8Array): NativeForegroundRuntime;
};

/**
 * Private, byte-oriented handle for one native in-memory foreground `Db`.
 *
 * The command and response bytes are postcard values owned by the shared Rust
 * relay ABI. This is deliberately not a React-Native-shaped row/query API:
 * `jazz-tools` is the sole adapter that will map its existing `NativeDb`
 * contract onto these commands.
 */
export type NativeForegroundRuntime = {
  execute(command: Uint8Array): Uint8Array;
  tick(): void;
  /** Native typed liveness; unexpected native failures still throw. */
  isClosed?(): boolean;
  /**
   * Private wake registration used by jazz-tools' normal NativeRuntimeAdapter.
   * The native HostObject coalesces owner-thread wakes onto the current JSI
   * runtime; this is not an application callback API.
   */
  setTickScheduler?(callback: (urgency: string) => void): void;
  /** @internal Bounded Android acceptance diagnostic; disabled by default. */
  setWakeTrace?(enabled: boolean): void;
  close(): boolean;
};

/**
 * Shared foreground NativeDb command vocabulary. Query bytes are the
 * canonical postcard query bytes from `jazz-tools`' existing codec, never a
 * React-Native-shaped query object. The first read slice intentionally fixes
 * `ReadOpts` to the regular local-first defaults; callers must not silently
 * reinterpret remote tiers, views, or relation terminal operations.
 */
export type NativeForegroundPermissionAdviceAction =
  | { type: "insert"; table: string; cells: Uint8Array }
  | { type: "read" | "delete"; table: string; rowId: Uint8Array }
  | { type: "update"; table: string; rowId: Uint8Array; patch: Uint8Array };

export type NativeForegroundCommand =
  | { type: "permissionAdvice"; action: NativeForegroundPermissionAdviceAction }
  | "probe"
  | "tick"
  | {
      type: "all";
      query: Uint8Array;
      optionsJson: string;
      transaction?: number;
    }
  | { type: "localCurrentRow"; table: string; rowId: Uint8Array }
  | {
      type: "subscribe";
      query: Uint8Array;
      optionsJson: string;
    }
  | { type: "drainSubscription"; subscription: number }
  | { type: "unsubscribe"; subscription: number }
  | "close"
  | { type: "poll"; operation: number }
  | { type: "cancel"; operation: number }
  | { type: "beginTransaction"; kind: NativeForegroundTransactionKind }
  | {
      type: "insert";
      transaction: number;
      table: string;
      cells: Uint8Array;
      rowId?: Uint8Array;
    }
  | {
      type: "update";
      transaction: number;
      table: string;
      rowId: Uint8Array;
      patch: Uint8Array;
    }
  | {
      type: "upsert";
      transaction: number;
      table: string;
      rowId: Uint8Array;
      cells: Uint8Array;
    }
  | { type: "delete"; transaction: number; table: string; rowId: Uint8Array }
  | { type: "commitTransaction"; transaction: number }
  | { type: "rollbackTransaction"; transaction: number }
  | { type: "waitForTransaction"; txId: Uint8Array; tier: string }
  | { type: "waitForPendingWrites"; tier: string }
  | { type: "disconnectNativeUpstream" }
  | { type: "reconnectNativeUpstream" }
  | { type: "nativeConnectionStatus" }
  | {
      type: "directMutation";
      mutation: "insert" | "update" | "upsert" | "delete" | "restore";
      table: string;
      rowId?: Uint8Array;
      cells: Uint8Array;
      optionsJson: string;
    }
  | {
      type: "stageMutation";
      transaction: number;
      mutation: "insert" | "update" | "upsert" | "delete" | "restore";
      table: string;
      rowId?: Uint8Array;
      cells: Uint8Array;
      optionsJson: string;
    }
  | { type: "nativeSessionMetadata" }
  | { type: "writeState"; txId: Uint8Array }
  | { type: "drainMutationErrors" }
  | {
      type: "beginStreamingMutation";
      mutation: "insert" | "update" | "upsert";
      table: string;
      rowId: Uint8Array;
      cells: Uint8Array;
      column: string;
      optionsJson: string;
    }
  | { type: "pushStreamingMutation"; upload: number; chunk: Uint8Array }
  | { type: "finishStreamingMutation" | "abortStreamingMutation"; upload: number }
  | {
      type: "updateLargeValues";
      table: string;
      rowId: Uint8Array;
      patch: Uint8Array;
      descriptorsJson: string;
      updatedAtMs?: number;
    };
/** The existing core transaction semantics selected by the foreground codec. */
export type NativeForegroundTransactionKind = "mergeable" | "exclusive";

export type NativeForegroundResponse =
  | {
      type: "nativeSessionMetadata";
      node: Uint8Array;
      registryAuthority: string;
      accountId: string | null;
      issuer: string;
      userId: string;
    }
  | {
      type: "nativeConnectionStatus";
      configured: boolean;
      explicitlyOffline: boolean;
      connected: boolean;
    }
  | { type: "probe"; abiVersion: number }
  | { type: "ticked" }
  | { type: "rows"; rows: Uint8Array }
  | { type: "subscribed"; subscription: number }
  | { type: "subscriptionEvents"; events: NativeForegroundSubscriptionEvent[] }
  | { type: "unsubscribed"; closed: boolean }
  | { type: "closed"; closed: boolean }
  | { type: "pending"; operation: number }
  | { type: "operationError"; reason: string }
  | { type: "cancelled"; cancelled: boolean }
  | { type: "transactionOpened"; transaction: number }
  | { type: "inserted"; rowId: Uint8Array }
  | { type: "mutationStaged" }
  | { type: "transactionCommitted"; txId: Uint8Array }
  | { type: "transactionRolledBack"; rolledBack: boolean }
  | { type: "transactionSettled"; txId: Uint8Array }
  | { type: "writeState"; stateJson: string }
  | { type: "mutationErrors"; eventsJson: string }
  | { type: "streamingMutationOpened"; upload: number }
  | { type: "streamingMutationPushed" }
  | { type: "streamingMutationAborted"; aborted: boolean }
  | { type: "mutationCommitted"; txId: Uint8Array; rowId: Uint8Array }
  | { type: "permissionAdvice"; advice: "allowed" | "denied" | "unknown" };

export type NativeForegroundSubscriptionEvent =
  | {
      type: "delta";
      reset: boolean;
      settled: boolean;
      tier: string;
      delta: Uint8Array;
      terminalOperations?: unknown[];
    }
  | { type: "rejected"; reason: string }
  | { type: "closed" };

function foregroundRuntimeInstallationError(): Error {
  return new Error(
    "Jazz native foreground runtime installation failed: the native build did not install a compatible JSI foreground engine. Install a matching native development or release build.",
  );
}

function requireCompatibleRelay() {
  const relay = requireNativeRelay();
  const nativeAbi = relay.getAbiVersion();
  if (nativeAbi < NATIVE_RELAY_ABI.minimum || nativeAbi > NATIVE_RELAY_ABI.maximum) {
    throw new Error(
      `Jazz native relay ABI ${nativeAbi} is incompatible with JavaScript ABI ${NATIVE_RELAY_ABI.minimum}..=${NATIVE_RELAY_ABI.maximum}; install a matching native development or release build.`,
    );
  }
  return relay;
}

/**
 * Install and retrieve the private JSI foreground-runtime factory.
 *
 * It verifies the embedded relay before looking at the global: an OTA bundle
 * must not attach an old factory merely because a stale global survives a
 * bridge reload. The factory receives only the platform-issued opaque
 * capability, never path/schema/claims/identity/token configuration.
 *
 * @internal `jazz-tools/react-native` will call this once it selects the
 * native JSI engine instead of the browser/WASM runtime.
 */
export function installNativeForegroundRuntime(): NativeForegroundRuntimeFactory {
  const relay = requireCompatibleRelay();
  // React Native's TurboModuleWithJSIBindings lifecycle installs this factory
  // while resolving NativeJazzRelay in exactly the current JSI runtime. A new
  // runtime has a new global object; deleting the factory and trying to
  // reconstruct it through an ordinary TurboModule call discards the runtime
  // binding React Native deliberately provided.
  const descriptor = Object.getOwnPropertyDescriptor(globalThis, NATIVE_FOREGROUND_RUNTIME_GLOBAL);
  const factory = descriptor?.value;
  if (
    !factory ||
    typeof factory !== "object" ||
    (factory as { abiVersion?: unknown }).abiVersion !== relay.getAbiVersion() ||
    typeof (factory as { openAttached?: unknown }).openAttached !== "function"
  ) {
    throw foregroundRuntimeInstallationError();
  }
  const installed = factory as NativeForegroundRuntimeFactory;
  return {
    abiVersion: installed.abiVersion,
    beginAccountSession: installed.beginAccountSession?.bind(installed),
    attachAccountSchema: installed.attachAccountSchema?.bind(installed),
    releaseAccountSession: installed.releaseAccountSession?.bind(installed),
    refreshAccountSession: installed.refreshAccountSession?.bind(installed),
    withAccountStoreLock: installed.withAccountStoreLock?.bind(installed),
    accountSecret:
      typeof installed.accountSecret === "function"
        ? installed.accountSecret.bind(installed)
        : undefined,
    mintLocalFirstToken:
      typeof installed.mintLocalFirstToken === "function"
        ? installed.mintLocalFirstToken.bind(installed)
        : undefined,
    openAttached(capability: Uint8Array): NativeForegroundRuntime {
      // This is not the authorization check--the native host still validates
      // capability admission and copies its bytes before queuing work. It does
      // keep malformed JavaScript input from reaching the JSI command bridge.
      if (!(capability instanceof Uint8Array) || capability.byteLength !== 32) {
        throw new Error("Jazz native foreground runtime requires a 32-byte admitted capability");
      }
      const foreground = installed.openAttached(capability) as Partial<NativeForegroundRuntime>;
      if (
        !foreground ||
        typeof foreground.execute !== "function" ||
        typeof foreground.tick !== "function" ||
        typeof foreground.close !== "function"
      ) {
        throw foregroundRuntimeInstallationError();
      }
      const setWakeTrace =
        typeof foreground.setWakeTrace === "function"
          ? (enabled: boolean): void => {
              if (typeof enabled !== "boolean") {
                throw new Error("Jazz native foreground wake trace requires a boolean");
              }
              foreground.setWakeTrace!(enabled);
            }
          : undefined;
      return {
        isClosed:
          typeof foreground.isClosed === "function" ? () => foreground.isClosed!() : undefined,
        execute(command: Uint8Array): Uint8Array {
          if (!(command instanceof Uint8Array)) {
            throw new Error("Jazz native foreground command requires a Uint8Array");
          }
          const response = foreground.execute!(command);
          if (!(response instanceof Uint8Array)) {
            throw foregroundRuntimeInstallationError();
          }
          return response;
        },
        tick(): void {
          foreground.tick!();
        },
        setTickScheduler(callback: (urgency: string) => void): void {
          if (typeof foreground.setTickScheduler !== "function") {
            throw foregroundRuntimeInstallationError();
          }
          foreground.setTickScheduler(callback);
        },
        setWakeTrace,
        close(): boolean {
          return foreground.close!();
        },
      };
    },
  };
}

/** Encode one foreground NativeDb command without exposing a row/object ABI. */
export function encodeNativeForegroundCommand(command: NativeForegroundCommand): Uint8Array {
  if (command === "probe") return Uint8Array.of(0);
  if (command === "tick") return Uint8Array.of(1);
  if (command === "close") return Uint8Array.of(6);
  if (command.type === "disconnectNativeUpstream") return Uint8Array.of(19);
  if (command.type === "reconnectNativeUpstream") return Uint8Array.of(20);
  if (command.type === "nativeConnectionStatus") return Uint8Array.of(21);
  if (command.type === "nativeSessionMetadata") return Uint8Array.of(22);
  if (command.type === "drainMutationErrors") return Uint8Array.of(24);
  if (command.type === "waitForPendingWrites") {
    return concatForegroundBytes(Uint8Array.of(33), encodeForegroundString(command.tier));
  }
  if (command.type === "permissionAdvice") {
    const action = command.action;
    const tag = { insert: 0, read: 1, update: 2, delete: 3 }[action.type];
    const target =
      action.type === "insert"
        ? encodeForegroundBytes(action.cells)
        : concatForegroundBytes(
            encodeForegroundId(action.rowId, "row id"),
            ...(action.type === "update" ? [encodeForegroundBytes(action.patch)] : []),
          );
    return concatForegroundBytes(
      Uint8Array.of(32, tag),
      encodeForegroundString(action.table),
      target,
    );
  }
  if (command.type === "all") {
    return concatForegroundBytes(
      Uint8Array.of(2),
      encodeForegroundBytes(command.query),
      encodeForegroundString(command.optionsJson),
      command.transaction === undefined
        ? Uint8Array.of(0)
        : concatForegroundBytes(Uint8Array.of(1), encodeForegroundU64(command.transaction)),
    );
  }
  if (command.type === "subscribe") {
    return concatForegroundBytes(
      Uint8Array.of(3),
      encodeForegroundBytes(command.query),
      encodeForegroundString(command.optionsJson),
    );
  }
  if (command.type === "drainSubscription") {
    return concatForegroundBytes(Uint8Array.of(4), encodeForegroundU64(command.subscription));
  }
  if (command.type === "unsubscribe") {
    return concatForegroundBytes(Uint8Array.of(5), encodeForegroundU64(command.subscription));
  }
  if (command.type === "poll") {
    return concatForegroundBytes(Uint8Array.of(7), encodeForegroundU64(command.operation));
  }
  if (command.type === "cancel") {
    return concatForegroundBytes(Uint8Array.of(8), encodeForegroundU64(command.operation));
  }
  if (command.type === "beginTransaction") {
    if (command.kind !== "mergeable" && command.kind !== "exclusive") {
      throw new Error("Jazz native foreground transaction kind must be mergeable or exclusive");
    }
    return Uint8Array.of(9, command.kind === "mergeable" ? 0 : 1);
  }
  if (command.type === "insert") {
    return concatForegroundBytes(
      Uint8Array.of(10),
      encodeForegroundU64(command.transaction),
      encodeForegroundString(command.table),
      encodeForegroundBytes(command.cells),
      command.rowId === undefined
        ? Uint8Array.of(0)
        : concatForegroundBytes(Uint8Array.of(1), encodeForegroundId(command.rowId, "row id")),
    );
  }
  if (command.type === "update") {
    return concatForegroundBytes(
      Uint8Array.of(11),
      encodeForegroundU64(command.transaction),
      encodeForegroundString(command.table),
      encodeForegroundId(command.rowId, "row id"),
      encodeForegroundBytes(command.patch),
    );
  }
  if (command.type === "upsert") {
    return concatForegroundBytes(
      Uint8Array.of(12),
      encodeForegroundU64(command.transaction),
      encodeForegroundString(command.table),
      encodeForegroundId(command.rowId, "row id"),
      encodeForegroundBytes(command.cells),
    );
  }
  if (command.type === "delete") {
    return concatForegroundBytes(
      Uint8Array.of(13),
      encodeForegroundU64(command.transaction),
      encodeForegroundString(command.table),
      encodeForegroundId(command.rowId, "row id"),
    );
  }
  if (command.type === "commitTransaction") {
    return concatForegroundBytes(Uint8Array.of(14), encodeForegroundU64(command.transaction));
  }
  if (command.type === "rollbackTransaction") {
    return concatForegroundBytes(Uint8Array.of(15), encodeForegroundU64(command.transaction));
  }
  if (command.type === "waitForTransaction") {
    return concatForegroundBytes(
      Uint8Array.of(17),
      encodeForegroundId(command.txId, "transaction id"),
      encodeForegroundString(command.tier),
    );
  }
  if (command.type === "stageMutation" || command.type === "directMutation") {
    const kinds = ["insert", "update", "upsert", "delete", "restore"];
    const kind = kinds.indexOf(command.mutation);
    if (kind < 0) throw new Error("Invalid foreground mutation kind");
    return concatForegroundBytes(
      Uint8Array.of(command.type === "directMutation" ? 31 : 18),
      command.type === "directMutation"
        ? new Uint8Array()
        : encodeForegroundU64(command.transaction),
      Uint8Array.of(kind),
      encodeForegroundString(command.table),
      command.rowId === undefined
        ? Uint8Array.of(0)
        : concatForegroundBytes(Uint8Array.of(1), encodeForegroundId(command.rowId, "row id")),
      encodeForegroundBytes(command.cells),
      encodeForegroundString(command.optionsJson),
    );
  }
  if (command.type === "writeState") {
    return concatForegroundBytes(Uint8Array.of(23), encodeForegroundId(command.txId, "txId"));
  }
  if (command.type === "beginStreamingMutation") {
    const kind = ["insert", "update", "upsert"].indexOf(command.mutation);
    if (kind < 0) throw new Error("Invalid streaming mutation kind");
    return concatForegroundBytes(
      Uint8Array.of(25, kind),
      encodeForegroundString(command.table),
      encodeForegroundId(command.rowId, "row id"),
      encodeForegroundBytes(command.cells),
      encodeForegroundString(command.column),
      encodeForegroundString(command.optionsJson),
    );
  }
  if (command.type === "pushStreamingMutation") {
    return concatForegroundBytes(
      Uint8Array.of(26),
      encodeForegroundU64(command.upload),
      encodeForegroundBytes(command.chunk),
    );
  }
  if (command.type === "finishStreamingMutation" || command.type === "abortStreamingMutation") {
    return concatForegroundBytes(
      Uint8Array.of(command.type === "finishStreamingMutation" ? 27 : 28),
      encodeForegroundU64(command.upload),
    );
  }
  if (command.type === "localCurrentRow") {
    return concatForegroundBytes(
      Uint8Array.of(29),
      encodeForegroundString(command.table),
      encodeForegroundId(command.rowId, "row id"),
    );
  }
  if (command.type === "updateLargeValues") {
    return concatForegroundBytes(
      Uint8Array.of(30),
      encodeForegroundString(command.table),
      encodeForegroundId(command.rowId, "row id"),
      encodeForegroundBytes(command.patch),
      encodeForegroundString(command.descriptorsJson),
      command.updatedAtMs === undefined
        ? Uint8Array.of(0)
        : concatForegroundBytes(Uint8Array.of(1), encodeForegroundU64(command.updatedAtMs)),
    );
  }
  throw new Error("Unsupported native foreground command");
}
/** Decode the first vertical-slice foreground NativeDb response vocabulary. */
export function decodeNativeForegroundResponse(bytes: Uint8Array): NativeForegroundResponse {
  if (!(bytes instanceof Uint8Array) || bytes.length === 0) {
    throw new Error("Jazz native foreground returned an empty or malformed command response");
  }
  const tag = bytes[0]!;
  if (tag === 0) {
    const abiVersion = decodePostcardU16(bytes.subarray(1));
    if (abiVersion === null) {
      throw new Error("Jazz native foreground returned a malformed probe response");
    }
    return { type: "probe", abiVersion };
  }
  if (tag === 1 && bytes.length === 1) return { type: "ticked" };
  if (tag === 2)
    return {
      type: "rows",
      rows: decodeForegroundBytes(bytes.subarray(1), "rows"),
    };
  if (tag === 3)
    return {
      type: "subscribed",
      subscription: decodeForegroundU64(bytes.subarray(1), "subscription"),
    };
  if (tag === 4)
    return {
      type: "subscriptionEvents",
      events: decodeForegroundSubscriptionEvents(bytes.subarray(1)),
    };
  if (tag === 5 && bytes.length === 2 && (bytes[1] === 0 || bytes[1] === 1)) {
    return { type: "unsubscribed", closed: bytes[1] === 1 };
  }
  if (tag === 6 && bytes.length === 2 && (bytes[1] === 0 || bytes[1] === 1)) {
    return { type: "closed", closed: bytes[1] === 1 };
  }
  if (tag === 7)
    return {
      type: "pending",
      operation: decodeForegroundU64(bytes.subarray(1), "pending operation"),
    };
  if (tag === 8)
    return {
      type: "operationError",
      reason: decodeForegroundString(bytes.subarray(1), "operation error"),
    };
  if (tag === 9 && bytes.length === 2 && (bytes[1] === 0 || bytes[1] === 1)) {
    return { type: "cancelled", cancelled: bytes[1] === 1 };
  }
  if (tag === 10)
    return {
      type: "transactionOpened",
      transaction: decodeForegroundU64(bytes.subarray(1), "transaction"),
    };
  if (tag === 11)
    return {
      type: "inserted",
      rowId: decodeForegroundId(bytes.subarray(1), "inserted row id"),
    };
  if (tag === 12 && bytes.length === 1) return { type: "mutationStaged" };
  if (tag === 13)
    return {
      type: "transactionCommitted",
      txId: decodeForegroundId(bytes.subarray(1), "committed txId"),
    };
  if (tag === 15)
    return {
      type: "transactionSettled",
      txId: decodeForegroundId(bytes.subarray(1), "settled txId"),
    };
  if (tag === 17) {
    // Postcard node bytes, registry string, account option, exact principal.
    if (bytes.length < 19) throw new Error("Malformed native session metadata");
    const node = bytes.slice(1, 17);
    let registryEnd = 17;
    while (registryEnd < bytes.length && (bytes[registryEnd]! & 0x80) !== 0) registryEnd++;
    if (registryEnd >= bytes.length) throw new Error("Malformed native session registry");
    const registryLength = decodeForegroundU64(
      bytes.subarray(17, registryEnd + 1),
      "registry length",
    );
    const registryAuthority = decodeForegroundUtf8(
      bytes,
      registryEnd + 1,
      registryLength,
      "registry",
    );
    const accountOffset = registryEnd + 1 + registryLength;
    const present = bytes[accountOffset];
    if (present !== 0 && present !== 1) throw new Error("Malformed native session account");
    const start = accountOffset + (present === 1 ? 17 : 1);
    if (bytes.length <= start) throw new Error("Malformed native session metadata");
    const hex =
      present === 1
        ? Array.from(bytes.subarray(accountOffset + 1, accountOffset + 17), (byte) =>
            byte.toString(16).padStart(2, "0"),
          ).join("")
        : null;
    const accountId =
      hex === null
        ? null
        : `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
    let end = start;
    while (end < bytes.length && (bytes[end]! & 0x80) !== 0) end++;
    if (end >= bytes.length) throw new Error("Malformed native session metadata");
    const length = decodeForegroundU64(bytes.subarray(start, end + 1), "issuer length");
    const next = end + 1 + length;
    if (next > bytes.length) throw new Error("Malformed native session metadata");
    return {
      type: "nativeSessionMetadata",
      node,
      registryAuthority,
      accountId,
      issuer: decodeForegroundUtf8(bytes, end + 1, length, "issuer"),
      userId: decodeForegroundString(bytes.subarray(next), "user id"),
    };
  }

  if (tag === 18)
    return {
      type: "writeState",
      stateJson: decodeForegroundString(bytes.subarray(1), "write state"),
    };
  if (tag === 19)
    return {
      type: "mutationErrors",
      eventsJson: decodeForegroundString(bytes.subarray(1), "mutation errors"),
    };
  if (tag === 20)
    return {
      type: "streamingMutationOpened",
      upload: decodeForegroundU64(bytes.subarray(1), "upload"),
    };
  if (tag === 21 && bytes.length === 1) return { type: "streamingMutationPushed" };
  if (tag === 23 && bytes.length === 33)
    return { type: "mutationCommitted", txId: bytes.slice(1, 17), rowId: bytes.slice(17) };
  if (tag === 24 && bytes.length === 2 && bytes[1]! <= 2)
    return {
      type: "permissionAdvice",
      advice: (["allowed", "denied", "unknown"] as const)[bytes[1]!]!,
    };
  if (tag === 22 && bytes.length === 2 && (bytes[1] === 0 || bytes[1] === 1))
    return { type: "streamingMutationAborted", aborted: bytes[1] === 1 };
  if (
    tag === 16 &&
    bytes.length === 4 &&
    bytes.subarray(1).every((value) => value === 0 || value === 1)
  ) {
    return {
      type: "nativeConnectionStatus",
      configured: bytes[1] === 1,
      explicitlyOffline: bytes[2] === 1,
      connected: bytes[3] === 1,
    };
  }
  if (tag === 14 && bytes.length === 2 && (bytes[1] === 0 || bytes[1] === 1)) {
    return { type: "transactionRolledBack", rolledBack: bytes[1] === 1 };
  }
  throw new Error("Jazz native foreground returned an unknown or malformed command response");
}

function encodeForegroundU64(value: number): Uint8Array {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new Error("Jazz native foreground handle must be a non-negative safe integer");
  }
  const bytes: number[] = [];
  let remaining = value;
  do {
    let byte = remaining % 128;
    remaining = Math.floor(remaining / 128);
    if (remaining > 0) byte |= 0x80;
    bytes.push(byte);
  } while (remaining > 0);
  return Uint8Array.from(bytes);
}

function encodeForegroundBytes(value: Uint8Array): Uint8Array {
  if (!(value instanceof Uint8Array))
    throw new Error("Jazz native foreground command requires Uint8Array bytes");
  return concatForegroundBytes(encodeForegroundU64(value.byteLength), value);
}

function encodeForegroundId(value: Uint8Array, label: string): Uint8Array {
  if (!(value instanceof Uint8Array) || value.byteLength !== 16) {
    throw new Error(`Jazz native foreground ${label} must be a 16-byte Uint8Array`);
  }
  return value;
}

function decodeForegroundId(bytes: Uint8Array, label: string): Uint8Array {
  if (bytes.byteLength !== 16)
    throw new Error(`Jazz native foreground returned malformed ${label}`);
  return bytes.slice();
}

function encodeForegroundString(value: string): Uint8Array {
  if (typeof value !== "string") throw new Error("Jazz native foreground table must be a string");
  // React Native's configured TS lib does not promise TextEncoder. This is
  // the inverse of the strict URI-based UTF-8 decoder below and keeps the
  // command codec dependency-free in Hermes.
  const encoded = encodeURIComponent(value);
  const bytes: number[] = [];
  for (let index = 0; index < encoded.length; index += 1) {
    if (encoded[index] === "%") {
      const hex = encoded.slice(index + 1, index + 3);
      if (hex.length !== 2) throw new Error("Jazz native foreground table is malformed UTF-8");
      bytes.push(Number.parseInt(hex, 16));
      index += 2;
    } else {
      bytes.push(encoded.charCodeAt(index));
    }
  }
  return encodeForegroundBytes(Uint8Array.from(bytes));
}

function concatForegroundBytes(...parts: Uint8Array[]): Uint8Array {
  const result = new Uint8Array(parts.reduce((length, part) => length + part.byteLength, 0));
  let offset = 0;
  for (const part of parts) {
    result.set(part, offset);
    offset += part.byteLength;
  }
  return result;
}

function decodeForegroundU64(bytes: Uint8Array, label: string): number {
  let value = 0;
  let multiplier = 1;
  for (let index = 0; index < bytes.length && index < 10; index += 1) {
    const byte = bytes[index]!;
    value += (byte & 0x7f) * multiplier;
    if ((byte & 0x80) === 0) {
      // A handle occupies the whole response payload. Postcard varints must
      // stop at their first terminator and use the shortest representation.
      if (
        index + 1 === bytes.length &&
        (index === 0 || (byte & 0x7f) !== 0) &&
        Number.isSafeInteger(value)
      )
        return value;
      break;
    }
    multiplier *= 128;
  }
  throw new Error(`Jazz native foreground returned malformed ${label}`);
}

function decodeForegroundBytes(bytes: Uint8Array, label: string): Uint8Array {
  let length = 0;
  let multiplier = 1;
  for (let index = 0; index < bytes.length && index < 10; index += 1) {
    const byte = bytes[index]!;
    length += (byte & 0x7f) * multiplier;
    if ((byte & 0x80) === 0) {
      const body = bytes.subarray(index + 1);
      if (body.byteLength === length) return body;
      break;
    }
    multiplier *= 128;
  }
  throw new Error(`Jazz native foreground returned malformed ${label}`);
}

function decodeForegroundString(bytes: Uint8Array, label: string): string {
  const encoded = decodeForegroundBytes(bytes, label);
  return decodeForegroundUtf8(encoded, 0, encoded.byteLength, label);
}

function decodeForegroundSubscriptionEvents(
  bytes: Uint8Array,
): NativeForegroundSubscriptionEvent[] {
  // This slice intentionally exposes the encoded event envelope only through
  // this module. `jazz-tools` will consume the normal binding delta bytes;
  // malformed/unknown events fail closed instead of becoming an empty update.
  let offset = 0;
  const readVarint = (): number => {
    let value = 0;
    let multiplier = 1;
    for (let index = 0; index < 10; index += 1) {
      const byte = bytes[offset++];
      if (byte === undefined)
        throw new Error("Jazz native foreground returned truncated subscription events");
      value += (byte & 0x7f) * multiplier;
      if ((byte & 0x80) === 0 && Number.isSafeInteger(value)) return value;
      multiplier *= 128;
    }
    throw new Error("Jazz native foreground returned malformed subscription events");
  };
  const count = readVarint();
  const events: NativeForegroundSubscriptionEvent[] = [];
  for (let index = 0; index < count; index += 1) {
    const tag = readVarint();
    if (tag === 0 || tag === 3) {
      const reset = bytes[offset++];
      const settled = bytes[offset++];
      if ((reset !== 0 && reset !== 1) || (settled !== 0 && settled !== 1))
        throw new Error("Jazz native foreground returned malformed delta flags");
      const tierLength = readVarint();
      const tier = decodeForegroundUtf8(bytes, offset, tierLength, "tier");
      offset += tierLength;
      const deltaLength = readVarint();
      const delta = bytes.slice(offset, offset + deltaLength);
      offset += deltaLength;
      if (delta.byteLength !== deltaLength)
        throw new Error("Jazz native foreground returned truncated subscription delta");
      let terminalOperations: unknown[] | undefined;
      if (tag === 3) {
        const length = readVarint();
        const json = decodeForegroundUtf8(bytes, offset, length, "terminal operations");
        offset += length;
        const parsed: unknown = JSON.parse(json);
        if (!Array.isArray(parsed))
          throw new Error("Jazz native foreground returned malformed terminal operations");
        terminalOperations = parsed;
      }
      events.push({
        type: "delta",
        reset: reset === 1,
        settled: settled === 1,
        tier,
        delta,
        ...(terminalOperations === undefined ? {} : { terminalOperations }),
      });
    } else if (tag === 1) {
      const length = readVarint();
      const reason = decodeForegroundUtf8(bytes, offset, length, "rejection");
      offset += length;
      events.push({ type: "rejected", reason });
    } else if (tag === 2) events.push({ type: "closed" });
    else throw new Error("Jazz native foreground returned unknown subscription event");
  }
  if (offset !== bytes.length)
    throw new Error("Jazz native foreground returned trailing subscription bytes");
  return events;
}

function decodeForegroundUtf8(
  bytes: Uint8Array,
  start: number,
  length: number,
  label: string,
): string {
  const slice = bytes.subarray(start, start + length);
  if (slice.byteLength !== length)
    throw new Error(`Jazz native foreground returned truncated ${label}`);
  // React Native's configured TS lib deliberately does not promise
  // `TextDecoder`; `decodeURIComponent` is available in Hermes and gives us a
  // strict UTF-8 decode without adding a platform polyfill to this tiny ABI.
  let escaped = "";
  for (const byte of slice) escaped += `%${byte.toString(16).padStart(2, "0")}`;
  try {
    return decodeURIComponent(escaped);
  } catch {
    throw new Error(`Jazz native foreground returned malformed UTF-8 ${label}`);
  }
}

function decodePostcardU16(bytes: Uint8Array): number | null {
  let value = 0;
  for (let index = 0; index < bytes.length && index < 3; index += 1) {
    const byte = bytes[index]!;
    value |= (byte & 0x7f) << (index * 7);
    if ((byte & 0x80) === 0) {
      return value <= 0xffff && index + 1 === bytes.length ? value : null;
    }
  }
  return null;
}

/**
 * Execute one opaque base64-encoded native-relay command after checking the
 * embedded ABI.
 *
 * The command codec is intentionally not defined by this package yet: it will
 * be generated from the shared relay command contract once the native module
 * is implemented. This adapter establishes the only permitted JS/native shape
 * in advance—one version probe plus encoded-binary commands—not a row-object
 * API.
 */
export async function executeNativeRelayCommand(commandBase64: string): Promise<string> {
  const relay = requireCompatibleRelay();
  return relay.execute(commandBase64);
}
