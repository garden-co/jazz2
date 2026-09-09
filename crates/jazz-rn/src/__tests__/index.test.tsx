type FixtureNativeRelay = {
  getAbiVersion(): number;
  execute(commandBase64: string): Promise<string>;
};

import { NATIVE_RELAY_ABI_V1 } from "../native-relay-abi";

const foregroundRuntimeGlobal = "__jazzNativeForegroundRuntimeV1";

type NativeForegroundCommand =
  | "probe"
  | "tick"
  | "close"
  | {
      type: "all";
      query: Uint8Array;
      optionsJson: string;
      transaction?: number;
    }
  | { type: "poll"; operation: number }
  | { type: "cancel"; operation: number }
  | { type: "beginTransaction"; kind: "mergeable" | "exclusive" }
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
  | { type: "rollbackTransaction"; transaction: number };

type RelayExports = {
  executeNativeRelayCommand(command: string): Promise<string>;
  installNativeForegroundRuntime(): {
    abiVersion: number;
    openAttached(capability: Uint8Array): {
      execute(command: Uint8Array): Uint8Array;
      tick(): void;
      setWakeTrace?(enabled: boolean): void;
      close(): boolean;
    };
  };
  encodeNativeForegroundCommand(command: NativeForegroundCommand): Uint8Array;
  decodeNativeForegroundResponse(bytes: Uint8Array): unknown;
};

function foregroundFixture() {
  return {
    execute: jest.fn(() => Uint8Array.of(1)),
    tick: jest.fn(),
    close: jest.fn(() => true),
  };
}

function loadRelay(nativeRelay: FixtureNativeRelay | null) {
  jest.resetModules();
  jest.doMock("../NativeJazzRelay", () => ({
    __esModule: true,
    default: nativeRelay,
  }));
  // Each case supplies the native boundary before importing the wrapper. That
  // is the same one-time lookup Metro performs for an installed native build.
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  return require("../relay") as RelayExports;
}

afterEach(() => {
  delete (globalThis as Record<string, unknown>)[foregroundRuntimeGlobal];
  jest.resetModules();
  jest.dontMock("../NativeJazzRelay");
});

it("tells Expo Go and old development builds that a native artifact is required", async () => {
  const relay = loadRelay(null);

  await expect(relay.executeNativeRelayCommand("AA==")).rejects.toThrow(
    "install a matching native development or release build containing the Jazz relay artifact. Expo Go never includes it.",
  );
});

it("rejects an installed native build with an incompatible ABI before executing a command", async () => {
  const nativeRelay: FixtureNativeRelay = {
    getAbiVersion: () => 2,
    execute: jest.fn(),
  };
  const relay = loadRelay(nativeRelay);

  await expect(relay.executeNativeRelayCommand("AA==")).rejects.toThrow(
    "Jazz native relay ABI 2 is incompatible with JavaScript ABI 1..=1; install a matching native development or release build.",
  );
  expect(nativeRelay.execute).not.toHaveBeenCalled();
});

it("forwards opaque commands only after the embedded relay ABI matches", async () => {
  const nativeRelay: FixtureNativeRelay = {
    getAbiVersion: () => NATIVE_RELAY_ABI_V1,
    execute: jest.fn().mockResolvedValue("AQ=="),
  };
  const relay = loadRelay(nativeRelay);

  await expect(relay.executeNativeRelayCommand("AA==")).resolves.toBe("AQ==");
  expect(nativeRelay.execute).toHaveBeenCalledWith("AA==");
});

it("requires the matching bindings-installed foreground factory instead of attempting browser WASM", () => {
  const nativeRelay: FixtureNativeRelay = {
    getAbiVersion: () => NATIVE_RELAY_ABI_V1,
    execute: jest.fn(),
  };
  const relay = loadRelay(nativeRelay);

  expect(() => relay.installNativeForegroundRuntime()).toThrow(
    "Jazz native foreground runtime installation failed: the native build did not install a compatible JSI foreground engine. Install a matching native development or release build.",
  );
  expect(nativeRelay.execute).not.toHaveBeenCalled();
});

it("accepts only the matching capability-only JSI foreground factory", () => {
  const foreground = foregroundFixture();
  const openAttached = jest.fn(() => foreground);
  (globalThis as Record<string, unknown>)[foregroundRuntimeGlobal] = {
    abiVersion: NATIVE_RELAY_ABI_V1,
    openAttached,
  };
  const nativeRelay: FixtureNativeRelay = {
    getAbiVersion: () => NATIVE_RELAY_ABI_V1,
    execute: jest.fn(),
  };
  const relay = loadRelay(nativeRelay);

  const factory = relay.installNativeForegroundRuntime();

  expect(factory.abiVersion).toBe(NATIVE_RELAY_ABI_V1);
  const capability = new Uint8Array(32);
  expect(factory.openAttached(capability)).toMatchObject({
    execute: expect.any(Function),
    tick: expect.any(Function),
    close: expect.any(Function),
  });
  expect(openAttached).toHaveBeenCalledWith(capability);
  expect(nativeRelay.execute).not.toHaveBeenCalled();
});

it("forwards the private wake trace switch only when the native handle provides it", () => {
  const setWakeTrace = jest.fn();
  const foreground = { ...foregroundFixture(), setWakeTrace };
  const nativeRelay: FixtureNativeRelay = {
    getAbiVersion: () => NATIVE_RELAY_ABI_V1,
    execute: jest.fn(),
  };
  (globalThis as Record<string, unknown>)[foregroundRuntimeGlobal] = {
    abiVersion: NATIVE_RELAY_ABI_V1,
    openAttached: jest.fn(() => foreground),
  };
  const runtime = loadRelay(nativeRelay)
    .installNativeForegroundRuntime()
    .openAttached(new Uint8Array(32));

  runtime.setWakeTrace?.(true);
  runtime.setWakeTrace?.(false);

  expect(setWakeTrace).toHaveBeenNthCalledWith(1, true);
  expect(setWakeTrace).toHaveBeenNthCalledWith(2, false);
});

it("keeps wake tracing absent for an older private native handle", () => {
  const nativeRelay: FixtureNativeRelay = {
    getAbiVersion: () => NATIVE_RELAY_ABI_V1,
    execute: jest.fn(),
  };
  (globalThis as Record<string, unknown>)[foregroundRuntimeGlobal] = {
    abiVersion: NATIVE_RELAY_ABI_V1,
    openAttached: jest.fn(foregroundFixture),
  };

  const runtime = loadRelay(nativeRelay)
    .installNativeForegroundRuntime()
    .openAttached(new Uint8Array(32));

  expect(runtime.setWakeTrace).toBeUndefined();
});

it("rejects a missing, malformed, or ABI-incompatible bindings-installed JSI foreground factory", () => {
  for (const factory of [
    undefined,
    {},
    { abiVersion: 2, openAttached: () => foregroundFixture() },
  ]) {
    const nativeRelay: FixtureNativeRelay = {
      getAbiVersion: () => NATIVE_RELAY_ABI_V1,
      execute: jest.fn(),
    };
    if (factory !== undefined)
      (globalThis as Record<string, unknown>)[foregroundRuntimeGlobal] = factory;
    const relay = loadRelay(nativeRelay);

    expect(() => relay.installNativeForegroundRuntime()).toThrow(
      "Jazz native foreground runtime installation failed: the native build did not install a compatible JSI foreground engine. Install a matching native development or release build.",
    );
    delete (globalThis as Record<string, unknown>)[foregroundRuntimeGlobal];
  }
});

it("keeps malformed capability input out of the JSI foreground factory", () => {
  const openAttached = jest.fn(() => foregroundFixture());
  const nativeRelay: FixtureNativeRelay = {
    getAbiVersion: () => NATIVE_RELAY_ABI_V1,
    execute: jest.fn(),
  };
  (globalThis as Record<string, unknown>)[foregroundRuntimeGlobal] = {
    abiVersion: NATIVE_RELAY_ABI_V1,
    openAttached,
  };
  const relay = loadRelay(nativeRelay);
  const factory = relay.installNativeForegroundRuntime();

  for (const malformed of [new Uint8Array(31), new Uint8Array(33), [] as unknown as Uint8Array]) {
    expect(() => factory.openAttached(malformed)).toThrow(
      "Jazz native foreground runtime requires a 32-byte admitted capability",
    );
  }
  expect(openAttached).not.toHaveBeenCalled();

  const admitted = new Uint8Array(32);
  factory.openAttached(admitted);
  expect(openAttached).toHaveBeenCalledWith(admitted);
});

it("uses the compact canonical byte vocabulary for the foreground NativeDb slice", () => {
  const relay = loadRelay({
    getAbiVersion: () => NATIVE_RELAY_ABI_V1,
    execute: jest.fn(),
  });

  expect(relay.encodeNativeForegroundCommand("probe")).toEqual(Uint8Array.of(0));
  expect(relay.encodeNativeForegroundCommand("tick")).toEqual(Uint8Array.of(1));
  expect(relay.encodeNativeForegroundCommand("close")).toEqual(Uint8Array.of(6));
  expect(
    relay.encodeNativeForegroundCommand({
      type: "all",
      query: Uint8Array.of(1, 2),
      optionsJson: "{}",
    }),
  ).toEqual(Uint8Array.of(2, 2, 1, 2, 2, 123, 125, 0));
  expect(relay.encodeNativeForegroundCommand({ type: "poll", operation: 129 })).toEqual(
    Uint8Array.of(7, 129, 1),
  );
  expect(relay.encodeNativeForegroundCommand({ type: "cancel", operation: 129 })).toEqual(
    Uint8Array.of(8, 129, 1),
  );
  expect(
    relay.encodeNativeForegroundCommand({
      type: "beginTransaction",
      kind: "mergeable",
    }),
  ).toEqual(Uint8Array.of(9, 0));
  expect(
    relay.encodeNativeForegroundCommand({
      type: "beginTransaction",
      kind: "exclusive",
    }),
  ).toEqual(Uint8Array.of(9, 1));
  expect(
    relay.encodeNativeForegroundCommand({
      type: "insert",
      transaction: 3,
      table: "todos",
      cells: Uint8Array.of(1, 2),
      rowId: undefined,
    }),
  ).toEqual(Uint8Array.of(10, 3, 5, 116, 111, 100, 111, 115, 2, 1, 2, 0));
  expect(
    relay.encodeNativeForegroundCommand({
      type: "update",
      transaction: 3,
      table: "todos",
      rowId: new Uint8Array(16).fill(7),
      patch: Uint8Array.of(9),
    }),
  ).toEqual(
    Uint8Array.from([11, 3, 5, 116, 111, 100, 111, 115, ...new Uint8Array(16).fill(7), 1, 9]),
  );
  expect(
    relay.encodeNativeForegroundCommand({
      type: "upsert",
      transaction: 3,
      table: "todos",
      rowId: new Uint8Array(16).fill(8),
      cells: Uint8Array.of(9),
    }),
  ).toEqual(
    Uint8Array.from([12, 3, 5, 116, 111, 100, 111, 115, ...new Uint8Array(16).fill(8), 1, 9]),
  );
  expect(
    relay.encodeNativeForegroundCommand({
      type: "delete",
      transaction: 3,
      table: "todos",
      rowId: new Uint8Array(16).fill(9),
    }),
  ).toEqual(Uint8Array.from([13, 3, 5, 116, 111, 100, 111, 115, ...new Uint8Array(16).fill(9)]));
  expect(
    relay.encodeNativeForegroundCommand({
      type: "commitTransaction",
      transaction: 129,
    }),
  ).toEqual(Uint8Array.of(14, 129, 1));
  expect(
    relay.encodeNativeForegroundCommand({
      type: "rollbackTransaction",
      transaction: 129,
    }),
  ).toEqual(Uint8Array.of(15, 129, 1));
  expect(() =>
    relay.encodeNativeForegroundCommand({
      type: "beginTransaction",
      kind: "neither",
    } as unknown as NativeForegroundCommand),
  ).toThrow("Jazz native foreground transaction kind must be mergeable or exclusive");
  expect(relay.decodeNativeForegroundResponse(Uint8Array.of(0, NATIVE_RELAY_ABI_V1))).toEqual({
    type: "probe",
    abiVersion: NATIVE_RELAY_ABI_V1,
  });
  expect(relay.decodeNativeForegroundResponse(Uint8Array.of(1))).toEqual({
    type: "ticked",
  });
  expect(relay.decodeNativeForegroundResponse(Uint8Array.of(6, 1))).toEqual({
    type: "closed",
    closed: true,
  });
  expect(relay.decodeNativeForegroundResponse(Uint8Array.of(7, 129, 1))).toEqual({
    type: "pending",
    operation: 129,
  });
  expect(relay.decodeNativeForegroundResponse(Uint8Array.of(8, 4, 111, 111, 112, 115))).toEqual({
    type: "operationError",
    reason: "oops",
  });
  expect(relay.decodeNativeForegroundResponse(Uint8Array.of(9, 1))).toEqual({
    type: "cancelled",
    cancelled: true,
  });
  expect(relay.decodeNativeForegroundResponse(Uint8Array.of(10, 129, 1))).toEqual({
    type: "transactionOpened",
    transaction: 129,
  });
  expect(
    relay.decodeNativeForegroundResponse(Uint8Array.from([11, ...new Uint8Array(16).fill(3)])),
  ).toEqual({
    type: "inserted",
    rowId: new Uint8Array(16).fill(3),
  });
  expect(relay.decodeNativeForegroundResponse(Uint8Array.of(12))).toEqual({
    type: "mutationStaged",
  });
  expect(
    relay.decodeNativeForegroundResponse(Uint8Array.from([13, ...new Uint8Array(16).fill(4)])),
  ).toEqual({
    type: "transactionCommitted",
    txId: new Uint8Array(16).fill(4),
  });
  expect(relay.decodeNativeForegroundResponse(Uint8Array.of(14, 1))).toEqual({
    type: "transactionRolledBack",
    rolledBack: true,
  });
  expect(() => relay.decodeNativeForegroundResponse(Uint8Array.of(1, 0))).toThrow(
    "unknown or malformed command response",
  );
});

it("decodes canonical foreground handles through the JavaScript safe integer limit", () => {
  const relay = loadRelay(null);
  const corpus = [
    [0, [0]],
    [127, [127]],
    [128, [128, 1]],
    [16384, [128, 128, 1]],
    [Number.MAX_SAFE_INTEGER, [255, 255, 255, 255, 255, 255, 255, 15]],
  ] as const;
  for (const [value, bytes] of corpus) {
    expect(relay.decodeNativeForegroundResponse(Uint8Array.from([3, ...bytes]))).toEqual({
      type: "subscribed",
      subscription: value,
    });
    expect(relay.decodeNativeForegroundResponse(Uint8Array.from([7, ...bytes]))).toEqual({
      type: "pending",
      operation: value,
    });
    expect(relay.decodeNativeForegroundResponse(Uint8Array.from([10, ...bytes]))).toEqual({
      type: "transactionOpened",
      transaction: value,
    });
  }
});

it("rejects trailing, nonminimal, truncated, and out-of-range foreground handles", () => {
  const relay = loadRelay(null);
  const corpus = [
    [],
    [10, 0],
    [0, 0],
    [128, 0],
    [129, 0],
    [255, 0],
    [128],
    [128, 128, 128, 128, 128, 128, 128, 16], // 2^53 exceeds JS safe integers.
    [255, 255, 255, 255, 255, 255, 255, 255, 255, 1], // u64::MAX cannot be represented safely.
    [128, 128, 128, 128, 128, 128, 128, 128, 128, 2], // u64 overflow.
    [128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 0],
  ];
  for (const bytes of corpus) {
    for (const tag of [3, 7, 10]) {
      expect(() => relay.decodeNativeForegroundResponse(Uint8Array.from([tag, ...bytes]))).toThrow(
        /malformed/,
      );
    }
  }
});
