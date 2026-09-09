import { describe, expect, it, vi } from "vitest";
import { authorForNativeOpenConfig } from "./native-codec.js";
import { NativeRuntimeAdapter } from "./native-runtime-adapter.js";

const schema = {};
const node = new Uint8Array(16);
const author = new TextEncoder().encode(JSON.stringify(["https://issuer.example", "alice"]));
const proof = {
  token: "signed-proof",
  appId: "proof-app",
  claimedAuthor: JSON.stringify(["urn:jazz:local-first", "alice"]),
};

function fakeDb() {
  // The constructor returns a full NativeDb even though this narrow ABI test
  // only reaches the scheduler and close boundary. Keep the fixture structural
  // so production native-runtime contracts remain checked by TypeScript.
  const unused = (): never => {
    throw new Error("unexpected native database operation in open ABI test");
  };
  return {
    registerSchema: unused,
    beginTransaction: unused,
    commitTransaction: unused,
    rollbackTransaction: unused,
    all: unused,
    insert: unused,
    insertInTransaction: unused,
    restore: unused,
    restoreInTransaction: unused,
    update: unused,
    updateInTransaction: unused,
    upsert: unused,
    upsertInTransaction: unused,
    delete: unused,
    deleteInTransaction: unused,
    setTickScheduler: vi.fn(),
    onMutationError: unused,
    connectUpstream: unused,
    tick: unused,
    close: vi.fn(),
  };
}

describe("self-signed native open ABI", () => {
  it("keeps reserved authors out of the ordinary config while retaining external authors", () => {
    for (const issuer of ["urn:jazz:local-first", "urn:jazz:anonymous"]) {
      const reserved = new TextEncoder().encode(JSON.stringify([issuer, "alice"]));
      expect(new TextDecoder().decode(authorForNativeOpenConfig(reserved, proof))).toBe(
        '["https://jazz.invalid","self-signed-open"]',
      );
    }
    expect(authorForNativeOpenConfig(author)).toBe(author);
  });

  it("fails explicitly against an old native artifact instead of falling back to its raw open", () => {
    const oldArtifact = { openMemory: vi.fn(() => fakeDb()) };

    expect(
      () =>
        new NativeRuntimeAdapter(oldArtifact, schema, node, author, 1, false, {
          selfSignedClientProof: proof,
        }),
    ).toThrow(/does not support self-signed client opens/);
    expect(oldArtifact.openMemory).not.toHaveBeenCalled();
  });

  it("uses only the distinct proof-bearing entrypoint with the bounded proof fields", () => {
    const openMemory = vi.fn(() => fakeDb());
    const openMemoryWithSelfSignedProof = vi.fn(() => fakeDb());
    const runtime = new NativeRuntimeAdapter(
      { openMemory, openMemoryWithSelfSignedProof },
      schema,
      node,
      author,
      1,
      false,
      { selfSignedClientProof: proof },
    );

    expect(openMemory).not.toHaveBeenCalled();
    expect(openMemoryWithSelfSignedProof).toHaveBeenCalledWith(
      expect.any(Uint8Array),
      expect.any(Uint8Array),
      proof.token,
      proof.appId,
      proof.claimedAuthor,
    );
    void runtime.close();
  });

  it("fails closed if a worker artifact lacks proof-verified subscriber admission", async () => {
    const rawAdmission = vi.fn();
    const runtime = NativeRuntimeAdapter.fromDb(
      { ...fakeDb(), acceptSubscriber: rawAdmission },
      schema,
      node,
      new TextEncoder().encode(proof.claimedAuthor),
      1,
      false,
      { selfSignedClientProof: proof },
    );

    await expect(runtime.acceptPeer()).rejects.toBeInstanceOf(Error);
    expect(rawAdmission).not.toHaveBeenCalled();
    await runtime.close();
  });

  it("uses only the distinct backend entrypoint for an intentional backend runtime", () => {
    const openMemory = vi.fn(() => fakeDb());
    const openMemoryAsBackend = vi.fn(() => fakeDb());
    const runtime = new NativeRuntimeAdapter(
      { openMemory, openMemoryAsBackend },
      schema,
      node,
      author,
      1,
      false,
      { backendMode: true },
    );

    expect(openMemory).not.toHaveBeenCalled();
    expect(openMemoryAsBackend).toHaveBeenCalledWith(
      expect.any(Uint8Array),
      expect.any(Uint8Array),
    );
    void runtime.close();
  });
});
