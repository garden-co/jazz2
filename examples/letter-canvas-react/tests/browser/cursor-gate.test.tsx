import { describe, expect, it } from "vitest";
import { createCursorWriteGate } from "../../src/cursor-gate.js";

describe("createCursorWriteGate", () => {
  it("buffers cursor presence until the cursor row is ready, then replays only the latest one", () => {
    const gate = createCursorWriteGate();

    gate.beginCreate("cursor-1");
    expect(
      gate.notePresence({
        x: 10,
        y: 20,
        gesture: "pointer",
        dragValue: null,
        dragX: null,
        dragY: null,
      }),
    ).toBeNull();
    expect(
      gate.notePresence({
        x: 30,
        y: 40,
        gesture: "hand",
        dragValue: "D",
        dragX: 30,
        dragY: 40,
      }),
    ).toBeNull();

    expect(gate.markReady("cursor-1")).toEqual({
      cursorId: "cursor-1",
      x: 30,
      y: 40,
      gesture: "hand",
      dragValue: "D",
      dragX: 30,
      dragY: 40,
    });

    expect(
      gate.notePresence({
        x: 50,
        y: 60,
        gesture: "pointer",
        dragValue: null,
        dragX: null,
        dragY: null,
      }),
    ).toEqual({
      cursorId: "cursor-1",
      x: 50,
      y: 60,
      gesture: "pointer",
      dragValue: null,
      dragX: null,
      dragY: null,
    });
  });

  it("does not treat a locally visible cursor as ready while creation is still in flight", () => {
    const gate = createCursorWriteGate();

    gate.beginCreate();
    expect(
      gate.notePresence({
        x: 10,
        y: 20,
        gesture: "pointer",
        dragValue: null,
        dragX: null,
        dragY: null,
      }),
    ).toBeNull();
    expect(gate.adoptExisting("cursor-1")).toBeNull();
    expect(
      gate.notePresence({
        x: 30,
        y: 40,
        gesture: "hand",
        dragValue: "A",
        dragX: 30,
        dragY: 40,
      }),
    ).toBeNull();

    expect(gate.markReady("cursor-1")).toEqual({
      cursorId: "cursor-1",
      x: 30,
      y: 40,
      gesture: "hand",
      dragValue: "A",
      dragX: 30,
      dragY: 40,
    });
  });
});
