import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, describe, expect, it, vi } from "vitest";
import { Db, type DbConfig } from "jazz-tools";
import { App } from "../../src/App.js";
import "../../src/styles.css";
import { APP_ID, TEST_PORT, testSecret } from "./test-constants.js";

function uniqueDbName(label: string): string {
  return `test-${label}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

const SERVER_SYNC_TIMEOUT_MS = 15000;

async function waitFor(check: () => boolean, timeoutMs: number, message: string): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (check()) return;
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  throw new Error(`Timeout: ${message}`);
}

function dispatchPointer(
  target: EventTarget,
  type: string,
  { x, y }: { x: number; y: number },
): void {
  target.dispatchEvent(
    new PointerEvent(type, {
      clientX: x,
      clientY: y,
      bubbles: true,
      cancelable: true,
      pointerId: 1,
      buttons: type === "pointerup" ? 0 : 1,
    }),
  );
}

function centerOf(element: Element): { x: number; y: number } {
  const rect = element.getBoundingClientRect();
  return {
    x: rect.left + rect.width / 2,
    y: rect.top + rect.height / 2,
  };
}

function boardPoint(
  board: Element,
  xRatio: number,
  yRatio: number,
): {
  x: number;
  y: number;
} {
  const rect = board.getBoundingClientRect();
  return {
    x: rect.left + rect.width * xRatio,
    y: rect.top + rect.height * yRatio,
  };
}

function parseLetterPosition(letter: Element): { left: number; top: number } {
  const left = Number.parseFloat(letter.getAttribute("data-letter-x") ?? "NaN");
  const top = Number.parseFloat(letter.getAttribute("data-letter-y") ?? "NaN");
  return { left, top };
}

function currentCanvasId() {
  return new URLSearchParams(window.location.search).get("canvas");
}

describe("Letter Canvas E2E", () => {
  const mounts: Array<{ root: Root; container: HTMLDivElement }> = [];

  async function mountApp(
    config: {
      appId?: string;
      serverUrl?: string;
      secret?: string;
      driver?: DbConfig["driver"];
    } = {},
  ): Promise<HTMLDivElement> {
    const container = document.createElement("div");
    document.body.appendChild(container);
    const root = createRoot(container);
    mounts.push({ root, container });

    await act(async () => {
      root.render(
        <App config={{ appId: config.appId ?? APP_ID, serverUrl: config.serverUrl, ...config }} />,
      );
    });

    await waitFor(
      () => container.querySelector('[data-testid="shared-board"]') !== null,
      10000,
      "App should render the shell",
    );

    return container;
  }

  async function unmountContainer(container: HTMLDivElement): Promise<void> {
    const mountIndex = mounts.findIndex((entry) => entry.container === container);
    if (mountIndex === -1) {
      throw new Error("Mount not found");
    }

    const [{ root, container: mountedContainer }] = mounts.splice(mountIndex, 1);
    try {
      await act(async () => root.unmount());
    } catch {
      // best effort
    }
    mountedContainer.remove();
  }

  afterEach(async () => {
    for (const { root, container } of mounts) {
      try {
        await act(async () => root.unmount());
      } catch {
        // best effort
      }
      container.remove();
    }
    mounts.length = 0;
    window.history.replaceState({}, "", "/");
    vi.restoreAllMocks();
  });

  it("renders a fixed-ratio board, a letter drawer, and creates a canvas URL when missing", async () => {
    window.history.replaceState({}, "", "/");

    const container = await mountApp({
      driver: { type: "persistent", dbName: uniqueDbName("layout") },
    });

    await waitFor(
      () => container.querySelector('[data-testid="shared-board"]') !== null,
      2000,
      "Shared board should appear",
    );

    const board = container.querySelector('[data-testid="shared-board"]') as HTMLElement;
    const drawer = container.querySelector('[data-testid="letter-drawer"]') as HTMLElement;
    const drawerRows = [...container.querySelectorAll('[data-testid="drawer-row"]')].map((row) =>
      row.getAttribute("data-drawer-row"),
    );

    expect(board).toBeTruthy();
    expect(drawer).toBeTruthy();
    expect(board.dataset.aspectRatio).toBe("16:9");
    expect(drawerRows).toEqual(["qwertyuiop", "asdfghjkl", "zxcvbnm"]);
    expect(container.textContent).not.toContain("Shared Letter Canvas");
    expect(container.textContent).not.toContain("Local-first shared demo");
    expect(container.textContent).not.toContain("Guest mode");
    expect(container.textContent).not.toContain("Letter sourcing drawer");
    expect(container.textContent).not.toContain("Everyone can move every letter.");
    const source = container.querySelector('[data-letter-source="a"]') as HTMLElement;
    const drawerGrid = container.querySelector(".drawer-grid") as HTMLElement;
    const drawerRow = container.querySelector('[data-testid="drawer-row"]') as HTMLElement;
    await waitFor(
      () =>
        getComputedStyle(drawer).touchAction === "none" &&
        getComputedStyle(drawerGrid).touchAction === "none" &&
        getComputedStyle(drawerRow).touchAction === "none" &&
        getComputedStyle(source).touchAction === "none",
      1000,
      "The drawer surface should opt out of touch scrolling",
    );

    await waitFor(() => currentCanvasId() !== null, 5000, "Canvas id should be written to the URL");
    expect(currentCanvasId()).toBeTruthy();

    const locationQr = container.querySelector('[data-testid="location-qr"]') as HTMLElement;
    expect(locationQr).toBeTruthy();
    expect(locationQr.getAttribute("data-qr-value")).toBe(window.location.href);
    expect(locationQr.querySelector("svg")).toBeTruthy();
    expect(getComputedStyle(locationQr).position).toBe("fixed");
    expect(getComputedStyle(locationQr).top).toBe("0px");
    expect(getComputedStyle(locationQr).right).toBe("0px");
    expect(getComputedStyle(locationQr).pointerEvents).toBe("none");
  });

  it("forwards arrow key presses to the parent frame", async () => {
    window.history.replaceState({}, "", "/");

    const postMessageSpy = vi
      .spyOn(window.parent, "postMessage")
      .mockImplementation(() => undefined);

    await mountApp({
      driver: { type: "persistent", dbName: uniqueDbName("arrow-key-forwarding") },
    });

    const arrowEvent = new KeyboardEvent("keydown", {
      key: "ArrowRight",
      code: "ArrowRight",
      bubbles: true,
      cancelable: true,
      shiftKey: true,
      repeat: true,
    });
    const enterEvent = new KeyboardEvent("keydown", {
      key: "Enter",
      code: "Enter",
      bubbles: true,
      cancelable: true,
    });

    window.dispatchEvent(arrowEvent);
    window.dispatchEvent(enterEvent);

    expect(postMessageSpy).toHaveBeenCalledTimes(1);
    expect(postMessageSpy).toHaveBeenCalledWith(
      {
        type: "jazz-letter-canvas:arrow-key",
        key: "ArrowRight",
        code: "ArrowRight",
        altKey: false,
        ctrlKey: false,
        metaKey: false,
        shiftKey: true,
        repeat: true,
      },
      "*",
    );
    expect(arrowEvent.defaultPrevented).toBe(true);
    expect(enterEvent.defaultPrevented).toBe(false);
  });

  it("creates a canvas URL locally even while durable canvas confirmation is still pending", async () => {
    window.history.replaceState({}, "", "/");

    const originalInsert = Db.prototype.insert;
    vi.spyOn(Db.prototype, "insert").mockImplementation(function (table, data, options) {
      const pendingInsert = originalInsert.call(this, table, data, options);
      if ((table as { _table?: string })._table === "canvases") {
        return {
          ...pendingInsert,
          wait: () => new Promise(() => {}),
        };
      }

      return pendingInsert;
    });

    const container = await mountApp({
      driver: { type: "persistent", dbName: uniqueDbName("canvas-bootstrap-pending") },
    });

    await waitFor(
      () => currentCanvasId() !== null,
      5000,
      "Canvas id should still be written to the URL",
    );
    await waitFor(
      () => !container.textContent?.includes("Creating a new shared canvas..."),
      5000,
      "Canvas bootstrap message should clear once the local canvas id exists",
    );

    expect(currentCanvasId()).toBeTruthy();
    expect(container.textContent).not.toContain("Creating a new shared canvas...");
  });

  it("waits on the local cursor insert handle before the board becomes ready", async () => {
    window.history.replaceState({}, "", "/");

    const originalInsert = Db.prototype.insert;
    const cursorWaitSpy = vi.fn();
    vi.spyOn(Db.prototype, "insert").mockImplementation(function (table, data, options) {
      const pendingInsert = originalInsert.call(this, table, data, options);
      if ((table as { _table?: string })._table !== "cursors") {
        return pendingInsert;
      }

      return {
        ...pendingInsert,
        wait: async (waitOptions: { tier: "local" | "edge" | "global" }) => {
          cursorWaitSpy(waitOptions);
          return pendingInsert.wait(waitOptions);
        },
      };
    });

    const container = await mountApp({
      driver: { type: "persistent", dbName: uniqueDbName("cursor-wait-api") },
    });

    await waitFor(
      () =>
        container
          .querySelector('[data-testid="shared-board"]')
          ?.getAttribute("data-canvas-ready") === "true",
      5000,
      "Board should become ready after waiting on the cursor insert handle",
    );

    expect(cursorWaitSpy).toHaveBeenCalledWith({ tier: "local" });
  });

  it("adds a new letter by dragging from the drawer onto the board", async () => {
    window.history.replaceState({}, "", "/");

    const container = await mountApp({
      driver: { type: "persistent", dbName: uniqueDbName("spawn-letter") },
    });

    await waitFor(
      () =>
        container.querySelector('[data-testid="shared-board"]') !== null &&
        container.querySelector('[data-letter-source="a"]') !== null &&
        container
          .querySelector('[data-testid="shared-board"]')
          ?.getAttribute("data-canvas-ready") === "true" &&
        currentCanvasId() !== null,
      5000,
      "Board, drawer source, and canvas id should appear",
    );

    const board = container.querySelector('[data-testid="shared-board"]') as HTMLElement;
    const source = container.querySelector('[data-letter-source="a"]') as HTMLElement;

    const start = centerOf(source);
    const drop = boardPoint(board, 0.5, 0.35);

    await act(async () => {
      dispatchPointer(source, "pointerdown", start);
      dispatchPointer(window, "pointermove", drop);
      dispatchPointer(window, "pointerup", drop);
    });

    await waitFor(
      () => {
        const letter = container.querySelector('[data-testid="canvas-letter"]');
        return (
          container.querySelectorAll('[data-testid="canvas-letter"]').length === 1 &&
          letter?.getAttribute("data-letter-ready") === "true"
        );
      },
      3000,
      "Dragged letter should appear on the board once it is ready",
    );

    const letter = container.querySelector('[data-testid="canvas-letter"]') as HTMLElement;
    expect(letter.textContent).toBe("a");
    expect(letter.getAttribute("data-letter-ready")).toBe("true");
  });

  it("shows a brief drag hint when a drawer letter is tapped instead of dragged", async () => {
    window.history.replaceState({}, "", "/");

    const container = await mountApp({
      driver: { type: "persistent", dbName: uniqueDbName("tap-source-hint") },
    });

    await waitFor(
      () =>
        container
          .querySelector('[data-testid="shared-board"]')
          ?.getAttribute("data-canvas-ready") === "true" &&
        container.querySelector('[data-letter-source="a"]') !== null,
      5000,
      "Board should be ready before tapping a drawer source",
    );

    const source = container.querySelector('[data-letter-source="a"]') as HTMLElement;
    const tapPoint = centerOf(source);

    await act(async () => {
      dispatchPointer(source, "pointerdown", tapPoint);
      dispatchPointer(window, "pointerup", tapPoint);
    });

    await waitFor(
      () => container.querySelector('[data-testid="drawer-hint"]')?.textContent !== undefined,
      1000,
      "Tap hint should appear",
    );

    const hint = container.querySelector('[data-testid="drawer-hint"]') as HTMLElement;
    expect(hint.textContent).toBe("Drag letters onto the canvas");
    expect(getComputedStyle(hint).position).toBe("absolute");
    expect(getComputedStyle(hint).fontSize).toBe("26px");
    expect(getComputedStyle(hint).pointerEvents).toBe("none");
    expect(container.querySelectorAll('[data-testid="canvas-letter"]').length).toBe(0);

    await new Promise((resolve) => setTimeout(resolve, 1800));

    await waitFor(
      () => container.querySelector('[data-testid="drawer-hint"]') === null,
      1000,
      "Tap hint should disappear",
    );
  });

  it("shows the drag hint after a short drawer drag that never reaches the canvas", async () => {
    window.history.replaceState({}, "", "/");

    const container = await mountApp({
      driver: { type: "persistent", dbName: uniqueDbName("short-source-drag-hint") },
    });

    await waitFor(
      () =>
        container
          .querySelector('[data-testid="shared-board"]')
          ?.getAttribute("data-canvas-ready") === "true" &&
        container.querySelector('[data-letter-source="a"]') !== null,
      5000,
      "Board should be ready before trying a short drawer drag",
    );

    const source = container.querySelector('[data-letter-source="a"]') as HTMLElement;
    const start = centerOf(source);
    const shortDrag = { x: start.x + 48, y: start.y + 18 };

    await act(async () => {
      dispatchPointer(source, "pointerdown", start);
      dispatchPointer(window, "pointermove", shortDrag);
      dispatchPointer(window, "pointerup", shortDrag);
    });

    await waitFor(
      () => container.querySelector('[data-testid="drawer-hint"]')?.textContent !== undefined,
      1000,
      "Short abandoned drawer drag should show the hint",
    );

    expect(container.querySelector('[data-testid="drawer-hint"]')?.textContent).toBe(
      "Drag letters onto the canvas",
    );
    expect(container.querySelectorAll('[data-testid="canvas-letter"]').length).toBe(0);
  });

  it("shows a local source preview until the drag reaches the canvas preview", async () => {
    window.history.replaceState({}, "", "/");

    const container = await mountApp({
      driver: { type: "persistent", dbName: uniqueDbName("local-source-preview") },
    });

    await waitFor(
      () =>
        container
          .querySelector('[data-testid="shared-board"]')
          ?.getAttribute("data-canvas-ready") === "true" &&
        container.querySelector('[data-letter-source="a"]') !== null,
      5000,
      "Board should be ready before dragging a drawer source",
    );

    const board = container.querySelector('[data-testid="shared-board"]') as HTMLElement;
    const source = container.querySelector('[data-letter-source="a"]') as HTMLElement;
    const start = centerOf(source);
    const preCanvasPoint = { x: start.x + 48, y: start.y + 18 };
    const boardPointForSharedPreview = boardPoint(board, 0.42, 0.36);

    await act(async () => {
      dispatchPointer(source, "pointerdown", start);
      dispatchPointer(window, "pointermove", preCanvasPoint);
    });

    await waitFor(
      () => container.querySelector('[data-testid="drawer-letter-preview"]') !== null,
      1000,
      "A local drawer preview should appear before the drag reaches the board",
    );

    const localPreview = container.querySelector(
      '[data-testid="drawer-letter-preview"]',
    ) as HTMLElement;
    expect(localPreview.textContent).toBe("a");
    expect(getComputedStyle(localPreview).position).toBe("fixed");
    expect(getComputedStyle(localPreview).pointerEvents).toBe("none");
    expect(container.querySelectorAll('[data-testid="canvas-letter-pending"]').length).toBe(0);

    await act(async () => {
      dispatchPointer(window, "pointermove", boardPointForSharedPreview);
    });

    await waitFor(
      () =>
        container.querySelector('[data-testid="drawer-letter-preview"]') === null &&
        container.querySelectorAll('[data-testid="canvas-letter-pending"]').length === 1,
      3000,
      "The shared canvas preview should take over once the drag reaches the board",
    );

    await act(async () => {
      dispatchPointer(window, "pointercancel", boardPointForSharedPreview);
    });
  });

  it("clears a pending dragged letter when the drag is cancelled", async () => {
    window.history.replaceState({}, "", "/");

    const container = await mountApp({
      driver: { type: "persistent", dbName: uniqueDbName("cancel-new-letter") },
    });

    await waitFor(
      () =>
        container
          .querySelector('[data-testid="shared-board"]')
          ?.getAttribute("data-canvas-ready") === "true" &&
        container.querySelector('[data-letter-source="a"]') !== null,
      5000,
      "Board should be ready before starting the drag",
    );

    const board = container.querySelector('[data-testid="shared-board"]') as HTMLElement;
    const source = container.querySelector('[data-letter-source="a"]') as HTMLElement;
    const dragPoint = boardPoint(board, 0.48, 0.4);

    await act(async () => {
      dispatchPointer(source, "pointerdown", centerOf(source));
      dispatchPointer(window, "pointermove", dragPoint);
    });

    await waitFor(
      () => container.querySelectorAll('[data-testid="canvas-letter-pending"]').length === 1,
      3000,
      "Pending letter preview should appear during the drag",
    );

    await act(async () => {
      dispatchPointer(window, "pointercancel", dragPoint);
    });

    await waitFor(
      () => container.querySelectorAll('[data-testid="canvas-letter-pending"]').length === 0,
      3000,
      "Pending letter preview should clear after cancellation",
    );
  });

  it("clears stale cursor drag previews when rejoining an existing canvas", async () => {
    window.history.replaceState({}, "", "/");

    const secret = await testSecret(`stale-preview-${Date.now()}`);
    const dbName = uniqueDbName("stale-preview");
    const firstContainer = await mountApp({
      secret,
      driver: { type: "persistent", dbName },
    });

    await waitFor(
      () =>
        firstContainer
          .querySelector('[data-testid="shared-board"]')
          ?.getAttribute("data-canvas-ready") === "true" && currentCanvasId() !== null,
      5000,
      "First session should join the board",
    );

    const board = firstContainer.querySelector('[data-testid="shared-board"]') as HTMLElement;
    const source = firstContainer.querySelector('[data-letter-source="s"]') as HTMLElement;
    const dragPoint = boardPoint(board, 0.36, 0.42);

    await act(async () => {
      dispatchPointer(source, "pointerdown", centerOf(source));
      dispatchPointer(window, "pointermove", dragPoint);
    });

    await waitFor(
      () => firstContainer.querySelectorAll('[data-testid="canvas-letter-pending"]').length === 1,
      3000,
      "The first session should show a pending preview before closing",
    );

    await unmountContainer(firstContainer);

    const secondContainer = await mountApp({
      secret,
      driver: { type: "persistent", dbName },
    });

    await waitFor(
      () =>
        secondContainer
          .querySelector('[data-testid="shared-board"]')
          ?.getAttribute("data-canvas-ready") === "true",
      5000,
      "Rejoined session should become ready",
    );

    await waitFor(
      () => secondContainer.querySelectorAll('[data-testid="canvas-letter-pending"]').length === 0,
      3000,
      "Rejoined session should clear any stale pending preview",
    );
  });

  it("shows pending dragged letters and hand cursors to collaborators before drop", async () => {
    const serverUrl = `http://127.0.0.1:${TEST_PORT}`;

    window.history.replaceState({}, "", "/");

    const aliceContainer = await mountApp({
      appId: APP_ID,
      serverUrl,
      secret: await testSecret(`pending-alice-${Date.now()}`),
      driver: { type: "persistent", dbName: uniqueDbName("pending-alice") },
    });

    await waitFor(
      () => currentCanvasId() !== null,
      SERVER_SYNC_TIMEOUT_MS,
      "Alice should create the shared canvas before Bob joins",
    );

    const bobContainer = await mountApp({
      appId: APP_ID,
      serverUrl,
      secret: await testSecret(`pending-bob-${Date.now()}`),
      driver: { type: "persistent", dbName: uniqueDbName("pending-bob") },
    });

    await waitFor(
      () =>
        aliceContainer.querySelector('[data-testid="shared-board"]') !== null &&
        bobContainer.querySelector('[data-testid="shared-board"]') !== null &&
        aliceContainer.querySelector('[data-letter-source="d"]') !== null &&
        aliceContainer
          .querySelector('[data-testid="shared-board"]')
          ?.getAttribute("data-canvas-ready") === "true" &&
        bobContainer.querySelectorAll('[data-testid="cursor-marker"]').length >= 2,
      SERVER_SYNC_TIMEOUT_MS,
      "Both boards, Alice's drawer, and both cursor markers should render",
    );

    const aliceBoard = aliceContainer.querySelector('[data-testid="shared-board"]') as HTMLElement;
    const aliceSource = aliceContainer.querySelector('[data-letter-source="d"]') as HTMLElement;
    const dragPoint = boardPoint(aliceBoard, 0.34, 0.38);

    await act(async () => {
      dispatchPointer(aliceSource, "pointerdown", centerOf(aliceSource));
      dispatchPointer(window, "pointermove", dragPoint);
    });

    await waitFor(
      () => bobContainer.querySelectorAll('[data-testid="canvas-letter-pending"]').length === 1,
      SERVER_SYNC_TIMEOUT_MS,
      "Bob should see Alice's pending letter while she is still dragging",
    );

    await waitFor(
      () =>
        [...bobContainer.querySelectorAll('[data-testid="cursor-marker"]')].some(
          (cursor) => cursor.getAttribute("data-cursor-icon") === "hand",
        ),
      SERVER_SYNC_TIMEOUT_MS,
      "Bob should see Alice's cursor switch to the hand icon over the letter",
    );

    await act(async () => {
      dispatchPointer(window, "pointerup", dragPoint);
    });

    await waitFor(
      () => bobContainer.querySelectorAll('[data-testid="canvas-letter"]').length === 1,
      SERVER_SYNC_TIMEOUT_MS,
      "Bob should see the dragged letter become ready after the drop",
    );

    expect(bobContainer.querySelectorAll('[data-testid="canvas-letter-pending"]').length).toBe(0);
  });

  it("hides collaborator cursors after they have been stationary for too long", async () => {
    const serverUrl = `http://127.0.0.1:${TEST_PORT}`;

    window.history.replaceState({}, "", "/");

    const aliceContainer = await mountApp({
      appId: APP_ID,
      serverUrl,
      secret: await testSecret(`idle-cursor-alice-${Date.now()}`),
      driver: { type: "persistent", dbName: uniqueDbName("idle-cursor-alice") },
    });

    await waitFor(
      () => currentCanvasId() !== null,
      SERVER_SYNC_TIMEOUT_MS,
      "Alice should create the shared canvas before Bob joins",
    );

    const bobContainer = await mountApp({
      appId: APP_ID,
      serverUrl,
      secret: await testSecret(`idle-cursor-bob-${Date.now()}`),
      driver: { type: "persistent", dbName: uniqueDbName("idle-cursor-bob") },
    });

    await waitFor(
      () =>
        aliceContainer
          .querySelector('[data-testid="shared-board"]')
          ?.getAttribute("data-canvas-ready") === "true" &&
        bobContainer.querySelectorAll('[data-testid="cursor-marker"]').length >= 2,
      SERVER_SYNC_TIMEOUT_MS,
      "Both sessions should show both cursors before going idle",
    );

    const aliceBoard = aliceContainer.querySelector('[data-testid="shared-board"]') as HTMLElement;
    const aliceMovePoint = boardPoint(aliceBoard, 0.62, 0.31);

    await act(async () => {
      dispatchPointer(aliceBoard, "pointermove", aliceMovePoint);
    });

    await waitFor(
      () => bobContainer.querySelectorAll('[data-testid="cursor-marker"]').length >= 2,
      SERVER_SYNC_TIMEOUT_MS,
      "Bob should still see Alice right after her move",
    );

    await waitFor(
      () => bobContainer.querySelectorAll('[data-testid="cursor-marker"]').length === 1,
      5000,
      "Bob should hide Alice after she has been idle for too long",
    );
  });

  it("loads the canvas from the URL and keeps canvases isolated from each other", async () => {
    const serverUrl = `http://127.0.0.1:${TEST_PORT}`;

    window.history.replaceState({}, "", "/");

    const firstCanvasContainer = await mountApp({
      appId: APP_ID,
      serverUrl,
      secret: await testSecret(`letters-canvas-a-${Date.now()}`),
      driver: { type: "persistent", dbName: uniqueDbName("canvas-a") },
    });

    await waitFor(
      () => currentCanvasId() !== null,
      SERVER_SYNC_TIMEOUT_MS,
      "First canvas id should be created",
    );
    const canvasA = currentCanvasId() as string;

    await waitFor(
      () =>
        firstCanvasContainer
          .querySelector('[data-testid="shared-board"]')
          ?.getAttribute("data-canvas-ready") === "true",
      SERVER_SYNC_TIMEOUT_MS,
      "Canvas A should become write-ready before dragging",
    );

    const firstBoard = firstCanvasContainer.querySelector(
      '[data-testid="shared-board"]',
    ) as HTMLElement;
    const firstSource = firstCanvasContainer.querySelector(
      '[data-letter-source="c"]',
    ) as HTMLElement;

    await act(async () => {
      dispatchPointer(firstSource, "pointerdown", centerOf(firstSource));
      dispatchPointer(window, "pointermove", boardPoint(firstBoard, 0.4, 0.45));
      dispatchPointer(window, "pointerup", boardPoint(firstBoard, 0.4, 0.45));
    });

    await waitFor(
      () => firstCanvasContainer.querySelectorAll('[data-testid="canvas-letter"]').length === 1,
      SERVER_SYNC_TIMEOUT_MS,
      "Canvas A should contain its spawned letter",
    );

    window.history.replaceState({}, "", "/");

    const secondCanvasContainer = await mountApp({
      appId: APP_ID,
      serverUrl,
      secret: await testSecret(`letters-canvas-b-${Date.now()}`),
      driver: { type: "persistent", dbName: uniqueDbName("canvas-b") },
    });

    await waitFor(
      () => currentCanvasId() !== null,
      SERVER_SYNC_TIMEOUT_MS,
      "Second canvas id should be created",
    );
    const canvasB = currentCanvasId() as string;

    expect(canvasB).not.toBe(canvasA);
    expect(secondCanvasContainer.querySelectorAll('[data-testid="canvas-letter"]').length).toBe(0);

    window.history.replaceState({}, "", `/?canvas=${canvasA}`);

    const revisitCanvasA = await mountApp({
      appId: APP_ID,
      serverUrl,
      secret: await testSecret(`letters-canvas-a-revisit-${Date.now()}`),
      driver: { type: "persistent", dbName: uniqueDbName("canvas-a-revisit") },
    });

    await waitFor(
      () => revisitCanvasA.querySelectorAll('[data-testid="canvas-letter"]').length === 1,
      SERVER_SYNC_TIMEOUT_MS,
      "Revisiting canvas A should load its existing letter",
    );

    expect(revisitCanvasA.querySelector('[data-testid="canvas-letter"]')?.textContent).toBe("c");

    window.history.replaceState({}, "", `/?canvas=${canvasB}`);

    const revisitCanvasB = await mountApp({
      appId: APP_ID,
      serverUrl,
      secret: await testSecret(`letters-canvas-b-revisit-${Date.now()}`),
      driver: { type: "persistent", dbName: uniqueDbName("canvas-b-revisit") },
    });

    await waitFor(
      () => revisitCanvasB.querySelector('[data-testid="shared-board"]') !== null,
      SERVER_SYNC_TIMEOUT_MS,
      "Revisiting canvas B should render the board",
    );

    expect(revisitCanvasB.querySelectorAll('[data-testid="canvas-letter"]').length).toBe(0);
  });

  it("syncs letters and colored cursors across sessions on the same canvas", async () => {
    const serverUrl = `http://127.0.0.1:${TEST_PORT}`;

    window.history.replaceState({}, "", "/");

    const aliceContainer = await mountApp({
      appId: APP_ID,
      serverUrl,
      secret: await testSecret(`letters-alice-${Date.now()}`),
      driver: { type: "persistent", dbName: uniqueDbName("alice") },
    });

    await waitFor(
      () => currentCanvasId() !== null,
      SERVER_SYNC_TIMEOUT_MS,
      "Alice should create a shared canvas before Bob joins",
    );

    const bobContainer = await mountApp({
      appId: APP_ID,
      serverUrl,
      secret: await testSecret(`letters-bob-${Date.now()}`),
      driver: { type: "persistent", dbName: uniqueDbName("bob") },
    });

    await waitFor(
      () =>
        aliceContainer.querySelector('[data-testid="shared-board"]') !== null &&
        bobContainer.querySelector('[data-testid="shared-board"]') !== null &&
        aliceContainer
          .querySelector('[data-testid="shared-board"]')
          ?.getAttribute("data-canvas-ready") === "true",
      SERVER_SYNC_TIMEOUT_MS,
      "Both boards should render",
    );

    const aliceBoard = aliceContainer.querySelector('[data-testid="shared-board"]') as HTMLElement;
    const aliceSource = aliceContainer.querySelector('[data-letter-source="b"]') as HTMLElement;
    const bobBoard = bobContainer.querySelector('[data-testid="shared-board"]') as HTMLElement;

    const spawnStart = centerOf(aliceSource);
    const spawnDrop = boardPoint(aliceBoard, 0.28, 0.42);

    await act(async () => {
      dispatchPointer(aliceSource, "pointerdown", spawnStart);
      dispatchPointer(window, "pointermove", spawnDrop);
      dispatchPointer(window, "pointerup", spawnDrop);
    });

    await waitFor(
      () => bobContainer.querySelectorAll('[data-testid="canvas-letter"]').length === 1,
      SERVER_SYNC_TIMEOUT_MS,
      "Bob should receive Alice's spawned letter",
    );

    const bobLetter = bobContainer.querySelector('[data-testid="canvas-letter"]') as HTMLElement;
    const moveTo = boardPoint(bobBoard, 0.72, 0.66);

    await act(async () => {
      dispatchPointer(bobLetter, "pointerdown", centerOf(bobLetter));
      dispatchPointer(window, "pointermove", moveTo);
      dispatchPointer(window, "pointerup", moveTo);
    });

    await waitFor(
      () => {
        const aliceLetter = aliceContainer.querySelector('[data-testid="canvas-letter"]');
        if (!aliceLetter) return false;
        const { left, top } = parseLetterPosition(aliceLetter);
        return left > 60 && top > 55;
      },
      SERVER_SYNC_TIMEOUT_MS,
      "Alice should see Bob's letter movement",
    );

    const aliceCursorPoint = boardPoint(aliceBoard, 0.18, 0.18);
    const bobCursorPoint = boardPoint(bobBoard, 0.82, 0.2);

    await act(async () => {
      dispatchPointer(aliceBoard, "pointermove", aliceCursorPoint);
      dispatchPointer(bobBoard, "pointermove", bobCursorPoint);
    });

    await waitFor(
      () =>
        aliceContainer.querySelectorAll('[data-testid="cursor-marker"]').length >= 2 &&
        bobContainer.querySelectorAll('[data-testid="cursor-marker"]').length >= 2,
      SERVER_SYNC_TIMEOUT_MS,
      "Both sessions should see both cursor markers",
    );

    const aliceCursors = [...aliceContainer.querySelectorAll('[data-testid="cursor-marker"]')];
    const bobCursors = [...bobContainer.querySelectorAll('[data-testid="cursor-marker"]')];

    expect(aliceCursors.every((cursor) => cursor.getAttribute("data-cursor-color"))).toBe(true);
    expect(bobCursors.every((cursor) => cursor.getAttribute("data-cursor-color"))).toBe(true);
  });
});
