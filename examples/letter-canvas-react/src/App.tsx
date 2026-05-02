import * as React from "react";
import { JazzProvider, useAll, useDb, useLocalFirstAuth, useSession } from "jazz-tools/react";
import type { DbConfig } from "jazz-tools";
import { QRCodeSVG } from "qrcode.react";
import { app, type Letter } from "../schema.js";
import { BoardScene, type PendingLetterPreview } from "./board-scene.js";
import {
  appId,
  BOARD_CENTER,
  BOARD_HEIGHT,
  BOARD_WIDTH,
  CURSOR_MOTION_SETTINGS,
  CURSOR_VISIBILITY_TICK_MS,
  DRAWER_TAP_DISTANCE_PX,
  DRAWER_TAP_HINT_MS,
  INPUT_WRITE_INTERVAL_MS,
  LETTER_HALF_WIDTH,
  LETTER_MOTION_SETTINGS,
  LOCATION_QR_SIZE,
  serverUrl,
} from "./constants.js";
import { createCursorWriteGate, type CursorPresence } from "./cursor-gate.js";
import { cursorShouldRender } from "./cursor-visibility.js";
import { formatLetterGlyph } from "./format.js";
import {
  clamp,
  clientPointToClampedLogical,
  clientPointToLogical,
  pointOverLetter,
  type Point,
} from "./geometry.js";
import { LetterDrawer } from "./letter-drawer.js";
import { useSmoothedPositions } from "./motion.js";
import { cursorHasActiveDraftPreview } from "./pending-draft.js";
import { createRateLimiter } from "./rate-limit.js";
import { readCanvasIdFromUrl, writeCanvasIdToUrl } from "./url-state.js";
import { useForwardArrowKeysToParent } from "./use-forward-arrow-keys-to-parent.js";

function defaultConfig(secret: string, overrides: Partial<DbConfig> = {}): DbConfig {
  return {
    appId,
    env: "dev",
    userBranch: "main",
    serverUrl,
    secret,
    ...overrides,
  };
}

type AppProps = {
  config?: Partial<DbConfig>;
  fallback?: React.ReactNode;
};

export function App({ config, fallback }: AppProps = {}) {
  useForwardArrowKeysToParent();

  const { secret, isLoading } = useLocalFirstAuth();

  if (isLoading || !secret) {
    return <>{fallback ?? <p>Loading...</p>}</>;
  }

  const resolvedConfig = defaultConfig(secret, config);

  return (
    <JazzProvider config={resolvedConfig} fallback={fallback ?? <p>Loading...</p>}>
      <LetterCanvas config={resolvedConfig} />
    </JazzProvider>
  );
}

function LetterCanvas({ config }: { config: DbConfig }) {
  const db = useDb();
  const session = useSession();
  const sharedWriteTier: "edge" | "local" = config.serverUrl ? "edge" : "local";
  const [activeCanvasId, setActiveCanvasId] = React.useState<string | null>(() =>
    readCanvasIdFromUrl(),
  );
  const [canvasBootstrapError, setCanvasBootstrapError] = React.useState<string | null>(null);
  const [writeReadyCanvasId, setWriteReadyCanvasId] = React.useState<string | null>(null);
  const canvasRows =
    useAll(activeCanvasId ? app.canvases.where({ id: activeCanvasId }).limit(1) : undefined) ?? [];
  const canvasReady = canvasRows.length > 0;
  const canvasWriteReady = activeCanvasId !== null && writeReadyCanvasId === activeCanvasId;
  const letters = (
    useAll(activeCanvasId ? app.letters.where({ canvasId: activeCanvasId }) : undefined) ?? []
  )
    .slice()
    .sort((a, b) => a.id.localeCompare(b.id));
  const cursors = (
    useAll(
      activeCanvasId
        ? app.cursors.where({ canvasId: activeCanvasId }).select("*", "$updatedAt")
        : undefined,
    ) ?? []
  )
    .slice()
    .sort((a, b) => a.user_id.localeCompare(b.user_id));
  const boardRef = React.useRef<HTMLDivElement>(null);
  const dragInProgressRef = React.useRef(false);
  const sessionUserId = session?.user_id ?? null;
  const ownCursor = sessionUserId
    ? (cursors.find((cursor) => cursor.user_id === sessionUserId) ?? null)
    : null;
  const [localCursorPreview, setLocalCursorPreview] = React.useState<CursorPresence | null>(null);
  const [localLetterPreview, setLocalLetterPreview] = React.useState<{
    letterId: string;
    x: number;
    y: number;
  } | null>(null);
  const [localSourcePreview, setLocalSourcePreview] = React.useState<{
    value: string;
    clientX: number;
    clientY: number;
  } | null>(null);
  const [drawerHintVisible, setDrawerHintVisible] = React.useState(false);
  const [cursorVisibilityNowMs, setCursorVisibilityNowMs] = React.useState(() => Date.now());
  const interactionReady = canvasWriteReady && sessionUserId !== null && ownCursor !== null;
  const cursorUpdateLimiterRef = React.useRef<ReturnType<
    typeof createRateLimiter<CursorPresence & { cursorId: string }>
  > | null>(null);
  const letterMoveLimiterRef = React.useRef<ReturnType<
    typeof createRateLimiter<{ letterId: string; x: number; y: number }>
  > | null>(null);
  const cursorWriteGateRef = React.useRef(createCursorWriteGate());
  const isMountedRef = React.useRef(true);
  const canvasBootstrapInFlightRef = React.useRef(false);
  const pendingCanvasBootstrapIdRef = React.useRef<string | null>(null);
  const activeDragCleanupRef = React.useRef<(() => void) | null>(null);
  const drawerHintTimerRef = React.useRef<ReturnType<typeof setTimeout> | null>(null);
  const pendingCursorCreateKeyRef = React.useRef<string | null>(null);
  const pendingCursorCreateTokenRef = React.useRef(0);
  const ownCursorId = ownCursor?.id ?? null;

  React.useEffect(() => {
    return () => {
      isMountedRef.current = false;
      pendingCursorCreateKeyRef.current = null;
      pendingCursorCreateTokenRef.current += 1;
      activeDragCleanupRef.current?.();
      if (drawerHintTimerRef.current !== null) {
        clearTimeout(drawerHintTimerRef.current);
      }
    };
  }, []);

  React.useEffect(() => {
    if (activeCanvasId || canvasBootstrapInFlightRef.current) {
      return;
    }

    setCanvasBootstrapError(null);
    canvasBootstrapInFlightRef.current = true;

    try {
      const pendingCanvas = db.insert(app.canvases, {
        created_at: new Date(),
      });
      const canvas = pendingCanvas.value;
      pendingCanvasBootstrapIdRef.current = canvas.id;
      writeCanvasIdToUrl(canvas.id);
      setActiveCanvasId(canvas.id);

      void pendingCanvas
        .wait({ tier: sharedWriteTier })
        .then(() => {
          if (!isMountedRef.current) {
            return;
          }

          canvasBootstrapInFlightRef.current = false;
          if (pendingCanvasBootstrapIdRef.current === canvas.id) {
            pendingCanvasBootstrapIdRef.current = null;
          }
          setCanvasBootstrapError(null);
          setWriteReadyCanvasId(canvas.id);
        })
        .catch(() => {
          if (!isMountedRef.current) {
            return;
          }

          canvasBootstrapInFlightRef.current = false;
          if (pendingCanvasBootstrapIdRef.current === canvas.id) {
            pendingCanvasBootstrapIdRef.current = null;
          }
          setCanvasBootstrapError("Could not finish setting up the shared canvas.");
        });
    } catch {
      canvasBootstrapInFlightRef.current = false;
      setCanvasBootstrapError("Could not create a shared canvas.");
    }
  }, [activeCanvasId, db, sharedWriteTier]);

  React.useEffect(() => {
    if (!activeCanvasId || !canvasReady) {
      return;
    }

    if (pendingCanvasBootstrapIdRef.current === activeCanvasId) {
      return;
    }

    setWriteReadyCanvasId(activeCanvasId);
  }, [activeCanvasId, canvasReady]);

  React.useEffect(() => {
    activeDragCleanupRef.current?.();
    setLocalCursorPreview(null);
    setLocalLetterPreview(null);
    setLocalSourcePreview(null);
  }, [activeCanvasId, sessionUserId]);

  React.useEffect(() => {
    if (!sessionUserId || !activeCanvasId || !canvasWriteReady) {
      pendingCursorCreateKeyRef.current = null;
      pendingCursorCreateTokenRef.current += 1;
      cursorWriteGateRef.current.reset();
      return;
    }

    const cursorCreateKey = `${activeCanvasId}:${sessionUserId}`;

    if (ownCursorId) {
      const replay = cursorWriteGateRef.current.adoptExisting(ownCursorId);
      if (replay) {
        cursorUpdateLimiterRef.current?.schedule(replay);
        cursorUpdateLimiterRef.current?.flush();
      }
      return;
    }

    if (pendingCursorCreateKeyRef.current === cursorCreateKey) {
      return;
    }

    pendingCursorCreateKeyRef.current = cursorCreateKey;
    const createToken = pendingCursorCreateTokenRef.current + 1;
    pendingCursorCreateTokenRef.current = createToken;
    cursorWriteGateRef.current.beginCreate();

    try {
      const pendingCursor = db.insert(app.cursors, {
        canvasId: activeCanvasId,
        user_id: sessionUserId,
        x: BOARD_CENTER.x,
        y: BOARD_CENTER.y,
        gesture: "pointer",
        drag_value: null,
        drag_x: null,
        drag_y: null,
      });

      void pendingCursor
        .wait({ tier: sharedWriteTier })
        .then((cursor) => {
          if (
            pendingCursorCreateTokenRef.current !== createToken ||
            pendingCursorCreateKeyRef.current !== cursorCreateKey
          ) {
            return;
          }

          pendingCursorCreateKeyRef.current = null;
          const replay = cursorWriteGateRef.current.markReady(cursor.id);
          if (replay) {
            cursorUpdateLimiterRef.current?.schedule(replay);
            cursorUpdateLimiterRef.current?.flush();
          }
        })
        .catch(() => {
          if (
            pendingCursorCreateTokenRef.current !== createToken ||
            pendingCursorCreateKeyRef.current !== cursorCreateKey
          ) {
            return;
          }
          pendingCursorCreateKeyRef.current = null;
          cursorWriteGateRef.current.reset();
        });
    } catch {
      pendingCursorCreateKeyRef.current = null;
      cursorWriteGateRef.current.reset();
    }
  }, [activeCanvasId, canvasWriteReady, db, ownCursorId, sessionUserId, sharedWriteTier]);

  const sendCursorUpdate = React.useEffectEvent(
    ({
      cursorId,
      x,
      y,
      gesture,
      dragValue,
      dragX,
      dragY,
    }: CursorPresence & { cursorId: string }) => {
      db.update(app.cursors, cursorId, {
        x,
        y,
        gesture,
        drag_value: dragValue,
        drag_x: dragX,
        drag_y: dragY,
      });
    },
  );

  const sendLetterMove = React.useEffectEvent(
    ({ letterId, x, y }: { letterId: string; x: number; y: number }) => {
      db.update(app.letters, letterId, { x, y });
    },
  );

  if (cursorUpdateLimiterRef.current === null) {
    cursorUpdateLimiterRef.current = createRateLimiter(INPUT_WRITE_INTERVAL_MS, (value) => {
      sendCursorUpdate(value);
    });
  }

  if (letterMoveLimiterRef.current === null) {
    letterMoveLimiterRef.current = createRateLimiter(INPUT_WRITE_INTERVAL_MS, (value) => {
      sendLetterMove(value);
    });
  }

  React.useEffect(() => {
    if (!localLetterPreview) {
      return;
    }

    const syncedLetter = letters.find((letter) => letter.id === localLetterPreview.letterId);
    if (!syncedLetter) {
      setLocalLetterPreview(null);
      return;
    }

    if (
      Math.abs(syncedLetter.x - localLetterPreview.x) <= 0.5 &&
      Math.abs(syncedLetter.y - localLetterPreview.y) <= 0.5
    ) {
      setLocalLetterPreview(null);
    }
  }, [letters, localLetterPreview]);

  React.useEffect(() => {
    return () => {
      activeDragCleanupRef.current?.();
      cursorUpdateLimiterRef.current?.cancel();
      letterMoveLimiterRef.current?.cancel();
    };
  }, []);

  React.useEffect(() => {
    const intervalId = window.setInterval(() => {
      setCursorVisibilityNowMs(Date.now());
    }, CURSOR_VISIBILITY_TICK_MS);

    return () => {
      window.clearInterval(intervalId);
    };
  }, []);

  const ownCursorShouldHoldHand =
    ownCursor !== null ? pointOverLetter({ x: ownCursor.x, y: ownCursor.y }, letters) : false;

  React.useEffect(() => {
    if (!canvasWriteReady || !ownCursor || localCursorPreview) {
      return;
    }

    if (
      ownCursor.drag_value === null &&
      ownCursor.drag_x === null &&
      ownCursor.drag_y === null &&
      ownCursor.gesture === (ownCursorShouldHoldHand ? "hand" : "pointer")
    ) {
      return;
    }

    const normalizedPresence: CursorPresence = {
      x: ownCursor.x,
      y: ownCursor.y,
      gesture: ownCursorShouldHoldHand ? "hand" : "pointer",
      dragValue: null,
      dragX: null,
      dragY: null,
    };

    commitCursorPresence(normalizedPresence);
  }, [
    canvasWriteReady,
    localCursorPreview,
    ownCursor?.drag_value,
    ownCursor?.drag_x,
    ownCursor?.drag_y,
    ownCursor?.gesture,
    ownCursor?.x,
    ownCursor?.y,
    ownCursorShouldHoldHand,
  ]);

  const renderedCursors = cursors.map((cursor) =>
    cursor.user_id === sessionUserId && localCursorPreview
      ? {
          ...cursor,
          x: localCursorPreview.x,
          y: localCursorPreview.y,
          gesture: localCursorPreview.gesture,
          drag_value: localCursorPreview.dragValue,
          drag_x: localCursorPreview.dragX,
          drag_y: localCursorPreview.dragY,
        }
      : cursor,
  );
  const visibleCursors = renderedCursors.filter((cursor) =>
    cursorShouldRender(cursor, sessionUserId, cursorVisibilityNowMs),
  );
  const cursorPositions = useSmoothedPositions(
    visibleCursors.map((cursor) => ({
      id: cursor.id,
      x: cursor.x,
      y: cursor.y,
      immediate: cursor.user_id === sessionUserId,
      predictive: cursor.user_id !== sessionUserId,
    })),
    CURSOR_MOTION_SETTINGS,
  );
  const renderedLetters = letters.map((letter) =>
    localLetterPreview?.letterId === letter.id
      ? {
          ...letter,
          x: localLetterPreview.x,
          y: localLetterPreview.y,
        }
      : letter,
  );
  const letterPositions = useSmoothedPositions(
    renderedLetters.map((letter) => ({
      id: letter.id,
      x: letter.x,
      y: letter.y,
      immediate: localLetterPreview?.letterId === letter.id,
      predictive: localLetterPreview?.letterId !== letter.id,
    })),
    LETTER_MOTION_SETTINGS,
  );
  const pendingLetters: PendingLetterPreview[] = visibleCursors.flatMap((cursor) => {
    if (!cursorHasActiveDraftPreview(cursor)) {
      return [];
    }

    return [
      {
        id: `draft-${cursor.id}`,
        sourceCursorId: cursor.id,
        value: cursor.drag_value!,
        x: cursor.drag_x!,
        y: cursor.drag_y!,
      },
    ];
  });

  const publishCursorPresence = React.useEffectEvent(
    (
      point: { x: number; y: number } | null,
      options: {
        dragValue?: string | null;
        dragPoint?: { x: number; y: number } | null;
        gesture?: "pointer" | "hand";
      } = {},
    ) => {
      if (!point) {
        return null;
      }

      const presence = createCursorPresence(point, letters, options);
      setLocalCursorPreview(presence);
      const nextUpdate = cursorWriteGateRef.current.notePresence(presence);
      if (nextUpdate) {
        cursorUpdateLimiterRef.current?.schedule(nextUpdate);
      }
      return point;
    },
  );

  const commitCursorPresence = React.useEffectEvent((presence: CursorPresence) => {
    setLocalCursorPreview(presence);
    cursorUpdateLimiterRef.current?.cancel();

    if (ownCursorId && pendingCursorCreateKeyRef.current === null) {
      sendCursorUpdate({
        cursorId: ownCursorId,
        ...presence,
      });
      return;
    }

    const nextUpdate = cursorWriteGateRef.current.notePresence(presence);
    if (nextUpdate) {
      cursorUpdateLimiterRef.current?.schedule(nextUpdate);
      cursorUpdateLimiterRef.current?.flush();
    }
  });

  const commitCursorPresenceAtPoint = React.useEffectEvent(
    (
      point: { x: number; y: number } | null,
      options: {
        dragValue?: string | null;
        dragPoint?: { x: number; y: number } | null;
        gesture?: "pointer" | "hand";
      } = {},
    ) => {
      if (!point) {
        return null;
      }

      const presence = createCursorPresence(point, letters, options);
      commitCursorPresence(presence);
      return point;
    },
  );

  const showDrawerTapHint = React.useEffectEvent(() => {
    if (drawerHintTimerRef.current !== null) {
      clearTimeout(drawerHintTimerRef.current);
    }

    setDrawerHintVisible(true);
    drawerHintTimerRef.current = setTimeout(() => {
      drawerHintTimerRef.current = null;
      setDrawerHintVisible(false);
    }, DRAWER_TAP_HINT_MS);
  });

  const beginDrag = React.useEffectEvent(
    (
      dragState:
        | { kind: "new"; value: string; startClientX: number; startClientY: number }
        | { kind: "existing"; value: string; letterId: string; offsetX: number; offsetY: number },
    ) => {
      activeDragCleanupRef.current?.();
      dragInProgressRef.current = true;

      if (dragState.kind === "existing") {
        letterMoveLimiterRef.current?.cancel();
      } else {
        setLocalSourcePreview({
          value: dragState.value,
          clientX: dragState.startClientX,
          clientY: dragState.startClientY,
        });
      }

      let lastCursorPoint: { x: number; y: number } | null = null;
      let sourceMovedBeyondTap = false;

      function detachDragListeners() {
        dragInProgressRef.current = false;
        if (dragState.kind === "new") {
          setLocalSourcePreview(null);
        }
        window.removeEventListener("pointermove", handleMove);
        window.removeEventListener("pointerup", handleUp);
        window.removeEventListener("pointercancel", handleCancel);
        window.removeEventListener("blur", handleCancel);
        window.removeEventListener("pagehide", handleCancel);
        document.removeEventListener("visibilitychange", handleVisibilityChange);
        if (activeDragCleanupRef.current === detachDragListeners) {
          activeDragCleanupRef.current = null;
        }
      }

      function fallbackCursorPoint() {
        if (lastCursorPoint) {
          return lastCursorPoint;
        }

        if (localCursorPreview) {
          return { x: localCursorPreview.x, y: localCursorPreview.y };
        }

        if (ownCursor) {
          return { x: ownCursor.x, y: ownCursor.y };
        }

        return null;
      }

      function noteSourceMovement(event: PointerEvent) {
        if (dragState.kind !== "new" || sourceMovedBeyondTap) {
          return;
        }

        sourceMovedBeyondTap =
          Math.hypot(
            event.clientX - dragState.startClientX,
            event.clientY - dragState.startClientY,
          ) > DRAWER_TAP_DISTANCE_PX;
      }

      function handleMove(event: PointerEvent) {
        noteSourceMovement(event);

        if (dragState.kind === "new") {
          const point = clientPointToLogical(boardRef.current, event.clientX, event.clientY);
          if (!point) {
            if (!lastCursorPoint) {
              setLocalSourcePreview({
                value: dragState.value,
                clientX: event.clientX,
                clientY: event.clientY,
              });
            }
            return;
          }

          setLocalSourcePreview(null);
          lastCursorPoint = publishCursorPresence(point, {
            gesture: "hand",
            dragValue: dragState.value,
            dragPoint: point,
          });
          return;
        }

        const point = clientPointToClampedLogical(boardRef.current, event.clientX, event.clientY);
        if (!point) {
          return;
        }

        const nextX = clamp(
          point.x - dragState.offsetX,
          LETTER_HALF_WIDTH,
          BOARD_WIDTH - LETTER_HALF_WIDTH,
        );
        const nextY = clamp(point.y - dragState.offsetY, 36, BOARD_HEIGHT - 36);
        lastCursorPoint = publishCursorPresence(point, { gesture: "hand" });
        setLocalLetterPreview({
          letterId: dragState.letterId,
          x: nextX,
          y: nextY,
        });

        letterMoveLimiterRef.current?.schedule({
          letterId: dragState.letterId,
          x: nextX,
          y: nextY,
        });
      }

      function handleCancel() {
        const point = fallbackCursorPoint();

        if (dragState.kind === "existing") {
          letterMoveLimiterRef.current?.cancel();
          setLocalLetterPreview(null);
        }

        if (point) {
          commitCursorPresenceAtPoint(point);
        }

        detachDragListeners();
      }

      function handleVisibilityChange() {
        if (document.visibilityState === "hidden") {
          handleCancel();
        }
      }

      function handleUp(event: PointerEvent) {
        noteSourceMovement(event);
        const point = clientPointToLogical(boardRef.current, event.clientX, event.clientY);

        if (dragState.kind === "new") {
          const shouldShowTapHint = !point && !lastCursorPoint && !sourceMovedBeyondTap;
          void (async () => {
            if (point && sessionUserId && activeCanvasId && canvasWriteReady) {
              db.insert(app.letters, {
                canvasId: activeCanvasId,
                value: dragState.value,
                x: point.x,
                y: point.y,
                placed_by: sessionUserId,
              });

              lastCursorPoint = commitCursorPresenceAtPoint(point);
            } else if (lastCursorPoint) {
              commitCursorPresenceAtPoint(lastCursorPoint);
            } else if (shouldShowTapHint) {
              showDrawerTapHint();
            }
          })().finally(() => {
            detachDragListeners();
          });
          return;
        }

        if (dragState.kind === "existing") {
          if (point) {
            setLocalLetterPreview({
              letterId: dragState.letterId,
              x: clamp(
                point.x - dragState.offsetX,
                LETTER_HALF_WIDTH,
                BOARD_WIDTH - LETTER_HALF_WIDTH,
              ),
              y: clamp(point.y - dragState.offsetY, 36, BOARD_HEIGHT - 36),
            });
            lastCursorPoint = commitCursorPresenceAtPoint(point, { gesture: "hand" });
          } else if (lastCursorPoint) {
            commitCursorPresenceAtPoint(lastCursorPoint);
          }
          letterMoveLimiterRef.current?.flush();
        }

        detachDragListeners();
      }

      window.addEventListener("pointermove", handleMove);
      window.addEventListener("pointerup", handleUp);
      window.addEventListener("pointercancel", handleCancel);
      window.addEventListener("blur", handleCancel);
      window.addEventListener("pagehide", handleCancel);
      document.addEventListener("visibilitychange", handleVisibilityChange);
      activeDragCleanupRef.current = detachDragListeners;
    },
  );

  function handleBoardPointerMove(event: React.PointerEvent<HTMLDivElement>) {
    if (dragInProgressRef.current) {
      return;
    }

    publishCursorPresence(clientPointToLogical(boardRef.current, event.clientX, event.clientY));
  }

  function handleSourcePointerDown(value: string, event: React.PointerEvent<HTMLButtonElement>) {
    if (!activeCanvasId || !interactionReady) {
      return;
    }

    event.preventDefault();
    beginDrag({
      kind: "new",
      value,
      startClientX: event.clientX,
      startClientY: event.clientY,
    });
  }

  function handleDrawerPointerDown(event: React.PointerEvent<HTMLElement>) {
    const target = event.target;
    if (target instanceof Element && target.closest("[data-letter-source]")) {
      return;
    }

    event.preventDefault();
  }

  function handleLetterPointerDown(letter: Letter, event: React.PointerEvent<HTMLButtonElement>) {
    if (!interactionReady) {
      return;
    }

    event.preventDefault();
    const point = clientPointToClampedLogical(boardRef.current, event.clientX, event.clientY);
    if (!point) {
      return;
    }

    beginDrag({
      kind: "existing",
      value: letter.value,
      letterId: letter.id,
      offsetX: point.x - letter.x,
      offsetY: point.y - letter.y,
    });
  }

  const boardStatusMessage = !activeCanvasId
    ? (canvasBootstrapError ?? "Creating a new shared canvas...")
    : canvasBootstrapError && !canvasWriteReady
      ? canvasBootstrapError
      : !canvasWriteReady
        ? "Finishing shared canvas setup..."
        : ownCursor
          ? null
          : "Joining this canvas...";
  const locationQrValue = window.location.href;

  return (
    <main className="app-shell">
      <div
        data-testid="location-qr"
        data-qr-value={locationQrValue}
        className="location-qr"
        aria-label={`QR code for ${locationQrValue}`}
      >
        <QRCodeSVG value={locationQrValue} size={LOCATION_QR_SIZE} marginSize={2} />
      </div>

      <section className="canvas-stack">
        <section className="canvas-panel">
          <BoardScene
            boardRef={boardRef}
            interactionReady={interactionReady}
            pendingLetters={pendingLetters}
            letters={letters}
            visibleCursors={visibleCursors}
            cursorPositions={cursorPositions}
            letterPositions={letterPositions}
            sessionUserId={sessionUserId}
            statusMessage={boardStatusMessage}
            onBoardPointerMove={handleBoardPointerMove}
            onLetterPointerDown={handleLetterPointerDown}
          />
        </section>

        {localSourcePreview ? (
          <div
            data-testid="drawer-letter-preview"
            className="drawer-letter-preview"
            style={{
              left: localSourcePreview.clientX,
              top: localSourcePreview.clientY,
            }}
          >
            {formatLetterGlyph(localSourcePreview.value)}
          </div>
        ) : null}

        <LetterDrawer
          interactionReady={interactionReady}
          hintVisible={drawerHintVisible}
          onDrawerPointerDown={handleDrawerPointerDown}
          onSourcePointerDown={handleSourcePointerDown}
        />
      </section>
    </main>
  );
}

function createCursorPresence(
  point: Point,
  letters: Letter[],
  options: {
    dragValue?: string | null;
    dragPoint?: Point | null;
    gesture?: "pointer" | "hand";
  } = {},
): CursorPresence {
  const dragValue = options.dragValue ?? null;
  const dragPoint = options.dragPoint ?? null;

  return {
    x: point.x,
    y: point.y,
    gesture: options.gesture ?? (dragValue || pointOverLetter(point, letters) ? "hand" : "pointer"),
    dragValue,
    dragX: dragPoint?.x ?? null,
    dragY: dragPoint?.y ?? null,
  };
}
