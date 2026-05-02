import type * as React from "react";
import type { Cursor, Letter } from "../schema.js";
import { BOARD_ASPECT_RATIO, BOARD_HEIGHT, BOARD_WIDTH } from "./constants.js";
import { CursorMarker } from "./cursor-marker.js";
import { formatLetterGlyph } from "./format.js";
import {
  formatPercent,
  logicalPositionStyle,
  resolveDisplayPoint,
  type Point,
} from "./geometry.js";

export type PendingLetterPreview = {
  id: string;
  sourceCursorId: string;
  value: string;
  x: number;
  y: number;
};

export function BoardScene({
  boardRef,
  interactionReady,
  pendingLetters,
  letters,
  visibleCursors,
  cursorPositions,
  letterPositions,
  sessionUserId,
  statusMessage,
  onBoardPointerMove,
  onLetterPointerDown,
}: {
  boardRef: React.RefObject<HTMLDivElement | null>;
  interactionReady: boolean;
  pendingLetters: PendingLetterPreview[];
  letters: Letter[];
  visibleCursors: Cursor[];
  cursorPositions: ReadonlyMap<string, Point>;
  letterPositions: ReadonlyMap<string, Point>;
  sessionUserId: string | null;
  statusMessage: string | null;
  onBoardPointerMove: (event: React.PointerEvent<HTMLDivElement>) => void;
  onLetterPointerDown: (letter: Letter, event: React.PointerEvent<HTMLButtonElement>) => void;
}) {
  return (
    <div
      ref={boardRef}
      data-testid="shared-board"
      data-aspect-ratio={BOARD_ASPECT_RATIO}
      data-canvas-ready={interactionReady ? "true" : "false"}
      className="shared-board"
      onPointerMove={onBoardPointerMove}
    >
      <div className="board-grid" aria-hidden="true" />

      {pendingLetters.map((letter) => {
        const position = resolveDisplayPoint(
          cursorPositions,
          letter.sourceCursorId,
          letter.x,
          letter.y,
        );

        return (
          <div
            key={letter.id}
            data-testid="canvas-letter-pending"
            data-letter-id={letter.id}
            data-letter-ready="false"
            className="canvas-letter canvas-letter-pending"
            style={logicalPositionStyle(position.x, position.y)}
          >
            {formatLetterGlyph(letter.value)}
          </div>
        );
      })}

      {letters.map((letter) => {
        const position = resolveDisplayPoint(letterPositions, letter.id, letter.x, letter.y);

        return (
          <button
            key={letter.id}
            type="button"
            data-testid="canvas-letter"
            data-letter-id={letter.id}
            data-letter-ready="true"
            data-letter-x={formatPercent((position.x / BOARD_WIDTH) * 100)}
            data-letter-y={formatPercent((position.y / BOARD_HEIGHT) * 100)}
            className="canvas-letter"
            style={logicalPositionStyle(position.x, position.y)}
            onPointerDown={(event) => onLetterPointerDown(letter, event)}
          >
            {formatLetterGlyph(letter.value)}
          </button>
        );
      })}

      {visibleCursors.map((cursor) => (
        <CursorMarker
          key={cursor.id}
          cursor={cursor}
          isMe={cursor.user_id === sessionUserId}
          iconKind={cursor.gesture}
          position={resolveDisplayPoint(cursorPositions, cursor.id, cursor.x, cursor.y)}
        />
      ))}

      {statusMessage ? (
        <div
          className="cursor-label"
          style={{
            position: "absolute",
            left: "50%",
            top: "50%",
            transform: "translate(-50%, -50%)",
          }}
        >
          {statusMessage}
        </div>
      ) : null}
    </div>
  );
}
