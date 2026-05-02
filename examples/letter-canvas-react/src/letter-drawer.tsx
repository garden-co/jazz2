import type * as React from "react";
import { LETTER_SOURCE_ROWS } from "./constants.js";
import { drawerOffset, formatLetterGlyph } from "./format.js";

export function LetterDrawer({
  interactionReady,
  hintVisible,
  onDrawerPointerDown,
  onSourcePointerDown,
}: {
  interactionReady: boolean;
  hintVisible: boolean;
  onDrawerPointerDown: (event: React.PointerEvent<HTMLElement>) => void;
  onSourcePointerDown: (value: string, event: React.PointerEvent<HTMLButtonElement>) => void;
}) {
  return (
    <section data-testid="letter-drawer" className="drawer" onPointerDown={onDrawerPointerDown}>
      {hintVisible ? (
        <div data-testid="drawer-hint" className="drawer-hint" role="status">
          Drag letters onto the canvas
        </div>
      ) : null}
      <div className="drawer-grid">
        {LETTER_SOURCE_ROWS.map((row, rowIndex) => (
          <div
            key={`drawer-row-${rowIndex}`}
            data-testid="drawer-row"
            data-drawer-row={row.join("")}
            className="drawer-row"
            style={{ ["--drawer-offset" as string]: `${drawerOffset(rowIndex)}px` }}
          >
            {row.map((value) => (
              <button
                key={value}
                type="button"
                data-letter-source={value}
                data-source-ready={interactionReady ? "true" : "false"}
                className="drawer-tile"
                disabled={!interactionReady}
                onPointerDown={(event) => onSourcePointerDown(value, event)}
              >
                {formatLetterGlyph(value)}
              </button>
            ))}
          </div>
        ))}
      </div>
    </section>
  );
}
