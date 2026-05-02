import * as React from "react";
import { FORWARDED_ARROW_KEYS, PARENT_ARROW_KEY_MESSAGE_TYPE } from "./constants.js";

export function useForwardArrowKeysToParent() {
  React.useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      if (!FORWARDED_ARROW_KEYS.has(event.key)) {
        return;
      }

      event.preventDefault();
      window.parent.postMessage(
        {
          type: PARENT_ARROW_KEY_MESSAGE_TYPE,
          key: event.key,
          code: event.code,
          altKey: event.altKey,
          ctrlKey: event.ctrlKey,
          metaKey: event.metaKey,
          shiftKey: event.shiftKey,
          repeat: event.repeat,
        },
        "*",
      );
    };

    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, []);
}
