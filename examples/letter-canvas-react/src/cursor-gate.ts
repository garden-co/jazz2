export type CursorPresence = {
  x: number;
  y: number;
  gesture: "pointer" | "hand";
  dragValue: string | null;
  dragX: number | null;
  dragY: number | null;
};

export type CursorUpdate = CursorPresence & {
  cursorId: string;
};

type CursorGateState =
  | {
      status: "idle";
      cursorId: null;
      pendingPresence: CursorPresence | null;
    }
  | {
      status: "creating";
      cursorId: string | null;
      pendingPresence: CursorPresence | null;
    }
  | {
      status: "ready";
      cursorId: string;
      pendingPresence: CursorPresence | null;
    };

export type CursorWriteGate = {
  beginCreate: (cursorId?: string | null) => void;
  adoptExisting: (cursorId: string) => CursorUpdate | null;
  markReady: (cursorId: string) => CursorUpdate | null;
  notePresence: (presence: CursorPresence) => CursorUpdate | null;
  reset: () => void;
};

export function createCursorWriteGate(): CursorWriteGate {
  let state: CursorGateState = {
    status: "idle",
    cursorId: null,
    pendingPresence: null,
  };

  return {
    beginCreate(cursorId = null) {
      state = {
        status: "creating",
        cursorId,
        pendingPresence: state.pendingPresence,
      };
    },

    adoptExisting(cursorId) {
      if (state.status === "creating") {
        state = {
          ...state,
          cursorId,
        };
        return null;
      }

      const pendingPresence = state.pendingPresence;
      state = {
        status: "ready",
        cursorId,
        pendingPresence: null,
      };

      return pendingPresence ? { cursorId, ...pendingPresence } : null;
    },

    markReady(cursorId) {
      const pendingPresence = state.pendingPresence;
      state = {
        status: "ready",
        cursorId,
        pendingPresence: null,
      };

      return pendingPresence ? { cursorId, ...pendingPresence } : null;
    },

    notePresence(presence) {
      state = {
        ...state,
        pendingPresence: presence,
      };

      if (state.status !== "ready") {
        return null;
      }

      return {
        cursorId: state.cursorId,
        ...presence,
      };
    },

    reset() {
      state = {
        status: "idle",
        cursorId: null,
        pendingPresence: null,
      };
    },
  };
}
