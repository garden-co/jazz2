type TimerApi = {
  clearTimeout: (timeoutId: number) => void;
  now: () => number;
  setTimeout: (callback: () => void, delayMs: number) => number;
};

export type RateLimiter<T> = {
  schedule: (value: T) => void;
  flush: () => void;
  cancel: () => void;
};

export function createRateLimiter<T>(
  intervalMs: number,
  send: (value: T) => void,
  timerApi: TimerApi = {
    clearTimeout: window.clearTimeout.bind(window),
    now: () => performance.now(),
    setTimeout: (callback, delayMs) => window.setTimeout(callback, delayMs),
  },
): RateLimiter<T> {
  let lastSentAt = Number.NEGATIVE_INFINITY;
  let pendingValue: T | null = null;
  let timeoutId: number | null = null;

  function clearScheduledFlush() {
    if (timeoutId === null) {
      return;
    }

    timerApi.clearTimeout(timeoutId);
    timeoutId = null;
  }

  function dispatch(value: T, sentAt: number) {
    pendingValue = null;
    lastSentAt = sentAt;
    send(value);
  }

  function flushPending() {
    if (pendingValue === null) {
      clearScheduledFlush();
      return;
    }

    const value = pendingValue;
    clearScheduledFlush();
    dispatch(value, timerApi.now());
  }

  function scheduleFlush(delayMs: number) {
    if (timeoutId !== null) {
      return;
    }

    timeoutId = timerApi.setTimeout(() => {
      timeoutId = null;
      if (pendingValue === null) {
        return;
      }

      dispatch(pendingValue, timerApi.now());
    }, delayMs);
  }

  return {
    schedule(value) {
      const now = timerApi.now();

      if (now - lastSentAt >= intervalMs && timeoutId === null && pendingValue === null) {
        dispatch(value, now);
        return;
      }

      pendingValue = value;
      scheduleFlush(Math.max(0, intervalMs - (now - lastSentAt)));
    },

    flush() {
      flushPending();
    },

    cancel() {
      pendingValue = null;
      clearScheduledFlush();
    },
  };
}
