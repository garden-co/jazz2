import { afterEach, describe, expect, it, vi } from "vitest";
import { createRateLimiter } from "../../src/rate-limit.js";

describe("createRateLimiter", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it("sends immediately, collapses bursts, and flushes the latest pending value", () => {
    vi.useFakeTimers();

    const sent: number[] = [];
    const limiter = createRateLimiter<number>(50, (value) => {
      sent.push(value);
    });

    limiter.schedule(1);
    limiter.schedule(2);
    limiter.schedule(3);

    expect(sent).toEqual([1]);

    vi.advanceTimersByTime(49);
    expect(sent).toEqual([1]);

    vi.advanceTimersByTime(1);
    expect(sent).toEqual([1, 3]);

    limiter.schedule(4);
    expect(sent).toEqual([1, 3]);

    vi.advanceTimersByTime(50);
    expect(sent).toEqual([1, 3, 4]);

    limiter.schedule(5);
    limiter.schedule(6);
    limiter.flush();
    expect(sent).toEqual([1, 3, 4, 6]);
  });
});
