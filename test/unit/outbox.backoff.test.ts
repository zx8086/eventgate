// test/unit/outbox.backoff.test.ts
import { describe, expect, test } from "bun:test";
import { nextDelayMs } from "../../src/outbox/backoff.ts";

describe("nextDelayMs", () => {
  test("first failure waits 1s", () => {
    expect(nextDelayMs(1, 600_000)).toBe(1_000);
  });

  test("doubles each attempt before cap", () => {
    expect(nextDelayMs(2, 600_000)).toBe(2_000);
    expect(nextDelayMs(3, 600_000)).toBe(4_000);
    expect(nextDelayMs(4, 600_000)).toBe(8_000);
    expect(nextDelayMs(5, 600_000)).toBe(16_000);
  });

  test("caps at capMs", () => {
    expect(nextDelayMs(20, 600_000)).toBe(600_000);
    expect(nextDelayMs(100, 600_000)).toBe(600_000);
  });

  test("respects a smaller cap", () => {
    expect(nextDelayMs(10, 10_000)).toBe(10_000);
    expect(nextDelayMs(2, 10_000)).toBe(2_000);
  });

  test("attempts <= 0 returns base 1s (defensive)", () => {
    expect(nextDelayMs(0, 600_000)).toBe(1_000);
    expect(nextDelayMs(-3, 600_000)).toBe(1_000);
  });
});
