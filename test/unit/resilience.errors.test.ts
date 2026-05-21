// test/unit/resilience.errors.test.ts
import { describe, expect, it } from "bun:test";
import {
  CircuitBreakerOpenError,
  isApplicationLevelError,
} from "../../src/resilience/errors.ts";

describe("CircuitBreakerOpenError", () => {
  it("carries the next attempt timestamp", () => {
    const nextAttempt = new Date("2026-05-21T08:01:00.000Z");
    const err = new CircuitBreakerOpenError(nextAttempt);
    expect(err.nextAttemptAt).toBe(nextAttempt);
    expect(err.name).toBe("CircuitBreakerOpenError");
    expect(err.message).toMatch(/2026-05-21T08:01:00\.000Z/);
  });

  it("is an instanceof Error and CircuitBreakerOpenError", () => {
    const err = new CircuitBreakerOpenError(new Date());
    expect(err).toBeInstanceOf(Error);
    expect(err).toBeInstanceOf(CircuitBreakerOpenError);
  });
});

describe("isApplicationLevelError", () => {
  it("returns false for any error in PR1 (conservative default-trip)", () => {
    expect(isApplicationLevelError(new Error("metadata failed"))).toBe(false);
    expect(isApplicationLevelError(new TypeError("oops"))).toBe(false);
    expect(isApplicationLevelError("a string")).toBe(false);
    expect(isApplicationLevelError(undefined)).toBe(false);
    expect(isApplicationLevelError(null)).toBe(false);
    expect(isApplicationLevelError({ code: "UNKNOWN_TOPIC_OR_PARTITION" })).toBe(false);
  });
});
