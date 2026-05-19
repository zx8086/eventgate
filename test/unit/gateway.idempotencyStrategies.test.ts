// test/unit/gateway.idempotencyStrategies.test.ts
import { describe, expect, it } from "bun:test";
import {
  idempotencyStrategies,
  knownIdempotencyStrategy,
  resolveIdempotencyStrategy,
} from "../../src/gateway/idempotencyStrategies.ts";

describe("idempotencyStrategies", () => {
  it("includes elastic-autoops by default", () => {
    expect(typeof idempotencyStrategies["elastic-autoops"]).toBe("function");
  });

  it("elastic-autoops derives a hash for an AutoOps-shaped body", () => {
    const fn = idempotencyStrategies["elastic-autoops"];
    const key = fn?.({
      resourceId: "dep-1",
      title: "Cluster red",
      status: "open",
      startTime: "2026-05-19T00:00:00Z",
    });
    expect(typeof key).toBe("string");
    expect((key ?? "").length).toBeGreaterThan(10);
  });

  it("elastic-autoops returns undefined for a non-AutoOps body", () => {
    const fn = idempotencyStrategies["elastic-autoops"];
    expect(fn?.({ foo: "bar" })).toBeUndefined();
  });
});

describe("knownIdempotencyStrategy", () => {
  it("returns true for registered names", () => {
    expect(knownIdempotencyStrategy("elastic-autoops")).toBe(true);
  });
  it("returns false for unknown names", () => {
    expect(knownIdempotencyStrategy("does-not-exist")).toBe(false);
  });
});

describe("resolveIdempotencyStrategy", () => {
  it("returns undefined when name is undefined", () => {
    expect(resolveIdempotencyStrategy(undefined)).toBeUndefined();
  });
  it("returns the function for a known name", () => {
    expect(typeof resolveIdempotencyStrategy("elastic-autoops")).toBe("function");
  });
  it("returns undefined for unknown name (defensive at runtime)", () => {
    expect(resolveIdempotencyStrategy("nope")).toBeUndefined();
  });
});
