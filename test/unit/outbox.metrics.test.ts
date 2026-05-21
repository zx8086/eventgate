// test/unit/outbox.metrics.test.ts
import { describe, expect, it } from "bun:test";
import { createDrainMetrics } from "../../src/outbox/metrics.ts";

describe("DrainMetrics", () => {
  it("starts empty", () => {
    const m = createDrainMetrics({ windowMs: 60_000, now: () => 1_000 });
    expect(m.snapshot()).toEqual({
      publishedLast60s: 0,
      lastPublishedAt: null,
      lastError: null,
      breakerOpenCount: 0,
    });
  });

  it("counts publishes within the window", () => {
    let t = 0;
    const m = createDrainMetrics({ windowMs: 60_000, now: () => t });
    t = 1_000; m.recordPublished("T1");
    t = 2_000; m.recordPublished("T1");
    t = 3_000; m.recordPublished("T2");
    t = 4_000;
    const snap = m.snapshot();
    expect(snap.publishedLast60s).toBe(3);
    expect(snap.lastPublishedAt).toBe(3_000);
  });

  it("evicts publishes older than the window on read", () => {
    let t = 0;
    const m = createDrainMetrics({ windowMs: 60_000, now: () => t });
    t = 1_000; m.recordPublished("T1");
    t = 2_000; m.recordPublished("T1");
    t = 70_000;
    expect(m.snapshot().publishedLast60s).toBe(0);
    expect(m.snapshot().lastPublishedAt).toBe(2_000);
  });

  it("captures the last error", () => {
    let t = 0;
    const m = createDrainMetrics({ windowMs: 60_000, now: () => t });
    t = 5_000;
    m.recordError("T_FOO", "UNKNOWN_TOPIC_OR_PARTITION");
    expect(m.snapshot().lastError).toEqual({
      topic: "T_FOO",
      message: "UNKNOWN_TOPIC_OR_PARTITION",
      at: 5_000,
    });
  });

  it("does not clear lastError after a successful publish - operators may still want the last failure visible", () => {
    let t = 0;
    const m = createDrainMetrics({ windowMs: 60_000, now: () => t });
    t = 1_000; m.recordError("T", "boom");
    t = 2_000; m.recordPublished("T");
    expect(m.snapshot().lastError?.message).toBe("boom");
    expect(m.snapshot().publishedLast60s).toBe(1);
  });
});

describe("DrainMetrics breakerOpenCount", () => {
  it("starts at zero", () => {
    const m = createDrainMetrics({ windowMs: 60_000, now: () => 1_000 });
    expect(m.snapshot().breakerOpenCount).toBe(0);
  });

  it("increments on incrementBreakerOpenCount()", () => {
    const m = createDrainMetrics({ windowMs: 60_000, now: () => 1_000 });
    m.incrementBreakerOpenCount();
    m.incrementBreakerOpenCount();
    m.incrementBreakerOpenCount();
    expect(m.snapshot().breakerOpenCount).toBe(3);
  });
});
