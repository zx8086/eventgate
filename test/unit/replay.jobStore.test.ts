// test/unit/replay.jobStore.test.ts
import { describe, expect, it } from "bun:test";
import { createReplayJobStore } from "../../src/replay/jobStore.ts";

describe("createReplayJobStore", () => {
  it("creates a job with sane defaults", () => {
    const store = createReplayJobStore();
    const j = store.create({
      route: "elastic-autoops",
      partition: 0,
      mode: "single",
      dryRun: true,
      fromOffset: 5,
      toOffset: 5,
    });
    expect(j.id).toMatch(/[0-9a-f-]{36}/);
    expect(j.status).toBe("pending");
    expect(j.scanned).toBe(0);
    expect(j.fromOffset).toBe(5);
    expect(j.toOffset).toBe(5);
    expect(j.lastOffset).toBeNull();
    expect(j.finishedAt).toBeNull();
  });

  it("get returns null for unknown id", () => {
    const store = createReplayJobStore();
    expect(store.get("nope")).toBeNull();
  });

  it("update merges a patch", () => {
    const store = createReplayJobStore();
    const j = store.create({ route: "r", partition: 0, mode: "manual", dryRun: true });
    store.update(j.id, { scanned: 10, replayed: 5, status: "running" });
    const cur = store.get(j.id);
    expect(cur?.scanned).toBe(10);
    expect(cur?.replayed).toBe(5);
    expect(cur?.status).toBe("running");
  });

  it("terminal status is sticky (cancel then attempted status change)", () => {
    const store = createReplayJobStore();
    const j = store.create({ route: "r", partition: 0, mode: "manual", dryRun: true });
    const ctl = new AbortController();
    store.setCancelHandle(j.id, ctl);
    expect(store.cancel(j.id)).toBe(true);
    expect(store.get(j.id)?.status).toBe("cancelled");
    // Attempting to flip back to "running" must be ignored.
    store.update(j.id, { status: "running", scanned: 1 });
    const cur = store.get(j.id);
    expect(cur?.status).toBe("cancelled");
    // Non-status fields still update (counter writes after cancel are allowed).
    expect(cur?.scanned).toBe(1);
  });

  it("cancel returns false for a terminal job", () => {
    const store = createReplayJobStore();
    const j = store.create({ route: "r", partition: 0, mode: "manual", dryRun: true });
    store.update(j.id, { status: "done", finishedAt: Date.now() });
    expect(store.cancel(j.id)).toBe(false);
  });

  it("hasActiveJob is true while pending/running/paused", () => {
    const store = createReplayJobStore();
    store.create({ route: "r", partition: 0, mode: "manual", dryRun: true });
    expect(store.hasActiveJob("r", 0)).toBe(true);
    expect(store.hasActiveJob("r", 1)).toBe(false);
    expect(store.hasActiveJob("other", 0)).toBe(false);
  });

  it("hasActiveJob is false once a job reaches a terminal state", () => {
    const store = createReplayJobStore();
    const j = store.create({ route: "r", partition: 0, mode: "manual", dryRun: true });
    store.update(j.id, { status: "done", finishedAt: Date.now() });
    expect(store.hasActiveJob("r", 0)).toBe(false);
  });

  it("cancelAll aborts every registered handle", () => {
    const store = createReplayJobStore();
    const j1 = store.create({ route: "r", partition: 0, mode: "manual", dryRun: true });
    const j2 = store.create({ route: "r", partition: 1, mode: "manual", dryRun: true });
    const c1 = new AbortController();
    const c2 = new AbortController();
    store.setCancelHandle(j1.id, c1);
    store.setCancelHandle(j2.id, c2);
    store.cancelAll();
    expect(c1.signal.aborted).toBe(true);
    expect(c2.signal.aborted).toBe(true);
  });
});
