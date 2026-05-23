// test/unit/replay.jobStore.sqlite.test.ts
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import {
  closeOutbox,
  openOutbox,
  runReplayMigrations,
  sweepReplayGhostJobs,
  type OutboxDatabase,
} from "../../src/outbox/db.ts";
import { createSqliteReplayJobStore } from "../../src/replay/jobStore.ts";

let db: OutboxDatabase;

beforeEach(() => {
  db = openOutbox(":memory:");
  runReplayMigrations(db);
});

afterEach(() => closeOutbox(db));

describe("createSqliteReplayJobStore", () => {
  it("persists create + get round-trip", () => {
    const store = createSqliteReplayJobStore(db);
    const j = store.create({
      route: "elastic-autoops",
      partition: 2,
      mode: "manual",
      dryRun: true,
      fromOffset: 100,
      toOffset: 200,
    });
    const back = store.get(j.id);
    expect(back).not.toBeNull();
    expect(back?.id).toBe(j.id);
    expect(back?.route).toBe("elastic-autoops");
    expect(back?.partition).toBe(2);
    expect(back?.mode).toBe("manual");
    expect(back?.dryRun).toBe(true);
    expect(back?.fromOffset).toBe(100);
    expect(back?.toOffset).toBe(200);
    expect(back?.status).toBe("pending");
  });

  it("update merges counters + status", () => {
    const store = createSqliteReplayJobStore(db);
    const j = store.create({
      route: "r",
      partition: 0,
      mode: "manual",
      dryRun: true,
    });
    store.update(j.id, {
      scanned: 5,
      replayed: 3,
      parked: 1,
      lastOffset: 42,
      status: "running",
    });
    const cur = store.get(j.id);
    expect(cur?.scanned).toBe(5);
    expect(cur?.replayed).toBe(3);
    expect(cur?.parked).toBe(1);
    expect(cur?.lastOffset).toBe(42);
    expect(cur?.status).toBe("running");
  });

  it("status changes are sticky once terminal (cannot un-cancel)", () => {
    const store = createSqliteReplayJobStore(db);
    const j = store.create({
      route: "r",
      partition: 0,
      mode: "manual",
      dryRun: true,
    });
    expect(store.cancel(j.id)).toBe(true);
    expect(store.get(j.id)?.status).toBe("cancelled");
    // Attempt to flip back is silently dropped (status guard).
    store.update(j.id, { status: "running", scanned: 99 });
    const cur = store.get(j.id);
    expect(cur?.status).toBe("cancelled");
    // Counter writes are also blocked when status was in the patch + the
    // guard fired; this is OK because the sticky semantics fire at the
    // statement level. Counter-only updates (no status in patch) still land.
    store.update(j.id, { scanned: 100 });
    expect(store.get(j.id)?.scanned).toBe(100);
  });

  it("cancel returns false on a terminal job", () => {
    const store = createSqliteReplayJobStore(db);
    const j = store.create({
      route: "r",
      partition: 0,
      mode: "manual",
      dryRun: true,
    });
    store.update(j.id, { status: "done", finishedAt: Date.now() });
    expect(store.cancel(j.id)).toBe(false);
  });

  it("hasActiveJob is keyed by (route, partition)", () => {
    const store = createSqliteReplayJobStore(db);
    store.create({ route: "r", partition: 0, mode: "manual", dryRun: true });
    expect(store.hasActiveJob("r", 0)).toBe(true);
    expect(store.hasActiveJob("r", 1)).toBe(false);
    expect(store.hasActiveJob("other", 0)).toBe(false);
  });

  it("lastJobForRoute returns the most recent job for that route", async () => {
    const store = createSqliteReplayJobStore(db);
    const j1 = store.create({ route: "r", partition: 0, mode: "manual", dryRun: true });
    // Different startedAt — sleep briefly to ensure ordering is reliable.
    await new Promise((r) => setTimeout(r, 5));
    const j2 = store.create({ route: "r", partition: 1, mode: "auto", dryRun: false });
    const last = store.lastJobForRoute("r");
    expect(last?.id).toBe(j2.id);
    expect(last?.id).not.toBe(j1.id);
  });

  it("setLastReplayedOffset upserts per (route, partition)", () => {
    const store = createSqliteReplayJobStore(db);
    store.setLastReplayedOffset("r", 0, 100);
    expect(store.getLastReplayedOffset("r", 0)).toBe(100);
    // Subsequent calls overwrite.
    store.setLastReplayedOffset("r", 0, 250);
    expect(store.getLastReplayedOffset("r", 0)).toBe(250);
    // Different partition is independent.
    expect(store.getLastReplayedOffset("r", 1)).toBeNull();
    store.setLastReplayedOffset("r", 1, 50);
    expect(store.getLastReplayedOffset("r", 1)).toBe(50);
    expect(store.getLastReplayedOffset("r", 0)).toBe(250);
  });
});

describe("sweepReplayGhostJobs", () => {
  it("marks pending/running/paused rows as failed with the ghost lastError", () => {
    const store = createSqliteReplayJobStore(db);
    const pending = store.create({ route: "r", partition: 0, mode: "manual", dryRun: false });
    const running = store.create({ route: "r", partition: 1, mode: "manual", dryRun: false });
    store.update(running.id, { status: "running" });
    const done = store.create({ route: "r", partition: 2, mode: "manual", dryRun: false });
    store.update(done.id, { status: "done", finishedAt: Date.now() });

    const swept = sweepReplayGhostJobs(db);
    expect(swept).toBe(2);

    expect(store.get(pending.id)?.status).toBe("failed");
    expect(store.get(pending.id)?.lastError).toBe("gateway_restart_orphan");
    expect(store.get(running.id)?.status).toBe("failed");
    expect(store.get(done.id)?.status).toBe("done"); // terminal, untouched
  });

  it("returns 0 when there are no orphan rows", () => {
    expect(sweepReplayGhostJobs(db)).toBe(0);
  });
});
