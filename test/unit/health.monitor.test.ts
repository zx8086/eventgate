// test/unit/health.monitor.test.ts
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { closeOutbox, openOutbox, type OutboxDatabase } from "../../src/outbox/db.ts";
import { createHealthMonitor } from "../../src/health/monitor.ts";

let db: OutboxDatabase;
beforeEach(() => {
  db = openOutbox(":memory:");
});
afterEach(() => closeOutbox(db));

function fakeAdmin(listTopics: () => Promise<string[]>) {
  return { listTopics };
}

function fakeProducer(connected: boolean) {
  return { isConnected: () => connected };
}

describe("HealthMonitor", () => {
  it("returns a snapshot synchronously after start() resolves", async () => {
    const monitor = createHealthMonitor({
      producer: fakeProducer(true),
      outboxDb: db,
      admin: fakeAdmin(async () => ["T_PRIVATE_SOURCE_ELASTIC_AUTOOPS"]),
      expectedTopics: ["T_PRIVATE_SOURCE_ELASTIC_AUTOOPS"],
      probeIntervalMs: 10_000,
      probeTimeoutMs: 1_000,
    });
    await monitor.start();
    const snap = monitor.snapshot();
    expect(snap.status).toBe("healthy");
    expect(snap.ok).toBe(true);
    expect(snap.dependencies.kafkaProducer?.ok).toBe(true);
    expect(snap.dependencies.outboxDb?.ok).toBe(true);
    expect(snap.dependencies.kafkaBroker?.ok).toBe(true);
    expect(snap.dependencies.topics?.ok).toBe(true);
    await monitor.stop();
  });

  it("reports degraded (200-class) when only topics are missing", async () => {
    const monitor = createHealthMonitor({
      producer: fakeProducer(true),
      outboxDb: db,
      admin: fakeAdmin(async () => []),
      expectedTopics: ["T_MISSING"],
      probeIntervalMs: 10_000,
      probeTimeoutMs: 1_000,
    });
    await monitor.start();
    const snap = monitor.snapshot();
    expect(snap.status).toBe("degraded");
    expect(snap.ok).toBe(true);
    expect(snap.dependencies.topics?.missing).toEqual(["T_MISSING"]);
    await monitor.stop();
  });

  it("reports unhealthy when producer is disconnected", async () => {
    const monitor = createHealthMonitor({
      producer: fakeProducer(false),
      outboxDb: db,
      admin: fakeAdmin(async () => ["T_A"]),
      expectedTopics: ["T_A"],
      probeIntervalMs: 10_000,
      probeTimeoutMs: 1_000,
    });
    await monitor.start();
    const snap = monitor.snapshot();
    expect(snap.status).toBe("unhealthy");
    expect(snap.ok).toBe(false);
    expect(snap.dependencies.kafkaProducer?.ok).toBe(false);
    await monitor.stop();
  });

  it("logs a state transition only when a dependency flips", async () => {
    let topics = ["T_A"];
    const logs: Array<{ level: string; obj: object; msg: string }> = [];
    const monitor = createHealthMonitor({
      producer: fakeProducer(true),
      outboxDb: db,
      admin: fakeAdmin(async () => topics),
      expectedTopics: ["T_A"],
      probeIntervalMs: 10_000,
      probeTimeoutMs: 1_000,
      logger: {
        info: (obj, msg) => logs.push({ level: "info", obj: obj as object, msg: msg as string }),
        warn: () => {},
        error: () => {},
        debug: () => {},
        trace: () => {},
        fatal: () => {},
        child: () => ({} as never),
        flush: () => {},
      },
    });
    await monitor.start();
    logs.length = 0;
    topics = []; // simulate the topic disappearing
    await monitor.probeOnce();
    const flipLogs = logs.filter((l) => l.msg === "dependency state changed");
    expect(flipLogs.length).toBe(1);
    expect((flipLogs[0]!.obj as { dependency: string }).dependency).toBe("topics");
    topics = ["T_A"]; // restore
    await monitor.probeOnce();
    const restoreLogs = logs.filter((l) => l.msg === "dependency state changed");
    expect(restoreLogs.length).toBe(2);
    await monitor.stop();
  });

  it("isolates probe failures - a thrown probe must not stop the monitor", async () => {
    let throwOnce = true;
    const monitor = createHealthMonitor({
      producer: fakeProducer(true),
      outboxDb: db,
      admin: {
        listTopics: async () => {
          if (throwOnce) {
            throwOnce = false;
            throw new Error("broker boom");
          }
          return ["T_A"];
        },
      },
      expectedTopics: ["T_A"],
      probeIntervalMs: 10_000,
      probeTimeoutMs: 1_000,
    });
    await monitor.start();
    expect(monitor.snapshot().dependencies.kafkaBroker?.ok).toBe(false);
    await monitor.probeOnce();
    expect(monitor.snapshot().dependencies.kafkaBroker?.ok).toBe(true);
    await monitor.stop();
  });

  it("reports healthy when outboxDb is intentionally absent (OUTBOX_ENABLED=false)", async () => {
    const monitor = createHealthMonitor({
      producer: fakeProducer(true),
      // outboxDb intentionally omitted — escape hatch
      admin: fakeAdmin(async () => ["T_A"]),
      expectedTopics: ["T_A"],
      probeIntervalMs: 10_000,
      probeTimeoutMs: 1_000,
    });
    await monitor.start();
    const snap = monitor.snapshot();
    expect(snap.status).toBe("healthy");
    expect(snap.ok).toBe(true);
    expect(snap.dependencies.outboxDb).toBeUndefined();
    await monitor.stop();
  });
});
