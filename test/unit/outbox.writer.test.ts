// test/unit/outbox.writer.test.ts
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { closeOutbox, openOutbox, type OutboxDatabase } from "../../src/outbox/db.ts";
import { createWriter } from "../../src/outbox/writer.ts";

let db: OutboxDatabase;

beforeEach(() => {
  db = openOutbox(":memory:");
});
afterEach(() => closeOutbox(db));

describe("createWriter.enqueue", () => {
  it("inserts a single pending row with the given fields", () => {
    const writer = createWriter(db);
    writer.enqueue({
      topic: "raw",
      messageKey: "deploy-1",
      payload: JSON.stringify({ a: 1 }),
      headers: { source: "elastic-autoops" },
    });
    const row = db.query("SELECT * FROM outbox").get() as Record<string, unknown>;
    expect(row.topic).toBe("raw");
    expect(row.message_key).toBe("deploy-1");
    expect(row.status).toBe("pending");
    expect(row.attempts).toBe(0);
    expect(row.payload).toBe(JSON.stringify({ a: 1 }));
    expect(row.headers).toBe(JSON.stringify({ source: "elastic-autoops" }));
  });

  it("stores headers as null when none provided", () => {
    const writer = createWriter(db);
    writer.enqueue({
      topic: "raw",
      messageKey: "k",
      payload: "{}",
      headers: null,
    });
    const row = db.query("SELECT headers FROM outbox").get() as { headers: string | null };
    expect(row.headers).toBeNull();
  });

  it("rejects unsupported topic values at the writer boundary", () => {
    const writer = createWriter(db);
    // @ts-expect-error — runtime guard, type forbids this
    expect(() => writer.enqueue({ topic: "events", messageKey: "k", payload: "{}", headers: null })).toThrow();
  });
});

describe("createWriter.backlogStats", () => {
  it("counts pending and failed rows", () => {
    const writer = createWriter(db);
    writer.enqueue({ topic: "raw", messageKey: "a", payload: "{}", headers: null });
    writer.enqueue({ topic: "raw", messageKey: "b", payload: "{}", headers: null });
    db.run("UPDATE outbox SET status='failed' WHERE message_key='b'");
    const stats = writer.backlogStats();
    expect(stats.pending).toBe(1);
    expect(stats.failed).toBe(1);
    expect(stats.oldestPendingAgeMs).toBeGreaterThanOrEqual(0);
  });

  it("returns oldestPendingAgeMs=0 when nothing pending", () => {
    const writer = createWriter(db);
    expect(writer.backlogStats()).toEqual({ pending: 0, failed: 0, oldestPendingAgeMs: 0 });
  });
});
