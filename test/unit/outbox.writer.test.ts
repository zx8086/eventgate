// test/unit/outbox.writer.test.ts
import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { closeOutbox, openOutbox, type OutboxDatabase } from "../../src/outbox/db.ts";
import { createWriter, type EnqueueInput } from "../../src/outbox/writer.ts";

let db: OutboxDatabase;

beforeEach(() => {
  db = openOutbox(":memory:");
});

afterEach(() => {
  closeOutbox(db);
});

function makePair(): [EnqueueInput, EnqueueInput] {
  return [
    {
      topic: "raw",
      messageKey: "res-1",
      payload: JSON.stringify({ raw: true }),
      headers: null,
    },
    {
      topic: "events",
      messageKey: "res-1",
      payload: JSON.stringify({ event: true }),
      headers: { eventType: "alert" },
    },
  ];
}

describe("createWriter.enqueuePair", () => {
  test("writes both rows atomically", () => {
    const writer = createWriter(db);
    const [raw, normalized] = makePair();
    writer.enqueuePair(raw, normalized);

    const rows = db.query("SELECT * FROM outbox ORDER BY topic").all() as Array<{
      topic: string;
      status: string;
      attempts: number;
      next_attempt_at: number;
      created_at: number;
    }>;
    expect(rows).toHaveLength(2);
    expect(rows.map((r) => r.topic).sort()).toEqual(["events", "raw"]);
    for (const row of rows) {
      expect(row.status).toBe("pending");
      expect(row.attempts).toBe(0);
      expect(row.next_attempt_at).toBe(row.created_at);
    }
  });

  test("rolls back when the second insert throws", () => {
    const writer = createWriter(db);
    const [raw] = makePair();
    expect(() =>
      writer.enqueuePair(raw, {
        // bad row: topic missing/empty triggers an insertion error
        topic: "" as unknown as EnqueueInput["topic"],
        messageKey: "res-1",
        payload: "{}",
        headers: null,
      }),
    ).toThrow();

    const count = (db.query("SELECT COUNT(*) AS c FROM outbox").get() as { c: number }).c;
    expect(count).toBe(0);
  });

  test("serializes headers as JSON when provided", () => {
    const writer = createWriter(db);
    const [, normalized] = makePair();
    writer.enqueuePair(makePair()[0], normalized);

    const row = db
      .query("SELECT headers FROM outbox WHERE topic = $t")
      .get({ t: "events" }) as { headers: string };
    expect(JSON.parse(row.headers)).toEqual({ eventType: "alert" });
  });

  test("backlogStats counts pending, failed, and oldest age", async () => {
    const writer = createWriter(db);
    const [raw, normalized] = makePair();
    writer.enqueuePair(raw, normalized);

    const before = writer.backlogStats();
    expect(before.pending).toBe(2);
    expect(before.failed).toBe(0);
    expect(before.oldestPendingAgeMs).toBeGreaterThanOrEqual(0);

    db.run("UPDATE outbox SET status='failed' WHERE topic='raw'");
    const after = writer.backlogStats();
    expect(after.pending).toBe(1);
    expect(after.failed).toBe(1);
  });
});
