// test/unit/outbox.drainer.test.ts
import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { closeOutbox, openOutbox, type OutboxDatabase } from "../../src/outbox/db.ts";
import { createWriter, type EnqueueInput } from "../../src/outbox/writer.ts";
import { runOutboxIteration } from "../../src/outbox/drainer.ts";

type Sent = { topic: string; key: string; value: string; headers?: Record<string, string> | null };

function makeFakeProducer(failuresPerTopic: Record<string, number> = {}) {
  const sent: Sent[] = [];
  const remaining = { ...failuresPerTopic };
  const sendByTopic = async (
    topic: string,
    key: string,
    value: string,
    headers?: Record<string, string> | null,
  ) => {
    const leftToFail = remaining[topic] ?? 0;
    if (leftToFail > 0) {
      remaining[topic] = leftToFail - 1;
      throw new Error(`fake producer: ${topic} forced failure`);
    }
    sent.push({ topic, key, value, headers: headers ?? null });
  };
  return { sendByTopic, sent };
}

const topics = { raw: "raw.t" };

const cfg = {
  topics,
  batchSize: 100,
  backoffMaxMs: 600_000,
  maxAgeMs: 24 * 60 * 60 * 1_000,
};

function seedTwoPending(writer: { enqueue(row: EnqueueInput): void }): [EnqueueInput, EnqueueInput] {
  const rows: [EnqueueInput, EnqueueInput] = [
    { topic: "raw", messageKey: "k1", payload: JSON.stringify({ raw: 1 }), headers: null },
    {
      topic: "raw",
      messageKey: "k2",
      payload: JSON.stringify({ raw: 2 }),
      headers: { idempotencyKey: "abc" },
    },
  ];
  writer.enqueue(rows[0]);
  writer.enqueue(rows[1]);
  return rows;
}

let db: OutboxDatabase;
beforeEach(() => {
  db = openOutbox(":memory:");
});
afterEach(() => closeOutbox(db));

describe("runOutboxIteration", () => {
  test("publishes pending rows and marks them dispatched", async () => {
    const writer = createWriter(db);
    seedTwoPending(writer);

    const producer = makeFakeProducer();
    const result = await runOutboxIteration({ db, producer, config: cfg });

    expect(result.published).toBe(2);
    expect(result.retried).toBe(0);
    expect(result.failedPermanently).toBe(0);
    // Outbox only routes to topics.raw today (SIO-801); both distinct rows shipped in order.
    expect(producer.sent.map((s) => s.topic)).toEqual(["raw.t", "raw.t"]);
    expect(producer.sent.map((s) => s.key)).toEqual(["k1", "k2"]);

    const rows = db.query("SELECT status FROM outbox").all() as Array<{ status: string }>;
    expect(rows.every((r) => r.status === "dispatched")).toBe(true);
  });

  test("schedules retry with exponential backoff on transient failure", async () => {
    const writer = createWriter(db);
    seedTwoPending(writer);

    const producer = makeFakeProducer({ "raw.t": 2 });
    const before = Date.now();
    const result = await runOutboxIteration({ db, producer, config: cfg });

    expect(result.published).toBe(0);
    expect(result.retried).toBe(2);
    const rows = db.query("SELECT attempts, next_attempt_at, status, last_error FROM outbox").all() as Array<{
      attempts: number;
      next_attempt_at: number;
      status: string;
      last_error: string;
    }>;
    for (const row of rows) {
      expect(row.status).toBe("pending");
      expect(row.attempts).toBe(1);
      expect(row.next_attempt_at).toBeGreaterThanOrEqual(before + 1_000);
      expect(row.last_error).toMatch(/forced failure/);
    }
  });

  test("marks row failed when older than maxAgeMs", async () => {
    const writer = createWriter(db);
    seedTwoPending(writer);
    // backdate created_at past the age threshold
    db.query("UPDATE outbox SET created_at = $oldCreated, next_attempt_at = $now").run({
      oldCreated: Date.now() - 25 * 60 * 60 * 1_000,
      now: Date.now(),
    });

    const producer = makeFakeProducer({ "raw.t": 5 });
    const result = await runOutboxIteration({ db, producer, config: cfg });

    expect(result.failedPermanently).toBe(2);
    const rows = db.query("SELECT status FROM outbox").all() as Array<{ status: string }>;
    expect(rows.every((r) => r.status === "failed")).toBe(true);
  });

  test("skips rows whose next_attempt_at is in the future", async () => {
    const writer = createWriter(db);
    seedTwoPending(writer);
    db.query("UPDATE outbox SET next_attempt_at = $next").run({ next: Date.now() + 60_000 });

    const producer = makeFakeProducer();
    const result = await runOutboxIteration({ db, producer, config: cfg });
    expect(result.published).toBe(0);
    expect(result.retried).toBe(0);
    expect(producer.sent).toHaveLength(0);
  });

  test("respects batchSize", async () => {
    const writer = createWriter(db);
    for (let i = 0; i < 3; i++) seedTwoPending(writer);

    const producer = makeFakeProducer();
    const result = await runOutboxIteration({
      db,
      producer,
      config: { ...cfg, batchSize: 2 },
    });
    expect(result.published).toBe(2);
    const dispatched = (db.query("SELECT COUNT(*) AS c FROM outbox WHERE status='dispatched'").get() as { c: number }).c;
    expect(dispatched).toBe(2);
  });

  test("parses and forwards headers JSON", async () => {
    const writer = createWriter(db);
    seedTwoPending(writer);
    const producer = makeFakeProducer();
    await runOutboxIteration({ db, producer, config: cfg });
    const withHeaders = producer.sent.find((s) => s.headers !== null);
    expect(withHeaders?.headers).toEqual({ idempotencyKey: "abc" });
  });
});
