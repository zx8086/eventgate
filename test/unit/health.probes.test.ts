// test/unit/health.probes.test.ts
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { closeOutbox, openOutbox, type OutboxDatabase } from "../../src/outbox/db.ts";
import { probeOutboxDb, probeKafkaAdmin } from "../../src/health/probes.ts";

let db: OutboxDatabase;
beforeEach(() => {
  db = openOutbox(":memory:");
});
afterEach(() => closeOutbox(db));

describe("probeOutboxDb", () => {
  it("returns ok=true on a healthy DB", () => {
    const status = probeOutboxDb(db);
    expect(status.ok).toBe(true);
    expect(status.lastError).toBeUndefined();
    expect(status.lastCheckedAt).toBeGreaterThan(0);
  });

  it("returns ok=false when SELECT throws", () => {
    closeOutbox(db);
    const status = probeOutboxDb(db);
    expect(status.ok).toBe(false);
    expect(status.lastError).toMatch(/./);
  });
});

describe("probeKafkaAdmin", () => {
  it("returns ok=true with broker probe ms and empty missing when all topics exist", async () => {
    const admin = { listTopics: async () => ["A", "B", "C"] };
    const result = await probeKafkaAdmin({
      admin,
      expectedTopics: ["A", "B"],
      timeoutMs: 1_000,
    });
    expect(result.broker.ok).toBe(true);
    expect(result.broker.brokerProbeMs).toBeGreaterThanOrEqual(0);
    expect(result.topics.ok).toBe(true);
    expect(result.topics.missing).toEqual([]);
  });

  it("reports missing topics", async () => {
    const admin = { listTopics: async () => ["A"] };
    const result = await probeKafkaAdmin({
      admin,
      expectedTopics: ["A", "B"],
      timeoutMs: 1_000,
    });
    expect(result.broker.ok).toBe(true);
    expect(result.topics.ok).toBe(false);
    expect(result.topics.missing).toEqual(["B"]);
  });

  it("returns broker.ok=false on probe error", async () => {
    const admin = {
      listTopics: async () => {
        throw new Error("connection refused");
      },
    };
    const result = await probeKafkaAdmin({
      admin,
      expectedTopics: ["A"],
      timeoutMs: 1_000,
    });
    expect(result.broker.ok).toBe(false);
    expect(result.broker.lastError).toMatch(/connection refused/);
    expect(result.topics.ok).toBe(false);
    expect(result.topics.lastError).toMatch(/broker probe failed/);
  });

  it("times out a hanging probe", async () => {
    const admin = {
      listTopics: () => new Promise<string[]>(() => {}),
    };
    const result = await probeKafkaAdmin({
      admin,
      expectedTopics: ["A"],
      timeoutMs: 20,
    });
    expect(result.broker.ok).toBe(false);
    expect(result.broker.lastError).toMatch(/timeout/i);
  });
});
