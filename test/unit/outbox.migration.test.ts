// test/unit/outbox.migration.test.ts
import { afterEach, describe, expect, it } from "bun:test";
import { closeOutbox, openOutbox, runOutboxMigrations, type OutboxDatabase } from "../../src/outbox/db.ts";

let db: OutboxDatabase;

afterEach(() => {
  if (db) closeOutbox(db);
});

describe("runOutboxMigrations", () => {
  it("rewrites topic='raw' rows to T_PRIVATE_SOURCE_ELASTIC_AUTOOPS", () => {
    db = openOutbox(":memory:");
    db.run(
      `INSERT INTO outbox (id, topic, message_key, payload, status, attempts, next_attempt_at, created_at)
       VALUES ('legacy-1', 'raw', 'k', '{}', 'pending', 0, 0, 0)`,
    );
    runOutboxMigrations(db);
    const row = db.query("SELECT topic FROM outbox WHERE id='legacy-1'").get() as { topic: string };
    expect(row.topic).toBe("T_PRIVATE_SOURCE_ELASTIC_AUTOOPS");
  });

  it("is idempotent (running twice leaves rows unchanged)", () => {
    db = openOutbox(":memory:");
    db.run(
      `INSERT INTO outbox (id, topic, message_key, payload, status, attempts, next_attempt_at, created_at)
       VALUES ('legacy-2', 'raw', 'k', '{}', 'pending', 0, 0, 0)`,
    );
    runOutboxMigrations(db);
    runOutboxMigrations(db);
    const row = db.query("SELECT topic FROM outbox WHERE id='legacy-2'").get() as { topic: string };
    expect(row.topic).toBe("T_PRIVATE_SOURCE_ELASTIC_AUTOOPS");
  });

  it("leaves non-legacy topic values untouched", () => {
    db = openOutbox(":memory:");
    db.run(
      `INSERT INTO outbox (id, topic, message_key, payload, status, attempts, next_attempt_at, created_at)
       VALUES ('new-1', 'T_PRIVATE_SOURCE_DATADOG_ALERTS', 'k', '{}', 'pending', 0, 0, 0)`,
    );
    runOutboxMigrations(db);
    const row = db.query("SELECT topic FROM outbox WHERE id='new-1'").get() as { topic: string };
    expect(row.topic).toBe("T_PRIVATE_SOURCE_DATADOG_ALERTS");
  });
});
