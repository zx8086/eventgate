// src/outbox/writer.ts
import { randomUUID } from "node:crypto";
import type { OutboxDatabase } from "./db.ts";
import { outboxTopicSchema, type OutboxTopic } from "./schemas.ts";

export type EnqueueInput = {
  topic: OutboxTopic;
  messageKey: string;
  payload: string;
  headers: Record<string, string> | null;
};

export type BacklogStats = {
  pending: number;
  failed: number;
  oldestPendingAgeMs: number;
};

export type OutboxWriter = {
  enqueuePair(raw: EnqueueInput, normalized: EnqueueInput): void;
  backlogStats(): BacklogStats;
};

export function createWriter(db: OutboxDatabase): OutboxWriter {
  const insertRow = db.query(
    `INSERT INTO outbox
       (id, topic, message_key, payload, headers, status, attempts, next_attempt_at, created_at)
     VALUES
       ($id, $topic, $message_key, $payload, $headers, 'pending', 0, $now, $now)`,
  );

  const enqueueTx = db.transaction((rows: EnqueueInput[]) => {
    const now = Date.now();
    for (const row of rows) {
      const topic = outboxTopicSchema.parse(row.topic);
      insertRow.run({
        id: randomUUID(),
        topic,
        message_key: row.messageKey,
        payload: row.payload,
        headers: row.headers === null ? null : JSON.stringify(row.headers),
        now,
      });
    }
  });

  const pendingCountStmt = db.query(
    "SELECT COUNT(*) AS c FROM outbox WHERE status = 'pending'",
  );
  const failedCountStmt = db.query(
    "SELECT COUNT(*) AS c FROM outbox WHERE status = 'failed'",
  );
  const oldestPendingStmt = db.query(
    "SELECT MIN(created_at) AS m FROM outbox WHERE status = 'pending'",
  );

  return {
    enqueuePair(raw, normalized) {
      enqueueTx([raw, normalized]);
    },
    backlogStats(): BacklogStats {
      const pending = (pendingCountStmt.get() as { c: number }).c;
      const failed = (failedCountStmt.get() as { c: number }).c;
      const oldest = (oldestPendingStmt.get() as { m: number | null }).m;
      const oldestPendingAgeMs = oldest === null ? 0 : Date.now() - oldest;
      return { pending, failed, oldestPendingAgeMs };
    },
  };
}
