// src/outbox/drainer.ts
import { getLogger } from "../logging/index.ts";
import { nextDelayMs } from "./backoff.ts";
import type { OutboxDatabase } from "./db.ts";
import type { OutboxTopic } from "./schemas.ts";

const log = getLogger("outbox.drainer");

export type DrainerProducer = {
  sendByTopic(
    topic: string,
    key: string,
    value: string,
    headers?: Record<string, string> | null,
  ): Promise<void>;
};

export type DrainerConfig = {
  topics: { raw: string; events: string; dlq: string };
  batchSize: number;
  backoffMaxMs: number;
  maxAgeMs: number;
};

export type DrainerStartConfig = DrainerConfig & {
  idlePollMs: number;
  busyPollMs: number;
  backlogWarnThreshold: number;
};

export type IterationResult = {
  scanned: number;
  published: number;
  retried: number;
  failedPermanently: number;
};

type PendingRow = {
  id: string;
  topic: string;
  message_key: string;
  payload: string;
  headers: string | null;
  attempts: number;
  created_at: number;
};

function topicToKafka(topic: string, topics: DrainerConfig["topics"]): string {
  switch (topic as OutboxTopic) {
    case "raw":
      return topics.raw;
    case "events":
      return topics.events;
    case "dlq":
      return topics.dlq;
  }
}

function parseHeaders(raw: string | null): Record<string, string> | null {
  if (raw === null) return null;
  try {
    const parsed = JSON.parse(raw) as unknown;
    if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
      return parsed as Record<string, string>;
    }
    return null;
  } catch {
    return null;
  }
}

export async function runOutboxIteration(opts: {
  db: OutboxDatabase;
  producer: DrainerProducer;
  config: DrainerConfig;
}): Promise<IterationResult> {
  const { db, producer, config } = opts;
  const now = Date.now();

  const pending = db
    .query(
      `SELECT id, topic, message_key, payload, headers, attempts, created_at
       FROM outbox
       WHERE status = 'pending' AND next_attempt_at <= $now
       ORDER BY next_attempt_at ASC
       LIMIT $limit`,
    )
    .all({ now, limit: config.batchSize }) as PendingRow[];

  const markDispatched = db.query(
    `UPDATE outbox SET status='dispatched', dispatched_at=$now WHERE id=$id`,
  );
  const markRetry = db.query(
    `UPDATE outbox SET attempts=$attempts, next_attempt_at=$next, last_error=$err WHERE id=$id`,
  );
  const markFailed = db.query(
    `UPDATE outbox SET status='failed', attempts=$attempts, last_error=$err WHERE id=$id`,
  );

  let published = 0;
  let retried = 0;
  let failedPermanently = 0;

  for (const row of pending) {
    const kafkaTopic = topicToKafka(row.topic, config.topics);
    const headers = parseHeaders(row.headers);
    try {
      await producer.sendByTopic(kafkaTopic, row.message_key, row.payload, headers);
      markDispatched.run({ id: row.id, now: Date.now() });
      published += 1;
    } catch (err) {
      const attempts = row.attempts + 1;
      const ageMs = Date.now() - row.created_at;
      const message = err instanceof Error ? err.message : String(err);
      if (ageMs > config.maxAgeMs) {
        markFailed.run({ id: row.id, attempts, err: message });
        failedPermanently += 1;
        log.warn(
          { id: row.id, topic: row.topic, ageMs, attempts, err: message },
          "outbox row exceeded maxAgeMs; marked failed",
        );
      } else {
        const delay = nextDelayMs(attempts, config.backoffMaxMs);
        markRetry.run({
          id: row.id,
          attempts,
          next: Date.now() + delay,
          err: message,
        });
        retried += 1;
      }
    }
  }

  return { scanned: pending.length, published, retried, failedPermanently };
}

export type DrainerHandle = {
  stop(): Promise<void>;
};

export function startDrainer(opts: {
  db: OutboxDatabase;
  producer: DrainerProducer;
  config: DrainerStartConfig;
}): DrainerHandle {
  const { db, producer, config } = opts;
  let stopped = false;
  let currentTimer: ReturnType<typeof setTimeout> | undefined;
  let inFlight: Promise<void> = Promise.resolve();

  const tick = async () => {
    if (stopped) return;
    inFlight = (async () => {
      try {
        const result = await runOutboxIteration({ db, producer, config });

        const backlog = (db
          .query("SELECT COUNT(*) AS c FROM outbox WHERE status='pending'")
          .get() as { c: number }).c;
        if (backlog > config.backlogWarnThreshold) {
          log.warn({ backlog, threshold: config.backlogWarnThreshold }, "outbox backlog over threshold");
        }

        const nextDelay = result.scanned >= config.batchSize ? config.busyPollMs : config.idlePollMs;
        if (!stopped) {
          currentTimer = setTimeout(() => void tick(), nextDelay);
        }
      } catch (err) {
        log.error({ err }, "outbox drainer iteration crashed");
        if (!stopped) {
          currentTimer = setTimeout(() => void tick(), config.idlePollMs);
        }
      }
    })();
    await inFlight;
  };

  currentTimer = setTimeout(() => void tick(), config.busyPollMs);

  return {
    async stop() {
      stopped = true;
      if (currentTimer) clearTimeout(currentTimer);
      await inFlight;
    },
  };
}
