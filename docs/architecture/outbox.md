# SQLite Outbox (eventgate)

> Local durability buffer between HTTP accept and Kafka publish. Uses `bun:sqlite` — no external service, no extra process.

## Problem

Before the outbox, `src/gateway/routes.ts` published to Kafka inline. If Kafka was unreachable the call threw, the handler logged a `warn`, and still returned `202`. AutoOps does not retry, so an outage permanently dropped events.

## Solution

The webhook path now writes the raw event into a local SQLite table and returns `202`. A background drainer in the same Bun process publishes pending rows to Kafka with exponential backoff. Downstream consumers can dedupe on the opportunistic `idempotencyKey` Kafka header, so at-least-once delivery is safe.

## Flow

```
POST /webhooks/elastic/autoops
   │
   ▼ parse JSON
   │
   ▼ outbox.enqueue(rawRow)
   │
   ▼ 202 Accepted

  meanwhile, in the same Bun process:

  OutboxDrainer (busy: busyPollMs, idle: idlePollMs)
    SELECT … WHERE status='pending' AND next_attempt_at<=now LIMIT batchSize
    for row: producer.sendByTopic(...)   // publishes to raw.v1
      success → UPDATE status='dispatched', dispatched_at=now
      failure → UPDATE attempts++, next_attempt_at=now+backoff(attempts),
                last_error=…; if (now - created_at) > maxAgeMs → status='failed'
```

## File layout

```
src/outbox/
  db.ts          openOutbox(dbPath) — Database + WAL + macOS sidecar cleanup on close
  schemas.ts     Zod OutboxRow, OutboxTopic, OutboxStatus
  writer.ts      createWriter(db) → { enqueue, backlogStats }
  drainer.ts     runOutboxIteration (pure-testable) + startDrainer (loop)
  backoff.ts     nextDelayMs(attempts, capMs) — min(2^(attempts-1) * 1s, capMs)
```

Wiring lives in `src/gateway/index.ts` and `src/gateway/routes.ts`.

## Schema

```sql
CREATE TABLE IF NOT EXISTS outbox (
  id              TEXT PRIMARY KEY,         -- crypto.randomUUID()
  topic           TEXT NOT NULL,            -- raw (only value currently written)
  message_key     TEXT NOT NULL,
  payload         TEXT NOT NULL,            -- JSON.stringify(value)
  headers         TEXT,                     -- JSON object or NULL
  status          TEXT NOT NULL DEFAULT 'pending',  -- pending|dispatched|failed
  attempts        INTEGER NOT NULL DEFAULT 0,
  next_attempt_at INTEGER NOT NULL,         -- epoch ms
  created_at      INTEGER NOT NULL,         -- epoch ms
  dispatched_at   INTEGER,
  last_error      TEXT
);
CREATE INDEX IF NOT EXISTS idx_outbox_drain
  ON outbox (status, next_attempt_at);

PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
```

On close (`closeOutbox`), the code runs `db.fileControl(SQLITE_FCNTL_PERSIST_WAL, 0)` followed by `PRAGMA wal_checkpoint(TRUNCATE)`. Apple's system SQLite (used by Bun on macOS) enables persistent WAL by default; without these calls the `-wal` and `-shm` sidecar files linger after shutdown.

## Topic mapping

The drainer is topic-agnostic. Each row stores the exact Kafka topic name it should publish to (from `route.topic` at enqueue time); the drainer just hands that string to `producer.sendByTopic` and lets `@platformatic/kafka` route it. Renaming a route's topic in config affects newly enqueued rows only; previously enqueued, undispatched rows continue to publish to the topic they were enqueued with.

## Delivery semantics

- **At-least-once** to Kafka. Downstream consumers may dedupe on the opportunistic `idempotencyKey` Kafka header (`src/kafka/producer.ts`), set by the gateway when the body looks AutoOps-shaped.
- **Exponential backoff**: `min(2^(attempts-1) * 1000ms, backoffMaxMs)`. Default cap 10 minutes. The first retry waits 1s; the curve is 1, 2, 4, 8, 16, 32, 64, …, then flat at the cap.
- **Give-up rule is age-based, not count-based**: when `now - created_at > maxAgeHours * 3600 * 1000`, the row transitions to `status='failed'`, the drainer logs a warn with `idempotencyKey`, and the row is surfaced in `/healthz`. Default 24 hours.
- **Backpressure**: when pending rows exceed `backlogWarnThreshold` (default 50k), the drainer logs a warn each iteration. The gateway never returns 503 from backlog — the right behavior is to buffer.

## Config

```
config.outbox.{
  enabled: boolean = true
  dbPath: string = "./data/outbox.db"
  batchSize: number = 100
  backoffMaxMs: number = 600_000
  maxAgeHours: number = 24
  idlePollMs: number = 5_000
  busyPollMs: number = 250
  backlogWarnThreshold: number = 50_000
}
```

Env vars: `OUTBOX_ENABLED`, `OUTBOX_DB_PATH`, `OUTBOX_BATCH_SIZE`, `OUTBOX_BACKOFF_MAX_MS`, `OUTBOX_MAX_AGE_HOURS`, `OUTBOX_IDLE_POLL_MS`, `OUTBOX_BUSY_POLL_MS`, `OUTBOX_BACKLOG_WARN`. `OUTBOX_ENABLED` accepts `true|false|1|0|yes|no` (case-insensitive).

## `/healthz`

```json
{
  "ok": true,
  "producer": { "connected": true },
  "outbox": {
    "enabled": true,
    "pending": 0,
    "failed": 0,
    "oldestPendingAgeMs": 0
  }
}
```

When `OUTBOX_ENABLED=false`, the outbox block degrades to `{ "enabled": false }`. `ok` follows `producer.connected` — a stuck outbox does not flip readiness, but `oldestPendingAgeMs` makes a degradation visible to dashboards.

## Escape hatch

Setting `OUTBOX_ENABLED=false` switches the gateway back to inline publish (the pre-outbox behavior). No SQLite file is opened. Useful when diagnosing a SQLite-layer problem in production without redeploying. A `warn` log line is emitted at startup so this state is obvious in logs.

## Testing

- `outbox.backoff.test.ts` — pure backoff curve.
- `outbox.writer.test.ts` — single-row enqueue, header JSON serialization, backlog stats.
- `outbox.drainer.test.ts` — drives `runOutboxIteration` with a fake producer; covers publish, retry-with-backoff, age-based fail, future-row skip, batch-size limit, header forwarding.
- `config.outbox.test.ts` — defaults, env overrides (incl. boolean parsing), schema invariants.

All outbox tests use `:memory:` SQLite. No Kafka required.

## Operational notes

- The DB file path is created with `mkdirSync(dirname(dbPath), { recursive: true })` at startup. Default location is `./data/outbox.db` — gitignored.
- The drainer is a single in-process loop; do not run two gateway processes against the same `dbPath`. If you need horizontal scale, run separate gateways with separate DB paths and partition upstream traffic.
- Process restart durability is provided by SQLite itself. Pending rows resume on next startup.

## Out of scope

- CDC-style outbox draining (Debezium etc.) — eventgate is the publisher.
- Multi-process drainer coordination.
- CLI for replaying `failed` rows (defer until first incident calls for it).
- Bun Worker threads for the drainer (single-process, in-loop is sufficient at this volume).
- Exactly-once via Kafka transactions (downstream already dedupes).
