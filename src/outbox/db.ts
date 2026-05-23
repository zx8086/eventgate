// src/outbox/db.ts
import { Database, constants } from "bun:sqlite";

export type OutboxDatabase = Database;

const SCHEMA = `
CREATE TABLE IF NOT EXISTS outbox (
  id              TEXT PRIMARY KEY,
  topic           TEXT NOT NULL,
  message_key     TEXT NOT NULL,
  payload         TEXT NOT NULL,
  headers         TEXT,
  status          TEXT NOT NULL DEFAULT 'pending',
  attempts        INTEGER NOT NULL DEFAULT 0,
  next_attempt_at INTEGER NOT NULL,
  created_at      INTEGER NOT NULL,
  dispatched_at   INTEGER,
  last_error      TEXT
);
CREATE INDEX IF NOT EXISTS idx_outbox_drain ON outbox (status, next_attempt_at);
`;

// Replay subsystem tables (SIO-827 Phase 3). Created via runReplayMigrations
// ONLY when config.replay?.enabled === true, so sites that never use replay
// keep a clean SQLite file. Offsets stored as INTEGER (Kafka offsets fit in
// 2^53); per-partition watermark in replay_state with (route, partition)
// composite PK.
const REPLAY_SCHEMA = `
CREATE TABLE IF NOT EXISTS replay_jobs (
  id              TEXT PRIMARY KEY,
  route           TEXT NOT NULL,
  partition       INTEGER NOT NULL,
  mode            TEXT NOT NULL,
  dry_run         INTEGER NOT NULL,
  scanned         INTEGER NOT NULL DEFAULT 0,
  replayed        INTEGER NOT NULL DEFAULT 0,
  parked          INTEGER NOT NULL DEFAULT 0,
  skipped         INTEGER NOT NULL DEFAULT 0,
  errors          INTEGER NOT NULL DEFAULT 0,
  from_offset     INTEGER,
  to_offset       INTEGER,
  last_offset     INTEGER,
  status          TEXT NOT NULL DEFAULT 'pending',
  last_error      TEXT,
  next_resume_at  INTEGER,
  started_at      INTEGER NOT NULL,
  finished_at     INTEGER
);
CREATE INDEX IF NOT EXISTS idx_replay_jobs_route_started ON replay_jobs (route, started_at DESC);

CREATE TABLE IF NOT EXISTS replay_state (
  route                 TEXT NOT NULL,
  partition             INTEGER NOT NULL,
  last_replayed_offset  INTEGER NOT NULL,
  updated_at            INTEGER NOT NULL,
  PRIMARY KEY (route, partition)
);
`;

// Rewrites legacy rows written before the topic-naming policy was introduced.
// Pre-change deployments only had one route ("raw"), so the only possible
// legacy value is "raw" -> T_PRIVATE_SOURCE_ELASTIC_AUTOOPS.
export function runOutboxMigrations(db: OutboxDatabase): void {
  db.run(
    "UPDATE outbox SET topic = 'T_PRIVATE_SOURCE_ELASTIC_AUTOOPS' WHERE topic = 'raw'",
  );
}

export function runReplayMigrations(db: OutboxDatabase): void {
  db.exec(REPLAY_SCHEMA);
}

// Ghost-job sweep: gateway startup runs this after replay migrations to
// reconcile rows orphaned by a previous crash. status='pending' / 'running' /
// 'paused' rows that survived restart cannot resume (their AbortController
// died with the process) — mark them failed with a clear lastError so
// operators see ghosts as failed, not forever-running.
export function sweepReplayGhostJobs(db: OutboxDatabase): number {
  const now = Date.now();
  const result = db
    .query(
      `UPDATE replay_jobs
          SET status = 'failed',
              last_error = 'gateway_restart_orphan',
              finished_at = $now
        WHERE status IN ('pending', 'running', 'paused')`,
    )
    .run({ now });
  return result.changes;
}

export function openOutbox(dbPath: string): OutboxDatabase {
  const db = new Database(dbPath, { create: true, strict: true });
  if (dbPath !== ":memory:") {
    db.run("PRAGMA journal_mode = WAL;");
    db.run("PRAGMA synchronous = NORMAL;");
  }
  db.run("PRAGMA foreign_keys = ON;");
  db.exec(SCHEMA);
  runOutboxMigrations(db);
  return db;
}

export function closeOutbox(db: OutboxDatabase): void {
  // Prevent -wal/-shm sidecars from lingering on macOS (Apple's system SQLite
  // builds WAL with persistence on by default; bun:sqlite uses it on darwin).
  try {
    db.fileControl(constants.SQLITE_FCNTL_PERSIST_WAL, 0);
    db.run("PRAGMA wal_checkpoint(TRUNCATE);");
  } catch {
    // file-mode-only operations; on :memory: these may throw, that's fine.
  }
  db.close(false);
}
