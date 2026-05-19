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

// Rewrites legacy rows written before the topic-naming policy was introduced.
// Pre-change deployments only had one route ("raw"), so the only possible
// legacy value is "raw" -> T_PRIVATE_SOURCE_ELASTIC_AUTOOPS.
export function runOutboxMigrations(db: OutboxDatabase): void {
  db.run(
    "UPDATE outbox SET topic = 'T_PRIVATE_SOURCE_ELASTIC_AUTOOPS' WHERE topic = 'raw'",
  );
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
