// src/replay/jobStore.ts
import { randomUUID } from "node:crypto";
import type { OutboxDatabase } from "../outbox/db.ts";
import type { ReplayJob, ReplayJobMode, ReplayJobStatus } from "./types.ts";

export type ReplayJobCreate = {
  route: string;
  partition: number;
  mode: ReplayJobMode;
  dryRun: boolean;
  fromOffset?: number;
  toOffset?: number;
};

export type ReplayJobStore = {
  create(input: ReplayJobCreate): ReplayJob;
  get(id: string): ReplayJob | null;
  // update merges patch into the existing row. Terminal-state stickiness:
  // once a job is done/failed/cancelled, status patches are silently dropped
  // (counters still update). Spec §"Cancel/breaker-pause race": the cancel
  // path and the runner's terminal write resolve to whichever lands first.
  update(id: string, patch: Partial<ReplayJob>): void;
  cancel(id: string): boolean;
  hasActiveJob(route: string, partition: number): boolean;
  setCancelHandle(id: string, ctl: AbortController): void;
  cancelAll(): void;
  // Phase 3+: returns the most recently started job for a route (any
  // partition); used by GET /admin/dlq to show last-job summary.
  lastJobForRoute(route: string): ReplayJob | null;
  // Phase 3+: per-partition watermark; advanced by the bulk runner after each
  // record. Resume jobs start from the watermark instead of offset 0.
  setLastReplayedOffset(route: string, partition: number, offset: number): void;
  getLastReplayedOffset(route: string, partition: number): number | null;
};

const TERMINAL: ReadonlySet<ReplayJobStatus> = new Set([
  "done",
  "failed",
  "cancelled",
]);

// Default in-memory implementation. Phase 1 default; still useful for tests
// (createSqliteReplayJobStore demands a live DB).
export function createReplayJobStore(): ReplayJobStore {
  const jobs = new Map<string, ReplayJob>();
  const handles = new Map<string, AbortController>();
  const watermarks = new Map<string, number>(); // key = `${route}:${partition}`

  return {
    create(input) {
      const id = randomUUID();
      const job: ReplayJob = {
        id,
        route: input.route,
        partition: input.partition,
        mode: input.mode,
        dryRun: input.dryRun,
        scanned: 0,
        replayed: 0,
        parked: 0,
        skipped: 0,
        errors: 0,
        fromOffset: input.fromOffset ?? null,
        toOffset: input.toOffset ?? null,
        lastOffset: null,
        status: "pending",
        lastError: null,
        nextResumeAt: null,
        startedAt: Date.now(),
        finishedAt: null,
      };
      jobs.set(id, job);
      return job;
    },

    get(id) {
      return jobs.get(id) ?? null;
    },

    update(id, patch) {
      const cur = jobs.get(id);
      if (cur === undefined) return;
      const next: ReplayJob = { ...cur, ...patch };
      if (
        TERMINAL.has(cur.status) &&
        patch.status !== undefined &&
        patch.status !== cur.status
      ) {
        next.status = cur.status;
      }
      jobs.set(id, next);
    },

    cancel(id) {
      const cur = jobs.get(id);
      if (cur === undefined) return false;
      if (TERMINAL.has(cur.status)) return false;
      handles.get(id)?.abort();
      jobs.set(id, { ...cur, status: "cancelled", finishedAt: Date.now() });
      return true;
    },

    hasActiveJob(route, partition) {
      for (const j of jobs.values()) {
        if (j.route !== route || j.partition !== partition) continue;
        if (!TERMINAL.has(j.status)) return true;
      }
      return false;
    },

    setCancelHandle(id, ctl) {
      handles.set(id, ctl);
    },

    cancelAll() {
      for (const ctl of handles.values()) ctl.abort();
    },

    lastJobForRoute(route) {
      let best: ReplayJob | null = null;
      for (const j of jobs.values()) {
        if (j.route !== route) continue;
        if (best === null || j.startedAt > best.startedAt) best = j;
      }
      return best;
    },

    setLastReplayedOffset(route, partition, offset) {
      watermarks.set(`${route}:${partition}`, offset);
    },

    getLastReplayedOffset(route, partition) {
      return watermarks.get(`${route}:${partition}`) ?? null;
    },
  };
}

// SQLite-backed implementation. Persists across gateway restarts; combined
// with sweepReplayGhostJobs at startup, ensures orphaned rows are reconciled.
type DbRow = {
  id: string;
  route: string;
  partition: number;
  mode: string;
  dry_run: number;
  scanned: number;
  replayed: number;
  parked: number;
  skipped: number;
  errors: number;
  from_offset: number | null;
  to_offset: number | null;
  last_offset: number | null;
  status: string;
  last_error: string | null;
  next_resume_at: number | null;
  started_at: number;
  finished_at: number | null;
};

function rowToJob(row: DbRow): ReplayJob {
  return {
    id: row.id,
    route: row.route,
    partition: row.partition,
    mode: row.mode as ReplayJobMode,
    dryRun: row.dry_run === 1,
    scanned: row.scanned,
    replayed: row.replayed,
    parked: row.parked,
    skipped: row.skipped,
    errors: row.errors,
    fromOffset: row.from_offset,
    toOffset: row.to_offset,
    lastOffset: row.last_offset,
    status: row.status as ReplayJobStatus,
    lastError: row.last_error,
    nextResumeAt: row.next_resume_at,
    startedAt: row.started_at,
    finishedAt: row.finished_at,
  };
}

export function createSqliteReplayJobStore(db: OutboxDatabase): ReplayJobStore {
  // AbortControllers live in memory; they're inherently per-process anyway.
  const handles = new Map<string, AbortController>();

  const insertStmt = db.query(
    `INSERT INTO replay_jobs
       (id, route, partition, mode, dry_run, scanned, replayed, parked, skipped, errors,
        from_offset, to_offset, last_offset, status, last_error, next_resume_at, started_at, finished_at)
     VALUES
       ($id, $route, $partition, $mode, $dry_run, 0, 0, 0, 0, 0,
        $from_offset, $to_offset, NULL, 'pending', NULL, NULL, $started_at, NULL)`,
  );

  const getStmt = db.query("SELECT * FROM replay_jobs WHERE id = $id");

  const lastByRouteStmt = db.query(
    "SELECT * FROM replay_jobs WHERE route = $route ORDER BY started_at DESC LIMIT 1",
  );

  // Sticky-terminal cancel: only flip to 'cancelled' if the row is currently
  // in a non-terminal state. Returns 0 changes when the row is already
  // terminal (or missing).
  const cancelStmt = db.query(
    `UPDATE replay_jobs
        SET status = 'cancelled', finished_at = $now
      WHERE id = $id AND status NOT IN ('done', 'failed', 'cancelled')`,
  );

  const activeStmt = db.query(
    `SELECT COUNT(*) AS c FROM replay_jobs
      WHERE route = $route AND partition = $partition
        AND status NOT IN ('done', 'failed', 'cancelled')`,
  );

  const watermarkUpsertStmt = db.query(
    `INSERT INTO replay_state (route, partition, last_replayed_offset, updated_at)
     VALUES ($route, $partition, $offset, $now)
     ON CONFLICT(route, partition) DO UPDATE SET
       last_replayed_offset = excluded.last_replayed_offset,
       updated_at = excluded.updated_at`,
  );

  const watermarkGetStmt = db.query(
    "SELECT last_replayed_offset AS o FROM replay_state WHERE route = $route AND partition = $partition",
  );

  return {
    create(input) {
      const id = randomUUID();
      const startedAt = Date.now();
      insertStmt.run({
        id,
        route: input.route,
        partition: input.partition,
        mode: input.mode,
        dry_run: input.dryRun ? 1 : 0,
        from_offset: input.fromOffset ?? null,
        to_offset: input.toOffset ?? null,
        started_at: startedAt,
      });
      return {
        id,
        route: input.route,
        partition: input.partition,
        mode: input.mode,
        dryRun: input.dryRun,
        scanned: 0,
        replayed: 0,
        parked: 0,
        skipped: 0,
        errors: 0,
        fromOffset: input.fromOffset ?? null,
        toOffset: input.toOffset ?? null,
        lastOffset: null,
        status: "pending",
        lastError: null,
        nextResumeAt: null,
        startedAt,
        finishedAt: null,
      };
    },

    get(id) {
      const row = getStmt.get({ id }) as DbRow | null;
      return row === null ? null : rowToJob(row);
    },

    // Build UPDATE dynamically from the patch keys present so we don't write
    // columns we didn't intend to. Terminal-state stickiness is enforced via
    // the WHERE NOT IN clause when the patch includes a status change.
    // bun:sqlite's .run() binding type doesn't accept Record<string, unknown>
    // directly (each key must be a primitive); we narrow at the boundary.
    update(id, patch) {
      type Bindable = string | number | bigint | boolean | Uint8Array | null;
      const sets: string[] = [];
      const binds: Record<string, Bindable> = { id };
      const map: Record<string, string> = {
        scanned: "scanned",
        replayed: "replayed",
        parked: "parked",
        skipped: "skipped",
        errors: "errors",
        fromOffset: "from_offset",
        toOffset: "to_offset",
        lastOffset: "last_offset",
        status: "status",
        lastError: "last_error",
        nextResumeAt: "next_resume_at",
        finishedAt: "finished_at",
      };
      for (const [k, col] of Object.entries(map)) {
        if (k in patch) {
          sets.push(`${col} = $${col}`);
          const raw = (patch as Record<string, unknown>)[k];
          if (raw === undefined || raw === null) {
            binds[col] = null;
            continue;
          }
          if (
            typeof raw === "string" ||
            typeof raw === "number" ||
            typeof raw === "boolean" ||
            typeof raw === "bigint"
          ) {
            binds[col] = raw;
            continue;
          }
          // ReplayJob fields are all primitive-typed (status enums, numbers,
          // strings, null); anything else here is a caller bug. Stringify so
          // we don't silently drop data; tests will catch unexpected shapes.
          binds[col] = String(raw);
        }
      }
      if (sets.length === 0) return;

      const guard =
        "status" in patch
          ? " AND status NOT IN ('done', 'failed', 'cancelled')"
          : "";
      db.query(
        `UPDATE replay_jobs SET ${sets.join(", ")} WHERE id = $id${guard}`,
      ).run(binds);
    },

    cancel(id) {
      const result = cancelStmt.run({ id, now: Date.now() });
      const cancelled = result.changes > 0;
      if (cancelled) handles.get(id)?.abort();
      return cancelled;
    },

    hasActiveJob(route, partition) {
      const row = activeStmt.get({ route, partition }) as { c: number };
      return row.c > 0;
    },

    setCancelHandle(id, ctl) {
      handles.set(id, ctl);
    },

    cancelAll() {
      for (const ctl of handles.values()) ctl.abort();
    },

    lastJobForRoute(route) {
      const row = lastByRouteStmt.get({ route }) as DbRow | null;
      return row === null ? null : rowToJob(row);
    },

    setLastReplayedOffset(route, partition, offset) {
      watermarkUpsertStmt.run({
        route,
        partition,
        offset,
        now: Date.now(),
      });
    },

    getLastReplayedOffset(route, partition) {
      const row = watermarkGetStmt.get({ route, partition }) as
        | { o: number }
        | null;
      return row === null ? null : row.o;
    },
  };
}
