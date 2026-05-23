// src/replay/jobStore.ts
import { randomUUID } from "node:crypto";
import type { ReplayJob, ReplayJobMode } from "./types.ts";

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
  // update merges patch into the existing row. Terminal-state stickiness
  // (Risk 5 from the spec) lands in Phase 3 alongside the SQLite-backed
  // store; the Phase 1 stub mirrors the same semantics via a status-IN guard.
  update(id: string, patch: Partial<ReplayJob>): void;
  cancel(id: string): boolean;
  hasActiveJob(route: string, partition: number): boolean;
  setCancelHandle(id: string, ctl: AbortController): void;
  cancelAll(): void;
};

const TERMINAL = new Set<ReplayJob["status"]>(["done", "failed", "cancelled"]);

// Phase 1 in-memory implementation. Phase 3 swaps this for a SQLite-backed
// store against replay_jobs + replay_state. The interface is stable so the
// admin endpoint and gateway wiring do not change between phases.
export function createReplayJobStore(): ReplayJobStore {
  const jobs = new Map<string, ReplayJob>();
  const handles = new Map<string, AbortController>();

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
      // Terminal-state stickiness: once a job is done/failed/cancelled, the
      // only allowed transition is more counters, never a status change.
      const next: ReplayJob = { ...cur, ...patch };
      if (TERMINAL.has(cur.status) && patch.status !== undefined && patch.status !== cur.status) {
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
  };
}
