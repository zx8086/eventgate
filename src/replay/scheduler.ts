// src/replay/scheduler.ts
import type { ReplayConfig, RouteConfig } from "../config/schemas.ts";
import type { EventProducer } from "../kafka/producer.ts";
import { getLogger } from "../logging/index.ts";
import type { ReplayConsumer } from "./consumer.ts";
import type { DlqDepthCache } from "./dlqInspector.ts";
import type { ReplayJobStore } from "./jobStore.ts";
import { runReplayBatch } from "./runner.ts";

const log = getLogger("replay.scheduler");

export type ReplayScheduler = {
  start(): void;
  stop(): Promise<void>;
  // Run a single tick synchronously; used by tests.
  tick(): Promise<void>;
};

export type SchedulerDeps = {
  cfg: ReplayConfig;
  routes: ReadonlyArray<RouteConfig>;
  dlqDepth: DlqDepthCache;
  jobStore: ReplayJobStore;
  producer: EventProducer;
  createConsumer: (
    route: RouteConfig,
    jobId: string,
  ) => Promise<ReplayConsumer>;
};

// Iterates (route, partition) pairs sequentially per tick. Spec §"Scheduler":
// one in-flight job per route+partition (via hasActiveJob); overlapping ticks
// skip via tickInFlight flag; sequential not parallel to avoid broker thrash.
// Spec §"Cancel/breaker-pause race" applies — runner pauses on breaker open;
// scheduler picks it up on the next tick as a fresh job.
export function createReplayScheduler(deps: SchedulerDeps): ReplayScheduler {
  const { cfg, routes, dlqDepth, jobStore, producer, createConsumer } = deps;
  let tickInFlight = false;
  let stopped = false;
  let timer: ReturnType<typeof setTimeout> | null = null;
  let currentRun: Promise<void> = Promise.resolve();

  async function runOnePartition(
    route: RouteConfig,
    partition: number,
  ): Promise<void> {
    if (jobStore.hasActiveJob(route.name, partition)) return;
    // Resume from the per-partition watermark if one exists; otherwise start
    // from the earliest record the consumer can fetch (offset 0; broker may
    // serve a higher earliest if retention has expired older records).
    const watermark = jobStore.getLastReplayedOffset(route.name, partition);
    const fromOffset = watermark === null ? 0 : watermark + 1;
    const job = jobStore.create({
      route: route.name,
      partition,
      mode: "auto",
      dryRun: false,
      fromOffset,
    });
    const ctl = new AbortController();
    jobStore.setCancelHandle(job.id, ctl);
    jobStore.update(job.id, { status: "running" });

    try {
      const consumer = await createConsumer(route, job.id);
      try {
        const records = consumer.streamRange({
          partition,
          fromOffset,
          maxRecords: cfg.maxRecordsPerJob,
          signal: ctl.signal,
        });
        const result = await runReplayBatch({
          records,
          producer,
          cfg,
          jobId: job.id,
          route,
          signal: ctl.signal,
          onProgress: (snap) => jobStore.update(job.id, snap),
        });
        if (result.lastOffset !== null) {
          jobStore.setLastReplayedOffset(
            route.name,
            partition,
            result.lastOffset,
          );
        }
        const terminal = result.paused ? "paused" : "done";
        jobStore.update(job.id, {
          scanned: result.scanned,
          replayed: result.replayed,
          parked: result.parked,
          skipped: result.skipped,
          errors: result.errors,
          lastOffset: result.lastOffset,
          status: terminal,
          lastError: result.lastError,
          nextResumeAt: result.nextResumeAt,
          finishedAt: Date.now(),
        });
      } finally {
        await consumer.close();
      }
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      log.warn(
        { jobId: job.id, route: route.name, partition, err: message },
        "scheduler auto-replay run failed",
      );
      jobStore.update(job.id, {
        status: "failed",
        lastError: message,
        finishedAt: Date.now(),
      });
    }
  }

  async function tick(): Promise<void> {
    if (tickInFlight) {
      log.debug("scheduler tick skipped: previous tick still in flight");
      return;
    }
    tickInFlight = true;
    try {
      for (const route of routes) {
        if (stopped) return;
        const partitions = dlqDepth.get(route.name);
        for (const p of partitions) {
          if (stopped) return;
          if (p.depth === null) continue; // depth unknown (Redpanda fallback)
          if (p.depth < cfg.auto.dlqDepthThreshold) continue;
          if (p.partition < 0) continue; // sentinel
          await runOnePartition(route, p.partition);
        }
      }
    } finally {
      tickInFlight = false;
    }
  }

  function schedule(): void {
    if (stopped) return;
    timer = setTimeout(() => {
      currentRun = tick();
      void currentRun.finally(schedule);
    }, cfg.auto.intervalMs);
    if (typeof (timer as { unref?: () => void }).unref === "function") {
      (timer as { unref: () => void }).unref();
    }
  }

  return {
    start() {
      schedule();
    },
    async stop() {
      stopped = true;
      if (timer !== null) {
        clearTimeout(timer);
        timer = null;
      }
      await currentRun;
    },
    tick,
  };
}
