// src/replay/runner.ts
import { CircuitBreakerOpenError } from "../resilience/errors.ts";
import type { ReplayConfig, RouteConfig } from "../config/schemas.ts";
import type { EventProducer } from "../kafka/producer.ts";
import { getLogger } from "../logging/index.ts";
import {
  parseAttempt,
  readHeader,
  stampAuditHeaders,
  stripConnectHeaders,
} from "./headers.ts";
import { triage } from "./triage.ts";
import type { DlqRecord, ReplayBatchResult } from "./types.ts";

const log = getLogger("replay.runner");

export type DryRunOpts = {
  records: AsyncIterable<DlqRecord>;
  // Accepted for signature parity with the real runner; dry-run never calls it.
  producer: EventProducer;
};

// Counts records and tracks last offset; never produces. Useful as a "how
// many records would I scan" probe without invoking triage at all.
export async function runReplayBatchDryRun(opts: DryRunOpts): Promise<ReplayBatchResult> {
  let scanned = 0;
  let lastOffset: number | null = null;
  for await (const rec of opts.records) {
    scanned += 1;
    lastOffset = rec.offset;
  }
  return {
    scanned,
    replayed: 0,
    parked: 0,
    skipped: 0,
    errors: 0,
    lastOffset,
  };
}

export type RunReplayBatchOpts = {
  records: AsyncIterable<DlqRecord>;
  producer: EventProducer;
  cfg: ReplayConfig;
  jobId: string;
  route: RouteConfig;
  signal: AbortSignal;
  // Per-record counter snapshot for the caller's jobStore to persist. The
  // runner intentionally never writes status here — terminal transitions are
  // owned by the caller when the runner resolves.
  onProgress?: (
    snapshot: Pick<
      ReplayBatchResult,
      "scanned" | "replayed" | "parked" | "skipped" | "errors" | "lastOffset"
    >,
  ) => void;
};

export type RunReplayBatchResult = ReplayBatchResult & {
  // True iff the run paused due to CircuitBreakerOpenError. Caller persists
  // status='paused'. lastOffset still points at the record that triggered the
  // breaker so a resume picks up at the same offset.
  paused: boolean;
  lastError: string | null;
  nextResumeAt: number | null;
};

// Token-bucket rate limiter. Refills perSec/10 tokens every 100ms; acquire()
// awaits when empty. 100ms granularity is coarse enough to avoid CPU
// thrashing, fine enough that a 500/s rate doesn't burst.
class TokenBucket {
  private tokens: number;
  private readonly capacity: number;
  private readonly refillPerTick: number;
  private timer: ReturnType<typeof setInterval> | null = null;
  private waiters: Array<() => void> = [];

  constructor(perSec: number) {
    this.capacity = perSec;
    this.refillPerTick = Math.max(1, Math.round(perSec / 10));
    this.tokens = perSec;
  }

  start(): void {
    if (this.timer !== null) return;
    this.timer = setInterval(() => {
      this.tokens = Math.min(this.capacity, this.tokens + this.refillPerTick);
      while (this.tokens > 0 && this.waiters.length > 0) {
        this.tokens -= 1;
        const w = this.waiters.shift();
        if (w !== undefined) w();
      }
    }, 100);
    if (typeof (this.timer as { unref?: () => void }).unref === "function") {
      (this.timer as { unref: () => void }).unref();
    }
  }

  stop(): void {
    if (this.timer !== null) {
      clearInterval(this.timer);
      this.timer = null;
    }
    // Release any pending waiters so the promise chain unwinds cleanly.
    for (const w of this.waiters.splice(0)) w();
  }

  acquire(): Promise<void> {
    if (this.tokens > 0) {
      this.tokens -= 1;
      return Promise.resolve();
    }
    return new Promise((resolve) => {
      this.waiters.push(resolve);
    });
  }
}

// Bulk runner: iterates DLQ records, triages each, produces to source topic
// (replay) or parking topic (park; count-only when parkingTopicSuffix=""),
// rate-limited via token bucket. Pauses cleanly on CircuitBreakerOpenError
// per spec §"Breaker open mid-job": no attempt++, no maxAge giveup; the
// resume is a brand-new job from the operator/scheduler. Honours
// signal.aborted on every iteration (cancel respected promptly; spec
// §"Cancel semantics" accepts that one in-flight produce may still land).
export async function runReplayBatch(
  opts: RunReplayBatchOpts,
): Promise<RunReplayBatchResult> {
  const { records, producer, cfg, jobId, route, signal, onProgress } = opts;

  const bucket = new TokenBucket(cfg.rateLimitPerSec);
  bucket.start();

  let scanned = 0;
  let replayed = 0;
  let parked = 0;
  const skipped = 0;
  let errors = 0;
  let lastOffset: number | null = null;
  let paused = false;
  let lastError: string | null = null;
  let nextResumeAt: number | null = null;

  const emitProgress = (): void => {
    onProgress?.({ scanned, replayed, parked, skipped, errors, lastOffset });
  };

  try {
    for await (const rec of records) {
      if (signal.aborted) break;
      scanned += 1;
      lastOffset = rec.offset;

      const decision = triage(rec, cfg);
      const attempt = parseAttempt(
        readHeader(rec, "x-eventgate-replay-attempt"),
      );

      const targetTopic =
        decision.kind === "replay"
          ? route.topic
          : cfg.parkingTopicSuffix === ""
            ? null
            : route.topic + cfg.parkingTopicSuffix;

      if (targetTopic === null) {
        // Park with empty parkingTopicSuffix => count-only, no produce.
        parked += 1;
        emitProgress();
        continue;
      }

      // Park records do NOT bump the attempt counter (spec §"On park").
      const nextAttempt = decision.kind === "replay" ? attempt + 1 : attempt;
      const headersOut = stampAuditHeaders(stripConnectHeaders(rec.headers), {
        jobId,
        sourceTopic: rec.topic,
        partition: rec.partition,
        offset: rec.offset,
        attempt: nextAttempt,
      });
      const headersArr: Array<[string, string | Buffer | null]> = [];
      for (const [k, v] of headersOut) {
        if (k === null) continue;
        headersArr.push([k.toString("utf-8"), v]);
      }
      const keyStr = rec.key === null ? "" : rec.key.toString("utf-8");
      const valueStr =
        rec.value === null ? "" : rec.value.toString("utf-8");

      try {
        await bucket.acquire();
        if (signal.aborted) break;
        await producer.sendByTopic(targetTopic, keyStr, valueStr, headersArr);
        if (decision.kind === "replay") replayed += 1;
        else parked += 1;
      } catch (err) {
        if (err instanceof CircuitBreakerOpenError) {
          paused = true;
          lastError = "circuit_breaker_open";
          nextResumeAt = err.nextAttemptAt.getTime();
          log.info(
            {
              jobId,
              route: route.name,
              partition: rec.partition,
              offset: rec.offset,
              nextResumeAt,
            },
            "replay job paused: circuit breaker open",
          );
          emitProgress();
          break;
        }
        errors += 1;
        const message = err instanceof Error ? err.message : String(err);
        lastError = message;
        log.warn(
          {
            jobId,
            route: route.name,
            partition: rec.partition,
            offset: rec.offset,
            err: message,
          },
          "replay produce failed",
        );
      }

      emitProgress();
    }
  } finally {
    bucket.stop();
  }

  if (signal.aborted && !paused) {
    log.debug({ jobId }, "replay run cancelled");
  }

  return {
    scanned,
    replayed,
    parked,
    skipped,
    errors,
    lastOffset,
    paused,
    lastError,
    nextResumeAt,
  };
}
