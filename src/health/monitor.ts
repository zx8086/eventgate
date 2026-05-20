// src/health/monitor.ts
import { getLogger, type ILogger } from "../logging/index.ts";
import type { OutboxDatabase } from "../outbox/db.ts";
import { probeOutboxDb, probeKafkaAdmin, type AdminLike } from "./probes.ts";
import {
  DEFAULT_REQUIREDNESS,
  type DependencyName,
  type HealthSnapshot,
} from "./types.ts";

export type HealthMonitor = {
  start(): Promise<void>;
  stop(): Promise<void>;
  snapshot(): HealthSnapshot;
  probeOnce(): Promise<void>;
};

type ProducerLike = { isConnected(): boolean };

export type HealthMonitorOptions = {
  producer: ProducerLike;
  outboxDb?: OutboxDatabase;
  admin: AdminLike;
  expectedTopics: string[];
  probeIntervalMs: number;
  probeTimeoutMs: number;
  logger?: ILogger;
};

function emptySnapshot(): HealthSnapshot {
  return {
    status: "healthy",
    ok: true,
    checkedAt: 0,
    dependencies: {},
  };
}

export function createHealthMonitor(opts: HealthMonitorOptions): HealthMonitor {
  const log = opts.logger ?? getLogger("gateway.health");
  let current: HealthSnapshot = emptySnapshot();
  let stopped = false;
  let timer: ReturnType<typeof setTimeout> | undefined;
  let inFlight: Promise<void> = Promise.resolve();

  const runProbeCycle = async (): Promise<HealthSnapshot> => {
    const checkedAt = Date.now();
    const producerOk = opts.producer.isConnected();
    const deps: HealthSnapshot["dependencies"] = {
      kafkaProducer: { ok: producerOk, lastCheckedAt: checkedAt, connected: producerOk },
    };

    if (opts.outboxDb) {
      deps.outboxDb = probeOutboxDb(opts.outboxDb);
    }

    try {
      const kafkaResult = await probeKafkaAdmin({
        admin: opts.admin,
        expectedTopics: opts.expectedTopics,
        timeoutMs: opts.probeTimeoutMs,
      });
      deps.kafkaBroker = kafkaResult.broker;
      deps.topics = kafkaResult.topics;
    } catch (err) {
      // probeKafkaAdmin already catches internally; this is a defensive guard.
      const message = err instanceof Error ? err.message : String(err);
      const at = Date.now();
      deps.kafkaBroker = { ok: false, lastCheckedAt: at, lastError: message };
      deps.topics = { ok: false, lastCheckedAt: at, lastError: "broker probe failed; topic check skipped", missing: opts.expectedTopics };
    }

    const required = DEFAULT_REQUIREDNESS.required;
    const requiredOk = required.every((name) => deps[name]?.ok ?? false);
    const allOk = requiredOk && (deps.topics?.ok ?? true);
    const status: HealthSnapshot["status"] = !requiredOk ? "unhealthy" : !allOk ? "degraded" : "healthy";

    return { status, ok: requiredOk, checkedAt, dependencies: deps };
  };

  const diffAndLog = (prev: HealthSnapshot, next: HealthSnapshot): void => {
    const names: DependencyName[] = ["kafkaProducer", "outboxDb", "kafkaBroker", "topics"];
    for (const name of names) {
      const before = prev.dependencies[name];
      const after = next.dependencies[name];
      if (after === undefined) continue;
      const beforeOk = before?.ok;
      // First-cycle dependencies (no `before` recorded) are not logged as a
      // transition — only true healthy<->unhealthy flips after at least one
      // baseline cycle generate a "dependency state changed" line.
      if (beforeOk === undefined) continue;
      if (beforeOk === after.ok) continue;
      log.info(
        {
          dependency: name,
          ok: after.ok,
          ...(after.lastError ? { lastError: after.lastError } : {}),
          ...(after.missing && after.missing.length > 0 ? { missing: after.missing } : {}),
        },
        "dependency state changed",
      );
    }
  };

  const cycle = async (): Promise<void> => {
    if (stopped) return;
    try {
      const next = await runProbeCycle();
      diffAndLog(current, next);
      current = next;
    } catch (err) {
      // Telemetry must never crash the monitor loop.
      log.warn({ err: err instanceof Error ? err.message : String(err) }, "health monitor cycle failed");
    }
    if (!stopped) {
      timer = setTimeout(() => {
        inFlight = cycle();
      }, opts.probeIntervalMs);
      if (typeof (timer as { unref?: () => void }).unref === "function") {
        (timer as { unref: () => void }).unref();
      }
    }
  };

  return {
    async start() {
      // First cycle awaited so /healthz never serves an empty snapshot.
      current = await runProbeCycle();
      if (!stopped) {
        timer = setTimeout(() => {
          inFlight = cycle();
        }, opts.probeIntervalMs);
        if (typeof (timer as { unref?: () => void }).unref === "function") {
          (timer as { unref: () => void }).unref();
        }
      }
    },
    async stop() {
      stopped = true;
      if (timer) clearTimeout(timer);
      await inFlight;
    },
    snapshot() {
      return current;
    },
    async probeOnce() {
      const next = await runProbeCycle();
      diffAndLog(current, next);
      current = next;
    },
  };
}
