// src/gateway/index.ts
import { dirname } from "node:path";
import { mkdirSync } from "node:fs";
import { makeReplayHandlers } from "../admin/replayEndpoint.ts";
import { config } from "../config/index.ts";
import { resetConfigCache } from "../config/loader.ts";
import type { RouteConfig } from "../config/schemas.ts";
import { createHealthAdmin } from "../health/admin.ts";
import { createHealthMonitor } from "../health/monitor.ts";
import { getLogger } from "../logging/index.ts";
import { startHeartbeat, type HeartbeatHandle } from "../logging/heartbeat.ts";
import { createProducerHandle, type ProducerHandle } from "../kafka/producerHandle.ts";
import { createKafkaProvider } from "../kafka/providers/index.ts";
import {
  closeOutbox,
  openOutbox,
  runReplayMigrations,
  sweepReplayGhostJobs,
  type OutboxDatabase,
} from "../outbox/db.ts";
import { startDrainer, type DrainerHandle } from "../outbox/drainer.ts";
import { createDrainMetrics } from "../outbox/metrics.ts";
import { createWriter, type OutboxWriter } from "../outbox/writer.ts";
import { createReplayConsumer } from "../replay/consumer.ts";
import {
  createDlqInspector,
  type DlqDepthCache,
} from "../replay/dlqInspector.ts";
import {
  createReplayJobStore,
  createSqliteReplayJobStore,
  type ReplayJobStore,
} from "../replay/jobStore.ts";
import { createReplayScheduler, type ReplayScheduler } from "../replay/scheduler.ts";
import { buildRoutes, type ReplayContext } from "./routes.ts";

const log = getLogger("gateway");

const provider = createKafkaProvider(config);
log.info({ provider: provider.name, providerType: provider.type }, "kafka provider selected");

const drainMetrics = createDrainMetrics();

const producer: ProducerHandle = await createProducerHandle(
  config.kafka.clientId,
  provider,
  {
    failureThreshold: config.breaker.failureThreshold,
    successThreshold: config.breaker.successThreshold,
    recoveryTimeoutMs: config.breaker.recoveryTimeoutMs,
  },
  () => drainMetrics.incrementBreakerOpenCount(),
);

let outboxDb: OutboxDatabase | undefined;
let outboxWriter: OutboxWriter | undefined;
let drainer: DrainerHandle | undefined;

if (config.outbox.enabled) {
  if (config.outbox.dbPath !== ":memory:") {
    mkdirSync(dirname(config.outbox.dbPath), { recursive: true });
  }
  outboxDb = openOutbox(config.outbox.dbPath);
  outboxWriter = createWriter(outboxDb);
  drainer = startDrainer({
    db: outboxDb,
    producer,
    config: {
      batchSize: config.outbox.batchSize,
      backoffMaxMs: config.outbox.backoffMaxMs,
      maxAgeMs: config.outbox.maxAgeHours * 60 * 60 * 1_000,
      idlePollMs: config.outbox.idlePollMs,
      busyPollMs: config.outbox.busyPollMs,
      backlogWarnThreshold: config.outbox.backlogWarnThreshold,
    },
    metrics: drainMetrics,
  });
  log.info(
    { dbPath: config.outbox.dbPath, batchSize: config.outbox.batchSize },
    "outbox enabled",
  );
} else {
  log.warn("outbox disabled; inline publish (escape hatch)");
}

const healthAdmin = await createHealthAdmin(provider);
const expectedTopics = config.routes.flatMap((r) => [r.topic, r.dlqTopic]);
const monitor = createHealthMonitor({
  producer,
  outboxDb,
  admin: healthAdmin,
  expectedTopics,
  probeIntervalMs: config.health.probeIntervalMs,
  probeTimeoutMs: config.health.probeTimeoutMs,
});
await monitor.start();

// Replay subsystem. Only wired when BOTH ADMIN_TOKEN and REPLAY_ENABLED are
// set — REPLAY_ENABLED alone is meaningless without an auth gate. When off,
// jobStore is undefined and routes.ts skips endpoint registration.
let replayJobStore: ReplayJobStore | undefined;
let replayContext: ReplayContext | undefined;
let dlqInspector: DlqDepthCache | undefined;
let replayScheduler: ReplayScheduler | undefined;
if (config.admin?.token && config.replay?.enabled) {
  // Persistent SQLite store when the outbox is on (shared DB); fall back to
  // in-memory if outbox is disabled (escape hatch — replay state then dies
  // with the process, but the outbox escape hatch already implies "don't
  // expect durable buffering").
  if (outboxDb !== undefined) {
    runReplayMigrations(outboxDb);
    const swept = sweepReplayGhostJobs(outboxDb);
    if (swept > 0) {
      log.info({ swept }, "replay: marked orphaned jobs failed (ghost-job sweep)");
    }
    replayJobStore = createSqliteReplayJobStore(outboxDb);
  } else {
    replayJobStore = createReplayJobStore();
    log.warn("replay: outbox disabled, using in-memory jobStore (jobs do not survive restart)");
  }

  const routesByName = new Map<string, RouteConfig>(
    config.routes.map((r) => [r.name, r]),
  );

  // dlqInspector: per-partition depth cache. Reuses healthAdmin (long-lived
  // Admin client). HealthAdmin re-exports metadata + listOffsets so the
  // structural match against DlqAdminLike is direct.
  dlqInspector = createDlqInspector({
    admin: healthAdmin,
    routes: config.routes,
    probeIntervalMs: config.health.probeIntervalMs,
  });
  dlqInspector.start();

  const groupCleanup = async (groupId: string): Promise<void> => {
    await healthAdmin.deleteGroups({ groups: [groupId] });
  };
  const handlers = makeReplayHandlers({
    expectedToken: config.admin.token,
    cfg: config.replay,
    jobStore: replayJobStore,
    routes: routesByName,
    producer,
    admin: healthAdmin,
    createConsumer: (route, jobId) =>
      createReplayConsumer(provider, route, jobId, groupCleanup),
    dlqDepth: dlqInspector,
  });
  replayContext = { handlers };

  // Scheduler (Phase 5). Off by default; reads dlqInspector cache and fires
  // bounded replay jobs when a (route, partition) depth meets the threshold.
  if (config.replay.auto.enabled) {
    replayScheduler = createReplayScheduler({
      cfg: config.replay,
      routes: config.routes,
      dlqDepth: dlqInspector,
      jobStore: replayJobStore,
      producer,
      createConsumer: (route, jobId) =>
        createReplayConsumer(provider, route, jobId, groupCleanup),
    });
    replayScheduler.start();
    log.info(
      {
        intervalMs: config.replay.auto.intervalMs,
        threshold: config.replay.auto.dlqDepthThreshold,
      },
      "replay scheduler started",
    );
  }

  log.info({ replay: true }, "replay subsystem enabled");
} else if (config.replay?.enabled && !config.admin?.token) {
  log.warn(
    "REPLAY_ENABLED=true but ADMIN_TOKEN unset; replay subsystem disabled (auth gate is mandatory)",
  );
}

let server: ReturnType<typeof Bun.serve> | undefined;

function rebuildRoutes(newRoutes?: RouteConfig[]): ReturnType<typeof buildRoutes> {
  if (newRoutes !== undefined) {
    process.env.ROUTES_JSON = JSON.stringify(newRoutes);
    resetConfigCache();
  }
  const adminContext = config.admin?.token && config.routesFile
    ? {
        routesFilePath: config.routesFile,
        onReload: async (routes: RouteConfig[]) => {
          const map = rebuildRoutes(routes);
          if (!server) throw new Error("server not initialized");
          server.reload({ routes: map });
        },
      }
    : undefined;
  return buildRoutes({
    producer,
    outbox: outboxWriter,
    monitor,
    metrics: drainMetrics,
    adminContext,
    replayContext,
  });
}

if (config.admin?.token) {
  if (config.routesFile) {
    log.info({ routesFilePath: config.routesFile }, "admin endpoint will be enabled");
  } else {
    log.warn("ADMIN_TOKEN set but ROUTES_FILE unset; admin endpoint disabled (cannot persist)");
  }
} else {
  log.info("admin endpoint disabled (ADMIN_TOKEN unset)");
}

server = Bun.serve({
  port: config.server.port,
  routes: rebuildRoutes(),
  fetch(req: Request) {
    const url = new URL(req.url);
    log.warn(
      { method: req.method, path: url.pathname, ua: req.headers.get("user-agent") },
      "unmatched request",
    );
    return new Response("Not found", { status: 404 });
  },
  error(err) {
    log.error({ err }, "unhandled error");
    return new Response("Internal error", { status: 500 });
  },
});

log.info(
  {
    host: server.hostname,
    port: server.port,
    provider: provider.name,
    providerType: provider.type,
    outbox: {
      enabled: config.outbox.enabled,
      dbPath: config.outbox.enabled ? config.outbox.dbPath : null,
    },
    routes: config.routes.map((r) => ({ name: r.name, path: r.path, topic: r.topic })),
    health: {
      probeIntervalMs: config.health.probeIntervalMs,
      probeTimeoutMs: config.health.probeTimeoutMs,
      dependencies: [
        "kafkaProducer",
        config.outbox.enabled ? "outboxDb" : null,
        "kafkaBroker",
        "topics",
      ].filter((d): d is string => d !== null),
    },
    heartbeatMs: config.health.heartbeatMs,
  },
  "gateway listening",
);

const heartbeat: HeartbeatHandle = startHeartbeat({
  intervalMs: config.health.heartbeatMs,
  snapshot: () => {
    const snap = monitor.snapshot();
    const drain = drainMetrics.snapshot();
    const stats = outboxWriter?.backlogStats();
    return {
      producerConnected: snap.dependencies.kafkaProducer?.ok ?? false,
      producerBreaker: {
        state: producer.getBreakerSnapshot().state,
        failures: producer.getBreakerSnapshot().failures,
      },
      brokerOk: snap.dependencies.kafkaBroker?.ok ?? false,
      topicsOk: snap.dependencies.topics?.ok ?? true,
      status: snap.status,
      ...(stats
        ? {
            pending: stats.pending,
            failed: stats.failed,
            oldestPendingAgeMs: stats.oldestPendingAgeMs,
            pendingByTopic: stats.pendingByTopic,
          }
        : {}),
      publishedLast60s: drain.publishedLast60s,
      lastPublishedAt: drain.lastPublishedAt,
      breakerOpenCount: drain.breakerOpenCount,
    };
  },
});

async function shutdown(signal: string) {
  log.info({ signal }, "shutting down gateway");
  server?.stop();
  heartbeat.stop();
  // Stop the scheduler tick first so no NEW replay jobs spawn during
  // teardown; THEN cancel in-flight jobs so their AbortControllers fire
  // before the producer + provider close out from under them.
  if (replayScheduler) await replayScheduler.stop();
  if (replayJobStore) replayJobStore.cancelAll();
  if (dlqInspector) dlqInspector.stop();
  await monitor.stop();
  await healthAdmin.close();
  if (drainer) await drainer.stop();
  await producer.disconnect();
  await provider.close();
  if (outboxDb) closeOutbox(outboxDb);
  process.exit(0);
}

process.on("SIGINT", () => void shutdown("SIGINT"));
process.on("SIGTERM", () => void shutdown("SIGTERM"));
