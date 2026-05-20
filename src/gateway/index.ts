// src/gateway/index.ts
import { dirname } from "node:path";
import { mkdirSync } from "node:fs";
import { config } from "../config/index.ts";
import { resetConfigCache } from "../config/loader.ts";
import type { RouteConfig } from "../config/schemas.ts";
import { createHealthAdmin } from "../health/admin.ts";
import { createHealthMonitor } from "../health/monitor.ts";
import { getLogger } from "../logging/index.ts";
import { createProducer } from "../kafka/producer.ts";
import { createKafkaProvider } from "../kafka/providers/index.ts";
import { closeOutbox, openOutbox, type OutboxDatabase } from "../outbox/db.ts";
import { startDrainer, type DrainerHandle } from "../outbox/drainer.ts";
import { createDrainMetrics } from "../outbox/metrics.ts";
import { createWriter, type OutboxWriter } from "../outbox/writer.ts";
import { buildRoutes } from "./routes.ts";

const log = getLogger("gateway");

const provider = createKafkaProvider(config);
log.info({ provider: provider.name, providerType: provider.type }, "kafka provider selected");

const producer = await createProducer(config.kafka.clientId, provider);

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
  });
  log.info(
    { dbPath: config.outbox.dbPath, batchSize: config.outbox.batchSize },
    "outbox enabled",
  );
} else {
  log.warn("outbox disabled; inline publish (escape hatch)");
}

const drainMetrics = createDrainMetrics();

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
    routes: config.routes.map((r) => ({ name: r.name, path: r.path, topic: r.topic })),
  },
  "gateway listening",
);

async function shutdown(signal: string) {
  log.info({ signal }, "shutting down gateway");
  server?.stop();
  if (drainer) await drainer.stop();
  await monitor.stop();
  await healthAdmin.close();
  await producer.disconnect();
  await provider.close();
  if (outboxDb) closeOutbox(outboxDb);
  process.exit(0);
}

process.on("SIGINT", () => void shutdown("SIGINT"));
process.on("SIGTERM", () => void shutdown("SIGTERM"));
