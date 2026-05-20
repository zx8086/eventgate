// src/gateway/index.ts
import { dirname } from "node:path";
import { mkdirSync } from "node:fs";
import { config } from "../config/index.ts";
import { getLogger } from "../logging/index.ts";
import { createProducer } from "../kafka/producer.ts";
import { createKafkaProvider } from "../kafka/providers/index.ts";
import { closeOutbox, openOutbox, type OutboxDatabase } from "../outbox/db.ts";
import { startDrainer, type DrainerHandle } from "../outbox/drainer.ts";
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

const server = Bun.serve({
  port: config.server.port,
  routes: buildRoutes({ producer, outbox: outboxWriter }),
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
    topics: { raw: config.kafka.topics.raw, events: config.kafka.topics.events },
  },
  "gateway listening",
);

async function shutdown(signal: string) {
  log.info({ signal }, "shutting down gateway");
  server.stop();
  if (drainer) await drainer.stop();
  await producer.disconnect();
  await provider.close();
  if (outboxDb) closeOutbox(outboxDb);
  process.exit(0);
}

process.on("SIGINT", () => void shutdown("SIGINT"));
process.on("SIGTERM", () => void shutdown("SIGTERM"));
