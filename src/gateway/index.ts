// src/gateway/index.ts
import { config } from "../config/index.ts";
import { getLogger } from "../logging/index.ts";
import { createProducer } from "../kafka/producer.ts";
import { buildRoutes } from "./routes.ts";

const log = getLogger("gateway");

const producer = await createProducer(config.kafka.clientIdGateway);

const server = Bun.serve({
  port: config.server.port,
  routes: buildRoutes(producer),
  fetch() {
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
  await producer.disconnect();
  process.exit(0);
}

process.on("SIGINT", () => void shutdown("SIGINT"));
process.on("SIGTERM", () => void shutdown("SIGTERM"));
