// src/gateway/index.ts
import { config } from "../config/index.ts";
import { getLogger } from "../logging/index.ts";
import { createProducer } from "../kafka/producer.ts";
import { createKafkaProvider } from "../kafka/providers/index.ts";
import { buildRoutes } from "./routes.ts";

const log = getLogger("gateway");

const provider = createKafkaProvider(config);
log.info({ provider: provider.name, providerType: provider.type }, "kafka provider selected");

const producer = await createProducer(config.kafka.clientId, provider);

const server = Bun.serve({
  port: config.server.port,
  routes: buildRoutes(producer),
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
  await producer.disconnect();
  await provider.close();
  process.exit(0);
}

process.on("SIGINT", () => void shutdown("SIGINT"));
process.on("SIGTERM", () => void shutdown("SIGTERM"));
