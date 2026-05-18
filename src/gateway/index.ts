import { config } from "../config.ts";
import { createProducer } from "../kafka/producer.ts";
import { buildRoutes } from "./routes.ts";

const producer = await createProducer(config.kafka.clientIdGateway);

const server = Bun.serve({
  port: config.port,
  routes: buildRoutes(producer),
  fetch() {
    return new Response("Not found", { status: 404 });
  },
  error(err) {
    console.error("[gateway] unhandled error", err);
    return new Response("Internal error", { status: 500 });
  },
});

console.log(`[gateway] listening on http://${server.hostname}:${server.port}`);
console.log(`[gateway] publishing to topics: ${config.kafka.topics.raw}, ${config.kafka.topics.events}`);

async function shutdown(signal: string) {
  console.log(`[gateway] received ${signal}, shutting down`);
  server.stop();
  await producer.disconnect();
  process.exit(0);
}

process.on("SIGINT", () => void shutdown("SIGINT"));
process.on("SIGTERM", () => void shutdown("SIGTERM"));
