// src/gateway/routes.ts
import { config } from "../config/index.ts";
import type { EventProducer } from "../kafka/producer.ts";
import { getLogger } from "../logging/index.ts";
import type { OutboxWriter } from "../outbox/writer.ts";
import { makeWebhookHandler } from "./handler.ts";

const log = getLogger("gateway.routes");

export type RouteDeps = {
  producer: EventProducer;
  outbox?: OutboxWriter;
};

type RouteHandler = (req: Request) => Promise<Response>;
type RoutesMap = Record<string, (() => Response) | { POST: RouteHandler }>;

export function buildRoutes(deps: RouteDeps): RoutesMap {
  const { producer, outbox } = deps;

  const routes: RoutesMap = {
    "/healthz": () => {
      const stats = outbox?.backlogStats();
      const producerOk = producer.isConnected();
      return Response.json(
        {
          ok: producerOk,
          producer: { connected: producerOk },
          outbox: stats
            ? {
                enabled: true,
                pending: stats.pending,
                failed: stats.failed,
                oldestPendingAgeMs: stats.oldestPendingAgeMs,
              }
            : { enabled: false },
          routes: config.routes.map((r) => ({
            name: r.name,
            path: r.path,
            topic: r.topic,
            dlqTopic: r.dlqTopic,
          })),
        },
        { status: producerOk ? 200 : 503 },
      );
    },
  };

  for (const route of config.routes) {
    routes[route.path] = { POST: makeWebhookHandler(route, { producer, outbox }) };
    log.info(
      {
        route: route.name,
        path: route.path,
        topic: route.topic,
        dlqTopic: route.dlqTopic,
        idempotency: route.idempotency,
      },
      "route registered",
    );
  }

  return routes;
}
