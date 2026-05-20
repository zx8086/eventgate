// src/gateway/routes.ts
import { makeAdminRoutesHandler } from "../admin/routesEndpoint.ts";
import { config } from "../config/index.ts";
import type { RouteConfig } from "../config/schemas.ts";
import type { EventProducer } from "../kafka/producer.ts";
import { getLogger } from "../logging/index.ts";
import type { OutboxWriter } from "../outbox/writer.ts";
import { makeWebhookHandler } from "./handler.ts";

const log = getLogger("gateway.routes");

export type AdminContext = {
  routesFilePath: string;
  onReload: (routes: RouteConfig[]) => void | Promise<void>;
};

export type RouteDeps = {
  producer: EventProducer;
  outbox?: OutboxWriter;
  adminContext?: AdminContext;
};

type RouteHandler = (req: Request) => Promise<Response>;
type RoutesMap = Record<
  string,
  | (() => Response)
  | { POST: RouteHandler }
  | { PUT: RouteHandler }
>;

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

  if (config.admin?.token && deps.adminContext) {
    routes["/admin/routes"] = {
      PUT: makeAdminRoutesHandler({
        expectedToken: config.admin.token,
        routesFilePath: deps.adminContext.routesFilePath,
        onReload: deps.adminContext.onReload,
      }),
    };
    log.info(
      { path: "/admin/routes", routesFilePath: deps.adminContext.routesFilePath },
      "admin endpoint enabled",
    );
  } else if (config.admin?.token) {
    log.warn(
      "ADMIN_TOKEN is set but no adminContext provided; admin endpoint NOT registered",
    );
  }

  return routes;
}
