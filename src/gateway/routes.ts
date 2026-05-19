// src/gateway/routes.ts
import { config } from "../config/index.ts";
import type { EventProducer } from "../kafka/producer.ts";
import { normalizeElasticAutoOps } from "../normalize.ts";
import type { OutboxWriter } from "../outbox/writer.ts";
import { autoOpsWebhookSchema, isSyntheticAutoOpsTest, normalizeAutoOpsBody } from "./schema.ts";
import { getLogger } from "../logging/index.ts";

const log = getLogger("gateway.routes");

export type RouteDeps = {
  producer: EventProducer;
  outbox?: OutboxWriter;
};

export function buildRoutes(deps: RouteDeps) {
  const { producer, outbox } = deps;

  return {
    "/healthz": () => {
      const stats = outbox?.backlogStats();
      const producerOk = producer.isConnected();
      const ok = producerOk;
      return Response.json(
        {
          ok,
          producer: { connected: producerOk },
          outbox: stats
            ? {
                enabled: true,
                pending: stats.pending,
                failed: stats.failed,
                oldestPendingAgeMs: stats.oldestPendingAgeMs,
              }
            : { enabled: false },
        },
        { status: ok ? 200 : 503 },
      );
    },

    "/webhooks/elastic/autoops": {
      POST: async (req: Request) => {
        let body: unknown;
        try {
          body = await req.json();
        } catch {
          return Response.json(
            { accepted: false, error: "invalid JSON body" },
            { status: 400 },
          );
        }

        if (isSyntheticAutoOpsTest(body)) {
          log.info({ body }, "autoops synthetic validate received");
          return Response.json(
            { accepted: true, validation: "synthetic" },
            { status: 202 },
          );
        }

        const normalized = normalizeAutoOpsBody(body);
        const result = autoOpsWebhookSchema.safeParse(normalized);
        if (!result.success) {
          log.warn(
            { issues: result.error.issues, body },
            "schema validation failed",
          );
          return Response.json(
            {
              accepted: false,
              error: "schema validation failed",
              issues: result.error.issues,
            },
            { status: 400 },
          );
        }
        const parsed = result.data;

        const event = normalizeElasticAutoOps(parsed, {
          tenant: config.app.tenant,
          environment: config.app.environment,
        });

        log.info({ event }, "autoops.event.received");

        if (outbox) {
          try {
            outbox.enqueuePair(
              {
                topic: "raw",
                messageKey: parsed.resourceId,
                payload: JSON.stringify({
                  receivedAt: new Date().toISOString(),
                  raw: body,
                }),
                headers: null,
              },
              {
                topic: "events",
                messageKey: event.routingKey,
                payload: JSON.stringify(event),
                headers: {
                  source: event.source,
                  eventType: event.eventType,
                  severity: event.alert.severity,
                  schemaVersion: String(event.schemaVersion),
                  idempotencyKey: event.idempotencyKey,
                },
              },
            );
          } catch (err) {
            log.error({ err, resourceId: parsed.resourceId }, "outbox enqueue failed");
            return Response.json(
              { accepted: false, error: "outbox enqueue failed" },
              { status: 500 },
            );
          }
        } else {
          try {
            await producer.publishRaw(parsed.resourceId, body);
            await producer.publishNormalized(event);
          } catch (err) {
            log.warn(
              { err, resourceId: parsed.resourceId },
              "kafka publish failed; event logged only",
            );
          }
        }

        return Response.json(
          {
            accepted: true,
            resourceId: event.resource.id,
            idempotencyKey: event.idempotencyKey,
          },
          { status: 202 },
        );
      },
    },
  };
}
