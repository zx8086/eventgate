// src/gateway/routes.ts
import { config } from "../config/index.ts";
import type { EventProducer } from "../kafka/producer.ts";
import { normalizeElasticAutoOps } from "../normalize.ts";
import { autoOpsWebhookSchema } from "./schema.ts";
import { getLogger } from "../logging/index.ts";

const log = getLogger("gateway.routes");

export function buildRoutes(producer: EventProducer) {
  return {
    "/healthz": () =>
      producer.isConnected()
        ? Response.json({ ok: true })
        : Response.json({ ok: false }, { status: 503 }),

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

        const result = autoOpsWebhookSchema.safeParse(body);
        if (!result.success) {
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

        try {
          await producer.publishRaw(parsed.resourceId, body);
          await producer.publishNormalized(event);
        } catch (err) {
          log.error({ err, resourceId: parsed.resourceId }, "kafka publish failed");
          return Response.json(
            { accepted: false, error: "downstream publish failed" },
            { status: 503 },
          );
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
