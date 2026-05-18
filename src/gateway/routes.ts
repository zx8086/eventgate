import { z } from "zod";
import { config } from "../config.ts";
import type { EventProducer } from "../kafka/producer.ts";
import { normalizeElasticAutoOps } from "../normalize.ts";
import { autoOpsWebhookSchema } from "./schema.ts";

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

        let parsed;
        try {
          parsed = autoOpsWebhookSchema.parse(body);
        } catch (error) {
          if (error instanceof z.ZodError) {
            return Response.json(
              {
                accepted: false,
                error: "schema validation failed",
                issues: error.issues,
              },
              { status: 400 },
            );
          }
          throw error;
        }

        const event = normalizeElasticAutoOps(parsed, {
          tenant: config.tenant,
          environment: config.environment,
        });

        try {
          await producer.publishRaw(parsed.resourceId, body);
          await producer.publishNormalized(event);
        } catch (error) {
          console.error("[gateway] kafka publish failed", error);
          return Response.json(
            {
              accepted: false,
              error: "downstream publish failed",
            },
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
