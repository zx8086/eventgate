// src/gateway/routes.ts
import { config } from "../config/index.ts";
import type { EventProducer } from "../kafka/producer.ts";
import { getLogger } from "../logging/index.ts";
import type { OutboxWriter } from "../outbox/writer.ts";
import { autoOpsIdempotencyKey } from "./idempotencyKey.ts";

const log = getLogger("gateway.routes");

export type RouteDeps = {
  producer: EventProducer;
  outbox?: OutboxWriter;
};

function pickKey(body: unknown): string {
  if (typeof body !== "object" || body === null || Array.isArray(body)) return "unkeyed";
  const b = body as Record<string, unknown>;
  for (const k of ["resourceId", "deployment-id"]) {
    const v = b[k];
    if (typeof v === "string" && v.length > 0) return v;
  }
  return "unkeyed";
}

export function buildRoutes(deps: RouteDeps) {
  const { producer, outbox } = deps;

  return {
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
        },
        { status: producerOk ? 200 : 503 },
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

        const messageKey = pickKey(body);
        const idempotencyKey = autoOpsIdempotencyKey(body);
        const headers: Record<string, string> = { source: "elastic-autoops" };
        if (idempotencyKey) headers.idempotencyKey = idempotencyKey;

        const payload = JSON.stringify({
          receivedAt: new Date().toISOString(),
          raw: body,
        });

        if (outbox) {
          try {
            // Bridge: routes.ts will iterate config.routes in Task 9. Until then,
            // publish the AutoOps webhook to the real topic name, not the outbox
            // enum key — the drainer's topicToKafka mapping was removed in Tasks 5+6.
            outbox.enqueue({
              topic: config.kafka.topics.raw,
              messageKey,
              payload,
              headers,
            });
          } catch (err) {
            log.error({ err, messageKey }, "outbox enqueue failed");
            return Response.json(
              { accepted: false, error: "outbox enqueue failed" },
              { status: 500 },
            );
          }
        } else {
          try {
            await producer.publishRaw(messageKey, body);
          } catch (err) {
            log.warn({ err, messageKey }, "kafka publish failed; will not retry without outbox");
          }
        }

        return Response.json({ accepted: true }, { status: 202 });
      },
    },
  };
}
