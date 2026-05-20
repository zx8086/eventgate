// src/gateway/handler.ts
import type { RouteConfig } from "../config/schemas.ts";
import type { EventProducer } from "../kafka/producer.ts";
import { getLogger } from "../logging/index.ts";
import type { OutboxWriter } from "../outbox/writer.ts";
import { resolveIdempotencyStrategy } from "./idempotencyStrategies.ts";

const log = getLogger("gateway.handler");

export type HandlerDeps = {
  producer: Pick<EventProducer, "sendByTopic">;
  outbox?: Pick<OutboxWriter, "enqueue">;
};

function pickKey(body: unknown, fields: readonly string[]): string {
  if (typeof body !== "object" || body === null || Array.isArray(body)) return "unkeyed";
  const b = body as Record<string, unknown>;
  for (const k of fields) {
    const v = b[k];
    if (typeof v === "string" && v.length > 0) return v;
  }
  return "unkeyed";
}

export function makeWebhookHandler(route: RouteConfig, deps: HandlerDeps) {
  const { producer, outbox } = deps;
  const sourceHeader = route.sourceHeader;
  const strategy = resolveIdempotencyStrategy(route.idempotency);

  return async function handler(req: Request): Promise<Response> {
    let body: unknown;
    try {
      body = await req.json();
    } catch {
      return Response.json(
        { accepted: false, error: "invalid JSON body" },
        { status: 400 },
      );
    }

    const messageKey = pickKey(body, route.keyFields);
    const idempotencyKey = strategy ? strategy(body) : undefined;
    const headers: Record<string, string> = { source: sourceHeader };
    if (idempotencyKey) headers.idempotencyKey = idempotencyKey;

    const payload = JSON.stringify({
      receivedAt: new Date().toISOString(),
      raw: body,
    });

    if (outbox) {
      try {
        outbox.enqueue({
          topic: route.topic,
          messageKey,
          payload,
          headers,
        });
      } catch (err) {
        log.error(
          { err, route: route.name, topic: route.topic, messageKey },
          "outbox enqueue failed",
        );
        return Response.json(
          { accepted: false, error: "outbox enqueue failed" },
          { status: 500 },
        );
      }
    } else {
      try {
        await producer.sendByTopic(route.topic, messageKey, payload, headers);
      } catch (err) {
        log.warn(
          { err, route: route.name, topic: route.topic, messageKey },
          "kafka publish failed; will not retry without outbox",
        );
      }
    }

    return Response.json({ accepted: true }, { status: 202 });
  };
}
