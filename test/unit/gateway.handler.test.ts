// test/unit/gateway.handler.test.ts
import { describe, expect, it } from "bun:test";
import type { RouteConfig } from "../../src/config/schemas.ts";
import { makeWebhookHandler } from "../../src/gateway/handler.ts";

function fakeOutbox() {
  const calls: Array<{
    topic: string;
    messageKey: string;
    payload: string;
    headers: Record<string, string> | null;
  }> = [];
  return {
    enqueue: (row: { topic: string; messageKey: string; payload: string; headers: Record<string, string> | null }) => {
      calls.push(row);
    },
    backlogStats: () => ({ pending: 0, failed: 0, oldestPendingAgeMs: 0 }),
    calls,
  };
}

const baseRoute: RouteConfig = {
  name: "elastic-autoops",
  path: "/webhooks/elastic/autoops",
  topic: "T_PRIVATE_SOURCE_ELASTIC_AUTOOPS",
  dlqTopic: "DLQ_T_PRIVATE_SOURCE_ELASTIC_AUTOOPS",
  sourceHeader: "elastic-autoops",
  keyFields: ["resourceId", "deployment-id"],
  idempotency: "elastic-autoops",
};

const noopProducer = {
  isConnected: () => true,
  disconnect: async () => {},
  sendByTopic: async () => {},
};

function postJson(body: unknown): Request {
  return new Request("http://localhost/webhooks/elastic/autoops", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
}

describe("makeWebhookHandler", () => {
  it("enqueues a row with the route's topic and the configured sourceHeader", async () => {
    const outbox = fakeOutbox();
    const handler = makeWebhookHandler(baseRoute, { producer: noopProducer, outbox });
    const res = await handler(
      postJson({ resourceId: "dep-1", title: "t", status: "open" }),
    );
    expect(res.status).toBe(202);
    expect(outbox.calls).toHaveLength(1);
    expect(outbox.calls[0]?.topic).toBe("T_PRIVATE_SOURCE_ELASTIC_AUTOOPS");
    expect(outbox.calls[0]?.headers?.source).toBe("elastic-autoops");
    expect(typeof outbox.calls[0]?.headers?.idempotencyKey).toBe("string");
  });

  it("picks the partition key from configured keyFields in order", async () => {
    const outbox = fakeOutbox();
    const handler = makeWebhookHandler(baseRoute, { producer: noopProducer, outbox });
    await handler(postJson({ "deployment-id": "fallback-key" }));
    expect(outbox.calls[0]?.messageKey).toBe("fallback-key");
  });

  it("returns 400 on non-JSON body", async () => {
    const outbox = fakeOutbox();
    const handler = makeWebhookHandler(baseRoute, { producer: noopProducer, outbox });
    const res = await handler(
      new Request("http://localhost/x", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: "not json",
      }),
    );
    expect(res.status).toBe(400);
    expect(outbox.calls).toHaveLength(0);
  });

  it("uses sourceHeader override when present", async () => {
    const outbox = fakeOutbox();
    const handler = makeWebhookHandler(
      { ...baseRoute, sourceHeader: "custom-source" },
      { producer: noopProducer, outbox },
    );
    await handler(postJson({ resourceId: "dep-1" }));
    expect(outbox.calls[0]?.headers?.source).toBe("custom-source");
  });

  it("omits idempotencyKey header when the strategy returns undefined for this body shape", async () => {
    // elastic-autoops strategy needs resourceId + title + status + startTime + endTime;
    // a body missing any of those returns undefined and no header is set.
    const outbox = fakeOutbox();
    const handler = makeWebhookHandler(baseRoute, { producer: noopProducer, outbox });
    await handler(postJson({ resourceId: "dep-1" }));
    expect(outbox.calls[0]?.headers?.idempotencyKey).toBeUndefined();
  });

  it("inline-publish path uses route.topic and forwards headers", async () => {
    const calls: Array<{
      topic: string;
      key: string;
      value: string;
      headers: Record<string, string> | null | undefined;
    }> = [];
    const spyProducer = {
      ...noopProducer,
      sendByTopic: async (
        topic: string,
        key: string,
        value: string,
        headers?: Record<string, string> | null,
      ) => {
        calls.push({ topic, key, value, headers });
      },
    };
    const handler = makeWebhookHandler(baseRoute, { producer: spyProducer });
    const res = await handler(
      postJson({ resourceId: "dep-1", title: "t", status: "open" }),
    );
    expect(res.status).toBe(202);
    expect(calls).toHaveLength(1);
    expect(calls[0]?.topic).toBe("T_PRIVATE_SOURCE_ELASTIC_AUTOOPS");
    expect(calls[0]?.key).toBe("dep-1");
    expect(calls[0]?.headers?.source).toBe("elastic-autoops");
    expect(typeof calls[0]?.headers?.idempotencyKey).toBe("string");
    const parsed = JSON.parse(calls[0]?.value ?? "{}");
    expect(parsed.raw).toEqual({ resourceId: "dep-1", title: "t", status: "open" });
  });

  it("inline-publish path uses each route's own topic in multi-route configs", async () => {
    const calls: Array<{ topic: string }> = [];
    const spyProducer = {
      ...noopProducer,
      sendByTopic: async (topic: string) => {
        calls.push({ topic });
      },
    };
    const routeA: RouteConfig = { ...baseRoute, topic: "T_PRIVATE_SOURCE_A_X" };
    const routeB: RouteConfig = {
      ...baseRoute,
      name: "b",
      path: "/webhooks/b",
      topic: "T_PRIVATE_SOURCE_B_Y",
    };
    const handlerA = makeWebhookHandler(routeA, { producer: spyProducer });
    const handlerB = makeWebhookHandler(routeB, { producer: spyProducer });
    await handlerA(postJson({ resourceId: "1" }));
    await handlerB(postJson({ resourceId: "2" }));
    expect(calls.map((c) => c.topic)).toEqual([
      "T_PRIVATE_SOURCE_A_X",
      "T_PRIVATE_SOURCE_B_Y",
    ]);
  });
});
