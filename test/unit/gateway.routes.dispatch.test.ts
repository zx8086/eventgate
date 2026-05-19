// test/unit/gateway.routes.dispatch.test.ts
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { resetConfigCache } from "../../src/config/loader.ts";
import { buildRoutes } from "../../src/gateway/routes.ts";

const noopProducer = {
  publishRaw: async () => {},
  isConnected: () => true,
  disconnect: async () => {},
  sendByTopic: async () => {},
};

function fakeOutbox() {
  const calls: Array<{ topic: string; messageKey: string }> = [];
  return {
    enqueue: (row: { topic: string; messageKey: string }) => calls.push(row),
    backlogStats: () => ({ pending: 0, failed: 0, oldestPendingAgeMs: 0 }),
    calls,
  };
}

let snapshot: NodeJS.ProcessEnv;

beforeEach(() => {
  snapshot = { ...process.env };
  process.env = {
    ...process.env,
    ENVIRONMENT: "dev",
    KAFKA_PROVIDER: "local",
    ROUTES_JSON: JSON.stringify([
      {
        name: "elastic-autoops",
        path: "/webhooks/elastic/autoops",
        topic: "T_PRIVATE_SOURCE_ELASTIC_AUTOOPS",
        keyFields: ["resourceId"],
        idempotency: "elastic-autoops",
      },
      {
        name: "datadog-alerts",
        path: "/webhooks/datadog/alerts",
        topic: "T_PRIVATE_SOURCE_DATADOG_ALERTS",
        keyFields: ["alert_id"],
      },
    ]),
  };
  resetConfigCache();
});

afterEach(() => {
  process.env = snapshot;
  resetConfigCache();
});

describe("buildRoutes with multiple routes", () => {
  it("registers a POST handler per configured route", () => {
    const outbox = fakeOutbox();
    const routes = buildRoutes({ producer: noopProducer, outbox });
    expect(routes["/webhooks/elastic/autoops"]).toBeDefined();
    expect(routes["/webhooks/datadog/alerts"]).toBeDefined();
    expect(routes["/healthz"]).toBeDefined();
  });

  it("dispatches each route to its own topic", async () => {
    const outbox = fakeOutbox();
    const routes = buildRoutes({ producer: noopProducer, outbox });

    const r1 = routes["/webhooks/elastic/autoops"] as { POST: (req: Request) => Promise<Response> };
    await r1.POST(
      new Request("http://localhost/webhooks/elastic/autoops", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ resourceId: "dep-1" }),
      }),
    );

    const r2 = routes["/webhooks/datadog/alerts"] as { POST: (req: Request) => Promise<Response> };
    await r2.POST(
      new Request("http://localhost/webhooks/datadog/alerts", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ alert_id: "a-1" }),
      }),
    );

    expect(outbox.calls).toHaveLength(2);
    expect(outbox.calls[0]?.topic).toBe("T_PRIVATE_SOURCE_ELASTIC_AUTOOPS");
    expect(outbox.calls[0]?.messageKey).toBe("dep-1");
    expect(outbox.calls[1]?.topic).toBe("T_PRIVATE_SOURCE_DATADOG_ALERTS");
    expect(outbox.calls[1]?.messageKey).toBe("a-1");
  });

  it("healthz reports producer + outbox status", () => {
    const outbox = fakeOutbox();
    const routes = buildRoutes({ producer: noopProducer, outbox });
    const healthz = routes["/healthz"] as () => Response;
    const res = healthz();
    expect(res.status).toBe(200);
  });

  it("healthz includes the registered routes", async () => {
    const outbox = fakeOutbox();
    const routes = buildRoutes({ producer: noopProducer, outbox });
    const healthz = routes["/healthz"] as () => Response;
    const res = healthz();
    const payload = await res.json() as { routes?: Array<{ name: string; path: string; topic: string; dlqTopic?: string }> };
    expect(payload.routes).toEqual([
      { name: "elastic-autoops", path: "/webhooks/elastic/autoops", topic: "T_PRIVATE_SOURCE_ELASTIC_AUTOOPS", dlqTopic: undefined },
      { name: "datadog-alerts", path: "/webhooks/datadog/alerts", topic: "T_PRIVATE_SOURCE_DATADOG_ALERTS", dlqTopic: undefined },
    ]);
  });
});
