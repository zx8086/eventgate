// test/unit/gateway.routes.dispatch.test.ts
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { resetConfigCache } from "../../src/config/loader.ts";
import { buildRoutes } from "../../src/gateway/routes.ts";
import type { HealthSnapshot } from "../../src/health/types.ts";
import type { DrainMetricsSnapshot } from "../../src/outbox/metrics.ts";

const noopProducer = {
  isConnected: () => true,
  disconnect: async () => {},
  sendByTopic: async () => {},
};

function fakeOutbox() {
  const calls: Array<{ topic: string; messageKey: string }> = [];
  return {
    enqueue: (row: { topic: string; messageKey: string }) => calls.push(row),
    backlogStats: () => ({ pending: 0, failed: 0, oldestPendingAgeMs: 0, pendingByTopic: {} }),
    calls,
  };
}

function fakeMonitor(snapshot: HealthSnapshot) {
  return {
    start: async () => {},
    stop: async () => {},
    snapshot: () => snapshot,
    probeOnce: async () => {},
  };
}

function fakeMetrics(snap: DrainMetricsSnapshot) {
  return {
    recordPublished: () => {},
    recordError: () => {},
    snapshot: () => snap,
  };
}

const healthySnap: HealthSnapshot = {
  status: "healthy",
  ok: true,
  checkedAt: 1_000,
  dependencies: {
    kafkaProducer: { ok: true, lastCheckedAt: 1_000, connected: true },
    outboxDb: { ok: true, lastCheckedAt: 1_000 },
    kafkaBroker: { ok: true, lastCheckedAt: 1_000, brokerProbeMs: 5 },
    topics: { ok: true, lastCheckedAt: 1_000, missing: [] },
  },
};

const emptyMetricsSnap: DrainMetricsSnapshot = {
  publishedLast60s: 0,
  lastPublishedAt: null,
  lastError: null,
};

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
        dlqTopic: "DLQ_T_PRIVATE_SOURCE_ELASTIC_AUTOOPS",
        sourceHeader: "elastic-autoops",
        keyFields: ["resourceId"],
        idempotency: "elastic-autoops",
      },
      {
        name: "datadog-alerts",
        path: "/webhooks/datadog/alerts",
        topic: "T_PRIVATE_SOURCE_DATADOG_ALERTS",
        dlqTopic: "DLQ_T_PRIVATE_SOURCE_DATADOG_ALERTS",
        sourceHeader: "datadog-alerts",
        keyFields: ["alert_id"],
        idempotency: "elastic-autoops",
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
    const routes = buildRoutes({
      producer: noopProducer,
      outbox,
      monitor: fakeMonitor(healthySnap),
      metrics: fakeMetrics(emptyMetricsSnap),
    });
    expect(routes["/webhooks/elastic/autoops"]).toBeDefined();
    expect(routes["/webhooks/datadog/alerts"]).toBeDefined();
    expect(routes["/healthz"]).toBeDefined();
  });

  it("dispatches each route to its own topic", async () => {
    const outbox = fakeOutbox();
    const routes = buildRoutes({
      producer: noopProducer,
      outbox,
      monitor: fakeMonitor(healthySnap),
      metrics: fakeMetrics(emptyMetricsSnap),
    });

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

  it("healthz returns 200 + healthy when all dependencies are ok", async () => {
    const outbox = fakeOutbox();
    const routes = buildRoutes({
      producer: noopProducer,
      outbox,
      monitor: fakeMonitor(healthySnap),
      metrics: fakeMetrics(emptyMetricsSnap),
    });
    const healthz = routes["/healthz"] as () => Response;
    const res = healthz();
    expect(res.status).toBe(200);
    const body = await res.json() as { status: string; dependencies: Record<string, { ok: boolean }> };
    expect(body.status).toBe("healthy");
    expect(body.dependencies.kafkaProducer?.ok).toBe(true);
  });

  it("healthz returns 200 + degraded when only topics are missing", async () => {
    const outbox = fakeOutbox();
    const degraded: HealthSnapshot = {
      ...healthySnap,
      status: "degraded",
      dependencies: {
        ...healthySnap.dependencies,
        topics: { ok: false, lastCheckedAt: 1_000, missing: ["T_MISSING"] },
      },
    };
    const routes = buildRoutes({
      producer: noopProducer,
      outbox,
      monitor: fakeMonitor(degraded),
      metrics: fakeMetrics(emptyMetricsSnap),
    });
    const healthz = routes["/healthz"] as () => Response;
    const res = healthz();
    expect(res.status).toBe(200);
    const body = await res.json() as { status: string; dependencies: { topics: { missing: string[] } } };
    expect(body.status).toBe("degraded");
    expect(body.dependencies.topics.missing).toEqual(["T_MISSING"]);
  });

  it("healthz returns 503 + unhealthy when a required dependency fails", async () => {
    const outbox = fakeOutbox();
    const unhealthy: HealthSnapshot = {
      ...healthySnap,
      status: "unhealthy",
      ok: false,
      dependencies: {
        ...healthySnap.dependencies,
        kafkaBroker: { ok: false, lastCheckedAt: 1_000, lastError: "connection refused" },
      },
    };
    const routes = buildRoutes({
      producer: noopProducer,
      outbox,
      monitor: fakeMonitor(unhealthy),
      metrics: fakeMetrics(emptyMetricsSnap),
    });
    const healthz = routes["/healthz"] as () => Response;
    const res = healthz();
    expect(res.status).toBe(503);
    const body = await res.json() as { status: string };
    expect(body.status).toBe("unhealthy");
  });

  it("healthz includes outbox stats and drain metrics", async () => {
    const outbox = fakeOutbox();
    const routes = buildRoutes({
      producer: noopProducer,
      outbox,
      monitor: fakeMonitor(healthySnap),
      metrics: fakeMetrics({
        publishedLast60s: 42,
        lastPublishedAt: 999,
        lastError: { topic: "T", message: "x", at: 500 },
      }),
    });
    const healthz = routes["/healthz"] as () => Response;
    const res = healthz();
    const body = await res.json() as {
      outbox: { publishedLast60s: number; lastPublishedAt: number | null; lastError: unknown; pendingByTopic: Record<string, number> };
    };
    expect(body.outbox.publishedLast60s).toBe(42);
    expect(body.outbox.lastPublishedAt).toBe(999);
    expect(body.outbox.lastError).toEqual({ topic: "T", message: "x", at: 500 });
    expect(body.outbox.pendingByTopic).toEqual({});
  });

  it("healthz still includes the registered routes", async () => {
    const outbox = fakeOutbox();
    const routes = buildRoutes({
      producer: noopProducer,
      outbox,
      monitor: fakeMonitor(healthySnap),
      metrics: fakeMetrics(emptyMetricsSnap),
    });
    const healthz = routes["/healthz"] as () => Response;
    const res = healthz();
    const body = await res.json() as { routes: Array<{ name: string }> };
    expect(body.routes.map((r) => r.name)).toEqual(["elastic-autoops", "datadog-alerts"]);
  });
});
