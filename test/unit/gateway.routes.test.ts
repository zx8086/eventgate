// test/unit/gateway.routes.test.ts
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { resetConfigCache } from "../../src/config/loader.ts";
import { buildRoutes } from "../../src/gateway/routes.ts";
import type { HealthSnapshot } from "../../src/health/types.ts";
import type { EventProducer } from "../../src/kafka/producer.ts";
import type { DrainMetricsSnapshot } from "../../src/outbox/metrics.ts";
import type { OutboxWriter, EnqueueInput, BacklogStats } from "../../src/outbox/writer.ts";

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

function fakeMonitor(snap: HealthSnapshot = healthySnap) {
  return {
    start: async () => {},
    stop: async () => {},
    snapshot: () => snap,
    probeOnce: async () => {},
  };
}

function fakeMetrics(snap: DrainMetricsSnapshot = emptyMetricsSnap) {
  return {
    recordPublished: () => {},
    recordError: () => {},
    snapshot: () => snap,
  };
}

function fakeProducer(): EventProducer {
  return {
    sendByTopic: async () => {},
    isConnected: () => true,
    disconnect: async () => {},
  };
}

function fakeOutbox(): { writer: OutboxWriter; rows: EnqueueInput[] } {
  const rows: EnqueueInput[] = [];
  const writer: OutboxWriter = {
    enqueue(row: EnqueueInput) {
      rows.push(row);
    },
    backlogStats(): BacklogStats {
      return { pending: rows.length, failed: 0, oldestPendingAgeMs: 0, pendingByTopic: {} };
    },
  };
  return { writer, rows };
}

async function postJson(routes: ReturnType<typeof buildRoutes>, body: string, contentType = "application/json") {
  const route = routes["/webhooks/elastic/autoops"] as { POST: (req: Request) => Promise<Response> };
  return route.POST(
    new Request("http://test/webhooks/elastic/autoops", {
      method: "POST",
      headers: { "content-type": contentType },
      body,
    }),
  );
}

let snapshot: NodeJS.ProcessEnv;

beforeEach(() => {
  snapshot = { ...process.env };
  // Use default route config (elastic-autoops only); reset to ensure clean state.
  delete process.env.ROUTES_JSON;
  resetConfigCache();
});

afterEach(() => {
  process.env = snapshot;
  resetConfigCache();
});

describe("POST /webhooks/elastic/autoops", () => {
  it("returns 400 on non-JSON body", async () => {
    const { writer, rows } = fakeOutbox();
    const routes = buildRoutes({ producer: fakeProducer(), outbox: writer, monitor: fakeMonitor(), metrics: fakeMetrics() });
    const res = await postJson(routes, "not json");
    expect(res.status).toBe(400);
    const json = (await res.json()) as { accepted: boolean; error: string };
    expect(json.accepted).toBe(false);
    expect(json.error).toBe("invalid JSON body");
    expect(rows).toHaveLength(0);
  });

  it("returns 202 and enqueues one row with source header only for non-AutoOps JSON", async () => {
    const { writer, rows } = fakeOutbox();
    const routes = buildRoutes({ producer: fakeProducer(), outbox: writer, monitor: fakeMonitor(), metrics: fakeMetrics() });
    const res = await postJson(routes, JSON.stringify({ hello: "world" }));
    expect(res.status).toBe(202);
    expect(await res.json()).toEqual({ accepted: true });
    expect(rows).toHaveLength(1);
    // Topic comes from the default route config, not a hardcoded string.
    expect(rows[0].topic).toBe("T_PRIVATE_SOURCE_ELASTIC_AUTOOPS");
    expect(rows[0].messageKey).toBe("unkeyed");
    expect(rows[0].headers).toEqual({ source: "elastic-autoops" });
    const payload = JSON.parse(rows[0].payload) as { receivedAt: string; raw: unknown };
    expect(payload.raw).toEqual({ hello: "world" });
    expect(payload.receivedAt).toMatch(/^\d{4}-\d{2}-\d{2}T/);
  });

  it("returns 202 and includes the idempotencyKey header for an AutoOps body", async () => {
    const { writer, rows } = fakeOutbox();
    const routes = buildRoutes({ producer: fakeProducer(), outbox: writer, monitor: fakeMonitor(), metrics: fakeMetrics() });
    const body = {
      resourceId: "deploy-1",
      title: "JVM pressure",
      status: "open",
      startTime: "2026-05-19T00:00:00Z",
    };
    const res = await postJson(routes, JSON.stringify(body));
    expect(res.status).toBe(202);
    expect(rows).toHaveLength(1);
    expect(rows[0].messageKey).toBe("deploy-1");
    expect(rows[0].headers).toHaveProperty("source", "elastic-autoops");
    expect(rows[0].headers).toHaveProperty("idempotencyKey");
    expect((rows[0].headers as Record<string, string>).idempotencyKey).toMatch(/^[0-9a-f]{64}$/);
  });

  it("uses deployment-id (hyphenated) as message key when resourceId is absent", async () => {
    const { writer, rows } = fakeOutbox();
    const routes = buildRoutes({ producer: fakeProducer(), outbox: writer, monitor: fakeMonitor(), metrics: fakeMetrics() });
    const body = {
      "deployment-id": "deploy-2",
      title: "x",
      status: "open",
    };
    const res = await postJson(routes, JSON.stringify(body));
    expect(res.status).toBe(202);
    expect(rows[0].messageKey).toBe("deploy-2");
  });

  it("falls back to 'unkeyed' when neither resourceId nor deployment-id is present", async () => {
    const { writer, rows } = fakeOutbox();
    const routes = buildRoutes({ producer: fakeProducer(), outbox: writer, monitor: fakeMonitor(), metrics: fakeMetrics() });
    await postJson(routes, JSON.stringify({ anything: 1 }));
    expect(rows[0].messageKey).toBe("unkeyed");
  });
});
