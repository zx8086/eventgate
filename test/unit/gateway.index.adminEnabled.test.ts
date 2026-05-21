// test/unit/gateway.index.adminEnabled.test.ts
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
const noopOutbox = {
  enqueue: () => {},
  backlogStats: () => ({ pending: 0, failed: 0, oldestPendingAgeMs: 0, pendingByTopic: {} }),
};

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
  breakerOpenCount: 0,
};

const monitor = {
  start: async () => {},
  stop: async () => {},
  snapshot: () => healthySnap,
  probeOnce: async () => {},
};

const metrics = {
  recordPublished: () => {},
  recordError: () => {},
  incrementBreakerOpenCount: () => {},
  snapshot: () => emptyMetricsSnap,
};

let snapshot: NodeJS.ProcessEnv;

beforeEach(() => { snapshot = { ...process.env }; resetConfigCache(); });
afterEach(() => { process.env = snapshot; resetConfigCache(); });

describe("buildRoutes admin endpoint registration", () => {
  it("does NOT register /admin/routes when ADMIN_TOKEN is unset", () => {
    process.env = { ...process.env, ENVIRONMENT: "dev", KAFKA_PROVIDER: "local" };
    delete process.env.ADMIN_TOKEN;
    resetConfigCache();
    const routes = buildRoutes({ producer: noopProducer, outbox: noopOutbox, monitor, metrics });
    expect(routes["/admin/routes"]).toBeUndefined();
  });

  it("registers /admin/routes PUT when ADMIN_TOKEN is set AND adminContext is provided", () => {
    process.env = {
      ...process.env,
      ENVIRONMENT: "dev",
      KAFKA_PROVIDER: "local",
      ADMIN_TOKEN: "a".repeat(32),
    };
    resetConfigCache();
    const routes = buildRoutes({
      producer: noopProducer,
      outbox: noopOutbox,
      monitor,
      metrics,
      adminContext: {
        onReload: () => {},
        routesFilePath: "/tmp/eventgate-test-routes.json",
      },
    });
    const adminEntry = routes["/admin/routes"];
    expect(adminEntry).toBeDefined();
    expect((adminEntry as { PUT?: unknown }).PUT).toBeDefined();
  });

  it("does NOT register /admin/routes when ADMIN_TOKEN set but adminContext is not passed", () => {
    process.env = {
      ...process.env,
      ENVIRONMENT: "dev",
      KAFKA_PROVIDER: "local",
      ADMIN_TOKEN: "a".repeat(32),
    };
    resetConfigCache();
    const routes = buildRoutes({ producer: noopProducer, outbox: noopOutbox, monitor, metrics });
    expect(routes["/admin/routes"]).toBeUndefined();
  });
});
