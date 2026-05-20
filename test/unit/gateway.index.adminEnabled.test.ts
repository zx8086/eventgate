// test/unit/gateway.index.adminEnabled.test.ts
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { resetConfigCache } from "../../src/config/loader.ts";
import { buildRoutes } from "../../src/gateway/routes.ts";

const noopProducer = {
  isConnected: () => true,
  disconnect: async () => {},
  sendByTopic: async () => {},
};
const noopOutbox = {
  enqueue: () => {},
  backlogStats: () => ({ pending: 0, failed: 0, oldestPendingAgeMs: 0 }),
};

let snapshot: NodeJS.ProcessEnv;

beforeEach(() => { snapshot = { ...process.env }; resetConfigCache(); });
afterEach(() => { process.env = snapshot; resetConfigCache(); });

describe("buildRoutes admin endpoint registration", () => {
  it("does NOT register /admin/routes when ADMIN_TOKEN is unset", () => {
    process.env = { ...process.env, ENVIRONMENT: "dev", KAFKA_PROVIDER: "local" };
    delete process.env.ADMIN_TOKEN;
    resetConfigCache();
    const routes = buildRoutes({ producer: noopProducer, outbox: noopOutbox });
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
    const routes = buildRoutes({ producer: noopProducer, outbox: noopOutbox });
    expect(routes["/admin/routes"]).toBeUndefined();
  });
});
