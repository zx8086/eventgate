// test/unit/config.routes.env.test.ts
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { buildConfig } from "../../src/config/loader.ts";

const baseEnv = {
  ENVIRONMENT: "dev",
  KAFKA_PROVIDER: "local",
};

let snapshot: NodeJS.ProcessEnv;

beforeEach(() => {
  snapshot = { ...process.env };
});
afterEach(() => {
  process.env = snapshot;
});

describe("ROUTES_JSON override", () => {
  it("falls back to defaults when ROUTES_JSON is absent", () => {
    const cfg = buildConfig({ ...baseEnv });
    expect(cfg.routes).toHaveLength(1);
    expect(cfg.routes[0]?.name).toBe("elastic-autoops");
    expect(cfg.routes[0]?.topic).toBe("T_PRIVATE_SOURCE_ELASTIC_AUTOOPS");
  });

  it("replaces the default array wholesale when ROUTES_JSON is set", () => {
    const ROUTES_JSON = JSON.stringify([
      {
        name: "datadog-alerts",
        path: "/webhooks/datadog/alerts",
        topic: "T_PRIVATE_SOURCE_DATADOG_ALERTS",
        keyFields: ["alert_id", "id"],
      },
    ]);
    const cfg = buildConfig({ ...baseEnv, ROUTES_JSON });
    expect(cfg.routes).toHaveLength(1);
    expect(cfg.routes[0]?.name).toBe("datadog-alerts");
    expect(cfg.routes[0]?.topic).toBe("T_PRIVATE_SOURCE_DATADOG_ALERTS");
  });

  it("rejects malformed ROUTES_JSON via Zod (entry without keyFields)", () => {
    const ROUTES_JSON = JSON.stringify([
      {
        name: "x",
        path: "/x",
        topic: "T_PRIVATE_SOURCE_X_Y",
      },
    ]);
    expect(() => buildConfig({ ...baseEnv, ROUTES_JSON })).toThrow(/Invalid configuration/);
  });

  it("ignores ROUTES_JSON that is not a JSON array (defaults remain)", () => {
    const cfg = buildConfig({ ...baseEnv, ROUTES_JSON: "not json" });
    expect(cfg.routes[0]?.name).toBe("elastic-autoops");
  });

  it("rejects empty ROUTES_JSON array (defaults are not used as a fallback)", () => {
    expect(() => buildConfig({ ...baseEnv, ROUTES_JSON: "[]" })).toThrow(/Invalid configuration/);
  });
});
