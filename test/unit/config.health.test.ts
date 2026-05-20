// test/unit/config.health.test.ts
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { resetConfigCache } from "../../src/config/loader.ts";

let snapshot: NodeJS.ProcessEnv;

beforeEach(() => {
  snapshot = { ...process.env };
  resetConfigCache();
});

afterEach(() => {
  process.env = snapshot;
  resetConfigCache();
});

describe("config.health", () => {
  it("exposes defaults when no env vars are set", async () => {
    const { config } = await import("../../src/config/index.ts");
    expect(config.health.probeIntervalMs).toBe(30_000);
    expect(config.health.probeTimeoutMs).toBe(5_000);
    expect(config.health.heartbeatMs).toBe(60_000);
  });

  it("applies env overrides", async () => {
    process.env = {
      ...process.env,
      HEALTH_PROBE_INTERVAL_MS: "15000",
      HEALTH_PROBE_TIMEOUT_MS: "2500",
      STATS_HEARTBEAT_MS: "0",
    };
    resetConfigCache();
    const { config } = await import("../../src/config/index.ts");
    expect(config.health.probeIntervalMs).toBe(15_000);
    expect(config.health.probeTimeoutMs).toBe(2_500);
    expect(config.health.heartbeatMs).toBe(0);
  });

  it("rejects negative probeIntervalMs", async () => {
    process.env = { ...process.env, HEALTH_PROBE_INTERVAL_MS: "-1" };
    resetConfigCache();
    expect(async () => {
      const { config } = await import("../../src/config/index.ts");
      void config.health;
    }).toThrow();
  });
});
