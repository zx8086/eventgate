// test/unit/config.breaker.test.ts
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

describe("config.breaker", () => {
  it("exposes defaults when no env vars are set", async () => {
    const { config } = await import("../../src/config/index.ts");
    expect(config.breaker.failureThreshold).toBe(5);
    expect(config.breaker.successThreshold).toBe(3);
    expect(config.breaker.recoveryTimeoutMs).toBe(60_000);
  });

  it("applies env overrides", async () => {
    process.env = {
      ...process.env,
      CIRCUIT_BREAKER_FAILURE_THRESHOLD: "3",
      CIRCUIT_BREAKER_SUCCESS_THRESHOLD: "2",
      CIRCUIT_BREAKER_RECOVERY_TIMEOUT_MS: "30000",
    };
    resetConfigCache();
    const { config } = await import("../../src/config/index.ts");
    expect(config.breaker.failureThreshold).toBe(3);
    expect(config.breaker.successThreshold).toBe(2);
    expect(config.breaker.recoveryTimeoutMs).toBe(30_000);
  });

  it("rejects failureThreshold below 1", async () => {
    process.env = { ...process.env, CIRCUIT_BREAKER_FAILURE_THRESHOLD: "0" };
    resetConfigCache();
    const { config } = await import("../../src/config/index.ts");
    expect(() => config.breaker).toThrow(/failureThreshold/);
  });

  it("rejects recoveryTimeoutMs below 1000", async () => {
    process.env = { ...process.env, CIRCUIT_BREAKER_RECOVERY_TIMEOUT_MS: "500" };
    resetConfigCache();
    const { config } = await import("../../src/config/index.ts");
    expect(() => config.breaker).toThrow(/recoveryTimeoutMs/);
  });
});
