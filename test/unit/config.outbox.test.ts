// test/unit/config.outbox.test.ts
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { buildConfig, resetConfigCache } from "../../src/config/loader.ts";

describe("outbox config", () => {
  beforeEach(() => resetConfigCache());
  afterEach(() => resetConfigCache());

  it("defaults to enabled with sensible knobs", () => {
    const cfg = buildConfig({});
    expect(cfg.outbox.enabled).toBe(true);
    expect(cfg.outbox.dbPath).toBe("./data/outbox.db");
    expect(cfg.outbox.batchSize).toBe(100);
    expect(cfg.outbox.backoffMaxMs).toBe(600_000);
    expect(cfg.outbox.maxAgeHours).toBe(24);
    expect(cfg.outbox.idlePollMs).toBe(5_000);
    expect(cfg.outbox.busyPollMs).toBe(250);
    expect(cfg.outbox.backlogWarnThreshold).toBe(50_000);
  });

  it("accepts env-driven overrides", () => {
    const cfg = buildConfig({
      OUTBOX_ENABLED: "false",
      OUTBOX_DB_PATH: "/tmp/outbox.db",
      OUTBOX_BATCH_SIZE: "50",
      OUTBOX_BACKOFF_MAX_MS: "120000",
      OUTBOX_MAX_AGE_HOURS: "6",
      OUTBOX_IDLE_POLL_MS: "1000",
      OUTBOX_BUSY_POLL_MS: "100",
      OUTBOX_BACKLOG_WARN: "10000",
    });
    expect(cfg.outbox.enabled).toBe(false);
    expect(cfg.outbox.dbPath).toBe("/tmp/outbox.db");
    expect(cfg.outbox.batchSize).toBe(50);
    expect(cfg.outbox.backoffMaxMs).toBe(120_000);
    expect(cfg.outbox.maxAgeHours).toBe(6);
    expect(cfg.outbox.idlePollMs).toBe(1_000);
    expect(cfg.outbox.busyPollMs).toBe(100);
    expect(cfg.outbox.backlogWarnThreshold).toBe(10_000);
  });

  it("OUTBOX_ENABLED accepts true/false/1/0", () => {
    expect(buildConfig({ OUTBOX_ENABLED: "false" }).outbox.enabled).toBe(false);
    resetConfigCache();
    expect(buildConfig({ OUTBOX_ENABLED: "0" }).outbox.enabled).toBe(false);
    resetConfigCache();
    expect(buildConfig({ OUTBOX_ENABLED: "true" }).outbox.enabled).toBe(true);
    resetConfigCache();
    expect(buildConfig({ OUTBOX_ENABLED: "1" }).outbox.enabled).toBe(true);
  });

  it("rejects non-positive batchSize", () => {
    expect(() => buildConfig({ OUTBOX_BATCH_SIZE: "0" })).toThrow();
  });

  it("rejects non-positive maxAgeHours", () => {
    expect(() => buildConfig({ OUTBOX_MAX_AGE_HOURS: "0" })).toThrow();
  });

  it("rejects non-positive backoffMaxMs", () => {
    expect(() => buildConfig({ OUTBOX_BACKOFF_MAX_MS: "-1" })).toThrow();
  });

  it("treats whitespace-only OUTBOX_DB_PATH as unset (falls back to default)", () => {
    const cfg = buildConfig({ OUTBOX_DB_PATH: "   " });
    expect(cfg.outbox.dbPath).toBe("./data/outbox.db");
  });
});
