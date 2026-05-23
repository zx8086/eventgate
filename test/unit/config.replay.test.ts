// test/unit/config.replay.test.ts
import { afterEach, describe, expect, it } from "bun:test";
import { resetConfigCache } from "../../src/config/loader.ts";
import { config } from "../../src/config/index.ts";

const KEYS = [
  "REPLAY_ENABLED",
  "REPLAY_MAX_ATTEMPTS",
  "REPLAY_TRANSIENT_ERRORS",
  "REPLAY_POISON_ERRORS",
  "REPLAY_DEFAULT",
  "REPLAY_MAX_RECORDS_PER_JOB",
  "REPLAY_RATE_LIMIT_PER_SEC",
  "REPLAY_PARKING_TOPIC_SUFFIX",
  "REPLAY_AUTO_ENABLED",
  "REPLAY_AUTO_INTERVAL_MS",
  "REPLAY_AUTO_DLQ_DEPTH_THRESHOLD",
  "REPLAY_AUTO_PROBE_WINDOW_RECORDS",
] as const;

const SAVE: Record<string, string | undefined> = {};

function snapshot(): void {
  for (const k of KEYS) SAVE[k] = process.env[k];
}
function restore(): void {
  for (const k of KEYS) {
    if (SAVE[k] === undefined) delete process.env[k];
    else process.env[k] = SAVE[k];
  }
  resetConfigCache();
}

afterEach(restore);

describe("config.replay", () => {
  it("is undefined when no REPLAY_* env var is set", () => {
    snapshot();
    for (const k of KEYS) delete process.env[k];
    resetConfigCache();
    expect(config.replay).toBeUndefined();
  });

  it("is populated with defaults when REPLAY_ENABLED=true and no other replay vars are set", () => {
    snapshot();
    for (const k of KEYS) delete process.env[k];
    process.env.REPLAY_ENABLED = "true";
    resetConfigCache();
    expect(config.replay?.enabled).toBe(true);
    expect(config.replay?.maxAttempts).toBe(5);
    expect(config.replay?.default).toBe("park");
    expect(config.replay?.parkingTopicSuffix).toBe(".parked");
    expect(config.replay?.maxRecordsPerJob).toBe(10_000);
    expect(config.replay?.rateLimitPerSec).toBe(500);
    expect(config.replay?.transientErrors).toEqual([]);
    expect(config.replay?.poisonErrors).toEqual([]);
    expect(config.replay?.auto.enabled).toBe(false);
    expect(config.replay?.auto.intervalMs).toBe(300_000);
    expect(config.replay?.auto.dlqDepthThreshold).toBe(100);
    expect(config.replay?.auto.probeWindowRecords).toBe(500);
  });

  it("is populated with enabled=false when only a sub-key is set", () => {
    snapshot();
    for (const k of KEYS) delete process.env[k];
    process.env.REPLAY_MAX_ATTEMPTS = "10";
    resetConfigCache();
    expect(config.replay).toBeDefined();
    expect(config.replay?.enabled).toBe(false);
    expect(config.replay?.maxAttempts).toBe(10);
  });

  it("parses CSV env vars into string arrays", () => {
    snapshot();
    for (const k of KEYS) delete process.env[k];
    process.env.REPLAY_ENABLED = "true";
    process.env.REPLAY_TRANSIENT_ERRORS =
      "org.apache.kafka.common.errors.TimeoutException,org.apache.kafka.common.errors.RetriableException";
    process.env.REPLAY_POISON_ERRORS =
      "org.apache.kafka.common.errors.SerializationException";
    resetConfigCache();
    expect(config.replay?.transientErrors).toEqual([
      "org.apache.kafka.common.errors.TimeoutException",
      "org.apache.kafka.common.errors.RetriableException",
    ]);
    expect(config.replay?.poisonErrors).toEqual([
      "org.apache.kafka.common.errors.SerializationException",
    ]);
  });

  it("accepts numeric overrides for auto sub-block", () => {
    snapshot();
    for (const k of KEYS) delete process.env[k];
    process.env.REPLAY_ENABLED = "true";
    process.env.REPLAY_AUTO_ENABLED = "true";
    process.env.REPLAY_AUTO_INTERVAL_MS = "600000";
    process.env.REPLAY_AUTO_DLQ_DEPTH_THRESHOLD = "250";
    process.env.REPLAY_AUTO_PROBE_WINDOW_RECORDS = "1000";
    resetConfigCache();
    expect(config.replay?.auto.enabled).toBe(true);
    expect(config.replay?.auto.intervalMs).toBe(600_000);
    expect(config.replay?.auto.dlqDepthThreshold).toBe(250);
    expect(config.replay?.auto.probeWindowRecords).toBe(1000);
  });

  it("rejects REPLAY_DEFAULT outside the allowed enum", () => {
    snapshot();
    for (const k of KEYS) delete process.env[k];
    process.env.REPLAY_ENABLED = "true";
    process.env.REPLAY_DEFAULT = "explode";
    resetConfigCache();
    expect(() => config.replay).toThrow(/replay.default/);
  });

  it("rejects REPLAY_AUTO_INTERVAL_MS below the 60s floor", () => {
    snapshot();
    for (const k of KEYS) delete process.env[k];
    process.env.REPLAY_ENABLED = "true";
    process.env.REPLAY_AUTO_INTERVAL_MS = "30000";
    resetConfigCache();
    expect(() => config.replay).toThrow(/replay.auto.intervalMs/);
  });
});
