// test/unit/replay.triage.test.ts
import { describe, expect, it } from "bun:test";
import type { ReplayConfig } from "../../src/config/schemas.ts";
import { triage } from "../../src/replay/triage.ts";
import type { DlqRecord, HeaderTuple } from "../../src/replay/types.ts";

function h(name: string, val: string): HeaderTuple {
  return [Buffer.from(name), Buffer.from(val)];
}

function rec(headers: HeaderTuple[] = []): DlqRecord {
  return {
    topic: "DLQ_T_PRIVATE_SOURCE_ELASTIC_AUTOOPS",
    partition: 0,
    offset: 1,
    key: Buffer.from("k"),
    value: Buffer.from("v"),
    headers,
    timestamp: Date.now(),
  };
}

const baseCfg: ReplayConfig = {
  enabled: true,
  maxAttempts: 5,
  transientErrors: ["org.apache.kafka.common.errors.RetriableException"],
  poisonErrors: ["org.apache.kafka.connect.errors.DataException"],
  default: "park",
  maxRecordsPerJob: 100,
  rateLimitPerSec: 100,
  parkingTopicSuffix: ".parked",
  auto: {
    enabled: false,
    intervalMs: 300_000,
    dlqDepthThreshold: 100,
    probeWindowRecords: 500,
  },
};

describe("triage", () => {
  it("parks when attempt >= maxAttempts (loop guard)", () => {
    const r = rec([
      h("x-eventgate-replay-attempt", "5"),
      h(
        "__connect.errors.exception.class.name",
        "org.apache.kafka.common.errors.RetriableException",
      ),
    ]);
    const d = triage(r, baseCfg);
    expect(d.kind).toBe("park");
    if (d.kind === "park") {
      expect(d.reason).toBe("exceeded_attempts");
      expect(d.exceptionClass).toBe(
        "org.apache.kafka.common.errors.RetriableException",
      );
    }
  });

  it("parks when class is in poisonErrors", () => {
    const r = rec([
      h(
        "__connect.errors.exception.class.name",
        "org.apache.kafka.connect.errors.DataException",
      ),
    ]);
    const d = triage(r, baseCfg);
    expect(d.kind).toBe("park");
    if (d.kind === "park") expect(d.reason).toBe("poison_class");
  });

  it("replays when class is in transientErrors", () => {
    const r = rec([
      h(
        "__connect.errors.exception.class.name",
        "org.apache.kafka.common.errors.RetriableException",
      ),
    ]);
    const d = triage(r, baseCfg);
    expect(d.kind).toBe("replay");
    if (d.kind === "replay") {
      expect(d.exceptionClass).toBe(
        "org.apache.kafka.common.errors.RetriableException",
      );
    }
  });

  it("falls back to default=park when class matches neither list", () => {
    const r = rec([
      h("__connect.errors.exception.class.name", "some.Unknown.Exception"),
    ]);
    const d = triage(r, baseCfg);
    expect(d.kind).toBe("park");
    if (d.kind === "park") expect(d.reason).toBe("default_park");
  });

  it("falls back to default=replay when configured", () => {
    const cfg: ReplayConfig = { ...baseCfg, default: "replay" };
    const r = rec([
      h("__connect.errors.exception.class.name", "some.Unknown.Exception"),
    ]);
    const d = triage(r, cfg);
    expect(d.kind).toBe("replay");
  });

  it("treats a missing exception class header as the fallback path", () => {
    const r = rec([]);
    const d = triage(r, baseCfg);
    expect(d.kind).toBe("park");
    if (d.kind === "park") {
      expect(d.reason).toBe("default_park");
      expect(d.exceptionClass).toBeNull();
    }
  });

  it("loop-guard trumps poison classification when attempt is at cap", () => {
    const r = rec([
      h("x-eventgate-replay-attempt", "5"),
      h(
        "__connect.errors.exception.class.name",
        "org.apache.kafka.connect.errors.DataException",
      ),
    ]);
    const d = triage(r, baseCfg);
    expect(d.kind).toBe("park");
    if (d.kind === "park") expect(d.reason).toBe("exceeded_attempts");
  });

  it("malformed attempt header (negative) coerces to 0 and does not bypass cap", () => {
    const r = rec([
      h("x-eventgate-replay-attempt", "-99"),
      h(
        "__connect.errors.exception.class.name",
        "org.apache.kafka.common.errors.RetriableException",
      ),
    ]);
    const d = triage(r, baseCfg);
    expect(d.kind).toBe("replay");
  });

  it("garbage attempt header (5abc) coerces to 0", () => {
    const r = rec([
      h("x-eventgate-replay-attempt", "5abc"),
      h(
        "__connect.errors.exception.class.name",
        "org.apache.kafka.common.errors.RetriableException",
      ),
    ]);
    const d = triage(r, baseCfg);
    expect(d.kind).toBe("replay");
  });
});
