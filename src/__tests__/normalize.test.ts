import { describe, expect, it } from "bun:test";
import {
  asArray,
  buildAlertSignature,
  buildIdempotencyKey,
  normalizeElasticAutoOps,
  normalizeSeverity,
  normalizeStatus,
  severityRank,
} from "../normalize.ts";
import type { ElasticAutoOpsWebhook } from "../types.ts";

const ctx = {
  tenant: "elastic-cloud",
  environment: "prod",
  now: () => new Date("2026-05-18T19:28:00.000Z"),
};

describe("normalizeSeverity", () => {
  it("maps the AutoOps docs values regardless of case", () => {
    expect(normalizeSeverity("High")).toBe("high");
    expect(normalizeSeverity("HIGH")).toBe("high");
    expect(normalizeSeverity("medium")).toBe("medium");
    expect(normalizeSeverity("Low")).toBe("low");
  });

  it("returns unknown for missing or garbage", () => {
    expect(normalizeSeverity(undefined)).toBe("unknown");
    expect(normalizeSeverity("")).toBe("unknown");
    expect(normalizeSeverity("critical")).toBe("unknown");
  });
});

describe("severityRank", () => {
  it("maps to ordered ranks", () => {
    expect(severityRank("low")).toBe(1);
    expect(severityRank("medium")).toBe(2);
    expect(severityRank("high")).toBe(3);
    expect(severityRank("unknown")).toBe(0);
  });
});

describe("normalizeStatus", () => {
  it("accepts AutoOps docs spelling (open/close)", () => {
    expect(normalizeStatus("open")).toBe("opened");
    expect(normalizeStatus("close")).toBe("closed");
  });

  it("accepts the longer spelling (opened/closed)", () => {
    expect(normalizeStatus("opened")).toBe("opened");
    expect(normalizeStatus("Closed")).toBe("closed");
  });

  it("returns unknown for empty/garbage", () => {
    expect(normalizeStatus(undefined)).toBe("unknown");
    expect(normalizeStatus("")).toBe("unknown");
    expect(normalizeStatus("pending")).toBe("unknown");
  });
});

describe("asArray", () => {
  it("returns empty for missing", () => {
    expect(asArray(undefined)).toEqual([]);
  });

  it("preserves arrays and trims entries", () => {
    expect(asArray(["a", " b ", ""])).toEqual(["a", "b"]);
  });

  it("splits CSV strings", () => {
    expect(asArray("a, b ,c")).toEqual(["a", "b", "c"]);
  });
});

describe("buildAlertSignature", () => {
  it("is stable across open/close because status is excluded", () => {
    const sig = buildAlertSignature("r-123", "JVM memory pressure high");
    expect(sig).toBe("r-123-jvm-memory-pressure-high");
  });
});

describe("buildIdempotencyKey", () => {
  const base = {
    resourceId: "r-123",
    title: "JVM memory pressure high",
    status: "opened" as const,
    startTime: "2026-05-18T19:27:40Z",
    endTime: null,
  };

  it("is deterministic for the same inputs", () => {
    expect(buildIdempotencyKey(base)).toBe(buildIdempotencyKey(base));
  });

  it("changes when status changes", () => {
    expect(buildIdempotencyKey(base)).not.toBe(
      buildIdempotencyKey({ ...base, status: "closed" }),
    );
  });

  it("changes when endTime changes (close event distinct from open)", () => {
    expect(buildIdempotencyKey(base)).not.toBe(
      buildIdempotencyKey({ ...base, endTime: "2026-05-18T20:00:00Z" }),
    );
  });

  it("changes when title changes", () => {
    expect(buildIdempotencyKey(base)).not.toBe(
      buildIdempotencyKey({ ...base, title: "Different alert" }),
    );
  });
});

describe("normalizeElasticAutoOps", () => {
  const raw: ElasticAutoOpsWebhook = {
    resourceId: "r-123",
    resourceName: "search-prod-eu",
    title: "JVM memory pressure high",
    description: "Heap is high",
    severity: "High",
    status: "open",
    message: "JVM heap exceeded 85% for 10 minutes",
    startTime: "2026-05-18T19:27:40Z",
    endTime: null,
    endpointType: "Webhook",
    affectedNodes: "node-1, node-2",
    affectedIndices: ["metrics-2026.05"],
    eventLink: "https://autoops.example/events/abc",
  };

  it("produces a fully shaped normalized event", () => {
    const event = normalizeElasticAutoOps(raw, ctx);
    expect(event).toEqual({
      schemaVersion: 1,
      source: "elastic-autoops",
      eventCategory: "cluster-alert",
      eventType: "opened",
      receivedAt: "2026-05-18T19:28:00.000Z",
      occurredAt: "2026-05-18T19:27:40Z",
      idempotencyKey: event.idempotencyKey,
      routingKey: "r-123",
      tenant: "elastic-cloud",
      environment: "prod",
      endpointType: "Webhook",
      resource: {
        id: "r-123",
        name: "search-prod-eu",
        kind: "elastic-deployment",
      },
      alert: {
        title: "JVM memory pressure high",
        description: "Heap is high",
        severity: "high",
        severityRank: 3,
        status: "opened",
        message: "JVM heap exceeded 85% for 10 minutes",
        link: "https://autoops.example/events/abc",
        startTime: "2026-05-18T19:27:40Z",
        endTime: null,
        affectedNodes: ["node-1", "node-2"],
        affectedIndices: ["metrics-2026.05"],
        alertSignature: "r-123-jvm-memory-pressure-high",
        isOpen: true,
      },
      raw,
    });
    expect(event.idempotencyKey).toMatch(/^[a-f0-9]{64}$/);
  });

  it("falls back occurredAt to receivedAt when startTime missing", () => {
    const event = normalizeElasticAutoOps({ ...raw, startTime: undefined }, ctx);
    expect(event.occurredAt).toBe("2026-05-18T19:28:00.000Z");
  });

  it("marks isOpen=false on close", () => {
    const event = normalizeElasticAutoOps(
      { ...raw, status: "close", endTime: "2026-05-18T20:00:00Z" },
      ctx,
    );
    expect(event.eventType).toBe("closed");
    expect(event.alert.isOpen).toBe(false);
  });
});
