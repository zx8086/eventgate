import { describe, expect, it } from "bun:test";
import { autoOpsIdempotencyKey } from "../../src/gateway/idempotencyKey.ts";

describe("autoOpsIdempotencyKey", () => {
  it("returns a stable sha256 hex string for a complete AutoOps body (camelCase)", () => {
    const body = {
      resourceId: "deploy-1",
      title: "JVM high",
      status: "open",
      startTime: "2026-05-19T00:00:00Z",
      endTime: null,
    };
    const k1 = autoOpsIdempotencyKey(body);
    const k2 = autoOpsIdempotencyKey({ ...body });
    expect(k1).toMatch(/^[0-9a-f]{64}$/);
    expect(k1).toBe(k2);
  });

  it("treats hyphenated keys (deployment-id, start-time, end-time) as equivalent", () => {
    const camel = autoOpsIdempotencyKey({
      resourceId: "deploy-1",
      title: "JVM high",
      status: "RESOLVED",
      startTime: "2026-05-19T00:00:00Z",
      endTime: "2026-05-19T00:05:00Z",
    });
    const hyphen = autoOpsIdempotencyKey({
      "deployment-id": "deploy-1",
      title: "JVM high",
      status: "RESOLVED",
      "start-time": "2026-05-19T00:00:00Z",
      "end-time": "2026-05-19T00:05:00Z",
    });
    expect(hyphen).toBe(camel);
  });

  it("returns undefined when resourceId, title, or status is missing", () => {
    expect(autoOpsIdempotencyKey({ title: "x", status: "open" })).toBeUndefined();
    expect(autoOpsIdempotencyKey({ resourceId: "x", status: "open" })).toBeUndefined();
    expect(autoOpsIdempotencyKey({ resourceId: "x", title: "x" })).toBeUndefined();
    expect(autoOpsIdempotencyKey({})).toBeUndefined();
    expect(autoOpsIdempotencyKey(null)).toBeUndefined();
    expect(autoOpsIdempotencyKey("not an object")).toBeUndefined();
    expect(autoOpsIdempotencyKey([1, 2, 3])).toBeUndefined();
  });

  it("returns different hashes for different bodies", () => {
    const a = autoOpsIdempotencyKey({ resourceId: "1", title: "t", status: "open" });
    const b = autoOpsIdempotencyKey({ resourceId: "2", title: "t", status: "open" });
    const c = autoOpsIdempotencyKey({ resourceId: "1", title: "t", status: "close" });
    expect(a).not.toBe(b);
    expect(a).not.toBe(c);
  });
});
