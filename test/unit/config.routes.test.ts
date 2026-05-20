// test/unit/config.routes.test.ts
import { describe, expect, it } from "bun:test";
import { routesSchema, type RouteConfig } from "../../src/config/schemas.ts";

const validRoute: RouteConfig = {
  name: "elastic-autoops",
  path: "/webhooks/elastic/autoops",
  topic: "T_PRIVATE_SOURCE_ELASTIC_AUTOOPS",
  keyFields: ["resourceId", "deployment-id"],
  idempotency: "elastic-autoops",
};

describe("routesSchema basics", () => {
  it("accepts a minimal valid route", () => {
    const r = routesSchema.safeParse([validRoute]);
    expect(r.success).toBe(true);
  });

  it("rejects an empty routes array", () => {
    const r = routesSchema.safeParse([]);
    expect(r.success).toBe(false);
  });

  it("rejects a path that doesn't start with /", () => {
    const r = routesSchema.safeParse([{ ...validRoute, path: "webhooks/x" }]);
    expect(r.success).toBe(false);
  });

  it("rejects empty keyFields", () => {
    const r = routesSchema.safeParse([{ ...validRoute, keyFields: [] }]);
    expect(r.success).toBe(false);
  });

  it("rejects an unknown idempotency strategy", () => {
    const r = routesSchema.safeParse([{ ...validRoute, idempotency: "nope" }]);
    expect(r.success).toBe(false);
    if (!r.success) {
      expect(r.error.issues.some((i) => /idempotency/.test(i.message))).toBe(true);
    }
  });

  it("rejects duplicate paths across routes", () => {
    const r = routesSchema.safeParse([
      validRoute,
      { ...validRoute, name: "second", topic: "T_PRIVATE_SOURCE_OTHER_THING" },
    ]);
    expect(r.success).toBe(false);
    if (!r.success) {
      expect(r.error.issues.some((i) => /path/i.test(i.message))).toBe(true);
    }
  });

  it("rejects duplicate topics across routes", () => {
    const r = routesSchema.safeParse([
      validRoute,
      { ...validRoute, name: "second", path: "/webhooks/other" },
    ]);
    expect(r.success).toBe(false);
    if (!r.success) {
      expect(r.error.issues.some((i) => /topic/i.test(i.message))).toBe(true);
    }
  });

  it("accepts an absent idempotency field", () => {
    const { idempotency: _omit, ...withoutIdem } = validRoute;
    const r = routesSchema.safeParse([withoutIdem]);
    expect(r.success).toBe(true);
  });

  it("accepts an absent dlqTopic", () => {
    const r = routesSchema.safeParse([validRoute]);
    expect(r.success).toBe(true);
  });
});
