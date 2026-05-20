// test/unit/config.routes.reserved.test.ts
import { describe, expect, it } from "bun:test";
import { routesSchema, type RouteConfig } from "../../src/config/schemas.ts";

const base: RouteConfig = {
  name: "x",
  path: "/webhooks/x",
  topic: "T_PRIVATE_SOURCE_X_Y",
  dlqTopic: "DLQ_T_PRIVATE_SOURCE_X_Y",
  sourceHeader: "x",
  keyFields: ["id"],
  idempotency: "elastic-autoops",
};

describe("routesSchema rejects reserved paths", () => {
  it("rejects /healthz with a reserved-path message", () => {
    const r = routesSchema.safeParse([{ ...base, path: "/healthz" }]);
    expect(r.success).toBe(false);
    if (!r.success) {
      expect(r.error.issues.some((i) => /'\/healthz' is reserved/.test(i.message))).toBe(true);
    }
  });

  it("rejects /admin/routes with a reserved-path message", () => {
    const r = routesSchema.safeParse([{ ...base, path: "/admin/routes" }]);
    expect(r.success).toBe(false);
    if (!r.success) {
      expect(r.error.issues.some((i) => /'\/admin\/routes' is reserved/.test(i.message))).toBe(true);
    }
  });

  it("accepts non-reserved paths", () => {
    expect(routesSchema.safeParse([base]).success).toBe(true);
  });

  it("accepts /admin (no trailing /routes)", () => {
    expect(routesSchema.safeParse([{ ...base, path: "/admin" }]).success).toBe(true);
  });
});
