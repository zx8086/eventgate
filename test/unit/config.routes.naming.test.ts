// test/unit/config.routes.naming.test.ts
import { describe, expect, it } from "bun:test";
import { routesSchema, type RouteConfig } from "../../src/config/schemas.ts";

const base: RouteConfig = {
  name: "elastic-autoops",
  path: "/webhooks/elastic/autoops",
  topic: "T_PRIVATE_SOURCE_ELASTIC_AUTOOPS",
  keyFields: ["resourceId"],
};

describe("topic naming policy enforced by routesSchema", () => {
  it("accepts T_PRIVATE_SOURCE_<SYSTEM>_<ENTITY>", () => {
    expect(routesSchema.safeParse([base]).success).toBe(true);
  });

  it("rejects lowercase", () => {
    const r = routesSchema.safeParse([{ ...base, topic: "t_private_source_elastic_autoops" }]);
    expect(r.success).toBe(false);
  });

  it("rejects legacy ops.*.raw.v1", () => {
    const r = routesSchema.safeParse([{ ...base, topic: "ops.elastic.autoops.raw.v1" }]);
    expect(r.success).toBe(false);
  });

  it("rejects T_PUBLIC_* with MDM message", () => {
    const r = routesSchema.safeParse([{ ...base, topic: "T_PUBLIC_SOURCE_PIM_ARTICLES_MDM" }]);
    expect(r.success).toBe(false);
    if (!r.success) {
      expect(r.error.issues.some((i) => /MDM publisher/i.test(i.message))).toBe(true);
    }
  });

  it("rejects T_PRIVATE_SINK_* with sink message", () => {
    const r = routesSchema.safeParse([{ ...base, topic: "T_PRIVATE_SINK_COUCHBASE_PRICE_DOCUMENTS" }]);
    expect(r.success).toBe(false);
    if (!r.success) {
      expect(r.error.issues.some((i) => /sink connector/i.test(i.message))).toBe(true);
    }
  });

  it("rejects DLQ_T_* used as topic", () => {
    const r = routesSchema.safeParse([{ ...base, topic: "DLQ_T_PRIVATE_SOURCE_ELASTIC_AUTOOPS" }]);
    expect(r.success).toBe(false);
    if (!r.success) {
      expect(r.error.issues.some((i) => /dlqTopic/.test(i.message))).toBe(true);
    }
  });

  it("rejects internal event streams (_RICH_NOTIFICATIONS, _EVENTS)", () => {
    expect(
      routesSchema.safeParse([{ ...base, topic: "T_PRIVATE_PRODUCT_RICH_NOTIFICATIONS" }]).success,
    ).toBe(false);
    expect(
      routesSchema.safeParse([{ ...base, topic: "T_PRIVATE_CORRECTED_DELIVERY_DATES_CHANGED_EVENTS" }])
        .success,
    ).toBe(false);
  });

  it("rejects system prefixes (__, _schemas, _confluent-)", () => {
    expect(routesSchema.safeParse([{ ...base, topic: "__consumer_offsets" }]).success).toBe(false);
    expect(routesSchema.safeParse([{ ...base, topic: "_schemas" }]).success).toBe(false);
    expect(routesSchema.safeParse([{ ...base, topic: "_confluent-monitoring" }]).success).toBe(false);
  });

  it("rejects topic length > 249", () => {
    const long = "T_PRIVATE_SOURCE_X_" + "A".repeat(240);
    expect(routesSchema.safeParse([{ ...base, topic: long }]).success).toBe(false);
  });
});

describe("dlqTopic rule", () => {
  it("accepts dlqTopic equal to DLQ_T_<topic>", () => {
    const r = routesSchema.safeParse([
      { ...base, dlqTopic: "DLQ_T_PRIVATE_SOURCE_ELASTIC_AUTOOPS" },
    ]);
    expect(r.success).toBe(true);
  });

  it("rejects mismatched dlqTopic", () => {
    const r = routesSchema.safeParse([{ ...base, dlqTopic: "DLQ_T_WRONG_NAME" }]);
    expect(r.success).toBe(false);
    if (!r.success) {
      expect(r.error.issues.some((i) => /dlqTopic must be/.test(i.message))).toBe(true);
    }
  });

  it("rejects duplicate dlqTopic across routes (defensive)", () => {
    const r = routesSchema.safeParse([
      { ...base, dlqTopic: "DLQ_T_PRIVATE_SOURCE_ELASTIC_AUTOOPS" },
      {
        name: "second",
        path: "/webhooks/elastic/autoops-2",
        topic: "T_PRIVATE_SOURCE_ELASTIC_AUTOOPS_TWO",
        keyFields: ["x"],
        dlqTopic: "DLQ_T_PRIVATE_SOURCE_ELASTIC_AUTOOPS",
      },
    ]);
    expect(r.success).toBe(false);
    if (!r.success) {
      // Both the mismatch rule and the dedup rule fire here; assert the
      // dedup message explicitly so future deletion of the dedup check
      // would be caught by this test.
      expect(r.error.issues.some((i) => /duplicate dlqTopic/.test(i.message))).toBe(true);
    }
  });
});
