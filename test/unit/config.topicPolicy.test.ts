// test/unit/config.topicPolicy.test.ts
import { describe, expect, it } from "bun:test";
import { checkGatewayTopic, expectedDlqTopic, isGatewayTopic } from "../../src/config/topicPolicy.ts";

describe("isGatewayTopic", () => {
  it("accepts T_PRIVATE_SOURCE_<SYSTEM>_<ENTITY>", () => {
    expect(isGatewayTopic("T_PRIVATE_SOURCE_ELASTIC_AUTOOPS")).toBe(true);
    expect(isGatewayTopic("T_PRIVATE_SOURCE_DATADOG_ALERTS")).toBe(true);
    expect(isGatewayTopic("T_PRIVATE_SOURCE_GITHUB_PR_EVENTS")).toBe(true);
  });

  it("rejects lowercase", () => {
    expect(isGatewayTopic("t_private_source_elastic_autoops")).toBe(false);
  });

  it("rejects legacy ops.*.raw.v1", () => {
    expect(isGatewayTopic("ops.elastic.autoops.raw.v1")).toBe(false);
  });

  it("rejects T_PUBLIC_*", () => {
    expect(isGatewayTopic("T_PUBLIC_SOURCE_PIM_ARTICLES_MDM")).toBe(false);
  });

  it("rejects T_PRIVATE_SINK_*", () => {
    expect(isGatewayTopic("T_PRIVATE_SINK_COUCHBASE_PRICE_DOCUMENTS")).toBe(false);
  });

  it("rejects T_PRIVATE_*_RICH_NOTIFICATIONS and _EVENTS", () => {
    expect(isGatewayTopic("T_PRIVATE_PRODUCT_RICH_NOTIFICATIONS")).toBe(false);
    expect(isGatewayTopic("T_PRIVATE_CORRECTED_DELIVERY_DATES_CHANGED_EVENTS")).toBe(false);
  });

  it("rejects DLQ_T_*", () => {
    expect(isGatewayTopic("DLQ_T_PRIVATE_SOURCE_ELASTIC_AUTOOPS")).toBe(false);
  });

  it("rejects system topics", () => {
    expect(isGatewayTopic("__consumer_offsets")).toBe(false);
    expect(isGatewayTopic("_schemas")).toBe(false);
    expect(isGatewayTopic("_confluent-monitoring")).toBe(false);
  });
});

describe("checkGatewayTopic", () => {
  it("returns ok for a valid topic", () => {
    expect(checkGatewayTopic("T_PRIVATE_SOURCE_ELASTIC_AUTOOPS")).toEqual({ ok: true });
  });

  it("returns a distinct message for T_PUBLIC_*", () => {
    const r = checkGatewayTopic("T_PUBLIC_SOURCE_PIM_ARTICLES_MDM");
    expect(r.ok).toBe(false);
    if (!r.ok) expect(r.message).toMatch(/MDM publisher/i);
  });

  it("returns a distinct message for T_PRIVATE_SINK_*", () => {
    const r = checkGatewayTopic("T_PRIVATE_SINK_COUCHBASE_PRICE_DOCUMENTS");
    expect(r.ok).toBe(false);
    if (!r.ok) expect(r.message).toMatch(/sink connector/i);
  });

  it("returns a distinct message for DLQ_T_*", () => {
    const r = checkGatewayTopic("DLQ_T_PRIVATE_SOURCE_ELASTIC_AUTOOPS");
    expect(r.ok).toBe(false);
    if (!r.ok) expect(r.message).toMatch(/dlqTopic/);
  });

  it("returns a distinct message for system topics", () => {
    const r = checkGatewayTopic("__consumer_offsets");
    expect(r.ok).toBe(false);
    if (!r.ok) expect(r.message).toMatch(/system topic/i);
  });

  it("rejects topics longer than 249 characters", () => {
    const long = "T_PRIVATE_SOURCE_X_" + "A".repeat(240);
    const r = checkGatewayTopic(long);
    expect(r.ok).toBe(false);
    if (!r.ok) expect(r.message).toMatch(/length/i);
  });
});

describe("expectedDlqTopic", () => {
  it("prefixes with DLQ_T_", () => {
    expect(expectedDlqTopic("T_PRIVATE_SOURCE_ELASTIC_AUTOOPS")).toBe(
      "DLQ_T_PRIVATE_SOURCE_ELASTIC_AUTOOPS",
    );
  });
});
