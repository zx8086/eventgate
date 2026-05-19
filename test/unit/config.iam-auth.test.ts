import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { buildConfig } from "../../src/config/loader.ts";
import { resetConfigCache } from "../../src/config/loader.ts";

const baseProdEnv = {
  ENVIRONMENT: "prod",
  COUCHBASE_ENABLED: "false",
  KAFKA_BROKERS: "b-1.example.kafka-serverless.eu-central-1.amazonaws.com:9098",
  KAFKA_AUTH: "iam",
  KAFKA_REGION: "eu-central-1",
};

describe("IAM SASL config", () => {
  beforeEach(() => resetConfigCache());
  afterEach(() => resetConfigCache());

  it("accepts kafka.auth=iam with a region", () => {
    const cfg = buildConfig(baseProdEnv);
    expect(cfg.kafka.auth).toBe("iam");
    expect(cfg.kafka.region).toBe("eu-central-1");
  });

  it("rejects kafka.auth=iam without a region", () => {
    const env = { ...baseProdEnv, KAFKA_REGION: "" };
    expect(() => buildConfig(env)).toThrow(/kafka\.region.*required when auth=iam/);
  });

  it("rejects prod with kafka.auth=none", () => {
    const env = { ...baseProdEnv, KAFKA_AUTH: "none" };
    expect(() => buildConfig(env)).toThrow(/iam.*required in prod/);
  });

  it("defaults kafka.auth to none for dev", () => {
    const cfg = buildConfig({});
    expect(cfg.kafka.auth).toBe("none");
  });
});

describe("Couchbase enabled gate", () => {
  beforeEach(() => resetConfigCache());
  afterEach(() => resetConfigCache());

  it("defaults couchbase.enabled to true", () => {
    const cfg = buildConfig({});
    expect(cfg.couchbase.enabled).toBe(true);
  });

  it("skips couchbase prod-safety refinements when enabled=false", () => {
    const env = {
      ENVIRONMENT: "prod",
      COUCHBASE_ENABLED: "false",
      KAFKA_BROKERS: "b-1.example.kafka-serverless.eu-central-1.amazonaws.com:9098",
      KAFKA_AUTH: "iam",
      KAFKA_REGION: "eu-central-1",
      COUCHBASE_CONNSTR: "couchbase://localhost",
      COUCHBASE_PASSWORD: "password",
    };
    const cfg = buildConfig(env);
    expect(cfg.couchbase.enabled).toBe(false);
  });

  it("enforces couchbase prod-safety refinements when enabled=true", () => {
    const env = {
      ENVIRONMENT: "prod",
      COUCHBASE_ENABLED: "true",
      KAFKA_BROKERS: "b-1.example.kafka-serverless.eu-central-1.amazonaws.com:9098",
      KAFKA_AUTH: "iam",
      KAFKA_REGION: "eu-central-1",
      COUCHBASE_CONNSTR: "couchbase://localhost",
      COUCHBASE_PASSWORD: "password",
    };
    expect(() => buildConfig(env)).toThrow(/localhost connection string is not allowed in prod/);
  });
});
