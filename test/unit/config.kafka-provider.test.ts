import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { buildConfig, resetConfigCache } from "../../src/config/loader.ts";

describe("kafka.provider dispatch", () => {
  beforeEach(() => resetConfigCache());
  afterEach(() => resetConfigCache());

  it("defaults provider to local with localhost:9092", () => {
    const cfg = buildConfig({});
    expect(cfg.kafka.provider).toBe("local");
    expect(cfg.kafka.local.bootstrapServers).toEqual(["localhost:9092"]);
  });

  it("accepts a CSV KAFKA_LOCAL_BOOTSTRAP_SERVERS override", () => {
    const cfg = buildConfig({ KAFKA_LOCAL_BOOTSTRAP_SERVERS: "a:9092,b:9092" });
    expect(cfg.kafka.local.bootstrapServers).toEqual(["a:9092", "b:9092"]);
  });
});

describe("msk provider validation", () => {
  beforeEach(() => resetConfigCache());
  afterEach(() => resetConfigCache());

  it("accepts msk with region + clusterArn", () => {
    const cfg = buildConfig({
      KAFKA_PROVIDER: "msk",
      MSK_REGION: "eu-central-1",
      MSK_CLUSTER_ARN: "arn:aws:kafka:eu-central-1:123:cluster/x/u",
    });
    expect(cfg.kafka.provider).toBe("msk");
    expect(cfg.kafka.msk.authMode).toBe("iam");
  });

  it("accepts msk with region + brokers (no ARN needed)", () => {
    const cfg = buildConfig({
      KAFKA_PROVIDER: "msk",
      MSK_REGION: "eu-central-1",
      MSK_BROKERS: "b-1.example.kafka-serverless.eu-central-1.amazonaws.com:9098",
    });
    expect(cfg.kafka.msk.brokers).toContain("b-1.example");
  });

  it("rejects msk without region", () => {
    expect(() =>
      buildConfig({
        KAFKA_PROVIDER: "msk",
        MSK_CLUSTER_ARN: "arn:aws:kafka:eu-central-1:123:cluster/x/u",
      }),
    ).toThrow(/msk\.region is required when provider=msk/);
  });

  it("rejects msk without brokers or arn", () => {
    expect(() =>
      buildConfig({
        KAFKA_PROVIDER: "msk",
        MSK_REGION: "eu-central-1",
      }),
    ).toThrow(/msk requires either clusterArn or brokers/);
  });

  it("rejects an invalid authMode", () => {
    expect(() =>
      buildConfig({
        KAFKA_PROVIDER: "msk",
        MSK_REGION: "eu-central-1",
        MSK_BROKERS: "b:9098",
        MSK_AUTH_MODE: "bogus",
      }),
    ).toThrow();
  });
});

describe("confluent provider validation", () => {
  beforeEach(() => resetConfigCache());
  afterEach(() => resetConfigCache());

  it("accepts confluent with the full triplet", () => {
    const cfg = buildConfig({
      KAFKA_PROVIDER: "confluent",
      CONFLUENT_BOOTSTRAP_SERVERS: "pkc-1.eu-central-1.aws.confluent.cloud:9092",
      CONFLUENT_API_KEY: "key",
      CONFLUENT_API_SECRET: "secret",
    });
    expect(cfg.kafka.provider).toBe("confluent");
  });

  it("rejects confluent without apiKey", () => {
    expect(() =>
      buildConfig({
        KAFKA_PROVIDER: "confluent",
        CONFLUENT_BOOTSTRAP_SERVERS: "pkc-1:9092",
        CONFLUENT_API_SECRET: "secret",
      }),
    ).toThrow(/confluent\.apiKey is required/);
  });

  it("rejects confluent without bootstrapServers", () => {
    expect(() =>
      buildConfig({
        KAFKA_PROVIDER: "confluent",
        CONFLUENT_API_KEY: "key",
        CONFLUENT_API_SECRET: "secret",
      }),
    ).toThrow(/confluent\.bootstrapServers is required/);
  });
});

describe("prod-safety rule", () => {
  beforeEach(() => resetConfigCache());
  afterEach(() => resetConfigCache());

  it("rejects provider=local in prod", () => {
    expect(() => buildConfig({ ENVIRONMENT: "prod", KAFKA_PROVIDER: "local" })).toThrow(
      /provider=local is not allowed in prod/,
    );
  });

  it("allows provider=msk in prod with full config", () => {
    const cfg = buildConfig({
      ENVIRONMENT: "prod",
      KAFKA_PROVIDER: "msk",
      MSK_REGION: "eu-central-1",
      MSK_BROKERS: "b-1.example.kafka-serverless.eu-central-1.amazonaws.com:9098",
    });
    expect(cfg.app.environment).toBe("prod");
    expect(cfg.kafka.provider).toBe("msk");
  });
});
