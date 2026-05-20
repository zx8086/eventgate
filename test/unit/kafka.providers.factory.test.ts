import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { buildConfig, resetConfigCache } from "../../src/config/loader.ts";
import {
  ConfluentKafkaProvider,
  KafkaProviderError,
  LocalKafkaProvider,
  MskKafkaProvider,
  createKafkaProvider,
} from "../../src/kafka/providers/index.ts";
import type { AppConfig } from "../../src/config/index.ts";

describe("createKafkaProvider dispatch", () => {
  beforeEach(() => resetConfigCache());
  afterEach(() => resetConfigCache());

  it("returns LocalKafkaProvider for provider=local", () => {
    const cfg = buildConfig({});
    const provider = createKafkaProvider(cfg);
    expect(provider).toBeInstanceOf(LocalKafkaProvider);
    expect(provider.type).toBe("local");
    expect(provider.name).toBe("Local Kafka");
  });

  it("returns ConfluentKafkaProvider for provider=confluent", () => {
    const cfg = buildConfig({
      KAFKA_PROVIDER: "confluent",
      KAFKA_BROKERS: "pkc-1:9092",
      CONFLUENT_API_KEY: "k",
      CONFLUENT_API_SECRET: "s",
    });
    const provider = createKafkaProvider(cfg);
    expect(provider).toBeInstanceOf(ConfluentKafkaProvider);
    expect(provider.type).toBe("confluent");
  });

  it("returns MskKafkaProvider for provider=msk", () => {
    const cfg = buildConfig({
      KAFKA_PROVIDER: "msk",
      MSK_REGION: "eu-central-1",
      KAFKA_BROKERS: "b-1:9098",
      MSK_AUTH_MODE: "iam",
    });
    const provider = createKafkaProvider(cfg, { MSK_AUTH_MODE: "iam" });
    expect(provider).toBeInstanceOf(MskKafkaProvider);
    expect(provider.type).toBe("msk");
    expect(provider.name).toBe("AWS MSK (iam)");
  });

  it("throws PROVIDER_NOT_FOUND for an unknown provider value", () => {
    const cfg = buildConfig({}) as AppConfig;
    // Force an unknown provider value past Zod; this simulates a future enum value
    // the factory hasn't been taught about yet.
    (cfg.kafka as { provider: string }).provider = "kinesis";
    try {
      createKafkaProvider(cfg);
      throw new Error("expected createKafkaProvider to throw");
    } catch (err) {
      expect(err).toBeInstanceOf(KafkaProviderError);
      const provErr = err as KafkaProviderError;
      expect(provErr.code).toBe("PROVIDER_NOT_FOUND");
      expect(provErr.provider).toBe("kinesis");
    }
  });
});

describe("LocalKafkaProvider connection config", () => {
  beforeEach(() => resetConfigCache());
  afterEach(() => resetConfigCache());

  it("returns plain bootstrap brokers with no sasl/tls", async () => {
    const cfg = buildConfig({ KAFKA_BROKERS: "host1:9092,host2:9092" });
    const provider = createKafkaProvider(cfg);
    const conn = await provider.getConnectionConfig();
    expect(conn.bootstrapBrokers).toEqual(["host1:9092", "host2:9092"]);
    expect(conn.sasl).toBeUndefined();
    expect(conn.tls).toBeUndefined();
  });
});

describe("ConfluentKafkaProvider connection config", () => {
  beforeEach(() => resetConfigCache());
  afterEach(() => resetConfigCache());

  it("returns SASL/PLAIN + TLS for confluent", async () => {
    const cfg = buildConfig({
      KAFKA_PROVIDER: "confluent",
      KAFKA_BROKERS: "pkc-1:9092,pkc-2:9092",
      CONFLUENT_API_KEY: "k",
      CONFLUENT_API_SECRET: "s",
    });
    const provider = createKafkaProvider(cfg);
    const conn = await provider.getConnectionConfig();
    expect(conn.bootstrapBrokers).toEqual(["pkc-1:9092", "pkc-2:9092"]);
    expect(conn.sasl).toEqual({ mechanism: "PLAIN", username: "k", password: "s" });
    expect(conn.tls).toEqual({ rejectUnauthorized: true });
  });
});
