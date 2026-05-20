import { afterEach, beforeEach, describe, expect, it, mock } from "bun:test";
import { resetConfigCache } from "../../src/config/loader.ts";
import { KafkaProviderError, MskKafkaProvider, pickBrokerString } from "../../src/kafka/providers/index.ts";
import type { BootstrapBrokersResponse } from "../../src/kafka/providers/msk.ts";

const SAMPLE_RESPONSE: BootstrapBrokersResponse = {
  BootstrapBrokerString: "b-private:9092",
  BootstrapBrokerStringPublic: "b-public:9092",
  BootstrapBrokerStringTls: "b-private:9094",
  BootstrapBrokerStringPublicTls: "b-public:9094",
  BootstrapBrokerStringSaslIam: "b-private:9098",
  BootstrapBrokerStringPublicSaslIam: "b-public:9098",
};

describe("pickBrokerString", () => {
  it("prefers private SaslIam endpoint for authMode=iam", () => {
    expect(pickBrokerString(SAMPLE_RESPONSE, "iam")).toBe("b-private:9098");
  });

  it("falls back to public SaslIam when private missing", () => {
    expect(pickBrokerString({ BootstrapBrokerStringPublicSaslIam: "b-public:9098" }, "iam")).toBe(
      "b-public:9098",
    );
  });

  it("prefers private TLS endpoint for authMode=tls", () => {
    expect(pickBrokerString(SAMPLE_RESPONSE, "tls")).toBe("b-private:9094");
  });

  it("falls back to public TLS when private missing", () => {
    expect(pickBrokerString({ BootstrapBrokerStringPublicTls: "b-public:9094" }, "tls")).toBe(
      "b-public:9094",
    );
  });

  it("uses plaintext brokers for authMode=none with public fallback", () => {
    expect(pickBrokerString(SAMPLE_RESPONSE, "none")).toBe("b-private:9092");
    expect(pickBrokerString({ BootstrapBrokerStringPublic: "b-public:9092" }, "none")).toBe(
      "b-public:9092",
    );
  });

  it("returns undefined when no matching broker string is present", () => {
    expect(pickBrokerString({}, "iam")).toBeUndefined();
  });
});

describe("MskKafkaProvider.getConnectionConfig", () => {
  beforeEach(() => resetConfigCache());
  afterEach(() => resetConfigCache());

  it("returns plaintext config for authMode=none", async () => {
    const provider = new MskKafkaProvider(["b-1:9092"], "", "eu-central-1", "cid", "none", true);
    const conn = await provider.getConnectionConfig();
    expect(conn.bootstrapBrokers).toEqual(["b-1:9092"]);
    expect(conn.sasl).toBeUndefined();
    expect(conn.tls).toBeUndefined();
  });

  it("returns TLS-only config for authMode=tls", async () => {
    const provider = new MskKafkaProvider(["b-1:9094"], "", "eu-central-1", "cid", "tls", true);
    const conn = await provider.getConnectionConfig();
    expect(conn.tls).toEqual({ rejectUnauthorized: true });
    expect(conn.sasl).toBeUndefined();
  });

  it("returns OAUTHBEARER SASL + TLS + generous timeouts for authMode=iam", async () => {
    const provider = new MskKafkaProvider(["b-1:9098"], "", "eu-central-1", "cid", "iam", true);
    const conn = await provider.getConnectionConfig();
    expect(conn.sasl?.mechanism).toBe("OAUTHBEARER");
    expect(typeof conn.sasl?.token).toBe("function");
    expect(conn.tls).toEqual({ rejectUnauthorized: true });
    expect(conn.connectTimeout).toBe(60_000);
    expect(conn.retries).toBe(5);
  });

  it("throws PROVIDER_CONFIG_INVALID when brokers and clusterArn are both empty", async () => {
    const provider = new MskKafkaProvider([], "", "eu-central-1", "cid", "iam", true);
    await expect(provider.getConnectionConfig()).rejects.toMatchObject({
      name: "KafkaProviderError",
      code: "PROVIDER_CONFIG_INVALID",
    });
  });
});

describe("MSK IAM token caching", () => {
  let callCount = 0;
  let nextExpiry = Date.now() + 15 * 60_000;
  let nextToken = "token-A";

  beforeEach(() => {
    callCount = 0;
    nextExpiry = Date.now() + 15 * 60_000;
    nextToken = "token-A";
    mock.module("aws-msk-iam-sasl-signer-js", () => ({
      generateAuthToken: async () => {
        callCount += 1;
        return { token: nextToken, expiryTime: nextExpiry };
      },
    }));
  });

  it("caches the token across calls until the 60s safety margin trips", async () => {
    const provider = new MskKafkaProvider(["b-1:9098"], "", "eu-central-1", "cid", "iam", true);
    const conn = await provider.getConnectionConfig();
    const tokenFn = conn.sasl?.token;
    if (typeof tokenFn !== "function") throw new Error("expected token function");

    const t1 = await tokenFn();
    const t2 = await tokenFn();
    expect(t1).toBe("token-A");
    expect(t2).toBe("token-A");
    expect(callCount).toBe(1);

    // Simulate token within 60s of expiry — must refetch.
    nextExpiry = Date.now() + 30_000;
    nextToken = "token-B";
    // Manually invalidate cache by setting a near-expiry first
    const provider2 = new MskKafkaProvider(["b-1:9098"], "", "eu-central-1", "cid", "iam", true);
    const conn2 = await provider2.getConnectionConfig();
    const tokenFn2 = conn2.sasl?.token;
    if (typeof tokenFn2 !== "function") throw new Error("expected token function");
    callCount = 0;
    const tA = await tokenFn2();
    const tB = await tokenFn2();
    // Both calls happen within the 60s safety margin -> second call must refetch.
    expect(tA).toBe("token-B");
    expect(tB).toBe("token-B");
    expect(callCount).toBe(2);
  });

  it("wraps signer errors in KafkaProviderError(PROVIDER_AUTH_FAILED)", async () => {
    mock.module("aws-msk-iam-sasl-signer-js", () => ({
      generateAuthToken: async () => {
        throw new Error("STS denied");
      },
    }));
    const provider = new MskKafkaProvider(["b-1:9098"], "", "eu-central-1", "cid", "iam", true);
    const conn = await provider.getConnectionConfig();
    const tokenFn = conn.sasl?.token;
    if (typeof tokenFn !== "function") throw new Error("expected token function");

    try {
      await tokenFn();
      throw new Error("expected throw");
    } catch (err) {
      expect(err).toBeInstanceOf(KafkaProviderError);
      const provErr = err as KafkaProviderError;
      expect(provErr.code).toBe("PROVIDER_AUTH_FAILED");
      expect(provErr.message).toContain("STS denied");
    }
  });
});
