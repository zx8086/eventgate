// src/kafka/providers/msk.ts
import { getLogger } from "../../logging/index.ts";
import { KafkaProviderError } from "./errors.ts";
import type { KafkaConnectionConfig, KafkaProvider, MskAuthMode } from "./types.ts";

const log = getLogger("kafka.provider.msk");

// AWS SDK shape for GetBootstrapBrokersCommand response; subset we read.
export type BootstrapBrokersResponse = {
  BootstrapBrokerString?: string;
  BootstrapBrokerStringPublic?: string;
  BootstrapBrokerStringTls?: string;
  BootstrapBrokerStringPublicTls?: string;
  BootstrapBrokerStringSaslIam?: string;
  BootstrapBrokerStringPublicSaslIam?: string;
};

// Token cache shape; expiryTime is unix ms.
type CachedToken = { token: string; expiresAt: number };

// 60s safety margin: refetch before the broker rejects the token mid-handshake.
const TOKEN_REFRESH_MARGIN_MS = 60_000;

export function pickBrokerString(
  response: BootstrapBrokersResponse,
  authMode: MskAuthMode,
): string | undefined {
  if (authMode === "iam") {
    return response.BootstrapBrokerStringSaslIam ?? response.BootstrapBrokerStringPublicSaslIam;
  }
  if (authMode === "tls") {
    return response.BootstrapBrokerStringTls ?? response.BootstrapBrokerStringPublicTls;
  }
  return response.BootstrapBrokerString ?? response.BootstrapBrokerStringPublic;
}

export class MskKafkaProvider implements KafkaProvider {
  readonly type = "msk" as const;
  readonly name: string;
  private cachedToken: CachedToken | null = null;
  private resolvedBrokers: string[] | null = null;

  constructor(
    private readonly bootstrapBrokers: string[],
    private readonly clusterArn: string,
    private readonly region: string,
    private readonly clientId: string,
    private readonly authMode: MskAuthMode,
    private readonly authModeExplicit: boolean,
  ) {
    this.name = `AWS MSK (${authMode})`;
    if (!this.authModeExplicit) {
      log.warn(
        { resolvedAuthMode: this.authMode },
        "MSK_AUTH_MODE is unset; defaulting to configured value. Set MSK_AUTH_MODE=iam|tls|none to be explicit.",
      );
    }
  }

  async getConnectionConfig(): Promise<KafkaConnectionConfig> {
    const bootstrapBrokers = await this.resolveBrokers();

    if (this.authMode === "none") {
      return { clientId: this.clientId, bootstrapBrokers };
    }
    if (this.authMode === "tls") {
      return {
        clientId: this.clientId,
        bootstrapBrokers,
        tls: { rejectUnauthorized: true },
      };
    }
    return {
      clientId: this.clientId,
      bootstrapBrokers,
      sasl: {
        mechanism: "OAUTHBEARER",
        token: () => this.getToken(),
      },
      tls: { rejectUnauthorized: true },
      // MSK Serverless cold-starts can take 30-60s on first connect.
      connectTimeout: 60_000,
      timeout: 60_000,
      retries: 5,
      retryDelay: 2_000,
    };
  }

  async close(): Promise<void> {
    this.cachedToken = null;
    this.resolvedBrokers = null;
  }

  private async getToken(): Promise<string> {
    if (this.cachedToken && this.cachedToken.expiresAt > Date.now() + TOKEN_REFRESH_MARGIN_MS) {
      return this.cachedToken.token;
    }
    try {
      const { generateAuthToken } = await import("aws-msk-iam-sasl-signer-js");
      const result = await generateAuthToken({ region: this.region });
      this.cachedToken = { token: result.token, expiresAt: result.expiryTime };
      return result.token;
    } catch (err) {
      throw new KafkaProviderError(
        `Failed to generate MSK IAM token: ${err instanceof Error ? err.message : String(err)}`,
        "PROVIDER_AUTH_FAILED",
        "msk",
        err,
      );
    }
  }

  private async resolveBrokers(): Promise<string[]> {
    if (this.bootstrapBrokers.length > 0) return this.bootstrapBrokers;
    if (this.resolvedBrokers) return this.resolvedBrokers;

    if (!this.clusterArn) {
      throw new KafkaProviderError(
        "MSK provider requires either KAFKA_BROKERS or MSK_CLUSTER_ARN",
        "PROVIDER_CONFIG_INVALID",
        "msk",
      );
    }

    const { KafkaClient, GetBootstrapBrokersCommand } = await import("@aws-sdk/client-kafka");
    const client = new KafkaClient({ region: this.region });
    const response = await client.send(
      new GetBootstrapBrokersCommand({ ClusterArn: this.clusterArn }),
    );
    const brokerString = pickBrokerString(response as BootstrapBrokersResponse, this.authMode);
    if (!brokerString) {
      throw new KafkaProviderError(
        `No bootstrap brokers found for MSK cluster (authMode=${this.authMode})`,
        "PROVIDER_CONFIG_INVALID",
        "msk",
      );
    }
    const brokers = brokerString.split(",").map((s) => s.trim()).filter(Boolean);
    this.resolvedBrokers = brokers;
    return brokers;
  }
}
