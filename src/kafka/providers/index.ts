// src/kafka/providers/index.ts
import type { AppConfig } from "../../config/index.ts";
import { ConfluentKafkaProvider } from "./confluent.ts";
import { KafkaProviderError } from "./errors.ts";
import { LocalKafkaProvider } from "./local.ts";
import { MskKafkaProvider } from "./msk.ts";
import type { KafkaProvider } from "./types.ts";

export type { KafkaConnectionConfig, KafkaProvider, MskAuthMode } from "./types.ts";
export { KafkaProviderError } from "./errors.ts";
export type { ProviderErrorCode } from "./errors.ts";
export { LocalKafkaProvider } from "./local.ts";
export { ConfluentKafkaProvider } from "./confluent.ts";
export { MskKafkaProvider, pickBrokerString } from "./msk.ts";

export function createKafkaProvider(
  config: AppConfig,
  env: NodeJS.ProcessEnv = process.env,
): KafkaProvider {
  const { kafka } = config;

  switch (kafka.provider) {
    case "local":
      return new LocalKafkaProvider(kafka.local.bootstrapServers, kafka.clientId);

    case "confluent":
      return new ConfluentKafkaProvider(
        kafka.confluent.bootstrapServers,
        kafka.confluent.apiKey,
        kafka.confluent.apiSecret,
        kafka.clientId,
      );

    case "msk":
      return new MskKafkaProvider(
        kafka.msk.brokers,
        kafka.msk.clusterArn,
        kafka.msk.region,
        kafka.clientId,
        kafka.msk.authMode,
        env.MSK_AUTH_MODE !== undefined && env.MSK_AUTH_MODE !== "",
      );

    default: {
      const provider: string = kafka.provider;
      throw new KafkaProviderError(
        `Unknown Kafka provider: ${provider}`,
        "PROVIDER_NOT_FOUND",
        provider,
      );
    }
  }
}
