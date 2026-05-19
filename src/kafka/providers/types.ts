// src/kafka/providers/types.ts
import type { ConnectionOptions, SASLOptions } from "@platformatic/kafka";

export type MskAuthMode = "iam" | "tls" | "none";

export type KafkaProviderType = "local" | "msk" | "confluent";

export type KafkaConnectionConfig = {
  clientId: string;
  bootstrapBrokers: string[];
  sasl?: SASLOptions;
  tls?: ConnectionOptions["tls"];
  connectTimeout?: number;
  timeout?: number;
  retries?: number | boolean;
  retryDelay?: number;
};

export type KafkaProvider = {
  readonly type: KafkaProviderType;
  readonly name: string;
  getConnectionConfig(): Promise<KafkaConnectionConfig>;
  close(): Promise<void>;
};
