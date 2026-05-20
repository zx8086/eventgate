// src/kafka/providers/local.ts
import type { KafkaConnectionConfig, KafkaProvider } from "./types.ts";

export class LocalKafkaProvider implements KafkaProvider {
  readonly type = "local" as const;
  readonly name = "Local Kafka";

  constructor(
    private readonly bootstrapBrokers: string[],
    private readonly clientId: string,
  ) {}

  async getConnectionConfig(): Promise<KafkaConnectionConfig> {
    return {
      clientId: this.clientId,
      bootstrapBrokers: this.bootstrapBrokers,
    };
  }

  async close(): Promise<void> {}
}
