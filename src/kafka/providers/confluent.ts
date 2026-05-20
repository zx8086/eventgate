// src/kafka/providers/confluent.ts
import type { KafkaConnectionConfig, KafkaProvider } from "./types.ts";

export class ConfluentKafkaProvider implements KafkaProvider {
  readonly type = "confluent" as const;
  readonly name = "Confluent Cloud";

  constructor(
    private readonly bootstrapBrokers: string[],
    private readonly apiKey: string,
    private readonly apiSecret: string,
    private readonly clientId: string,
  ) {}

  async getConnectionConfig(): Promise<KafkaConnectionConfig> {
    return {
      clientId: this.clientId,
      bootstrapBrokers: this.bootstrapBrokers,
      sasl: {
        mechanism: "PLAIN",
        username: this.apiKey,
        password: this.apiSecret,
      },
      tls: { rejectUnauthorized: true },
    };
  }

  async close(): Promise<void> {}
}
