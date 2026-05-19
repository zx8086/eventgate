// src/kafka/producer.ts
import { Producer, stringSerializers } from "@platformatic/kafka";
import { config } from "../config/index.ts";
import type { KafkaProvider } from "./providers/index.ts";

export type EventProducer = {
  publishRaw(resourceId: string, raw: unknown): Promise<void>;
  sendByTopic(
    topic: string,
    key: string,
    value: string,
    headers?: Record<string, string> | null,
  ): Promise<void>;
  isConnected(): boolean;
  disconnect(): Promise<void>;
};

export async function createProducer(
  clientId: string,
  provider: KafkaProvider,
): Promise<EventProducer> {
  const conn = await provider.getConnectionConfig();
  const producer = new Producer<string, string, string, string>({
    clientId,
    bootstrapBrokers: conn.bootstrapBrokers,
    ...(conn.sasl ? { sasl: conn.sasl } : {}),
    ...(conn.tls ? { tls: conn.tls } : {}),
    ...(conn.connectTimeout !== undefined ? { connectTimeout: conn.connectTimeout } : {}),
    ...(conn.timeout !== undefined ? { timeout: conn.timeout } : {}),
    ...(conn.retries !== undefined ? { retries: conn.retries } : {}),
    ...(conn.retryDelay !== undefined ? { retryDelay: conn.retryDelay } : {}),
    serializers: stringSerializers,
  });

  let connected = true;

  return {
    isConnected: () => connected,
    async disconnect() {
      connected = false;
      await producer.close();
    },
    async publishRaw(resourceId, raw) {
      await producer.send({
        messages: [
          {
            topic: config.kafka.topics.raw,
            key: resourceId,
            value: JSON.stringify({
              receivedAt: new Date().toISOString(),
              raw,
            }),
          },
        ],
      });
    },
    async sendByTopic(topic, key, value, headers) {
      await producer.send({
        messages: [
          {
            topic,
            key,
            value,
            ...(headers ? { headers } : {}),
          },
        ],
      });
    },
  };
}

export function resolveOutboxTopic(
  topic: "raw" | "events" | "dlq",
  topics: { raw: string; events: string; dlq: string },
): string {
  return topics[topic];
}
