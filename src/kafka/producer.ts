// src/kafka/producer.ts
import { Producer, stringSerializers } from "@platformatic/kafka";
import type { KafkaProvider } from "./providers/index.ts";

export type EventProducer = {
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
