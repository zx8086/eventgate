// src/kafka/producer.ts
import { Producer, stringSerializers } from "@platformatic/kafka";
import { config } from "../config/index.ts";
import type { NormalizedEvent } from "../types.ts";
import type { KafkaProvider } from "./providers/index.ts";

export type EventProducer = {
  publishRaw(resourceId: string, raw: unknown): Promise<void>;
  publishNormalized(event: NormalizedEvent): Promise<void>;
  publishDlq(reason: string, payload: unknown, key?: string): Promise<void>;
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
    async publishNormalized(event) {
      await producer.send({
        messages: [
          {
            topic: config.kafka.topics.events,
            key: event.routingKey,
            value: JSON.stringify(event),
            headers: {
              source: event.source,
              eventType: event.eventType,
              severity: event.alert.severity,
              schemaVersion: String(event.schemaVersion),
              idempotencyKey: event.idempotencyKey,
            },
          },
        ],
      });
    },
    async publishDlq(reason, payload, key) {
      await producer.send({
        messages: [
          {
            topic: config.kafka.topics.dlq,
            key: key ?? "",
            value: JSON.stringify({
              receivedAt: new Date().toISOString(),
              reason,
              payload,
            }),
          },
        ],
      });
    },
  };
}
