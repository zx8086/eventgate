import { Kafka, type Producer } from "kafkajs";
import { config } from "../config/index.ts";
import type { NormalizedEvent } from "../types.ts";
import { buildKafkaConfig } from "./clientConfig.ts";

export type EventProducer = {
  publishRaw(resourceId: string, raw: unknown): Promise<void>;
  publishNormalized(event: NormalizedEvent): Promise<void>;
  publishDlq(reason: string, payload: unknown, key?: string): Promise<void>;
  isConnected(): boolean;
  disconnect(): Promise<void>;
};

export async function createProducer(clientId: string): Promise<EventProducer> {
  const kafka = new Kafka(buildKafkaConfig(clientId));
  const producer: Producer = kafka.producer({ allowAutoTopicCreation: true });
  await producer.connect();
  let connected = true;

  producer.on(producer.events.DISCONNECT, () => {
    connected = false;
  });
  producer.on(producer.events.CONNECT, () => {
    connected = true;
  });

  return {
    isConnected: () => connected,
    async disconnect() {
      await producer.disconnect();
    },
    async publishRaw(resourceId, raw) {
      await producer.send({
        topic: config.kafka.topics.raw,
        messages: [
          {
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
        topic: config.kafka.topics.events,
        messages: [
          {
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
        topic: config.kafka.topics.dlq,
        messages: [
          {
            key: key ?? null,
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
