import { Kafka, logLevel, type Consumer } from "kafkajs";
import { config } from "../config/index.ts";

export async function createConsumer(clientId: string, groupId: string): Promise<Consumer> {
  const kafka = new Kafka({
    clientId,
    brokers: config.kafka.brokers,
    logLevel: logLevel.INFO,
  });
  const consumer = kafka.consumer({ groupId });
  await consumer.connect();
  return consumer;
}
